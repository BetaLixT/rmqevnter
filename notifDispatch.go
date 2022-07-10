package streamNotif

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/betalixt/gorr"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type NotificationDispatch struct {
	// Event queue
	eventQueue chan TracedEvent

	// Rabbit MQ
	connection  *amqp.Connection
	confirms    chan amqp.Confirmation
	rmqchan     *amqp.Channel

	// Message tracking
	messageCount      int
	messageCountMutex sync.Mutex

	// ack pending
	pendingMutex sync.Mutex
	pendingsRaw map[uint64]TracedEvent

	// Common
	closing bool
	lgr     *zap.Logger
	optn    *RabbitMQBatchPublisherOptions
	wg      sync.WaitGroup
}

func NewNotifDispatch(
	conn *amqp.Connection,
	optn *RabbitMQBatchPublisherOptions,
	lgr *zap.Logger,
) *NotificationDispatch {
	disp := &NotificationDispatch{
		eventQueue:   make(chan TracedEvent, 1000),
		connection:   conn,
		confirms:     make(chan amqp.Confirmation, 1),
		messageCount: 0,
		pendingsRaw:  map[uint64]TracedEvent{},
		closing:      false,
		lgr:          lgr,
		optn:         optn,
	}

  // - setting up channel
	chnl, err := disp.connection.Channel()
	if err != nil {
		panic(fmt.Errorf("error creating channel: %w", err))
	}
	disp.rmqchan = chnl
	err = disp.rmqchan.ExchangeDeclare(
		disp.optn.ExchangeName,
		disp.optn.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(fmt.Errorf("error declaring exchange: %w", err))
	}
	err = disp.rmqchan.Confirm(false)
	if err != nil {
		panic(fmt.Errorf("error setting to confirm mode: %w", err))
	}

	disp.rmqchan.NotifyPublish(disp.confirms)
	
	// - dispatching chan workers
	disp.wg.Add(2)
	go func() {	
		disp.confirmHandler()
		disp.wg.Done()
	}()
	go func() {	
		disp.processQueue()
		disp.wg.Done()
	}()

	return disp
}

func (disp *NotificationDispatch) Close() {
	disp.closing = true
	
	disp.lgr.Info("closing publish observer...")
	if disp.pendingMessages() != 0 {
		disp.lgr.Info(
			"waiting for pending messages",
		)
	}
	prevCount := disp.pendingMessages()
	sameCountRetr := 0
	
	for disp.pendingMessages() != 0 && sameCountRetr < 10 {
		time.Sleep(100 * time.Millisecond)
		curr := disp.pendingMessages()
		if prevCount == curr {
			sameCountRetr++
		} else {
			sameCountRetr = 0
			prevCount = curr
		}
	}
	close(disp.eventQueue)
	disp.rmqchan.Close()
	disp.wg.Wait()
}

// - Event channel handling
func (disp *NotificationDispatch) DispatchEventNotification(
	eventId string,
	stream string,
	streamId string,
	event string,
	version int,
	data interface{},
	createdDateTime time.Time,
	traceparent string,
	tracepart string,
) error {

	if disp.closing {
		return gorr.NewError(
			gorr.ErrorCode{
				Code:    12000,
				Message: "DispatchChannelClosed",
			},
			500,
			"",
		)
	}
	disp.messageQueued()
	disp.eventQueue <- TracedEvent{
		Event: EventEntity{
			Id:              eventId,
			Stream:          stream,
			StreamId:        streamId,
			Event:           event,
			StreamVersion:   version,
			Data:            data,
			CreatedDateTime: createdDateTime,
		},
		Traceparent: traceparent,
		Tracepart:   tracepart,
		Retries:     0,
	}
	return nil
}

func (disp *NotificationDispatch) retryEventNotification(
	evnt TracedEvent,
) {
	evnt.Retries += 1
	disp.eventQueue <- evnt
}

func (disp *NotificationDispatch) processQueue() {
	active := true
	var evnt TracedEvent
	for active {
		evnt, active = <-disp.eventQueue
		disp.publishEvent(evnt)
	}
}

// - RMQ publish handler
func (disp *NotificationDispatch) publishEvent(evnt TracedEvent) error {
	json, err := json.Marshal(evnt.Event)
	if err != nil {
		disp.lgr.Error("error marshalling message", zap.Error(err))
		return fmt.Errorf("error unmarshalling: %w", err)
	}
	sqno := disp.rmqchan.GetNextPublishSeqNo()
	// TODO implement tracepart
	err = disp.rmqchan.Publish(
		disp.optn.ExchangeName,
		fmt.Sprintf(
			"%s.%s.%s",
			disp.optn.ServiceName,
			evnt.Event.Stream,
			evnt.Event.Event,
		),
		true,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(json),
			Headers:     amqp.Table{
				"traceparent": evnt.Traceparent,
			},
		},
	)
	if err != nil {
		disp.lgr.Error("Failed to publish message", zap.Error(err))
		// TODO handle re connection
	}
	disp.setPending(sqno, evnt)
	return nil
}

func (b *NotificationDispatch) confirmHandler() {
	open := true
	var confirmed amqp.Confirmation
	for open {
		confirmed, open = <-b.confirms
		if confirmed.DeliveryTag > 0 {
			if confirmed.Ack {
			  // TODO: error handling just incase
				conf := b.getPending(confirmed.DeliveryTag)	
				b.lgr.Info(
					"confirmed notification delivery",
					zap.String("trcprnt", conf.Traceparent),
					zap.String("tpart", conf.Tracepart),
				)
				go func() {
					b.messageDispatched()
				}()
			} else {
				failed := b.getPending(confirmed.DeliveryTag)
				b.lgr.Warn(
					"failed notification delivery",
					zap.Int("retry", failed.Retries),
					zap.String("trcprnt", failed.Traceparent),
					zap.String("tpart", failed.Tracepart),
				)
				// the channel may be filled
				go func() {
					b.retryEventNotification(failed)
				}()
			}
			b.delPending(confirmed.DeliveryTag)
		}
	}
}

// - Pending ack messages
func (b *NotificationDispatch)getPending(key uint64) TracedEvent {
	b.pendingMutex.Lock()
	v := b.pendingsRaw[key]
	b.pendingMutex.Unlock()
	return v
}

func (b *NotificationDispatch)setPending(key uint64, evnt TracedEvent) {
	b.pendingMutex.Lock()
  b.pendingsRaw[key] = evnt
	b.pendingMutex.Unlock()
}

func (b *NotificationDispatch)delPending(key uint64) {
	b.pendingMutex.Lock()
  delete(b.pendingsRaw, key)
	b.pendingMutex.Unlock()
}

// - Message tracking handler
func (disp *NotificationDispatch) messageQueued() {
	disp.messageCountMutex.Lock()
	disp.messageCount += 1
	disp.messageCountMutex.Unlock()
}

func (disp *NotificationDispatch) messageDispatched() {
	disp.messageCountMutex.Lock()
	disp.messageCount -= 1
	disp.messageCountMutex.Unlock()
}

func (disp *NotificationDispatch) pendingMessages() int {
	disp.messageCountMutex.Lock()
	pending := disp.messageCount
	disp.messageCountMutex.Unlock()
	return pending
}


