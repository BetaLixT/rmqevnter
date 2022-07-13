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

const (
	CONNECTION_KEY = "rmqevnter"
)

type NotificationDispatch struct {
	// Event queue
	eventQueue chan TracedEvent

	// Rabbit MQ
	connection IRabbitMQConnection
	cnfrmch    *chan amqp.Confirmation
	clsntch    *chan *amqp.Error
	rmqchan    *amqp.Channel
	rmqchanMtx sync.Mutex

	// Message tracking
	messageCount      int
	messageCountMutex sync.Mutex

	// ack pending
	pendingMutex sync.Mutex
	pendingsRaw  map[uint64]TracedEvent

	// Common
	closing bool
	closed  bool
	lgr     *zap.Logger
	optn    *RabbitMQBatchPublisherOptions
	wg      sync.WaitGroup
	tracer  ITracer
}

func NewNotifDispatch(
	conn IRabbitMQConnection,
	optn *RabbitMQBatchPublisherOptions,
	lgr *zap.Logger,
	tracer ITracer,
) *NotificationDispatch {
	confirms := make(chan amqp.Confirmation, 1)
	closenotif := make(chan *amqp.Error)
	disp := &NotificationDispatch{
		eventQueue:   make(chan TracedEvent, 1000),
		connection:   conn,
		cnfrmch:      &confirms,
		clsntch:      &closenotif,
		messageCount: 0,
		pendingsRaw:  map[uint64]TracedEvent{},
		closing:      false,
		closed:       false,
		lgr:          lgr,
		optn:         optn,
		tracer:       tracer,
	}

	// - setting up channel
	chnl, err := disp.connection.GetConnection(CONNECTION_KEY).Channel()
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

	disp.rmqchan.NotifyPublish(*disp.cnfrmch)
	disp.rmqchan.NotifyClose(*disp.clsntch)

	// - dispatching chan workers
	disp.wg.Add(3)
	go func() {
		disp.confirmHandler(*disp.cnfrmch)
		disp.wg.Done()
	}()
	go func() {
		disp.closeHandler(*disp.clsntch)
		disp.wg.Done()
	}()
	go func() {
		disp.processQueue()
		disp.wg.Done()
	}()

	return disp
}

func (disp *NotificationDispatch) reconnect() error {
	for true {
		chnl, err := disp.connection.GetConnection(CONNECTION_KEY).Channel()
		if err != nil {
			disp.lgr.Warn("error creating channel", zap.Error(err))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		disp.rmqchan = chnl

		confirms := make(chan amqp.Confirmation, 1)
		closenotif := make(chan *amqp.Error)
		// TODO review memory leak risk
		disp.cnfrmch = &confirms
		disp.clsntch = &closenotif

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
			disp.lgr.Error("error declaring exchange", zap.Error(err))
		}
		err = disp.rmqchan.Confirm(false)
		if err != nil {
			disp.lgr.Error("error setting to confirm mode", zap.Error(err))
		}

		disp.rmqchan.NotifyPublish(*disp.cnfrmch)
		disp.rmqchan.NotifyClose(*disp.clsntch)

		// - dispatching chan workers
		disp.wg.Add(2)
		go func() {
			disp.confirmHandler(*disp.cnfrmch)
			disp.wg.Done()
		}()
		go func() {
			disp.closeHandler(*disp.clsntch)
			disp.wg.Done()
		}()
		break
	}
	return nil
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
	disp.closed = true
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
	ver string,
	tid string,
	pid string,
	rid string,
	flg string,
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
		Ver:       ver,
		Tid:       tid,
		Pid:       pid,
		Rid:       rid,
		Flg:       flg,
		Tracepart: tracepart,
		Retries:   0,
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

	disp.rmqchanMtx.Lock()
	sqno := disp.rmqchan.GetNextPublishSeqNo()
	// TODO implement tracepart
	evnt.RequestStartTime = time.Now()
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
			Headers: amqp.Table{
				"traceparent": fmt.Sprintf(
					"%s-%s-%s-%s",
					evnt.Ver,
					evnt.Tid,
					evnt.Pid,
					evnt.Flg,
				),
			},
		},
	)
	if err != nil {
		disp.lgr.Error("Failed to publish message", zap.Error(err))
		disp.eventQueue <- evnt
		// TODO handle re connection
	} else {
		disp.setPending(sqno, evnt)
	}
	disp.rmqchanMtx.Unlock()
	return nil
}

func (dis *NotificationDispatch) closeHandler(closentf chan *amqp.Error) {
	err, _ := <-closentf
	if !dis.closed {
		dis.rmqchanMtx.Lock()
		dis.lgr.Warn("rabbit mq channel has been disconnected", zap.Error(err))
		// reconnect
		dis.reconnect()
		dis.rmqchanMtx.Unlock()
	}
}

func (b *NotificationDispatch) confirmHandler(confirms chan amqp.Confirmation) {
	open := true
	var confirmed amqp.Confirmation
	for open {
		confirmed, open = <-confirms
		if confirmed.DeliveryTag > 0 {
			if confirmed.Ack {
				// TODO: error handling just incase
				conf := b.getPending(confirmed.DeliveryTag)
				b.tracer.TraceDependencyCustom(
					conf.Tid,
					conf.Rid,
					"",
					"RabbitMQ",
					b.optn.ExchangeName,
					"notify",
					true,
					conf.RequestStartTime,
					time.Now(),
					map[string]string{},
				)
				b.lgr.Debug(
					"confirmed notification delivery",
					zap.String("tid", conf.Tid),
					zap.String("pid", conf.Pid),
					zap.String("rid", conf.Rid),
					zap.String("tpart", conf.Tracepart),
				)
				b.messageDispatched()
			} else {
				failed := b.getPending(confirmed.DeliveryTag)
				b.tracer.TraceDependencyCustom(
					failed.Tid,
					failed.Rid,
					"",
					"RabbitMQ",
					b.optn.ExchangeName,
					"notify",
					false,
					failed.RequestStartTime,
					time.Now(),
					map[string]string{},
				)
				b.lgr.Warn(
					"failed notification delivery",
					zap.Int("retry", failed.Retries),
					zap.String("tid", failed.Tid),
					zap.String("pid", failed.Pid),
					zap.String("rid", failed.Rid),
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
	b.lgr.Debug("confirms channel has closed")
}

// - Pending ack messages
func (b *NotificationDispatch) getPending(key uint64) TracedEvent {
	b.pendingMutex.Lock()
	v := b.pendingsRaw[key]
	b.pendingMutex.Unlock()
	return v
}

func (b *NotificationDispatch) setPending(key uint64, evnt TracedEvent) {
	b.pendingMutex.Lock()
	b.pendingsRaw[key] = evnt
	b.pendingMutex.Unlock()
}

func (b *NotificationDispatch) delPending(key uint64) {
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
