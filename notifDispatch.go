package rmqevnter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/BetaLixT/usago"
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
	chnlManager *usago.ChannelManager
	channelCtx  *usago.ChannelContext
	cnfrmch     *chan amqp.Confirmation

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
	chnlManager *usago.ChannelManager,
	optn *RabbitMQBatchPublisherOptions,
	lgr *zap.Logger,
	tracer ITracer,
) *NotificationDispatch {

	disp := &NotificationDispatch{
		eventQueue:   make(chan TracedEvent, 1000),
		chnlManager:  chnlManager,
		messageCount: 0,
		pendingsRaw:  map[uint64]TracedEvent{},
		closing:      false,
		closed:       false,
		lgr:          lgr,
		optn:         optn,
		tracer:       tracer,
	}

	// - setting up channel
	chnlBuilder := usago.NewChannelBuilder().WithExchange(
		disp.optn.ExchangeName,
		disp.optn.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	).WithConfirms(true)

	channelCtx, err := chnlManager.NewChannel(*chnlBuilder)
	if err != nil {
		lgr.Error(
			"failed to build channel",
			zap.Error(err),
		)
		panic(err)
	}
	disp.channelCtx = channelCtx
	cnfrmch, err := channelCtx.GetConfirmsChannel()
	if err != nil {
		lgr.Error(
			"failed to fetch confirms channel",
			zap.Error(err),
		)
		panic(err)
	}
	disp.cnfrmch = &cnfrmch

	// - dispatching chan workers
	disp.wg.Add(2)
	go func() {
		disp.confirmHandler(*disp.cnfrmch)
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
	disp.closed = true
	disp.chnlManager.Discard(disp.channelCtx)
	disp.wg.Wait()
}

// - Event channel handling
func (disp *NotificationDispatch) DispatchEventNotification(
	ctx context.Context,
	eventId string,
	stream string,
	streamId string,
	event string,
	version int,
	data interface{},
	createdDateTime time.Time,
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
	ver, tid, pid, rid, flg := disp.tracer.ExtractTraceInfo(ctx)
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
		Tracepart: "000",
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
		if active {
			disp.publishEvent(evnt)
		}
	}
}

// - RMQ publish handler
func (disp *NotificationDispatch) publishEvent(evnt TracedEvent) error {
	json, err := json.Marshal(evnt.Event)
	if err != nil {
		disp.lgr.Error("error marshalling message", zap.Error(err))
		return fmt.Errorf("error unmarshalling: %w", err)
	}

	// TODO implement tracepart
	evnt.RequestStartTime = time.Now()
	sqno, err := disp.channelCtx.Publish(
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
		disp.retryEventNotification(evnt)
	} else {
		disp.setPending(sqno, evnt)
	}
	return nil
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
				b.tracer.TraceDependencyWithIds(
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
				b.lgr.Info(
					"confirmed notification delivery",
					zap.String("tid", conf.Tid),
					zap.String("pid", conf.Pid),
					zap.String("rid", conf.Rid),
					zap.String("tpart", conf.Tracepart),
					zap.Int("stver", conf.Event.StreamVersion),
				)
				b.messageDispatched()
			} else {
				failed := b.getPending(confirmed.DeliveryTag)
				b.tracer.TraceDependencyWithIds(
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
