package streamNotif

import (
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type RabbitMQBatchPublisher struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	optn         *RabbitMQBatchPublisherOptions
	pendingMutex sync.Mutex
	pendingsRaw  map[uint64]TracedEvent
	confirms     chan amqp.Confirmation
	confrmwg     sync.WaitGroup
	retrych      chan TracedEvent
	trachch      chan int
	closing      bool
	lgr          *zap.Logger
}

func (b *RabbitMQBatchPublisher)getPending(key uint64) TracedEvent {
	b.pendingMutex.Lock()
	v := b.pendingsRaw[key]
	b.pendingMutex.Unlock()
	return v
}

func (b *RabbitMQBatchPublisher)setPending(key uint64, evnt TracedEvent) {
	b.pendingMutex.Lock()
  b.pendingsRaw[key] = evnt
	b.pendingMutex.Unlock()
}

func (b *RabbitMQBatchPublisher)delPending(key uint64) {
	b.pendingMutex.Lock()
  delete(b.pendingsRaw, key)
	b.pendingMutex.Unlock()
}

func (b *RabbitMQBatchPublisher) Open(
	retrych chan TracedEvent,
	trackch chan int,
) error {

	// - setting up channels
	chnl, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("error creating channel: %w", err)
	}
	b.ch = chnl
	err = b.ch.ExchangeDeclare(
		b.optn.ExchangeName,
		b.optn.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error declaring exchange: %w", err)
	}
	err = b.ch.Confirm(false)
	if err != nil {
		return fmt.Errorf("error setting to confirm mode: %w", err)
	}

	// - setting up properties
	b.ch.NotifyPublish(b.confirms)
	b.retrych = retrych
	b.trachch = trackch

	go func() {
		b.confrmwg.Add(1)
		b.confirmHandler()
		b.confrmwg.Done()
	}()
	return nil
}

func (b *RabbitMQBatchPublisher) PublishBatch(evnts []TracedEvent) error {
	if b.closing {
		return fmt.Errorf("publisher is now closed")
	}
	for _, evnt := range evnts {
		json, err := json.Marshal(evnt.Event)
		if err != nil {
			b.lgr.Error("error marshalling message", zap.Error(err))
			return fmt.Errorf("error unmarshalling: %w", err)
		}
		sqno := b.ch.GetNextPublishSeqNo()
		// TODO implement tracepart
		err = b.ch.Publish(
			b.optn.ExchangeName,
			fmt.Sprintf(
				"%s.%s.%s",
				b.optn.ServiceName,
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
			b.lgr.Error("Failed to publish message", zap.Error(err))
			// TODO handle re connection
			continue
		}
		b.setPending(sqno, evnt)
	}
	return nil
}

func (b *RabbitMQBatchPublisher) Close() {
	b.ch.Close()
	// this one is closed by rabbitmq client TIL
	// close(b.confirms)
	b.confrmwg.Wait()
}

func (b *RabbitMQBatchPublisher) confirmHandler() {

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
					b.trachch <- -1
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
					b.retrych <- failed
				}()
			}
			b.delPending(confirmed.DeliveryTag)
		}
		// if len(b.pendings) > 1 {
		// 	b.lgr.Info(
		// 		"outstanding confirmations",
		// 		zap.Int("unconfirmed", len(b.pendings)),
		// 	)
		// }
	}
}

var _ IBatchPublisher = (*RabbitMQBatchPublisher)(nil)

func NewRabbitMQBatchPublisher(
	conn *amqp.Connection,
	optn *RabbitMQBatchPublisherOptions,
	lgr *zap.Logger,
) *RabbitMQBatchPublisher {
	return &RabbitMQBatchPublisher{
		conn:     conn,
		optn:     optn,
		pendingsRaw: map[uint64]TracedEvent{},
		confirms: make(chan amqp.Confirmation, 1),
		closing:  false,
		lgr:      lgr,
	}
}
