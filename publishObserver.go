package streamNotif

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type PublishObserver struct {
	publisher     IBatchPublisher
	key           string
	eventQueue    chan TracedEvent
	retryQueue    chan TracedEvent
	trackingQueue chan int
	messageCount  int
	dispatch      *NotificationDispatch
	processingWg  sync.WaitGroup
	lgr           *zap.Logger
	optn          *PublishObserverOptions
	stopping      bool
	batch         []TracedEvent
}

var _ INotificationObserver = (*PublishObserver)(nil)

func (obs *PublishObserver) Subscribe(
	dispatch *NotificationDispatch,
) (err error) {
	if dispatch != nil {
		obs.retryQueue = make(chan TracedEvent, 1000)
		obs.trackingQueue = make(chan int, 1000)
		obs.messageCount = 0
		err := obs.publisher.Open(obs.retryQueue, obs.trackingQueue)
		if err != nil {
			return err
		}

		obs.dispatch = dispatch
		obs.eventQueue = make(chan TracedEvent, 1000)

		obs.processingWg.Add(3)
		go func() {
			obs.processQueue()
			obs.processingWg.Done()
		}()
		go func() {
			obs.processRetryQueue()
			obs.processingWg.Done()
		}()
		go func() {
			obs.processTrackingQueue()
			obs.processingWg.Done()
		}()

		obs.dispatch.Subscribe(obs.key, obs)
	} else {
		err = fmt.Errorf("already subscribed")
	}
	return
}

func (obs *PublishObserver) Unsubscribe() {
	obs.dispatch.Unsubscribe(obs.key)
}

func (obs *PublishObserver) OnNext (
	evnt EventEntity,
	traceparent string,
	tracepart string,
) error {

	if obs.stopping {
		return fmt.Errorf("service is closed")
	}
	obs.trackingQueue <- 1			
	obs.eventQueue <- TracedEvent{
		Event:       evnt,
		Traceparent: traceparent,
		Tracepart:   tracepart,
	}
	return nil
}

func (obs *PublishObserver) OnCompleted() {
	obs.stopping = true
	obs.lgr.Info("closing publish observer...")
	if obs.messageCount != 0 {
		obs.lgr.Info(
			"waiting for pending messages",
			zap.Int("pending", obs.messageCount),
		)
	}
	prevCount := obs.messageCount
	sameCountRetr := 0
	// TODO maybe lock required since read writes on messageCount may happen
	// at the same time
	for obs.messageCount != 0 && sameCountRetr < 10 {
		time.Sleep(100 * time.Millisecond)
		if prevCount == obs.messageCount {
			sameCountRetr++
		} else {
			sameCountRetr = 0
			prevCount = obs.messageCount
		}
	}
	close(obs.eventQueue)
	close(obs.retryQueue)
	close(obs.trackingQueue)
	obs.processingWg.Wait()
	obs.publisher.Close()
}

func (obs *PublishObserver) processQueue() {
	ticker := time.NewTicker(200 * time.Millisecond)
	ticker.Stop()
	defer ticker.Stop()

	active := true
	obs.batch = make([]TracedEvent, 0)
	evnt := TracedEvent{}

	for active {
		select {
		case <-ticker.C:
			if len(obs.batch) != 0 {
				ticker.Stop()
				obs.publisher.PublishBatch(obs.batch)
				obs.batch = make([]TracedEvent, 0)
				// ticker.Reset(200 * time.Millisecond)
			}

		case evnt, active = <-obs.eventQueue:
			if active {
				if len(obs.batch) == 0 {
					ticker.Reset(200 * time.Millisecond)
				}
				obs.batch = append(obs.batch, evnt)
			}
		}
	}
	// publish remaining messages
	if len(obs.batch) != 0 {
		obs.publisher.PublishBatch(obs.batch)
	}
}

func (obs *PublishObserver) processRetryQueue() {
	active := true
	var evnt TracedEvent
	for active {
		evnt, active = <-obs.retryQueue
		if active {
			if evnt.Retries < obs.optn.MaxPublishRetries {
				evnt.Retries++
				obs.eventQueue <- evnt
			} else {
				obs.trackingQueue <- -1
				obs.lgr.Error(
					"message publishing failed after max retries",
					zap.Int("retries", evnt.Retries),
					zap.Any("event", evnt.Event),
					zap.String("trcprnt", evnt.Traceparent),
					zap.String("tpart", evnt.Tracepart),
				)
			}
		}
	}
}

func (obs *PublishObserver) processTrackingQueue() {
	active := true
	var val int
	for active {
		val, active = <-obs.trackingQueue
		if active {
			obs.messageCount += val
		}
	}
}

func NewPublishObserverAndSubscribe(
	publisher IBatchPublisher,
	dispatch *NotificationDispatch,
	lgr *zap.Logger,
	optn *PublishObserverOptions,
) *PublishObserver {
	obs := PublishObserver{
		publisher: publisher,
		key:       "PublishObserver",
		lgr:       lgr,
		optn:      optn,
		stopping:  false,
	}
	err := obs.Subscribe(dispatch)
	if err != nil {
		panic(fmt.Errorf("failed to subscribe: %w", err))
	}
	return &obs
}
