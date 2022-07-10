package streamNotif

import (
	"sync"
	"time"

	"github.com/betalixt/gorr"
)

type NotificationDispatch struct {
	eventQueue        chan TracedEvent
	messageCount      int
	messageCountMutex sync.Mutex
	closing           bool
}

func NewNotifDispatch() *NotificationDispatch {
	return &NotificationDispatch{
		eventQueue:    make(chan TracedEvent, 1000),
		messageCount: 0,
		closing:       false,
	}
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
func (disp *NotificationDispatch) publishEvent(evnt TracedEvent) {

}

// - Message tracking handler
func (disp *NotificationDispatch) messageQueued(){
	disp.messageCountMutex.Lock()
	disp.messageCount += 1
	disp.messageCountMutex.Unlock()
}

func (disp *NotificationDispatch) messageDispatched(){
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



func (disp *NotificationDispatch) Close() {
	disp.closing = true
}
