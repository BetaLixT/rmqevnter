package streamNotif

import "time"

type NotificationDispatch struct {
	observers map[string]INotificationObserver
}

func (disp *NotificationDispatch) Subscribe(
	key string,
	obs INotificationObserver,
) {
	_, exists := disp.observers[key]
	if !exists {
		disp.observers[key] = obs

	}
}

func (disp *NotificationDispatch) Unsubscribe(key string) {
	obs := disp.observers[key]
	if obs != nil {
		obs.OnCompleted()
		delete(disp.observers, key)
	}
}

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
) {
	for _, obs := range disp.observers {
		obs.OnNext(
			EventEntity{
				Id:              eventId,
				Stream:          stream,
				StreamId:        streamId,
				Event:           event,
				StreamVersion:   version,
				Data:            data,
				CreatedDateTime: createdDateTime,
			},
			traceparent,
			tracepart,
		)
	}
}

func (disp *NotificationDispatch) Close() {
	for _, obs := range disp.observers {
		obs.OnCompleted()
	}
}

func NewNotifDispatch() *NotificationDispatch {
	return &NotificationDispatch{
		observers: make(map[string]INotificationObserver),
	}
}
