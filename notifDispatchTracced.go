package streamNotif

import "time"

type NotificationDispatchTraceContext struct {
	ver   string
	tid   string
	pid   string
	rid   string
	flg   string
	tpart string
	dis   *NotificationDispatch
}

func NewNotificationDispatchTraceContext(
	dis *NotificationDispatch,
	ver string,
	tid string,
	pid string,
	rid string,
	flg string,
	tpart string,
) *NotificationDispatchTraceContext {
	return &NotificationDispatchTraceContext{
		dis:   dis,
		ver:   ver,
		tid:   tid,
		pid:   pid,
		rid:   rid,
		flg:   flg,
		tpart: tpart,
	}
}

func (tdis *NotificationDispatchTraceContext) DispatchNotification(
	eventId string,
	stream string,
	streamId string,
	event string,
	version int,
	data interface{},
	createdDateTime time.Time,
) {
	tdis.dis.DispatchEventNotification(
		eventId,
		stream,
		streamId,
		event,
		version,
		data,
		createdDateTime,
		tdis.ver,
		tdis.tid,
		tdis.pid,
		tdis.rid,
		tdis.flg,
		tdis.tpart,
	)
}
