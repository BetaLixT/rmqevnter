package streamNotif

import "time"

type EventEntity struct {
	ServiceName     string      `json:"serviceName"`
	Id              string      `json:"id"`
	Stream          string      `json:"stream"`
	StreamId        string      `json:"streamId"`
	StreamVersion   int         `json:"streamVersion"`
	Event           string      `json:"event"`
	Data            interface{} `json:"data"`
	CreatedDateTime time.Time   `json:"createdDateTime"`
}

type TracedEvent struct {
	Event       EventEntity
	Traceparent string
	Tracepart   string
	Retries     int
}
