package rmqevnter

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
	Event            EventEntity
	Ver              string
	Tid              string
	Pid              string
	Rid              string
	Flg              string
	Tracepart        string
	RequestStartTime time.Time
	Retries          int
}
