package rmqevnter

import (
	"time"
)

type ITracer interface {
	TraceDependencyCustom(
		tid string,
		rid string,
		spanId string,
		dependencyType string,
		serviceName string,
		commandName string,
		success bool,
		startTimestamp time.Time,
		eventTimestamp time.Time,
		fields map[string]string,
	)
}
