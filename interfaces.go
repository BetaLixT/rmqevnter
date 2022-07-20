package rmqevnter

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

type IRabbitMQConnection interface {
	GetConnection(key string) *amqp.Connection
}
