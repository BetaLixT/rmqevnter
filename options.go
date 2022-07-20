package rmqevnter

type RabbitMQBatchPublisherOptions struct {
	ExchangeName string
	ExchangeType string
	ServiceName  string
}

type PublishObserverOptions struct {
	MaxPublishRetries int
}
