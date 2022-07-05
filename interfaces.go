package streamNotif

type INotificationObserver interface {
  OnNext(
    evnt EventEntity,
    traceparent string,
    tracepartition string,
  ) error
  OnCompleted()
}

type IBatchPublisher interface {
  PublishBatch([]TracedEvent) error
  Open(retrych chan TracedEvent, trackch chan int) error
  Close()
}
