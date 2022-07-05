# rmqevnter
Package to fire and forget events

## Description
Library to fire and forget events, built for an event based microservices
environment, the following functionalities are provided in the library
* Dispatch notification on events
* Observer for notifications that batches and publishes events
* RabbitMQ publisher for the above mentioned observer
* Custom publisher can be developed for the batch observer by implementing the
IBatchPublisher interface and providing to the publish observer
* Custom observer can be developed for the notification dispatch by implementing
the INotificationObserver interface, multiple observers can be regsistered to
the NotificationDispatch

## Installation
1. Install module
```bash
go get github.com/BetaLixT/rmqevnter
```
2. Import
```go
import "github.com/BetaLixT/rmqevnter"
```

## Usage
Construct dispatcher
```go
dis := NewNotifDispatch()
```
Observers can be registered with the Subscribe function, the PublishObserver
has a construct + subscribe function included that will automatically subscribe
to the provided dispatch

A batch publisher needs to be created so it can be passed on to the the publish
observer, a RabbitMQ batch publisher is bundled in this package, a connection
will need to be created and passed down to the RabbitMQ publisher

```go
pub := NewRabbitMQBatchPublisher(
  conn,
  &RabbitMQBatchPublisherOptions{
    ExchangeName: "notifications",
    ExchangeType: "topic",
    ServiceName: "test",
  },
  lgr, // zap logger
)
obs := NewPublishObserverAndSubscribe(
  pub,
  dis,
  lgr, // zap logger
  &PublishObserverOptions{
    MaxPublishRetries: 10,
  },
)
```
New notifications can be dispatched with the DispatchEventNotification function
every subscribed observer will get a copy of the event
```go
dis.DispatchEventNotification(
  "f0927d88-de63-4133-b8ab-e16b6e5ce391", // unique id for the event
  "Users", // stream(Domain) event is occuring for
  "150b59f5-9694-49a1-a19d-08820fab753f", // unique identifier for the stream item
  "Updated", // event that has been actioned on the stream item 
  2, // Version of the stream item (-1 recommended if it's not tracked)
  time.Now(), // time the event has occured
  "{\"key\":\"value\"}", // data related to the action
  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // w3c traceparent
  "1234", // trace partition to be used for partitioning and querying logs
)
```
