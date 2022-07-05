package streamNotif

import (
	"fmt"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func TestPublishBatch(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("error while creating rabbitmq connection : %v", err)
		t.FailNow()
	}
	lch, err := conn.Channel()
	if err != nil {
		fmt.Printf("error while creating rabbitmq channel : %v", err)
		t.FailNow()
	}
	lch.ExchangeDeclare(
		"notifications",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	q, err := lch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	err = lch.QueueBind(
		q.Name,
		"#",
		"notifications",
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("error while creating rabbitmq queue: %v", err)
		t.FailNow()
	}
	msgs, err := lch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	lgr, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("error while creating rabbitmq connection : %v", err)
		t.FailNow()
	}
	recvCount := 0
	go func() {
		for range msgs {
			log.Printf("recv: %d", recvCount)
			recvCount++
		}
	}()

	dis := NewNotifDispatch()
	pub := NewRabbitMQBatchPublisher(
		conn,
		&RabbitMQBatchPublisherOptions{
			ExchangeName: "notifications",
			ExchangeType: "topic",
			ServiceName:  "test",
		},
		lgr,
	)
	_ = NewPublishObserverAndSubscribe(
		pub,
		dis,
		lgr,
		&PublishObserverOptions{
			MaxPublishRetries: 10,
		},
	)
	start := time.Now()
	n := 10000
	for i := 0; i < n; i++ {
		dis.DispatchEventNotification(
			"test",
			"test",
			"test",
			"test",
			i,
			nil,
			time.Now(),
			"test",
			"test",
		)
		// if i%5 == 0 {
		// 	time.Sleep(100 * time.Millisecond)
		// }
	}

	dis.Close()
	retr := 0
	for recvCount != n && retr < 100 {
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("Completed in %f", time.Now().Sub(start).Seconds())
	if recvCount != n {
		fmt.Printf("only %d messages were recieved out of %d", recvCount, n)
		t.FailNow()
	}
}
