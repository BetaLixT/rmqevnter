package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BetaLixT/rmqevnter"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type MockTracer struct {
}

var _ streamNotif.ITracer = (*MockTracer)(nil)

func (_ *MockTracer) TraceDependencyCustom(
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
) {

}

type RabbitMQMockConnection struct {
	Connection *amqp091.Connection
	CloseNotif chan *amqp091.Error
	mtx sync.Mutex
}

func (r *RabbitMQMockConnection) GetConnection(
	key string,
) *amqp091.Connection {
	r.mtx.Lock()
	x := r.Connection
	r.mtx.Unlock()
	return x
}

func NewRabbitMQMockConnection(
) *RabbitMQMockConnection {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}

	n := RabbitMQMockConnection {
		Connection: conn,
		CloseNotif: make(chan *amqp091.Error, 1),
	}
	conn.NotifyClose(n.CloseNotif)
	go func() {
		active := true
		var _ error
		for active {
			err, active = <- n.CloseNotif
			
			// fmt.Printf(err.Error())
			n.mtx.Lock()
			for true {
				n.Connection, err = amqp091.Dial("amqp://guest:guest@localhost:5672/")
				if err != nil {
					fmt.Printf("recconnecting ")
					fmt.Printf("connection failed")
					time.Sleep(100 * time.Millisecond)
					continue
				}
				break
			}
			n.mtx.Unlock()
			
		}
	}()
	return &n
}

func TestNotificationDispatch() error {
	r := NewRabbitMQMockConnection()
	lch, err := r.GetConnection("").Channel()
	if err != nil {
		fmt.Printf("error while creating rabbitmq channel : %v", err)
		return err
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
		return err
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
		return err
	}
	recvCount := 0
	go func() {
		for range msgs {
			log.Printf("recv: %d", recvCount)
			recvCount++
		}
	}()

	disCore := streamNotif.NewNotifDispatch(
		r,
		&streamNotif.RabbitMQBatchPublisherOptions{
			ExchangeName: "notifications",
			ExchangeType: "topic",
			ServiceName:  "test",
		},
		lgr,
		&MockTracer{},
	)
	dis := streamNotif.NewNotificationDispatchTraceContext(
		disCore,
		"00",
		"0000000000000000",
		"00000000",
		"00000000",
		"00",
		"0000",
	)

	start := time.Now()
	n := 5
	for i := 0; i < n; i++ {
		dis.DispatchNotification(
			"test",
			"test",
			"test",
			"test",
			i,
			nil,
			time.Now(),
		)
		time.Sleep(5 * time.Second)
		// if i%5 == 0 {
		// 	time.Sleep(100 * time.Millisecond)
		// }
	}

	disCore.Close()
	retr := 0
	for recvCount != n && retr < 100 {
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("Completed in %f", time.Now().Sub(start).Seconds())
	if recvCount != n {
		fmt.Printf("only %d messages were recieved out of %d", recvCount, n)
		return err
	}
	return nil
}


func main() {
	err := TestNotificationDispatch()
	if err != nil {
		fmt.Printf(err.Error())
	}
	
}


