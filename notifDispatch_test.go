package streamNotif

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/BetaLixT/usago"
	"go.uber.org/zap"
)

type MockTracer struct {
}

var _ ITracer = (*MockTracer)(nil)

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

func TestNotificationDispatch(t *testing.T) {
	lgr, err := zap.NewProduction()
	manager := usago.NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)
	if err != nil {
		fmt.Printf("error while creating rabbitmq channel : %v", err)
		t.FailNow()
	}
	cnsmBldr := usago.NewChannelBuilder().WithExchange(
		"notifications",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	).WithQueue(
		"temporary01",
		false,
		false,
		true,
		false,
		nil,
	).WithQueueBinding(
		"temporary01",
		"#",
		"notifications",
		false,
		nil,
	)
	
	cnsmCtx, err := manager.NewChannel(*cnsmBldr)
	if err != nil {
		fmt.Printf("error while creating rabbitmq queue: %v", err)
		t.FailNow()
	}

	cnsmChan, err := cnsmCtx.RegisterConsumer(
		"temporary01",
		"",
		true,
		false,
		false,
		false,
		nil,
	)	
	if err != nil {
		fmt.Printf("error while registering consumer : %v", err)
		t.FailNow()
	}
	recvCount := 0
	go func() {
		active := true
		for active {
			_, active = <- cnsmChan
			log.Printf("recv: %d", recvCount)
			recvCount++
		}	
	}()

	disCore := NewNotifDispatch(
		manager,
		&RabbitMQBatchPublisherOptions{
			ExchangeName: "notifications",
			ExchangeType: "topic",
			ServiceName:  "test",
		},
		lgr,
		&MockTracer{},
	)
	dis := NewNotificationDispatchTraceContext(
		disCore,
		"00",
		"0000000000000000",
		"00000000",
		"00000000",
		"00",
		"0000",
	)

	start := time.Now()
	n := 1000000
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
		// time.Sleep(5 * time.Second)
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
		t.FailNow()
	}
}
