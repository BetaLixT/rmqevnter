package streamNotif

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/zap"
)

type TestBatchPublisher struct {
	Batches   int
	Count     int
	Completed bool
	retr      chan TracedEvent
	trch      chan int
}

func (b *TestBatchPublisher) Open(retr chan TracedEvent, trch chan int) error {
	b.retr = retr
	b.trch = trch
	return nil
}

func (b *TestBatchPublisher) PublishBatch(evnts []TracedEvent) error {
	for _, evnt := range evnts {
		if rand.Intn(100) == 50 {
			fmt.Printf("error lmao\n")
			b.retr <- evnt
			continue
		}
		b.trch <- -1
		fmt.Printf("%d: %d %v\n", b.Batches, b.Count, evnt)
		b.Count++
	}
	b.Batches++
	return nil
}

func (b *TestBatchPublisher) Close() {
	b.Completed = true
}

var _ IBatchPublisher = (*TestBatchPublisher)(nil)

func TestNotificationDispatch(t *testing.T) {
	lgr, err := zap.NewProduction()
	if err != nil {
		fmt.Println("logger has failed me")
		t.FailNow()
	}
	dispatch := NewNotifDispatch()
	batchPub := TestBatchPublisher{}
	_ = NewPublishObserverAndSubscribe(
		&batchPub,
		dispatch,
		lgr,
		&PublishObserverOptions{
			MaxPublishRetries: 100,
		},
	)
	n := 2000
	for i := 0; i < n; i++ {
		dispatch.DispatchEventNotification(
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
		if i%5 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	dispatch.Close()
	if batchPub.Count != n {
		fmt.Printf("count miss match")
		t.Fail()
	}
	if batchPub.Completed == false {
		fmt.Printf("not completed")
		t.Fail()
	}
}
