package examples

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	worker2 "github.com/hextechpal/prio/internal/worker"
)

func TestE2E(t *testing.T) {
	ctx := context.TODO()
	rand.Seed(time.Now().UnixMilli())
	topic := fmt.Sprintf("topic_%d", rand.Intn(100))
	w, err := setup(ctx, t)
	if err != nil {
		t.Fatalf("cannot start worker %v", err)
	}

	_, err = w.RegisterTopic(ctx, &worker2.RegisterTopicRequest{
		Name:        topic,
		Description: "Description for topic1",
	})

	if err != nil {
		t.Logf("failed to register topic: %s", err.Error())
		t.FailNow()
	}

	gotCh := make(chan int32)
	doneCh := make(chan bool)
	count := 100

	start := time.Now()
	want := enqueueJobs(t, ctx, w, topic, count)
	t.Logf("Enqueue took %v", time.Since(start))

	start = time.Now()
	go dequeueJobs(t, ctx, w, topic, gotCh, doneCh, 1)

	got := make([]int32, 0)

	for i := 0; i < count; i++ {
		got = append(got, <-gotCh)
	}

	t.Logf("Dequeue took %v", time.Since(start))
	if !reflect.DeepEqual(want, got) {
		t.Fail()
	}

}

func dequeueJobs(t *testing.T, ctx context.Context, p *worker2.Worker, topic string, gotCh chan int32, done chan bool, workers int) {
	t.Helper()
	for i := 0; i < workers; i++ {
		go func(wid int, gotCh chan int32, done chan bool) {
			for {
				select {
				case <-done:
					return
				default:
					dequeue, err := p.Dequeue(ctx, &worker2.DequeueRequest{Topic: topic})
					if err != nil {
						break
					}
					gotCh <- dequeue.Priority
				}
			}
		}(i, gotCh, done)
	}
}

func enqueueJobs(t *testing.T, ctx context.Context, p *worker2.Worker, topic string, count int) []int32 {
	t.Helper()
	type jobPriority struct {
		JobId    int64
		priority int32
	}

	allJobs := make([]jobPriority, count)
	for i := 0; i < count; i++ {
		priority := int32(rand.Intn(100))
		res, err := p.Enqueue(ctx, &worker2.EnqueueRequest{
			Topic:    topic,
			Priority: priority,
			Payload:  []byte(fmt.Sprintf("payload_%d", i)),
		})

		if err != nil {
			t.Errorf("error while enqueue=%s", err.Error())
		}
		allJobs[i] = jobPriority{JobId: res.JobId, priority: priority}

	}
	sort.Slice(allJobs, func(i, j int) bool {
		return allJobs[i].priority > allJobs[j].priority
	})
	jobIds := make([]int32, count)
	for i, jp := range allJobs {
		jobIds[i] = jp.priority
	}
	return jobIds
}
