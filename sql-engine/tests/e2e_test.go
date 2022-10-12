package main

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/mysql-engine/storage"
)

const (
	user     = "root"
	password = "root"
	host     = "127.0.0.1"
	port     = 3306
	dbname   = "prio"
)

func TestE2E(t *testing.T) {
	ctx := context.TODO()
	rand.Seed(time.Now().UnixMilli())
	topic := fmt.Sprintf("topic_%d", rand.Intn(100))
	p := setup(ctx, t)

	_, err := p.RegisterTopic(ctx, &core.RegisterTopicRequest{
		Name:        topic,
		Description: "Description for topic1",
	})

	if err != nil {
		t.Logf("failed to register topic: %s", err.Error())
		t.FailNow()
	}

	gotCh := make(chan int32)
	doneCh := make(chan bool)
	count := 100000

	start := time.Now()
	want := enqueueJobs(t, ctx, p, topic, count)
	t.Logf("Enqueue took %v", time.Since(start))

	start = time.Now()
	go dequeueJobs(t, ctx, p, topic, gotCh, doneCh, 1)

	got := make([]int32, 0)

	for i := 0; i < count; i++ {
		got = append(got, <-gotCh)
	}

	t.Logf("Dequeue took %v", time.Since(start))
	if !reflect.DeepEqual(want, got) {
		t.Fail()
	}

}

func dequeueJobs(t *testing.T, ctx context.Context, p *core.Prio, topic string, gotCh chan int32, done chan bool, workers int) {
	t.Helper()
	for i := 0; i < workers; i++ {
		go func(wid int, gotCh chan int32, done chan bool) {
			for {
				select {
				case <-done:
					return
				default:
					dequeue, err := p.Dequeue(ctx, &core.DequeueRequest{Topic: topic})
					if err != nil {
						break
					}
					gotCh <- dequeue.Priority
				}
			}
		}(i, gotCh, done)
	}
}

func enqueueJobs(t *testing.T, ctx context.Context, p *core.Prio, topic string, count int) []int32 {
	t.Helper()
	type jobPriority struct {
		JobId    int64
		priority int32
	}

	allJobs := make([]jobPriority, count)
	for i := 0; i < count; i++ {
		priority := int32(rand.Intn(100))
		res, err := p.Enqueue(ctx, &core.EnqueueRequest{
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

func setup(ctx context.Context, t *testing.T) *core.Prio {
	t.Helper()
	c := &storage.Config{
		Driver: "mysql",
		DSN:    fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname),
	}

	l := commons.NewLogger(ctx)
	s, err := storage.NewStorage(c, l)
	if err != nil {
		t.Logf("failed to init storage: %s", err.Error())
		t.FailNow()
	}

	return core.NewPrio(s)
}
