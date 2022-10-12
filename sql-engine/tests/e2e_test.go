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

	wantCh := make(chan []int64)
	gotCh := make(chan []int64)

	go func(ch chan []int64, count int) {
		type jobPriority struct {
			JobId    int64
			priority int32
		}

		allJobs := make([]jobPriority, count)
		for i := 0; i < count; i++ {
			priority := int32(rand.Intn(100))
			res, _ := p.Enqueue(ctx, &core.EnqueueRequest{
				Topic:    topic,
				Priority: priority,
				Payload:  []byte(fmt.Sprintf("payload_%d", i)),
			})
			allJobs[i] = jobPriority{JobId: res.JobId, priority: priority}

		}
		sort.Slice(allJobs, func(i, j int) bool {
			return allJobs[i].priority < allJobs[j].priority
		})
		jobIds := make([]int64, count)
		for i, jp := range allJobs {
			jobIds[i] = jp.JobId
		}
		ch <- jobIds
	}(wantCh, 1000)

	go func(ch chan []int64) {
		jobOrder := make([]int64, 0)
		for len(jobOrder) < 1000 {
			dequeue, err := p.Dequeue(ctx, &core.DequeueRequest{Topic: topic})
			if err != nil {
				if err == core.ErrorJobNotPresent {
					time.Sleep(2 * time.Millisecond)
					continue
				} else {
					t.Logf("breaking deque %s, dequed jobs=%d", err.Error(), len(jobOrder))
					break
				}
			}

			jobOrder = append(jobOrder, dequeue.JobId)
		}
		ch <- jobOrder
	}(gotCh)

	want := <-wantCh
	got := <-gotCh

	if !reflect.DeepEqual(want, got) {
		t.Fail()
	}

}

func setup(ctx context.Context, t *testing.T) *core.Prio {
	rand.Seed(time.Now().UnixMilli())
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
