package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/mysql-engine/storage"
)

func TestE2E(t *testing.T) {
	rand.Seed(time.Now().UnixMilli())
	ctx := context.TODO()
	c := &storage.Config{
		Driver: "mysql",
		DSN:    fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", "root", "root", "localhost", 3306, "prio"),
	}

	l := commons.NewLogger(ctx)
	s, err := storage.NewStorage(c, l)
	if err != nil {
		t.Logf("failed to init storage: %s", err.Error())
		t.FailNow()
	}

	topic := fmt.Sprintf("topic_%d", rand.Intn(100))
	p := core.NewPrio(s)
	_, err = p.RegisterTopic(ctx, &core.RegisterTopicRequest{
		Name:        topic,
		Description: "Description for topic1",
	})

	if err != nil {
		t.Logf("failed to register topic: %s", err.Error())
		t.FailNow()
	}

	res, err := p.Enqueue(ctx, &core.EnqueueRequest{
		Topic:    topic,
		Priority: 1,
		Payload:  []byte("payload"),
	})

	if err != nil {
		t.Logf("failed to enqueue job: %s", err.Error())
		t.FailNow()
	}

	t.Logf("enqueued job with job id=%d", res.JobId)

	deq, err := p.Dequeue(ctx, &core.DequeueRequest{Topic: topic})
	if err != nil {
		t.Logf("failed to dequeue job: %s", err.Error())
		t.FailNow()
	}

	t.Logf("enqueued job with job id=%d, payload=%s", deq.JobId, deq.Payload)

}
