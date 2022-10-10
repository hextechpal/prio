package main

import (
	"context"

	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/mysql-engine/storage"
)

func main() {
	ctx := context.TODO()
	c := &storage.Config{
		User:     "root",
		Password: "root",
		Host:     "127.0.0.1",
		Port:     3306,
	}

	l := commons.NewLogger(ctx)
	s, err := storage.NewStorage(c, l)
	if err != nil {
		panic(err)
	}
	p := core.NewPrio(s)

	_, err = p.RegisterTopic(ctx, &core.RegisterTopicRequest{
		Name:        "topic1",
		Description: "Description for topic1",
	})

	if err != nil {
		panic(err)
	}

	res, err := p.Enqueue(ctx, &core.EnqueueRequest{
		Topic:    "topic1",
		Priority: 1,
		Payload:  []byte("payload"),
	})

	if err != nil {
		panic(err)
	}

	l.Info().Msgf("Task Enqueued with task id: %d", res.JobId)
}
