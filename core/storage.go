package core

import (
	"context"

	"github.com/hextechpal/prio/core/models"
)

type Storage interface {
	// CreateTopic :Creates a new topic
	CreateTopic(ctx context.Context, topic string, desciption string) (int64, error)

	// Enqueue : Persist a jon in to the datastore
	Enqueue(ctx context.Context, job *models.Job) (int64, error)

	Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error)

	Ack(ctx context.Context, topic string, id int64, consumer string) error
}