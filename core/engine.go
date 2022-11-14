package core

import (
	"context"
	"github.com/hextechpal/prio/core/models"
)

type Engine interface {
	// GetTopics :Creates a new topic
	// TODO: Add support for filters and cache
	GetTopics(ctx context.Context) ([]string, error)

	// CreateTopic :Creates a new topic
	CreateTopic(ctx context.Context, topic string, description string) (int64, error)

	// Enqueue : Persist a jon in to the datastore
	Enqueue(ctx context.Context, job *models.Job) (int64, error)

	// Dequeue :
	Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error)

	// Ack :
	Ack(ctx context.Context, topic string, id int64, consumer string) error

	// ReQueue :
	ReQueue(ctx context.Context, topic string, lastTs int64) (int, error)
}
