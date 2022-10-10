package core

import (
	"context"

	"github.com/hextechpal/prio/core/models"
)

type Storage interface {
	// CreateTopic :Creates a new topic
	CreateTopic(context.Context, string, string) (int64, error)

	// Enqueue : Persist a jon in to the datastore
	Enqueue(context.Context, *models.Job) (int64, error)

	Dequeue(context.Context, string) (*models.Job, error)
}
