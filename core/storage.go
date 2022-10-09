package core

import (
	"context"

	"github.com/hextechpal/prio/core/models"
)

type Storage interface {
	// Save : Persist a jon in to the datastore
	Save(context.Context, *models.Job) (int64, error)
}
