package core

import (
	"context"

	"github.com/hextechpal/prio/internal/models"
)

type Engine interface {
	// Save : Persist a jon in to the datastore
	Save(context.Context, *models.Job) (int64, error)
}
