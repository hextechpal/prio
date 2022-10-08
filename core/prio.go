package core

import (
	"context"
	"time"

	"github.com/hextechpal/prio/internal/models"
)

type Prio struct {
	engine Engine
}

func NewPrio(e Engine) *Prio {
	return &Prio{engine: e}
}

// Enqueue : Accepts an incoming job request and persist it durably via the persistence engine
func (p *Prio) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
	job := toJob(r)
	jobId, err := p.engine.Save(ctx, job)
	if err != nil {
		return nil, err
	}
	return &EnqueueResponse{JobId: jobId}, nil
}

func (p *Prio) Dequeue(r *DequeueRequest) (*DequeueResponse, error) {
	return nil, nil
}

func toJob(r *EnqueueRequest) *models.Job {
	return &models.Job{
		Payload:   r.Payload,
		Priority:  r.Priority,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		Status:    models.PENDING,
	}
}
