package core

import (
	"context"
	"time"

	"github.com/hextechpal/prio/core/models"
)

type Prio struct {
	s Storage
}

func NewPrio(s Storage) *Prio {
	return &Prio{s: s}
}

// Enqueue : Accepts an incoming job request and persist it durably via the persistence engine
func (p *Prio) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
	job := toJob(r)
	jobId, err := p.s.Save(ctx, job)
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
