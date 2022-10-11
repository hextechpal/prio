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

// RegisterTopic : Register a new topic with prio instance
func (p *Prio) RegisterTopic(ctx context.Context, r *RegisterTopicRequest) (*RegisterTopicResponse, error) {
	topicID, err := p.s.CreateTopic(ctx, r.Name, r.Description)
	if err != nil {
		return nil, err
	}
	return &RegisterTopicResponse{topicID: topicID}, nil
}

// Enqueue : Accepts an incoming job request and persist it durably via the persistence engine
func (p *Prio) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
	job := toJob(r)
	jobId, err := p.s.Enqueue(ctx, job)
	if err != nil {
		return nil, err
	}
	return &EnqueueResponse{JobId: jobId}, nil
}

func (p *Prio) Dequeue(ctx context.Context, r *DequeueRequest) (*DequeueResponse, error) {
	job, err := p.s.Dequeue(ctx, r.Topic)
	if err != nil {
		return nil, err
	}

	return toDequeue(job), nil
}

func toDequeue(job *models.Job) *DequeueResponse {
	return &DequeueResponse{
		JobId:   job.ID,
		Topic:   job.Topic,
		Payload: job.Payload,
	}
}

func toJob(r *EnqueueRequest) *models.Job {
	return &models.Job{
		Payload:   r.Payload,
		Priority:  r.Priority,
		Topic:     r.Topic,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		Status:    models.PENDING,
	}
}
