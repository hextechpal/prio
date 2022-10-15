package worker

import (
	"context"
	"time"

	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/core/models"
)

type prio struct {
	s core.Storage
}

// RegisterTopic : Register a new topic with prio instance
func (p *prio) RegisterTopic(ctx context.Context, r *RegisterTopicRequest) (*RegisterTopicResponse, error) {
	topicID, err := p.s.CreateTopic(ctx, r.Name, r.Description)
	if err != nil {
		return nil, err
	}
	return &RegisterTopicResponse{topicID: topicID}, nil
}

// Enqueue : Accepts an incoming job request and persist it durably via the persistence engine
func (p *prio) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
	job := toJob(r)
	jobId, err := p.s.Enqueue(ctx, job)
	if err != nil {
		return nil, err
	}
	return &EnqueueResponse{JobId: jobId}, nil
}

func (p *prio) Dequeue(ctx context.Context, r *DequeueRequest) (*DequeueResponse, error) {
	job, err := p.s.Dequeue(ctx, r.Topic, r.Consumer)
	if err != nil {
		return nil, err
	}

	return toDequeue(job), nil
}

func (p *prio) Ack(ctx context.Context, r *AckRequest) (*AckResponse, error) {
	err := p.s.Ack(ctx, r.Topic, r.JobId, r.Consumer)
	if err != nil {
		return nil, err
	}
	return &AckResponse{acked: true}, nil
}

func toDequeue(job *models.Job) *DequeueResponse {
	return &DequeueResponse{
		JobId:    job.ID,
		Topic:    job.Topic,
		Payload:  job.Payload,
		Priority: job.Priority,
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
