package api

import (
	"context"
)

type Engine interface {
	// GetTopics :Creates a new topic
	// TODO: Add support for filters and cache
	GetTopics(ctx context.Context) ([]string, error)

	// RegisterTopic :Creates a new topic
	RegisterTopic(ctx context.Context, req RegisterTopicRequest) (RegisterTopicResponse, error)

	// Enqueue : Persist a job in to the storage engine
	Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResponse, error)

	// Dequeue : Picks the top priority job from the given topic and returns empty if no job is present
	Dequeue(ctx context.Context, req DequeueRequest) (DequeueResponse, error)

	// Ack : Acknowledge a claimed jon, job is marked as complete once the acknowledgement is received
	// If consumer do not ack the jobId after a fixed amount of time (10sec) for mysql engine
	// the job will be moved back to pending state and is available to deque again based on priority
	// TODO : Make the ack interval configurable
	// TODO : Think about the priority should the get higher priority because they were delivered once
	Ack(ctx context.Context, request AckRequest) (AckResponse, error)

	// ReQueue : Requeue operation runs periodically and move the unacked jobs to the pending queue again
	ReQueue(ctx context.Context, req RequeueRequest) (RequeueResponse, error)
}
