package memory

import (
	"context"
	"errors"
	"github.com/hextechpal/prio/core/api"
	"github.com/hextechpal/prio/engine/memory/internal/heap"
	"github.com/hextechpal/prio/engine/memory/internal/models"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultCapacity = 10000
)

type (
	Engine struct {
		mu        sync.RWMutex
		topicsMap map[string]*heap.MaxHeap[node]
		jobMap    map[int64]*models.Job
	}

	node struct {
		jobId    int64
		priority int32
	}
)

func (n node) Compare(x node) int {
	if n.priority == x.priority {
		return 0
	} else if n.priority < x.priority {
		return -1
	} else {
		return 1
	}
}

func NewEngine() *Engine {
	return &Engine{
		topicsMap: make(map[string]*heap.MaxHeap[node]),
		jobMap:    make(map[int64]*models.Job),
	}
}

func (m *Engine) GetTopics(_ context.Context) ([]string, error) {
	topics := make([]string, len(m.topicsMap))
	for tn := range m.topicsMap {
		topics = append(topics, tn)
	}
	return topics, nil
}

func (m *Engine) RegisterTopic(_ context.Context, req api.RegisterTopicRequest) (api.RegisterTopicResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topicsMap[req.Name]; ok {
		return api.RegisterTopicResponse{}, errors.New("topic already registered")
	}

	m.topicsMap[req.Name] = heap.NewMaxHeap[node](defaultCapacity)
	return api.RegisterTopicResponse{}, nil
}

func (m *Engine) Enqueue(_ context.Context, req api.EnqueueRequest) (api.EnqueueResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topicsMap[req.Topic]; !ok {
		return api.EnqueueResponse{}, errors.New("topics mot registered")
	}

	id := rand.Int63()

	job := &models.Job{
		ID:        id,
		Topic:     req.Topic,
		Payload:   req.Payload,
		Priority:  req.Priority,
		Status:    models.PENDING,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
	}

	err := m.topicsMap[req.Topic].Insert(node{
		jobId:    job.ID,
		priority: job.Priority,
	})
	if err != nil {
		return api.EnqueueResponse{}, err
	}

	m.jobMap[job.ID] = job

	return api.EnqueueResponse{JobId: id}, nil
}

func (m *Engine) Dequeue(_ context.Context, req api.DequeueRequest) (api.DequeueResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topicsMap[req.Topic]; !ok {
		return api.DequeueResponse{}, errors.New("topics mot registered")
	}

	n, err := m.topicsMap[req.Topic].ExtractMax()
	if err != nil {
		return api.DequeueResponse{}, errors.New("topics mot registered")
	}
	job := m.jobMap[n.jobId]
	job.ClaimedAt = time.Now().UnixMilli()

	job.ClaimedBy = req.Consumer
	job.Status = models.CLAIMED
	job.UpdatedAt = time.Now().UnixMilli()

	return api.DequeueResponse{
		JobId:    job.ID,
		Topic:    job.Topic,
		Payload:  job.Payload,
		Priority: job.Priority,
	}, nil
}

func (m *Engine) Ack(_ context.Context, req api.AckRequest) (api.AckResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.jobMap[req.JobId]
	if !ok {
		return api.AckResponse{}, api.ErrorAlreadyAcked
	}
	delete(m.jobMap, req.JobId)
	return api.AckResponse{Acked: true}, nil
}

func (m *Engine) ReQueue(_ context.Context, req api.RequeueRequest) (api.RequeueResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := int64(0)
	for _, job := range m.jobMap {
		if job.Status == models.CLAIMED && job.ClaimedAt < req.RequeueTs {
			job.Status = models.PENDING
			job.ClaimedAt = 0
			job.ClaimedBy = ""
			_ = m.topicsMap[job.Topic].Insert(node{jobId: job.ID, priority: job.Priority})
			count++
		}
	}

	return api.RequeueResponse{Count: count}, nil
}
