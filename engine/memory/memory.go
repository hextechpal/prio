package memory

import (
	"context"
	"github.com/hextechpal/prio/core/models"
	"sync"
	"time"
)

type Engine struct {
	mu        sync.RWMutex
	topicsMap map[string]models.Topic
	jobMap    map[int64]models.Job
}

func NewEngine() *Engine {
	return &Engine{
		topicsMap: make(map[string]models.Topic),
		jobMap:    make(map[int64]models.Job),
	}
}

func (m *Engine) CreateTopic(ctx context.Context, topic string, description string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := int64(len(m.topicsMap) + 1)
	m.topicsMap[topic] = models.Topic{
		Name:        topic,
		Description: &description,
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	}
	return id, nil
}

func (m *Engine) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Engine) Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Engine) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Engine) GetTopics(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Engine) ReQueue(ctx context.Context, topic string, lastTs int64) (int, error) {
	//TODO implement me
	panic("implement me")
}
