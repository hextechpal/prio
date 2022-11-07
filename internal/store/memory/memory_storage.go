package memory

import (
	"context"
	"github.com/hextechpal/prio/internal/models"
	"sync"
	"time"
)

type Storage struct {
	mu        sync.RWMutex
	topicsMap map[string]models.Topic
	jobMap    map[int64]models.Job
}

func NewStorage() *Storage {
	return &Storage{
		topicsMap: make(map[string]models.Topic),
		jobMap:    make(map[int64]models.Job),
	}
}

func (m *Storage) CreateTopic(ctx context.Context, topic string, description string) (int64, error) {
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

func (m *Storage) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Storage) Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Storage) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	//TODO implement me
	panic("implement me")
}
