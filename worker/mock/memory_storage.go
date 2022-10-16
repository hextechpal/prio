package mock

import (
	"context"
	"sync"
	"time"

	"github.com/hextechpal/prio/core/models"
)

type MemoryStorage struct {
	mu        sync.RWMutex
	topicsMap map[string]models.Topic
	jobMap    map[int64]models.Job
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		topicsMap: make(map[string]models.Topic),
		jobMap:    make(map[int64]models.Job),
	}
}

func (m *MemoryStorage) CreateTopic(ctx context.Context, topic string, desciption string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := int64(len(m.topicsMap) + 1)
	m.topicsMap[topic] = models.Topic{
		Name:        topic,
		Description: &desciption,
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	}
	return id, nil
}

func (m *MemoryStorage) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryStorage) Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryStorage) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	//TODO implement me
	panic("implement me")
}
