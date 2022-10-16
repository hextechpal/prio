package mock

import (
	"context"
	"sync"
	"time"

	models2 "github.com/hextechpal/prio/internal/models"
)

type MemoryStorage struct {
	mu        sync.RWMutex
	topicsMap map[string]models2.Topic
	jobMap    map[int64]models2.Job
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		topicsMap: make(map[string]models2.Topic),
		jobMap:    make(map[int64]models2.Job),
	}
}

func (m *MemoryStorage) CreateTopic(ctx context.Context, topic string, desciption string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := int64(len(m.topicsMap) + 1)
	m.topicsMap[topic] = models2.Topic{
		Name:        topic,
		Description: &desciption,
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	}
	return id, nil
}

func (m *MemoryStorage) Enqueue(ctx context.Context, job *models2.Job) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryStorage) Dequeue(ctx context.Context, topic string, consumer string) (*models2.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryStorage) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	//TODO implement me
	panic("implement me")
}
