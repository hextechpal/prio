package storage

import (
	"context"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core/models"
	"github.com/jmoiron/sqlx"
)

const (
	dbName   = "prio"
	addTopic = `INSERT INTO topics(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)`
	addJob   = `INSERT INTO jobs(topic, payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`
	popJob   = `SELECT * FROM jobs where `
)

type Storage struct {
	*sqlx.DB
	c *Config
	l *commons.PLogger
}

func NewStorage(c *Config, l *commons.PLogger) (*Storage, error) {
	db, err := sqlx.Connect("mysql", assembleDSN(c))
	if err != nil {
		l.Error().Err(err)
		return nil, err
	}
	s := &Storage{db, c, l}
	return s, nil
}

func assembleDSN(c *Config) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, dbName)
}

func (s *Storage) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	stmt, err := s.Preparex(addJob)
	if err != nil {
		return -1, err
	}
	r, err := stmt.Exec(job.Topic, job.Payload, job.Priority, job.Status, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	return r.LastInsertId()
}

func (s *Storage) Dequeue(ctx context.Context, topic string) (*models.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) CreateTopic(ctx context.Context, name, description string) (int64, error) {
	stmt, err := s.Preparex(addTopic)
	if err != nil {
		return -1, err
	}
	r, err := stmt.Exec(name, description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	s.l.Info().Msgf("topic %s registered successfully", name)
	return r.LastInsertId()
}
