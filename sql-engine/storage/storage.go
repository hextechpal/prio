package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core/models"
	"github.com/jmoiron/sqlx"
)

const (
	addTopic = `INSERT INTO topics(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)`
	addJob   = `INSERT INTO jobs(topic, payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`

	topJob   = `SELECT jobs.id, jobs.payload from jobs where jobs.topic = ? ORDER BY priority DESC, updated_at ASC LIMIT 1 FOR UPDATE`
	claimJob = `UPDATE jobs SET status = ?, delivered_at = ?  WHERE jobs.id = ?`
)

var (
	log                 = &commons.PLogger{}
	ErrorJobNotAcquired = errors.New("job not acquired")
	ErrorJobNotPresent  = errors.New("job not present")
)

type Storage struct {
	*sqlx.DB
	c *Config
}

func NewStorage(c *Config, l *commons.PLogger) (*Storage, error) {
	log = l
	db, err := sqlx.Connect(c.Driver, c.DSN)
	if err != nil {
		l.Error().Err(err)
		return nil, err
	}
	s := &Storage{db, c}
	return s, nil
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

func (s *Storage) CreateTopic(ctx context.Context, name, description string) (int64, error) {
	stmt, err := s.Preparex(addTopic)
	if err != nil {
		return -1, err
	}
	r, err := stmt.Exec(name, description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	log.Info().Msgf("topic %s registered successfully", name)
	return r.LastInsertId()
}

func (s *Storage) Dequeue(ctx context.Context, topic string) (*models.Job, error) {
	// Find Top priority item with status pending
	// Update the status to delivered and delivered_at timestamp to NOW()
	// return the updated object

	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Error().Err(err)
		}
	}()

	var job models.Job
	sJOb, err := tx.Preparex(topJob)
	if err != nil {
		return nil, err

	}
	err = sJOb.GetContext(ctx, &job, topic)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrorJobNotPresent
		}
		return nil, err
	}

	result, err := tx.ExecContext(ctx, claimJob, models.DELIVERED, time.Now().UnixMilli(), job.ID)
	if err != nil {
		return nil, err
	}

	if affected, err := result.RowsAffected(); err != nil || affected == 0 {
		return nil, ErrorJobNotAcquired
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &job, nil
}
