package mysql

import (
	"context"
	"database/sql"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/core/models"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

const (
	allTopics = `SELECT topics.name from topics`
	addTopic  = `INSERT INTO topics(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)`

	addJob = `INSERT INTO jobs(topic, payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`

	topJob   = `SELECT jobs.id, jobs.payload, jobs.priority from jobs where jobs.topic = ? AND jobs.status = ? ORDER BY priority DESC, updated_at LIMIT 1 FOR UPDATE`
	claimJob = `UPDATE jobs SET status = ?, claimed_at = ?, claimed_by = ?  WHERE jobs.id = ?`

	jobById     = `SELECT jobs.id, jobs.status from jobs where jobs.id = ? AND jobs.topic = ? FOR UPDATE `
	completeJob = `UPDATE jobs SET status = ?, completed_at = ? WHERE jobs.id = ?`

	reQueue = `UPDATE jobs SET status = ?, claimed_at = ?, claimed_by = ?, updated_at = ? WHERE jobs.topic = ? AND jobs.status = ? AND jobs.claimed_at < ?`
)

type Engine struct {
	*sqlx.DB
	config Config
	logger *zerolog.Logger
}

func NewEngine(config Config) (*Engine, error) {
	db, err := sqlx.Connect("mysql", config.dsn())
	if err != nil {
		return nil, err
	}

	s := &Engine{
		DB:     db,
		config: config,
	}
	return s, nil
}

func (s *Engine) GetTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := s.SelectContext(ctx, &topics, allTopics)
	if err != nil {
		return []string{}, err
	}
	return topics, nil
}

func (s *Engine) CreateTopic(ctx context.Context, name, description string) (int64, error) {
	r, err := s.ExecContext(ctx, addTopic, name, description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	s.logger.Info().Msgf("topic %s registered successfully", name)
	return r.LastInsertId()
}

func (s *Engine) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	r, err := s.ExecContext(ctx, addJob, job.Topic, job.Payload, job.Priority, job.Status, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	return r.LastInsertId()
}

func (s *Engine) Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error) {
	// Find Top priority item with status pending
	// Update the status to delivered and delivered_at timestamp to NOW()
	// return the updated object

	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.Error().Err(err)
		}
	}()

	var job models.Job
	err := s.GetContext(ctx, &job, topJob, topic, models.PENDING)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, core.ErrorJobNotPresent
		}
		return nil, err
	}

	result, err := tx.ExecContext(ctx, claimJob, models.CLAIMED, time.Now().UnixMilli(), consumer, job.ID)
	if err != nil {
		return nil, err
	}

	if affected, err := result.RowsAffected(); err != nil || affected == 0 {
		return nil, core.ErrorJobNotAcquired
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Engine) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.Error().Err(err)
		}
	}()

	var job models.Job
	err := s.GetContext(ctx, &job, jobById, id, topic)
	if err != nil {
		if err == sql.ErrNoRows {
			return core.ErrorJobNotPresent
		}
		return err
	}

	if job.Status == models.COMPLETED {
		return core.ErrorAlreadyAcked
	}

	if job.Status == models.PENDING {
		return core.ErrorLeaseExceeded
	}

	if job.Status == models.CLAIMED && *job.ClaimedBy != consumer {
		return core.ErrorWrongConsumer
	}

	result, err := tx.ExecContext(ctx, completeJob, models.CLAIMED, time.Now().UnixMilli(), job.ID)
	if err != nil {
		return err
	}

	if affected, _ := result.RowsAffected(); affected == 0 {
		return core.ErrorGeneral
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *Engine) ReQueue(ctx context.Context, topic string, lastTs int64) (int, error) {
	result, err := s.ExecContext(ctx, reQueue, models.PENDING, nil, nil, time.Now().UnixMilli(), topic, models.CLAIMED, lastTs)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}
