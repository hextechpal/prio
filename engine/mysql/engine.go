package mysql

import (
	"context"
	"database/sql"
	"github.com/hextechpal/prio/core/api"
	"github.com/hextechpal/prio/core/commons"
	"github.com/hextechpal/prio/engine/mysql/internal/models"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	allTopics = `SELECT topics.name from topics`
	addTopic  = `INSERT INTO topics(name, description, created_at, updated_at) VALUES (?, ?, ?, ?)`

	addJob = `INSERT INTO jobs(topic, payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`

	topJob   = `SELECT jobs.id, jobs.payload, jobs.priority from jobs where jobs.topic = ? AND jobs.status = ? ORDER BY priority DESC, updated_at LIMIT 1 FOR UPDATE`
	claimJob = `UPDATE jobs SET status = ?, claimed_at = ?, claimed_by = ?  WHERE jobs.id = ?`

	jobById     = `SELECT jobs.id, jobs.status from jobs where jobs.id = ? FOR UPDATE `
	completeJob = `UPDATE jobs SET status = ?, completed_at = ? WHERE jobs.id = ?`

	reQueue = `UPDATE jobs SET status = ?, claimed_at = ?, claimed_by = ?, updated_at = ? WHERE jobs.topic = ? AND jobs.status = ? AND jobs.claimed_at < ?`
)

type Engine struct {
	*sqlx.DB
	config Config
	logger commons.Logger
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

func (s *Engine) RegisterTopic(ctx context.Context, req api.RegisterTopicRequest) (api.RegisterTopicResponse, error) {
	_, err := s.ExecContext(ctx, addTopic, req.Name, req.Description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return api.RegisterTopicResponse{}, err
	}
	s.logger.Info("topic %s registered successfully id", req.Name)
	return api.RegisterTopicResponse{}, nil
}

func (s *Engine) Enqueue(ctx context.Context, req api.EnqueueRequest) (api.EnqueueResponse, error) {
	r, err := s.ExecContext(ctx, addJob, req.Topic, req.Payload, req.Priority, models.PENDING, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return api.EnqueueResponse{}, err
	}
	id, _ := r.LastInsertId()
	s.logger.Info("job enqueued id=%d\n", id)
	return api.EnqueueResponse{JobId: id}, err
}

func (s *Engine) Dequeue(ctx context.Context, req api.DequeueRequest) (api.DequeueResponse, error) {
	// Find Top priority item with status pending
	// Update the status to delivered and delivered_at timestamp to NOW()
	// return the updated object

	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.Error(err, "error while dequeue")
		}
	}()

	var job models.Job
	err := s.GetContext(ctx, &job, topJob, req.Topic, models.PENDING)
	if err != nil {
		if err == sql.ErrNoRows {
			return api.DequeueResponse{}, nil
		}
		return api.DequeueResponse{}, err
	}

	result, err := tx.ExecContext(ctx, claimJob, models.CLAIMED, time.Now().UnixMilli(), req.Consumer, job.ID)
	if err != nil {
		return api.DequeueResponse{}, err
	}

	if affected, err := result.RowsAffected(); err != nil || affected == 0 {
		return api.DequeueResponse{}, api.ErrorJobNotAcquired
	}

	if err := tx.Commit(); err != nil {
		return api.DequeueResponse{}, err
	}
	return api.DequeueResponse{
		JobId:    job.ID,
		Topic:    job.Topic,
		Payload:  job.Payload,
		Priority: job.Priority,
	}, nil
}

func (s *Engine) Ack(ctx context.Context, req api.AckRequest) (api.AckResponse, error) {
	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			s.logger.Error(err, "error during ack")
		}
	}()

	var job models.Job
	err := s.GetContext(ctx, &job, jobById, req.JobId)
	errRes := api.AckResponse{Acked: false}
	if err != nil {
		if err == sql.ErrNoRows {
			return errRes, api.ErrorJobNotPresent
		}
		return errRes, err
	}

	if job.Status == models.COMPLETED {
		return errRes, api.ErrorAlreadyAcked
	}

	if job.Status == models.PENDING {
		return errRes, api.ErrorLeaseExceeded
	}

	if job.Status == models.CLAIMED && job.ClaimedBy.String != req.Consumer {
		return errRes, api.ErrorWrongConsumer
	}

	result, err := tx.ExecContext(ctx, completeJob, models.CLAIMED, time.Now().UnixMilli(), job.ID)
	if err != nil {
		return errRes, err
	}

	if affected, _ := result.RowsAffected(); affected == 0 {
		return errRes, api.ErrorGeneral
	}

	if err = tx.Commit(); err != nil {
		return errRes, err
	}

	return api.AckResponse{Acked: true}, nil
}

func (s *Engine) ReQueue(ctx context.Context, req api.RequeueRequest) (api.RequeueResponse, error) {
	result, err := s.ExecContext(ctx, reQueue, models.PENDING, nil, nil, time.Now().UnixMilli(), req.Topic, models.CLAIMED, req.RequeueTs)
	if err != nil {
		return api.RequeueResponse{}, err
	}
	rows, _ := result.RowsAffected()
	return api.RequeueResponse{Count: rows}, nil
}

func (s *Engine) GetTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := s.SelectContext(ctx, &topics, allTopics)
	if err != nil {
		return []string{}, err
	}
	return topics, nil
}
