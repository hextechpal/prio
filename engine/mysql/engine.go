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

type Engine struct {
	*sqlx.DB
	driver string
	dsn    string
	qm     *QueryManager
	logger *zerolog.Logger
}

func NewEngine(driver string, dsn string, logger *zerolog.Logger) (*Engine, error) {
	db, err := sqlx.Connect(driver, dsn)
	if err != nil {
		logger.Error().Err(err)
		return nil, err
	}

	s := &Engine{
		DB:     db,
		driver: driver,
		dsn:    dsn,
		qm:     NewQueryManager(driver),
		logger: logger,
	}
	return s, nil
}

func (s *Engine) GetTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := s.SelectContext(ctx, &topics, s.qm.allTopics())
	if err != nil {
		return []string{}, err
	}
	return topics, nil
}

func (s *Engine) CreateTopic(ctx context.Context, name, description string) (int64, error) {
	r, err := s.ExecContext(ctx, s.qm.addTopic(), name, description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	s.logger.Info().Msgf("topic %s registered successfully", name)
	return r.LastInsertId()
}

func (s *Engine) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	r, err := s.ExecContext(ctx, s.qm.addJob(), job.Topic, job.Payload, job.Priority, job.Status, time.Now().UnixMilli(), time.Now().UnixMilli())
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
	err := s.GetContext(ctx, &job, s.qm.topJob(), topic, models.PENDING)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, core.ErrorJobNotPresent
		}
		return nil, err
	}

	result, err := tx.ExecContext(ctx, s.qm.claimJob(), models.CLAIMED, time.Now().UnixMilli(), consumer, job.ID)
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
	err := s.GetContext(ctx, &job, s.qm.jobById(), id, topic)
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

	result, err := tx.ExecContext(ctx, s.qm.completeJob(), models.CLAIMED, time.Now().UnixMilli(), job.ID)
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
	result, err := s.ExecContext(ctx, s.qm.reQueue(), models.PENDING, nil, nil, time.Now().UnixMilli(), topic, models.CLAIMED, lastTs)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}
