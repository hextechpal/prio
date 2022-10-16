package mysql

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/internal"
	"github.com/hextechpal/prio/internal/config"
	"github.com/hextechpal/prio/internal/models"
	"github.com/jmoiron/sqlx"
)

var (
	log = &commons.PLogger{}
)

type Storage struct {
	*sqlx.DB
	c  *config.Config
	qm *QueryManager
}

func NewStorage(c *config.Config, l *commons.PLogger) (*Storage, error) {
	log = l
	db, err := sqlx.Connect(c.Driver, c.DSN)
	if err != nil {
		l.Error().Err(err)
		return nil, err
	}
	s := &Storage{db, c, NewQueryManager(c.Driver)}
	return s, nil
}

func (s *Storage) CreateTopic(ctx context.Context, name, description string) (int64, error) {
	r, err := s.ExecContext(ctx, s.qm.addTopic(), name, description, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	log.Info().Msgf("topic %s registered successfully", name)
	return r.LastInsertId()
}

func (s *Storage) Enqueue(ctx context.Context, job *models.Job) (int64, error) {
	r, err := s.ExecContext(ctx, s.qm.addJob(), job.Topic, job.Payload, job.Priority, job.Status, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	return r.LastInsertId()
}

func (s *Storage) Dequeue(ctx context.Context, topic string, consumer string) (*models.Job, error) {
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
	err := s.GetContext(ctx, &job, s.qm.topJob(), topic, models.PENDING)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, internal.ErrorJobNotPresent
		}
		return nil, err
	}

	result, err := tx.ExecContext(ctx, s.qm.claimJob(), models.CLAIMED, time.Now().UnixMilli(), consumer, job.ID)
	if err != nil {
		return nil, err
	}

	if affected, err := result.RowsAffected(); err != nil || affected == 0 {
		return nil, internal.ErrorJobNotAcquired
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Storage) Ack(ctx context.Context, topic string, id int64, consumer string) error {
	tx := s.MustBeginTx(ctx, &sql.TxOptions{})
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Error().Err(err)
		}
	}()

	var job models.Job
	err := s.GetContext(ctx, &job, s.qm.jobById(), id, topic)
	if err != nil {
		if err == sql.ErrNoRows {
			return internal.ErrorJobNotPresent
		}
		return err
	}

	if job.Status == models.COMPLETED {
		return internal.ErrorAlreadyAcked
	}

	if job.Status == models.PENDING {
		return internal.ErrorLeaseExceeded
	}

	if job.Status == models.CLAIMED && *job.ClaimedBy != consumer {
		return internal.ErrorWrongConsumer
	}

	result, err := tx.ExecContext(ctx, s.qm.completeJob(), models.CLAIMED, time.Now().UnixMilli(), job.ID)
	if err != nil {
		return err
	}

	if affected, _ := result.RowsAffected(); affected == 0 {
		return internal.ErrorGeneral
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}
