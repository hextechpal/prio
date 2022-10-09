package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/hextechpal/prio/core/models"
	"github.com/jmoiron/sqlx"
)

const (
	migrationPath = "../migrations"
	addJob        = `INSERT INTO jobs(payload, priority, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`
)

type Storage struct {
	*sqlx.DB
	c *Config
}

func NewStorage(c *Config) (*Storage, error) {
	db, err := sqlx.Connect("mysql", assembleDSN(c))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	s := &Storage{db, c}
	err = s.migrateDB()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) Save(ctx context.Context, job *models.Job) (int64, error) {
	stmt, err := s.Preparex(addJob)
	if err != nil {
		return -1, err
	}
	r, err := stmt.Exec(job.Payload, job.Priority, job.Status, time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return -1, err
	}
	return r.LastInsertId()
}

func assembleDSN(c *Config) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DBName)
}

func (s *Storage) migrateDB() error {
	driver, err := mysql.WithInstance(s.DB.DB, &mysql.Config{})
	if err != nil {
		return err
	}
	m, err := migrate.NewWithDatabaseInstance(migrationPath, s.c.DBName, driver)
	if err != nil {
		return err
	}
	return m.Up()
}
