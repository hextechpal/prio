package models

import "database/sql"

type Topic struct {
	Name        string         `db:"name"`
	Description sql.NullString `db:"description"`
	CreatedAt   int64          `db:"created_at"`
	UpdatedAt   int64          `db:"updated_at"`
}
