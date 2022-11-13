package models

type Topic struct {
	Name        string  `db:"name"`
	Description *string `db:"description"`
	CreatedAt   int64   `db:"created_at"`
	UpdatedAt   int64   `db:"updated_at"`
}
