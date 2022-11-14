package models

type Status int

const (
	PENDING   Status = iota // Represents the job is just inserted in to the database
	CLAIMED                 // The job has been claimed by the consumer
	COMPLETED               // The job has been marked completed by the consumer
)

type Job struct {
	ID       int64  `db:"id"`
	Topic    string `db:"topic"`
	Payload  []byte `db:"payload"`
	Priority int32  `db:"priority"`
	Status   Status `db:"status"`

	ClaimedAt int64   `db:"claimed_at"`
	ClaimedBy *string `db:"claimed_by"`

	CompletedAt int64 `db:"completed_at"`

	CreatedAt int64 `db:"created_at"`
	UpdatedAt int64 `db:"updated_at"`
}
