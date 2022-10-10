package models

type Status int

const (
	PENDING   Status = iota // Represents the job is just inserted in to the database
	DELIVERED               // The job has been added to the prefetch buffer
	ACKED                   // The job has been acked by the consumer and processed
)

type Job struct {
	ID        int64  `db:"id"`
	Topic     string `db:"topic"`
	Payload   []byte `db:"payload"`
	Priority  int32  `db:"priority"`
	CreatedAt int64  `db:"created_at"`
	UpdatedAt int64  `db:"updated_at"`
	Status    Status `db:"status"`
}
