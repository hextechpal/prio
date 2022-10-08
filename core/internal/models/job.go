package models

type Status int

const (
	PENDING   Status = iota // Represents the job is just inserted in to the database
	DELIVERED               // The job has been added to the prefetch buffer
	ACKED                   // The job has been acked by the consumer and processed
)

type Job struct {
	ID        int64
	Payload   []byte
	Priority  int32
	CreatedAt int64
	UpdatedAt int64
	Status    Status
}
