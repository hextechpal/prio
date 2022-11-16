package models

type Status int

const (
	PENDING Status = iota // Represents the job is just inserted in to the database
	CLAIMED               // The job has been claimed by the consumer
)

type Job struct {
	ID       int64
	Topic    string
	Payload  []byte
	Priority int32
	Status   Status

	ClaimedAt int64
	ClaimedBy string

	CreatedAt int64
	UpdatedAt int64
}
