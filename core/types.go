package core

type RegisterTopicRequest struct {
	Name        string
	Description string
}

type RegisterTopicResponse struct {
	topicID int64
}

type EnqueueRequest struct {
	Topic    string
	Priority int32
	Payload  []byte
}

type EnqueueResponse struct {
	JobId int64
}

type DequeueRequest struct {
	Topic    string
	Consumer string
}

type DequeueResponse struct {
	JobId   int64
	Topic   string
	Payload []byte
}

type AckRequest struct {
	JobId    int64
	Topic    string
	Consumer string
}

type AckResponse struct {
	acked bool
}
