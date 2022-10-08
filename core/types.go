package core

type EnqueueRequest struct {
	Topic    string
	Priority int32
	Payload  []byte
}

type EnqueueResponse struct {
	JobId int64
}

type DequeueRequest struct {
}

type DequeueResponse struct {
}
