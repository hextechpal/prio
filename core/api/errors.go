package api

import "errors"

var (
	ErrorGeneral = errors.New("something went wrong try again")

	ErrorJobNotAcquired = errors.New("job not acquired")
	ErrorJobNotPresent  = errors.New("job not present")
	ErrorAlreadyAcked   = errors.New("job already acked")
	ErrorWrongConsumer  = errors.New("job claimed by a different consumer")
	ErrorLeaseExceeded  = errors.New("lease time exceeded")
)
