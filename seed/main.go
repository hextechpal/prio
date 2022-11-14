package main

import (
	"context"
	"fmt"
	"github.com/hextechpal/prio/core/models"
	"github.com/hextechpal/prio/mysql-backend"
	"github.com/rs/zerolog"
	"math/rand"
)

func main() {
	//fmt.Printf("enqueued 100 jobs for cricket %v\n", enqueueJobs("cricket", 100))
	//fmt.Printf("enqueued 100 jobs for hockey %v\n", enqueueJobs("hockey", 100))

	fmt.Printf("dequeued 10 jobs for cricket %v\n", dequeJobs("cricket", 10, "example"))
	fmt.Printf("dequeued 10 jobs for cricket %v\n", dequeJobs("hockey", 10, "example"))
}

func enqueueJobs(topic string, count int) []int64 {
	jobIds := make([]int64, count)
	storage, _ := mysql_backend.NewStorage("mysql", "root:root@tcp(127.0.0.1:3306)/prio", &zerolog.Logger{})
	for i := 0; i < count; i++ {
		priority := int32(rand.Intn(100))
		res, _ := storage.Enqueue(context.Background(), &models.Job{
			Topic:    topic,
			Priority: priority,
			Payload:  []byte(fmt.Sprintf("payload_%d", i)),
		})
		fmt.Printf("jobid= %d", res)
		jobIds[i] = res
	}
	return jobIds
}

func dequeJobs(topic string, count int, consumer string) []int64 {
	jobIds := make([]int64, count)
	storage, _ := mysql_backend.NewStorage("mysql", "root:root@tcp(127.0.0.1:3306)/prio", &zerolog.Logger{})
	for i := 0; i < count; i++ {
		res, _ := storage.Dequeue(context.Background(), topic, consumer)
		jobIds[i] = res.ID
	}
	return jobIds
}
