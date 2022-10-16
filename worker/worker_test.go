package worker

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/worker/mock"
)

const (
	wcount = 3
	zkHost = "127.0.0.1"
)

var ns string

func init() {
	rand.Seed(time.Now().UnixMilli())
	ns = fmt.Sprintf("ns_%d", rand.Intn(100))
}

type resp struct {
	w   *Worker
	err error
}

func Test_leader_election_multi(t *testing.T) {
	ctx := context.TODO()
	t.Logf("Starting test for namespace %s", ns)
	ch := make(chan resp)

	wmap := make(map[string]*Worker)

	for i := 0; i < wcount; i++ {
		go setup(ctx, t, ch)
	}

	for i := 0; i < wcount; i++ {
		r := <-ch
		if r.err != nil {
			t.Fatalf("error intilizing worker err=%v", r.err)
		}
		wmap[r.w.id] = r.w
	}

	start := time.Now()
	leaderId := ""
	for leaderId == "" && time.Since(start) < 30*time.Second {
		time.Sleep(100 * time.Millisecond)
		leaderId = leader(wmap)
	}

	if leaderId == "" {
		t.Fatalf("no worker is not selected as leader")
	}

	t.Logf("leader id found %s", leaderId)
	time.Sleep(10 * time.Second)
	t.Logf("shutting down the leader %s", leaderId)
	_ = wmap[leaderId].ShutDown()
	delete(wmap, leaderId)

	start = time.Now()
	leaderId = ""
	for leaderId == "" && time.Since(start) < 3*time.Second {
		time.Sleep(100 * time.Millisecond)
		leaderId = leader(wmap)
	}

	if leaderId == "" {
		t.Fatalf("no worker is not selected as leader")
	}
	t.Logf("newnleader id found %s. shutting down the leader", leaderId)
}

func leader(workers map[string]*Worker) string {
	for id, w := range workers {
		if w.isLeader() {
			return id
		}
	}
	return ""
}

func Test_leader_election_single(t *testing.T) {
	ctx := context.TODO()
	ch := make(chan resp)
	go setup(ctx, t, ch)
	r := <-ch

	if r.err != nil {
		t.Fatalf("error initializing worker err=%v", r.err)
	}

	w := r.w
	start := time.Now()
	for !w.isLeader() && time.Since(start) < 30*time.Second {
		time.Sleep(100 * time.Millisecond)
	}

	if !w.isLeader() {
		t.Errorf("woker is not selected as leader")
	}
}

func setup(ctx context.Context, t *testing.T, ch chan resp) {
	t.Helper()
	conn, _, err := zk.Connect([]string{zkHost}, 60*time.Second)
	if err != nil {
		panic(err)
	}
	w, err := NewWorker(ns, mock.NewMemoryStorage(), conn)
	ch <- resp{w, err}
}
