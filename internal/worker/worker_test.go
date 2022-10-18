package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/internal/worker/mock"
	"github.com/rs/zerolog"
)

const (
	wcount = 3
	zkHost = "127.0.0.1"
)

var ns string
var ctx context.Context
var logger zerolog.Logger

func init() {
	rand.Seed(time.Now().UnixMilli())
	ns = fmt.Sprintf("ns_%d", rand.Intn(10000))
	ctx = context.WithValue(context.Background(), "ns", ns)
	logger = zerolog.
		New(os.Stderr).
		With().
		Timestamp().
		Str("ns", ns).
		Logger().
		Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

type resp struct {
	w   *Worker
	err error
}

func Test_leader_election_multi(t *testing.T) {
	defer cleanup()
	t.Logf("Starting test for namespace %s", ns)
	ch := make(chan resp)

	wmap := make(map[string]*Worker)

	for i := 0; i < wcount; i++ {
		go setup(t, ch)
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
	for leaderId == "" && time.Since(start) < 3*time.Second {
		time.Sleep(100 * time.Millisecond)
		leaderId = leader(wmap)
	}

	if leaderId == "" {
		t.Fatalf("no worker is not selected as leader")
	}

	t.Logf("leader id found %s", leaderId)
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

func cleanup() {

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
	ch := make(chan resp)
	go setup(t, ch)
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

func setup(t *testing.T, ch chan resp) {
	t.Helper()
	conn, _, err := zk.Connect([]string{zkHost}, 60*time.Second)
	if err != nil {
		panic(err)
	}
	w, err := NewWorker(ctx, ns, conn, mock.NewMemoryStorage(), &logger)
	ch <- resp{w, err}
}
