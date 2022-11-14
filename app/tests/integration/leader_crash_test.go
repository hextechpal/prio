package integration

import (
	"context"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/inmemory-engine"
	"github.com/rs/zerolog"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	wcount = 3
	zkHost = "127.0.0.1"
)

var ns string
var conn *zk.Conn
var ctx context.Context
var logger zerolog.Logger

func init() {
	var err error
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

	conn, _, err = zk.Connect([]string{zkHost}, 60*time.Second)
	if err != nil {
		panic(err)
	}
}

type resp struct {
	w   *core.Worker
	err error
}

func Test_leader_election_multi(t *testing.T) {
	defer cleanup()
	t.Logf("Starting test for namespace %s", ns)
	ch := make(chan resp)

	wmap := make(map[string]*core.Worker)

	for i := 0; i < wcount; i++ {
		go setup(t, ch)
	}

	for i := 0; i < wcount; i++ {
		r := <-ch
		if r.err != nil {
			t.Fatalf("error intilizing worker err=%v", r.err)
		}
		wmap[r.w.ID] = r.w
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

func leader(workers map[string]*core.Worker) string {
	for id, w := range workers {
		if w.IsLeader() {
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
	for !w.IsLeader() && time.Since(start) < 30*time.Second {
		time.Sleep(100 * time.Millisecond)
	}

	if !w.IsLeader() {
		t.Errorf("woker is not selected as leader")
	}
}

func setup(t *testing.T, ch chan resp) {
	t.Helper()
	w := core.NewWorker(ctx, ns, []string{zkHost}, time.Second, inmemory_engine.NewStorage(), &logger)
	ch <- resp{w, w.Start()}
}

func cleanup() {
	_ = conn.Delete(fmt.Sprintf("/prio/%s", ns), -1)
}
