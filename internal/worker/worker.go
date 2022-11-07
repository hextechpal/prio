package worker

import (
	"context"
	"fmt"
	"github.com/hextechpal/prio/internal/election"
	"github.com/hextechpal/prio/internal/store"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/commons"
	"github.com/rs/zerolog"
)

const (
	nsPath       = "/prio/%s"
	electionRoot = "/prio/%s/election"
)

type Worker struct {
	mu sync.RWMutex

	Namespace string // Namespace: identifies a group of connected prio instances
	ID        string // ID: unique ID of the prio worker instance

	zkServers []string      // zkServers: slice of zookeeper servers to connect to
	timeout   time.Duration // timeout: zookeeper connection timeout
	role      election.Role // role: role assumed by the worker

	done chan bool // done : channel to signal the all the go routines to stop as worker is shutting down

	store.Storage                 // underneath storage implementation
	logger        *zerolog.Logger // logger
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(ctx context.Context, namespace string, servers []string, timeout time.Duration, s store.Storage, logger *zerolog.Logger) *Worker {
	uuid := commons.GenerateUuid()
	l := logger.With().Str("wid", uuid).Logger()
	w := &Worker{
		Namespace: namespace,
		ID:        uuid,
		zkServers: servers,
		timeout:   timeout,
		Storage:   s,
		role:      election.FOLLOWER,
		done:      make(chan bool),
		logger:    &l,
	}

	go func() {
		<-ctx.Done()
		w.ShutDown()
	}()

	return w
}

// Start : Registers the instance with a new zookeeper
// TODO: change this to use AuthACL and validate prio instances
func (w *Worker) Start() error {
	conn, zkCh, err := zk.Connect(w.zkServers, w.timeout, zk.WithLogger(w.logger))
	if err != nil {
		return err
	}

	err = w.ensureZNodes(conn)
	if err != nil {
		return err
	}

	elector, err := election.NewElector(conn, fmt.Sprintf(electionRoot, w.Namespace), w.logger)
	if err != nil {
		return err
	}
	go w.monitor(elector, zkCh)
	return nil
}

// monitor : Forever running go routine which is responsible for listening to membership changes.
// It also performs clean up for tasks in case the acknowledgement is not
func (w *Worker) monitor(elector *election.Elector, _ <-chan zk.Event) {
	go elector.Elect(w.ID)
	t := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-w.done:
			w.logger.Info().Msgf("received done signal, resigning")
			elector.Resign()
			return
		case <-t.C:
			//TODO : perform maintenance here sleeping meanwhile
			time.Sleep(100 * time.Millisecond)
		case status := <-elector.Status():
			if status.Err != nil {
				w.logger.Error().Err(status.Err)
				continue
			}

			if status.Role == election.LEADER {
				w.electedLeader()
			}
		}
	}
}

func (w *Worker) ShutDown() bool {
	w.done <- true
	return true
}

func (w *Worker) IsLeader() bool {
	return w.role == election.LEADER
}

func (w *Worker) ensureZNodes(conn *zk.Conn) error {
	err := w.ensureZnodePath(conn, fmt.Sprintf(nsPath, w.Namespace))
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(conn, fmt.Sprintf(electionRoot, w.Namespace))
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) ensureZnodePath(conn *zk.Conn, path string) error {
	_, err := conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		w.logger.Error().Err(err).Msg("failed to create Namespace key")
		return err
	}
	return nil
}

func (w *Worker) electedLeader() {
	w.logger.Info().Msgf("done node created setting state to be a leader")
	w.mu.Lock()
	defer w.mu.Unlock()
	w.role = election.LEADER
}
