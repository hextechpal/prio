package worker

import (
	"context"
	"encoding/json"
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
	nsPath         = "/prio/%s"
	electionRoot   = "/prio/%s/election"
	membershipRoot = "/prio/%s/members"
	memberNode     = "/prio/%s/members/%s"
	partitionNode  = "/prio/%s/partition"
)

type Worker struct {
	mu sync.RWMutex

	Namespace string    // Namespace: identifies a group of connected prio instances
	ID        string    // ID: unique ID of the prio worker instance
	done      chan bool // done : channel to signal the all the go routines to stop as worker is shutting down

	store.Storage // Storage underneath storage implementation

	zkServers []string      // zkServers: slice of zookeeper servers to connect to
	timeout   time.Duration // timeout: zookeeper connection timeout
	conn      *zk.Conn      // conn zookeeper connection
	ldrDoneCh chan any      // ldrDoneCh inform the leader to stop watch on membership node
	role      election.Role // role: role assumed by the worker

	logger *zerolog.Logger // logger

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

	w.conn = conn
	err = w.ensureZNodes()
	if err != nil {
		return err
	}

	_, err = w.conn.Create(fmt.Sprintf(memberNode, w.Namespace, w.ID), []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	elector, err := election.NewElector(conn, fmt.Sprintf(electionRoot, w.Namespace), w.logger)
	if err != nil {
		return err
	}

	go w.work(elector, zkCh)
	return nil
}

// work : Forever running go routine which is responsible for listening to membership changes.
// It also performs clean up for tasks in case the acknowledgement is not
func (w *Worker) work(elector *election.Elector, _ <-chan zk.Event) {
	go elector.Elect(w.ID)
	t := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-w.done:
			w.logger.Info().Msgf("work: received done signal, resigning")
			elector.Resign()
			return
		case <-t.C:
			w.maintain()
		case status := <-elector.Status():
			if status.Err != nil {
				w.logger.Error().Err(status.Err)
				continue
			}

			if status.Role == election.LEADER {
				w.logger.Info().Msgf("work: elected as leader, running leader setup")
				err := w.leaderSetup()
				if err != nil {
					w.logger.Error().Err(err)
				}
			}

			if status.Role == election.FOLLOWER {
				w.logger.Info().Msgf("work: got message to be a follower, running follower setup")
				w.followerSetup()
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

func (w *Worker) ensureZNodes() error {
	err := w.ensureZnodePath(fmt.Sprintf(nsPath, w.Namespace))
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(fmt.Sprintf(electionRoot, w.Namespace))
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(fmt.Sprintf(membershipRoot, w.Namespace))
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(fmt.Sprintf(partitionNode, w.Namespace))
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) ensureZnodePath(path string) error {
	_, err := w.conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		w.logger.Error().Err(err).Msg("failed to create Namespace key")
		return err
	}
	return nil
}

func (w *Worker) leaderSetup() error {
	children, _, membersCh, err := w.conn.ChildrenW(fmt.Sprintf(membershipRoot, w.Namespace))
	if err != nil {
		return err
	}

	w.logger.Info().Msgf("re-balancing children")
	err = w.reBalanceChildren(children)
	if err != nil {
		w.logger.Error().Err(err).Msgf("error re-balancing")
		return err
	}

	go w.watchMembers(membersCh)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.role = election.LEADER
	return err

}

func (w *Worker) followerSetup() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.role == election.LEADER {
		w.role = election.FOLLOWER
		w.ldrDoneCh <- nil
	}
}

func (w *Worker) maintain() {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.role != election.LEADER {
		return
	}

}

func (w *Worker) watchMembers(ch <-chan zk.Event) {
	w.logger.Info().Msgf("setting up member watch")
	for {
		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged {
				err := w.reBalance()
				if err != nil {
					return
				}
			}
		case <-w.ldrDoneCh:
			w.logger.Info().Msgf("watchMembers: stopping watch on the members")
			return
		}
	}
}

func (w *Worker) reBalance() error {
	children, _, err := w.conn.Children(fmt.Sprintf(membershipRoot, w.Namespace))
	if err != nil {
		return err
	}
	return w.reBalanceChildren(children)
}

func (w *Worker) reBalanceChildren(children []string) error {
	topics, err := w.GetTopics(context.Background())
	if err != nil {
		return err
	}

	w.logger.Info().Msgf("balancing %d topics among %d workers", len(topics), len(children))
	partition := make(map[string]map[string]bool)
	if len(topics) > 0 && len(children) > 0 {
		tpw := len(topics) / len(children)
		i := 0
		for ; i < len(children)-1; i++ {
			partition[children[i]] = topicsToMap(topics[i*tpw : (i+1)*tpw])
		}
		partition[children[i]] = topicsToMap(topics[i*tpw:])
	}

	w.logger.Info().Msgf("partition: %v", partition)
	data, err := json.Marshal(partition)
	if err != nil {
		return err
	}

	_, err = w.conn.Set(fmt.Sprintf(partitionNode, w.Namespace), data, -1)
	if err != nil {
		return err
	}

	return nil
}

func topicsToMap(topics []string) map[string]bool {
	assignments := make(map[string]bool)
	for _, topic := range topics {
		assignments[topic] = true
	}
	return assignments
}
