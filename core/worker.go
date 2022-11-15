package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hextechpal/prio/core/election"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/core/commons"
)

const (
	nsPath         = "/%s"
	electionRoot   = "/%s/election"
	membershipRoot = "/%s/members"
	memberNode     = "/%s/members/%s"
	partitionNode  = "/%s/partition"
)

type (
	Worker struct {
		mu sync.RWMutex

		Namespace string    // Namespace: identifies a group of connected prio instances
		ID        string    // ID: unique ID of the prio worker instance
		done      chan bool // done : channel to signal the all the go routines to stop as worker is shutting down

		Engine // Engine underneath storage implementation

		zkServers []string      // zkServers: slice of zookeeper servers to connect to
		timeout   time.Duration // timeout: zookeeper connection timeout
		conn      *zk.Conn      // conn zookeeper connection
		ldrDoneCh chan any      // ldrDoneCh inform the leader to stop watch on membership node
		role      election.Role // role: role assumed by the worker

		logger commons.Logger // logger
	}

	Option = func(w *Worker)

	membershipData map[string]map[string]bool
)

var (
	ErrorNoAssignedTopics = errors.New("no assigned topics")
)

func WithTimeout(timeout time.Duration) Option {
	return func(w *Worker) {
		w.timeout = timeout
	}
}

func WithID(ID string) Option {
	return func(w *Worker) {
		w.ID = ID
	}
}

func WithNamespace(namespace string) Option {
	return func(w *Worker) {
		w.Namespace = namespace
	}
}

func WithLogger(logger commons.Logger) Option {
	return func(w *Worker) {
		w.logger = logger
	}
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(servers []string, engine Engine, opts ...Option) *Worker {
	rand.Seed(time.Now().UnixMilli())
	w := &Worker{
		Namespace: fmt.Sprintf("ns_%d", rand.Intn(10000)),
		ID:        commons.GenerateUuid(),
		zkServers: servers,
		timeout:   5 * time.Second,
		Engine:    engine,
		role:      election.FOLLOWER,
		done:      make(chan bool),
		logger:    &commons.DefaultLogger{},
	}

	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Start : Registers the instance with a new zookeeper
// TODO: change this to use AuthACL and validate prio instances
func (w *Worker) Start(ctx context.Context) error {
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

	go w.work(ctx, elector, zkCh)
	return nil
}

// work : Forever running go routine which is responsible for listening to membership changes.
// It also performs clean up for tasks in case the acknowledgement is not
func (w *Worker) work(ctx context.Context, elector *election.Elector, _ <-chan zk.Event) {
	go elector.Elect(w.ID)
	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("work: received done signal, resigning")
			elector.Resign()
			return
		case <-t.C:
			err := w.maintain()
			if err != nil {
				if err == ErrorNoAssignedTopics {
					continue
				}
				w.logger.Error(err, "ticker maintenance error")
			}
		case status := <-elector.Status():
			if status.Err != nil {
				w.logger.Error(status.Err, "elector status error")
				continue
			}

			if status.Role == election.LEADER {
				w.logger.Info("work: elected as leader, running leader setup")
				err := w.leaderSetup()
				if err != nil {
					w.logger.Error(err, "leader setup error")
				}
			}

			if status.Role == election.FOLLOWER {
				w.logger.Info("work: got message to be a follower, running follower setup")
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
		w.logger.Error(err, "failed to create Namespace key")
		return err
	}
	return nil
}

func (w *Worker) leaderSetup() error {
	children, _, membersCh, err := w.conn.ChildrenW(fmt.Sprintf(membershipRoot, w.Namespace))
	if err != nil {
		return err
	}

	w.logger.Info("re-balancing children")
	err = w.reBalanceChildren(children)
	if err != nil {
		w.logger.Error(err, "error re-balancing")
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

func (w *Worker) maintain() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	data, _, err := w.conn.Get(fmt.Sprintf(partitionNode, w.Namespace))
	if err != nil {
		return ErrorNoAssignedTopics
	}

	var mdata membershipData
	err = json.Unmarshal(data, &mdata)
	if err != nil {
		return err
	}

	if topicMap, ok := mdata[w.ID]; ok {
		w.logger.Info("starting maintenance topics=%v", topicMap)
		for topic := range topicMap {
			go func(tName string) {
				lastTime := time.Now().Add(-10 * time.Second)
				count, err := w.ReQueue(context.Background(), tName, lastTime.UnixMilli())
				if err != nil {
					w.logger.Error(err, "failed for requeue jobs for topic %s", tName)
					return
				}
				w.logger.Info("re-queued jobs=%d, topic=%s", count, tName)
			}(topic)
		}
	}

	return nil
}

func (w *Worker) watchMembers(ch <-chan zk.Event) {
	w.logger.Info("watchMembers: setting up member watch")
	for {
		select {
		case event := <-ch:
			w.logger.Info("watchMembers: event received=%v", event)
			if event.Type == zk.EventNodeChildrenChanged {
				err := w.reBalance()
				if err != nil {
					w.logger.Error(err, "watchMembers: re-balance error")
				}

				err = w.reWatchMembers()
				if err != nil {
					w.logger.Error(err, "watchMembers: reWatchMembers error")
				}
				return
			}
		case <-w.ldrDoneCh:
			w.logger.Info("watchMembers: stopping watch on the members")
			return
		}
	}
}

func (w *Worker) reWatchMembers() error {
	w.logger.Info("reWatchMembers: setting up member watch")
	_, _, membersCh, err := w.conn.ChildrenW(fmt.Sprintf(membershipRoot, w.Namespace))
	if err != nil {
		return err
	}
	go w.watchMembers(membersCh)
	return err
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

	w.logger.Info("balancing %d topics among %d workers", len(topics), len(children))
	data, err := json.Marshal(calculatePartition(topics, children))
	if err != nil {
		return err
	}

	w.logger.Info("partition: %v", string(data))
	_, err = w.conn.Set(fmt.Sprintf(partitionNode, w.Namespace), data, -1)
	if err != nil {
		return err
	}

	return nil
}

func calculatePartition(topics, children []string) membershipData {
	partition := membershipData(make(map[string]map[string]bool))
	if len(topics) > 0 && len(children) > 0 {
		tpw := int(math.Round(float64(len(topics)) / float64(len(children))))
		i := 0
		for ; i < len(children)-1; i++ {
			partition[children[i]] = topicsToMap(topics[i*tpw : (i+1)*tpw])
		}
		partition[children[i]] = topicsToMap(topics[i*tpw:])
	}

	return partition
}

func topicsToMap(topics []string) map[string]bool {
	assignments := make(map[string]bool)
	for _, topic := range topics {
		assignments[topic] = true
	}
	return assignments
}
