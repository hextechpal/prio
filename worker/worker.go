package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core"
)

const (
	nsPath         = "/prio/%s"
	membershipPath = "/prio/%s/membership"
	nodesPath      = "/prio/%s/nodes"
	workerPath     = "/prio/%s/nodes/%s_"
)

var (
	l *commons.PLogger = nil
)

type Worker struct {
	mu sync.Mutex

	namespace string // namespace identifies a group of connected prio instances
	id        string // id unique id of the prio instance

	zk     *zk.Conn // zk zookeeper connection of listening to changes
	leader bool     // leader : Denotes if this particular prio instance is leader or not
	topics []string // topics owned by this particular prio instances

	*prio

	done chan bool
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(namespace string, s core.Storage, zk *zk.Conn, logger *commons.PLogger) (*Worker, error) {
	l = logger
	w := &Worker{
		namespace: namespace,
		id:        commons.GenerateUuid(),
		zk:        zk,
		leader:    false,
		topics:    make([]string, 0),
		prio:      &prio{s: s},
		done:      make(chan bool),
	}

	znode, err := w.register()
	if err != nil {
		return nil, err
	}
	l.Info().Msgf("znode created %s", znode)

	err = w.startTopicWatcher()
	if err != nil {
		w.done <- true
		return nil, err
	}

	err = w.startNodeWatcher()
	if err != nil {
		w.done <- true
		return nil, err
	}

	go w.startMaintenance()
	if err != nil {
		w.done <- true
		return nil, err
	}

	return w, nil
}

// register : Registers the instance with a new zookeeper
// TODO: change this to use AuthACL and validate prio instances
func (w *Worker) register() (string, error) {
	if exists, _, _ := w.zk.Exists(w.nsKey()); !exists {
		_, err := w.zk.Create(w.nsKey(), nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", err
		}
	}

	if exists, _, _ := w.zk.Exists(w.membershipKey()); !exists {
		_, err := w.zk.Create(w.membershipKey(), nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", err
		}
	}

	if exists, _, _ := w.zk.Exists(w.nodeKey()); !exists {
		_, err := w.zk.Create(w.nodeKey(), nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", err
		}
	}
	return w.zk.Create(w.workerKey(), []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
}

func (w *Worker) startTopicWatcher() error {
	exists, _, ch, err := w.zk.ExistsW(w.membershipKey())
	if err != nil || !exists {
		return err
	}
	go w.watchTopics(ch)
	return nil
}

func (w *Worker) watchTopics(ch <-chan zk.Event) {
	for {
		select {
		case <-w.done:
			// TODO: Check if i need to remove the znode
			return
		case e := <-ch:
			l.Info().Msgf("event received from zookeeper %v", e)
			if e.Type == zk.EventNodeDataChanged {
				data, _, err := w.zk.Get(e.Path)
				if err != nil {
					l.Error().Err(err)
					continue
				}
				var mi membershipInfo
				_ = json.Unmarshal(data, &mi)
				w.mu.Lock()
				w.topics = mi.getTopics(w.id)
				w.mu.Unlock()
			}
		}
	}
}

func (w *Worker) startMaintenance() {
	t := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-w.done:
			return
		case <-t.C:
			w.mu.Lock()
			w.performMaintenance()
			w.mu.Unlock()
		}
	}
}

func (w *Worker) performMaintenance() {
	l.Info().Msgf("Starting to perform maintenance %v", w.topics)
}

func (w *Worker) startNodeWatcher() error {
	exists, _, ch, err := w.zk.ExistsW(w.nodeKey())
	if err != nil {
		return err
	}

	if !exists {
		return errors.New("node key do not exist")
	}

	go w.watchNodes(ch)
	return nil
}

func (w *Worker) watchNodes(ch <-chan zk.Event) {
	for {
		select {
		case <-w.done:
			return
		case e := <-ch:
			if e.Type == zk.EventNodeDeleted || e.Type == zk.EventNodeCreated {
				w.mu.Lock()
				if !w.leader {
					w.mu.Unlock()
					continue
				}
				err := w.partition(e.Path, e.Type)
				if err != nil {
					l.Error().Err(err).Msgf("Cannot repartition ")
				}
				w.mu.Unlock()
			}

		}
	}
}

func (w *Worker) partition(path string, eventType zk.EventType) error {
	mi, stat, err := w.getMembershipData()
	if err != nil {
		return err
	}
	if eventType == zk.EventNodeCreated {
		mi.addMember(path)
	} else {
		mi.removeMember(path)
	}
	data, _ := json.Marshal(mi)
	_, err = w.zk.Set(w.membershipKey(), data, stat.Version)
	return err
}

func (w *Worker) getMembershipData() (*membershipInfo, *zk.Stat, error) {
	data, stat, err := w.zk.Get(w.membershipKey())
	if err != nil {
		return nil, nil, err
	}
	// Map of workerId: {topicName: true}, The key is a map to simulate a set for O(1) lookups
	var minfo membershipInfo
	err = json.Unmarshal(data, &minfo)
	if err != nil {
		return nil, nil, err
	}
	return &minfo, stat, nil
}

func (w *Worker) GetNamespace() string {
	return w.namespace
}

func (w *Worker) nodeKey() string {
	return fmt.Sprintf(nodesPath, w.namespace)
}

func (w *Worker) nsKey() string {
	return fmt.Sprintf(nsPath, w.namespace)
}

func (w *Worker) workerKey() string {
	return fmt.Sprintf(workerPath, w.namespace, w.id)
}

func (w *Worker) membershipKey() string {
	return fmt.Sprintf(membershipPath, w.namespace)
}
