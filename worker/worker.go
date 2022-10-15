package worker

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/core"
)

const (
	nodesPath = "/prio/nodes"
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

	prio *prio

	done chan bool
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(s core.Storage, zk *zk.Conn, logger *commons.PLogger) (*Worker, error) {
	l = logger
	uuid := commons.GenerateUuid()
	znode, err := register(uuid, zk)
	if err != nil {
		return nil, err
	}
	l.Info().Msgf("znode created %s", znode)

	w := &Worker{
		prio:   &prio{s: s},
		zk:     zk,
		leader: false,
		topics: make([]string, 0),
	}

	err = w.startWatcher(znode)
	if err != nil {
		w.done <- true
		return nil, err
	}

	err = w.startMaintenance()
	if err != nil {
		w.done <- true
		return nil, err
	}

	return w, nil
}

// register : Registers the instance with a new zookeeper
func register(uuid string, conn *zk.Conn) (string, error) {
	key := fmt.Sprintf("%s/%s_", nodesPath, uuid)
	// TODO: change this to use AuthACL and validate prio instances
	return conn.Create(key, []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
}

func (w *Worker) startWatcher(znode string) error {
	_, _, ch, err := w.zk.GetW(znode)
	if err != nil {
		return err
	}
	go w.watchNodes(ch)
	return nil
}

func (w *Worker) watchNodes(ch <-chan zk.Event) {
	for {
		select {
		case <-w.done:
			// TODO: Check if i need to remove the znode
			return
		case e := <-ch:
			if e.Type == zk.EventNodeDataChanged {
				data, _, err := w.zk.Get(e.Path)
				if err != nil {
					l.Error().Err(err)
					continue
				}
				var topics []string
				_ = json.Unmarshal(data, &topics)
				w.mu.Lock()
				w.topics = topics
				w.mu.Unlock()
			}
		}
	}
}

func (w *Worker) startMaintenance() error {
	t := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-w.done:
			return nil
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

func (w *Worker) GetNamespace() string {
	return w.namespace
}
