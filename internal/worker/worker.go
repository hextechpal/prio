package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/commons"
	"github.com/hextechpal/prio/internal/store"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type state string

const (
	LEADER   state = "leader"
	FOLLOWER state = "follower"
)
const (
	nsPath         = "/prio/%s"
	membershipPath = "/prio/%s/membership"
	nodesPath      = "/prio/%s/election"
	workerPath     = "/prio/%s/election/%s_"
)

type Worker struct {
	mu sync.RWMutex

	namespace string // namespace identifies a group of connected prio instances
	id        string // id unique id of the prio instance

	zk      *zk.Conn // zk zookeeper connection of listening to changes
	state   state    // state : Denotes the state of particular prio instance LEADER | FOLLOWER
	znode   string   // znode represent the registered path in zookeeper
	version int      // znode represent the registered path in zookeeper

	store.Storage

	done chan bool

	logger *zerolog.Logger
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(ctx context.Context, namespace string, zk *zk.Conn, s store.Storage, logger *zerolog.Logger) (*Worker, error) {
	uuid := commons.GenerateUuid()
	l := logger.With().Str("wid", uuid).Logger()
	w := &Worker{
		namespace: namespace,
		id:        uuid,
		zk:        zk,
		state:     FOLLOWER,
		Storage:   s,
		done:      make(chan bool),
		logger:    &l,
	}

	// Ensure the base znodes are present in zookeeper
	err := w.ensureZNodes()
	if err != nil {
		return nil, err
	}

	// Register this instance with zookeeper
	znode, err := w.register()
	if err != nil {
		w.done <- true
		return nil, err
	}
	w.znode = znode
	w.logger.Info().Msgf("znode created %s", znode)

	// Nominate current instance to be the leader
	// If the leader already exist this will setup a watch to the predecessor znode
	go w.nominate()

	// This instance is responsible for a subset of the topics
	go w.startMaintenance()
	go func() {
		<-ctx.Done()
		w.ShutDown()
	}()

	return w, nil
}

// register : Registers the instance with a new zookeeper
// TODO: change this to use AuthACL and validate prio instances
func (w *Worker) register() (string, error) {
	path, err := w.zk.Create(w.workerKey(), []byte(w.id), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		w.logger.Error().Err(err).Msgf("registration failed")
		return "", err
	}
	w.logger.Info().Msgf("registration successful znode=%v", path[len(w.nodeKey())+1:])
	return path[len(w.nodeKey())+1:], nil
}

func (w *Worker) startMaintenance() {
	t := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-w.done:
			return
		case <-t.C:
			//TODO : perform maintenance here sleeping meanwhile
			time.Sleep(time.Second)

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

func (w *Worker) nominate() {
	attempts := 0
	for attempts < 3 {
		attempts++
		children, _, err := w.zk.Children(w.nodeKey())
		if err != nil {
			w.logger.Error().Err(err)
			continue
		}

		if len(children) == 0 {
			w.logger.Error().Msgf("no children present")
			continue
		}

		sort.Slice(children, func(i, j int) bool {
			i1, _ := strconv.Atoi(children[i][24:])
			i2, _ := strconv.Atoi(children[j][24:])
			return i1 < i2
		})

		w.logger.Info().Msgf("Found %d children %v", len(children), children)
		w.mu.Lock()
		if w.znode == children[0] {
			w.state = LEADER
			w.mu.Unlock()
			break
		}
		w.state = FOLLOWER
		w.mu.Unlock()

		w.mu.RLock()
		prev := 0
		for i := 1; i < len(children); i++ {
			if children[i] == w.znode {
				break
			}
			prev = i
		}
		_ = w.setupPredecessorWatch(children[prev])
		w.mu.RUnlock()
		break
	}
}

func (w *Worker) setupPredecessorWatch(prevMember string) error {
	wpath := fmt.Sprintf("%s/%s", w.nodeKey(), prevMember)
	_, _, ch, err := w.zk.ExistsW(wpath)
	if err != nil {
		w.logger.Error().Err(err).Msgf("cannot setup watch for predecessor")
		return err
	}
	w.logger.Info().Msgf("predecessor for %s found. listening changes on %s", w.id, prevMember)
	go w.watchPredecessor(ch)
	return nil
}

func (w *Worker) watchPredecessor(ch <-chan zk.Event) {
	for {
		select {
		case <-w.done:
			return
		case e := <-ch:
			if e.Type == zk.EventNodeDeleted {
				go w.nominate()
				return
			}
			w.logger.Warn().Msgf("predecessor watch fired with eventType=%d", e.Type)
		}
	}
}

func (w *Worker) ShutDown() bool {
	w.done <- true
	mpath := fmt.Sprintf("%s/%s", w.nodeKey(), w.znode)
	err := w.zk.Delete(mpath, -1)
	if err != nil {
		log.Error().Err(err).Msgf("error deleting node")
		return false
	}
	return true
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

func (w *Worker) isLeader() bool {
	return w.state == LEADER
}

func (w *Worker) ensureZNodes() error {
	err := w.ensureZnodePath(w.nsKey())
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(w.nodeKey())
	if err != nil {
		return err
	}

	err = w.ensureZnodePath(w.membershipKey())
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) ensureZnodePath(path string) error {
	_, err := w.zk.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		w.logger.Error().Err(err).Msg("failed to create namespace key")
		return err
	}
	return nil
}
