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
	"github.com/hextechpal/prio/core"
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

var (
	l *commons.PLogger = nil
)

type Worker struct {
	mu sync.RWMutex

	namespace string // namespace identifies a group of connected prio instances
	id        string // id unique id of the prio instance

	zk      *zk.Conn // zk zookeeper connection of listening to changes
	state   state    // leader : Denotes if this particular prio instance is leader or not
	topics  []string // topics owned by this particular prio instances
	znode   string   // znode represent the registered path in zookeeper
	version int      // znode represent the registered path in zookeeper

	*prio

	done chan bool
}

// NewWorker : Initializes a new prio instance registers it with zookeeper
// It also starts all the watchers and background workers
func NewWorker(namespace string, s core.Storage, zk *zk.Conn) (*Worker, error) {
	w := &Worker{
		namespace: namespace,
		id:        commons.GenerateUuid(),
		zk:        zk,
		state:     FOLLOWER,
		topics:    make([]string, 0),
		prio:      &prio{s: s},
		done:      make(chan bool),
	}

	initLogger(namespace, w.id)

	//err := w.startNodeWatcher()
	//if err != nil {
	//	w.done <- true
	//	return nil, err
	//}

	//err := w.startMembershipWatcher()
	//if err != nil {
	//	w.done <- true
	//	return nil, err
	//}

	znode, err := w.register()
	if err != nil {
		w.done <- true
		return nil, err
	}
	w.znode = znode
	l.Info().Msgf("znode created %s", znode)
	go w.nominate()
	go w.startMaintenance()
	return w, nil
}

func initLogger(namespace string, id string) {
	ctx := context.WithValue(context.Background(), "wid", id)
	ctx = context.WithValue(ctx, "ns", namespace)
	l = commons.NewLogger(ctx)
}

// register : Registers the instance with a new zookeeper
// TODO: change this to use AuthACL and validate prio instances
func (w *Worker) register() (string, error) {
	_, err := w.zk.Create(w.nsKey(), []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		l.Error().Err(err).Msg("failed to create namespace key")
		return "", err
	}

	_, err = w.zk.Create(w.nodeKey(), []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		l.Error().Err(err).Msg("failed to create node key")
		return "", err
	}

	path, err := w.zk.Create(w.workerKey(), []byte(w.id), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		l.Error().Err(err).Msgf("registration failed")
		return "", err
	}
	l.Info().Msgf("registration successful znode=%v", path[len(w.nodeKey())+1:])
	return path[len(w.nodeKey())+1:], nil
}

func (w *Worker) startMembershipWatcher() error {
	exists, _, ch, err := w.zk.ExistsW(w.membershipKey())
	if exists {
		go w.watchMembership(ch)
		return nil
	}

	if !exists || err == zk.ErrNoNode {
		_, err = w.zk.Create(w.membershipKey(), nil, 0, zk.WorldACL(zk.PermAll))
		if err == nil || err == zk.ErrNodeExists {
			_, _, ch, err = w.zk.GetW(w.membershipKey())
			if err != nil {
				return err
			}
			go w.watchMembership(ch)
			return nil
		}
		return err
	}
	return err
}

func (w *Worker) watchMembership(ch <-chan zk.Event) {
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
	//l.Info().Msgf("Starting to perform maintenance %v", w.topics)
}

func (w *Worker) startNodeWatcher() error {
	exists, _, ch, err := w.zk.ExistsW(w.nodeKey())
	if exists {
		go w.watchNodes(ch)
		return nil
	}

	l.Info().Err(err).Msgf("node watch key do not exist")
	if !exists || err == zk.ErrNoNode {
		_, err = w.zk.Create(w.nodeKey(), nil, 0, zk.WorldACL(zk.PermAll))
		if err == nil || err == zk.ErrNodeExists {
			_, _, ch, err = w.zk.GetW(w.nodeKey())
			if err != nil {
				return err
			}
			go w.watchNodes(ch)
			return nil
		}
		return err
	}
	return err
}

func (w *Worker) watchNodes(ch <-chan zk.Event) {
	for {
		select {
		case <-w.done:
			return
		case e := <-ch:
			if e.Type == zk.EventNodeDeleted || e.Type == zk.EventNodeCreated {
				w.mu.Lock()
				//if !w.isLeader() {
				//	w.mu.Unlock()
				//	continue
				//}
				//err := w.partition(e.Path, e.Type)
				//if err != nil {
				//	l.Error().Err(err).Msgf("Cannot repartition ")
				//}
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

func (w *Worker) nominate() {
	attempts := 0
	for attempts < 3 {
		attempts++
		children, _, err := w.zk.Children(w.nodeKey())
		if err != nil {
			l.Error().Err(err)
			continue
		}

		if len(children) == 0 {
			l.Error().Msgf("no children present")
			continue
		}

		sort.SliceIsSorted(children, func(i, j int) bool {
			i1, _ := strconv.Atoi(children[i][24:])
			i2, _ := strconv.Atoi(children[j][24:])
			return i1 < i2
		})

		w.mu.Lock()

		if w.znode == children[0] {
			l.Info().Msgf("I am the smallest children. Promoting to leader")
			w.state = LEADER
			w.mu.Unlock()
			break
		}

		prev := 0
		for i := 1; i < len(children); i++ {
			if children[i] == w.znode {
				break
			}
			prev = i
		}
		_ = w.setupPredecessorWatch(children[prev])
		w.mu.Unlock()
		break
	}
}

func (w *Worker) setupPredecessorWatch(prevMember string) error {
	wpath := fmt.Sprintf("%s/%s", w.nodeKey(), prevMember)
	_, _, ch, err := w.zk.ExistsW(wpath)
	if err != nil {
		l.Error().Err(err).Msgf("cannot setup watch for predecessor")
		return err
	}
	l.Info().Msgf("predecessor for %s found. listening changes on %s", w.id, prevMember)
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
			l.Warn().Msgf("predecessor watch fired with eventType=%d", e.Type)
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
