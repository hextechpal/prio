package election

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/core/commons"
	"sort"
	"strings"
	"sync"
)

type (
	Role int

	Status struct {
		CandidateId string
		Role        Role
		Err         error
		Following   string
	}

	Elector struct {
		znode string
		root  string
		conn  *zk.Conn

		// Status channel for communicating with the clients
		statusCh chan Status

		// Zookeeper channels
		myCh          <-chan zk.Event // myCh channel watches the events for the znode created for the instance
		predecessorCh <-chan zk.Event // predecessorCh channel watched for predecessor delete events and trigger a check if deleted

		// Internal channels for communication
		triggerElectionCh chan error // triggerElectionCh watches for trigger election events
		resignCh          chan any   // resignCh informs routine of the resignation and cleanup resource
		stopCh            chan any   // stopCh stop all the watch routines

		once   sync.Once
		logger commons.Logger
	}
)

const (
	LEADER Role = iota
	FOLLOWER

	sep = "/"
)

func (s Status) String() string {
	return fmt.Sprintf("canditateId=%s, role=%d, following=%s, err=%s", s.CandidateId, s.Role, s.Following, s.Err)
}

func NewElector(conn *zk.Conn, electionRoot string, logger commons.Logger) (*Elector, error) {
	if conn == nil {
		return nil, errors.New("conn cannot be nil")
	}

	exists, _, err := conn.Exists(electionRoot)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("election root %s should be present", electionRoot)
	}

	return &Elector{
		root:     electionRoot,
		conn:     conn,
		statusCh: make(chan Status, 1),

		triggerElectionCh: make(chan error),
		resignCh:          make(chan any),
		stopCh:            make(chan any),

		logger: logger,
	}, nil
}

func (e *Elector) Elect(candidateId string) {
	status := e.initiateElection(candidateId)
	e.logger.Info("%v", status)
	e.statusCh <- status
	for {
		select {
		case err := <-e.triggerElectionCh:
			if err != nil {
				status.Err = err
			} else {
				e.logger.Info("finding leader again status=%v", status)
				e.findLeader(&status)
			}

			select {
			case e.statusCh <- status:
				if err != nil {
					e.resign()
					return
				}
			case <-e.resignCh:
				e.resign()
				return
			}

		case <-e.resignCh:
			e.resign()
			return
		}
	}
}

func (e *Elector) Status() chan Status {
	return e.statusCh
}

func (e *Elector) Resign() {
	close(e.resignCh)
}

func (e *Elector) initiateElection(candidateId string) Status {
	// populate default status object
	status := Status{
		CandidateId: candidateId,
		Role:        FOLLOWER,
	}
	e.logger.Info("%v\n", "nominating candidate")
	znode, myCh, err := e.nominate(candidateId)
	if err != nil {
		status.Err = err
		return status
	}
	// Save znode into the elector
	// TODO : check if this correct place to store this
	e.logger.Info("candidate nominated, znode=%s, storing=%s", znode, znode[len(e.root)+1:])

	// Populate myCh here
	e.znode = znode[len(e.root)+1:]
	e.myCh = myCh

	// Check if I am leader
	// If I am the leader then update the status and publish the status on the status channel
	// If I am not the leader find out who to follow and set up a watch on that
	// TODO check if election is over

	e.logger.Info("starting to find leader")
	e.findLeader(&status)
	if status.Err != nil {
		status.Err = fmt.Errorf("%s Unexpected error attempting to determine leader. Error (%v)",
			"runElection:", status.Err)
		return status
	}
	return status
}

func (e *Elector) findLeader(status *Status) {
	leader, toFollow, err := e.amILeader()
	if err != nil {
		status.Err = err
		return
	}

	if leader {
		e.logger.Info("selected as leader, updating status")
		status.Role = LEADER
		status.Following = ""
		go e.watchLeader()
		return
	}

	e.logger.Info("setting up follower watch for znode=%s", toFollow)
	followCh, err := e.setPredecessorWatch(toFollow)
	if err != nil {
		status.Err = err
		return
	}
	e.predecessorCh = followCh
	status.Following = toFollow
	go e.watchPredecessor()
}

func (e *Elector) nominate(candidateId string) (string, <-chan zk.Event, error) {
	flags := int32(zk.FlagEphemeral | zk.FlagSequence)
	acl := zk.WorldACL(zk.PermAll)
	znode, err := e.conn.Create(strings.Join([]string{e.root, "le_"}, sep), []byte(candidateId), flags, acl)
	if err != nil {
		return "", nil, err
	}

	exists, _, myCh, err := e.conn.ExistsW(znode)
	if err != nil {
		return "", nil, err
	}

	if !exists {
		return "", nil, errors.New("cannot fnd newly created node")
	}
	return znode, myCh, nil
}

func (e *Elector) amILeader() (bool, string, error) {
	children, err := e.getChildren()
	if err != nil {
		return false, "", err
	}
	sort.Slice(children, func(i, j int) bool {
		return children[i] < children[j]
	})

	if e.znode == children[0] {
		return true, "", nil
	}

	// TODO handle error here what if your node is not found, Can it happen?
	i := 1
	for ; i < len(children); i++ {
		if children[i] == e.znode {
			break
		}
	}

	if i == len(children) {
		return false, "", errors.New("cannot find the newly created node")
	}
	return false, children[i-1], nil
}

func (e *Elector) getChildren() ([]string, error) {
	children, _, err := e.conn.Children(e.root)
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (e *Elector) setPredecessorWatch(follow string) (<-chan zk.Event, error) {
	exists, _, followCh, err := e.conn.ExistsW(strings.Join([]string{e.root, follow}, sep))
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("cannot fnd newly created node")
	}
	return followCh, nil
}

func (e *Elector) watchLeader() {
	for {
		select {
		case event := <-e.myCh:
			e.logger.Info("predecessor node deleted sending notification to triggerElectionCh")
			if event.Type == zk.EventNodeDeleted {
				err := fmt.Errorf("%s Leader (%v) has been deleted", "watchForLeaderDeleteEvents", e.znode)
				e.triggerElectionCh <- err
				return
			}
			return
		case <-e.stopCh:
			return
		}
	}
}

func (e *Elector) watchPredecessor() {
	for {
		select {
		case event := <-e.predecessorCh:
			if event.Type == zk.EventNodeDeleted {
				e.logger.Info("predecessor node deleted sending notification to triggerElectionCh")
				e.triggerElectionCh <- nil
				return
			}
			return
		case event := <-e.myCh:
			if event.Type == zk.EventNodeDeleted {
				err := fmt.Errorf("%s Leader (%v) has been deleted", "watchForLeaderDeleteEvents", e.znode)
				e.triggerElectionCh <- err
				return
			}
		case <-e.stopCh:
			return
		}
	}
}

func (e *Elector) resign() {
	close(e.stopCh)
	err := e.conn.Delete(strings.Join([]string{e.root, e.znode}, sep), -1)
	if err != nil {
		e.logger.Error(err, "error deleting znode")
	}
	e.once.Do(func() {
		close(e.statusCh)
	})
	return
}
