package node

import (
	"encoding/json"
	"fmt"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
	"sync"

	logrus "github.com/sirupsen/logrus"
)

// IStateMachine defines the interface for state machine.
// State machine should be used to gather appended log entries and
// contacted when an external client queries a particular key.
type IStateMachine interface {
	Create(key string, value []byte)
	Get(key string) ([]byte, bool)
	Delete(key string)
	LoadState(writeChannel <-chan persister.CommandLog) error
}

type StateMachine struct {
	state      map[string][]byte
	stateMutex sync.RWMutex
	logger     *logrus.Entry
}

func NewStateMachine(logger *logrus.Entry) *StateMachine {
	return &StateMachine{
		state:  make(map[string][]byte),
		logger: logger,
	}
}

func (sm *StateMachine) Create(key string, value []byte) {
	sm.stateMutex.Lock()
	sm.state[key] = value
	sm.stateMutex.Unlock()
}

func (sm *StateMachine) Get(key string) ([]byte, bool) {
	sm.stateMutex.RLock()
	value, ok := sm.state[key]
	sm.stateMutex.RUnlock()

	return value, ok
}

func (sm *StateMachine) Delete(key string) {
	sm.stateMutex.Lock()
	delete(sm.state, key)
	sm.stateMutex.Unlock()
}

func (sm *StateMachine) LoadState(writeChannel <-chan persister.CommandLog) error {
	sm.stateMutex.RLock()
	defer sm.stateMutex.RUnlock()
	for commandLog := range writeChannel {
		logEntry := pb.AppendEntriesRequest_Entry{}
		err := json.Unmarshal(commandLog.Command, &logEntry)
		if err != nil {
			return err
		}

		switch logEntry.GetType() {
		case pb.AppendEntriesRequest_CREATED:
			sm.state[logEntry.GetKey()] = logEntry.GetPayload()
		case pb.AppendEntriesRequest_DELETED:
			delete(sm.state, logEntry.GetKey())
		default:
			return fmt.Errorf("unknown log entry type: %+v", logEntry.GetType())
		}
	}

	return nil
}
