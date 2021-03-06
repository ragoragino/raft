package node

import (
	"fmt"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
	"sync"

	"github.com/golang/protobuf/proto"
	logrus "github.com/sirupsen/logrus"
)

// IStateMachine defines the interface for state machine.
// State machine should be used to gather appended log entries and
// contacted when an external client queries a particular key.
type IStateMachine interface {
	Create(key string, value []byte)
	Get(key string) ([]byte, bool)
	Delete(key string)
	LoadState(iterator persister.ILogEntryPersisterIterator) error
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

func (sm *StateMachine) LoadState(iterator persister.ILogEntryPersisterIterator) error {
	sm.stateMutex.RLock()
	defer sm.stateMutex.RUnlock()

	for iterator.Next() {
		commandLog := iterator.Value()

		logEntry := pb.AppendEntriesRequest_Entry{}
		err := proto.Unmarshal(commandLog.Command, &logEntry)
		if err != nil {
			sm.logger.Printf("value: %s", commandLog.Command)
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

	return iterator.Error()
}
