package node

import (
	"encoding/json"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
	"sync"

	logrus "github.com/sirupsen/logrus"
)

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
	defer sm.stateMutex.RUnlock()
	value, ok := sm.state[key]
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
	for log := range writeChannel {
		clientLogMarshalled := pb.AppendEntriesRequest_Entry{}
		err := json.Unmarshal(log.Command, &clientLogMarshalled)
		if err != nil {
			return err
		}

		sm.state[clientLogMarshalled.Key] = clientLogMarshalled.GetPayload()
	}

	return nil
}
