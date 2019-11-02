package persister

import (
	"encoding/json"
	logrus "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"sync"
)

var (
	stateDbKey = "state"
)

type State struct {
	CurrentTerm uint64  `json:"CurrentTerm"`
	VotedFor    *string `json:"VotedFor"`
}

type IStateLogger interface {
	UpdateState(state *State) error
	GetState() *State
	Close()
}

// LevelDBStateLogger is safe for concurrent usage,
// although we currently don't use it from multiple goroutines
// because AppendEntries RPC are handled sequentially
type LevelDBStateLogger struct {
	// leveldb.DB is safe for concurrent usage
	db         *leveldb.DB
	state      *State
	logger     *logrus.Entry
	stateMutex sync.RWMutex
}

func NewLevelDBStateLogger(logger *logrus.Entry, filePath string) *LevelDBStateLogger {
	db, err := leveldb.OpenFile(filePath, nil)
	if leveldb_errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(filePath, nil)
		if err != nil {
			logger.Panicf("LevelDB could not recover file at %s: %+v", filePath, err)
		}
	} else if err != nil {
		logger.Panicf("LevelDB could not open file at %s: %+v", filePath, err)
	}

	var state *State
	data, err := db.Get([]byte(stateDbKey), nil)
	if err != nil && err != leveldb_errors.ErrNotFound {
		logger.Panicf("LevelDB getting state key failed: %+v", err)
	} else if err == nil {
		state = &State{}
		err := json.Unmarshal(data, state)
		if err != nil {
			logger.Panicf("LevelDB unmarshalling state failed from data %+v: %+v", data, err)
		}
	}

	return &LevelDBStateLogger{
		db:     db,
		state:  state,
		logger: logger,
	}
}

func (l *LevelDBStateLogger) UpdateState(state *State) error {
	marshalledState, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Update state on a persistent disk
	err = l.db.Put([]byte(stateDbKey), marshalledState, nil)
	if err != nil {
		return err
	}

	// Update state in-memory
	l.stateMutex.Lock()
	l.state = state
	l.stateMutex.Unlock()

	return nil
}

func (l *LevelDBStateLogger) GetState() *State {
	l.stateMutex.RLock()
	defer l.stateMutex.RUnlock()

	if l.state == nil {
		return nil 
	}

	// Make a copy so that client won't modify the member
	state := *l.state

	return &state
}

func (l *LevelDBStateLogger) Close() {
	l.db.Close()
}
