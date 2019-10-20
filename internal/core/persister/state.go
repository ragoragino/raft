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
	CurrentTerm int64   `json:"CurrentTerm"`
	VotedFor    *string `json:"VotedFor"`
}

type IStateLogger interface {
	UpdateState(state *State) error
	GetState() *State
	Close()
}

type LevelDBStateLogger struct {
	db         *leveldb.DB
	state      *State
	logger     *logrus.Entry
	stateMutex sync.RWMutex
}

func NewLevelDBStateLogger(logger *logrus.Entry, path string) *LevelDBStateLogger {
	db, err := leveldb.OpenFile(path, nil)
	if leveldb_errors.IsCorrupted(err) {
		var err error
		db, err = leveldb.RecoverFile(path, nil)
		if err != nil {
			logger.Panicf("LevelDB could not recover file: %+v", err)
		}
	} else if err != nil {
		logger.Panicf("LevelDB could not open file: %+v", err)
	}

	var state *State
	data, err := db.Get([]byte(stateDbKey), nil)
	if err != nil && err != leveldb_errors.ErrNotFound {
		logger.Panicf("LevelDB getting state failed: %+v", err)
	} else if err == nil {
		state = &State{}
		err := json.Unmarshal(data, state)
		if err != nil {
			logger.Panicf("LevelDB getting state failed: %+v", err)
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
	return l.state
}

func (l *LevelDBStateLogger) Close() {
	l.db.Close()
}
