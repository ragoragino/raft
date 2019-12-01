package persister

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	logrus "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"sync"
)

const (
	// First DB Index - must be always empty
	firstLevelDBLogIndex = 0

	// We will use lastLogDbKey to track which log entry is currently at the end.
	// In case our node crashes, we just have to get the index inserted in
	// lastLogDbKey and find the highest index present in the DB higher or equal to that.
	// TODO: Maybe save it in a separate DB???
	lastLogDbKey = firstLevelDBLogIndex
)

var (
	ErrIndexedLogDoesNotExists = errors.New("Log with given index does not exist.")

	ErrDatabaseEmpty = errors.New("Database is empty")
)

type CommandLog struct {
	Entry
	Index uint64 `json:"Index"`
}

type Entry struct {
	Term    uint64 `json:"Term"`
	Command []byte `json:"Command"`
}

type ILogEntryPersister interface {
	// TODO: AppendLogs could maybe take []*CommandLog
	AppendLogs(logs []*Entry) error
	GetLastLog() *CommandLog
	FindLogByIndex(index uint64) (*CommandLog, error)
	DeleteLogsAferIndex(index uint64) error
	Replay(doneChannel <-chan struct{}, writeChannel chan<- CommandLog) error
	Close()
}

// LevelDBLogEntryPersister is safe for concurrent usage
type LevelDBLogEntryPersister struct {
	// leveldb.DB is safe for concurrent usage
	db                 *leveldb.DB
	logger             *logrus.Entry
	lastCommandLogLock sync.RWMutex
	lastCommandLog     *CommandLog
}

func NewLevelDBLogEntryPersister(logger *logrus.Entry, filePath string) *LevelDBLogEntryPersister {
	db, err := leveldb.OpenFile(filePath, nil)
	if leveldb_errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(filePath, nil)
		if err != nil {
			logger.Panicf("LevelDB could not recover file at %s: %+v", filePath, err)
		}
	} else if err != nil {
		logger.Panicf("LevelDB could not open file at %s: %+v", filePath, err)
	}

	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, lastLogDbKey)
	value, err := db.Get(buffer, nil)
	if err == leveldb_errors.ErrNotFound {
		return &LevelDBLogEntryPersister{
			db:             db,
			logger:         logger,
			lastCommandLog: nil,
		}
	} else if err != nil {
		logger.Panicf("LevelDB getting last index failed: %+v", err)
	}

	lastCommandLogMarshalled, err := db.Get(value, nil)
	if err != nil {
		logger.Panicf("LevelDB getting last index value %+v failed: %+v", value, err)
	}

	lastCommandLog := CommandLog{}
	err = json.Unmarshal(lastCommandLogMarshalled, &lastCommandLog)
	if err != nil {
		logger.Panicf("unmarshalling of log %+v failed: %+v", lastCommandLogMarshalled, err)
	}

	return &LevelDBLogEntryPersister{
		db:             db,
		logger:         logger,
		lastCommandLog: &lastCommandLog,
	}
}

func (l *LevelDBLogEntryPersister) Close() {
	l.db.Close()
}

func (l *LevelDBLogEntryPersister) GetLastLog() *CommandLog {
	l.lastCommandLogLock.RLock()
	defer l.lastCommandLogLock.RUnlock()

	if l.lastCommandLog == nil {
		return nil
	}

	// Make a copy, so that client cannot change lastCommandLog member
	log := *l.lastCommandLog

	return &log
}

func (l *LevelDBLogEntryPersister) FindLogByIndex(index uint64) (*CommandLog, error) {
	if index <= firstLevelDBLogIndex {
		return nil, ErrIndexedLogDoesNotExists
	}

	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, index)
	commandLogMarshalled, err := l.db.Get(buffer, nil)
	if err == leveldb_errors.ErrNotFound {
		return nil, ErrIndexedLogDoesNotExists
	} else if err != nil {
		return nil, err
	}

	commandLogUnmarshalled := CommandLog{}
	err = json.Unmarshal(commandLogMarshalled, &commandLogUnmarshalled)
	if err != nil {
		return nil, err
	}

	return &commandLogUnmarshalled, nil
}

func (l *LevelDBLogEntryPersister) AppendLogs(logs []*Entry) error {
	var lastIndex uint64 = firstLevelDBLogIndex

	l.lastCommandLogLock.Lock()
	defer l.lastCommandLogLock.Unlock()

	if l.lastCommandLog != nil {
		lastIndex = l.lastCommandLog.Index
	}

	batch := &leveldb.Batch{}

	indexBuffer := make([]byte, 8)
	for i, log := range logs {
		lastIndex++

		commandLog := CommandLog{
			Index: lastIndex,
			Entry: *log,
		}

		commandLogMarshalled, err := json.Marshal(&commandLog)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(indexBuffer, lastIndex)
		batch.Put(indexBuffer, commandLogMarshalled)

		if i == (len(logs) - 1) {
			lastLogDbKeyBuffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(lastLogDbKeyBuffer, lastLogDbKey)
			batch.Put(lastLogDbKeyBuffer, indexBuffer)
		}
	}

	err := l.db.Write(batch, nil)
	if err != nil {
		return err
	}

	lastLog := logs[len(logs)-1]
	l.lastCommandLog = &CommandLog{
		Index: lastIndex,
		Entry: *lastLog,
	}

	return nil
}

func (l *LevelDBLogEntryPersister) DeleteLogsAferIndex(index uint64) error {
	l.lastCommandLogLock.Lock()
	defer l.lastCommandLogLock.Unlock()

	if l.lastCommandLog == nil {
		return ErrDatabaseEmpty
	}

	lastIndex := l.lastCommandLog.Index

	if index > lastIndex || index <= firstLevelDBLogIndex {
		return ErrIndexedLogDoesNotExists
	}

	var lastCommandLog *CommandLog
	if index == firstLevelDBLogIndex+1 {
		lastCommandLog = nil
	} else {
		lastCommandLogFound, err := l.FindLogByIndex(index - 1)
		if err != nil {
			return err
		}

		lastCommandLog = &CommandLog{
			Index: lastCommandLogFound.Index,
			Entry: lastCommandLogFound.Entry,
		}
	}

	batch := &leveldb.Batch{}

	indexBuffer := make([]byte, 8)
	for i := index; i <= lastIndex; i++ {
		binary.LittleEndian.PutUint64(indexBuffer, i)
		batch.Delete(indexBuffer)
	}

	lastLogDbKeyBuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastLogDbKeyBuffer, lastLogDbKey)

	// Check if we did not delete all the documents
	// If so, delete also the lastLogDbKey key
	if lastCommandLog == nil {
		batch.Delete(lastLogDbKeyBuffer)
	} else {
		binary.LittleEndian.PutUint64(indexBuffer, lastCommandLog.Index)
		batch.Put(lastLogDbKeyBuffer, indexBuffer)
	}

	err := l.db.Write(batch, nil)
	if err != nil {
		return err
	}

	l.lastCommandLog = lastCommandLog

	return nil
}

func (l *LevelDBLogEntryPersister) Replay(doneChannel <-chan struct{}, writeChannel chan<- CommandLog) error {
	l.lastCommandLogLock.RLock()
	defer l.lastCommandLogLock.RUnlock()

	var firstIndex uint64 = firstLevelDBLogIndex + uint64(1)
	var lastIndex uint64 = firstLevelDBLogIndex + uint64(1)
	if l.lastCommandLog == nil {
		return ErrIndexedLogDoesNotExists
	}

	lastIndex = l.lastCommandLog.Index

	firstIndexBuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstIndexBuffer, firstIndex)

	// End must be adjusted by one due to open range iteration, e.g. [0, 10)
	lastIndexBuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastIndexBuffer, lastIndex+1)

	iter := l.db.NewIterator(&util.Range{Start: firstIndexBuffer, Limit: lastIndexBuffer}, nil)
processLoop:
	for {
		select {
		case <-doneChannel:
			iter.Release()
			return nil
		default:
			if !iter.Next() {
				break processLoop
			}

			value := iter.Value()
			commandLogUnmarshalled := CommandLog{}
			err := json.Unmarshal(value, &commandLogUnmarshalled)
			if err != nil {
				iter.Release()
				return err
			}

			writeChannel <- commandLogUnmarshalled
		}
	}

	iter.Release()
	return iter.Error()
}
