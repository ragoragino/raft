package persister

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	logrus "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"sync"
)

const (
	// We will use lastLogDbKey to track which log entry is currently at the end.
	// In case our node crashes, we just have to get the index inserted in
	// lastLogDbKey and find the highest index present in the DB higher or equal to that.
	lastLogDbKey         = "last_log"

	// First DB Index - will be always empty
	firstLevelDBLogIndex = 0
)

var (
	ErrIndexedLogDoesNotExit = errors.New("Log with given index does not exist.")
)

type CommandLog struct {
	Entry
	Index uint64 `json:"Index"`
}

type Entry struct {
	Term    uint64 `json:"Term"`
	Command []byte `json:"Command"`
}

type EntryLogger interface {
	AddLogs(logs []*Entry) error
	GetLastLog() *CommandLog
	FindLogByIndex(index uint64) (*CommandLog, error)
	DeleteLogsAferIndex(index uint64)
	Close()
}

// LevelDBEntryLogger is safe for concurrent usage,
// although we currently don't use it from multiple goroutines
// because AppendEntries RPC are handled sequentially
type LevelDBEntryLogger struct {
	// leveldb.DB is safe for concurrent usage
	db                 *leveldb.DB
	logger             *logrus.Entry
	lastCommandLogLock sync.RWMutex
	lastCommandLog     *CommandLog
}

func NewLevelDBEntryLogger(logger *logrus.Entry, filePath string) *LevelDBEntryLogger {
	db, err := leveldb.OpenFile(filePath, nil)
	if leveldb_errors.IsCorrupted(err) {
		var err error
		db, err = leveldb.RecoverFile(filePath, nil)
		if err != nil {
			logger.Panicf("LevelDB could not recover file: %+v", err)
		}
	} else if err != nil {
		logger.Panicf("LevelDB could not open file: %+v", err)
	}

	value, err := db.Get([]byte(lastLogDbKey), nil)
	if err == leveldb_errors.ErrNotFound {
		return &LevelDBEntryLogger{
			db:             db,
			logger:         logger,
			lastCommandLog: nil,
		}
	} else if err != nil {
		logger.Panicf("LevelDB getting state failed: %+v", err)
	}

	lastKnownIndex := binary.LittleEndian.Uint64(value)

	for {
		_, err := db.Get([]byte(value), nil)
		if err == leveldb_errors.ErrNotFound {
			lastKnownIndex--
			break
		} else if err != nil {
			logger.Panicf("LevelDB getting index %+v failed: %+v", value, err)
		}

		lastKnownIndex++
		binary.LittleEndian.PutUint64(value, lastKnownIndex)
	}

	binary.LittleEndian.PutUint64(value, lastKnownIndex)
	lastCommandLogMarshalled, err := db.Get([]byte(value), nil)
	if err != nil {
		logger.Panicf("LevelDB getting index %+v failed: %+v", value, err)
	}

	lastCommandLog := CommandLog{}
	err = json.Unmarshal(lastCommandLogMarshalled, &lastCommandLog)
	if err != nil {
		logger.Panicf("unmarshalling of log %+v failed: %+v", lastCommandLogMarshalled, err)
	}

	return &LevelDBEntryLogger{
		db:             db,
		logger:         logger,
		lastCommandLog: &lastCommandLog,
	}
}

func (l *LevelDBEntryLogger) Close() {
	l.db.Close()
}

func (l *LevelDBEntryLogger) GetLastLog() *CommandLog {
	l.lastCommandLogLock.RLock()
	defer l.lastCommandLogLock.RUnlock()

	if l.lastCommandLog == nil {
		return nil
	}

	// Make a copy, so that client cannot change lastCommandLog member
	log := *l.lastCommandLog

	return &log
}

func (l *LevelDBEntryLogger) FindLogByIndex(index uint64) (*CommandLog, error) {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, index)
	commandLogMarshalled, err := l.db.Get([]byte(buffer), nil)
	if err != nil {
		return nil, err
	}

	commandLogUnmarshalled := CommandLog{}
	err = json.Unmarshal(commandLogMarshalled, &commandLogUnmarshalled)
	if err != nil {
		return nil, err
	}

	return &commandLogUnmarshalled, nil
}

func (l *LevelDBEntryLogger) AddLogs(logs []*Entry) error {
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
			batch.Put([]byte(lastLogDbKey), indexBuffer)
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

func (l *LevelDBEntryLogger) DeleteLogsAferIndex(index uint64) error {
	l.lastCommandLogLock.Lock()
 	defer l.lastCommandLogLock.Unlock()

	if l.lastCommandLog == nil {
		return ErrIndexedLogDoesNotExit
	}

	lastIndex := l.lastCommandLog.Index

	if index > lastIndex || index <= firstLevelDBLogIndex {
		return ErrIndexedLogDoesNotExit
	}

	var lastCommandLog *CommandLog
	if index == firstLevelDBLogIndex+1 {
		lastCommandLog = nil
	} else {
		lastCommandEntry, err := l.FindLogByIndex(index - 1)
		if err != nil {
			return ErrIndexedLogDoesNotExit
		}

		lastCommandLog = &CommandLog{
			Index: lastCommandEntry.Index,
			Entry: lastCommandEntry.Entry,
		}
	}

	batch := &leveldb.Batch{}

	indexBuffer := make([]byte, 8)
	for i := index; i <= lastIndex; i++ {
		binary.LittleEndian.PutUint64(indexBuffer, i)
		batch.Delete(indexBuffer)
	}

	binary.LittleEndian.PutUint64(indexBuffer, lastCommandLog.Index)
	batch.Put([]byte(lastLogDbKey), indexBuffer)

	err := l.db.Write(batch, nil)
	if err != nil {
		return err
	}

	l.lastCommandLog = lastCommandLog

	return nil
}
