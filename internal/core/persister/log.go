package persister

import (
	"errors"
	"os"
	"sync"
	logrus "github.com/sirupsen/logrus"
)

var (
	ErrIndexedLogDoesNotExit = errors.New("Log with given index does not exist.")
)

type commandLog struct {
	Entry
	Index uint64 `json:"Index"`
}

type Entry struct {
	Term    uint64 `json:"Term"`
	Command []byte `json:"Command"`
}

type EntryLogger interface {
	AddLogs(logs []*Entry) uint64
	GetLastLog() Entry
	FindLogByIndex() (*Entry, error)
	DeleteLogsAferIndex(index uint64)
	Close()
}

// FileEntryLogger will be just a simple file database (similar to /etc/passwd, and others)
// with in-memory indexes
// TODO: Optimize
type FileEntryLogger struct {
	file           *os.File
	fileLock       sync.Mutex
	logger         *logrus.Entry
	lastCommandLog *commandLog
	currentOffset  uint64
}

func NewFileEntryLogger(logger *logrus.Entry, filePath string) *FileEntryLogger {
	return &FileEntryLogger{}
}
