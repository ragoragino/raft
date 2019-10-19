package persister

import (
	"os"
	"sync"
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
	GetLastLog() *Entry
	FindLog()
	DeleteLogsAfer(log *Entry)
}

type FileEntryLogger struct {
	storage     *os.File
	storageLock sync.Mutex
}

func NewFileEntryLogger(filePath string) *FileEntryLogger {
	return &FileEntryLogger{}
}
