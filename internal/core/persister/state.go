package persister

import (
	"encoding/json"
	logrus "github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
	"sync"
)

type State struct {
	CurrentTerm int64     `json:"CurrentTerm"`
	VotedFor    *string `json:"VotedFor"`
}

type StateLogger interface {
	UpdateState(state *State)
	GetState() *State
	Close()
}

type FileStateLogger struct {
	file       *os.File
	state      *State
	logger     *logrus.Entry
	stateMutex sync.RWMutex
}

func NewFileStateLogger(logger *logrus.Entry, path string) *FileStateLogger {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logger.Panicf("%+v", err)
	}

	var state *State

	fileStat, err := os.Stat(path)
	if fileStat.Size() != 0 {
		// In case the file is not empty, find the last JSON string delimited by \n
		var readStart int64
		var expectedLineSize int64 = 100
		var unmarshalledState State

		for {
			if fileStat.Size() < expectedLineSize {
				readStart = 0
			} else {
				readStart = fileStat.Size() - expectedLineSize
			}

			buffer := make([]byte, expectedLineSize)

			// Read the batch of file and only panic if the error is not EOF
			n, err := file.ReadAt(buffer, readStart)
			if err != nil && err != io.EOF {
				logger.Panicf("%+v", err)
			}

			// Try to split the read buffer and panic if there is no element
			// and we have already read the whole file
			jsonSlice := strings.Split(string(buffer[:n]), "\n")
			if len(jsonSlice) == 0 && readStart == 0 {
				logger.Panicf("no valid state in the file at: %s", path)
			}

			// We have found the json and we try to unmarshal it to State struct
			// strings.Split always adds an empty element if the splitting string comes at the end,
			// therefore we do not take the last element of the slice, but one before that
			err = json.Unmarshal([]byte(jsonSlice[len(jsonSlice)-2]), &unmarshalledState)
			if err == nil {
				state = &unmarshalledState
				break
			}

			// If unmarshalling failed, we try to read a larger buffer from the file
			expectedLineSize *= 2
		}
	}

	return &FileStateLogger{
		file:   file,
		state:  state,
		logger: logger,
	}
}

func (f *FileStateLogger) UpdateState(state *State) error {
	marshalledState, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Delimit the buffer with '\n'
	marshalledDelimitedState := make([]byte, len(marshalledState)+1)
	copy(marshalledDelimitedState, marshalledState)
	marshalledDelimitedState[len(marshalledState)] = '\n'

	// Update state on a persistent disk
	_, err = f.file.Write(marshalledDelimitedState)
	if err != nil {
		return err
	}

	// Update state in-memory
	f.stateMutex.Lock()
	f.state = state
	f.stateMutex.Unlock()

	return nil
}

func (f *FileStateLogger) GetState() *State {
	f.stateMutex.RLock()
	defer f.stateMutex.RUnlock()
	return f.state
}

func (f *FileStateLogger) Close() {
	f.file.Close()
}
