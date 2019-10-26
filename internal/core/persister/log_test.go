package persister

import (
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileEntryLoggerWithoutRestarts(t *testing.T) {
	logger := logrus.StandardLogger()

	fileEntryPath := `./db_log_test_TestFileEntryLogger`
	os.RemoveAll(fileEntryPath)

	defer func() {
		err := os.RemoveAll(fileEntryPath)
		assert.NoError(t, err)
	}()

	loggerEntry := logger.WithFields(logrus.Fields{
		"name": "FileEntryLogger",
	})

	fileEntryLogger := NewLevelDBEntryLogger(loggerEntry, fileEntryPath)
	defer fileEntryLogger.Close()

	entries := []*Entry{
		&Entry{Term: uint64(1), Command: []byte("1")},
		&Entry{Term: uint64(2), Command: []byte("2")},
		&Entry{Term: uint64(3), Command: []byte("3")},
		&Entry{Term: uint64(4), Command: []byte("4")},
		&Entry{Term: uint64(5), Command: []byte("5")},
	}

	t.Run("Add&Get", func(t *testing.T) {
		err := fileEntryLogger.AddLogs(entries)
		assert.NoError(t, err)

		lastActualEntry := entries[len(entries)-1]
		lastReceivedLog := fileEntryLogger.GetLastLog()
		assert.NotNil(t, lastReceivedLog)
		assert.Equal(t, &lastReceivedLog.Entry, lastActualEntry)
		assert.Equal(t, lastReceivedLog.Index, uint64(len(entries)))
	})

	t.Run("Find", func(t *testing.T){
		indexOfLogToFind := 2
		actualEntry := entries[indexOfLogToFind]
		foundLog, err := fileEntryLogger.FindLogByIndex(uint64(indexOfLogToFind+1))
		assert.NoError(t, err)
		assert.NotNil(t, foundLog)
		assert.Equal(t, &foundLog.Entry, actualEntry)
		assert.Equal(t, foundLog.Index, uint64(indexOfLogToFind+1))
	})

	t.Run("Delete&Get", func(t *testing.T) {
		var indexToDeleteLogsAfter uint64 = 3
		err := fileEntryLogger.DeleteLogsAferIndex(indexToDeleteLogsAfter)
		assert.NoError(t, err)

		lastEntryIndex := indexToDeleteLogsAfter-2
		lastActualEntry := entries[lastEntryIndex]
		lastReceivedLog := fileEntryLogger.GetLastLog()
		assert.NotNil(t, lastReceivedLog)
		assert.Equal(t, &lastReceivedLog.Entry, lastActualEntry)
		assert.Equal(t, lastReceivedLog.Index, uint64(lastEntryIndex+1))
	})
}

func TestFileEntryLoggerWithRestarts(t *testing.T) {
	logger := logrus.StandardLogger()

	fileEntryPath := `./db_log_test_TestFileEntryLogger`
	os.RemoveAll(fileEntryPath)

	defer func() {
		err := os.RemoveAll(fileEntryPath)
		assert.NoError(t, err)
	}()

	loggerEntry := logger.WithFields(logrus.Fields{
		"name": "FileEntryLogger",
	})

	fileEntryLogger := NewLevelDBEntryLogger(loggerEntry, fileEntryPath)

	entries := []*Entry{
		&Entry{Term: uint64(1), Command: []byte("1")},
		&Entry{Term: uint64(2), Command: []byte("2")},
		&Entry{Term: uint64(3), Command: []byte("3")},
		&Entry{Term: uint64(4), Command: []byte("4")},
		&Entry{Term: uint64(5), Command: []byte("5")},
	}

	t.Run("Add", func(t *testing.T) {
		err := fileEntryLogger.AddLogs(entries)
		assert.NoError(t, err)
	})

	fileEntryLogger.Close()
	fileEntryLogger = NewLevelDBEntryLogger(loggerEntry, fileEntryPath)

	t.Run("GetAfterAdd", func(t *testing.T) {
		lastActualEntry := entries[len(entries)-1]
		lastReceivedLog := fileEntryLogger.GetLastLog()
		assert.NotNil(t, lastReceivedLog)
		assert.Equal(t, &lastReceivedLog.Entry, lastActualEntry)
		assert.Equal(t, lastReceivedLog.Index, uint64(len(entries)))
	})

	t.Run("Find", func(t *testing.T){
		indexOfLogToFind := 2
		actualEntry := entries[indexOfLogToFind]
		foundLog, err := fileEntryLogger.FindLogByIndex(uint64(indexOfLogToFind+1))
		assert.NoError(t, err)
		assert.NotNil(t, foundLog)
		assert.Equal(t, &foundLog.Entry, actualEntry)
		assert.Equal(t, foundLog.Index, uint64(indexOfLogToFind+1))
	})

	var indexToDeleteLogsAfter uint64 = 3
	t.Run("Delete", func(t *testing.T) {
		err := fileEntryLogger.DeleteLogsAferIndex(indexToDeleteLogsAfter)
		assert.NoError(t, err)
	})

	fileEntryLogger.Close()
	fileEntryLogger = NewLevelDBEntryLogger(loggerEntry, fileEntryPath)

	t.Run("GetAfterDelete", func(t *testing.T) {
		lastEntryIndex := indexToDeleteLogsAfter-2
		lastActualEntry := entries[lastEntryIndex]
		lastReceivedLog := fileEntryLogger.GetLastLog()
		assert.NotNil(t, lastReceivedLog)
		assert.Equal(t, &lastReceivedLog.Entry, lastActualEntry)
		assert.Equal(t, lastReceivedLog.Index, uint64(lastEntryIndex+1))
	})

	fileEntryLogger.Close()
}
