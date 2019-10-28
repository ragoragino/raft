package persister

import (
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileStateLogger(t *testing.T) {
	logger := logrus.StandardLogger()

	fileStatePath := `./db_state_test_TestFileStateLogger`
	os.RemoveAll(fileStatePath)

	defer func() {
		err := os.RemoveAll(fileStatePath)
		assert.NoError(t, err)
	}()

	loggerEntry := logger.WithFields(logrus.Fields{
		"name": "FileStateLogger",
	})

	fileStateLogger := NewLevelDBStateLogger(loggerEntry, fileStatePath)

	state := fileStateLogger.GetState()
	assert.Nil(t, state)

	votedFor := "1"
	fileStateLogger.UpdateState(&State{
		VotedFor:    &votedFor,
		CurrentTerm: uint64(1),
	})

	state = fileStateLogger.GetState()
	assert.Equal(t, *state.VotedFor, "1")
	assert.Equal(t, state.CurrentTerm, uint64(1))

	votedFor = "2"
	fileStateLogger.UpdateState(&State{
		VotedFor:    &votedFor,
		CurrentTerm: uint64(2),
	})

	fileStateLogger.Close()

	fileStateLogger = NewLevelDBStateLogger(loggerEntry, fileStatePath)
	state = fileStateLogger.GetState()
	assert.Equal(t, *state.VotedFor, "2")
	assert.Equal(t, state.CurrentTerm, uint64(2))

	fileStateLogger.Close()
}
