package persister

import (
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestFileStateLogger(t *testing.T) {
	logger := logrus.StandardLogger()

	fileStatePath := `./state.txt`
	defer func() {
		f, err := ioutil.ReadFile(fileStatePath)
		assert.NoError(t, err)

		logger.Printf(string(f))

		err = os.Remove(fileStatePath)
		assert.NoError(t, err)
	}()

	loggerEntry := logger.WithFields(logrus.Fields{
		"name": "FileStateLogger",
	})

	fileStateLogger := NewFileStateLogger(loggerEntry, fileStatePath)

	state := fileStateLogger.GetState()
	assert.Nil(t, state)

	votedFor := "1"
	fileStateLogger.UpdateState(&State{
		VotedFor:    &votedFor,
		CurrentTerm: int64(1),
	})

	state = fileStateLogger.GetState()
	assert.Equal(t, *state.VotedFor, "1")
	assert.Equal(t, state.CurrentTerm, int64(1))

	votedFor = "2"
	fileStateLogger.UpdateState(&State{
		VotedFor:    &votedFor,
		CurrentTerm: int64(2),
	})

	fileStateLogger.Close()

	fileStateLogger = NewFileStateLogger(loggerEntry, fileStatePath)
	state = fileStateLogger.GetState()
	assert.Equal(t, *state.VotedFor, "2")
	assert.Equal(t, state.CurrentTerm, int64(2))

	fileStateLogger.Close()
}
