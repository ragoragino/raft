package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	timer := NewTimer(1 * time.Second)
	start := time.Now().Add(1 * time.Second)
	<-timer.After()
	assert.True(t, time.Now().After(start))

	start = time.Now().Add(2 * time.Second)
	timer.Reset(2 * time.Second)
	<-timer.After()
	assert.True(t, time.Now().After(start))

	timer.Close()
}
