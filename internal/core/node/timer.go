package node

import (
	"sync"
	"time"
)

// I implemented this customer timer, because time.Timer
// seemed pretty shaky according to the Github issues,
// so I tailored one to my specific use case

type ITimer interface {
	After() <-chan struct{}
	Reset(d time.Duration)
	Close()
}

func NewTimer(d time.Duration) *Timer {
	timer := &Timer{
		done:           make(chan struct{}),
		wg:             sync.WaitGroup{},
		duration:       d,
		expiredChannel: make(chan struct{}),
		resetChannel:   make(chan time.Duration),
	}

	timer.wg.Add(1)

	go func() {
		defer timer.wg.Done()

		for {
			// Generally, when someone calls Reset and the timeout expires at the same time,
			// we do not care which one was first in reality. We take the one
			// that was picked by the go scheduler.
		startLoop:
			select {
			case <-timer.done:
				return
			case d = <-timer.resetChannel:
				break startLoop
			case <-time.After(d):
				select {
				case <-timer.done:
					return
				case timer.expiredChannel <- struct{}{}:
					select {
					case <-timer.done:
						return
					case d = <-timer.resetChannel:
						break startLoop
					}
				case d = <-timer.resetChannel:
					break startLoop
				}
			}
		}
	}()

	return timer
}

type Timer struct {
	done           chan struct{}
	wg             sync.WaitGroup
	duration       time.Duration
	expiredChannel chan struct{}
	resetChannel   chan time.Duration
}

func (t *Timer) After() <-chan struct{} {
	return t.expiredChannel
}

func (t *Timer) Reset(d time.Duration) {
	t.resetChannel <- d
}

func (t *Timer) Close() {
	close(t.done)
	t.wg.Wait()
	close(t.expiredChannel)
	close(t.resetChannel)
}
