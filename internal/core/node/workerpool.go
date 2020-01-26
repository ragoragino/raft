package node

import (
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"
)

const (
	// max number of worker goroutines
	hardLimitMaxGoroutines = 1024

	// time after which the goroutines stop being active
	maxInactivityDuration = 5 * time.Second
)

type IWorkerPool interface {
	AddWork(func())
	Close()
}

type workerPoolOptions struct {
	maxGoroutines      uint32
	inactivityDuration time.Duration
}

var (
	defaultWorkerPoolOptions = workerPoolOptions{
		maxGoroutines:      256,
		inactivityDuration: maxInactivityDuration,
	}
)

type WorkerPoolCallOption func(opt *workerPoolOptions)

func WithMaxGoroutines(maxGoroutines uint32) WorkerPoolCallOption {
	return func(opt *workerPoolOptions) {
		opt.maxGoroutines = maxGoroutines
	}
}

func applyWorkerPoolOptions(opts []WorkerPoolCallOption) *workerPoolOptions {
	options := defaultWorkerPoolOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &options
}

type DynamicWorkerPool struct {
	settings            *workerPoolOptions
	currentWorkers      uint32
	workChannel         chan func()
	logger              *logrus.Entry
	done                chan struct{}
	waitGroup           *sync.WaitGroup
	currentWorkersMutex sync.Mutex
}

func NewWorkerPool(logger *logrus.Entry, opts ...WorkerPoolCallOption) *DynamicWorkerPool {
	options := applyWorkerPoolOptions(opts)

	if options.maxGoroutines > hardLimitMaxGoroutines {
		options.maxGoroutines = hardLimitMaxGoroutines
	}

	workerPool := &DynamicWorkerPool{
		workChannel: make(chan func(), options.maxGoroutines),
		logger:      logger,
		settings:    options,
		done:        make(chan struct{}),
		waitGroup:   &sync.WaitGroup{},
	}

	return workerPool
}

func (wp *DynamicWorkerPool) AddWork(newWork func()) {
	wp.currentWorkersMutex.Lock()

	if wp.currentWorkers < wp.settings.maxGoroutines {
		wp.currentWorkers++
		wp.currentWorkersMutex.Unlock()

		wp.waitGroup.Add(1)
		go func() {
			defer wp.waitGroup.Done()

			for {
				select {
				case work := <-wp.workChannel:
					work()
				case <-wp.done:
					return
				case <-time.After(wp.settings.inactivityDuration):
					wp.currentWorkersMutex.Lock()
					wp.currentWorkers--
					wp.currentWorkersMutex.Unlock()
					return
				}
			}
		}()
	}

	wp.currentWorkersMutex.Unlock()

	wp.workChannel <- newWork
}

func (wp *DynamicWorkerPool) Close() {
	close(wp.done)

	wp.waitGroup.Wait()

	close(wp.workChannel)
}
