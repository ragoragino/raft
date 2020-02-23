package main

import (
	"context"
	"os"
	"os/signal"
	raft_node "raft/internal/core/node"
	"raft/internal/core/persister"
	http_server "raft/internal/core/server"
	"sync"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"

	"github.com/sirupsen/logrus"
)

var opts struct {
	// HTTP endpoint for external clients
	HTTPEndpoint string `long:"http-endpoint" description:"HTTP endpoint for external clients" default:"localhost:8000"`

	// Internal endpoint for cluster nodes
	InternalEndpoint string `long:"cluster-endpoint" description:"Internal endpoint for cluster nodes" required:"true"`

	// Name of the endpoint
	Name string `long:"name" description:"Names of the endpoint" required:"true"`

	// Names and cluster endpoints for other nodes
	OtherNamedEndpoints map[string]string `long:"cluster-endpoints" description:"Names and addresses of other endpoints of the form name:endpoint" required:"true"`

	// Names and cluster endpoints for other nodes
	OtherNamedHTTPEndpoints map[string]string `long:"http-endpoints" description:"Names and HTTP addresses of other endpoints of the form name:endpoint"`
}

func main() {
	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel,
		syscall.SIGINT,
		syscall.SIGTERM)

	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	_, err := flags.Parse(&opts)
	if err != nil {
		panic(err)
	}

	// Initialize cluster client
	raftClusterClientLogger := logger.WithFields(logrus.Fields{
		"component": "cluster",
	})
	raftClusterClient := raft_node.NewCluster(raftClusterClientLogger)

	// Initialize cluster server
	raftClusterServerLogger := logger.WithFields(logrus.Fields{
		"component": "server",
	})

	raftClusterServer := raft_node.NewClusterServer(raftClusterServerLogger, raft_node.WithEndpoint(opts.InternalEndpoint))

	// Initialize persister of state
	statePersisterLogger := logger.WithFields(logrus.Fields{
		"component": "LevelDBStatePersister",
	})

	fileStatePath := "db_node_State"
	fileStatePersister := persister.NewLevelDBStateLogger(statePersisterLogger, fileStatePath)
	defer fileStatePersister.Close()

	// Initialize persister of log entries
	logEntryPersisterLogger := logger.WithFields(logrus.Fields{
		"component": "LevelDBLogPersister",
	})

	fileLogPath := "db_node_Log"
	fileLogEntryPersister := persister.NewLevelDBLogEntryPersister(logEntryPersisterLogger, fileLogPath)
	defer fileLogEntryPersister.Close()

	stateHandler := raft_node.NewStateManager(fileStatePersister)
	logHandler := raft_node.NewLogEntryManager(fileLogEntryPersister)

	// Initialize state machine
	stateMachineLogger := logger.WithFields(logrus.Fields{
		"component": "stateMachine",
	})
	stateMachine := raft_node.NewStateMachine(stateMachineLogger)

	// Initialize HTTP Server
	// TODO: We could write a manager service that will spawn the cluster and
	// dynamically route requests to the leader based on redirect responses
	HTTPEndpoints := map[string]string{}
	for name, endpoint := range opts.OtherNamedHTTPEndpoints {
		HTTPEndpoints[name] = endpoint
	}
	externalServerLogger := logger.WithFields(logrus.Fields{
		"component": "httpServer",
	})
	httpServer := http_server.New(opts.HTTPEndpoint, HTTPEndpoints, externalServerLogger)

	// Initialize Raft engine
	raftEngineLogger := logger.WithFields(logrus.Fields{
		"component": "raft",
	})

	raftEngine := raft_node.NewRaft(raftEngineLogger, stateHandler, logHandler, stateMachine,
		raftClusterClient, raftClusterServer, httpServer, raft_node.WithServerID(opts.Name))

	// Start cluster server
	wgClusterServer := sync.WaitGroup{}
	wgClusterServer.Add(1)
	go func() {
		defer wgClusterServer.Done()
		err := raftClusterServer.Run()
		if err != nil {
			logger.Panicf("running cluster server failed: %+v", err)
		}
	}()

	// Start cluster client
	err = raftClusterClient.StartCluster(opts.OtherNamedEndpoints)
	if err != nil {
		logger.Panicf("unable to start Raft cluster client: %+v", err)
	}

	// Start Raft engine
	wgRaftEngine := sync.WaitGroup{}
	wgRaftEngine.Add(1)
	go func() {
		defer wgRaftEngine.Done()
		err := raftEngine.Run()
		if err != nil {
			logger.Panicf("running raft engine failed: %+v", err)
		}
	}()

	// Start HTTP server
	wgHTTPServer := sync.WaitGroup{}
	wgHTTPServer.Add(1)
	go func() {
		defer wgHTTPServer.Done()
		err := httpServer.Run()
		if err != nil {
			logger.Panicf("running http server failed: %+v", err)
		}
	}()

	// Now, the cluster should be running
	<-interruptChannel

	// Close servers, Raft instances and cluster clients down
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = httpServer.Shutdown(ctx)
	if err != nil {
		logger.Errorf("unable to shutdown HTTP server: %+v", err)
	}

	raftClusterServer.Close()
	raftEngine.Close()
	raftClusterClient.Close()

	wgHTTPServer.Wait()
	wgClusterServer.Wait()
	wgRaftEngine.Wait()
}
