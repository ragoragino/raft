package node

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/toxiproxy/client"
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"raft/internal/core/persister"
	external_server "raft/internal/core/server"
)

var (
	startingStateInfo = StateInfo{
		CurrentTerm: 0,
		VotedFor:    nil,
		Role:        FOLLOWER,
	}
)

func initializeStatePersister(t *testing.T, logger *logrus.Logger, fileStatePath string) (persister.IStateLogger, func()) {
	fileStatePersister := persister.NewLevelDBStateLogger(logger.WithFields(logrus.Fields{
		"name": "LevelDBStatePersister",
	}), fileStatePath)

	os.RemoveAll(fileStatePath)

	return fileStatePersister, func() {
		fileStatePersister.Close()
		err := os.RemoveAll(fileStatePath)
		assert.NoError(t, err)
	}
}

func initializeLogEntryPersister(t *testing.T, logger *logrus.Logger, fileStatePath string) (persister.ILogEntryPersister, func()) {
	fileLogEntryPersister := persister.NewLevelDBLogEntryPersister(logger.WithFields(logrus.Fields{
		"name": "LevelDBLogPersister",
	}), fileStatePath)

	os.RemoveAll(fileStatePath)

	return fileLogEntryPersister, func() {
		fileLogEntryPersister.Close()
		err := os.RemoveAll(fileStatePath)
		assert.NoError(t, err)
	}
}

func TestLeaderElected(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	endpoints := map[string]string{
		"Node0": "localhost:10000",
		"Node1": "localhost:10001",
		"Node2": "localhost:10002",
	}

	// Setup cluster clients for RAFT
	clusterClients := make(map[string]*Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"component": "cluster",
			"node":      name,
		})

		clusterClients[name] = NewCluster(logger)
	}

	// Start Raft handlers and cluster server
	rafts := make([]*Raft, 0, len(endpoints))
	clusterServers := make([]*ClusterServer, 0, len(endpoints))
	for name, endpoint := range endpoints {
		serverLogger := logger.WithFields(logrus.Fields{
			"component": "server",
			"node":      name,
		})

		clusterServer := NewClusterServer(serverLogger, WithEndpoint(endpoint))
		clusterServers = append(clusterServers, clusterServer)

		nodeLogger := logger.WithFields(logrus.Fields{
			"component": "raft",
			"node":      name,
		})

		fileStatePath := fmt.Sprintf("db_node_test_TestLeaderElected_State_%s.txt", name)
		fileStateLogger, fileStateLoggerClean :=
			initializeStatePersister(t, logger, fileStatePath)
		defer fileStateLoggerClean()

		fileLogPath := fmt.Sprintf("db_node_test_TestLeaderElected_Log_%s.txt", name)
		fileLogLogger, fileLogLoggerClean :=
			initializeLogEntryPersister(t, logger, fileLogPath)
		defer fileLogLoggerClean()

		stateHandler := NewStateManager(startingStateInfo, fileStateLogger)
		logHandler := NewLogEntryManager(fileLogLogger)

		// TODO
		stateMachineLogger := logger.WithFields(logrus.Fields{
			"component": "stateMachine",
			"node":      name,
		})
		stateMachine := NewStateMachine(stateMachineLogger)
		
		externalServerLogger := logger.WithFields(logrus.Fields{
			"component": "httpServer",
			"node":      name,
		})
		externalServer := external_server.New("localhost:80", externalServerLogger)

		rafts = append(rafts, NewRaft(clusterClients[name], nodeLogger, stateHandler, logHandler,
			stateMachine, clusterServer, externalServer, WithServerID(name)))
	}

	// Start servers
	wgServer := sync.WaitGroup{}
	wgServer.Add(len(clusterServers))
	for _, clusterServer := range clusterServers {
		go func(clusterServer *ClusterServer) {
			defer wgServer.Done()
			clusterServer.Run()
		}(clusterServer)
	}

	// Start client clusters
	for name, clusterClient := range clusterClients {
		clusterEndpoints := map[string]string{}
		for clusterName, clusterEndpoint := range endpoints {
			if clusterName == name {
				continue
			}
			clusterEndpoints[clusterName] = clusterEndpoint
		}

		err := clusterClient.StartCluster(clusterEndpoints)
		assert.NoError(t, err)
	}

	// Start Raft instances
	wgRaft := sync.WaitGroup{}
	wgRaft.Add(len(rafts))
	for _, raft := range rafts {
		go func(raft *Raft) {
			defer wgRaft.Done()
			raft.Run()
		}(raft)
	}

	// Check if leader was elected
	timeToLeader := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(timeToLeader * 2)

	leaderExists := false
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		if raft.stateManager.GetRole() == LEADER {
			leaderExists = true
		}
		raft.stateMutex.RUnlock()
	}
	assert.True(t, leaderExists)

	// Close servers, Raft instances and cluster clients down
	for _, clusterServer := range clusterServers {
		clusterServer.Close()
	}

	for _, raft := range rafts {
		raft.Close()
	}

	for _, clusterClients := range clusterClients {
		clusterClients.Close()
	}

	wgServer.Wait()
	wgRaft.Wait()
}

func createProxy(t *testing.T, endpoints map[string]string, clusterEndpoints map[string]map[string]string) *toxiproxy.Client {
	assert.Equal(t, len(endpoints), len(clusterEndpoints))

	toxiClient := toxiproxy.NewClient("localhost:8474")

	for nodeName, nodeEndpoint := range endpoints {
		for otherEndpointName, otherEndpoint := range clusterEndpoints {
			if otherEndpointName == nodeName {
				continue
			}

			_, err := toxiClient.CreateProxy(otherEndpointName+":"+nodeName, otherEndpoint[nodeName], nodeEndpoint)
			assert.NoError(t, err)
		}
	}

	return toxiClient
}

func startProxyServer(t *testing.T) (*exec.Cmd, func()) {
	toxiProxyBinaryPath := "../../../toxiproxy-server-linux-amd64"

	if runtime.GOOS == "windows" {
		toxiProxyBinaryPath = "../../../toxiproxy-server-windows-amd64.exe"
	}

	if _, err := os.Stat(toxiProxyBinaryPath); os.IsNotExist(err) {
		t.Fatalf("toxiproxy server binary is missing")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, toxiProxyBinaryPath)

	go cmd.Run()

	return cmd, cancel
}

func TestLeaderElectedAfterPartition(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	// Initialize all endpoints and proxy endpoints
	endpoints := map[string]string{
		"Node0": "localhost:10000",
		"Node1": "localhost:10001",
		"Node2": "localhost:10002",
	}

	clusterProxyEndpoints := map[string]map[string]string{
		"Node0": {"Node1": "localhost:11001", "Node2": "localhost:11002"},
		"Node1": {"Node0": "localhost:12001", "Node2": "localhost:12002"},
		"Node2": {"Node0": "localhost:13001", "Node1": "localhost:13002"},
	}

	proxyCmd, killProxyServer := startProxyServer(t)
	toxiClient := createProxy(t, endpoints, clusterProxyEndpoints)
	defer func() {
		proxies, err := toxiClient.Proxies()
		assert.NoError(t, err)

		for _, proxy := range proxies {
			err := proxy.Delete()
			assert.NoError(t, err)
		}

		killProxyServer()
		proxyCmd.Wait()
	}()

	clusterClients := make(map[string]*Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"component": "cluster",
			"node":      name,
		})

		clusterClients[name] = NewCluster(logger)
	}

	// Start servers
	clusterServers := make(map[string]*ClusterServer, len(endpoints))
	rafts := make(map[string]*Raft, len(endpoints))
	for name, endpoint := range endpoints {
		serverLogger := logger.WithFields(logrus.Fields{
			"component": "server",
			"node":      name,
		})

		clusterServers[name] = NewClusterServer(serverLogger, WithEndpoint(endpoint))

		nodeLogger := logger.WithFields(logrus.Fields{
			"component": "raft",
			"node":      name,
		})

		fileStatePath := fmt.Sprintf("db_node_test_TestLeaderElectedAfterPartition_State_%s.txt", name)
		fileStateLogger, fileStateLoggerClean :=
			initializeStatePersister(t, logger, fileStatePath)
		defer fileStateLoggerClean()

		fileLogPath := fmt.Sprintf("db_node_test_TestLeaderElectedAfterPartition_Log_%s.txt", name)
		fileLogLogger, fileLogLoggerClean :=
			initializeLogEntryPersister(t, logger, fileLogPath)
		defer fileLogLoggerClean()

		stateHandler := NewStateManager(startingStateInfo, fileStateLogger)
		logHandler := NewLogEntryManager(fileLogLogger)

		// TODO
		stateMachineLogger := logger.WithFields(logrus.Fields{
			"component": "stateMachine",
			"node":      name,
		})
		stateMachine := NewStateMachine(stateMachineLogger)
			
		externalServerLogger := logger.WithFields(logrus.Fields{
			"component": "httpServer",
			"node":      name,
		})
		externalServer := external_server.New("localhost:80", externalServerLogger)

		rafts[name] = NewRaft(clusterClients[name], nodeLogger, stateHandler, logHandler,
			stateMachine, clusterServers[name], externalServer, WithServerID(name))
	}

	wgServer := sync.WaitGroup{}
	wgServer.Add(len(clusterServers))
	for _, clusterServer := range clusterServers {
		go func(clusterServer *ClusterServer) {
			defer wgServer.Done()
			clusterServer.Run()
		}(clusterServer)
	}

	// Start clusters clients for RAFT instances
	for name, clusterClient := range clusterClients {
		err := clusterClient.StartCluster(clusterProxyEndpoints[name])
		assert.NoError(t, err)
	}

	wgRaft := sync.WaitGroup{}
	wgRaft.Add(len(rafts))
	for _, raft := range rafts {
		go func(raft *Raft) {
			defer wgRaft.Done()
			raft.Run()
		}(raft)
	}

	// Check if leader was elected
	timeToLeader := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(timeToLeader * 2)

	oldLeaderExists := false
	oldLeaderID := ""
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		if raft.stateManager.GetRole() == LEADER {
			oldLeaderExists = true
			oldLeaderID = raft.settings.ID
		}
		raft.stateMutex.RUnlock()
	}
	assert.True(t, oldLeaderExists)

	// Close all connections to leader
	logger.Debugf("Closing all proxies to %s", oldLeaderID)

	proxies, err := toxiClient.Proxies()
	assert.NoError(t, err)

	for name, proxy := range proxies {
		if strings.Contains(name, oldLeaderID) {
			err := proxy.Disable()
			assert.NoError(t, err)
		}
	}

	// Check if leader was elected
	time.Sleep(timeToLeader * 2)

	newLeaderExists := false
	newLeaderID := ""
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		if raft.stateManager.GetRole() == LEADER && raft.settings.ID != oldLeaderID {
			newLeaderExists = true
			newLeaderID = raft.settings.ID
		}
		raft.stateMutex.RUnlock()
	}
	assert.True(t, newLeaderExists)

	// Restart broken connections
	logger.Debugf("Opening all proxies to %s", oldLeaderID)

	proxies, err = toxiClient.Proxies()
	assert.NoError(t, err)

	for name, proxy := range proxies {
		if strings.Contains(name, oldLeaderID) {
			err := proxy.Enable()
			assert.NoError(t, err)
		}
	}

	// Check if the old leader respects the new one
	// Increase the sleep a bit due to proxies restarting
	time.Sleep(timeToLeader * 3)

	assert.Equal(t, clusterClients[oldLeaderID].GetClusterState().leaderName, newLeaderID)

	// Close servers, Raft instances and cluster clients down
	for _, clusterServer := range clusterServers {
		clusterServer.Close()
	}

	for _, raft := range rafts {
		raft.Close()
	}

	for _, clusterClient := range clusterClients {
		clusterClient.Close()
	}

	wgServer.Wait()
	wgRaft.Wait()
}
