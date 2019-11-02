package node

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"os/exec"
	"runtime"
	"context"

	"github.com/Shopify/toxiproxy/client"
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"raft/internal/core/persister"
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

	clusters := make(map[string]*Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"node": name,
		})

		clusters[name] = NewCluster(logger)
	}

	// Start servers
	servers := make([]*Server, 0, len(endpoints))
	for name, endpoint := range endpoints {
		nodeLogger := logger.WithFields(logrus.Fields{
			"node": name,
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

		servers = append(servers, NewServer(clusters[name], nodeLogger, stateHandler, logHandler,
			WithServerID(name), WithEndpoint(endpoint)))
	}

	// Start clusters
	for name, cluster := range clusters {
		clusterEndpoints := map[string]string{}
		for clusterName, clusterEndpoint := range endpoints {
			if clusterName == name {
				continue
			}
			clusterEndpoints[clusterName] = clusterEndpoint
		}

		err := cluster.StartCluster(clusterEndpoints)
		assert.NoError(t, err)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(servers))
	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			server.Run()
		}(server)
	}

	// Check if leader was elected
	timeToLeader := defaultServerOptions.MaxElectionTimeout + defaultServerOptions.MinElectionTimeout
	time.Sleep(timeToLeader * 2)

	leaderExists := false
	for _, server := range servers {
		server.stateMutex.RLock()
		if server.stateManager.GetRole() == LEADER {
			leaderExists = true
		}
		server.stateMutex.RUnlock()
	}
	assert.True(t, leaderExists)

	// Close clusters and servers down
	for _, server := range servers {
		server.Close()
	}

	for _, cluster := range clusters {
		cluster.Close()
	}

	wg.Wait()
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

	clusters := make(map[string]*Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"node": name,
		})

		clusters[name] = NewCluster(logger)
	}

	// Start servers
	servers := make(map[string]*Server, len(endpoints))
	for name, endpoint := range endpoints {
		nodeLogger := logger.WithFields(logrus.Fields{
			"node": name,
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

		servers[name] = NewServer(clusters[name], nodeLogger, stateHandler, logHandler,
			WithServerID(name), WithEndpoint(endpoint))
	}

	// Start clusters
	for name, cluster := range clusters {
		err := cluster.StartCluster(clusterProxyEndpoints[name])
		assert.NoError(t, err)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(servers))
	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			server.Run()
		}(server)
	}

	// Check if leader was elected
	timeToLeader := defaultServerOptions.MaxElectionTimeout + defaultServerOptions.MinElectionTimeout
	time.Sleep(timeToLeader * 2)

	oldLeaderExists := false
	oldLeaderID := ""
	for _, server := range servers {
		server.stateMutex.RLock()
		if server.stateManager.GetRole() == LEADER {
			oldLeaderExists = true
			oldLeaderID = server.settings.ID
		}
		server.stateMutex.RUnlock()
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
	for _, server := range servers {
		server.stateMutex.RLock()
		if server.stateManager.GetRole() == LEADER && server.settings.ID != oldLeaderID {
			newLeaderExists = true
			newLeaderID = server.settings.ID
		}
		server.stateMutex.RUnlock()
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

	assert.Equal(t, clusters[oldLeaderID].GetClusterState().leaderName, newLeaderID)

	// Close clusters and servers down
	for _, server := range servers {
		server.Close()
	}

	for _, cluster := range clusters {
		cluster.Close()
	}

	wg.Wait()
}
