package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

// TODO: Generalize all the setup of cluster, servers and raft components

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

	httpEndpoints := map[string]string{
		"Node0": "localhost:8000",
		"Node1": "localhost:8001",
		"Node2": "localhost:8002",
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

		stateMachineLogger := logger.WithFields(logrus.Fields{
			"component": "stateMachine",
			"node":      name,
		})
		stateMachine := NewStateMachine(stateMachineLogger)

		externalServerLogger := logger.WithFields(logrus.Fields{
			"component": "httpServer",
			"node":      name,
		})
		externalServer := external_server.New(httpEndpoints[name], httpEndpoints, externalServerLogger)

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

	httpEndpoints := map[string]string{
		"Node0": "localhost:8000",
		"Node1": "localhost:8001",
		"Node2": "localhost:8002",
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

		stateMachineLogger := logger.WithFields(logrus.Fields{
			"component": "stateMachine",
			"node":      name,
		})
		stateMachine := NewStateMachine(stateMachineLogger)

		externalServerLogger := logger.WithFields(logrus.Fields{
			"component": "httpServer",
			"node":      name,
		})
		externalServer := external_server.New(httpEndpoints[name], httpEndpoints, externalServerLogger)

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

func TestCreateAndGetDocument(t *testing.T) {
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

	httpEndpoints := map[string]string{
		"Node0": "localhost:8000",
		"Node1": "localhost:8001",
		"Node2": "localhost:8002",
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

	httpServers := make(map[string]external_server.Interface, len(httpEndpoints))

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

		fileStatePath := fmt.Sprintf("db_node_test_TestCreateAndGetDocument_State_%s.txt", name)
		fileStateLogger, fileStateLoggerClean :=
			initializeStatePersister(t, logger, fileStatePath)
		defer fileStateLoggerClean()

		fileLogPath := fmt.Sprintf("db_node_test_TestCreateAndGetDocument_Log_%s.txt", name)
		fileLogLogger, fileLogLoggerClean :=
			initializeLogEntryPersister(t, logger, fileLogPath)
		defer fileLogLoggerClean()

		stateHandler := NewStateManager(startingStateInfo, fileStateLogger)
		logHandler := NewLogEntryManager(fileLogLogger)

		stateMachineLogger := logger.WithFields(logrus.Fields{
			"component": "stateMachine",
			"node":      name,
		})
		stateMachine := NewStateMachine(stateMachineLogger)

		externalServerLogger := logger.WithFields(logrus.Fields{
			"component": "httpServer",
			"node":      name,
		})
		httpServers[name] = external_server.New(httpEndpoints[name], httpEndpoints, externalServerLogger)

		rafts = append(rafts, NewRaft(clusterClients[name], nodeLogger, stateHandler, logHandler,
			stateMachine, clusterServer, httpServers[name], WithServerID(name)))
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

	// Start external HTTP servers
	wgHTTP := sync.WaitGroup{}
	wgHTTP.Add(len(httpEndpoints))
	for _, httpServer := range httpServers {
		go func(server external_server.Interface) {
			defer wgHTTP.Done()
			err := server.Run()
			assert.NoError(t, err)
		}(httpServer)
	}

	// Wait for the election of leader
	timeToWait := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(timeToWait)

	startRequestsSend := time.Now()

	client := &http.Client{}

	// Start at a random node
	httpEndpoint := httpEndpoints["Node0"]

	// Create key-value pairs to send to Raft
	numberOfClientRequests := 100
	clientRequests := make(map[string][]byte, numberOfClientRequests)
	for i := 0; i != numberOfClientRequests; i++ {
		clientRequests[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
	}

	// Add key-value pairs to the Raft in parallel
	wgClientCreateRequests := sync.WaitGroup{}
	wgClientCreateRequests.Add(numberOfClientRequests)
	for clientRequestKey, clientRequestValue := range clientRequests {
		buffer := make([]byte, len(clientRequestValue))
		copy(buffer, clientRequestValue)

		go func(key string, value []byte) {
			defer wgClientCreateRequests.Done()

			request := &external_server.ClusterCreateRequest{
				Key:   key,
				Value: value,
			}
			createRequestJson, err := json.Marshal(request)
			assert.NoError(t, err)

			createRequest, err := http.NewRequest("POST", "http://"+httpEndpoint+"/create", bytes.NewBuffer(createRequestJson))
			assert.NoError(t, err)

			// Set GetBody so that client can copy body to follow redirects
			createRequest.GetBody = func() (io.ReadCloser, error) {
				return ioutil.NopCloser(bytes.NewBuffer(createRequestJson)), nil
			}

			createResponse, err := client.Do(createRequest)
			assert.NoError(t, err)
			assert.Equal(t, http.StatusOK, createResponse.StatusCode)

			createResponse.Body.Close()
		}(clientRequestKey, buffer)
	}

	wgClientCreateRequests.Wait()

	endRequestsSend := time.Now()

	// Get key-value pairs from Raft
	wgClientGetRequests := sync.WaitGroup{}
	wgClientGetRequests.Add(numberOfClientRequests)
	for clientRequestKey, clientRequestValue := range clientRequests {
		buffer := make([]byte, len(clientRequestValue))
		copy(buffer, clientRequestValue)

		go func(key string, value []byte) {
			defer wgClientGetRequests.Done()

			getRequestJson, err := json.Marshal(&external_server.ClusterGetRequest{
				Key: key,
			})
			assert.NoError(t, err)

			getRequest, err := http.NewRequest("POST", "http://"+httpEndpoint+"/get", bytes.NewBuffer(getRequestJson))
			assert.NoError(t, err)

			// Set GetBody so that client can copy body to follow redirects
			getRequest.GetBody = func() (io.ReadCloser, error) {
				return ioutil.NopCloser(bytes.NewBuffer(getRequestJson)), nil
			}

			getResponse, err := client.Do(getRequest)
			assert.NoError(t, err)
			assert.Equal(t, http.StatusOK, getResponse.StatusCode)

			getResponseJson, err := ioutil.ReadAll(getResponse.Body)
			assert.NoError(t, err)

			response := &external_server.ClientGetResponse{}
			err = json.Unmarshal(getResponseJson, response)
			assert.NoError(t, err)

			assert.Equal(t, value, response.Value)

			getResponse.Body.Close()
		}(clientRequestKey, buffer)
	}

	wgClientGetRequests.Wait()

	endRequestsGet := time.Now()

	// Close servers, Raft instances, cluster clients and http servers down
	for _, clusterServer := range clusterServers {
		clusterServer.Close()
	}

	for _, raft := range rafts {
		raft.Close()
	}

	for _, clusterClients := range clusterClients {
		clusterClients.Close()
	}

	for _, httpServer := range httpServers {
		err := httpServer.Shutdown(context.Background())
		assert.NoError(t, err)
	}

	wgServer.Wait()
	wgRaft.Wait()
	wgHTTP.Wait()

	logger.Printf("Sending requests took: %+v, Getting requests took: %+v",
		endRequestsSend.Sub(startRequestsSend), endRequestsGet.Sub(endRequestsSend))
}
