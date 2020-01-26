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

	"raft/internal/core/persister"
	external_server "raft/internal/core/server"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	startingStateInfo = PersistentStateInfo{
		CurrentTerm: 0,
		VotedFor:    nil,
		Role:        FOLLOWER,
	}
)

func initializeStatePersister(t *testing.T, logger *logrus.Entry, fileStatePath string) (persister.IStateLogger, func()) {
	fileStatePersister := persister.NewLevelDBStateLogger(logger, fileStatePath)

	os.RemoveAll(fileStatePath)

	return fileStatePersister, func() {
		fileStatePersister.Close()
		err := os.RemoveAll(fileStatePath)
		assert.NoError(t, err)
	}
}

func initializeLogEntryPersister(t *testing.T, logger *logrus.Entry, fileStatePath string) (persister.ILogEntryPersister, func()) {
	fileLogEntryPersister := persister.NewLevelDBLogEntryPersister(logger, fileStatePath)

	os.RemoveAll(fileStatePath)

	return fileLogEntryPersister, func() {
		fileLogEntryPersister.Close()
		err := os.RemoveAll(fileStatePath)
		assert.NoError(t, err)
	}
}

func constructClusterComponents(t *testing.T,
	logger *logrus.Logger,
	endpoints map[string]string,
	httpEndpoints map[string]string) (
	rafts map[string]*Raft,
	clusterClients map[string]*Cluster,
	clusterServers map[string]*ClusterServer,
	httpServers map[string]external_server.Interface,
	cleaner func()) {
	cleanerFuncs := []func(){}
	cleaner = func() {
		for _, cleanerFunc := range cleanerFuncs {
			cleanerFunc()
		}
	}

	clusterClients = make(map[string]*Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"component": "cluster",
			"node":      name,
		})

		clusterClients[name] = NewCluster(logger)
	}

	httpServers = make(map[string]external_server.Interface, len(httpEndpoints))
	rafts = make(map[string]*Raft, len(endpoints))
	clusterServers = make(map[string]*ClusterServer, len(endpoints))
	for name, endpoint := range endpoints {
		serverLogger := logger.WithFields(logrus.Fields{
			"component": "server",
			"node":      name,
		})

		clusterServer := NewClusterServer(serverLogger, WithEndpoint(endpoint))
		clusterServers[name] = clusterServer

		nodeLogger := logger.WithFields(logrus.Fields{
			"component": "raft",
			"node":      name,
		})

		statePersisterLogger := logger.WithFields(logrus.Fields{
			"component": "LevelDBStatePersister",
			"node":      name,
		})

		fileStatePath := fmt.Sprintf("db_node_test_%s_State_%s.txt", t.Name(), name)
		fileStateLogger, fileStateLoggerClean :=
			initializeStatePersister(t, statePersisterLogger, fileStatePath)
		cleanerFuncs = append(cleanerFuncs, fileStateLoggerClean)

		logEntryPersisterLogger := logger.WithFields(logrus.Fields{
			"component": "LevelDBLogPersister",
			"node":      name,
		})

		fileLogPath := fmt.Sprintf("db_node_test_%s_Log_%s.txt", t.Name(), name)
		fileLogLogger, fileLogLoggerClean :=
			initializeLogEntryPersister(t, logEntryPersisterLogger, fileLogPath)
		cleanerFuncs = append(cleanerFuncs, fileLogLoggerClean)

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

		rafts[name] = NewRaft(nodeLogger, stateHandler, logHandler, stateMachine,
			clusterClients[name], clusterServers[name], httpServers[name], WithServerID(name))
	}

	return
}

func startClusterServers(t *testing.T, clusterServers map[string]*ClusterServer) *sync.WaitGroup {
	wgServer := sync.WaitGroup{}
	wgServer.Add(len(clusterServers))
	for _, clusterServer := range clusterServers {
		go func(clusterServer *ClusterServer) {
			defer wgServer.Done()
			err := clusterServer.Run()
			if err != nil {
				fmt.Errorf("running cluster server failed: %+v", err)
				t.FailNow()
			}
		}(clusterServer)
	}

	return &wgServer
}

func startClusterClients(t *testing.T, clusterClients map[string]*Cluster, endpoints map[string]string) {
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
}

func startRaftEngines(rafts map[string]*Raft) *sync.WaitGroup {
	wgRaft := sync.WaitGroup{}
	wgRaft.Add(len(rafts))
	for _, raft := range rafts {
		go func(raft *Raft) {
			defer wgRaft.Done()
			raft.Run()
		}(raft)
	}

	return &wgRaft
}

func startHttpServer(t *testing.T, httpServers map[string]external_server.Interface) *sync.WaitGroup {
	wgHTTP := sync.WaitGroup{}
	wgHTTP.Add(len(httpServers))
	for _, httpServer := range httpServers {
		go func(server external_server.Interface) {
			defer wgHTTP.Done()
			err := server.Run()
			if err != nil {
				fmt.Errorf("running http server failed: %+v", err)
				t.FailNow()
			}
		}(httpServer)
	}

	return &wgHTTP
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

	rafts, clusterClients, clusterServers, _, clean := constructClusterComponents(t, logger, endpoints, httpEndpoints)
	defer clean()

	wgServer := startClusterServers(t, clusterServers)

	startClusterClients(t, clusterClients, endpoints)

	wgRaft := startRaftEngines(rafts)

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

	err := cmd.Start()
	if err != nil {
		t.Fatalf("unable to run command: %+v", err)
	}

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

	rafts, clusterClients, clusterServers, _, clean := constructClusterComponents(t, logger, endpoints, httpEndpoints)
	defer clean()

	wgServer := startClusterServers(t, clusterServers)

	// Start clusters clients for RAFT instances
	for name, clusterClient := range clusterClients {
		err := clusterClient.StartCluster(clusterProxyEndpoints[name])
		assert.NoError(t, err)
	}

	wgRaft := startRaftEngines(rafts)

	// Check if leader was elected
	timeToLeader := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(timeToLeader * 2)

	oldLeaderID := ""
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		if raft.stateManager.GetRole() == LEADER {
			oldLeaderID = raft.settings.ID
		}
		raft.stateMutex.RUnlock()
	}
	assert.NotEmpty(t, oldLeaderID)

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

	newLeaderID := ""
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		if raft.stateManager.GetRole() == LEADER && raft.settings.ID != oldLeaderID {
			newLeaderID = raft.settings.ID
		}
		raft.stateMutex.RUnlock()
	}
	assert.NotEmpty(t, newLeaderID)

	// Restart broken connections
	logger.Debugf("Opening all proxies to %s", oldLeaderID)

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

func TestCreateAndGetDocuments(t *testing.T) {
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

	rafts, clusterClients, clusterServers, httpServers, clean := constructClusterComponents(t, logger, endpoints, httpEndpoints)
	defer clean()

	wgServer := startClusterServers(t, clusterServers)

	startClusterClients(t, clusterClients, endpoints)

	wgRaft := startRaftEngines(rafts)

	wgHTTP := startHttpServer(t, httpServers)

	// Wait for the election of leader
	timeToWait := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(2 * timeToWait)

	startRequestsSend := time.Now()

	client := &http.Client{}

	// Start at a random node
	httpEndpoint := httpEndpoints["Node0"]

	// Add key-value pairs to the Raft in parallel
	nOfWorkers := 256
	nOfClientRequests := 1000
	workerCreateRequestChannel := make(chan *external_server.ClientCreateRequest, nOfClientRequests)
	workerGetRequestChannel := make(chan *external_server.ClientCreateRequest, nOfClientRequests)
	for i := 0; i != nOfClientRequests; i++ {
		key := fmt.Sprintf("key-%d", i)
		request := &external_server.ClientCreateRequest{
			Key:   key,
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		workerCreateRequestChannel <- request
		workerGetRequestChannel <- request
	}

	close(workerCreateRequestChannel)
	close(workerGetRequestChannel)

	wgClientCreateRequests := sync.WaitGroup{}
	wgClientCreateRequests.Add(nOfWorkers)
	for i := 0; i != nOfWorkers; i++ {
		go func() {
			defer wgClientCreateRequests.Done()

			for request := range workerCreateRequestChannel {
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
			}
		}()
	}

	wgClientCreateRequests.Wait()

	endRequestsSend := time.Now()

	// Get key-value pairs from Raft
	wgClientGetRequests := sync.WaitGroup{}
	wgClientGetRequests.Add(nOfWorkers)
	for i := 0; i != nOfWorkers; i++ {
		go func() {
			defer wgClientGetRequests.Done()

			for request := range workerGetRequestChannel {
				getRequestJson, err := json.Marshal(&external_server.ClientGetRequest{
					Key: request.Key,
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

				assert.Equal(t, request.Value, response.Value)

				getResponse.Body.Close()
			}
		}()
	}

	wgClientGetRequests.Wait()

	endRequestsGet := time.Now()

	// Close HTTP and cluster servers, Raft instances and cluster clients down
	for _, httpServer := range httpServers {
		err := httpServer.Shutdown(context.Background())
		assert.NoError(t, err)
	}

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
	wgHTTP.Wait()

	logger.Printf("Sending requests took: %+v, Getting requests took: %+v",
		endRequestsSend.Sub(startRequestsSend), endRequestsGet.Sub(endRequestsSend))
}

func TestCreateAndGetDocumentsForAFailedNode(t *testing.T) {
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

	rafts, clusterClients, clusterServers, httpServers, clean := constructClusterComponents(t, logger, endpoints, httpEndpoints)
	defer clean()

	wgServer := startClusterServers(t, clusterServers)

	for name, clusterClient := range clusterClients {
		err := clusterClient.StartCluster(clusterProxyEndpoints[name])
		assert.NoError(t, err)
	}

	wgRaft := startRaftEngines(rafts)

	wgHTTP := startHttpServer(t, httpServers)

	// Wait for the election of leader
	timeToWait := defaultRaftOptions.MaxElectionTimeout + defaultRaftOptions.MinElectionTimeout
	time.Sleep(2 * timeToWait)

	// Find a leader and one non-leader node
	var nonLeaderNode string
	var leaderNode string
	for _, raft := range rafts {
		raft.stateMutex.RLock()
		role := raft.stateManager.GetRole()
		raft.stateMutex.RUnlock()

		if role != LEADER {
			nonLeaderNode = raft.settings.ID
		} else if role == LEADER {
			leaderNode = raft.settings.ID
		}
	}
	assert.NotEmpty(t, nonLeaderNode)
	assert.NotEmpty(t, leaderNode)

	client := &http.Client{}

	httpEndpoint := httpEndpoints[leaderNode]

	clientCreateRequests := map[string]*external_server.ClientCreateRequest{}

	// Add first key-value pairs to the Raft
	nOfStartingRequests := 10
	nOfTotalRequests := 20
	for i := 0; i != nOfStartingRequests; i++ {
		key := fmt.Sprintf("key-%d", i)
		request := &external_server.ClientCreateRequest{
			Key:   key,
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		clientCreateRequests[key] = request

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
	}

	// Disable traffic to this node
	proxies, err := toxiClient.Proxies()
	assert.NoError(t, err)

	disabledNodeTraffic := false
	for name, proxy := range proxies {
		if strings.Contains(name, nonLeaderNode) {
			logger.Debugf("disabling proxy: %s", name)
			err := proxy.Disable()
			assert.NoError(t, err)
			disabledNodeTraffic = true
		}
	}
	assert.True(t, disabledNodeTraffic)

	// Add new key-value pairs to Raft
	for i := nOfStartingRequests; i != nOfTotalRequests; i++ {
		key := fmt.Sprintf("key-%d", i)
		request := &external_server.ClientCreateRequest{
			Key:   key,
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		clientCreateRequests[key] = request

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
	}

	// Enable traffic to the failed node
	for name, proxy := range proxies {
		if strings.Contains(name, nonLeaderNode) {
			logger.Debugf("enabling proxy: %s", name)
			err := proxy.Enable()
			assert.NoError(t, err)
		}
	}

	// Wait for the replication of documents
	time.Sleep(10 * time.Second)

	// Get documents from the previously failed node's state machine
	failedNode, ok := rafts[nonLeaderNode]
	assert.True(t, ok)

	for key, value := range clientCreateRequests {
		stateMachineValue, ok := failedNode.stateMachine.Get(key)
		assert.True(t, ok)
		assert.Equal(t, value.Value, stateMachineValue)
	}

	// Close HTTP and cluster servers, Raft instances and cluster clients down
	for _, httpServer := range httpServers {
		err := httpServer.Shutdown(context.Background())
		assert.NoError(t, err)
	}

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
	wgHTTP.Wait()
}

// TODO
func TestStress(t *testing.T) {

}
