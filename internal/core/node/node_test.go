package node_test

import (
	"os"
	node "raft/internal/core/node"
	"sync"
	"testing"

	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Out = os.Stdout

	endpoints := map[string]string{
		"Node0": "localhost:10000",
		"Node1": "localhost:10001",
		"Node2": "localhost:10002",
	}

	clusters := make(map[string]*node.Cluster, len(endpoints))
	for name := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"node": name,
		})

		clusters[name] = node.NewCluster(logger)
	}

	// Start servers
	servers := make([]*node.Server, 0, len(endpoints))
	for name, endpoint := range endpoints {
		logger := logger.WithFields(logrus.Fields{
			"node": name,
		})

		servers = append(servers, node.NewServer(clusters[name], logger,
			node.WithServerID(name), node.WithEndpoint(endpoint)))
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

		defer cluster.Close()
	}

	wg := sync.WaitGroup{}
	wg.Add(len(servers))
	for _, server := range servers {
		go func(server *node.Server) {
			defer wg.Done()
			server.Run()
		}(server)
	}

	wg.Wait()
}
