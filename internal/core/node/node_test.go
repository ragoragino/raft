package node_test

import (
	node "raft/internal/core/node"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	logrus "github.com/sirupsen/logrus"
)

func TestServer(t *testing.T) {
	endpoints := map[string]string{
		"Node0": "127.0.0.1:10000",
		"Node1": "127.0.0.1:10001",
		"Node2": "127.0.0.1:10002",
	}

	clusters := make(map[string]*node.Cluster, len(endpoints))
	for name := range endpoints {
		clusters[name] = node.NewCluster()
	}

	// Start servers
	servers := make([]*node.Server, 0, len(endpoints))
	for name := range endpoints {
		settings := node.Settings{
			ID: name,
			HeartbeatFrequency: 250 * time.Millisecond,
			MaxElectionTimeout: 1000 * time.Millisecond,
			MinElectionTimeout: 500 * time.Millisecond,
		}
	
		servers = append(servers, node.NewServer(settings, clusters[name], logrus.New()))
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

	for _, server := range servers {
		server.Run()
	}
}
