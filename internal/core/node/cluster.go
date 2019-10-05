package node

import (
	"context"
	"fmt"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	pb "raft/internal/core/node/gen"
	"sync"
	"time"
)

type clusterOptions struct {
	defaultRPCRequestTimeout time.Duration
}

var (
	defaultClusterOptions = clusterOptions{
		defaultRPCRequestTimeout: 5 * time.Second,
	}
)

type ClusterCallOption func(opt *clusterOptions)

func WithDefaultRPCRequestTimeout(timeout time.Duration) ClusterCallOption {
	return func(opt *clusterOptions) {
		opt.defaultRPCRequestTimeout = timeout
	}
}

func applyClusterOptions(opts []ClusterCallOption) *clusterOptions {
	options := defaultClusterOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &options
}

type ClusterState struct {
	numberOfNodes  int
	leaderName     string
	leaderEndpoint string
}

type ICluster interface {
	StartCluster(otherNodes map[string]string) error
	Close()

	GetClusterState() ClusterState
	SetLeader(leaderName string) error
	BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) []*pb.RequestVoteResponse
	BroadcastAppendEntriesRPCs(ctx context.Context, request *pb.AppendEntriesRequest) []*pb.AppendEntriesResponse
}

type NodeInfo struct {
	name       string
	endpoint   string
	connection grpc.ClientConn
	client     pb.NodeClient
}

type Cluster struct {
	logger      *logrus.Entry
	nodes       []*NodeInfo
	leader      *NodeInfo
	leaderMutex sync.RWMutex
	settings    *clusterOptions
}

func NewCluster(logger *logrus.Entry, opts ...ClusterCallOption) *Cluster {
	return &Cluster{
		logger:   logger,
		settings: applyClusterOptions(opts),
	}
}

func (c *Cluster) StartCluster(otherNodes map[string]string) error {
	if len(c.nodes) > 0 {
		return fmt.Errorf("cluster has already started")
	}

	nodes := make([]*NodeInfo, 0, len(otherNodes))

	for name, endpoint := range otherNodes {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithAuthority(endpoint),
			grpc.WithBackoffMaxDelay(1*time.Second))
		if err != nil {
			for _, node := range nodes {
				err := node.connection.Close()
				if err != nil {
					c.logger.Errorf("unable to close a connection for node: %s: %+v", node.name, err)
				}
			}

			return fmt.Errorf("unable to make a gRPC dial: %+v", err)
		}

		client := pb.NewNodeClient(conn)

		nodes = append(nodes, &NodeInfo{
			name:     name,
			endpoint: endpoint,
			client:   client,
		})
	}

	c.nodes = nodes

	return nil
}

func (c *Cluster) Close() {
	for _, node := range c.nodes {
		err := node.connection.Close()
		if err != nil {
			logrus.Errorf("unable to close a connection for node: %s: %+v", node.name, err)
		}
	}
}

func (c *Cluster) GetClusterState() ClusterState {
	c.leaderMutex.RLock()
	defer c.leaderMutex.RUnlock()

	leaderName := ""
	leaderEndpoint := ""

	// NOTE: In case there is no leader, maybe just set to some random node?
	if c.leader != nil {
		leaderName = c.leader.name
		leaderEndpoint = c.leader.endpoint
	}

	return ClusterState{
		numberOfNodes:  len(c.nodes),
		leaderName:     leaderName,
		leaderEndpoint: leaderEndpoint,
	}
}

func (c *Cluster) SetLeader(leaderName string) error {
	for _, node := range c.nodes {
		if node.name == leaderName {
			c.leaderMutex.Lock()
			c.leader = node
			c.leaderMutex.Unlock()
			return nil
		}
	}

	return fmt.Errorf("leader with this name was not found in the list of nodes")
}

func (c *Cluster) BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) []*pb.RequestVoteResponse {
	responses := make([]*pb.RequestVoteResponse, 0, len(c.nodes))
	responseChannel := make(chan *pb.RequestVoteResponse, len(c.nodes))

	wg := sync.WaitGroup{}
	wg.Add(len(c.nodes))

	ctx, cancel := context.WithTimeout(ctx, c.settings.defaultRPCRequestTimeout)
	defer cancel()
	for _, node := range c.nodes {
		go func(node *NodeInfo) {
			defer wg.Done()

			c.logger.Debugf("sending RPC RequestVote to client: %s", node.name)

			resp, err := node.client.RequestVote(ctx, request)
			if err != nil {
				c.logger.Errorf("unable to RPC RequestVote to client %s because: %+v", node.name, err)
				return
			}

			c.logger.Debugf("received response from RPC RequestVote %+v from client: %s", resp, node.name)

			responseChannel <- resp
		}(node)
	}

	wg.Wait()
	close(responseChannel)

	for resp := range responseChannel {
		responses = append(responses, resp)
	}

	return responses
}

func (c *Cluster) BroadcastAppendEntriesRPCs(ctx context.Context, request *pb.AppendEntriesRequest) []*pb.AppendEntriesResponse {
	responses := make([]*pb.AppendEntriesResponse, 0, len(c.nodes))
	responseChannel := make(chan *pb.AppendEntriesResponse, len(c.nodes))

	wg := sync.WaitGroup{}
	wg.Add(len(c.nodes))

	ctx, cancel := context.WithTimeout(ctx, c.settings.defaultRPCRequestTimeout)
	defer cancel()
	for _, node := range c.nodes {
		go func(node *NodeInfo) {
			defer wg.Done()

			c.logger.Debugf("sending RPC AppendEntries to client: %s", node.name)

			resp, err := node.client.AppendEntries(ctx, request)
			if err != nil {
				c.logger.Errorf("unable to RPC AppendEntries to client %+v because: %+v", node.name, err)
				return
			}

			c.logger.Debugf("received response from RPC AppendEntries %+v from client: %s", resp, node.name)

			responseChannel <- resp
		}(node)
	}

	wg.Wait()
	close(responseChannel)

	for resp := range responseChannel {
		responses = append(responses, resp)
	}

	return responses
}
