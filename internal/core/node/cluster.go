package node

import (
	"context"
	"fmt"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpc_backoff "google.golang.org/grpc/backoff"
	"math/rand"
	pb "raft/internal/core/node/gen"
	"sync"
	"time"
)

type clusterOptions struct {
	connectionDialTimeout time.Duration
}

var (
	defaultClusterOptions = clusterOptions{
		connectionDialTimeout: 15 * time.Second,
	}
)

type ClusterCallOption func(opt *clusterOptions)

func WithConnectionDialTimeout(timeout time.Duration) ClusterCallOption {
	return func(opt *clusterOptions) {
		opt.connectionDialTimeout = timeout
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
	leaderName     string
	leaderEndpoint string
	nodes          []string
}

type AppendEntriesAsyncResponseWrapper struct {
	Response *pb.AppendEntriesResponse
	NodeID   string
}

type ICluster interface {
	StartCluster(otherNodes map[string]string) error
	Close()

	GetClusterState() ClusterState
	SetLeader(leaderName string) error
	SendAppendEntries(ctx context.Context, nodeID string, newRequest *pb.AppendEntriesRequest) (<-chan *pb.AppendEntriesResponse, error)
	BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) []*pb.RequestVoteResponse
	BroadcastAppendEntriesRPCs(ctx context.Context, request *pb.AppendEntriesRequest) []*pb.AppendEntriesResponse
	BroadcastAppendEntriesRPCsAsync(ctx context.Context, request *pb.AppendEntriesRequest) <-chan *AppendEntriesAsyncResponseWrapper
}

type NodeInfo struct {
	name       string
	endpoint   string
	connection *grpc.ClientConn
	client     pb.NodeClient
}

type Cluster struct {
	logger                      *logrus.Entry
	nodes                       []*NodeInfo
	leader                      *NodeInfo
	leaderMutex                 sync.RWMutex
	settings                    *clusterOptions
	exponentialBackofferFactory ExponentialBackofferFactory

	closeOnce sync.Once
}

func NewCluster(logger *logrus.Entry, opts ...ClusterCallOption) *Cluster {
	return &Cluster{
		exponentialBackofferFactory: ExponentialBackofferFactory{
			grpc_backoff.DefaultConfig,
		},
		logger:   logger,
		settings: applyClusterOptions(opts),
	}
}

func (c *Cluster) StartCluster(otherNodes map[string]string) error {
	if len(c.nodes) > 0 {
		return fmt.Errorf("cluster has already started")
	}

	nodes := make([]*NodeInfo, 0, len(otherNodes))

	ctx, cancel := context.WithTimeout(context.Background(), c.settings.connectionDialTimeout)
	defer cancel()

	for name, endpoint := range otherNodes {
		conn, err := grpc.DialContext(ctx, endpoint, grpc.WithInsecure(), grpc.WithAuthority(endpoint),
			grpc.WithBlock())
		if err != nil {
			for _, node := range nodes {
				err := node.connection.Close()
				if err != nil {
					c.logger.Errorf("unable to close a connection for node: %s: %+v", node.name, err)
				}
			}

			return fmt.Errorf("unable to make a gRPC dial: %+v", err)
		}

		nodes = append(nodes, &NodeInfo{
			name:       name,
			endpoint:   endpoint,
			connection: conn,
			client:     pb.NewNodeClient(conn),
		})
	}

	c.nodes = nodes

	return nil
}

func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		c.logger.Errorf("closing cluster connections")

		for _, node := range c.nodes {
			err := node.connection.Close()
			if err != nil {
				logrus.Errorf("unable to close a connection for node: %s: %+v", node.name, err)
			}
		}
	})
}

func (c *Cluster) GetClusterState() ClusterState {
	c.leaderMutex.RLock()
	defer c.leaderMutex.RUnlock()

	leaderName := ""
	leaderEndpoint := ""

	if c.leader != nil {
		leaderName = c.leader.name
		leaderEndpoint = c.leader.endpoint
	} else {
		// Get a random node
		randomIndex := rand.Intn(len(c.nodes))
		leader := c.nodes[randomIndex]
		leaderName = leader.name
		leaderEndpoint = leader.endpoint
	}

	nodes := make([]string, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node.name)
	}

	return ClusterState{
		nodes:          nodes,
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

	return fmt.Errorf("leader with this name was not found in the list of nodes: %s", leaderName)
}

func (c *Cluster) SendAppendEntries(ctx context.Context, nodeID string, request *pb.AppendEntriesRequest) (<-chan *pb.AppendEntriesResponse, error) {
	var destinationNode *NodeInfo

	// TODO: Does it make sense to optimize this to map?
	for _, node := range c.nodes {
		if node.name == nodeID {
			destinationNode = node
		}
	}

	if destinationNode == nil {
		return nil, fmt.Errorf("unable to find node %s in a slice of nodes: %+v", nodeID, c.nodes)
	}

	responseChannel := make(chan *pb.AppendEntriesResponse)
	go func() {
		defer close(responseChannel)

		backoffer := c.exponentialBackofferFactory.NewExponentialBackoffer()
		for {
			c.logger.Debugf("sending RPC AppendEntries to client %s for the %d time", nodeID, backoffer.retries)

			resp, err := destinationNode.client.AppendEntries(ctx, request)

			c.logger.Debugf("received response from RPC AppendEntries %+v from client: %s", resp, nodeID)

			if err == nil {
				responseChannel <- resp
				return
			} else if err == context.Canceled {
				return
			}

			time.Sleep(backoffer.Do())
		}
	}()

	return responseChannel, nil
}

func (c *Cluster) BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) []*pb.RequestVoteResponse {
	responses := make([]*pb.RequestVoteResponse, 0, len(c.nodes))
	responseChannel := make(chan *pb.RequestVoteResponse, len(c.nodes))

	wg := sync.WaitGroup{}
	wg.Add(len(c.nodes))

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

func (c *Cluster) BroadcastAppendEntriesRPCsAsync(ctx context.Context, request *pb.AppendEntriesRequest) <-chan *AppendEntriesAsyncResponseWrapper {
	responseChannel := make(chan *AppendEntriesAsyncResponseWrapper, len(c.nodes))

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(c.nodes))

		for _, node := range c.nodes {
			go func(node *NodeInfo) {
				defer wg.Done()

				backoffer := c.exponentialBackofferFactory.NewExponentialBackoffer()
				for {
					c.logger.Debugf("sending RPC AppendEntries to client for the %d time: %s", backoffer.retries, node.name)

					resp, err := node.client.AppendEntries(ctx, request)
					c.logger.Debugf("received response from RPC AppendEntries %+v from client: %s", resp, node.name)

					if err == nil {
						responseChannel <- &AppendEntriesAsyncResponseWrapper{
							Response: resp,
							NodeID:   node.name,
						}
						return
					} else if err == context.Canceled {
						return
					}

					time.Sleep(backoffer.Do())
				}
			}(node)
		}

		wg.Wait()
		close(responseChannel)
	}()

	return responseChannel
}

type ExponentialBackofferFactory struct {
	config grpc_backoff.Config
}

func (ebf *ExponentialBackofferFactory) NewExponentialBackoffer() *ExpontialBackoffer {
	return &ExpontialBackoffer{
		config:  ebf.config,
		retries: 0,
	}
}

type ExpontialBackoffer struct {
	config  grpc_backoff.Config
	retries int
}

func (eb *ExpontialBackoffer) Do() time.Duration {
	defer func() {
		eb.retries++
	}()

	if eb.retries == 0 {
		return eb.config.BaseDelay
	}

	retries := eb.retries
	backoff, max := float64(eb.config.BaseDelay), float64(eb.config.MaxDelay)
	for backoff < max && retries > 0 {
		backoff *= eb.config.Multiplier
		retries--
	}

	if backoff > max {
		return eb.config.MaxDelay
	}

	// Add random jitter to the backoff
	backoff *= 1 + eb.config.Jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}

	return time.Duration(backoff)
}
