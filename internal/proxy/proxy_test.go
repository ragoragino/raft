package proxy_test

//go:generate protoc --proto_path=./proto --go_out=plugins=grpc:./pb ./proto/proxy.proto

import (
	"context"
	"fmt"
	"net"
	"net/http"
	proxy "raft/internal/proxy"
	pb "raft/internal/proxy/pb"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testNode struct {
	endpoint          string
	server            *grpc.Server
	testNodeClients   map[string]pb.TestNodeClient
	clientConnections map[string]*grpc.ClientConn
}

func newTestNode(t *testing.T, endpoint string) *testNode {
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTestNodeServer(s, &testNode{})

	go func() {
		if err := s.Serve(lis); err != nil && err != http.ErrServerClosed {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	return &testNode{
		endpoint:          endpoint,
		server:            s,
		testNodeClients:   make(map[string]pb.TestNodeClient),
		clientConnections: make(map[string]*grpc.ClientConn),
	}
}

func (n *testNode) shutdown() {
	n.server.Stop()

	for _, clientConnection := range n.clientConnections {
		clientConnection.Close()
	}
}

func (n *testNode) registerClient(name string, otherEndpoint string) error {
	conn, err := grpc.Dial(otherEndpoint, grpc.WithInsecure(), grpc.WithAuthority(otherEndpoint))
	if err != nil {
		log.Errorf("did not connect: %v", err)
		return err
	}
	n.testNodeClients[name] = pb.NewTestNodeClient(conn)
	n.clientConnections[name] = conn

	return nil
}

func (n *testNode) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	log.Printf("received ctx: %+v. req %+v", ctx, in)
	return &pb.TestResponse{}, nil
}

func (n *testNode) sendTestMessages(ctx context.Context) []error {
	errors := make([]error, len(n.testNodeClients))

	wg := sync.WaitGroup{}
	wg.Add(len(n.testNodeClients))
	i := 0
	for endpoint, _ := range n.testNodeClients {
		go func(err *error) {
			defer wg.Done()

			*err = n.sendTestMessage(ctx, endpoint)
		}(&errors[i])

		i++
	}

	wg.Wait()

	return errors
}

func (n *testNode) sendTestMessage(ctx context.Context, endpoint string) error {
	client, ok := n.testNodeClients[endpoint]
	if !ok {
		return fmt.Errorf("no client with endpoint: %s", endpoint)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "dest", endpoint, "src", n.endpoint)

	log.Printf("sending grpc request to %s", endpoint)
	_, err := client.Test(ctx, &pb.TestRequest{})
	log.Printf("received grpc response from %s: %+v", endpoint, err)

	return err
}

func TestNodes(t *testing.T) {
	endpoints := []string{
		"localhost:9001", "localhost:9002",
	}
	nodes := make([]*testNode, len(endpoints))

	for i := range nodes {
		nodes[i] = newTestNode(t, endpoints[i])
		defer func(i int) {
			log.Printf("shutting down nodes %d", i)
			nodes[i].shutdown()
		}(i)
	}

	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}

			nodes[i].registerClient(endpoints[j], endpoints[i])
		}
	}

	for i := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errors := nodes[i].sendTestMessages(ctx)
		for _, err := range errors {
			assert.NoError(t, err)
		}
	}
}

func TestGrpcProxy(t *testing.T) {
	proxyEndpoint := "localhost:9000"
	lis, err := net.Listen("tcp", proxyEndpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	endpoints := []string{
		"localhost:9001", "localhost:9002",
	}

	router := proxy.NewRouter(endpoints)

	p := proxy.NewProcessor(router.Route)
	defer func() {
		log.Printf("shutting down proxy server")
		p.GracefulStop()
	}()

	go func() {
		if err := p.Serve(lis); err != nil && err != http.ErrServerClosed {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	nodes := make([]*testNode, len(endpoints))

	// Start all test nodes
	for i := range nodes {
		nodes[i] = newTestNode(t, endpoints[i])
		defer func(i int) {
			log.Printf("shutting down nodes %d", i)
			nodes[i].shutdown()
		}(i)
	}

	// Register all relations between test nodes
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}

			nodes[i].registerClient(endpoints[j], proxyEndpoint)
		}
	}

	// Send messages from each test node to each test node
	for i := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		errors := nodes[i].sendTestMessages(ctx)
		for _, err := range errors {
			assert.NoError(t, err)
		}
	}
}

func TestFirewall(t *testing.T) {
	proxyEndpoint := "localhost:9000"
	lis, err := net.Listen("tcp", proxyEndpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	endpoints := []string{
		"localhost:9001", "localhost:9002",
	}

	router := proxy.NewRouter(endpoints)

	p := proxy.NewProcessor(router.Route)
	defer func() {
		log.Printf("shutting down server")
		p.GracefulStop()
	}()

	go func() {
		if err := p.Serve(lis); err != nil && err != http.ErrServerClosed {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	nodes := make([]*testNode, len(endpoints))

	// Start all test nodes
	for i := range nodes {
		nodes[i] = newTestNode(t, endpoints[i])
		defer func(i int) {
			log.Printf("shutting down nodes %d", i)
			nodes[i].shutdown()
		}(i)
	}

	// Register all relations between test nodes
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}

			nodes[i].registerClient(endpoints[j], proxyEndpoint)
		}
	}

	// Send messages from each test node to each test node
	for i := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		errors := nodes[i].sendTestMessages(ctx)
		for _, err := range errors {
			assert.NoError(t, err)
		}
	}

	// Put Node 1 under firewall
	router.AddFirewallRule(endpoints[0])

	// Send message from Node 2 to Node 1
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err := nodes[1].sendTestMessage(ctx, endpoints[0])
		grpcError, ok := status.FromError(err)
		if !ok {
			t.Fatalf("error is not a grpc error")
		}
		assert.Equal(t, grpcError.Code(), codes.Unavailable)
	}

	// Send message from Node 1 to Node 2
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err := nodes[0].sendTestMessage(ctx, endpoints[1])
		grpcError, ok := status.FromError(err)
		if !ok {
			t.Fatalf("error is not a grpc error")
		}
		assert.Equal(t, grpcError.Code(), codes.Unavailable)
	}

	// Remove Node 1 from firewall
	router.RemoveFirewallRule(endpoints[0])

	// Send messages to test
	for i := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		errors := nodes[i].sendTestMessages(ctx)
		for _, err := range errors {
			assert.NoError(t, err)
		}
	}
}
