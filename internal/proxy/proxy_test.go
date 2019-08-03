package proxy_test

//go:generate protoc --proto_path=./proto --go_out=plugins=grpc:./pb ./proto/proxy.proto

import (
	"context"
	"net"
	"net/http"
	pb "raft/internal/proxy/pb"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type testNode struct {
	server            *grpc.Server
	testNodeClients   map[string]pb.TestNodeClient
	clientConnections map[string]*grpc.ClientConn
}

func newTestNode(t *testing.T, endpoint string) *testNode {
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTestNodeServer(s, &testNode{})

	go func() {
		if err := s.Serve(lis); err != nil && err != http.ErrServerClosed {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	return &testNode{
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

func (n *testNode) registerClient(otherEndpoint string) error {
	conn, err := grpc.Dial(otherEndpoint, grpc.WithInsecure())
	if err != nil {
		log.Errorf("did not connect: %v", err)
		return err
	}
	n.testNodeClients[otherEndpoint] = pb.NewTestNodeClient(conn)
	n.clientConnections[otherEndpoint] = conn

	return nil
}

func (n *testNode) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	log.Printf("received: %+v", in)
	return &pb.TestResponse{}, nil
}

func (n *testNode) sendTestMessage(ctx context.Context) []error {
	errors := make([]error, len(n.testNodeClients))

	wg := sync.WaitGroup{}
	wg.Add(len(n.testNodeClients))
	i := 0
	for endpoint, client := range n.testNodeClients {
		go func(c pb.TestNodeClient, err *error) {
			defer wg.Done()

			log.Printf("sending grpc request to %s", endpoint)
			_, grpcErr := c.Test(ctx, &pb.TestRequest{})
			log.Printf("received grpc response from %s: %+v", endpoint, err)
			*err = grpcErr
		}(client, &errors[i])

		i++
	}

	wg.Wait()

	return errors
}

func TestProxy(t *testing.T) {
	endpoints := []string{
		"localhost:9001", "localhost:9002",
	}
	nodes := make([]*testNode, len(endpoints))

	for i := range nodes {
		nodes[i] = newTestNode(t, endpoints[i])
		defer nodes[i].shutdown()
	}

	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}

			nodes[i].registerClient(endpoints[i])
		}
	}

	for i := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errors := nodes[i].sendTestMessage(ctx)
		for _, err := range errors {
			assert.NoError(t, err)
		}
	}
}
