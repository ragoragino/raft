package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	pb "raft/internal/core/node/gen"
	"sync"

	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type serverOptions struct {
	Endpoint string
}

var (
	defaultServerOptions = serverOptions{
		Endpoint: "localhost:10000",
	}
)

type ServerCallOption func(opt *serverOptions)

func WithEndpoint(endpoint string) ServerCallOption {
	return func(opt *serverOptions) {
		opt.Endpoint = endpoint
	}
}

func applyServerOptions(opts []ServerCallOption) *serverOptions {
	options := defaultServerOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &options
}

type IClusterServer interface {
	GetAppendEntriesChannel() (<-chan appendEntriesProcessRequest, error)
	GetRequestVoteChannel() (<-chan requestVoteProcessRequest, error)
}

type appendEntriesProcessResponse struct {
	Response *pb.AppendEntriesResponse
	Error    error
}

type appendEntriesProcessRequest struct {
	Context         context.Context
	Request         *pb.AppendEntriesRequest
	ResponseChannel chan<- appendEntriesProcessResponse
}

type requestVoteProcessResponse struct {
	Response *pb.RequestVoteResponse
	Error    error
}

type requestVoteProcessRequest struct {
	Context         context.Context
	Request         *pb.RequestVoteRequest
	ResponseChannel chan<- requestVoteProcessResponse
}

type ClusterServer struct {
	grpcServer         *grpc.Server
	runFinishedChannel chan struct{}
	logger             *logrus.Entry
	closeOnce          sync.Once
	settings           *serverOptions

	appendEntriesProcessChannel chan appendEntriesProcessRequest
	requestVoteProcessChannel   chan requestVoteProcessRequest
}

func NewClusterServer(logger *logrus.Entry, opts ...ServerCallOption) *ClusterServer {
	clusterServer := &ClusterServer{
		settings:           applyServerOptions(opts),
		runFinishedChannel: make(chan struct{}),
		logger:             logger,
	}

	clusterServer.grpcServer = grpc.NewServer(grpc.UnaryInterceptor(contextGettingInterceptor))
	pb.RegisterNodeServer(clusterServer.grpcServer, clusterServer)

	return clusterServer
}

func (s *ClusterServer) Run() error {
	lis, err := net.Listen("tcp", s.settings.Endpoint)
	if err != nil {
		return err
	}

	if err := s.grpcServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// Should be closed only once
func (s *ClusterServer) Close() {
	s.logger.Debugf("closing cluster server")

	s.grpcServer.GracefulStop()

	// Closing process channels should be safe because we have
	// already closed the server (i.e. no more RPCs)
	close(s.appendEntriesProcessChannel)
	close(s.requestVoteProcessChannel)

	s.logger.Debugf("cluster server closed")
}

func (s *ClusterServer) GetAppendEntriesChannel() (<-chan appendEntriesProcessRequest, error) {
	if s.appendEntriesProcessChannel != nil {
		return nil, fmt.Errorf("append entries channel was already taken.")
	}

	s.appendEntriesProcessChannel = make(chan appendEntriesProcessRequest)
	return s.appendEntriesProcessChannel, nil
}
func (s *ClusterServer) GetRequestVoteChannel() (<-chan requestVoteProcessRequest, error) {
	if s.requestVoteProcessChannel != nil {
		return nil, fmt.Errorf("request vote channel was already taken.")
	}

	s.requestVoteProcessChannel = make(chan requestVoteProcessRequest)
	return s.requestVoteProcessChannel, nil
}

func (s *ClusterServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	responseChannel := make(chan appendEntriesProcessResponse)

	s.appendEntriesProcessChannel <- appendEntriesProcessRequest{
		Context:         ctx,
		Request:         request,
		ResponseChannel: responseChannel,
	}

	response := <-responseChannel

	close(responseChannel)

	return response.Response, response.Error
}

func (s *ClusterServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	responseChannel := make(chan requestVoteProcessResponse)

	s.requestVoteProcessChannel <- requestVoteProcessRequest{
		Context:         ctx,
		Request:         request,
		ResponseChannel: responseChannel,
	}

	response := <-responseChannel

	close(responseChannel)

	return response.Response, response.Error
}

func contextGettingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = contextFromGrpcMetadata(ctx)
	return handler(ctx, req)
}

func contextFromGrpcMetadata(ctx context.Context) context.Context {
	// If there is no request ID, it is a bug
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		panic(fmt.Sprintf("no metadata present in the ctx: %+v", ctx))
	} else if len(md[requestIDKey]) != 1 {
		panic(fmt.Sprintf("not a single request ID key present in the metadata: %+v", md))
	}

	return context.WithValue(ctx, requestIDKey, md[requestIDKey][0])
}
