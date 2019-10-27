package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	logrus "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"net"
	"net/http"
	pb "raft/internal/core/node/gen"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type serverOptions struct {
	ID                 string
	Endpoint           string
	HeartbeatFrequency time.Duration
	MaxElectionTimeout time.Duration
	MinElectionTimeout time.Duration
}

var (
	defaultServerOptions = serverOptions{
		ID:                 "Node",
		Endpoint:           "localhost:10000",
		HeartbeatFrequency: 500 * time.Millisecond,
		MaxElectionTimeout: 1000 * time.Millisecond,
		MinElectionTimeout: 750 * time.Millisecond,
	}
)

type ServerCallOption func(opt *serverOptions)

func WithServerID(id string) ServerCallOption {
	return func(opt *serverOptions) {
		opt.ID = id
	}
}

func WithEndpoint(endpoint string) ServerCallOption {
	return func(opt *serverOptions) {
		opt.Endpoint = endpoint
	}
}

func WithHeartbeatFrequency(freq time.Duration) ServerCallOption {
	return func(opt *serverOptions) {
		opt.HeartbeatFrequency = freq
	}
}

func WithMaxElectionTimeout(timeout time.Duration) ServerCallOption {
	return func(opt *serverOptions) {
		opt.MaxElectionTimeout = timeout
	}
}

func WithMinElectionTimeout(timeout time.Duration) ServerCallOption {
	return func(opt *serverOptions) {
		opt.MinElectionTimeout = timeout
	}
}

func applyServerOptions(opts []ServerCallOption) *serverOptions {
	options := defaultServerOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &options
}

type ServerRole int

const (
	FOLLOWER ServerRole = iota
	CANDIDATE
	LEADER
)

func (s ServerRole) String() string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	default:
		logrus.Panicf("Unrecognized ServerRole: %d", int(s))	
	}

	return ""
}

type appendEntriesEvent struct {
	byLeader bool
}

type requestVoteEvent struct {
	voteGranted bool
}

type appendEntriesProcessResponse struct {
	Response *pb.AppendEntriesResponse
	Error    error
}

type appendEntriesProcessRequest struct {
	Request         *pb.AppendEntriesRequest
	ResponseChannel chan appendEntriesProcessResponse
}

type requestVoteProcessResponse struct {
	Response *pb.RequestVoteResponse
	Error    error
}

type requestVoteProcessRequest struct {
	Request         *pb.RequestVoteRequest
	ResponseChannel chan requestVoteProcessResponse
}

type Server struct {
	settings     *serverOptions
	cluster      ICluster
	state        IState
	stateMutex   sync.RWMutex
	logger       *logrus.Entry
	closeChannel chan struct{}
	closeOnce    sync.Once
	grpcServer   *grpc.Server

	appendEntriesHandlersChannel chan appendEntriesEvent
	requestVoteHandlersChannel   chan requestVoteEvent

	appendEntriesProcessChannel chan appendEntriesProcessRequest
	requestVoteProcessChannel   chan requestVoteProcessRequest
}

func NewServer(cluster ICluster, logger *logrus.Entry, state IState,
	opts ...ServerCallOption) *Server {
	server := &Server{
		settings:                     applyServerOptions(opts),
		cluster:                      cluster,
		logger:                       logger,
		closeChannel:                 make(chan struct{}),
		appendEntriesHandlersChannel: make(chan appendEntriesEvent),
		requestVoteHandlersChannel:   make(chan requestVoteEvent),
		appendEntriesProcessChannel:  make(chan appendEntriesProcessRequest),
		requestVoteProcessChannel:    make(chan requestVoteProcessRequest),
	}

	lis, err := net.Listen("tcp", server.settings.Endpoint)
	if err != nil {
		server.logger.Panicf("failed to listen: %+v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServer(grpcServer, server)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			server.logger.Panicf("failed to serve: %v", err)
		}
	}()

	server.grpcServer = grpcServer
	server.state = state
	return server
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	responseChannel := make(chan appendEntriesProcessResponse)

	s.appendEntriesProcessChannel <- appendEntriesProcessRequest{
		Request:         request,
		ResponseChannel: responseChannel,
	}

	response := <-responseChannel

	close(responseChannel)

	return response.Response, response.Error
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	responseChannel := make(chan requestVoteProcessResponse)

	s.requestVoteProcessChannel <- requestVoteProcessRequest{
		Request:         request,
		ResponseChannel: responseChannel,
	}

	response := <-responseChannel

	close(responseChannel)

	return response.Response, response.Error
}

func (s *Server) Run() {
	stateChangedChannel := make(chan StateSwitched)
	id := s.state.AddStateHandler(stateChangedChannel)
	defer func() {
		s.state.RemoveStateHandler(id)
		close(stateChangedChannel)
	}()

	go s.processAppendEntries()
	go s.processRequestVote()

	ctx, cancel := context.WithCancel(context.Background())
	doneChannel := s.HandleRoleFollower(ctx, stateChangedChannel)

	for {
		// Waiting for the definitive end of the previous state
		var newRole ServerRole
		select {
		case newRole = <-doneChannel:
			cancel()
		case <-s.closeChannel:
			cancel()
			return
		}

		s.logger.Debugf("handler for old role finished. New role: %+v", newRole)

		ctx, cancel = context.WithCancel(context.Background())
		switch newRole {
		case FOLLOWER:
			doneChannel = s.HandleRoleFollower(ctx, stateChangedChannel)
		case CANDIDATE:
			doneChannel = s.HandleRoleCandidate(ctx, stateChangedChannel)
		case LEADER:
			doneChannel = s.HandleRoleLeader(ctx, stateChangedChannel)
		default:
			s.logger.Panicf("unrecognized role: %+v", newRole)
		}
	}
}

func (s *Server) Close() {
	s.logger.Debugf("closing server")

	s.grpcServer.GracefulStop()

	s.closeOnce.Do(func() {
		close(s.closeChannel)

		// Closing process channels should be safe because we are always
		// sending internal messages before RPC responses (in RPC processing methods)
		// and we have already closed the server (i.e. no more RPCs)
		close(s.appendEntriesHandlersChannel)
		close(s.requestVoteHandlersChannel)

		// Closing process channels should be safe because we have
		// already closed the server (i.e. no more RPCs)
		close(s.appendEntriesProcessChannel)
		close(s.requestVoteProcessChannel)
	})
}

func (s *Server) HandleRoleFollower(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan ServerRole {
	doneChannel := make(chan ServerRole)

	go func() {
		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

		electionTimedChannel := make(chan struct{})
		go func() {
			select {
			case <-electionTimedChannel:
				s.stateMutex.Lock()

				s.logger.Debugf("election timed out, therefore switching to CANDIDATE role")

				newTerm := s.state.GetCurrentTerm() + 1
				votedFor := s.settings.ID
				s.state.SwitchState(newTerm, &votedFor, CANDIDATE)

				s.stateMutex.Unlock()
			case <-ctx.Done():
			}
		}()

		serverRole := FOLLOWER
	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == FOLLOWER {
					break
				}

				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case event := <-s.appendEntriesHandlersChannel:
				if event.byLeader {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				}
			case event := <-s.requestVoteHandlersChannel:
				if event.voteGranted {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				}
			case <-time.After(electionTimeout):
				close(electionTimedChannel)
			case <-ctx.Done():
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) HandleRoleCandidate(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan ServerRole {
	doneChannel := make(chan ServerRole)

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				doneChannel <- stateChangedEvent.newState.Role
				close(doneChannel)
				return
			default:
				break stateChangedLoop
			}
		}

		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

		innerCtx, cancel := context.WithTimeout(context.Background(), s.settings.MinElectionTimeout)
		go s.broadcastRequestVote(innerCtx)

		serverRole := CANDIDATE
	outerloop:
		for {
			select {
			case <-s.appendEntriesHandlersChannel:
			case <-s.requestVoteHandlersChannel:
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				cancel()
				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case <-time.After(electionTimeout):
				cancel()
				s.logger.Debugf("election timed out without a winner, broadcasting new vote")

				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				innerCtx, cancel = context.WithTimeout(context.Background(), s.settings.MinElectionTimeout)
				go s.broadcastRequestVote(innerCtx)
			case <-ctx.Done():
				cancel()
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) broadcastRequestVote(ctx context.Context) {
	s.stateMutex.RLock()
	request := &pb.RequestVoteRequest{
		Term:         s.state.GetCurrentTerm(),
		CandidateId:  s.settings.ID,
		LastLogIndex: s.state.GetLastLogIndex(),
		LastLogTerm:  s.state.GetLastLogTerm(),
	}
	s.stateMutex.RUnlock()

	responses := s.cluster.BroadcastRequestVoteRPCs(ctx, request)

	// Count of votes starts at one because the server always votes for itself
	countOfVotes := 1

	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// Check if the state hasn't been changed in the meantime
	if s.state.GetRole() != CANDIDATE {
		return
	}

	// Count the number of votes
	for _, response := range responses {
		// If the response contains higher term, we convert to follower
		if response.GetTerm() > s.state.GetCurrentTerm() {
			s.state.SwitchState(response.GetTerm(), nil, FOLLOWER)
			return
		}

		if response.GetVoteGranted() {
			countOfVotes++
		}
	}

	// Check if majority reached
	clusterState := s.cluster.GetClusterState()
	clusterMajority := int(math.Round(float64(clusterState.numberOfNodes) * 0.5))
	if countOfVotes > clusterMajority {
		s.state.SwitchState(s.state.GetCurrentTerm(), nil, LEADER)
	}
}

func (s *Server) HandleRoleLeader(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan ServerRole {
	doneChannel := make(chan ServerRole)

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because leader can only result from the CANDIDATE state handler
				doneChannel <- stateChangedEvent.newState.Role
				close(doneChannel)
				return
			default:
				break stateChangedLoop
			}
		}

		innerCtx, cancel := context.WithTimeout(context.Background(), s.settings.MinElectionTimeout)
		go s.broadcastHeartbeat(innerCtx)

		serverRole := LEADER
	outerloop:
		for {
			select {
			case <-s.appendEntriesHandlersChannel:
			case <-s.requestVoteHandlersChannel:
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because leader can only result from the CANDIDATE state handler
				cancel()
				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case <-time.After(s.settings.HeartbeatFrequency):
				cancel()

				s.logger.Debugf("sending new heartbeat")

				innerCtx, cancel = context.WithTimeout(context.Background(), s.settings.HeartbeatFrequency)
				go s.broadcastHeartbeat(innerCtx)
			case <-ctx.Done():
				cancel()
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) broadcastHeartbeat(ctx context.Context) {
	s.stateMutex.RLock()
	request := &pb.AppendEntriesRequest{
		Term:     s.state.GetCurrentTerm(),
		LeaderId: s.settings.ID,
		Entries:  make([]*pb.AppendEntriesRequest_Entry, 0),
	}
	s.stateMutex.RUnlock()

	responses := s.cluster.BroadcastAppendEntriesRPCs(ctx, request)

	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// Check if we are still the leader
	if s.state.GetRole() != LEADER {
		return
	}

	// Check if any of the responses contains higher term, 
	// in which case we have to convert to FOLLOWER state
	for _, response := range responses {
		if response.GetTerm() > s.state.GetCurrentTerm() {
			s.state.SwitchState(response.GetTerm(), nil, FOLLOWER)
			return
		}
	}
}

// TODO: Does it make sense to have these functions run in goroutine mode?
func (s *Server) processAppendEntries() {
	for {
		appendEntryToProcess, ok := <-s.appendEntriesProcessChannel
		if !ok {
			return
		}

		request := appendEntryToProcess.Request
		responseChannel := appendEntryToProcess.ResponseChannel

		logger := s.logger.WithFields(logrus.Fields{"RPC": "AppendEntries", "Sender": request.GetLeaderId()})

		logger.Debugf("received RPC: %+v", request)

		s.stateMutex.Lock()

		senderTerm := request.GetTerm()
		receiverTerm := s.state.GetCurrentTerm()

		responseFunc := func(sentByLeader bool, entriesAppended bool) {
			s.appendEntriesHandlersChannel <- appendEntriesEvent{
				byLeader: sentByLeader,
			}

			responseChannel <- appendEntriesProcessResponse{
				Response: &pb.AppendEntriesResponse{
					Term:    receiverTerm,
					Success: entriesAppended,
				},
				Error: nil,
			}
		}

		if senderTerm > receiverTerm {
			// The node has higher term than the node, so we switch to FOLLOWER
			logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
			s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
		} else if senderTerm < receiverTerm {
			// The candidate has lower term than the node, so deny the request
			logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)

			s.stateMutex.Unlock()

			responseFunc(false, false)
			break
		} else if s.state.GetRole() == CANDIDATE {
			logger.Debugf("switching state to follower because received request with an equal term from a leader")
			s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
		}

		// Set the leader
		err := s.cluster.SetLeader(request.GetLeaderId())
		if err != nil {
			s.logger.Errorf("unable to set leader: %+v", err)
		}

		s.stateMutex.Unlock()

		// TODO: Handle all other cases
		// TODO: Check if it is a heartbeat from the leader

		logger.Debugf("sending accept response")

		responseFunc(true, true)
	}
}

func (s *Server) processRequestVote() {
	for {
		requestVoteToProcess, ok := <-s.requestVoteProcessChannel
		if !ok {
			return
		}

		request := requestVoteToProcess.Request
		responseChannel := requestVoteToProcess.ResponseChannel

		candidateID := request.GetCandidateId()

		logger := s.logger.WithFields(logrus.Fields{"RPC": "RequestVote", "Sender": candidateID})

		logger.Debugf("received RPC: %+v", request)

		s.stateMutex.Lock()

		senderTerm := request.GetTerm()
		receiverTerm := s.state.GetCurrentTerm()

		responseFunc := func(voteGranted bool) {
			s.requestVoteHandlersChannel <- requestVoteEvent{
				voteGranted: voteGranted,
			}

			responseChannel <- requestVoteProcessResponse{
				Response: &pb.RequestVoteResponse{
					Term:        receiverTerm,
					VoteGranted: voteGranted,
				},
				Error: nil,
			}
		}

		if senderTerm > receiverTerm {
			// Sender node has higher term than receiver node, so we switch to FOLLOWER
			logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
			s.state.SwitchState(senderTerm, nil, FOLLOWER)
		} else if senderTerm < receiverTerm {
			// The candidate has lower term than the node, so deny the request
			logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)

			s.stateMutex.Unlock()

			responseFunc(false)
			break
		}

		// The node has already voted
		if s.state.GetVotedFor() != nil && *s.state.GetVotedFor() != request.GetCandidateId() {
			logger.Debugf("sending reject response. Already voted for: %s", *s.state.GetVotedFor())

			s.stateMutex.Unlock()

			responseFunc(false)
			break
		}

		// TODO: Handle case: "and candidate's log is at least as up to date as receivers log"

		logger.Debugf("voting for sender")

		s.state.SwitchState(senderTerm, &candidateID, FOLLOWER)

		s.stateMutex.Unlock()

		responseFunc(true)
	}
}

func newElectionTimeout(min time.Duration, max time.Duration) time.Duration {
	electionTimeoutDiff := max - min
	electionTimeoutRand := rand.Int63n(electionTimeoutDiff.Nanoseconds())
	electionTimeoutRandNano := time.Duration(electionTimeoutRand) * time.Nanosecond
	electionTimeout := max + electionTimeoutRandNano

	return electionTimeout
}
