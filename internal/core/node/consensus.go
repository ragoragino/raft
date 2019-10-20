package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	"fmt"
	logrus "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"net"
	"net/http"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
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
		return fmt.Sprintf("%d", int(s))
	}
}

type appendEntriesEvent struct {
	byLeader bool
}

type requestVoteEvent struct {
	voteGranted bool
}

type Server struct {
	settings     *serverOptions
	cluster      ICluster
	state        *State
	stateMutex   sync.RWMutex
	logger       *logrus.Entry
	closeChannel chan struct{}
	closeOnce    sync.Once
	grpcServer   *grpc.Server

	appendEntriesChannel chan appendEntriesEvent
	requestVotesChannel  chan requestVoteEvent
}

func NewServer(cluster ICluster, logger *logrus.Entry, statePersister persister.IStateLogger,
	opts ...ServerCallOption) *Server {
	server := &Server{
		settings:             applyServerOptions(opts),
		cluster:              cluster,
		logger:               logger,
		closeChannel:         make(chan struct{}),
		appendEntriesChannel: make(chan appendEntriesEvent),
		requestVotesChannel:  make(chan requestVoteEvent),
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

	server.state = NewState(StateInfo{
		CurrentTerm: 0,
		VotedFor:    nil,
		Role:        FOLLOWER,
	}, statePersister)

	return server
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	logger := s.logger.WithFields(logrus.Fields{"RPC": "AppendEntries", "Sender": request.GetLeaderId()})

	logger.Debugf("received RPC: %+v", request)

	byLeader := true
	defer func() {
		s.appendEntriesChannel <- appendEntriesEvent{
			byLeader: byLeader,
		}
	}()

	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	senderTerm := request.GetTerm()
	receiverTerm := s.state.GetCurrentTerm()

	if senderTerm > receiverTerm {
		// The node has higher term than the node, so we switch to FOLLOWER
		logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
		s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
	} else if senderTerm < receiverTerm {
		byLeader = false

		// The candidate has lower term than the node, so deny the request
		logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
		return &pb.AppendEntriesResponse{
			Term:    receiverTerm,
			Success: false,
		}, nil
	} else {
		if s.state.GetRole() == CANDIDATE {
			logger.Debugf("switching state to follower because received request with an equal term from a leader")
			s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
		}
	}

	// Set the leader
	err := s.cluster.SetLeader(request.GetLeaderId())
	if err != nil {
		logger.Errorf("unable to set leader: %+v", err)
	}

	// TODO: Handle all other cases
	// TODO: Check if it is a heartbeat from the leader

	logger.Debugf("sending accept response")

	return &pb.AppendEntriesResponse{
		Term:    receiverTerm,
		Success: true,
	}, nil
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	logger := s.logger.WithFields(logrus.Fields{"RPC": "RequestVote", "Sender": request.GetCandidateId()})

	logger.Debugf("received RPC: %+v", request)

	voteGranted := false
	defer func() {
		s.requestVotesChannel <- requestVoteEvent{
			voteGranted: voteGranted,
		}
	}()

	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	senderTerm := request.GetTerm()
	receiverTerm := s.state.GetCurrentTerm()

	if senderTerm > receiverTerm {
		// The node has higher term than the node, so we switch to FOLLOWER
		logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
		s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
	} else if senderTerm < receiverTerm {
		// The candidate has lower term than the node, so deny the request
		logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
		return &pb.RequestVoteResponse{
			Term:        receiverTerm,
			VoteGranted: voteGranted,
		}, nil
	}

	// The node has already voted
	if s.state.GetVotedFor() != nil && *s.state.GetVotedFor() != request.GetCandidateId() {
		logger.Debugf("sending reject response. Already voted for: %s", *s.state.GetVotedFor())

		return &pb.RequestVoteResponse{
			Term:        receiverTerm,
			VoteGranted: voteGranted,
		}, nil
	}

	// TODO: Handle case: "and candidate's log is at least as up to date as receivers log"

	logger.Debugf("voting for sender")

	voteGranted = true

	return &pb.RequestVoteResponse{
		Term:        receiverTerm,
		VoteGranted: voteGranted,
	}, nil
}

func (s *Server) Insert(ctx context.Context, in *pb.InsertRequest) (*pb.InsertResponse, error) {
	s.stateMutex.RLock()
	if s.state.GetRole() != LEADER {
		s.stateMutex.RUnlock()
		clusterState := s.cluster.GetClusterState()

		return &pb.InsertResponse{
			Success: false,
			Leader:  clusterState.leaderEndpoint,
		}, nil
	}
	s.stateMutex.RUnlock()

	// TODO: Implement the case when the node is the leader

	return &pb.InsertResponse{
		Success: true,
	}, nil
}

func (s *Server) Find(ctx context.Context, in *pb.FindRequest) (*pb.FindResponse, error) {
	s.stateMutex.RLock()
	if s.state.GetRole() != LEADER {
		s.stateMutex.RUnlock()
		clusterState := s.cluster.GetClusterState()

		return &pb.FindResponse{
			Success: false,
			Leader:  clusterState.leaderEndpoint,
		}, nil
	}
	s.stateMutex.RUnlock()

	// TODO: Implement the case when the node is the leader

	return &pb.FindResponse{
		Success: true,
	}, nil
}

func (s *Server) Run() {
	stateChangedChannel := make(chan StateSwitched)
	id := s.state.AddStateHandler(stateChangedChannel)
	defer func() {
		s.state.RemoveStateHandler(id)
		close(stateChangedChannel)
	}()

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
			case event := <-s.appendEntriesChannel:
				if event.byLeader {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				}
			case event := <-s.requestVotesChannel:
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
			case <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
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
			case <-s.appendEntriesChannel:
			case <-s.requestVotesChannel:
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				cancel()
				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case <-time.After(electionTimeout):
				cancel()
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

				s.logger.Debugf("election timed out without a winner, broadcasting new vote")

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
	for _, response := range responses {
		// If the response contains higher term, we convert to follower
		if response.GetTerm() > s.state.GetCurrentTerm() {
			if s.state.GetRole() != CANDIDATE {
				s.stateMutex.Unlock()
				return
			}
			s.state.SwitchState(response.GetTerm(), nil, FOLLOWER)
			s.stateMutex.Unlock()
			return
		}

		if response.GetVoteGranted() {
			countOfVotes++
		}
	}
	s.stateMutex.Unlock()

	clusterState := s.cluster.GetClusterState()
	clusterMajority := int(math.Round(float64(clusterState.numberOfNodes) * 0.5))
	if countOfVotes > clusterMajority {
		s.stateMutex.Lock()
		if s.state.GetRole() != CANDIDATE {
			s.stateMutex.Unlock()
			return
		}
		s.state.SwitchState(s.state.GetCurrentTerm(), nil, LEADER)
		s.stateMutex.Unlock()
	}
}

func (s *Server) HandleRoleLeader(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan ServerRole {
	doneChannel := make(chan ServerRole)

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case <-stateChangedChannel:
				// No need to check what is the new state
				// because leader can only result from the CANDIDATE state handler
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
			case <-s.appendEntriesChannel:
			case <-s.requestVotesChannel:
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

	// Check if the response contains higher term, so that we convert to follower
	s.stateMutex.Lock()
	if s.state.GetRole() != LEADER {
		s.stateMutex.Unlock()
		return
	}

	for _, response := range responses {
		if response.GetTerm() > s.state.GetCurrentTerm() {
			s.state.SwitchState(response.GetTerm(), nil, FOLLOWER)
			s.stateMutex.Unlock()
			return
		}
	}
	s.stateMutex.Unlock()
}

func newElectionTimeout(min time.Duration, max time.Duration) time.Duration {
	electionTimeoutDiff := max - min
	electionTimeoutRand := rand.Int63n(electionTimeoutDiff.Nanoseconds())
	electionTimeoutRandNano := time.Duration(electionTimeoutRand) * time.Nanosecond
	electionTimeout := max + electionTimeoutRandNano

	return electionTimeout
}
