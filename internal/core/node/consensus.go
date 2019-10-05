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

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type ServerRole int

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

const (
	FOLLOWER ServerRole = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command string
	Term    int64
}

type StateInfo struct {
	CurrentTerm int64
	VotedFor    *string
	Role        ServerRole
}

type StateSwitched struct {
	oldState StateInfo
	newState StateInfo
}

type State struct {
	info StateInfo
	Log  []LogEntry

	handlers map[string]chan StateSwitched
}

func NewState(currentTerm int64, votedFor *string, role ServerRole) *State {
	return &State{
		info: StateInfo{
			CurrentTerm: currentTerm,
			VotedFor:    votedFor,
			Role:        role,
		},
		handlers: make(map[string]chan StateSwitched),
	}
}

func (s *State) AddStateHandler(handler chan StateSwitched) string {
	id, err := uuid.NewUUID()
	if err != nil {
		logrus.Panicf("unable to create new uuid: %+v", err)
	}

	idStr := id.String()

	s.handlers[idStr] = handler

	return idStr
}

func (s *State) RemoveStateHandler(id string) {
	delete(s.handlers, id)
}

func (s *State) GetCurrentTerm() int64 {
	return s.info.CurrentTerm
}

func (s *State) GetVotedFor() *string {
	return s.info.VotedFor
}

func (s *State) GetRole() ServerRole {
	return s.info.Role
}

func (s *State) GetLastLogIndex() int64 {
	// TODO
	return int64(len(s.Log) + 1)
}

func (s *State) GetLastLogTerm() int64 {
	logLength := len(s.Log)
	if logLength == 0 {
		// TODO
		return 1
	}

	return s.Log[len(s.Log)-1].Term
}

func (s *State) SwitchState(term int64, votedFor *string, role ServerRole) {
	oldInfo := s.info

	s.info.CurrentTerm = term
	s.info.VotedFor = votedFor
	s.info.Role = role

	for _, handler := range s.handlers {
		handler <- StateSwitched{
			oldState: oldInfo,
			newState: s.info,
		}
	}
}

type appendEntriesEvent struct {
}

type requestVoteEvent struct {
	voteGranted bool
}

type Server struct {
	settings   *serverOptions
	cluster    ICluster
	state      *State
	stateMutex sync.RWMutex
	logger     *logrus.Entry

	appendEntriesChannel chan appendEntriesEvent
	requestVotesChannel  chan requestVoteEvent
}

func NewServer(cluster ICluster, logger *logrus.Entry, opts ...ServerCallOption) *Server {
	server := &Server{
		settings:             applyServerOptions(opts),
		cluster:              cluster,
		logger:               logger,
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

	return server
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	logger := s.logger.WithFields(logrus.Fields{"RPC": "AppendEntries", "Sender": request.GetLeaderId()})

	logger.Debugf("received RPC: %+v", request)

	defer func() {
		s.appendEntriesChannel <- appendEntriesEvent{}
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

	// TODO: Handle all other cases

	if len(request.Entries) == 0 {
		err := s.cluster.SetLeader(request.GetLeaderId())
		if err != nil {
			logger.Errorf("unable to set leader: %+v", err)
		}
	}

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
	if s.state.GetVotedFor() != nil {
		logger.Debugf("sending reject response. voted for: %s", *s.state.GetVotedFor())

		return &pb.RequestVoteResponse{
			Term:        receiverTerm,
			VoteGranted: voteGranted,
		}, nil
	}

	// TODO: Handle all other cases

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
	s.state = NewState(1, nil, FOLLOWER)

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
		<-doneChannel
		cancel()

		s.stateMutex.RLock()
		newRole := s.state.GetRole()
		s.stateMutex.RUnlock()

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

func (s *Server) HandleRoleFollower(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

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

	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == FOLLOWER {
					break
				}

				break outerloop
			case <-s.appendEntriesChannel:
				// TODO: this should concern only when AppendEntries is sent by the leader!
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
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

		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) HandleRoleCandidate(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

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

	outerloop:
		for {
			select {
			case <-s.appendEntriesChannel:
			case <-s.requestVotesChannel:
			case <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				cancel()
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

	// Check if the context hasn't been cancelled
	select {
	case <-ctx.Done():
		return
	default:
	}

	// Count of votes starts at one because the server always votes for itself
	countOfVotes := 1
	s.stateMutex.RLock()
	for _, response := range responses {
		// If the response contains higher term, we convert to follower
		if response.GetTerm() > s.state.GetCurrentTerm() {
			s.stateMutex.RUnlock()

			s.stateMutex.Lock()
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
	s.stateMutex.RUnlock()

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

func (s *Server) HandleRoleLeader(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case <-s.appendEntriesChannel:
			case <-s.requestVotesChannel:
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

	outerloop:
		for {
			select {
			case <-stateChangedChannel:
				// No need to check what is the new state
				// because leader can only result from the CANDIDATE state handler
				cancel()
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

	// Check if the context hasn't been cancelled
	select {
	case <-ctx.Done():
		return
	default:
	}

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
