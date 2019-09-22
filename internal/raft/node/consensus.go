package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	"log"
	"math/rand"
	pb "raft/internal/raft/node/gen"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ServerRole int

type Settings struct {
	HeartbeatFrequency time.Duration
	MaxElectionTimeout time.Duration
	MinElectionTimeout time.Duration
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

type stateInfo struct {
	CurrentTerm int64
	VotedFor    *int64
	Role        ServerRole
}

type StateSwitched struct {
	oldState stateInfo
	newState stateInfo
}

type State struct {
	info stateInfo
	Log  []LogEntry

	handlers map[string]chan StateSwitched
}

func NewState(currentTerm int64, votedFor *int64, role ServerRole) *State {
	return &State{
		info: stateInfo{
			CurrentTerm: currentTerm,
			VotedFor:    votedFor,
			Role:        role,
		},
	}
}

func (s *State) AddStateHandler(handler chan StateSwitched) string {
	id, err := uuid.NewUUID()
	if err != nil {
		log.Panicf("unable to create new uuid: %+v", err)
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

func (s *State) GetVotedFor() *int64 {
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
	return s.Log[len(s.Log)-1].Term
}

func (s *State) SwitchState(term int64, votedFor *int64, role ServerRole) {
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
	term int64
}

type requestVoteEvent struct {
	term        int64
	voteGranted bool
}

type Server struct {
	ID         int64
	settings   Settings
	cluster    ICluster
	client     pb.NodeClient
	state      *State
	stateMutex sync.RWMutex

	appendEntriesChannel chan appendEntriesEvent
	requestVotesChannel  chan requestVoteEvent
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// TODO
	return &pb.AppendEntriesResponse{}, nil
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rejectResponse := &pb.RequestVoteResponse{
		Term:        s.state.GetCurrentTerm(),
		VoteGranted: false,
	}

	s.stateMutex.Lock()

	if request.GetTerm() > s.state.GetCurrentTerm() {
		// The candidate has higher term than the node
		s.state.SwitchState(request.GetTerm(), nil, FOLLOWER)
	} else if request.GetTerm() < s.state.GetCurrentTerm() {
		// The candidate has lower term than the node
		s.stateMutex.Unlock()
		return rejectResponse, nil
	}

	// The node has already voted
	if s.state.GetVotedFor() != nil {
		s.stateMutex.Unlock()
		return rejectResponse, nil
	}

	// TODO
	if request.GetLastLogIndex() < s.state.GetLastLogIndex() ||
		request.GetLastLogTerm() < s.state.GetLastLogTerm() {
		s.stateMutex.Unlock()
		return rejectResponse, nil
	}
	s.stateMutex.Unlock()

	return &pb.RequestVoteResponse{
		VoteGranted: true,
	}, nil
}

func (s *Server) Insert(ctx context.Context, in *pb.InsertRequest) (*pb.InsertResponse, error) {
	if s.state.GetRole() != LEADER {
		leaderIP := s.cluster.GetLeaderIP()

		return &pb.InsertResponse{
			Success: false,
			Leader:  leaderIP,
		}, nil
	}

	// TODO: Implement the case when the node is the leader

	return &pb.InsertResponse{
		Success: true,
	}, nil
}

func (s *Server) Find(ctx context.Context, in *pb.FindRequest) (*pb.FindResponse, error) {
	if s.state.GetRole() != LEADER {

		leaderIP := s.cluster.GetLeaderIP()

		return &pb.FindResponse{
			Success: false,
			Leader:  leaderIP,
		}, nil
	}

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
	doneChannel := s.HandleStateFollower(ctx, stateChangedChannel)

	for {
		// Waiting for the definitive end of the previous state
		<-doneChannel
		cancel()

		// Setting up new state
		newRole := s.state.GetRole()

		ctx, cancel = context.WithCancel(context.Background())
		switch newRole {
		case FOLLOWER:
			doneChannel = s.HandleStateFollower(ctx, stateChangedChannel)
		case CANDIDATE:
			doneChannel = s.HandleStateCandidate(ctx, stateChangedChannel)
		case LEADER:
			doneChannel = s.HandleStateLeader(ctx, stateChangedChannel)
		default:
			log.Panicf("unrecognized role: %+v", newRole)
		}
	}
}

func (s *Server) HandleStateFollower(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == FOLLOWER {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
					break
				}

				break outerloop
			case event := <-s.appendEntriesChannel:
				// TODO: this should concern only when AppendEntries is sent by the leader!
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
			case event := <-s.requestVotesChannel:
				if event.voteGranted {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				}
			case <-time.After(electionTimeout):
				s.stateMutex.Lock()

				newTerm := s.state.GetCurrentTerm() + 1
				votedFor := int64(s.ID)
				s.state.SwitchState(newTerm, &votedFor, CANDIDATE)

				s.stateMutex.Unlock()

				break outerloop
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) HandleStateCandidate(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				close(doneChannel)
				return
			default:
				break stateChangedLoop
			}
		}

		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

		cancel := s.broadcastRequestVote()

	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because candidate can only result from the FOLLOWER state handler
				cancel()
				break outerloop
			case event := <-s.appendEntriesChannel:
				s.stateMutex.Lock()
				if event.term >= s.state.GetCurrentTerm() {
					s.state.SwitchState(event.term, nil, FOLLOWER)
					s.stateMutex.Unlock()
					break outerloop
				}
				s.stateMutex.Unlock()
			case event := <-s.requestVotesChannel:
				s.stateMutex.Lock()
				if event.term > s.state.GetCurrentTerm() {
					s.stateMutex.Unlock()
					s.state.SwitchState(event.term, nil, FOLLOWER)
					break outerloop
				}
				s.stateMutex.Unlock()
			case <-time.After(electionTimeout):
				cancel()
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

				cancel = s.broadcastRequestVote()
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) broadcastRequestVote() func() {
	ctx, cancel := context.WithCancel(context.Background())

	s.stateMutex.RLock()
	request := &pb.RequestVoteRequest{
		Term:         s.state.GetCurrentTerm(),
		CandidateId:  s.ID,
		LastLogIndex: s.state.GetLastLogIndex(),
		LastLogTerm:  s.state.GetLastLogTerm(),
	}
	s.stateMutex.RUnlock()

	voteSuccessful := s.cluster.BroadcastRequestVoteRPCs(ctx, request)

	go func() {
		select {
		case <-voteSuccessful:
			s.stateMutex.Lock()
			s.state.SwitchState(s.state.GetCurrentTerm(), nil, LEADER)
			s.stateMutex.Unlock()
			cancel()
		case <-ctx.Done():
		}
	}()

	return cancel
}

func (s *Server) HandleStateLeader(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.cluster.BroadcastHeartbeat(ctx)

	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				// No need to check what is the new state
				// because leader can only result from the CANDIDATE state handler
				cancel()
				break outerloop
			case <-time.After(s.settings.HeartbeatFrequency):
				cancel()
				ctx, cancel = context.WithCancel(context.Background())
				s.cluster.BroadcastHeartbeat(ctx)
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func newElectionTimeout(min time.Duration, max time.Duration) time.Duration {
	electionTimeoutDiff := max - min
	electionTimeoutRand := rand.Int63n(electionTimeoutDiff.Nanoseconds())
	electionTimeoutRandNano := time.Duration(electionTimeoutRand) * time.Nanosecond
	electionTimeout := max + electionTimeoutRandNano

	return electionTimeout
}
