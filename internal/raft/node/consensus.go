package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	"math/rand"
	pb "raft/internal/raft/node/gen"
	"sync"
	"time"
)

type ServerRole int

type Settings struct {
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

type State struct {
	CurrentTerm int64
	VotedFor    *int64
	Log         []LogEntry
	Role        ServerRole

	stateMutex sync.RWMutex
}

func (s *State) GetCurrentTerm() int64 {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.CurrentTerm
}

func (s *State) GetVotedFor() *int64 {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.VotedFor
}

func (s *State) GetRole() ServerRole {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return s.Role
}

func (s *State) GetLastLogIndex() int64 {
	return int64(len(s.Log) + 1)
}

func (s *State) GetLastLogTerm() int64 {
	return s.Log[len(s.Log)-1].Term
}

func (s *State) SwitchState(term int64, votedFor *int64, role ServerRole) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	s.CurrentTerm = term
	s.VotedFor = votedFor
	s.Role = role
}

type appendEntriesEvent struct {
	term int64
}

type requestVoteEvent struct {
	term        int64
	voteGranted bool
}

type Server struct {
	settings Settings
	cluster  ICluster
	state    State

	appendEntriesChannel chan appendEntriesEvent
	requestVotesChannel  chan requestVoteEvent
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rejectResponse := &pb.RequestVoteResponse{
		Term:        s.state.CurrentTerm,
		VoteGranted: false,
	}

	// The candidate has lower term than the node
	if request.GetTerm() < s.state.GetCurrentTerm() {
		return rejectResponse, nil
	}

	// The node has already voted
	if s.state.GetVotedFor() != nil {
		return rejectResponse, nil
	}

	// TODO
	if request.GetLastLogIndex() < s.state.GetLastLogIndex() ||
		request.GetLastLogTerm() < s.state.GetLastLogTerm() {
		return rejectResponse, nil
	}

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
	s.state.SwitchState(1, nil, FOLLOWER)

	ctx, cancel := context.WithCancel(context.Background())
	doneChannel := s.HandleStateFollower(ctx)

	for {
		// Waiting for the definitive end of the previous state
		<-doneChannel
		cancel()

		// Setting up the new state
		newRole := s.state.GetRole()

		ctx, cancel = context.WithCancel(context.Background())
		switch newRole {
		case FOLLOWER:
			doneChannel = s.HandleStateFollower(ctx)
		case CANDIDATE:
			doneChannel = s.HandleStateCandidate(ctx)
		case LEADER:
			doneChannel = s.HandleStateLeader(ctx)
		}
	}
}

func (s *Server) HandleStateFollower(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)

	outerloop:
		for {
			select {
			case event := <-s.appendEntriesChannel:
				// TODO: this should concern only when AppendEntries is sent by the leader!
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
			case event := <-s.requestVotesChannel:
				if event.voteGranted {
					electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				}
			case <-time.After(electionTimeout):
				newTerm := s.state.GetCurrentTerm() + 1
				votedFor := int64(0) // TODO
				s.state.SwitchState(newTerm, &votedFor, CANDIDATE)

				break outerloop
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) HandleStateCandidate(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {

		electionTimeout := newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
		isLeaderChannel := s.SendRequestVoteRPCs()

	outerloop:
		for {
			select {
			case <-isLeaderChannel:
				newTerm := s.state.GetCurrentTerm() + 1
				s.state.SwitchState(newTerm, nil, LEADER)
				break outerloop
			case event := <-s.appendEntriesChannel:
				if event.term >= s.state.GetCurrentTerm() {
					s.state.SwitchState(event.term, nil, FOLLOWER)
				}
			case event := <-s.requestVotesChannel:
				if event.term >= s.state.GetCurrentTerm() {
					s.state.SwitchState(event.term, nil, FOLLOWER)
				}
				break
			case <-time.After(electionTimeout):
				electionTimeout = newElectionTimeout(s.settings.MinElectionTimeout, s.settings.MaxElectionTimeout)
				isLeaderChannel = s.SendRequestVoteRPCs()
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func (s *Server) HandleStateLeader(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		err := s.SendHeartbeat()

	outerloop:
		for {
			select {
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
