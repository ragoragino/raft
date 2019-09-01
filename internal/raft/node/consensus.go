package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	pb "raft/internal/raft/node/gen"
	"sync"
)

type ServerRole int

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
	currentTerm int64
	votedFor    *int64
	log         []LogEntry
}

func (s *State) GetCurrentTerm() int64 {
	return s.currentTerm
}

func (s *State) GetVotedFor() *int64 {
	return s.votedFor
}

func (s *State) GetLastLogIndex() int64 {
	return int64(len(s.log) + 1)
}

func (s *State) GetLastLogTerm() int64 {
	return s.log[len(s.log)-1].Term
}

type Server struct {
	cluster     ICluster
	state       State
	roleMutex   sync.RWMutex
	role        ServerRole
	roleChannel chan ServerRole
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rejectResponse := &pb.RequestVoteResponse{
		Term:        s.state.currentTerm,
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
	s.roleMutex.RLock()
	if s.role != LEADER {
		s.roleMutex.RUnlock()

		leaderIP := s.cluster.GetLeaderIP()

		return &pb.InsertResponse{
			Success: false,
			Leader:  leaderIP,
		}, nil
	}
	s.roleMutex.RUnlock()

	// TODO: Implement the case when the node is the leader

	return &pb.InsertResponse{
		Success: true,
	}, nil
}

func (s *Server) Find(ctx context.Context, in *pb.FindRequest) (*pb.FindResponse, error) {
	s.roleMutex.RLock()
	if s.role != LEADER {
		s.roleMutex.RUnlock()

		leaderIP := s.cluster.GetLeaderIP()

		return &pb.FindResponse{
			Success: false,
			Leader:  leaderIP,
		}, nil
	}
	s.roleMutex.RUnlock()

	// TODO: Implement the case when the node is the leader

	return &pb.FindResponse{
		Success: true,
	}, nil
}

func (s *Server) Run() {
	doneChannel := make(chan struct{})

	// Starting server as a follower
	s.roleMutex.Lock()
	s.role = FOLLOWER
	s.roleMutex.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	s.HandleStateFollower(ctx, doneChannel)

	for {
		// Waiting for the change of state
		newRole := <-s.roleChannel

		// NOTE: no need for mutex here, as only this method can modify s.role
		if s.role == newRole {
			continue
		}

		// Killing the previous state
		cancel()
		<-doneChannel

		// Setting up the new state
		s.roleMutex.Lock()
		s.role = newRole
		s.roleMutex.Unlock()

		ctx, cancel = context.WithCancel(context.Background())
		switch newRole {
		case FOLLOWER:
			s.HandleStateFollower(ctx, doneChannel)
		case CANDIDATE:
			s.HandleStateCandidate(ctx, doneChannel)
		case LEADER:
			s.HandleStateLeader(ctx, doneChannel)
		}
	}
}
