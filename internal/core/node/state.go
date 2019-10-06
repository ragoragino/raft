package node

import (
	"github.com/google/uuid"
	logrus "github.com/sirupsen/logrus"
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
