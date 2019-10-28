package node

import (
	"github.com/google/uuid"
	logrus "github.com/sirupsen/logrus"
	"raft/internal/core/persister"
)

type StateInfo struct {
	CurrentTerm uint64
	VotedFor    *string
	Role        ServerRole
}

type StateSwitched struct {
	oldState StateInfo
	newState StateInfo
}

type IStateManager interface {
	AddStateObserver(handler chan StateSwitched) string
	RemoveStateObserver(id string)

	GetCurrentTerm() uint64
	GetVotedFor() *string
	GetRole() ServerRole
	SwitchState(term uint64, votedFor *string, role ServerRole)
}

type StateManager struct {
	info StateInfo

	statePersister persister.IStateLogger

	handlers map[string]chan StateSwitched
}

// StateManager is not safe for concurrent usage, as it is expected to be used
// mainly with transaction operations. Therefore, clients using StateManager should
// implement appropriate locking mechanisms
func NewStateManager(stateInfo StateInfo, statePersister persister.IStateLogger) *StateManager {
	statePersister.UpdateState(&persister.State{
		VotedFor:    stateInfo.VotedFor,
		CurrentTerm: stateInfo.CurrentTerm,
	})

	return &StateManager{
		info:           stateInfo,
		statePersister: statePersister,
		handlers:       make(map[string]chan StateSwitched),
	}
}

func (s *StateManager) AddStateObserver(handler chan StateSwitched) string {
	id, err := uuid.NewUUID()
	if err != nil {
		logrus.Panicf("unable to create new uuid: %+v", err)
	}

	idStr := id.String()

	s.handlers[idStr] = handler

	return idStr
}

func (s *StateManager) RemoveStateObserver(id string) {
	delete(s.handlers, id)
}

func (s *StateManager) GetCurrentTerm() uint64 {
	return s.statePersister.GetState().CurrentTerm
}

func (s *StateManager) GetVotedFor() *string {
	return s.statePersister.GetState().VotedFor
}

func (s *StateManager) GetRole() ServerRole {
	return s.info.Role
}

func (s *StateManager) SwitchState(term uint64, votedFor *string, role ServerRole) {
	oldInfo := s.info

	s.statePersister.UpdateState(&persister.State{
		VotedFor:    votedFor,
		CurrentTerm: term,
	})

	s.info.Role = role

	for _, handler := range s.handlers {
		handler <- StateSwitched{
			oldState: oldInfo,
			newState: s.info,
		}
	}
}
