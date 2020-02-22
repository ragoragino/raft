package node

import (
	"fmt"
	"raft/internal/core/persister"

	"github.com/google/uuid"
)

type PersistentStateInfo struct {
	CurrentTerm uint64
	VotedFor    *string
	Role        RaftRole
}

type VolatileStateInfo struct {
	CommitIndex uint64
	MatchIndex  map[string]uint64
}

type StateSwitched struct {
	oldState PersistentStateInfo
	newState PersistentStateInfo
}

// IStateManager provides interface for managing Raft state variables
type IStateManager interface {
	AddPersistentStateObserver(handler chan StateSwitched) string
	RemovePersistentStateObserver(id string)

	GetCurrentTerm() uint64
	GetVotedFor() *string
	GetRole() RaftRole
	SwitchPersistentState(term uint64, votedFor *string, role RaftRole)

	SetCommitIndex(uint64)
	GetCommitIndex() uint64
	GetMatchIndexes() map[string]uint64
	SetMatchIndex(nodeID string, index uint64)
}

type StateManager struct {
	persistentInfo PersistentStateInfo
	volatileInfo   VolatileStateInfo

	statePersister persister.IStateLogger

	handlers map[string]chan StateSwitched
}

// StateManager is not safe for concurrent usage, as it is expected to be used
// mainly with transaction operations. Therefore, clients using StateManager should
// implement appropriate locking mechanisms
func NewStateManager(stateInfo PersistentStateInfo, volatileInfo VolatileStateInfo, statePersister persister.IStateLogger) *StateManager {
	statePersister.UpdateState(&persister.State{
		VotedFor:    stateInfo.VotedFor,
		CurrentTerm: stateInfo.CurrentTerm,
	})

	return &StateManager{
		persistentInfo: stateInfo,
		volatileInfo:   volatileInfo,
		statePersister: statePersister,
		handlers:       make(map[string]chan StateSwitched),
	}
}

func (s *StateManager) AddPersistentStateObserver(handler chan StateSwitched) string {
	id, err := uuid.NewUUID()
	if err != nil {
		panic(fmt.Sprintf("unable to create new uuid: %+v", err))
	}

	idStr := id.String()

	s.handlers[idStr] = handler

	return idStr
}

func (s *StateManager) RemovePersistentStateObserver(id string) {
	delete(s.handlers, id)
}

func (s *StateManager) GetCurrentTerm() uint64 {
	return s.statePersister.GetState().CurrentTerm
}

func (s *StateManager) GetVotedFor() *string {
	return s.statePersister.GetState().VotedFor
}

func (s *StateManager) GetRole() RaftRole {
	return s.persistentInfo.Role
}

func (s *StateManager) SwitchPersistentState(term uint64, votedFor *string, role RaftRole) {
	oldInfo := s.persistentInfo

	s.statePersister.UpdateState(&persister.State{
		VotedFor:    votedFor,
		CurrentTerm: term,
	})

	s.persistentInfo.Role = role

	for _, handler := range s.handlers {
		handler <- StateSwitched{
			oldState: oldInfo,
			newState: s.persistentInfo,
		}
	}
}

func (s *StateManager) SetCommitIndex(commitIndex uint64) {
	s.volatileInfo.CommitIndex = commitIndex
}

func (s *StateManager) GetCommitIndex() uint64 {
	return s.volatileInfo.CommitIndex
}

func (s *StateManager) GetMatchIndexes() map[string]uint64 {
	return s.volatileInfo.MatchIndex
}

func (s *StateManager) SetMatchIndex(nodeID string, index uint64) {
	s.volatileInfo.MatchIndex[nodeID] = index
}
