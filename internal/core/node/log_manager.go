package node

import (
	"fmt"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"

	"github.com/golang/protobuf/proto"
)

// IStateManager provides interface for managing Raft persistent logs
type ILogEntryManager interface {
	GetLastLogIndex() uint64
	GetLastLogTerm() uint64
	FindTermAtIndex(index uint64) (uint64, error)
	FindEntryAtIndex(index uint64) (*pb.AppendEntriesRequest_Entry, error)
	AppendEntries(entries []*pb.AppendEntriesRequest_Entry) error
	ReplayEntries() (persister.ILogEntryPersisterIterator, error)

	// GetEntriesBetweenIndexes gets all objects in the range (startIndex, endIndex]
	GetEntriesBetweenIndexes(startIndex uint64, endIndex uint64) ([]*pb.AppendEntriesRequest_Entry, error)

	// DeleteLogsAferIndex deletes all entries after given index (including also the index)
	DeleteLogsAferIndex(index uint64) error
}

// LogEntryManager is thread-safe
type LogEntryManager struct {
	entryPersister persister.ILogEntryPersister
}

func NewLogEntryManager(entryPersister persister.ILogEntryPersister) *LogEntryManager {
	return &LogEntryManager{
		entryPersister: entryPersister,
	}
}

func (l *LogEntryManager) GetLastLogIndex() uint64 {
	log := l.entryPersister.GetLastLog()
	if log == nil {
		return startingLogIndex - 1
	}

	return log.Index
}

func (l *LogEntryManager) GetLastLogTerm() uint64 {
	log := l.entryPersister.GetLastLog()
	if log == nil {
		return startingTerm
	}

	return log.Term
}

func (l *LogEntryManager) FindTermAtIndex(index uint64) (uint64, error) {
	commandLog, err := l.entryPersister.FindLogByIndex(index)
	if err != nil {
		return 0, err
	}

	return commandLog.Term, nil
}

func (l *LogEntryManager) FindEntryAtIndex(index uint64) (*pb.AppendEntriesRequest_Entry, error) {
	commandLog, err := l.entryPersister.FindLogByIndex(index)
	if err != nil {
		return nil, err
	}

	entry := &pb.AppendEntriesRequest_Entry{}
	err = proto.Unmarshal(commandLog.Command, entry)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (l *LogEntryManager) DeleteLogsAferIndex(index uint64) error {
	return l.entryPersister.DeleteLogsAferIndex(index)
}

func (l *LogEntryManager) AppendEntries(entries []*pb.AppendEntriesRequest_Entry) error {
	persisterEntries := make([]*persister.Entry, 0, len(entries))

	for _, entry := range entries {
		entryMarshalled, err := proto.Marshal(entry)
		if err != nil {
			return err
		}

		persisterEntries = append(persisterEntries, &persister.Entry{
			Term:    entry.Term,
			Command: entryMarshalled,
		})
	}

	return l.entryPersister.AppendLogs(persisterEntries)
}

func (l *LogEntryManager) GetEntriesBetweenIndexes(startIndex uint64, endIndex uint64) ([]*pb.AppendEntriesRequest_Entry, error) {
	if endIndex <= startIndex {
		return nil, fmt.Errorf("end index %d is not lower than the starting index %d", endIndex, startIndex)
	}

	entries := make([]*pb.AppendEntriesRequest_Entry, 0, endIndex-startIndex)

	iterator, err := l.entryPersister.ReplaySection(startIndex+1, endIndex)
	if err != nil {
		return nil, err
	}

	for iterator.Next() {
		log := iterator.Value()
		entry := &pb.AppendEntriesRequest_Entry{}
		err = proto.Unmarshal(log.Command, entry)
		if err != nil {
			iterator.Close()
			return nil, err
		}

		entries = append(entries, entry)
	}

	iterator.Close()
	return entries, iterator.Error()
}

func (l *LogEntryManager) ReplayEntries() (persister.ILogEntryPersisterIterator, error) {
	return l.entryPersister.Replay()
}
