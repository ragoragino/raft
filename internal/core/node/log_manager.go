package node

import (
	"github.com/golang/protobuf/proto"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
)

type ILogEntryManager interface {
	GetLastLogIndex() uint64
	GetLastLogTerm() uint64
	FindLogByIndex(index uint64) (*persister.CommandLog, error)
	DeleteLogsAferIndex(index uint64) error
	// TODO: Maybe change the type to some neutral type
	AppendLogs(term uint64, entries []*pb.AppendEntriesRequest_Entry) error
}

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
		return startingLogIndex
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

func (l *LogEntryManager) FindLogByIndex(index uint64) (*persister.CommandLog, error) {
	return l.entryPersister.FindLogByIndex(index)
}

func (l *LogEntryManager) DeleteLogsAferIndex(index uint64) error {
	return l.entryPersister.DeleteLogsAferIndex(index)
}

func (l *LogEntryManager) AppendLogs(term uint64, entries []*pb.AppendEntriesRequest_Entry) error {
	persisterEntries := make([]*persister.Entry, 0, len(entries))

	for _, entry := range entries {
		entryMarshalled, err := proto.Marshal(entry)
		if err != nil {
			return err
		}

		persisterEntries = append(persisterEntries, &persister.Entry{
			Term:    term,
			Command: entryMarshalled,
		})
	}

	return l.entryPersister.AppendLogs(persisterEntries)
}
