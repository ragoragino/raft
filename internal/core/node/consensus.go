package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	// "github.com/sasha-s/go-deadlock"
	"github.com/google/uuid"
	"math"
	"math/rand"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
	external_server "raft/internal/core/server"
	"sync"
	"sync/atomic"
	"time"

	logrus "github.com/sirupsen/logrus"
)

type raftOptions struct {
	ID                         string
	HeartbeatFrequency         time.Duration
	MaxElectionTimeout         time.Duration
	MinElectionTimeout         time.Duration
	AppendEntryRetryBufferSize int
}

var (
	defaultRaftOptions = raftOptions{
		ID:                         "Node",
		HeartbeatFrequency:         500 * time.Millisecond,
		MaxElectionTimeout:         1000 * time.Millisecond,
		MinElectionTimeout:         750 * time.Millisecond,
		AppendEntryRetryBufferSize: 100,
	}
)

type RaftCallOption func(opt *raftOptions)

func WithServerID(id string) RaftCallOption {
	return func(opt *raftOptions) {
		opt.ID = id
	}
}

func WithHeartbeatFrequency(freq time.Duration) RaftCallOption {
	return func(opt *raftOptions) {
		opt.HeartbeatFrequency = freq
	}
}

func WithMaxElectionTimeout(timeout time.Duration) RaftCallOption {
	return func(opt *raftOptions) {
		opt.MaxElectionTimeout = timeout
	}
}

func WithMinElectionTimeout(timeout time.Duration) RaftCallOption {
	return func(opt *raftOptions) {
		opt.MinElectionTimeout = timeout
	}
}

func WithAppendEntryRetryBufferSize(appendEntryBufferSize uint32) RaftCallOption {
	return func(opt *raftOptions) {
		opt.AppendEntryRetryBufferSize = int(appendEntryBufferSize)
	}
}

func applyRaftOptions(opts []RaftCallOption) *raftOptions {
	options := defaultRaftOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &options
}

type RaftRole int

const (
	FOLLOWER RaftRole = iota
	CANDIDATE
	LEADER
)

func (s RaftRole) String() string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	}

	logrus.Panicf("Unrecognized RaftRole: %d", int(s))
	return ""
}

const (
	startingLogIndex = 1
	startingTerm     = 0

	requestIDKey = "id"
)

type appendEntriesEvent struct {
	byLeader bool
}

type requestVoteEvent struct {
	voteGranted bool
}

type retryNodeAppendEntriesRequest struct {
	Context         context.Context
	OriginalRequest *pb.AppendEntriesRequest
	SuccessCallback func()
	FromError       error
}

type Raft struct {
	settings           *raftOptions
	cluster            ICluster
	stateManager       IStateManager
	logManager         ILogEntryManager
	stateMachine       IStateMachine
	stateMutex         sync.RWMutex
	logger             *logrus.Entry
	closeChannel       chan struct{}
	runFinishedChannel chan struct{}

	appendEntriesHandlersChannel chan appendEntriesEvent
	requestVoteHandlersChannel   chan requestVoteEvent

	appendEntriesProcessChannel <-chan appendEntriesProcessRequest
	requestVoteProcessChannel   <-chan requestVoteProcessRequest

	clientRequestsChannels *external_server.RequestsChannelsSub

	retryNodeAppendEntriesChannels map[string]chan *retryNodeAppendEntriesRequest
	retryNodeHeartbeatChannels     map[string]chan *retryNodeAppendEntriesRequest
}

func NewRaft(logger *logrus.Entry, stateManager IStateManager, logManager ILogEntryManager,
	stateMachine IStateMachine, cluster ICluster, clusterServer IClusterServer,
	externelServer external_server.Interface, opts ...RaftCallOption) *Raft {
	appendEntriesProcessChannel, err := clusterServer.GetAppendEntriesChannel()
	if err != nil {
		logger.Panicf("unable to obtain cluster server AppendEntries channel: %+v", err)
	}

	requestVoteProcessChannel, err := clusterServer.GetRequestVoteChannel()
	if err != nil {
		logger.Panicf("unable to obtain cluster server RequestVote channel: %+v", err)
	}

	clientRequestsChannels, err := externelServer.GetRequestsChannels()
	if err != nil {
		logger.Panicf("unable to obtain external server channel: %+v", err)
	}

	raft := &Raft{
		settings:                     applyRaftOptions(opts),
		cluster:                      cluster,
		logger:                       logger,
		stateManager:                 stateManager,
		logManager:                   logManager,
		stateMachine:                 stateMachine,
		closeChannel:                 make(chan struct{}),
		runFinishedChannel:           make(chan struct{}),
		appendEntriesHandlersChannel: make(chan appendEntriesEvent),
		requestVoteHandlersChannel:   make(chan requestVoteEvent),
		appendEntriesProcessChannel:  appendEntriesProcessChannel,
		requestVoteProcessChannel:    requestVoteProcessChannel,
		clientRequestsChannels:       clientRequestsChannels,
	}

	return raft
}

func (r *Raft) Run() {
	clusterState := r.cluster.GetClusterState()
	endRetryNodeAppendEntriesChannel := make(map[string]<-chan struct{}, len(clusterState.nodes))
	r.retryNodeAppendEntriesChannels = make(map[string]chan *retryNodeAppendEntriesRequest, len(clusterState.nodes))
	r.retryNodeHeartbeatChannels = make(map[string]chan *retryNodeAppendEntriesRequest, len(clusterState.nodes))
	for _, node := range clusterState.nodes {
		r.retryNodeAppendEntriesChannels[node] = make(chan *retryNodeAppendEntriesRequest, r.settings.AppendEntryRetryBufferSize)
		r.retryNodeHeartbeatChannels[node] = make(chan *retryNodeAppendEntriesRequest, 1)
	}

	for _, node := range clusterState.nodes {
		endRetryNodeAppendEntriesChannel[node] = r.retryNodeAppendEntries(r.closeChannel, node)
	}

	appendEntriesProcessingEndChannel := r.processAppendEntries(r.closeChannel)
	requestVoteProcessingEndChannel := r.processRequestVote(r.closeChannel)
	clientEndChannel := r.processClientRequests(r.closeChannel)

	ctx, cancel := context.WithCancel(context.Background())
	doneChannel := r.HandleRoleFollower(ctx)

	// This is the main Raft running loop
	currentRole := FOLLOWER
	roleChangedChannel, roleObserverEndChannel := r.observeStateChanges(r.closeChannel, currentRole)

handlerLoop:
	for {

		// Wait for the end of the previous state, or close signal from the user
	stateLoop:
		for {
			select {
			case <-doneChannel:
				r.logger.Panicf("unexpectedly received closed doneChannel")
			case <-r.closeChannel:
				cancel()
				<-doneChannel
				break handlerLoop
			case currentRole = <-roleChangedChannel:
				cancel()
				<-doneChannel
				break stateLoop
			}
		}

		r.logger.Debugf("handler for old role finished. New role: %+v", currentRole)

		ctx, cancel = context.WithCancel(context.Background())
		switch currentRole {
		case FOLLOWER:
			doneChannel = r.HandleRoleFollower(ctx)
		case CANDIDATE:
			doneChannel = r.HandleRoleCandidate(ctx)
		case LEADER:
			doneChannel = r.HandleRoleLeader(ctx)
		default:
			r.logger.Panicf("unrecognized role: %+v", currentRole)
		}
	}

	// Wait for the end of the state observer
	<-roleObserverEndChannel

	// Wait for the end of processing AppendEntries
	<-appendEntriesProcessingEndChannel
	close(r.appendEntriesHandlersChannel)

	// Wait for the end of processing RequestVote
	<-requestVoteProcessingEndChannel
	close(r.requestVoteHandlersChannel)

	// Wait for the end of client processing
	<-clientEndChannel

	// Wait for the end of per-node retry goroutines
	for nodeID, channel := range endRetryNodeAppendEntriesChannel {
		<-channel
		close(r.retryNodeAppendEntriesChannels[nodeID])
		close(r.retryNodeHeartbeatChannels[nodeID])
	}

	// Signal end of the Raft engine closing
	close(r.runFinishedChannel)
}

func (r *Raft) observeStateChanges(doneChannel <-chan struct{}, currentRole RaftRole) (<-chan RaftRole, <-chan struct{}) {
	finishChannel := make(chan struct{})
	roleChangedChannel := make(chan RaftRole)

	stateChangedChannel := make(chan StateSwitched)
	id := r.stateManager.AddPersistentStateObserver(stateChangedChannel)

	go func() {
		defer func() {
			r.stateManager.RemovePersistentStateObserver(id)
			close(stateChangedChannel)
			close(roleChangedChannel)
			close(finishChannel)
		}()

		for {
			select {
			case <-doneChannel:
				return
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role != currentRole {
					currentRole = stateChangedEvent.newState.Role
					roleChangedChannel <- currentRole
				}
			}
		}
	}()

	return roleChangedChannel, finishChannel
}

// Should be closed only once
func (r *Raft) Close() {
	r.logger.Debugf("closing raft")

	close(r.closeChannel)
	<-r.runFinishedChannel
}

func (r *Raft) HandleRoleFollower(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		electionTimeout := newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
		timer := NewTimer(electionTimeout)
		defer timer.Close()

		electionTimedChannel := make(chan struct{})
		defer close(electionTimedChannel)

		go func() {
			for {
				if _, ok := <-electionTimedChannel; !ok {
					return
				}

				r.stateMutex.Lock()

				r.logger.Debugf("election timed out, therefore switching to CANDIDATE role")

				newTerm := r.stateManager.GetCurrentTerm() + 1
				votedFor := r.settings.ID
				r.stateManager.SwitchPersistentState(newTerm, &votedFor, CANDIDATE)

				r.stateMutex.Unlock()
			}
		}()

	outerloop:
		for {
			select {
			case event, ok := <-r.appendEntriesHandlersChannel:
				if !ok {
					r.logger.Panicf("appendEntriesHandlersChannel was unexpectedly closed")
				}

				if event.byLeader {
					electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
					timer.Reset(electionTimeout)
				}
			case event, ok := <-r.requestVoteHandlersChannel:
				if !ok {
					r.logger.Panicf("requestVoteHandlersChannel was unexpectedly closed")
				}

				if event.voteGranted {
					electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
					timer.Reset(electionTimeout)
				}
			case <-timer.After():
				electionTimedChannel <- struct{}{}

				// Reset election timeout, so that we won't be always falling into this case
				// when it takes some time to propagate the new state
				electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
				timer.Reset(electionTimeout)
			case <-ctx.Done():
				break outerloop
			}
		}

		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) HandleRoleCandidate(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		electionTimeout := newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
		timer := NewTimer(electionTimeout)
		defer timer.Close()

		ctxWithRequestID := createNewRequestContext()

		innerCtx, cancel := context.WithTimeout(ctxWithRequestID, r.settings.MinElectionTimeout)
		go r.broadcastRequestVote(innerCtx)

	outerloop:
		for {
			select {
			case _, ok := <-r.appendEntriesHandlersChannel:
				if !ok {
					r.logger.Panicf("appendEntriesHandlersChannel was unexpectedly closed")
				}
			case _, ok := <-r.requestVoteHandlersChannel:
				if !ok {
					r.logger.Panicf("requestVoteHandlersChannel was unexpectedly closed")
				}
			case <-timer.After():
				cancel()
				r.logger.Debugf("election timed out without a winner, broadcasting new vote")

				electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
				timer.Reset(electionTimeout)

				innerCtx, cancel = context.WithTimeout(ctxWithRequestID, r.settings.MinElectionTimeout)
				go r.broadcastRequestVote(innerCtx)
			case <-ctx.Done():
				break outerloop
			}
		}

		cancel()
		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) broadcastRequestVote(ctx context.Context) {
	r.stateMutex.RLock()
	lastLogIndex := r.logManager.GetLastLogIndex()
	request := &pb.RequestVoteRequest{
		Term:         r.stateManager.GetCurrentTerm(),
		CandidateId:  r.settings.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  r.logManager.GetLastLogTerm(),
	}
	r.stateMutex.RUnlock()

	responses := r.cluster.BroadcastRequestVoteRPCs(ctx, request)

	// Count of votes starts at one because the server always votes for itself
	countOfVotes := 1

	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	// Check if the state hasn't been changed in the meantime
	if r.stateManager.GetRole() != CANDIDATE {
		return
	}

	// Count the number of votes
	for _, response := range responses {
		// If the response contains higher term, we convert to follower
		if response.GetTerm() > r.stateManager.GetCurrentTerm() {
			r.stateManager.SwitchPersistentState(response.GetTerm(), nil, FOLLOWER)
			return
		}

		if response.GetVoteGranted() {
			countOfVotes++
		}
	}

	// Check if majority reached
	if r.checkClusterMajority(countOfVotes) {
		r.stateManager.SwitchPersistentState(r.stateManager.GetCurrentTerm(), nil, LEADER)
	}
}

func (r *Raft) HandleRoleLeader(ctx context.Context) <-chan struct{} {
	doneChannel := make(chan struct{})

	go func() {
		innerCtx, cancel := context.WithCancel(ctx)
		heartbeatFinishedChannel := r.broadcastHeartbeat(innerCtx.Done())

	outerloop:
		for {
			select {
			case _, ok := <-r.appendEntriesHandlersChannel:
				if !ok {
					r.logger.Panicf("appendEntriesHandlersChannel was unexpectedly closed")
				}
			case _, ok := <-r.requestVoteHandlersChannel:
				if !ok {
					r.logger.Panicf("requestVoteHandlersChannel was unexpectedly closed")
				}
			case <-ctx.Done():
				break outerloop
			}
		}

		cancel()
		<-heartbeatFinishedChannel
		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) broadcastHeartbeat(done <-chan struct{}) <-chan struct{} {
	finishChannel := make(chan struct{})

	go func() {
		defer close(finishChannel)

		for {
			timedoutChannel := time.After(r.settings.HeartbeatFrequency)

			r.logger.Printf("broadcasting heartbeat")

			ctxWithRequestID := createNewRequestContext()

			r.stateMutex.RLock()
			currentTerm := r.stateManager.GetCurrentTerm()
			request := &pb.AppendEntriesRequest{
				Term:         r.stateManager.GetCurrentTerm(),
				LeaderId:     r.settings.ID,
				PrevLogIndex: r.logManager.GetLastLogIndex(),
				PrevLogTerm:  r.logManager.GetLastLogTerm(),
				LeaderCommit: r.stateManager.GetCommitIndex(),
				Entries:      make([]*pb.AppendEntriesRequest_Entry, 0),
			}
			r.stateMutex.RUnlock()

			ctx, cancel := context.WithTimeout(ctxWithRequestID, r.settings.HeartbeatFrequency)
			responseWrappers := r.cluster.BroadcastAppendEntriesRPCs(ctx, request)
			cancel()

			// Check if we are still the leader
			r.stateMutex.RLock()
			if r.stateManager.GetRole() != LEADER {
				r.stateMutex.RUnlock()
				r.logger.Debugf("not a leader anymore, therefore finishing broadcasting heartbeat")
				return
			}
			r.stateMutex.RUnlock()

			for _, responseWrapper := range responseWrappers {
				// Check if any of the responses contains higher term,
				// in which case we have to convert to FOLLOWER state
				if responseWrapper.Error == nil && responseWrapper.Response.GetTerm() > currentTerm {
					r.stateMutex.Lock()
					r.stateManager.SwitchPersistentState(responseWrapper.Response.GetTerm(), nil, FOLLOWER)
					r.stateMutex.Unlock()
					return
				}

				// Retry the heartbeat if unsuccessful or gRPC errors occured
				if !responseWrapper.Response.GetSuccess() {
					r.logger.Errorf("unable to broadcast heartbeat: %+v", responseWrapper)
					retryChannel, ok := r.retryNodeHeartbeatChannels[responseWrapper.NodeID]
					if !ok {
						r.logger.Panicf("unable to find node ID %s in the retry node heartbeat channels: %+v",
							responseWrapper.NodeID, r.retryNodeHeartbeatChannels)
					}

					// Do not wait for the channel to be empty,
					// because a full channel here signifies there is already a heartbeat
					// being retried to this particular node
					heartbeatToRetry := &retryNodeAppendEntriesRequest{
						Context:         ctxWithRequestID,
						OriginalRequest: request,
						SuccessCallback: func() {},
						FromError:       responseWrapper.Error,
					}
					select {
					case retryChannel <- heartbeatToRetry:
					default:
					}
				}
			}

			select {
			case <-done:
				return
			case <-timedoutChannel:
			}
		}
	}()

	return finishChannel
}

// TODO: processAppendEntries and processRequestVote should be optimized (if possible)
// because currently they take lock for almost all operations
// if not possible, move their calls from Run method to state handlers
func (r *Raft) processAppendEntries(done <-chan struct{}) <-chan struct{} {
	finishChannel := make(chan struct{})

	go func() {
	processLoop:
		for {
			select {
			case appendEntryToProcess, ok := <-r.appendEntriesProcessChannel:
				if !ok {
					r.logger.Debugf("channel for processing AppendEntries events was closed.")
					break processLoop
				}

				request := appendEntryToProcess.Request
				responseChannel := appendEntryToProcess.ResponseChannel

				logger := loggerFromContext(r.logger, appendEntryToProcess.Context)
				logger = logger.WithFields(logrus.Fields{"RPC": "AppendEntries", "Sender": request.GetLeaderId()})

				logger.Debugf("received RPC: %+v", request)

				r.stateMutex.Lock()

				senderTerm := request.GetTerm()
				receiverTerm := r.stateManager.GetCurrentTerm()
				entries := request.GetEntries()
				previousLogIndex := request.GetPrevLogIndex()
				previousLogTerm := request.GetPrevLogTerm()
				leaderID := request.GetLeaderId()
				leaderCommit := request.GetLeaderCommit()
				requestedNewLogIndex := previousLogIndex + uint64(len(entries))

				responseFunc := func(sentByLeader bool, entriesAppended bool) {
					r.appendEntriesHandlersChannel <- appendEntriesEvent{
						byLeader: sentByLeader,
					}

					responseChannel <- appendEntriesProcessResponse{
						Response: &pb.AppendEntriesResponse{
							Term:    receiverTerm,
							Success: entriesAppended,
						},
						Error: nil,
					}
				}

				if senderTerm > receiverTerm {
					// The sender node has higher term than the receiver node, so we switch to FOLLOWER
					logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
					r.stateManager.SwitchPersistentState(senderTerm, nil, FOLLOWER)
				} else if senderTerm < receiverTerm {
					// The candidate has lower term than the node, so deny the request
					logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
					r.stateMutex.Unlock()
					responseFunc(false, false)
					continue
				} else if r.stateManager.GetRole() == CANDIDATE {
					logger.Debugf("switching state to follower because received request with an equal term from a leader")
					r.stateManager.SwitchPersistentState(senderTerm, nil, FOLLOWER)
				}

				// Set the leader
				err := r.cluster.SetLeader(leaderID)
				if err != nil {
					logger.Panicf("unable to set leader %s: %+v", leaderID, err)
				}

				// We check if the index is correct
				// index == startingLogIndex - 1 signifies we received first log
				if previousLogIndex < startingLogIndex-1 {
					r.stateMutex.Unlock()
					logger.Panicf("received request with a previous index %d lower than the lower limit for log indexes",
						previousLogIndex)
				} else if previousLogIndex >= startingLogIndex {
					// If log does not contain an term-matching entry at previousLogIndex
					// reply false
					term, err := r.logManager.FindTermAtIndex(previousLogIndex)
					if err == persister.ErrIndexedLogDoesNotExists {
						r.stateMutex.Unlock()
						logger.Debugf("unable to find log with index: %+v", previousLogIndex)
						responseFunc(true, false)
						continue
					} else if err != nil {
						r.stateMutex.Unlock()
						logger.Panicf("failed when finding log by index %d: %+v", previousLogIndex, err)
					}

					if term != previousLogTerm {
						r.stateMutex.Unlock()
						logger.Debugf("terms not equal (local: %d, remote: %d) at index: %d", term, previousLogTerm, previousLogIndex)
						responseFunc(true, false)
						continue
					}

					// If an existing entry conflicts with a new one, i.e. same index
					// but different terms, delete the existing entry and all that follow it
					// Otherwise, append only new entries
					if len(entries) != 0 {
						firstNewLogIndex := previousLogIndex + 1
						firstNewLogTerm, err := r.logManager.FindTermAtIndex(firstNewLogIndex)
						if err != nil && err != persister.ErrIndexedLogDoesNotExists {
							r.stateMutex.Unlock()
							logger.Panicf("failed when finding log by index %d: %+v", firstNewLogIndex, err)
						} else if err == nil {
							if entries[0].GetTerm() != firstNewLogTerm {
								err := r.logManager.DeleteLogsAferIndex(firstNewLogIndex)
								if err != nil {
									r.stateMutex.Unlock()
									logger.Panicf("unable to delete log after index %d: %+v", previousLogIndex, err)
								}
							} else {
								// We presuppose that any logs after the first new log are equal
								lastLogIndex := r.logManager.GetLastLogIndex()
								nOfLogToAppend := requestedNewLogIndex - lastLogIndex
								if requestedNewLogIndex < lastLogIndex {
									nOfLogToAppend = 0
								}

								indexToAppendFrom := uint64(len(entries)) - nOfLogToAppend
								entries = entries[indexToAppendFrom:]
							}
						}
					}
				} else if len(entries) != 0 {
					// This is the case when we received first log to append
					// Therefore we need to delete all logs
					err := r.logManager.DeleteLogsAferIndex(startingLogIndex)
					if err != nil && err != persister.ErrDatabaseEmpty {
						r.stateMutex.Unlock()
						logger.Panicf("unable to delete log after index %d: %+v", previousLogIndex, err)
					}
				}

				if len(entries) != 0 {
					logger.Debugf("appending entries: %+v", entries)
					err := r.logManager.AppendEntries(entries)
					if err != nil {
						r.stateMutex.Unlock()
						logger.Panicf("unable to append logs: %+v", err)
					}
				}

				// Specs say: If leaderCommit > commitIndex, set commitIndex =
				// min(leaderCommit, index of last new entry).
				// Because we do not keep track of last applied, therefore
				// we set to leaderCommit if localCommitIndex < leaderCommit < indexOfLastNewEntry
				// we set to indexOfLastNewEntry if localCommitIndex < indexOfLastNewEntry < leaderCommit
				// we leave localCommitIndex if indexOfLastNewEntry < localCommitIndex < leaderCommit
				localCommitIndex := r.stateManager.GetCommitIndex()
				if leaderCommit > localCommitIndex {
					logger.Debugf("deciding whether to commit: localCommit: %d, newIndex: %d, leaderCommit: %d", localCommitIndex, requestedNewLogIndex, leaderCommit)

					newCommitIndex := localCommitIndex
					if localCommitIndex <= requestedNewLogIndex && requestedNewLogIndex <= leaderCommit {
						newCommitIndex = requestedNewLogIndex
					} else if localCommitIndex <= leaderCommit && leaderCommit <= requestedNewLogIndex {
						newCommitIndex = leaderCommit
					}

					r.commitLogsToStateMachine(appendEntryToProcess.Context, localCommitIndex, newCommitIndex)
					r.stateManager.SetCommitIndex(newCommitIndex)
				}

				r.stateMutex.Unlock()

				logger.Debugf("sending accept response")

				responseFunc(true, true)
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
	}()

	return finishChannel
}

func (r *Raft) processRequestVote(done <-chan struct{}) <-chan struct{} {
	finishChannel := make(chan struct{})

	go func() {
	processLoop:
		for {
			select {
			case requestVoteToProcess, ok := <-r.requestVoteProcessChannel:
				if !ok {
					r.logger.Debugf("channel for processing RequestVote events was closed.")
					break processLoop
				}

				request := requestVoteToProcess.Request
				responseChannel := requestVoteToProcess.ResponseChannel

				candidateID := request.GetCandidateId()

				logger := loggerFromContext(r.logger, requestVoteToProcess.Context)
				logger = logger.WithFields(logrus.Fields{"RPC": "RequestVote", "Sender": candidateID})

				logger.Debugf("received RPC: %+v", request)

				r.stateMutex.Lock()

				senderTerm := request.GetTerm()
				receiverTerm := r.stateManager.GetCurrentTerm()

				responseFunc := func(voteGranted bool) {
					r.requestVoteHandlersChannel <- requestVoteEvent{
						voteGranted: voteGranted,
					}

					responseChannel <- requestVoteProcessResponse{
						Response: &pb.RequestVoteResponse{
							Term:        receiverTerm,
							VoteGranted: voteGranted,
						},
						Error: nil,
					}
				}

				if senderTerm > receiverTerm {
					// Sender node has higher term than receiver node, so we switch to FOLLOWER
					logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
					r.stateManager.SwitchPersistentState(senderTerm, nil, FOLLOWER)
				} else if senderTerm < receiverTerm {
					// The candidate has lower term than the node, so deny the request
					logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)

					r.stateMutex.Unlock()
					responseFunc(false)
					continue
				}

				// The node has already voted
				if r.stateManager.GetVotedFor() != nil && *r.stateManager.GetVotedFor() != request.GetCandidateId() {
					logger.Debugf("sending reject response. Already voted for: %s", *r.stateManager.GetVotedFor())

					r.stateMutex.Unlock()
					responseFunc(false)
					continue
				}

				// Ensure that the candidate's log is at least as up to date as receivers log
				lastReceiverTerm := r.logManager.GetLastLogTerm()
				lastSenderTerm := request.GetLastLogTerm()
				if lastReceiverTerm > lastSenderTerm {
					logger.Debugf("sending reject response. Last log has higher term. Receiver: %d, Sender: %d",
						lastReceiverTerm, lastSenderTerm)

					r.stateMutex.Unlock()
					responseFunc(false)
					continue
				} else if lastSenderTerm == lastReceiverTerm {
					lastReceiverIndex := r.logManager.GetLastLogIndex()
					lastSenderIndex := request.GetLastLogIndex()

					if lastReceiverIndex > lastSenderIndex {
						logger.Debugf("sending reject response. Last log has higher index. Receiver: %d, Sender: %d",
							lastReceiverIndex, lastSenderIndex)

						r.stateMutex.Unlock()
						responseFunc(false)
						continue
					}
				}

				logger.Debugf("voting for sender")

				r.stateManager.SwitchPersistentState(senderTerm, &candidateID, FOLLOWER)

				r.stateMutex.Unlock()
				responseFunc(true)
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
	}()

	return finishChannel
}

func (r *Raft) processClientRequests(done <-chan struct{}) <-chan struct{} {
	finishChannel := make(chan struct{})
	go func() {
		requestChannel := make(chan interface{})
		go func() {
			defer close(requestChannel)

			for {
				select {
				case request, ok := <-r.clientRequestsChannels.ClusterCreateRequestChan:
					if !ok {
						return
					}

					requestChannel <- request
				case request, ok := <-r.clientRequestsChannels.ClusterDeleteRequestChan:
					if !ok {
						return
					}

					requestChannel <- request
				case <-done:
					return
				}
			}
		}()

	processLoop:
		for {
			select {
			case msg, ok := <-requestChannel:
				if !ok {
					r.logger.Debugf("channel for processing client requests was closed")
					break processLoop
				}

				switch request := msg.(type) {
				case external_server.ClusterCreateRequest:
					ctx := contextFromExternalServerContext(request.Context)

					responseChannel := request.ResponseChannel

					statusCode := r.processClientCreateRequest(ctx, &request)
					if statusCode == external_server.Redirect {
						clusterState := r.cluster.GetClusterState()

						responseChannel <- external_server.ClusterCreateResponse{
							ClusterResponse: external_server.ClusterResponse{
								StatusCode: external_server.Redirect,
								Message: external_server.ClusterMessage{
									LeaderName: clusterState.leaderName,
								},
							},
						}

						break
					}

					responseChannel <- external_server.ClusterCreateResponse{
						ClusterResponse: external_server.ClusterResponse{
							StatusCode: statusCode,
						},
					}
				case external_server.ClusterDeleteRequest:
					ctx := contextFromExternalServerContext(request.Context)

					responseChannel := request.ResponseChannel

					statusCode := r.processClientDeleteRequest(ctx, &request)
					if statusCode == external_server.Redirect {
						clusterState := r.cluster.GetClusterState()

						responseChannel <- external_server.ClusterDeleteResponse{
							ClusterResponse: external_server.ClusterResponse{
								StatusCode: external_server.Redirect,
								Message: external_server.ClusterMessage{
									LeaderName: clusterState.leaderName,
								},
							},
						}

						break
					}

					responseChannel <- external_server.ClusterDeleteResponse{
						ClusterResponse: external_server.ClusterResponse{
							StatusCode: statusCode,
						},
					}
				default:
					r.logger.Panicf("received unknown HTTP request: %+v", msg)
				}
			case request, ok := <-r.clientRequestsChannels.ClusterGetRequestChan:
				if !ok {
					r.logger.Debugf("channel for processing client requests was closed")
					break processLoop
				}

				ctx := contextFromExternalServerContext(request.Context)

				responseChannel := request.ResponseChannel

				value, statusCode := r.processClientGetRequest(ctx, &request)
				if statusCode == external_server.Redirect {
					clusterState := r.cluster.GetClusterState()

					responseChannel <- external_server.ClusterGetResponse{
						ClusterResponse: external_server.ClusterResponse{
							StatusCode: external_server.Redirect,
							Message: external_server.ClusterMessage{
								LeaderName: clusterState.leaderName,
							},
						},
					}

					break
				}

				responseChannel <- external_server.ClusterGetResponse{
					ClusterResponse: external_server.ClusterResponse{
						StatusCode: statusCode,
					},
					Value: value,
				}
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
	}()

	return finishChannel
}

// Sending this request should be always ok, because the majority
// of servers will reject our request if we are not the leader
//
// We try to get the approval of all nodes.
// For each failed node or node with false success response,
// we move the response to a special channel, which will be received
// by a specific per-node goroutine that shall continue trying to send the
// message to the node, together with all other failed requests to that node.
// Meanwhile, all goroutines spawned by this function will have closed
// by the time of client timeout.
func (r *Raft) processClientCreateRequest(ctx context.Context, request *external_server.ClusterCreateRequest) external_server.ClusterStatusCode {
	logger := loggerFromContext(r.logger, ctx)

	logger.Printf("received client create request: %+v", request)

	r.stateMutex.RLock()

	// Check if we are still the leader
	role := r.stateManager.GetRole()
	if role != LEADER {
		r.stateMutex.RUnlock()
		logger.Errorf("unable to continue with processing client create request - not a leader")
		return external_server.Redirect
	}

	// TODO: Maybe check the state machine with the SN of the request ?

	previousLogIndex := r.logManager.GetLastLogIndex()
	previousLogTerm := r.logManager.GetLastLogTerm()
	currentTerm := r.stateManager.GetCurrentTerm()
	commitIndex := r.stateManager.GetCommitIndex()

	r.stateMutex.RUnlock()

	newEntries := []*pb.AppendEntriesRequest_Entry{
		&pb.AppendEntriesRequest_Entry{
			Key:     request.Key,
			Payload: request.Value,
			Term:    currentTerm,
		},
	}

	// Append client command to log
	err := r.logManager.AppendEntries(newEntries)
	if err != nil {
		logger.Errorf("unable to append logs while processing client create request: %+v", err)
		return external_server.FailedInternal
	}

	clusterRequest := &pb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     r.settings.ID,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		LeaderCommit: commitIndex,
		Entries:      newEntries,
	}

	majorityReachedChannel := make(chan external_server.ClusterStatusCode)
	go func() {
		defer close(majorityReachedChannel)

		ctxWithRequestID := createNewRequestContext()

		sendMajorityReachedOnce := sync.Once{}
		sendMajorityReachedResponse := func(count int) {
			if r.checkClusterMajority(count) {
				sendMajorityReachedOnce.Do(func() {
					// Use default in select, because it might happen that there won't be any receivers
					// as the processing routine might have finished due to client timeout
					select {
					case majorityReachedChannel <- external_server.Ok:
					default:
					}

					// Update the index of the last committed log
					// Check if the terms match, see Figure 8 of the original paper
					r.stateMutex.Lock()
					if currentTerm == r.stateManager.GetCurrentTerm() {
						nextLogIndex := previousLogIndex + uint64(len(newEntries))
						r.commitLogsToStateMachine(ctxWithRequestID, r.stateManager.GetCommitIndex(), nextLogIndex)
						r.stateManager.SetCommitIndex(nextLogIndex)
					}
					r.stateMutex.Unlock()
				})
			}
		}

		logger.Printf("sending AppendEntries request: %+v", clusterRequest)

		requestCtx, cancel := context.WithCancel(ctxWithRequestID)
		responses := r.cluster.BroadcastAppendEntriesRPCs(requestCtx, clusterRequest)
		cancel()

		var countOfAcceptedReplies uint32 = 1
		for _, responseWrapper := range responses {
			logger.Printf("received AppendEntries response: %+v", responseWrapper)

			// This situation could happen under following conditions:
			// We start replication -> receive majority -> network partition occurs.
			// We are not the leader anymore and all servers that did not replicate
			// our AppendEntries will have higher term
			if responseWrapper.Error == nil && responseWrapper.Response.GetTerm() > currentTerm {
				r.stateMutex.Lock()
				r.stateManager.SwitchPersistentState(responseWrapper.Response.GetTerm(), nil, FOLLOWER)
				r.stateMutex.Unlock()

				majorityReachedChannel <- external_server.Redirect
				return
			}

			successCallback := func() {
				atomic.AddUint32(&countOfAcceptedReplies, 1)
				value := atomic.LoadUint32(&countOfAcceptedReplies)
				sendMajorityReachedResponse(int(value))
			}

			if responseWrapper.Error != nil || !responseWrapper.Response.GetSuccess() {
				nodeChannel := r.retryNodeAppendEntriesChannels[responseWrapper.NodeID]
				nodeChannel <- &retryNodeAppendEntriesRequest{
					Context:         ctxWithRequestID,
					OriginalRequest: clusterRequest,
					SuccessCallback: successCallback,
					FromError:       responseWrapper.Error,
				}

				continue
			}

			successCallback()
		}
	}()

	majorityReachedStatus := external_server.FailedInternal
	select {
	case majorityReachedStatus = <-majorityReachedChannel:
	case <-ctx.Done():
	}

	return majorityReachedStatus
}

// TODO
func (r *Raft) processClientDeleteRequest(ctx context.Context, request *external_server.ClusterDeleteRequest) external_server.ClusterStatusCode {
	return external_server.Ok
}

func (r *Raft) retryNodeAppendEntries(done <-chan struct{}, nodeID string) <-chan struct{} {
	finishChannel := make(chan struct{})
	go func() {
		requestChannel, ok := r.retryNodeAppendEntriesChannels[nodeID]
		if !ok {
			r.logger.Panicf("no channel for node %s in the map for retrying AppendEntries: %+v", nodeID,
				r.retryNodeAppendEntriesChannels)
		}

		heartbeatChannel, ok := r.retryNodeHeartbeatChannels[nodeID]
		if !ok {
			r.logger.Panicf("no channel for node %s in the map for retrying heartbeats: %+v", nodeID,
				r.retryNodeHeartbeatChannels)
		}

		contextCanceller := func(ctx context.Context, CancelFunc func()) {
			select {
			case <-done:
				CancelFunc()
			case <-ctx.Done():
			}
		}

	processLoop:
		for {
			var requestWrapper *retryNodeAppendEntriesRequest

			select {
			case requestWrapper = <-heartbeatChannel:
			case requestWrapper = <-requestChannel:
			case <-done:
				break processLoop
			}

			request := requestWrapper.OriginalRequest
			successCallback := requestWrapper.SuccessCallback
			fromError := requestWrapper.FromError
			ctxWithRequestID := requestWrapper.Context

			logger := loggerFromContext(r.logger, ctxWithRequestID)

		requestLoop:
			for {
				select {
				case <-done:
					break processLoop
				default:
				}

				// Check if we are not the leader
				// If we are not, we can safely finish retrying this request,
				// because the new leader will have it replicated (if it was committed)
				// or it won't (if the client request failed)
				r.stateMutex.RLock()
				if r.stateManager.GetRole() != LEADER {
					r.stateMutex.RUnlock()
					break requestLoop
				}

				currentTerm := r.stateManager.GetCurrentTerm()
				r.stateMutex.RUnlock()

				// Create new request
				newRequest := request
				if fromError == nil {
					newRequest = r.moveRequest(ctxWithRequestID, request)
				}

				logger.Printf("retrying AppendEntries request: %+v", newRequest)

				// SendAppendEntries will continue retrying until it receives a response
				ctx, cancel := context.WithCancel(ctxWithRequestID)
				go contextCanceller(ctx, cancel)
				response, err := r.cluster.SendAppendEntries(ctx, nodeID, newRequest)
				cancel()
				if err == context.Canceled || err == context.DeadlineExceeded {
					logger.Printf("retrying ended: %+v", err)
					break requestLoop
				} else if err != nil {
					// This is a non-gRPC request related error, e.g. nodeID is wrong
					logger.Panicf("unable to call SendAppendEntries due to: %+v", err)
				}

				logger.Printf("received AppendEntries retry response: %+v", response)

				// Check if the node does not have a higher term
				// If this happens here, it means that more than a majority of servers
				// are on a higher term, therefore we can throw away this request
				if response.GetTerm() > currentTerm {
					r.stateMutex.Lock()
					r.stateManager.SwitchPersistentState(response.GetTerm(), nil, FOLLOWER)
					r.stateMutex.Unlock()
					break requestLoop
				}

				if response.GetSuccess() {
					successCallback()
					break requestLoop
				}

				request = newRequest
			}
		}

		close(finishChannel)
	}()

	return finishChannel
}

func (r *Raft) moveRequest(ctx context.Context, oldRequest *pb.AppendEntriesRequest) *pb.AppendEntriesRequest {
	logger := loggerFromContext(r.logger, ctx)

	r.stateMutex.RLock()
	currentTerm := r.stateManager.GetCurrentTerm()
	commitIndex := r.stateManager.GetCommitIndex()
	r.stateMutex.RUnlock()

	var term uint64 = startingTerm
	prevLogIndex := oldRequest.PrevLogIndex - 1
	if prevLogIndex < startingLogIndex-1 {
		logger.Panicf("node could not accept applying log at index: %+v", prevLogIndex)
	} else if prevLogIndex > startingLogIndex-1 {
		var err error
		term, err = r.logManager.FindTermAtIndex(prevLogIndex)
		if err != nil {
			logger.Panicf("unable to find log for index %d: %+v", prevLogIndex, err)
		}
	}

	// Add last log to the entries
	entry, err := r.logManager.FindEntryAtIndex(oldRequest.PrevLogIndex)
	if err != nil {
		logger.Panicf("unable to find log for index %d: %+v", oldRequest.PrevLogIndex, err)
	}

	oldRequest.Entries = append(oldRequest.Entries, entry)

	return &pb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     r.settings.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  term,
		LeaderCommit: commitIndex,
		Entries:      oldRequest.Entries,
	}
}

func (r *Raft) processClientGetRequest(ctx context.Context, request *external_server.ClusterGetRequest) ([]byte, external_server.ClusterStatusCode) {
	logger := loggerFromContext(r.logger, ctx)

	logger.Printf("received client get request: %+v", request)

	r.stateMutex.RLock()
	role := r.stateManager.GetRole()
	if role != LEADER {
		r.stateMutex.RUnlock()
		return nil, external_server.Redirect
	}
	r.stateMutex.RUnlock()

	// TODO: The node might not be the leader (but doesn't know it)
	// so that we might still get stale data
	result, ok := r.stateMachine.Get(request.Key)
	if !ok {
		return nil, external_server.FailedNotFound
	}

	return result, external_server.Ok
}

func (r *Raft) checkClusterMajority(count int) bool {
	// TODO: maybe save this so that we avoid calling it all the time
	clusterState := r.cluster.GetClusterState()
	clusterMajority := int(math.Round(float64(len(clusterState.nodes)) * 0.5))
	if count > clusterMajority {
		return true
	}
	return false
}

// commit logs to state machine in the range (fromIndex, toIndex]
func (r *Raft) commitLogsToStateMachine(ctx context.Context, fromIndex uint64, toIndex uint64) {
	logger := loggerFromContext(r.logger, ctx)

	if toIndex > fromIndex {
		logEntriesToCommit, err := r.logManager.GetEntriesBetweenIndexes(fromIndex, toIndex)
		if err != nil {
			logger.Panicf("unable to find entry at indexes %d:%d: %+v", fromIndex, toIndex, err)
		}

		logger.Debugf("log entries to commit: %+v", logEntriesToCommit)

		for _, logEntryToCommit := range logEntriesToCommit {
			r.stateMachine.Create(logEntryToCommit.GetKey(), logEntryToCommit.GetPayload())
		}
	}
}

func newElectionTimeout(min time.Duration, max time.Duration) time.Duration {
	electionTimeoutDiff := max - min
	electionTimeoutRand := rand.Int63n(electionTimeoutDiff.Nanoseconds())
	electionTimeoutRandNano := time.Duration(electionTimeoutRand) * time.Nanosecond
	electionTimeout := min + electionTimeoutRandNano

	return electionTimeout
}

func createNewRequestContext() context.Context {
	return context.WithValue(context.Background(), requestIDKey, uuid.New().String())
}

func loggerFromContext(logger *logrus.Entry, ctx context.Context) *logrus.Entry {
	id := ctx.Value(requestIDKey)

	return logger.WithFields(logrus.Fields{
		requestIDKey: id,
	})
}

func contextFromExternalServerContext(ctx context.Context) context.Context {
	id := ctx.Value(external_server.RequestIDKey)
	return context.WithValue(ctx, requestIDKey, id)
}
