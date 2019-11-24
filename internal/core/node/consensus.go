package node

//go:generate protoc --proto_path=../../../api --go_out=plugins=grpc:gen ../../../api/raft.proto

import (
	"context"
	logrus "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	pb "raft/internal/core/node/gen"
	"raft/internal/core/persister"
	external_server "raft/internal/core/server"
	"sync"
	"sync/atomic"
	"time"
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
)

type clientLog struct {
	Key   string
	Value []byte
}

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

	clientRequestChannel <-chan external_server.ClusterRequestWrapper

	retryNodeAppendEntriesChannels map[string]chan *retryNodeAppendEntriesRequest
}

func NewRaft(cluster ICluster, logger *logrus.Entry, stateManager IStateManager,
	logManager ILogEntryManager, stateMachine IStateMachine, clusterServer IClusterServer,
	externelServer external_server.IExternalServer, opts ...RaftCallOption) *Raft {

	appendEntriesProcessChannel, err := clusterServer.GetAppendEntriesChannel()
	if err != nil {
		logger.Panicf("unable to obtain cluster server channel: %+v", err)
	}

	requestVoteProcessChannel, err := clusterServer.GetRequestVoteChannel()
	if err != nil {
		logger.Panicf("unable to obtain cluster server channel: %+v", err)
	}

	clientRequestChannel, err := externelServer.GetRequestChannel()
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
		clientRequestChannel:         clientRequestChannel,
	}

	return raft
}

func (r *Raft) Run() {
	stateChangedChannel := make(chan StateSwitched)
	id := r.stateManager.AddStateObserver(stateChangedChannel)
	defer func() {
		r.stateManager.RemoveStateObserver(id)
		close(stateChangedChannel)
	}()

	clusterState := r.cluster.GetClusterState()
	endRetryNodeAppendEntriesChannel := make(map[string]<-chan struct{}, len(clusterState.nodes))
	retryNodeAppendEntriesChannels := make(map[string]chan *retryNodeAppendEntriesRequest, len(clusterState.nodes))
	for _, node := range clusterState.nodes {
		retryNodeAppendEntriesChannels[node] = make(chan *retryNodeAppendEntriesRequest, r.settings.AppendEntryRetryBufferSize)
	}

	r.retryNodeAppendEntriesChannels = retryNodeAppendEntriesChannels

	for _, node := range clusterState.nodes {
		endRetryNodeAppendEntriesChannel[node] = r.retryNodeAppendEntries(r.closeChannel, node)
	}

	appendEntriesProcessingEndChannel := r.processAppendEntries(r.closeChannel)
	requestVoteProcessingEndChannel := r.processRequestVote(r.closeChannel)
	clientEndChannel := r.processClientRequests(r.closeChannel)

	ctx, cancel := context.WithCancel(context.Background())
	doneChannel := r.HandleRoleFollower(ctx, stateChangedChannel)

	for {
		// Waiting for the definitive end of the previous state
		var newRole RaftRole
		select {
		case newRole = <-doneChannel:
			cancel()
		case <-r.closeChannel:
			cancel()
			<-doneChannel
			<-appendEntriesProcessingEndChannel
			<-requestVoteProcessingEndChannel
			<-clientEndChannel
			for nodeID, channel := range endRetryNodeAppendEntriesChannel {
				<-channel
				close(retryNodeAppendEntriesChannels[nodeID])
			}
			close(r.runFinishedChannel)
			return
		}

		r.logger.Debugf("handler for old role finished. New role: %+v", newRole)

		ctx, cancel = context.WithCancel(context.Background())
		switch newRole {
		case FOLLOWER:
			doneChannel = r.HandleRoleFollower(ctx, stateChangedChannel)
		case CANDIDATE:
			doneChannel = r.HandleRoleCandidate(ctx, stateChangedChannel)
		case LEADER:
			doneChannel = r.HandleRoleLeader(ctx, stateChangedChannel)
		default:
			r.logger.Panicf("unrecognized role: %+v", newRole)
		}
	}
}

// Should be closed only once
func (r *Raft) Close() {
	r.logger.Debugf("closing raft")

	close(r.closeChannel)
	<-r.runFinishedChannel
}

func (r *Raft) HandleRoleFollower(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan RaftRole {
	doneChannel := make(chan RaftRole)

	go func() {
		electionTimeout := newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
		electionTimedChannel := make(chan struct{})

		// We will switch state in this goroutine after the follower times out
		// and wait for the invoked event to come in order to return from the handler
		go func() {
			select {
			case <-electionTimedChannel:
				r.stateMutex.Lock()

				r.logger.Debugf("election timed out, therefore switching to CANDIDATE role")

				newTerm := r.stateManager.GetCurrentTerm() + 1
				votedFor := r.settings.ID
				r.stateManager.SwitchState(newTerm, &votedFor, CANDIDATE)

				r.stateMutex.Unlock()
			case <-ctx.Done():
			}
		}()

		serverRole := FOLLOWER
	outerloop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == FOLLOWER {
					break
				}

				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case event := <-r.appendEntriesHandlersChannel:
				if event.byLeader {
					electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
				}
			case event := <-r.requestVoteHandlersChannel:
				if event.voteGranted {
					electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
				}
			case <-time.After(electionTimeout):
				close(electionTimedChannel)
			case <-ctx.Done():
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) HandleRoleCandidate(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan RaftRole {
	doneChannel := make(chan RaftRole)

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == CANDIDATE {
					break
				}

				doneChannel <- stateChangedEvent.newState.Role
				close(doneChannel)
				return
			default:
				break stateChangedLoop
			}
		}

		electionTimeout := newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)

		innerCtx, cancel := context.WithTimeout(context.Background(), r.settings.MinElectionTimeout)
		go r.broadcastRequestVote(innerCtx)

		serverRole := CANDIDATE
	outerloop:
		for {
			select {
			case <-r.appendEntriesHandlersChannel:
			case <-r.requestVoteHandlersChannel:
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == CANDIDATE {
					break
				}

				cancel()
				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case <-time.After(electionTimeout):
				cancel()
				r.logger.Debugf("election timed out without a winner, broadcasting new vote")

				electionTimeout = newElectionTimeout(r.settings.MinElectionTimeout, r.settings.MaxElectionTimeout)
				innerCtx, cancel = context.WithTimeout(context.Background(), r.settings.MinElectionTimeout)
				go r.broadcastRequestVote(innerCtx)
			case <-ctx.Done():
				cancel()
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) broadcastRequestVote(ctx context.Context) {
	r.stateMutex.RLock()
	request := &pb.RequestVoteRequest{
		Term:         r.stateManager.GetCurrentTerm(),
		CandidateId:  r.settings.ID,
		LastLogIndex: r.logManager.GetLastLogIndex(),
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
			r.stateManager.SwitchState(response.GetTerm(), nil, FOLLOWER)
			return
		}

		if response.GetVoteGranted() {
			countOfVotes++
		}
	}

	// Check if majority reached
	if r.checkClusterMajority(countOfVotes) {
		r.stateManager.SwitchState(r.stateManager.GetCurrentTerm(), nil, LEADER)
	}
}

func (r *Raft) HandleRoleLeader(ctx context.Context, stateChangedChannel <-chan StateSwitched) <-chan RaftRole {
	doneChannel := make(chan RaftRole)

	go func() {
		// Firstly, check if the state hasn't been changed in the meantime
	stateChangedLoop:
		for {
			select {
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == LEADER {
					break
				}

				doneChannel <- stateChangedEvent.newState.Role
				close(doneChannel)
				return
			default:
				break stateChangedLoop
			}
		}

		innerCtx, cancel := context.WithTimeout(context.Background(), r.settings.MinElectionTimeout)
		go r.broadcastHeartbeat(innerCtx)

		serverRole := LEADER
	outerloop:
		for {
			select {
			case <-r.appendEntriesHandlersChannel:
			case <-r.requestVoteHandlersChannel:
			case stateChangedEvent := <-stateChangedChannel:
				if stateChangedEvent.newState.Role == LEADER {
					break
				}

				cancel()
				serverRole = stateChangedEvent.newState.Role
				break outerloop
			case <-time.After(r.settings.HeartbeatFrequency):
				cancel()

				r.logger.Debugf("sending new heartbeat")

				innerCtx, cancel = context.WithTimeout(context.Background(), r.settings.HeartbeatFrequency)
				go r.broadcastHeartbeat(innerCtx)
			case <-ctx.Done():
				cancel()
				break outerloop
			}
		}

		doneChannel <- serverRole
		close(doneChannel)
	}()

	return doneChannel
}

func (r *Raft) broadcastHeartbeat(ctx context.Context) {
	r.stateMutex.RLock()
	request := &pb.AppendEntriesRequest{
		Term:     r.stateManager.GetCurrentTerm(),
		LeaderId: r.settings.ID,
		Entries:  make([]*pb.AppendEntriesRequest_Entry, 0),
	}
	r.stateMutex.RUnlock()

	responses := r.cluster.BroadcastAppendEntriesRPCs(ctx, request)

	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	// Check if we are still the leader
	if r.stateManager.GetRole() != LEADER {
		return
	}

	// Check if any of the responses contains higher term,
	// in which case we have to convert to FOLLOWER state
	for _, response := range responses {
		if response.GetTerm() > r.stateManager.GetCurrentTerm() {
			r.stateManager.SwitchState(response.GetTerm(), nil, FOLLOWER)
			return
		}
	}
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

				logger := r.logger.WithFields(logrus.Fields{"RPC": "AppendEntries", "Sender": request.GetLeaderId()})

				logger.Debugf("received RPC: %+v", request)

				r.stateMutex.Lock()

				senderTerm := request.GetTerm()
				receiverTerm := r.stateManager.GetCurrentTerm()

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
					// The node has higher term than the node, so we switch to FOLLOWER
					logger.Debugf("switching state to follower. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
					r.stateManager.SwitchState(request.GetTerm(), nil, FOLLOWER)
				} else if senderTerm < receiverTerm {
					// The candidate has lower term than the node, so deny the request
					logger.Debugf("sending reject response. sender's term: %d, receiver's term: %d", senderTerm, receiverTerm)
					r.stateMutex.Unlock()
					responseFunc(false, false)
					continue
				} else if r.stateManager.GetRole() == CANDIDATE {
					logger.Debugf("switching state to follower because received request with an equal term from a leader")
					r.stateManager.SwitchState(request.GetTerm(), nil, FOLLOWER)
				}

				// Set the leader
				err := r.cluster.SetLeader(request.GetLeaderId())
				if err != nil {
					logger.Errorf("unable to set leader: %+v", err)
				}

				index := request.GetPrevLogIndex()
				if index >= startingLogIndex-1 {
					log, err := r.logManager.FindLogByIndex(index)
					if err == persister.ErrIndexedLogDoesNotExists {
						r.stateMutex.Unlock()
						responseFunc(true, false)
						continue
					} else if err != nil {
						r.stateMutex.Unlock()
						logger.Panicf("unable to find log by index %d: %+v", index, err)
					}

					if log.Term != request.GetPrevLogTerm() {
						err := r.logManager.DeleteLogsAferIndex(index)
						if err != nil {
							r.stateMutex.Unlock()
							logger.Panicf("unable to delete log after index %d: %+v", index, err)
						}
					}
				}

				entries := request.GetEntries()
				if len(entries) != 0 {
					err := r.logManager.AppendLogs(r.stateManager.GetCurrentTerm(), entries)
					if err != nil {
						r.stateMutex.Unlock()
						logger.Panicf("unable to append logs: %+v", err)
					}
				}

				r.stateMutex.Unlock()

				logger.Debugf("sending accept response")

				responseFunc(true, true)
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
		close(r.appendEntriesHandlersChannel)
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

				logger := r.logger.WithFields(logrus.Fields{"RPC": "RequestVote", "Sender": candidateID})

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
					r.stateManager.SwitchState(senderTerm, nil, FOLLOWER)
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

				r.stateManager.SwitchState(senderTerm, &candidateID, FOLLOWER)

				r.stateMutex.Unlock()
				responseFunc(true)
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
		close(r.requestVoteHandlersChannel)
	}()

	return finishChannel
}

func (r *Raft) processClientRequests(done <-chan struct{}) <-chan struct{} {
	finishChannel := make(chan struct{})
	go func() {
	processLoop:
		for {
			// TODO: Allow get request processing while processing Create, Update or Delete
			select {
			case msg := <-r.clientRequestChannel:
				switch request := msg.Request.(type) {
				case external_server.ClusterCreateRequest:
					responseChannel := msg.ResponseChannel

					statusCode := r.processClientCreateRequest(msg.Context, request)
					if statusCode == external_server.Redirect {
						clusterState := r.cluster.GetClusterState()

						responseChannel <- external_server.ClusterCreateResponse{
							StatusCode: external_server.Redirect,
							Message: external_server.ClusterMessage{
								Address: clusterState.leaderEndpoint,
							},
						}

						break
					}

					responseChannel <- external_server.ClusterCreateResponse{
						StatusCode: statusCode,
					}
				case external_server.ClusterGetRequest:
					responseChannel := msg.ResponseChannel

					// TODO: We might receive stale values
					value, statusCode := r.processClientGetRequest(request)
					if statusCode == external_server.Redirect {
						clusterState := r.cluster.GetClusterState()

						responseChannel <- external_server.ClusterGetResponse{
							StatusCode: external_server.Redirect,
							Message: external_server.ClusterMessage{
								Address: clusterState.leaderEndpoint,
							},
						}

						break
					}

					responseChannel <- external_server.ClusterGetResponse{
						StatusCode: statusCode,
						Value:      value,
					}
				default:
					r.logger.Panicf("received unknown HTTP request: %+v", request)
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
// For reach failed node or node with false success response,
// we move the response to a special channel, which will be received
// by a specific per-node goroutine that shall continue trying to send the
// message to the node, together with all other failed requests.
// Meanwhile, all goroutines spawned by this function will have closed
// by the time of client timeout.
func (r *Raft) processClientCreateRequest(ctx context.Context, request external_server.ClusterCreateRequest) external_server.ClusterStatusCode {
	r.stateMutex.Lock()

	// Check if we are still the leader
	role := r.stateManager.GetRole()
	if role != LEADER {
		r.stateMutex.Unlock()
		r.logger.Errorf("unable to continue with processing client create request - not a leader")
		return external_server.Redirect
	}

	// TODO: Maybe check the state machine with the SN of the request ?

	newEntries := []*pb.AppendEntriesRequest_Entry{
		&pb.AppendEntriesRequest_Entry{
			Key:     request.Key,
			Payload: request.Value,
		},
	}

	previousLogIndex := r.logManager.GetLastLogIndex()
	previousLogTerm := r.logManager.GetLastLogTerm()

	// Append client command to log
	err := r.logManager.AppendLogs(r.stateManager.GetCurrentTerm(), newEntries)
	if err != nil {
		r.stateMutex.Unlock()
		r.logger.Errorf("unable to append logs while processing client create request: %+v", err)
		return external_server.FailedInternal
	}

	clusterRequest := &pb.AppendEntriesRequest{
		Term:         r.stateManager.GetCurrentTerm(),
		LeaderId:     r.settings.ID,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		LeaderCommit: 0, // TODO
		Entries:      newEntries,
	}
	r.stateMutex.Unlock()

	majorityReachedChannel := make(chan external_server.ClusterStatusCode)
	go func() {
		sendMajorityReachedOnce := sync.Once{}
		sendMajorityReachedResponse := func(count int) {
			if r.checkClusterMajority(count) {
				sendMajorityReachedOnce.Do(func() {
					majorityReachedChannel <- external_server.Ok
					close(majorityReachedChannel)
				})
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		responses := r.cluster.BroadcastAppendEntriesRPCsAsync(ctx, clusterRequest)

		r.stateMutex.RLock()
		currentTerm := r.stateManager.GetCurrentTerm()
		r.stateMutex.RUnlock()

		var countOfAcceptedReplies uint32 = 1
		for responseWrapper := range responses {
			if responseWrapper.Response.GetTerm() > currentTerm {
				r.stateMutex.Lock()
				r.stateManager.SwitchState(responseWrapper.Response.GetTerm(), nil, FOLLOWER)
				r.stateMutex.Unlock()
				cancel()

				majorityReachedChannel <- external_server.Redirect
				close(majorityReachedChannel)
				return
			}

			successCallback := func() {
				atomic.AddUint32(&countOfAcceptedReplies, 1)
				value := atomic.LoadUint32(&countOfAcceptedReplies)
				sendMajorityReachedResponse(int(value))
			}

			if !responseWrapper.Response.GetSuccess() {
				nodeChannel := r.retryNodeAppendEntriesChannels[responseWrapper.NodeID]
				nodeChannel <- &retryNodeAppendEntriesRequest{
					Context:         ctx,
					OriginalRequest: clusterRequest,
					SuccessCallback: successCallback,
				}
			}

			successCallback()
		}
	}()

	majorityReached := external_server.FailedInternal
	select {
	case majorityReached = <-majorityReachedChannel:
	case <-ctx.Done():
	}

	return majorityReached
}

func (r *Raft) retryNodeAppendEntries(done <-chan struct{}, nodeID string) <-chan struct{} {
	finishChannel := make(chan struct{})
	go func() {
		requestChannel, ok := r.retryNodeAppendEntriesChannels[nodeID]
		if !ok {
			r.logger.Panicf("no channel for node %s in the map for retrying AppendEntries: %+v", nodeID,
				r.retryNodeAppendEntriesChannels)
		}

	processLoop:
		for {
		requestLoop:
			select {
			case requestWrapper := <-requestChannel:
				for {
					select {
					case <-requestWrapper.Context.Done():
						break requestLoop
					default:
					}

					prevLogIndex := requestWrapper.OriginalRequest.PrevLogIndex - 1
					if prevLogIndex < startingLogIndex-1 {
						r.logger.Panicf("node could not accept applying log at index: %+v", prevLogIndex)
					}

					log, err := r.logManager.FindLogByIndex(prevLogIndex)
					if err != nil {
						r.logger.Panicf("unable to find log by index for log: %+v", prevLogIndex)
					}

					newRequest := &pb.AppendEntriesRequest{
						Term:         r.stateManager.GetCurrentTerm(),
						LeaderId:     r.settings.ID,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  log.Term,
						LeaderCommit: 0, // TODO
						Entries:      requestWrapper.OriginalRequest.Entries,
					}

					responseChannel, err := r.cluster.SendAppendEntries(requestWrapper.Context, nodeID, newRequest)
					if err != nil {
						// This is a non-gRPC request related error, e.g. nodeID is wrong
						r.logger.Panicf("unable to call SendAppendEntries due to: %+v", err)
					}

					response := <-responseChannel

					// Check if the node does not have a higher term
					// If this happens here, it means that more than a majority of servers
					// are on a higher term, therefore we can throw away this request
					r.stateMutex.RLock()
					currentTerm := r.stateManager.GetCurrentTerm()
					r.stateMutex.RUnlock()

					if response.GetTerm() > currentTerm {
						r.stateMutex.Lock()
						r.stateManager.SwitchState(response.GetTerm(), nil, FOLLOWER)
						r.stateMutex.Unlock()
						break requestLoop
					}

					if response.GetSuccess() {
						requestWrapper.SuccessCallback()
					}

					break requestLoop
				}
			case <-done:
				break processLoop
			}
		}

		close(finishChannel)
	}()

	return finishChannel
}

func (r *Raft) processClientGetRequest(request external_server.ClusterGetRequest) ([]byte, external_server.ClusterStatusCode) {
	r.stateMutex.Lock()
	role := r.stateManager.GetRole()
	if role != LEADER {
		r.stateMutex.Unlock()
		return nil, external_server.Redirect
	}
	r.stateMutex.Unlock()

	// TODO: The node might not be the leader (but doesn't know it)
	// so that we might still get stale data
	result := r.stateMachine.Get(request.Key)
	if result == nil {
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

func newElectionTimeout(min time.Duration, max time.Duration) time.Duration {
	electionTimeoutDiff := max - min
	electionTimeoutRand := rand.Int63n(electionTimeoutDiff.Nanoseconds())
	electionTimeoutRandNano := time.Duration(electionTimeoutRand) * time.Nanosecond
	electionTimeout := min + electionTimeoutRandNano

	return electionTimeout
}
