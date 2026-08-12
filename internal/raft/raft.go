// Package raft implements the deterministic QuorumKV consensus state machine.
package raft

import "sort"

const maxAppendEntriesBytes = 1<<20 + 4<<10

// NodeID identifies one Node in a Cluster.
type NodeID string

// Role is a Node's current participation state.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

// Event is one input to the deterministic state machine.
type Event interface{ isEvent() }

// Action is one effect for the runtime to perform outside the Raft core.
type Action interface{ isAction() }

// EntryType identifies the replicated command carried by a log entry.
type EntryType uint8

const (
	EntryNoOp EntryType = iota
	EntryOpenSession
	EntryCloseSession
	EntrySet
	EntryDelete
)

// SessionID is the replicated identity of one Client Session.
type SessionID [16]byte

// ProposalID correlates a local request with the replicated entry it created.
// It is runtime metadata and is never stored in the log or sent to peers.
type ProposalID uint64

// ReadID correlates one local read with the quorum round that authorizes it.
// It is runtime metadata and is never stored in the log.
type ReadID uint64

// LogEntry is one position in the replicated Raft log.
type LogEntry struct {
	Index     uint64
	Term      uint64
	Type      EntryType
	SessionID SessionID
	Sequence  uint64
	Key       string
	Value     []byte
}

// ElectionTimeout reports that the runtime's election timer fired.
type ElectionTimeout struct{}

func (ElectionTimeout) isEvent() {}

// HeartbeatTimeout asks a Leader to send its next heartbeat round.
type HeartbeatTimeout struct{}

func (HeartbeatTimeout) isEvent() {}

// CheckQuorumTimeout ends the current Leader contact window.
type CheckQuorumTimeout struct{}

func (CheckQuorumTimeout) isEvent() {}

// HardStatePersisted reports that a requested hard-state update is durable.
type HardStatePersisted struct{ PersistenceID uint64 }

func (HardStatePersisted) isEvent() {}

// LogEntriesPersisted reports that appended log entries are durable.
type LogEntriesPersisted struct{ PersistenceID uint64 }

func (LogEntriesPersisted) isEvent() {}

// CommitIndexPersisted reports that the committed prefix is durable.
type CommitIndexPersisted struct{ PersistenceID uint64 }

func (CommitIndexPersisted) isEvent() {}

// RecoverCommitted asks the core to reconstruct lastApplied by emitting the
// durable committed prefix once during startup.
type RecoverCommitted struct{}

func (RecoverCommitted) isEvent() {}

// SnapshotCompacted tells the core that applied history through Index now has
// an immutable durable Snapshot and may no longer be sent as log entries.
type SnapshotCompacted struct{ Index, Term uint64 }

func (SnapshotCompacted) isEvent() {}

// ProposeSession asks the current Leader to append one session command.
type ProposeSession struct {
	ProposalID ProposalID
	Type       EntryType
	SessionID  SessionID
}

func (ProposeSession) isEvent() {}

// ProposeSet asks the current Leader to append one SET command. Value ownership
// transfers to the core; callers must not mutate it after Step returns.
type ProposeSet struct {
	ProposalID ProposalID
	SessionID  SessionID
	Sequence   uint64
	Key        string
	Value      []byte
}

func (ProposeSet) isEvent() {}

// ProposeDelete asks the current Leader to append one DELETE command.
type ProposeDelete struct {
	ProposalID ProposalID
	SessionID  SessionID
	Sequence   uint64
	Key        string
}

func (ProposeDelete) isEvent() {}

// ConfirmRead asks the Leader to prove current authority before a local read.
type ConfirmRead struct{ ReadID ReadID }

func (ConfirmRead) isEvent() {}

// PreVoteRequest checks whether an election for Term would be viable without
// changing any Node's current Term or durable vote.
type PreVoteRequest struct {
	From         NodeID
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (PreVoteRequest) isEvent() {}

// PreVoteResponse reports a peer's current Term and pre-vote decision.
type PreVoteResponse struct {
	From        NodeID
	Term        uint64
	CurrentTerm uint64
	Granted     bool
}

func (PreVoteResponse) isEvent() {}

// VoteRequest asks a Node to vote in an election.
type VoteRequest struct {
	From         NodeID
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (VoteRequest) isEvent() {}

// VoteResponse reports whether a peer voted for this Node.
type VoteResponse struct {
	From    NodeID
	Term    uint64
	Granted bool
}

func (VoteResponse) isEvent() {}

// AppendEntries carries ordered log entries and also serves as a heartbeat
// when Entries is empty.
type AppendEntries struct {
	From         NodeID
	Term         uint64
	RequestID    uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
	ReadID       ReadID
}

func (AppendEntries) isEvent() {}

// AppendEntriesResponse reports the durable replicated prefix of a Follower.
type AppendEntriesResponse struct {
	From          NodeID
	Term          uint64
	RequestID     uint64
	Success       bool
	MatchIndex    uint64
	ConflictTerm  uint64
	ConflictIndex uint64
	ReadID        ReadID
}

func (AppendEntriesResponse) isEvent() {}

// AppendEntriesFailed reports that the runtime could not deliver one
// replication request. A later heartbeat may retry the same Follower.
type AppendEntriesFailed struct {
	To        NodeID
	RequestID uint64
}

func (AppendEntriesFailed) isEvent() {}

// InstallSnapshot carries one bounded chunk. The runtime validates and stages
// Data, then sets Success and Installed before delivering the event to Step.
type InstallSnapshot struct {
	From          NodeID
	Term          uint64
	RequestID     uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Length        uint64
	Checksum      uint32
	Offset        uint64
	Data          []byte
	Done          bool
	Success       bool
	NextOffset    uint64
	Installed     bool
}

func (InstallSnapshot) isEvent() {}

// InstallSnapshotResponse advances or completes one Leader transfer.
type InstallSnapshotResponse struct {
	From          NodeID
	Term          uint64
	RequestID     uint64
	SnapshotIndex uint64
	Success       bool
	NextOffset    uint64
	Done          bool
}

func (InstallSnapshotResponse) isEvent() {}

// PersistHardState asks the runtime to durably store the current Term and vote.
type PersistHardState struct {
	PersistenceID uint64
	Term          uint64
	VotedFor      NodeID
}

func (PersistHardState) isAction() {}

// PersistLogEntries asks the runtime to durably replace an optional
// uncommitted suffix, then append and sync entries in order.
type PersistLogEntries struct {
	PersistenceID uint64
	TruncateFrom  uint64
	Entries       []LogEntry
}

func (PersistLogEntries) isAction() {}

// PersistCommitIndex asks the runtime to sync the newly committed prefix
// before any entry in that prefix is applied.
type PersistCommitIndex struct {
	PersistenceID uint64
	CommitIndex   uint64
}

func (PersistCommitIndex) isAction() {}

// SendPreVoteRequest sends a pre-vote request to one peer.
type SendPreVoteRequest struct {
	To      NodeID
	Request PreVoteRequest
}

func (SendPreVoteRequest) isAction() {}

// SendPreVoteResponse sends a pre-vote decision to one peer.
type SendPreVoteResponse struct {
	To       NodeID
	Response PreVoteResponse
}

func (SendPreVoteResponse) isAction() {}

// SendVoteRequest sends a vote request to one peer.
type SendVoteRequest struct {
	To      NodeID
	Request VoteRequest
}

func (SendVoteRequest) isAction() {}

// SendVoteResponse sends a vote decision to one peer.
type SendVoteResponse struct {
	To       NodeID
	Response VoteResponse
}

func (SendVoteResponse) isAction() {}

// SendAppendEntries sends replication or heartbeat traffic to one peer.
type SendAppendEntries struct {
	To      NodeID
	Request AppendEntries
}

func (SendAppendEntries) isAction() {}

// SendAppendEntriesResponse acknowledges or rejects one append request.
type SendAppendEntriesResponse struct {
	To       NodeID
	Response AppendEntriesResponse
}

func (SendAppendEntriesResponse) isAction() {}

// SendInstallSnapshot asks the runtime to load and send one bounded chunk of
// the immutable Snapshot at SnapshotIndex/SnapshotTerm.
type SendInstallSnapshot struct {
	To            NodeID
	Term          uint64
	RequestID     uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Offset        uint64
}

func (SendInstallSnapshot) isAction() {}

// SendInstallSnapshotResponse acknowledges one staged or installed chunk.
type SendInstallSnapshotResponse struct {
	To       NodeID
	Response InstallSnapshotResponse
}

func (SendInstallSnapshotResponse) isAction() {}

// ResetElectionTimer asks the runtime to choose and schedule a new randomized
// election timeout.
type ResetElectionTimer struct{}

func (ResetElectionTimer) isAction() {}

// ResetHeartbeatTimer schedules the Leader's next heartbeat round.
type ResetHeartbeatTimer struct{}

func (ResetHeartbeatTimer) isAction() {}

// ResetCheckQuorumTimer starts a fresh Leader contact window.
type ResetCheckQuorumTimer struct{}

func (ResetCheckQuorumTimer) isAction() {}

// BecameLeader reports the observable result of winning an election.
type BecameLeader struct{ Term uint64 }

func (BecameLeader) isAction() {}

// LostLeadership reports that check-quorum removed local authority.
type LostLeadership struct{ Term uint64 }

func (LostLeadership) isAction() {}

// ApplyEntry asks the runtime to apply one newly committed entry.
type ApplyEntry struct{ Entry LogEntry }

func (ApplyEntry) isAction() {}

// BecameReadReady reports that the Leader's current-Term no-op is applied.
type BecameReadReady struct{ Term uint64 }

func (BecameReadReady) isAction() {}

// ProposalAccepted binds a local proposal to its replicated log position.
type ProposalAccepted struct {
	ProposalID ProposalID
	Index      uint64
}

func (ProposalAccepted) isAction() {}

// ProposalRejected reports that this Node is not the Leader.
type ProposalRejected struct {
	ProposalID ProposalID
	LeaderID   NodeID
}

func (ProposalRejected) isAction() {}

// ReadConfirmed authorizes a local read after a quorum response and local
// application through the captured committed prefix.
type ReadConfirmed struct {
	ReadID      ReadID
	CommitIndex uint64
}

func (ReadConfirmed) isAction() {}

// ReadRejected reports that this Node cannot currently authorize the read.
type ReadRejected struct {
	ReadID   ReadID
	LeaderID NodeID
}

func (ReadRejected) isAction() {}

// State is a read-only snapshot used by runtimes and deterministic assertions.
type State struct {
	ID              NodeID
	Role            Role
	Term            uint64
	VotedFor        NodeID
	LeaderID        NodeID
	LastLogIndex    uint64
	LastLogTerm     uint64
	CommitIndex     uint64
	LastApplied     uint64
	LastAppliedTerm uint64
	SnapshotIndex   uint64
	ReadReady       bool
}

// HardState is the election state restored before a Node processes events.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// RecoveredState is the durable consensus state restored by the runtime.
// SnapshotIndex is already represented in the restored state machine;
// RecoverCommitted emits only the later committed WAL suffix.
type RecoveredState struct {
	HardState     HardState
	Log           []LogEntry
	CommitIndex   uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

type pendingHardStatePersistence struct {
	term     uint64
	votedFor NodeID
	after    []Action
}

type logPersistencePurpose uint8

const (
	persistLeaderEntry logPersistencePurpose = iota
	persistFollowerAppend
)

type pendingLogPersistence struct {
	purpose      logPersistencePurpose
	term         uint64
	leader       NodeID
	requestID    uint64
	truncateFrom uint64
	lastIndex    uint64
	leaderCommit uint64
	readID       ReadID
}

type pendingCommitPersistence struct {
	commitIndex uint64
	after       []Action
}

type pendingRead struct {
	acknowledgements map[NodeID]struct{}
	commitIndex      uint64
	confirmed        bool
}

type pendingAppend struct {
	requestID    uint64
	leaderCommit uint64
}

// Node consumes one event at a time and emits effects without performing I/O.
type Node struct {
	id               NodeID
	peers            []NodeID
	role             Role
	term             uint64
	votedFor         NodeID
	leaderID         NodeID
	lastLogIndex     uint64
	lastLogTerm      uint64
	durableLogIndex  uint64
	logBaseIndex     uint64
	logBaseTerm      uint64
	log              []LogEntry
	commitIndex      uint64
	lastApplied      uint64
	readReady        bool
	votes            map[NodeID]struct{}
	preVotes         map[NodeID]struct{}
	activePeers      map[NodeID]struct{}
	matchIndex       map[NodeID]uint64
	nextIndex        map[NodeID]uint64
	appendInFlight   map[NodeID]pendingAppend
	snapshotInFlight map[NodeID]uint64
	nextRequestID    uint64
	pendingReads     map[ReadID]*pendingRead

	// recentLeaderContact is cleared only when the election timer fires. It
	// prevents a healthy Follower from helping an isolated peer disrupt a Leader.
	recentLeaderContact bool

	durableTerm              uint64
	durableVotedFor          NodeID
	nextHardStatePersistence uint64
	pendingHardState         map[uint64]pendingHardStatePersistence
	nextLogPersistence       uint64
	pendingLog               map[uint64]pendingLogPersistence
	durableCommitIndex       uint64
	nextCommitPersistence    uint64
	pendingCommit            map[uint64]pendingCommitPersistence
}

// NewNode creates a Follower with an empty log and no durable election state.
// Peers must exclude id.
func NewNode(id NodeID, peers []NodeID) *Node {
	return NewNodeWithHardState(id, peers, HardState{})
}

// NewNodeWithHardState creates a Follower from state recovered by the runtime.
// Recovered state is durable by definition, so a vote cannot be granted again
// in the same Term after restart.
func NewNodeWithHardState(id NodeID, peers []NodeID, hardState HardState) *Node {
	return NewNodeWithLog(id, peers, hardState, nil)
}

// NewNodeWithLog creates a Follower from durable election state and log
// entries recovered by the runtime.
func NewNodeWithLog(id NodeID, peers []NodeID, hardState HardState, entries []LogEntry) *Node {
	return NewNodeFromRecoveredState(id, peers, RecoveredState{HardState: hardState, Log: entries})
}

// NewNodeFromRecoveredState creates a Follower whose durable committed prefix
// will be applied only when the runtime delivers RecoverCommitted.
func NewNodeFromRecoveredState(id NodeID, peers []NodeID, recovered RecoveredState) *Node {
	if recovered.SnapshotIndex > 0 && len(recovered.Log) > 0 && recovered.Log[0].Index == 1 {
		if recovered.SnapshotIndex <= uint64(len(recovered.Log)) {
			if recovered.SnapshotTerm == 0 {
				recovered.SnapshotTerm = recovered.Log[recovered.SnapshotIndex-1].Term
			}
			recovered.Log = recovered.Log[recovered.SnapshotIndex:]
		}
	}
	orderedPeers := append([]NodeID(nil), peers...)
	sort.Slice(orderedPeers, func(i, j int) bool { return orderedPeers[i] < orderedPeers[j] })
	node := &Node{
		id:                 id,
		peers:              orderedPeers,
		role:               Follower,
		term:               recovered.HardState.Term,
		votedFor:           recovered.HardState.VotedFor,
		durableTerm:        recovered.HardState.Term,
		durableVotedFor:    recovered.HardState.VotedFor,
		commitIndex:        recovered.CommitIndex,
		durableCommitIndex: recovered.CommitIndex,
		lastApplied:        recovered.SnapshotIndex,
		logBaseIndex:       recovered.SnapshotIndex,
		logBaseTerm:        recovered.SnapshotTerm,
		lastLogIndex:       recovered.SnapshotIndex,
		lastLogTerm:        recovered.SnapshotTerm,
		durableLogIndex:    recovered.SnapshotIndex,
		votes:              make(map[NodeID]struct{}),
		preVotes:           make(map[NodeID]struct{}),
		activePeers:        make(map[NodeID]struct{}),
		matchIndex:         make(map[NodeID]uint64),
		nextIndex:          make(map[NodeID]uint64),
		appendInFlight:     make(map[NodeID]pendingAppend),
		snapshotInFlight:   make(map[NodeID]uint64),
		pendingHardState:   make(map[uint64]pendingHardStatePersistence),
		pendingLog:         make(map[uint64]pendingLogPersistence),
		pendingCommit:      make(map[uint64]pendingCommitPersistence),
		pendingReads:       make(map[ReadID]*pendingRead),
	}
	if len(recovered.Log) > 0 {
		node.log = cloneLogEntries(recovered.Log)
		node.lastLogIndex = recovered.Log[len(recovered.Log)-1].Index
		node.lastLogTerm = recovered.Log[len(recovered.Log)-1].Term
		node.durableLogIndex = node.lastLogIndex
	}
	return node
}

// State returns the Node's current deterministic state.
func (n *Node) State() State {
	var lastAppliedTerm uint64
	if n.lastApplied > 0 {
		lastAppliedTerm = n.termAt(n.lastApplied)
	}
	return State{
		ID:              n.id,
		Role:            n.role,
		Term:            n.term,
		VotedFor:        n.votedFor,
		LeaderID:        n.leaderID,
		LastLogIndex:    n.lastLogIndex,
		LastLogTerm:     n.lastLogTerm,
		CommitIndex:     n.commitIndex,
		LastApplied:     n.lastApplied,
		LastAppliedTerm: lastAppliedTerm,
		SnapshotIndex:   n.logBaseIndex,
		ReadReady:       n.readReady,
	}
}

// Step applies one event and returns actions in execution order.
func (n *Node) Step(event Event) []Action {
	switch event := event.(type) {
	case ElectionTimeout:
		return n.startPreVote()
	case HeartbeatTimeout:
		return n.heartbeatRound()
	case CheckQuorumTimeout:
		return n.checkQuorum()
	case HardStatePersisted:
		return n.hardStatePersisted(event)
	case LogEntriesPersisted:
		return n.logEntriesPersisted(event)
	case CommitIndexPersisted:
		return n.commitIndexPersisted(event)
	case RecoverCommitted:
		return n.applyThrough(n.durableCommitIndex)
	case SnapshotCompacted:
		n.compactLog(event.Index, event.Term)
		return nil
	case ProposeSession:
		return n.proposeSession(event)
	case ProposeSet:
		return n.proposeSet(event)
	case ProposeDelete:
		return n.proposeDelete(event)
	case ConfirmRead:
		return n.confirmRead(event)
	case PreVoteRequest:
		return n.handlePreVoteRequest(event)
	case PreVoteResponse:
		return n.handlePreVoteResponse(event)
	case VoteRequest:
		return n.handleVoteRequest(event)
	case VoteResponse:
		return n.handleVoteResponse(event)
	case AppendEntries:
		return n.handleAppendEntries(event)
	case AppendEntriesResponse:
		return n.handleAppendEntriesResponse(event)
	case AppendEntriesFailed:
		return n.handleAppendEntriesFailed(event)
	case InstallSnapshot:
		return n.handleInstallSnapshot(event)
	case InstallSnapshotResponse:
		return n.handleInstallSnapshotResponse(event)
	default:
		return nil
	}
}

func (n *Node) startPreVote() []Action {
	if n.role == Leader {
		return nil
	}
	n.recentLeaderContact = false
	n.leaderID = ""
	n.role = PreCandidate
	n.preVotes = map[NodeID]struct{}{n.id: {}}
	term := n.term + 1
	actions := make([]Action, 0, len(n.peers)+1)
	actions = append(actions, ResetElectionTimer{})
	for _, peer := range n.peers {
		actions = append(actions, SendPreVoteRequest{To: peer, Request: PreVoteRequest{From: n.id, Term: term, LastLogIndex: n.lastLogIndex, LastLogTerm: n.lastLogTerm}})
	}
	if n.quorum() == 1 {
		return append(actions, n.startElection()...)
	}
	return actions
}

func (n *Node) handlePreVoteRequest(request PreVoteRequest) []Action {
	grant := request.Term >= n.term+1 && !n.recentLeaderContact && n.candidateLogIsUpToDate(request.LastLogTerm, request.LastLogIndex)
	return []Action{SendPreVoteResponse{To: request.From, Response: PreVoteResponse{From: n.id, Term: request.Term, CurrentTerm: n.term, Granted: grant}}}
}

func (n *Node) handlePreVoteResponse(response PreVoteResponse) []Action {
	if response.CurrentTerm > n.term {
		n.becomeFollower(response.CurrentTerm, "")
		return n.persistHardState(nil)
	}
	if n.role != PreCandidate || response.Term != n.term+1 || !response.Granted {
		return nil
	}
	n.preVotes[response.From] = struct{}{}
	if len(n.preVotes) < n.quorum() {
		return nil
	}
	return n.startElection()
}

func (n *Node) startElection() []Action {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.readReady = false
	n.votes = map[NodeID]struct{}{n.id: {}}

	after := make([]Action, 0, len(n.peers)+1)
	after = append(after, ResetElectionTimer{})
	for _, peer := range n.peers {
		after = append(after, SendVoteRequest{To: peer, Request: VoteRequest{From: n.id, Term: n.term, LastLogIndex: n.lastLogIndex, LastLogTerm: n.lastLogTerm}})
	}
	return n.persistHardState(after)
}

func (n *Node) hardStatePersisted(event HardStatePersisted) []Action {
	pending, ok := n.pendingHardState[event.PersistenceID]
	if !ok {
		return nil
	}
	delete(n.pendingHardState, event.PersistenceID)
	n.durableTerm = pending.term
	n.durableVotedFor = pending.votedFor
	if pending.term != n.term || pending.votedFor != n.votedFor {
		return nil
	}
	return pending.after
}

func (n *Node) logEntriesPersisted(event LogEntriesPersisted) []Action {
	pending, ok := n.pendingLog[event.PersistenceID]
	if !ok {
		return nil
	}
	delete(n.pendingLog, event.PersistenceID)
	if pending.lastIndex > n.durableLogIndex {
		n.durableLogIndex = pending.lastIndex
	}

	switch pending.purpose {
	case persistLeaderEntry:
		if n.role != Leader || n.term != pending.term || n.lastLogIndex < pending.lastIndex {
			return nil
		}
		return n.sendAppendEntries(0)
	case persistFollowerAppend:
		if n.term != pending.term || n.role != Follower || n.leaderID != pending.leader {
			return nil
		}
		return n.advanceFollowerCommit(
			pending.leaderCommit,
			n.appendEntriesResponse(pending.leader, pending.requestID, true, pending.lastIndex, 0, 0, pending.readID),
		)
	default:
		return nil
	}
}

func (n *Node) handleVoteRequest(request VoteRequest) []Action {
	if request.Term < n.term {
		return n.voteResponse(request.From, false)
	}
	termChanged := request.Term > n.term
	if termChanged {
		n.becomeFollower(request.Term, "")
	}
	canVote := n.votedFor == "" || n.votedFor == request.From
	grant := canVote && n.candidateLogIsUpToDate(request.LastLogTerm, request.LastLogIndex)
	if grant {
		n.votedFor = request.From
	}
	response := n.voteResponse(request.From, grant)
	if termChanged || (grant && !n.voteIsDurable(request.From)) {
		return n.persistHardState(response)
	}
	return response
}

func (n *Node) handleVoteResponse(response VoteResponse) []Action {
	if response.Term > n.term {
		n.becomeFollower(response.Term, "")
		return n.persistHardState(nil)
	}
	if n.role != Candidate || response.Term != n.term || !response.Granted {
		return nil
	}
	n.votes[response.From] = struct{}{}
	if len(n.votes) < n.quorum() {
		return nil
	}
	n.role = Leader
	n.leaderID = n.id
	n.activePeers = make(map[NodeID]struct{})
	n.matchIndex = make(map[NodeID]uint64)
	n.nextIndex = make(map[NodeID]uint64)
	n.appendInFlight = make(map[NodeID]pendingAppend)
	n.snapshotInFlight = make(map[NodeID]uint64)
	n.readReady = false
	entry := LogEntry{Index: n.lastLogIndex + 1, Term: n.term, Type: EntryNoOp}
	n.appendEntries([]LogEntry{entry})
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex + 1
	}
	actions := []Action{BecameLeader{Term: n.term}, ResetHeartbeatTimer{}, ResetCheckQuorumTimer{}}
	return append(actions, n.persistLog([]LogEntry{entry}, pendingLogPersistence{
		purpose:   persistLeaderEntry,
		term:      n.term,
		lastIndex: entry.Index,
	})...)
}

func (n *Node) proposeSession(proposal ProposeSession) []Action {
	if n.role != Leader {
		return []Action{ProposalRejected{ProposalID: proposal.ProposalID, LeaderID: n.leaderID}}
	}
	if proposal.Type != EntryOpenSession && proposal.Type != EntryCloseSession {
		return nil
	}
	entry := LogEntry{
		Index:     n.lastLogIndex + 1,
		Term:      n.term,
		Type:      proposal.Type,
		SessionID: proposal.SessionID,
	}
	n.appendEntries([]LogEntry{entry})
	actions := []Action{ProposalAccepted{ProposalID: proposal.ProposalID, Index: entry.Index}}
	return append(actions, n.persistLog([]LogEntry{entry}, pendingLogPersistence{
		purpose:   persistLeaderEntry,
		term:      n.term,
		lastIndex: entry.Index,
	})...)
}

func (n *Node) proposeSet(proposal ProposeSet) []Action {
	if n.role != Leader {
		return []Action{ProposalRejected{ProposalID: proposal.ProposalID, LeaderID: n.leaderID}}
	}
	entry := LogEntry{
		Index:     n.lastLogIndex + 1,
		Term:      n.term,
		Type:      EntrySet,
		SessionID: proposal.SessionID,
		Sequence:  proposal.Sequence,
		Key:       proposal.Key,
		Value:     append([]byte(nil), proposal.Value...),
	}
	n.appendEntries([]LogEntry{entry})
	actions := []Action{ProposalAccepted{ProposalID: proposal.ProposalID, Index: entry.Index}}
	return append(actions, n.persistLog([]LogEntry{entry}, pendingLogPersistence{
		purpose:   persistLeaderEntry,
		term:      n.term,
		lastIndex: entry.Index,
	})...)
}

func (n *Node) proposeDelete(proposal ProposeDelete) []Action {
	if n.role != Leader {
		return []Action{ProposalRejected{ProposalID: proposal.ProposalID, LeaderID: n.leaderID}}
	}
	entry := LogEntry{
		Index:     n.lastLogIndex + 1,
		Term:      n.term,
		Type:      EntryDelete,
		SessionID: proposal.SessionID,
		Sequence:  proposal.Sequence,
		Key:       proposal.Key,
	}
	n.appendEntries([]LogEntry{entry})
	actions := []Action{ProposalAccepted{ProposalID: proposal.ProposalID, Index: entry.Index}}
	return append(actions, n.persistLog([]LogEntry{entry}, pendingLogPersistence{
		purpose:   persistLeaderEntry,
		term:      n.term,
		lastIndex: entry.Index,
	})...)
}

func (n *Node) confirmRead(read ConfirmRead) []Action {
	if n.role != Leader {
		return []Action{ReadRejected{ReadID: read.ReadID, LeaderID: n.leaderID}}
	}
	if !n.readReady {
		return []Action{ReadRejected{ReadID: read.ReadID}}
	}
	n.pendingReads[read.ReadID] = &pendingRead{acknowledgements: map[NodeID]struct{}{n.id: {}}}
	return n.sendAppendEntries(read.ReadID)
}

func (n *Node) heartbeatRound() []Action {
	if n.role != Leader {
		return nil
	}
	// A heartbeat interval is also the replication response deadline. Expiring
	// the logical request permits retry while preserving one active request per
	// Follower and makes a lost response an ordinary retry.
	clear(n.appendInFlight)
	return append([]Action{ResetHeartbeatTimer{}}, n.sendAppendEntries(0)...)
}

func (n *Node) sendAppendEntries(readID ReadID) []Action {
	actions := make([]Action, 0, len(n.peers))
	for _, peer := range n.peers {
		peerReadID := readID
		if peerReadID == 0 {
			peerReadID = n.pendingReadFor(peer)
		}
		actions = append(actions, n.sendAppendEntriesTo(peer, peerReadID)...)
	}
	return actions
}

func (n *Node) sendAppendEntriesTo(peer NodeID, readID ReadID) []Action {
	if n.appendInFlight[peer].requestID != 0 {
		return nil
	}
	durableLastIndex := min(n.durableLogIndex, n.lastLogIndex)
	nextIndex := n.nextIndex[peer]
	if nextIndex == 0 || nextIndex > durableLastIndex+1 {
		nextIndex = durableLastIndex + 1
	}
	// A peer behind the compacted prefix requires InstallSnapshot, which is
	// emitted by the Snapshot-transfer slice rather than fabricating log data.
	if nextIndex <= n.logBaseIndex {
		if n.snapshotInFlight[peer] != 0 {
			return nil
		}
		n.nextRequestID++
		n.snapshotInFlight[peer] = n.nextRequestID
		return []Action{SendInstallSnapshot{
			To: peer, Term: n.term, RequestID: n.nextRequestID,
			SnapshotIndex: n.logBaseIndex, SnapshotTerm: n.logBaseTerm,
		}}
	}
	prevIndex := nextIndex - 1
	n.nextRequestID++
	request := AppendEntries{
		From:         n.id,
		Term:         n.term,
		RequestID:    n.nextRequestID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.termAt(prevIndex),
		Entries:      n.entriesBatch(nextIndex, durableLastIndex),
		LeaderCommit: n.commitIndex,
		ReadID:       readID,
	}
	n.appendInFlight[peer] = pendingAppend{requestID: request.RequestID, leaderCommit: request.LeaderCommit}
	return []Action{SendAppendEntries{To: peer, Request: request}}
}

func (n *Node) handleAppendEntries(request AppendEntries) []Action {
	if request.Term < n.term {
		return n.appendEntriesResponse(request.From, request.RequestID, false, n.lastLogIndex, 0, 0, request.ReadID)
	}
	termChanged := request.Term > n.term
	if termChanged {
		n.becomeFollower(request.Term, request.From)
	} else {
		n.role = Follower
		n.leaderID = request.From
		n.readReady = false
	}
	n.recentLeaderContact = true
	after := []Action{ResetElectionTimer{}}
	if !n.matches(request.PrevLogIndex, request.PrevLogTerm) {
		conflictTerm, conflictIndex := n.conflictHint(request.PrevLogIndex)
		after = append(after, n.appendEntriesResponse(request.From, request.RequestID, false, n.lastLogIndex, conflictTerm, conflictIndex, request.ReadID)...)
		if termChanged {
			return n.persistHardState(after)
		}
		return after
	}

	newEntries, truncateFrom, ok := n.entriesToAppend(request.PrevLogIndex, request.Entries)
	if !ok {
		conflictTerm, conflictIndex := n.conflictHint(request.PrevLogIndex + 1)
		after = append(after, n.appendEntriesResponse(request.From, request.RequestID, false, n.lastLogIndex, conflictTerm, conflictIndex, request.ReadID)...)
		if termChanged {
			return n.persistHardState(after)
		}
		return after
	}
	if len(newEntries) > 0 {
		if truncateFrom != 0 {
			n.truncateLog(truncateFrom)
		}
		n.appendEntries(newEntries)
		after = append(after, n.persistLog(newEntries, pendingLogPersistence{
			purpose:      persistFollowerAppend,
			term:         request.Term,
			leader:       request.From,
			truncateFrom: truncateFrom,
			lastIndex:    n.lastLogIndex,
			leaderCommit: request.LeaderCommit,
			readID:       request.ReadID,
			requestID:    request.RequestID,
		})...)
	} else {
		if n.lastLogIndex > n.durableLogIndex {
			if termChanged {
				return n.persistHardState(after)
			}
			return after
		}
		after = append(after, n.advanceFollowerCommit(
			request.LeaderCommit,
			n.appendEntriesResponse(request.From, request.RequestID, true, n.lastLogIndex, 0, 0, request.ReadID),
		)...)
	}
	if termChanged {
		return n.persistHardState(after)
	}
	return after
}

func (n *Node) handleAppendEntriesResponse(response AppendEntriesResponse) []Action {
	if response.Term > n.term {
		n.becomeFollower(response.Term, "")
		return n.persistHardState(nil)
	}
	if n.role != Leader || response.Term != n.term {
		return nil
	}
	pending := n.appendInFlight[response.From]
	if pending.requestID != 0 && response.RequestID != 0 && response.RequestID != pending.requestID {
		return nil
	}
	delete(n.appendInFlight, response.From)
	n.activePeers[response.From] = struct{}{}
	if response.Success {
		if response.MatchIndex > n.matchIndex[response.From] {
			n.matchIndex[response.From] = response.MatchIndex
		}
		n.nextIndex[response.From] = n.matchIndex[response.From] + 1
	} else {
		n.nextIndex[response.From] = n.nextIndexFromConflict(response)
	}
	var actions []Action
	if n.advanceLeaderCommit() {
		actions = n.persistCommit(n.sendAppendEntries(0))
	} else {
		readID := n.pendingReadFor(response.From)
		durableLastIndex := min(n.durableLogIndex, n.lastLogIndex)
		if !response.Success || n.nextIndex[response.From] <= durableLastIndex || readID != 0 || pending.leaderCommit < n.commitIndex {
			actions = n.sendAppendEntriesTo(response.From, readID)
		}
	}
	return append(actions, n.acknowledgeRead(response)...)
}

func (n *Node) handleAppendEntriesFailed(event AppendEntriesFailed) []Action {
	if n.appendInFlight[event.To].requestID == event.RequestID {
		delete(n.appendInFlight, event.To)
	}
	if n.snapshotInFlight[event.To] == event.RequestID {
		delete(n.snapshotInFlight, event.To)
	}
	return nil
}

func (n *Node) handleInstallSnapshot(request InstallSnapshot) []Action {
	if request.Term < n.term {
		return n.snapshotResponse(request, false, request.Offset, false)
	}
	termChanged := request.Term > n.term
	if termChanged {
		n.becomeFollower(request.Term, request.From)
	} else {
		n.role = Follower
		n.leaderID = request.From
		n.readReady = false
	}
	n.recentLeaderContact = true
	if request.Installed && request.SnapshotIndex > n.logBaseIndex {
		first := 0
		for first < len(n.log) && n.log[first].Index <= request.SnapshotIndex {
			first++
		}
		if request.SnapshotIndex <= n.lastLogIndex && n.termAt(request.SnapshotIndex) == request.SnapshotTerm {
			n.log = cloneLogEntries(n.log[first:])
		} else {
			n.log = nil
			n.lastLogIndex = request.SnapshotIndex
			n.lastLogTerm = request.SnapshotTerm
			n.durableLogIndex = request.SnapshotIndex
		}
		n.logBaseIndex = request.SnapshotIndex
		n.logBaseTerm = request.SnapshotTerm
		n.commitIndex = max(n.commitIndex, request.SnapshotIndex)
		n.durableCommitIndex = max(n.durableCommitIndex, request.SnapshotIndex)
		n.lastApplied = request.SnapshotIndex
		if len(n.log) > 0 {
			n.lastLogIndex = n.log[len(n.log)-1].Index
			n.lastLogTerm = n.log[len(n.log)-1].Term
			n.durableLogIndex = n.lastLogIndex
		}
	}
	after := append([]Action{ResetElectionTimer{}}, n.snapshotResponse(request, request.Success, request.NextOffset, request.Installed)...)
	if termChanged {
		return n.persistHardState(after)
	}
	return after
}

func (n *Node) snapshotResponse(request InstallSnapshot, success bool, nextOffset uint64, done bool) []Action {
	return []Action{SendInstallSnapshotResponse{To: request.From, Response: InstallSnapshotResponse{
		From: n.id, Term: n.term, RequestID: request.RequestID, SnapshotIndex: request.SnapshotIndex,
		Success: success, NextOffset: nextOffset, Done: done,
	}}}
}

func (n *Node) handleInstallSnapshotResponse(response InstallSnapshotResponse) []Action {
	if response.Term > n.term {
		n.becomeFollower(response.Term, "")
		return n.persistHardState(nil)
	}
	if n.role != Leader || response.Term != n.term || n.snapshotInFlight[response.From] != response.RequestID {
		return nil
	}
	if !response.Success {
		delete(n.snapshotInFlight, response.From)
		return nil
	}
	if !response.Done {
		return []Action{SendInstallSnapshot{To: response.From, Term: n.term, RequestID: response.RequestID,
			SnapshotIndex: n.logBaseIndex, SnapshotTerm: n.logBaseTerm, Offset: response.NextOffset}}
	}
	delete(n.snapshotInFlight, response.From)
	n.matchIndex[response.From] = max(n.matchIndex[response.From], response.SnapshotIndex)
	n.nextIndex[response.From] = response.SnapshotIndex + 1
	return n.sendAppendEntriesTo(response.From, n.pendingReadFor(response.From))
}

func (n *Node) acknowledgeRead(response AppendEntriesResponse) []Action {
	read := n.pendingReads[response.ReadID]
	if response.ReadID == 0 || read == nil || read.confirmed {
		return nil
	}
	read.acknowledgements[response.From] = struct{}{}
	if len(read.acknowledgements) < n.quorum() {
		return nil
	}
	read.confirmed = true
	read.commitIndex = n.commitIndex
	return n.completeReads()
}

func (n *Node) checkQuorum() []Action {
	if n.role != Leader {
		return nil
	}
	if len(n.activePeers)+1 < n.quorum() {
		term := n.term
		n.becomeFollower(term, "")
		return []Action{ResetElectionTimer{}, LostLeadership{Term: term}}
	}
	n.activePeers = make(map[NodeID]struct{})
	return []Action{ResetCheckQuorumTimer{}}
}

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	if term > n.term {
		n.votedFor = ""
	}
	n.role = Follower
	n.term = term
	n.leaderID = leader
	n.recentLeaderContact = leader != ""
	n.votes = make(map[NodeID]struct{})
	n.preVotes = make(map[NodeID]struct{})
	n.activePeers = make(map[NodeID]struct{})
	n.matchIndex = make(map[NodeID]uint64)
	n.nextIndex = make(map[NodeID]uint64)
	n.appendInFlight = make(map[NodeID]pendingAppend)
	n.snapshotInFlight = make(map[NodeID]uint64)
	n.pendingReads = make(map[ReadID]*pendingRead)
	n.readReady = false
}

func (n *Node) candidateLogIsUpToDate(term, index uint64) bool {
	return logIsAtLeastAsUpToDate(term, index, n.lastLogTerm, n.lastLogIndex)
}

func logIsAtLeastAsUpToDate(candidateTerm, candidateIndex, localTerm, localIndex uint64) bool {
	return candidateTerm > localTerm || (candidateTerm == localTerm && candidateIndex >= localIndex)
}

func (n *Node) voteIsDurable(candidate NodeID) bool {
	return n.durableTerm == n.term && n.durableVotedFor == candidate
}

func (n *Node) voteResponse(to NodeID, granted bool) []Action {
	return []Action{SendVoteResponse{To: to, Response: VoteResponse{From: n.id, Term: n.term, Granted: granted}}}
}

func (n *Node) appendEntriesResponse(to NodeID, requestID uint64, success bool, matchIndex, conflictTerm, conflictIndex uint64, readID ReadID) []Action {
	return []Action{SendAppendEntriesResponse{To: to, Response: AppendEntriesResponse{
		From:          n.id,
		Term:          n.term,
		RequestID:     requestID,
		Success:       success,
		MatchIndex:    matchIndex,
		ConflictTerm:  conflictTerm,
		ConflictIndex: conflictIndex,
		ReadID:        readID,
	}}}
}

func (n *Node) persistHardState(after []Action) []Action {
	n.nextHardStatePersistence++
	id := n.nextHardStatePersistence
	n.pendingHardState[id] = pendingHardStatePersistence{term: n.term, votedFor: n.votedFor, after: after}
	return []Action{PersistHardState{PersistenceID: id, Term: n.term, VotedFor: n.votedFor}}
}

func (n *Node) persistLog(entries []LogEntry, pending pendingLogPersistence) []Action {
	n.nextLogPersistence++
	id := n.nextLogPersistence
	n.pendingLog[id] = pending
	return []Action{PersistLogEntries{
		PersistenceID: id,
		TruncateFrom:  pending.truncateFrom,
		Entries:       cloneLogEntries(entries),
	}}
}

func (n *Node) persistCommit(after []Action) []Action {
	n.nextCommitPersistence++
	id := n.nextCommitPersistence
	n.pendingCommit[id] = pendingCommitPersistence{
		commitIndex: n.commitIndex,
		after:       after,
	}
	return []Action{PersistCommitIndex{PersistenceID: id, CommitIndex: n.commitIndex}}
}

func (n *Node) commitIndexPersisted(event CommitIndexPersisted) []Action {
	pending, ok := n.pendingCommit[event.PersistenceID]
	if !ok {
		return nil
	}
	delete(n.pendingCommit, event.PersistenceID)
	if pending.commitIndex > n.durableCommitIndex {
		n.durableCommitIndex = pending.commitIndex
	}
	actions := n.applyThrough(pending.commitIndex)
	return append(actions, pending.after...)
}

func (n *Node) compactLog(index, term uint64) {
	if index <= n.logBaseIndex || index > n.lastApplied || n.termAt(index) != term {
		return
	}
	first := index - n.logBaseIndex
	n.log = cloneLogEntries(n.log[first:])
	n.logBaseIndex, n.logBaseTerm = index, term
}

func (n *Node) appendEntries(entries []LogEntry) {
	n.log = append(n.log, cloneLogEntries(entries)...)
	n.lastLogIndex = n.log[len(n.log)-1].Index
	n.lastLogTerm = n.log[len(n.log)-1].Term
}

func (n *Node) matches(index, term uint64) bool {
	return index == 0 && term == 0 || index <= n.lastLogIndex && n.termAt(index) == term
}

func (n *Node) termAt(index uint64) uint64 {
	if index == n.logBaseIndex {
		return n.logBaseTerm
	}
	if index <= n.logBaseIndex || index > n.lastLogIndex {
		return 0
	}
	return n.log[index-n.logBaseIndex-1].Term
}

func (n *Node) entriesBatch(first, last uint64) []LogEntry {
	if first == 0 || first > last || last > n.lastLogIndex {
		return nil
	}
	if first <= n.logBaseIndex {
		return nil
	}
	bytes := 0
	end := first - 1
	for index := first; index <= last; index++ {
		entry := n.log[index-n.logBaseIndex-1]
		entryBytes := 64 + len(entry.Key) + len(entry.Value)
		if bytes+entryBytes > maxAppendEntriesBytes && end >= first {
			break
		}
		bytes += entryBytes
		end = index
	}
	startOffset := first - n.logBaseIndex - 1
	endOffset := end - n.logBaseIndex
	return cloneLogEntries(n.log[startOffset:endOffset])
}

func (n *Node) conflictHint(index uint64) (uint64, uint64) {
	if index == 0 || index > n.lastLogIndex {
		return 0, n.lastLogIndex + 1
	}
	term := n.termAt(index)
	first := index
	for first > n.logBaseIndex+1 && n.termAt(first-1) == term {
		first--
	}
	return term, first
}

func (n *Node) nextIndexFromConflict(response AppendEntriesResponse) uint64 {
	next := response.ConflictIndex
	if response.ConflictTerm != 0 {
		for index := n.lastLogIndex; index > 0; index-- {
			if n.termAt(index) == response.ConflictTerm {
				next = index + 1
				break
			}
		}
	}
	if next == 0 {
		next = 1
	}
	return min(next, n.lastLogIndex+1)
}

func (n *Node) pendingReadFor(peer NodeID) ReadID {
	readIDs := make([]ReadID, 0, len(n.pendingReads))
	for id, read := range n.pendingReads {
		if _, acknowledged := read.acknowledgements[peer]; !acknowledged {
			readIDs = append(readIDs, id)
		}
	}
	sort.Slice(readIDs, func(i, j int) bool { return readIDs[i] < readIDs[j] })
	if len(readIDs) == 0 {
		return 0
	}
	return readIDs[0]
}

func (n *Node) entriesToAppend(prevIndex uint64, entries []LogEntry) ([]LogEntry, uint64, bool) {
	for offset, entry := range entries {
		wantIndex := prevIndex + uint64(offset) + 1
		if entry.Index != wantIndex {
			return nil, 0, false
		}
		if entry.Index <= n.lastLogIndex {
			if n.termAt(entry.Index) != entry.Term {
				if entry.Index <= n.commitIndex {
					return nil, 0, false
				}
				return cloneLogEntries(entries[offset:]), entry.Index, true
			}
			continue
		}
		if entry.Index != n.lastLogIndex+1 {
			return nil, 0, false
		}
		return cloneLogEntries(entries[offset:]), 0, true
	}
	return nil, 0, true
}

func (n *Node) truncateLog(firstIndex uint64) {
	n.log = n.log[:firstIndex-n.logBaseIndex-1]
	n.lastLogIndex = firstIndex - 1
	n.lastLogTerm = n.termAt(n.lastLogIndex)
	n.durableLogIndex = min(n.durableLogIndex, n.lastLogIndex)
}

func cloneLogEntries(entries []LogEntry) []LogEntry {
	cloned := make([]LogEntry, len(entries))
	for index, entry := range entries {
		cloned[index] = entry
		cloned[index].Value = append([]byte(nil), entry.Value...)
	}
	return cloned
}

func (n *Node) advanceLeaderCommit() bool {
	for index := n.lastLogIndex; index > n.commitIndex; index-- {
		if n.termAt(index) != n.term {
			continue
		}
		replicated := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				replicated++
			}
		}
		if replicated >= n.quorum() {
			n.commitIndex = index
			return true
		}
	}
	return false
}

func (n *Node) advanceFollowerCommit(leaderCommit uint64, after []Action) []Action {
	target := min(leaderCommit, n.lastLogIndex)
	if target > n.commitIndex {
		n.commitIndex = target
		return n.persistCommit(after)
	}
	return append(n.applyThrough(n.durableCommitIndex), after...)
}

func (n *Node) applyThrough(index uint64) []Action {
	if index <= n.lastApplied {
		return n.completeReads()
	}
	actions := make([]Action, 0, index-n.lastApplied+1)
	for n.lastApplied < index {
		n.lastApplied++
		entry := n.log[n.lastApplied-n.logBaseIndex-1]
		actions = append(actions, ApplyEntry{Entry: entry})
		if n.role == Leader && !n.readReady && entry.Type == EntryNoOp && entry.Term == n.term {
			n.readReady = true
			actions = append(actions, BecameReadReady{Term: n.term})
		}
	}
	return append(actions, n.completeReads()...)
}

func (n *Node) completeReads() []Action {
	var actions []Action
	readIDs := make([]ReadID, 0, len(n.pendingReads))
	for readID := range n.pendingReads {
		readIDs = append(readIDs, readID)
	}
	sort.Slice(readIDs, func(i, j int) bool { return readIDs[i] < readIDs[j] })
	for _, readID := range readIDs {
		read := n.pendingReads[readID]
		if !read.confirmed || n.lastApplied < read.commitIndex {
			continue
		}
		actions = append(actions, ReadConfirmed{ReadID: readID, CommitIndex: read.commitIndex})
		delete(n.pendingReads, readID)
	}
	return actions
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }
