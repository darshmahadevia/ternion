package node

import (
	"context"
	"crypto/rand"
	"fmt"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sessionFailure uint8

const (
	sessionSucceeded sessionFailure = iota
	sessionLimitReached
	sessionUnknown
	sessionClosed
	sessionAlreadyExists
	sessionStaleSequence
	sessionOutOfOrderSequence
)

type proposalResult struct {
	sessionID    raft.SessionID
	failure      sessionFailure
	sequence     uint64
	wantSequence uint64
	leaderID     raft.NodeID
	rejected     bool
	existed      bool
}

type sessionState uint8

const (
	sessionActive sessionState = iota
	sessionPermanentlyClosed
)

// sessionMachine is owned exclusively by the Raft runtime loop. Applying the
// same committed entries therefore produces the same state on every Node.
type sessionMachine struct {
	limit    int
	active   int
	sessions map[raft.SessionID]sessionRecord
	values   map[string][]byte
}

type sessionRecord struct {
	state              sessionState
	lastSequence       uint64
	lastMutationResult sessionFailure
	lastDeleteExisted  bool
}

func newSessionMachine(limit int) *sessionMachine {
	return &sessionMachine{limit: limit, sessions: make(map[raft.SessionID]sessionRecord), values: make(map[string][]byte)}
}

func (m *sessionMachine) apply(entry raft.LogEntry) proposalResult {
	result := proposalResult{sessionID: entry.SessionID}
	switch entry.Type {
	case raft.EntryNoOp:
		return result
	case raft.EntryOpenSession:
		if _, exists := m.sessions[entry.SessionID]; exists {
			result.failure = sessionAlreadyExists
			return result
		}
		if m.active >= m.limit {
			result.failure = sessionLimitReached
			return result
		}
		m.sessions[entry.SessionID] = sessionRecord{state: sessionActive}
		m.active++
		return result
	case raft.EntryCloseSession:
		record, exists := m.sessions[entry.SessionID]
		if !exists {
			result.failure = sessionUnknown
			return result
		}
		if record.state == sessionPermanentlyClosed {
			result.failure = sessionClosed
			return result
		}
		record.state = sessionPermanentlyClosed
		m.sessions[entry.SessionID] = record
		m.active--
		return result
	case raft.EntrySet:
		result, apply := m.evaluateMutation(entry.SessionID, entry.Sequence)
		if !apply {
			return result
		}
		record := m.sessions[entry.SessionID]
		m.values[entry.Key] = append([]byte(nil), entry.Value...)
		record.lastSequence = entry.Sequence
		record.lastMutationResult = result.failure
		record.lastDeleteExisted = false
		m.sessions[entry.SessionID] = record
		return result
	case raft.EntryDelete:
		result, apply := m.evaluateMutation(entry.SessionID, entry.Sequence)
		if !apply {
			return result
		}
		record := m.sessions[entry.SessionID]
		_, result.existed = m.values[entry.Key]
		delete(m.values, entry.Key)
		record.lastSequence = entry.Sequence
		record.lastMutationResult = result.failure
		record.lastDeleteExisted = result.existed
		m.sessions[entry.SessionID] = record
		return result
	default:
		panic(fmt.Sprintf("apply unsupported Raft entry type %d", entry.Type))
	}
}

// evaluateMutation decides whether a command is the next mutation to apply. The latest
// sequence replays its cached result, so a retry never creates a second effect.
func (m *sessionMachine) evaluateMutation(sessionID raft.SessionID, sequence uint64) (proposalResult, bool) {
	result := proposalResult{sessionID: sessionID, sequence: sequence}
	record, exists := m.sessions[sessionID]
	if !exists {
		result.failure = sessionUnknown
		return result, false
	}
	if record.state == sessionPermanentlyClosed {
		result.failure = sessionClosed
		return result, false
	}
	if sequence == record.lastSequence && sequence != 0 {
		result.failure = record.lastMutationResult
		result.existed = record.lastDeleteExisted
		return result, false
	}
	if sequence < record.lastSequence {
		result.failure = sessionStaleSequence
		result.wantSequence = record.lastSequence
		return result, false
	}
	if sequence != record.lastSequence+1 {
		result.failure = sessionOutOfOrderSequence
		result.wantSequence = record.lastSequence + 1
		return result, false
	}
	return result, true
}

func (m *sessionMachine) get(key string) ([]byte, bool) {
	value, exists := m.values[key]
	return append([]byte(nil), value...), exists
}

func (n *Node) OpenSession(ctx context.Context, _ *quorumkvv1.OpenSessionRequest) (*quorumkvv1.OpenSessionResponse, error) {
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	var sessionID raft.SessionID
	if _, err := rand.Read(sessionID[:]); err != nil {
		return nil, status.Errorf(codes.Internal, "generate Client Session identity: %v", err)
	}
	result, err := n.proposeSession(ctx, raft.EntryOpenSession, sessionID)
	if err != nil {
		return nil, err
	}
	return &quorumkvv1.OpenSessionResponse{SessionId: result.sessionID[:]}, nil
}

func (n *Node) CloseSession(ctx context.Context, request *quorumkvv1.CloseSessionRequest) (*quorumkvv1.CloseSessionResponse, error) {
	if len(request.SessionId) != len(raft.SessionID{}) {
		return nil, validationError("session_id", "Client Session identity is %d bytes, want 16", len(request.SessionId))
	}
	var sessionID raft.SessionID
	copy(sessionID[:], request.SessionId)
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	if _, err := n.proposeSession(ctx, raft.EntryCloseSession, sessionID); err != nil {
		return nil, err
	}
	return &quorumkvv1.CloseSessionResponse{}, nil
}

func (n *Node) rejectIfNotLeader() (proposalResult, bool) {
	state := n.raftState.Load().(raft.State)
	if state.Role == raft.Leader {
		return proposalResult{}, false
	}
	return proposalResult{leaderID: state.LeaderID, rejected: true}, true
}

func (n *Node) proposeSession(ctx context.Context, entryType raft.EntryType, sessionID raft.SessionID) (proposalResult, error) {
	proposalID := raft.ProposalID(n.nextProposal.Add(1))
	result, err := n.propose(ctx, raft.ProposeSession{ProposalID: proposalID, Type: entryType, SessionID: sessionID})
	if err != nil {
		return proposalResult{}, err
	}
	return result, n.proposalError(result)
}

func (n *Node) proposalError(result proposalResult) error {
	if result.rejected {
		if result.leaderID == "" {
			return status.Error(codes.Unavailable, "Leader is unknown")
		}
		leader, ok := n.config.Members[string(result.leaderID)]
		if !ok {
			return status.Errorf(codes.Unavailable, "Leader %q is not in the configured member map", result.leaderID)
		}
		base := status.New(codes.FailedPrecondition, fmt.Sprintf("Node %q is not the Leader; retry Node %q", n.config.Node.ID, result.leaderID))
		withDetails, err := base.WithDetails(&quorumkvv1.NotLeader{LeaderId: string(result.leaderID), LeaderAddress: leader.ClientAddress})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	}
	switch result.failure {
	case sessionSucceeded:
		return nil
	case sessionLimitReached:
		return status.Errorf(codes.ResourceExhausted, "active Client Session limit %d reached", n.config.ActiveSessionLimit)
	case sessionUnknown:
		base := status.New(codes.NotFound, "Client Session is unknown")
		withDetails, err := base.WithDetails(&quorumkvv1.InvalidSession{SessionId: result.sessionID[:], Reason: quorumkvv1.InvalidSessionReason_INVALID_SESSION_REASON_UNKNOWN})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	case sessionClosed:
		base := status.New(codes.FailedPrecondition, "Client Session is closed and cannot be reused")
		withDetails, err := base.WithDetails(&quorumkvv1.InvalidSession{SessionId: result.sessionID[:], Reason: quorumkvv1.InvalidSessionReason_INVALID_SESSION_REASON_CLOSED})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	case sessionAlreadyExists:
		return status.Error(codes.AlreadyExists, "Client Session identity was already used and cannot be reopened")
	case sessionStaleSequence:
		base := status.New(codes.FailedPrecondition, fmt.Sprintf("mutation sequence %d is stale; latest is %d", result.sequence, result.wantSequence))
		withDetails, err := base.WithDetails(&quorumkvv1.StaleSequence{ReceivedSequence: result.sequence, LastSequence: result.wantSequence})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	case sessionOutOfOrderSequence:
		base := status.New(codes.FailedPrecondition, fmt.Sprintf("mutation sequence %d is out of order; next is %d", result.sequence, result.wantSequence))
		withDetails, err := base.WithDetails(&quorumkvv1.OutOfOrderSequence{ReceivedSequence: result.sequence, NextSequence: result.wantSequence})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	default:
		return status.Error(codes.Internal, "Client Session command returned an unknown result")
	}
}
