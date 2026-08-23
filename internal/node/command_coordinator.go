package node

import (
	"context"
	"fmt"

	"github.com/darshmahadevia/ternion/internal/raft"
	"github.com/darshmahadevia/ternion/internal/snapshot"
)

// runtimeInput is a closed set of messages accepted by the single Raft owner.
// Distinct types prevent callers from constructing invalid field combinations.
type runtimeInput interface{ isRuntimeInput() }

type raftEventInput struct{ event raft.Event }

func (raftEventInput) isRuntimeInput() {}

type proposalInput struct {
	event  raft.Event
	result chan proposalResult
	ctx    context.Context
}

func (proposalInput) isRuntimeInput() {}

type readInput struct {
	event  raft.ConfirmRead
	result chan readResult
	ctx    context.Context
	key    string
}

func (readInput) isRuntimeInput() {}

type snapshotInput struct{ result chan error }

func (snapshotInput) isRuntimeInput() {}

type pendingProposal struct {
	result chan proposalResult
	ctx    context.Context
}

type inFlightMutation struct {
	sequence uint64
	index    uint64
}

type pendingRead struct {
	result chan readResult
	ctx    context.Context
	key    string
}

// commandCoordinator owns replicated Command state and every Client request
// waiting for a Raft result. The Raft loop is its only caller and goroutine.
type commandCoordinator struct {
	state             *sessionMachine
	pending           map[uint64][]pendingProposal
	inFlightMutations map[raft.SessionID]inFlightMutation
	pendingReads      map[raft.ReadID]pendingRead
	metrics           *nodeMetrics
	observeMutation   mutationObserver
}

func newCommandCoordinator(limit int, metrics *nodeMetrics, observeMutation mutationObserver) *commandCoordinator {
	return &commandCoordinator{
		state:             newSessionMachine(limit),
		pending:           make(map[uint64][]pendingProposal),
		inFlightMutations: make(map[raft.SessionID]inFlightMutation),
		pendingReads:      make(map[raft.ReadID]pendingRead),
		metrics:           metrics,
		observeMutation:   observeMutation,
	}
}

func (c *commandCoordinator) restore(state *snapshot.State) error {
	return c.state.restore(state)
}

func (c *commandCoordinator) snapshot(identity snapshot.Identity, includedIndex, includedTerm uint64) snapshot.State {
	return c.state.snapshot(identity, includedIndex, includedTerm)
}

func (c *commandCoordinator) recover(actions []raft.Action) error {
	for _, action := range actions {
		apply, ok := action.(raft.ApplyEntry)
		if !ok {
			return fmt.Errorf("recover committed state: unexpected Raft action %T", action)
		}
		c.state.apply(apply.Entry)
	}
	return nil
}

func (c *commandCoordinator) pruneCanceled() {
	for index, proposals := range c.pending {
		active := proposals[:0]
		for _, proposal := range proposals {
			if proposal.ctx.Err() == nil {
				active = append(active, proposal)
			}
		}
		if len(active) == 0 {
			delete(c.pending, index)
		} else {
			c.pending[index] = active
		}
	}
	for readID, read := range c.pendingReads {
		if read.ctx.Err() != nil {
			delete(c.pendingReads, readID)
		}
	}
}

// beginProposal either returns an event for Raft or completes the request from
// replicated state when it is a duplicate or invalid mutation.
func (c *commandCoordinator) beginProposal(input proposalInput) (raft.Event, bool) {
	sessionID, sequence, mutation := proposedMutation(input.event)
	if !mutation {
		return input.event, true
	}
	if inFlight, exists := c.inFlightMutations[sessionID]; exists && inFlight.sequence == sequence {
		c.pending[inFlight.index] = append(c.pending[inFlight.index], pendingProposal{result: input.result, ctx: input.ctx})
		return nil, false
	}
	if result, shouldPropose := c.state.evaluateMutation(sessionID, sequence); !shouldPropose {
		input.result <- result
		return nil, false
	}
	if c.observeMutation != nil {
		entry, _ := entryForMutation(input.event)
		c.observeMutation(mutationBeforeAppend, entry)
	}
	return input.event, true
}

func (c *commandCoordinator) beginRead(input readInput) raft.Event {
	c.pendingReads[input.event.ReadID] = pendingRead{result: input.result, ctx: input.ctx, key: input.key}
	return input.event
}

// handle consumes Command-related Raft actions. It reports whether the action
// was fully handled so the runtime loop only sees transport and timer effects.
func (c *commandCoordinator) handle(action raft.Action, current runtimeInput) bool {
	switch action := action.(type) {
	case raft.ProposalAccepted:
		if c.metrics != nil {
			c.metrics.proposals.Add(1)
		}
		input, ok := current.(proposalInput)
		if !ok {
			return true
		}
		c.pending[action.Index] = append(c.pending[action.Index], pendingProposal{result: input.result, ctx: input.ctx})
		if sessionID, sequence, mutation := proposedMutation(input.event); mutation {
			c.inFlightMutations[sessionID] = inFlightMutation{sequence: sequence, index: action.Index}
		}
		return true
	case raft.ProposalRejected:
		if input, ok := current.(proposalInput); ok {
			input.result <- proposalResult{leaderID: action.LeaderID, rejected: true}
		}
		return true
	case raft.ApplyEntry:
		result := c.state.apply(action.Entry)
		if c.observeMutation != nil && isMutationEntry(action.Entry) {
			c.observeMutation(mutationAfterApplication, action.Entry)
		}
		if isMutationEntry(action.Entry) && c.inFlightMutations[action.Entry.SessionID].sequence == action.Entry.Sequence {
			delete(c.inFlightMutations, action.Entry.SessionID)
		}
		if proposals, ok := c.pending[action.Entry.Index]; ok {
			for _, proposal := range proposals {
				proposal.result <- result
			}
			delete(c.pending, action.Entry.Index)
		}
		return true
	case raft.ReadConfirmed:
		read, ok := c.pendingReads[action.ReadID]
		if ok {
			value, found := c.state.get(read.key)
			read.result <- readResult{value: value, found: found}
			delete(c.pendingReads, action.ReadID)
		}
		return true
	case raft.ReadRejected:
		read, ok := c.pendingReads[action.ReadID]
		if ok {
			read.result <- readResult{leaderID: action.LeaderID, rejected: true}
			delete(c.pendingReads, action.ReadID)
		}
		return true
	default:
		return false
	}
}

func (c *commandCoordinator) lostLeadership(leaderID raft.NodeID) {
	for index, proposals := range c.pending {
		for _, proposal := range proposals {
			proposal.result <- proposalResult{leaderID: leaderID, rejected: true}
		}
		delete(c.pending, index)
	}
	clear(c.inFlightMutations)
	for readID, read := range c.pendingReads {
		read.result <- readResult{leaderID: leaderID, rejected: true}
		delete(c.pendingReads, readID)
	}
}

func proposedMutation(event raft.Event) (raft.SessionID, uint64, bool) {
	switch event := event.(type) {
	case raft.ProposeSet:
		return event.SessionID, event.Sequence, true
	case raft.ProposeDelete:
		return event.SessionID, event.Sequence, true
	default:
		return raft.SessionID{}, 0, false
	}
}

func isMutationEntry(entry raft.LogEntry) bool {
	return entry.Type == raft.EntrySet || entry.Type == raft.EntryDelete
}
