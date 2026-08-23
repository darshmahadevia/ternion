package node

import (
	"context"
	"testing"

	"github.com/darshmahadevia/ternion/internal/raft"
	"github.com/darshmahadevia/ternion/internal/snapshot"
)

func TestCommandCoordinatorCompletesDuplicateInFlightMutationTogether(t *testing.T) {
	coordinator := newCommandCoordinator(10, nil, nil)
	sessionID := raft.SessionID{1}
	mustRecoverCommands(t, coordinator, raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryOpenSession, SessionID: sessionID})

	event := raft.ProposeSet{ProposalID: 1, SessionID: sessionID, Sequence: 1, Key: "key", Value: []byte("value")}
	first := proposalInput{event: event, result: make(chan proposalResult, 1), ctx: context.Background()}
	if _, shouldStep := coordinator.beginProposal(first); !shouldStep {
		t.Fatal("first mutation was completed before reaching Raft")
	}
	coordinator.handle(raft.ProposalAccepted{ProposalID: 1, Index: 2}, first)

	second := proposalInput{event: event, result: make(chan proposalResult, 1), ctx: context.Background()}
	if _, shouldStep := coordinator.beginProposal(second); shouldStep {
		t.Fatal("duplicate in-flight mutation reached Raft")
	}
	coordinator.handle(raft.ApplyEntry{Entry: raft.LogEntry{Index: 2, Term: 1, Type: raft.EntrySet, SessionID: sessionID, Sequence: 1, Key: "key", Value: []byte("value")}}, nil)

	assertProposalSucceeded(t, first.result)
	assertProposalSucceeded(t, second.result)

	read := readInput{event: raft.ConfirmRead{ReadID: 1}, result: make(chan readResult, 1), ctx: context.Background(), key: "key"}
	coordinator.beginRead(read)
	coordinator.handle(raft.ReadConfirmed{ReadID: 1}, read)
	result := <-read.result
	if !result.found || string(result.value) != "value" {
		t.Fatalf("confirmed read = %#v, want value", result)
	}
}

func TestCommandCoordinatorDropsCanceledWaiterWithoutRetractingMutation(t *testing.T) {
	coordinator := newCommandCoordinator(10, nil, nil)
	sessionID := raft.SessionID{2}
	mustRecoverCommands(t, coordinator, raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryOpenSession, SessionID: sessionID})

	ctx, cancel := context.WithCancel(context.Background())
	input := proposalInput{
		event:  raft.ProposeDelete{ProposalID: 1, SessionID: sessionID, Sequence: 1, Key: "missing"},
		result: make(chan proposalResult, 1),
		ctx:    ctx,
	}
	if _, shouldStep := coordinator.beginProposal(input); !shouldStep {
		t.Fatal("mutation was completed before reaching Raft")
	}
	coordinator.handle(raft.ProposalAccepted{ProposalID: 1, Index: 2}, input)
	cancel()
	coordinator.pruneCanceled()
	coordinator.handle(raft.ApplyEntry{Entry: raft.LogEntry{Index: 2, Term: 1, Type: raft.EntryDelete, SessionID: sessionID, Sequence: 1, Key: "missing"}}, nil)

	select {
	case result := <-input.result:
		t.Fatalf("canceled waiter received result %#v", result)
	default:
	}

	retry := proposalInput{event: input.event, result: make(chan proposalResult, 1), ctx: context.Background()}
	if _, shouldStep := coordinator.beginProposal(retry); shouldStep {
		t.Fatal("applied retry reached Raft")
	}
	assertProposalSucceeded(t, retry.result)
}

func TestCommandCoordinatorRejectsPendingRequestsOnLeadershipLoss(t *testing.T) {
	coordinator := newCommandCoordinator(10, nil, nil)
	sessionID := raft.SessionID{3}
	mustRecoverCommands(t, coordinator, raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryOpenSession, SessionID: sessionID})

	proposal := proposalInput{
		event:  raft.ProposeSet{ProposalID: 1, SessionID: sessionID, Sequence: 1, Key: "key", Value: []byte("value")},
		result: make(chan proposalResult, 1),
		ctx:    context.Background(),
	}
	if _, shouldStep := coordinator.beginProposal(proposal); !shouldStep {
		t.Fatal("mutation was completed before reaching Raft")
	}
	coordinator.handle(raft.ProposalAccepted{ProposalID: 1, Index: 2}, proposal)

	read := readInput{event: raft.ConfirmRead{ReadID: 4}, result: make(chan readResult, 1), ctx: context.Background(), key: "key"}
	coordinator.beginRead(read)
	coordinator.lostLeadership("node-2")

	proposalResult := <-proposal.result
	if !proposalResult.rejected || proposalResult.leaderID != "node-2" {
		t.Fatalf("proposal result = %#v, want rejection by node-2", proposalResult)
	}
	readResult := <-read.result
	if !readResult.rejected || readResult.leaderID != "node-2" {
		t.Fatalf("read result = %#v, want rejection by node-2", readResult)
	}
}

func TestCommandCoordinatorRestoresSnapshotAndHandlesReadRejection(t *testing.T) {
	coordinator := newCommandCoordinator(10, nil, nil)
	sessionID := raft.SessionID{4}
	state := &snapshot.State{
		Identity:      snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}},
		IncludedIndex: 2,
		IncludedTerm:  1,
		Values:        map[string][]byte{"key": []byte("restored")},
		Sessions:      []snapshot.Session{{ID: sessionID, LastSequence: 1}},
	}
	if err := coordinator.restore(state); err != nil {
		t.Fatalf("restore() error = %v", err)
	}

	read := readInput{event: raft.ConfirmRead{ReadID: 5}, result: make(chan readResult, 1), ctx: context.Background(), key: "key"}
	coordinator.beginRead(read)
	coordinator.handle(raft.ReadConfirmed{ReadID: 5}, read)
	result := <-read.result
	if !result.found || string(result.value) != "restored" {
		t.Fatalf("restored read = %#v, want restored value", result)
	}

	rejected := readInput{event: raft.ConfirmRead{ReadID: 6}, result: make(chan readResult, 1), ctx: context.Background(), key: "key"}
	coordinator.beginRead(rejected)
	coordinator.handle(raft.ReadRejected{ReadID: 6, LeaderID: "node-3"}, rejected)
	rejection := <-rejected.result
	if !rejection.rejected || rejection.leaderID != "node-3" {
		t.Fatalf("read rejection = %#v, want node-3", rejection)
	}
}

func mustRecoverCommands(t *testing.T, coordinator *commandCoordinator, entries ...raft.LogEntry) {
	t.Helper()
	actions := make([]raft.Action, len(entries))
	for index, entry := range entries {
		actions[index] = raft.ApplyEntry{Entry: entry}
	}
	if err := coordinator.recover(actions); err != nil {
		t.Fatalf("recover() error = %v", err)
	}
}

func assertProposalSucceeded(t *testing.T, results <-chan proposalResult) {
	t.Helper()
	result := <-results
	if result.rejected || result.failure != sessionSucceeded {
		t.Fatalf("proposal result = %#v, want success", result)
	}
}
