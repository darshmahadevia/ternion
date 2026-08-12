package raft

import (
	"reflect"
	"testing"
)

func TestFollowerReportsConflictBoundaryAndProtectsCommittedPrefix(t *testing.T) {
	entries := []LogEntry{
		{Index: 1, Term: 1, Type: EntryNoOp},
		{Index: 2, Term: 2, Type: EntryNoOp},
		{Index: 3, Term: 2, Type: EntryNoOp},
		{Index: 4, Term: 3, Type: EntryNoOp},
		{Index: 5, Term: 3, Type: EntryNoOp},
	}
	node := NewNodeFromRecoveredState("node-2", []NodeID{"node-1", "node-3"}, RecoveredState{
		HardState:   HardState{Term: 4},
		Log:         entries,
		CommitIndex: 3,
	})

	actions := node.Step(AppendEntries{From: "node-1", Term: 4, RequestID: 1, PrevLogIndex: 5, PrevLogTerm: 9})
	wantConflict := AppendEntriesResponse{From: "node-2", Term: 4, RequestID: 1, MatchIndex: 5, ConflictTerm: 3, ConflictIndex: 4}
	if response := appendResponse(t, actions); response != wantConflict {
		t.Fatalf("conflict response = %#v, want %#v", response, wantConflict)
	}

	actions = node.Step(AppendEntries{From: "node-1", Term: 4, RequestID: 2, PrevLogIndex: 8, PrevLogTerm: 3})
	wantShort := AppendEntriesResponse{From: "node-2", Term: 4, RequestID: 2, MatchIndex: 5, ConflictIndex: 6}
	if response := appendResponse(t, actions); response != wantShort {
		t.Fatalf("short-log response = %#v, want %#v", response, wantShort)
	}

	actions = node.Step(AppendEntries{From: "node-1", Term: 4, RequestID: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Index: 2, Term: 4, Type: EntryNoOp}}})
	wantCommittedConflict := AppendEntriesResponse{From: "node-2", Term: 4, RequestID: 3, MatchIndex: 5, ConflictTerm: 2, ConflictIndex: 2}
	if response := appendResponse(t, actions); response != wantCommittedConflict {
		t.Fatalf("committed-prefix conflict response = %#v, want %#v", response, wantCommittedConflict)
	}
	if !reflect.DeepEqual(node.log, entries) {
		t.Fatalf("log after committed-prefix conflict = %#v, want unchanged %#v", node.log, entries)
	}

	replacement := []LogEntry{{Index: 4, Term: 4, Type: EntryNoOp}, {Index: 5, Term: 4, Type: EntryNoOp}}
	actions = node.Step(AppendEntries{From: "node-1", Term: 4, RequestID: 4, PrevLogIndex: 3, PrevLogTerm: 2, Entries: replacement})
	wantPersistence := PersistLogEntries{PersistenceID: 1, TruncateFrom: 4, Entries: replacement}
	if !containsAction(actions, wantPersistence) {
		t.Fatalf("uncommitted suffix repair actions = %#v, want %#v", actions, wantPersistence)
	}
	wantLog := append(cloneLogEntries(entries[:3]), replacement...)
	if !reflect.DeepEqual(node.log, wantLog) {
		t.Fatalf("repaired log = %#v, want %#v", node.log, wantLog)
	}
}

func TestLeaderUsesConflictJumpAndByteBoundedBatchesWithOneRequestInFlight(t *testing.T) {
	largeValue := make([]byte, 600<<10)
	entries := []LogEntry{
		{Index: 1, Term: 1, Type: EntrySet, Key: "first", Value: largeValue},
		{Index: 2, Term: 2, Type: EntrySet, Key: "second", Value: largeValue},
		{Index: 3, Term: 2, Type: EntryDelete, Key: "old"},
	}
	node := NewNodeFromRecoveredState("node-1", []NodeID{"node-2", "node-3"}, RecoveredState{HardState: HardState{Term: 3}, Log: entries})
	electRecoveredLeader(t, node, 4)

	actions := node.Step(AppendEntriesResponse{From: "node-2", Term: 4, RequestID: 1, ConflictTerm: 2, ConflictIndex: 2})
	jumped := onlyAppendRequest(t, actions)
	if jumped.PrevLogIndex != 3 || jumped.PrevLogTerm != 2 || len(jumped.Entries) != 1 || jumped.Entries[0].Index != 4 {
		t.Fatalf("conflict-Term jump request = %#v, want jump to after Leader's last Term-2 entry", jumped)
	}
	if duplicate := node.sendAppendEntriesTo("node-2", 0); len(duplicate) != 0 {
		t.Fatalf("second request while replication is in flight = %#v, want none", duplicate)
	}

	node.Step(AppendEntriesFailed{To: "node-2", RequestID: jumped.RequestID})
	actions = node.Step(AppendEntriesResponse{From: "node-2", Term: 4, ConflictIndex: 1})
	firstBatch := onlyAppendRequest(t, actions)
	if firstBatch.PrevLogIndex != 0 || len(firstBatch.Entries) != 1 || firstBatch.Entries[0].Index != 1 {
		t.Fatalf("first bounded batch = %#v, want only oversized-pair first entry", firstBatch)
	}
	if batchBytes(firstBatch.Entries) > maxAppendEntriesBytes {
		t.Fatalf("first batch bytes = %d, limit = %d", batchBytes(firstBatch.Entries), maxAppendEntriesBytes)
	}

	actions = node.Step(AppendEntriesResponse{From: "node-2", Term: 4, RequestID: firstBatch.RequestID, Success: true, MatchIndex: 1})
	secondBatch := onlyAppendRequest(t, actions)
	if got := []uint64{secondBatch.Entries[0].Index, secondBatch.Entries[len(secondBatch.Entries)-1].Index}; !reflect.DeepEqual(got, []uint64{2, 4}) {
		t.Fatalf("second batch indices = %v, want consecutive [2..4]", got)
	}
	if batchBytes(secondBatch.Entries) > maxAppendEntriesBytes {
		t.Fatalf("second batch bytes = %d, limit = %d", batchBytes(secondBatch.Entries), maxAppendEntriesBytes)
	}
}

func electRecoveredLeader(t *testing.T, node *Node, term uint64) {
	t.Helper()
	node.Step(ElectionTimeout{})
	node.Step(PreVoteResponse{From: "node-2", Term: term, CurrentTerm: term - 1, Granted: true})
	node.Step(HardStatePersisted{PersistenceID: 1})
	node.Step(VoteResponse{From: "node-2", Term: term, Granted: true})
	node.Step(LogEntriesPersisted{PersistenceID: 1})
	if node.role != Leader {
		t.Fatalf("role = %v, want Leader", node.role)
	}
}

func appendResponse(t *testing.T, actions []Action) AppendEntriesResponse {
	t.Helper()
	for _, action := range actions {
		if response, ok := action.(SendAppendEntriesResponse); ok {
			return response.Response
		}
	}
	t.Fatalf("actions = %#v, want AppendEntries response", actions)
	return AppendEntriesResponse{}
}

func onlyAppendRequest(t *testing.T, actions []Action) AppendEntries {
	t.Helper()
	var requests []AppendEntries
	for _, action := range actions {
		if send, ok := action.(SendAppendEntries); ok {
			requests = append(requests, send.Request)
		}
	}
	if len(requests) != 1 {
		t.Fatalf("AppendEntries requests = %#v, want exactly one", requests)
	}
	return requests[0]
}

func containsAction(actions []Action, want Action) bool {
	for _, action := range actions {
		if reflect.DeepEqual(action, want) {
			return true
		}
	}
	return false
}

func batchBytes(entries []LogEntry) int {
	bytes := 0
	for _, entry := range entries {
		bytes += 64 + len(entry.Key) + len(entry.Value)
	}
	return bytes
}
