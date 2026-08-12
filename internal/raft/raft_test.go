package raft_test

import (
	"reflect"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

func TestRecoveryAppliesOnlyCommittedSuffixAfterSnapshot(t *testing.T) {
	entries := []raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	}
	node := raft.NewNodeFromRecoveredState("node-1", []raft.NodeID{"node-2", "node-3"}, raft.RecoveredState{
		Log: entries, CommitIndex: 4, SnapshotIndex: 2,
	})
	actions := node.Step(raft.RecoverCommitted{})
	want := []raft.Action{
		raft.ApplyEntry{Entry: entries[2]},
		raft.ApplyEntry{Entry: entries[3]},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("recovery actions = %#v, want committed suffix %#v", actions, want)
	}
	if state := node.State(); state.LastApplied != 4 || state.LastAppliedTerm != 2 {
		t.Fatalf("recovered apply position = %d/%d, want 4/2", state.LastApplied, state.LastAppliedTerm)
	}
}

func TestPreVoteDoesNotChangeTermAndElectionWaitsForPersistentSelfVote(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-3", "node-2"})

	actions := node.Step(raft.ElectionTimeout{})
	wantPreVote := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendPreVoteRequest{To: "node-2", Request: raft.PreVoteRequest{From: "node-1", Term: 1}},
		raft.SendPreVoteRequest{To: "node-3", Request: raft.PreVoteRequest{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, wantPreVote) {
		t.Fatalf("timeout actions = %#v, want %#v", actions, wantPreVote)
	}
	if state := node.State(); state.Role != raft.PreCandidate || state.Term != 0 {
		t.Fatalf("state after timeout = %#v, want PreCandidate in Term 0", state)
	}

	actions = node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, CurrentTerm: 0, Granted: true})
	wantPersistence := []raft.Action{raft.PersistHardState{
		PersistenceID: 1,
		Term:          1,
		VotedFor:      "node-1",
	}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("pre-vote response actions = %#v, want %#v", actions, wantPersistence)
	}
	if got := node.State().Role; got != raft.Candidate {
		t.Fatalf("role after pre-vote quorum = %v, want Candidate", got)
	}

	actions = node.Step(raft.HardStatePersisted{PersistenceID: 1})
	wantRequests := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendVoteRequest{To: "node-2", Request: raft.VoteRequest{From: "node-1", Term: 1}},
		raft.SendVoteRequest{To: "node-3", Request: raft.VoteRequest{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, wantRequests) {
		t.Fatalf("persistence actions = %#v, want %#v", actions, wantRequests)
	}
}

func TestGrantedVoteResponseWaitsForPersistence(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})

	actions := node.Step(raft.VoteRequest{From: "node-1", Term: 1})
	wantPersistence := []raft.Action{raft.PersistHardState{
		PersistenceID: 1,
		Term:          1,
		VotedFor:      "node-1",
	}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("vote request actions = %#v, want %#v", actions, wantPersistence)
	}

	actions = node.Step(raft.HardStatePersisted{PersistenceID: 1})
	wantResponse := []raft.Action{raft.SendVoteResponse{
		To:       "node-1",
		Response: raft.VoteResponse{From: "node-2", Term: 1, Granted: true},
	}}
	if !reflect.DeepEqual(actions, wantResponse) {
		t.Fatalf("persistence actions = %#v, want %#v", actions, wantResponse)
	}
}

func TestCandidateBecomesLeaderWithAQuorum(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	noOp := raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNoOp}
	want := []raft.Action{
		raft.BecameLeader{Term: 1},
		raft.ResetHeartbeatTimer{},
		raft.ResetCheckQuorumTimer{},
		raft.PersistLogEntries{PersistenceID: 1, Entries: []raft.LogEntry{noOp}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("vote response actions = %#v, want %#v", actions, want)
	}
	if got := node.State().Role; got != raft.Leader {
		t.Fatalf("role = %v, want Leader", got)
	}
	actions = node.Step(raft.HeartbeatTimeout{})
	wantBeforePersistence := []raft.Action{
		raft.ResetHeartbeatTimer{},
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 1}},
		raft.SendAppendEntries{To: "node-3", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 2}},
	}
	if !reflect.DeepEqual(actions, wantBeforePersistence) {
		t.Fatalf("heartbeat before no-op persistence = %#v, want no unpersisted entries: %#v", actions, wantBeforePersistence)
	}
}

func TestFollowerResetsElectionTimerOnlyForCurrentTermLeaderContact(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})
	node.Step(raft.VoteRequest{From: "node-3", Term: 2})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.AppendEntries{From: "node-1", Term: 1})
	wantStale := []raft.Action{raft.SendAppendEntriesResponse{
		To:       "node-1",
		Response: raft.AppendEntriesResponse{From: "node-2", Term: 2, Success: false},
	}}
	if !reflect.DeepEqual(actions, wantStale) {
		t.Fatalf("stale heartbeat actions = %#v, want %#v", actions, wantStale)
	}

	actions = node.Step(raft.AppendEntries{From: "node-1", Term: 2})
	wantCurrent := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendAppendEntriesResponse{To: "node-1", Response: raft.AppendEntriesResponse{From: "node-2", Term: 2, Success: true}},
	}
	if !reflect.DeepEqual(actions, wantCurrent) {
		t.Fatalf("current-Term heartbeat actions = %#v, want %#v", actions, wantCurrent)
	}
	if got := node.State().LeaderID; got != "node-1" {
		t.Fatalf("Leader ID = %q, want node-1", got)
	}
}

func TestLeaderSendsPeriodicHeartbeats(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 1})

	actions := node.Step(raft.HeartbeatTimeout{})
	want := []raft.Action{
		raft.ResetHeartbeatTimer{},
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 3, PrevLogIndex: 1, PrevLogTerm: 1}},
		raft.SendAppendEntries{To: "node-3", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 4, PrevLogIndex: 1, PrevLogTerm: 1}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("heartbeat timeout actions = %#v, want %#v", actions, want)
	}
}

func TestFollowerValidatesPreviousPositionAndAcknowledgesOnlyAfterPersistence(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})
	node.Step(raft.VoteRequest{From: "node-1", Term: 1})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.AppendEntries{From: "node-1", Term: 1, PrevLogIndex: 1, PrevLogTerm: 1})
	wantRejected := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendAppendEntriesResponse{To: "node-1", Response: raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: false, ConflictIndex: 1}},
	}
	if !reflect.DeepEqual(actions, wantRejected) {
		t.Fatalf("mismatched append actions = %#v, want %#v", actions, wantRejected)
	}

	entry := raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNoOp}
	actions = node.Step(raft.AppendEntries{From: "node-1", Term: 1, Entries: []raft.LogEntry{entry}})
	wantPersistence := []raft.Action{
		raft.ResetElectionTimer{},
		raft.PersistLogEntries{PersistenceID: 1, Entries: []raft.LogEntry{entry}},
	}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("accepted append actions = %#v, want %#v", actions, wantPersistence)
	}
	actions = node.Step(raft.AppendEntries{From: "node-1", Term: 1, Entries: []raft.LogEntry{entry}})
	if want := []raft.Action{raft.ResetElectionTimer{}}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("duplicate before persistence actions = %#v, want no premature acknowledgement: %#v", actions, want)
	}

	actions = node.Step(raft.LogEntriesPersisted{PersistenceID: 1})
	wantAcknowledgement := []raft.Action{raft.SendAppendEntriesResponse{
		To:       "node-1",
		Response: raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1},
	}}
	if !reflect.DeepEqual(actions, wantAcknowledgement) {
		t.Fatalf("persistence completion actions = %#v, want %#v", actions, wantAcknowledgement)
	}
}

func TestCommittedEntriesApplyOnceInOrder(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})
	entry := raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNoOp}
	node.Step(raft.AppendEntries{From: "node-1", Term: 1, Entries: []raft.LogEntry{entry}, LeaderCommit: 1})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	actions := node.Step(raft.LogEntriesPersisted{PersistenceID: 1})
	wantPersistence := []raft.Action{raft.PersistCommitIndex{PersistenceID: 1, CommitIndex: 1}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("durable log actions = %#v, want commit persistence: %#v", actions, wantPersistence)
	}
	if state := node.State(); state.LastApplied != 0 || state.CommitIndex != 1 {
		t.Fatalf("state before commit persistence = %#v, want committed but not applied", state)
	}
	actions = node.Step(raft.CommitIndexPersisted{PersistenceID: 1})
	want := []raft.Action{
		raft.ApplyEntry{Entry: entry},
		raft.SendAppendEntriesResponse{To: "node-1", Response: raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("durable commit actions = %#v, want %#v", actions, want)
	}

	actions = node.Step(raft.AppendEntries{From: "node-1", Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1})
	wantDuplicate := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendAppendEntriesResponse{To: "node-1", Response: raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1}},
	}
	if !reflect.DeepEqual(actions, wantDuplicate) {
		t.Fatalf("duplicate commit actions = %#v, want no second apply: %#v", actions, wantDuplicate)
	}
	if state := node.State(); state.LastApplied != 1 || state.CommitIndex != 1 {
		t.Fatalf("state after duplicate commit = %#v, want applied and committed through 1", state)
	}
}

func TestLeaderCommitsDirectlyOnlyAnEntryFromItsCurrentTerm(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 1})

	node.Step(raft.VoteRequest{From: "node-2", Term: 2, LastLogIndex: 1, LastLogTerm: 1})
	node.Step(raft.HardStatePersisted{PersistenceID: 2})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 3, CurrentTerm: 2, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 3})
	node.Step(raft.VoteResponse{From: "node-2", Term: 3, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 2})

	actions := node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 3, Success: true, MatchIndex: 1})
	if len(actions) != 1 || node.State().CommitIndex != 0 {
		t.Fatalf("old-Term quorum response actions = %#v, state = %#v; want catch-up without direct commit", actions, node.State())
	}
	actions = node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 3, Success: true, MatchIndex: 2})
	wantPersistence := []raft.Action{raft.PersistCommitIndex{PersistenceID: 1, CommitIndex: 2}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("current-Term quorum actions = %#v, want commit persistence %#v", actions, wantPersistence)
	}
	if state := node.State(); state.CommitIndex != 2 || state.LastApplied != 0 || state.ReadReady {
		t.Fatalf("current-Term quorum state = %#v, want unapplied until commit persistence", state)
	}
	actions = node.Step(raft.CommitIndexPersisted{PersistenceID: 1})
	want := []raft.Action{
		raft.ApplyEntry{Entry: raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNoOp}},
		raft.ApplyEntry{Entry: raft.LogEntry{Index: 2, Term: 3, Type: raft.EntryNoOp}},
		raft.BecameReadReady{Term: 3},
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 3, RequestID: 6, PrevLogIndex: 2, PrevLogTerm: 3, LeaderCommit: 2}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("current-Term quorum actions = %#v, want %#v", actions, want)
	}
}

func TestSetAppliesOnlyAfterDurableQuorumReplication(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 1})
	node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1})
	node.Step(raft.CommitIndexPersisted{PersistenceID: 1})

	value := []byte{0, 1, 2, 255}
	actions := node.Step(raft.ProposeSet{
		ProposalID: 7,
		SessionID:  raft.SessionID{9},
		Sequence:   1,
		Key:        "opaque",
		Value:      value,
	})
	value[0] = 99
	wantEntry := raft.LogEntry{Index: 2, Term: 1, Type: raft.EntrySet, SessionID: raft.SessionID{9}, Sequence: 1, Key: "opaque", Value: []byte{0, 1, 2, 255}}
	wantProposal := []raft.Action{
		raft.ProposalAccepted{ProposalID: 7, Index: 2},
		raft.PersistLogEntries{PersistenceID: 2, Entries: []raft.LogEntry{wantEntry}},
	}
	if !reflect.DeepEqual(actions, wantProposal) {
		t.Fatalf("SET proposal actions = %#v, want %#v", actions, wantProposal)
	}
	if state := node.State(); state.CommitIndex != 1 || state.LastApplied != 1 {
		t.Fatalf("state before SET persistence = %#v, want applied only through no-op", state)
	}

	actions = node.Step(raft.LogEntriesPersisted{PersistenceID: 2})
	for _, action := range actions {
		if _, ok := action.(raft.ApplyEntry); ok {
			t.Fatalf("SET applied before Quorum replication: %#v", actions)
		}
	}
	actions = node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 2})
	wantCommit := []raft.Action{raft.PersistCommitIndex{PersistenceID: 2, CommitIndex: 2}}
	if !reflect.DeepEqual(actions, wantCommit) {
		t.Fatalf("SET Quorum acknowledgement actions = %#v, want commit persistence %#v", actions, wantCommit)
	}
	if state := node.State(); state.CommitIndex != 2 || state.LastApplied != 1 {
		t.Fatalf("state after SET Quorum = %#v, want committed but not applied through 2", state)
	}
	actions = node.Step(raft.CommitIndexPersisted{PersistenceID: 2})
	if len(actions) == 0 || !reflect.DeepEqual(actions[0], raft.ApplyEntry{Entry: wantEntry}) {
		t.Fatalf("SET durable commit actions = %#v, want ApplyEntry first", actions)
	}
	if state := node.State(); state.CommitIndex != 2 || state.LastApplied != 2 {
		t.Fatalf("state after durable SET commit = %#v, want committed and applied through 2", state)
	}
}

func TestReadWaitsForCurrentQuorumAndCapturedCommitApplication(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 1})
	node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1})
	node.Step(raft.CommitIndexPersisted{PersistenceID: 1})

	entry := raft.LogEntry{Index: 2, Term: 1, Type: raft.EntrySet, Key: "key", Value: []byte("latest")}
	node.Step(raft.ProposeSet{ProposalID: 1, Key: entry.Key, Value: entry.Value})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 2})

	actions := node.Step(raft.ConfirmRead{ReadID: 7})
	if len(actions) != 0 {
		t.Fatalf("read confirmation while replication is in flight = %#v, want queued read", actions)
	}
	actions = node.Step(raft.HeartbeatTimeout{})
	wantRound := []raft.Action{
		raft.ResetHeartbeatTimer{},
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 4, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []raft.LogEntry{entry}, LeaderCommit: 1, ReadID: 7}},
		raft.SendAppendEntries{To: "node-3", Request: raft.AppendEntries{From: "node-1", Term: 1, RequestID: 5, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []raft.LogEntry{entry}, LeaderCommit: 1, ReadID: 7}},
	}
	if !reflect.DeepEqual(actions, wantRound) {
		t.Fatalf("read confirmation actions = %#v, want quorum round without a log append %#v", actions, wantRound)
	}

	actions = node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, RequestID: 4, Success: true, MatchIndex: 2, ReadID: 7})
	wantCommit := []raft.Action{raft.PersistCommitIndex{PersistenceID: 2, CommitIndex: 2}}
	if !reflect.DeepEqual(actions, wantCommit) {
		t.Fatalf("quorum response actions = %#v, want commit persistence before read confirmation %#v", actions, wantCommit)
	}
	if state := node.State(); state.CommitIndex != 2 || state.LastApplied != 1 {
		t.Fatalf("state before commit persistence = %#v, want captured index 2 still unapplied", state)
	}

	actions = node.Step(raft.CommitIndexPersisted{PersistenceID: 2})
	if len(actions) < 2 || !reflect.DeepEqual(actions[0], raft.ApplyEntry{Entry: entry}) || !reflect.DeepEqual(actions[1], raft.ReadConfirmed{ReadID: 7, CommitIndex: 2}) {
		t.Fatalf("durable commit actions = %#v, want apply followed by read confirmation", actions)
	}
}

func TestReadWithoutQuorumIsNeverConfirmed(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.LogEntriesPersisted{PersistenceID: 1})
	node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1})
	node.Step(raft.CommitIndexPersisted{PersistenceID: 1})

	actions := node.Step(raft.ConfirmRead{ReadID: 9})
	for _, action := range actions {
		if _, ok := action.(raft.ReadConfirmed); ok {
			t.Fatalf("read confirmed before a peer response: %#v", actions)
		}
	}
	node.Step(raft.CheckQuorumTimeout{})
	node.Step(raft.CheckQuorumTimeout{})
	if actions := node.Step(raft.AppendEntriesResponse{From: "node-2", Term: 1, Success: true, MatchIndex: 1, ReadID: 9}); len(actions) != 0 {
		t.Fatalf("demoted Leader accepted a late read response: %#v", actions)
	}
}
