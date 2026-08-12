package simulation_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/simulation"
)

func newCluster(t *testing.T, seed int64) *simulation.Cluster {
	t.Helper()
	cluster, err := simulation.NewCluster(simulation.DefaultTiming(), simulation.NewSeededClock(seed))
	if err != nil {
		t.Fatalf("create Cluster: %v", err)
	}
	return cluster
}

func electLeader(t *testing.T, cluster *simulation.Cluster) raft.NodeID {
	t.Helper()
	if err := cluster.FireNextElectionTimeout(); err != nil {
		t.Fatalf("fire election timeout: %v", err)
	}
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if cluster.State(id).Role == raft.Leader {
			return id
		}
	}
	t.Fatal("Cluster has no Leader")
	return ""
}

func TestThreeNodeElectionElectsExactlyOneLeader(t *testing.T) {
	result, err := simulation.RunElection(42)
	if err != nil {
		t.Fatalf("run election: %v", err)
	}
	if result.Leader == "" {
		t.Fatal("election returned no Leader")
	}
	if result.Term != 1 {
		t.Fatalf("elected Term = %d, want 1", result.Term)
	}

	leaderActions := 0
	for _, step := range result.Trace {
		for _, action := range step.Actions {
			if _, ok := action.(raft.BecameLeader); ok {
				leaderActions++
			}
		}
	}
	if leaderActions != 1 {
		t.Fatalf("BecameLeader actions = %d, want 1", leaderActions)
	}
}

func TestElectedLeaderCommitsNoOpBeforeReportingReadReady(t *testing.T) {
	cluster := newCluster(t, 42)
	leader := electLeader(t, cluster)

	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		state := cluster.State(id)
		if state.CommitIndex != 1 || state.LastApplied != 1 {
			t.Fatalf("Node %s state = %#v, want no-op committed and applied", id, state)
		}
		applied := cluster.AppliedEntries(id)
		want := []raft.LogEntry{{Index: 1, Term: 1, Type: raft.EntryNoOp}}
		if !reflect.DeepEqual(applied, want) {
			t.Fatalf("Node %s applied entries = %#v, want %#v", id, applied, want)
		}
	}
	if state := cluster.State(leader); !state.ReadReady {
		t.Fatalf("Leader state = %#v, want read readiness after applied no-op", state)
	}

	readReadyStep := -1
	applyStep := -1
	for index, step := range cluster.Trace() {
		if step.Node != leader {
			continue
		}
		for _, action := range step.Actions {
			switch action.(type) {
			case raft.ApplyEntry:
				applyStep = index
			case raft.BecameReadReady:
				readReadyStep = index
			}
		}
	}
	if applyStep < 0 || readReadyStep != applyStep {
		t.Fatalf("Leader apply step = %d, read-ready step = %d; want readiness emitted with applied no-op", applyStep, readReadyStep)
	}

	if err := cluster.FireHeartbeatTimeout(leader); err != nil {
		t.Fatalf("fire duplicate heartbeat: %v", err)
	}
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if got := len(cluster.AppliedEntries(id)); got != 1 {
			t.Fatalf("Node %s applied entries after duplicate replication = %d, want 1", id, got)
		}
	}
}

func TestClientSessionCommandCommitsAfterLeaderChange(t *testing.T) {
	cluster := newCluster(t, 42)
	oldLeader := electLeader(t, cluster)
	sessionID := raft.SessionID{1, 2, 3}
	if err := cluster.ProposeSession(oldLeader, raft.ProposeSession{ProposalID: 1, Type: raft.EntryOpenSession, SessionID: sessionID}); err != nil {
		t.Fatalf("propose open Client Session: %v", err)
	}
	if err := cluster.ProposeSession(oldLeader, raft.ProposeSession{ProposalID: 2, Type: raft.EntryOpenSession, SessionID: raft.SessionID{9}}); err != nil {
		t.Fatalf("propose second open Client Session: %v", err)
	}

	var majority []raft.NodeID
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}
	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("start fresh check-quorum window: %v", err)
	}
	cluster.Partition(majority, []raft.NodeID{oldLeader})
	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("demote isolated Leader: %v", err)
	}
	for _, id := range majority {
		if err := cluster.FireElectionTimeout(id); err != nil {
			t.Fatalf("fire election timeout for %s: %v", id, err)
		}
	}
	var newLeader raft.NodeID
	for _, id := range majority {
		if cluster.State(id).Role == raft.Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("surviving Quorum elected no replacement Leader")
	}
	if err := cluster.ProposeSession(newLeader, raft.ProposeSession{ProposalID: 3, Type: raft.EntryCloseSession, SessionID: sessionID}); err != nil {
		t.Fatalf("propose close Client Session: %v", err)
	}
	for _, id := range majority {
		applied := cluster.AppliedEntries(id)
		if got := applied[len(applied)-1]; got.Type != raft.EntryCloseSession || got.SessionID != sessionID {
			t.Fatalf("Node %s last applied entry = %#v, want close for %#v", id, got, sessionID)
		}
	}
}

func TestFixedSeedReproducesEventAndActionSequence(t *testing.T) {
	first, err := simulation.RunElection(8675309)
	if err != nil {
		t.Fatalf("run first election: %v", err)
	}
	second, err := simulation.RunElection(8675309)
	if err != nil {
		t.Fatalf("run second election: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func TestTimingRejectsElectionTimeoutAtOrBelowHeartbeatInterval(t *testing.T) {
	timing := simulation.DefaultTiming()
	timing.ElectionTimeoutMin = timing.HeartbeatInterval
	_, err := simulation.NewCluster(timing, simulation.NewSeededClock(1))
	if err == nil || !strings.Contains(err.Error(), "minimum election timeout must exceed heartbeat interval") {
		t.Fatalf("NewCluster() error = %v, want heartbeat/election ordering detail", err)
	}
}

func TestSeededClockRandomizesElectionTimeoutsReproducibly(t *testing.T) {
	first := newCluster(t, 73).ScheduledElections()
	second := newCluster(t, 73).ScheduledElections()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed scheduled different elections:\nfirst:  %#v\nsecond: %#v", first, second)
	}

	seen := make(map[time.Duration]struct{})
	for _, scheduled := range first {
		if scheduled.After < 500*time.Millisecond || scheduled.After > time.Second {
			t.Fatalf("election timeout = %v, want [500ms, 1s]", scheduled.After)
		}
		seen[scheduled.After] = struct{}{}
	}
	if len(seen) == 1 {
		t.Fatalf("seed produced no timeout variation: %#v", first)
	}
}

func TestIsolatedFollowerPreVoteDoesNotDisruptHealthyMajority(t *testing.T) {
	cluster := newCluster(t, 41)
	leader := electLeader(t, cluster)
	var isolated raft.NodeID
	var majority []raft.NodeID
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if id != leader && isolated == "" {
			isolated = id
		} else {
			majority = append(majority, id)
		}
	}
	cluster.Partition(majority, []raft.NodeID{isolated})

	term := cluster.State(leader).Term
	for range 3 {
		if err := cluster.FireElectionTimeout(isolated); err != nil {
			t.Fatalf("fire isolated timeout: %v", err)
		}
	}
	if got := cluster.State(isolated).Term; got != term {
		t.Fatalf("isolated Follower Term = %d, want unchanged Term %d", got, term)
	}
	if got := cluster.State(leader).Role; got != raft.Leader {
		t.Fatalf("healthy majority Leader role = %v, want Leader", got)
	}

	cluster.Heal()
	if err := cluster.FireHeartbeatTimeout(leader); err != nil {
		t.Fatalf("send healing heartbeat: %v", err)
	}
	state := cluster.State(isolated)
	if state.Role != raft.Follower || state.Term != term || state.LeaderID != leader {
		t.Fatalf("healed Follower state = %#v, want Follower following %q in Term %d", state, leader, term)
	}
}

func TestCheckQuorumDemotesIsolatedLeaderAndMajorityElects(t *testing.T) {
	cluster := newCluster(t, 99)
	oldLeader := electLeader(t, cluster)
	if got := cluster.HeartbeatTimeout(oldLeader); got != 100*time.Millisecond {
		t.Fatalf("scheduled heartbeat = %v, want 100ms", got)
	}
	if got := cluster.CheckQuorumTimeout(oldLeader); got != time.Second {
		t.Fatalf("scheduled check-quorum window = %v, want 1s", got)
	}
	var majority []raft.NodeID
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}

	// Close the contact window populated by the election's initial heartbeat,
	// then isolate the Leader for an entire fresh window.
	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("start fresh check-quorum window: %v", err)
	}
	cluster.Partition(majority, []raft.NodeID{oldLeader})
	if err := cluster.FireHeartbeatTimeout(oldLeader); err != nil {
		t.Fatalf("send partitioned heartbeat: %v", err)
	}
	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("finish lost-quorum window: %v", err)
	}
	if got := cluster.State(oldLeader).Role; got != raft.Follower {
		t.Fatalf("isolated Leader role = %v, want Follower", got)
	}
	if got := cluster.State(oldLeader).VotedFor; got != oldLeader {
		t.Fatalf("demoted Leader vote = %q, want preserved self-vote %q", got, oldLeader)
	}

	// Both surviving election timers eventually expire. The second pre-vote
	// can then obtain a vote from the first and form the only authoritative side.
	for _, id := range majority {
		if err := cluster.FireElectionTimeout(id); err != nil {
			t.Fatalf("fire majority timeout for %s: %v", id, err)
		}
	}
	leaders := 0
	for _, id := range majority {
		if cluster.State(id).Role == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("majority Leaders = %d, want 1", leaders)
	}
	if cluster.State(oldLeader).Role == raft.Leader {
		t.Fatal("isolated minority retained an authoritative Leader")
	}
}

func TestHealedFollowerReplacesConflictingSuffixAndReachesCommittedState(t *testing.T) {
	cluster := newCluster(t, 42)
	oldLeader := electLeader(t, cluster)
	var majority []raft.NodeID
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}

	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("start fresh check-quorum window: %v", err)
	}
	cluster.Partition(majority, []raft.NodeID{oldLeader})
	if err := cluster.ProposeSet(oldLeader, raft.ProposeSet{ProposalID: 1, Key: "conflict", Value: []byte("isolated")}); err != nil {
		t.Fatalf("append isolated uncommitted SET: %v", err)
	}
	if err := cluster.FireCheckQuorumTimeout(oldLeader); err != nil {
		t.Fatalf("demote isolated Leader: %v", err)
	}
	for _, id := range majority {
		if err := cluster.FireElectionTimeout(id); err != nil {
			t.Fatalf("fire election timeout for %s: %v", id, err)
		}
	}
	var leader raft.NodeID
	for _, id := range majority {
		if cluster.State(id).Role == raft.Leader {
			leader = id
		}
	}
	if leader == "" {
		t.Fatal("surviving Quorum elected no replacement Leader")
	}
	if err := cluster.ProposeSet(leader, raft.ProposeSet{ProposalID: 2, Key: "committed", Value: []byte("majority")}); err != nil {
		t.Fatalf("commit majority SET: %v", err)
	}

	cluster.Heal()
	if err := cluster.FireHeartbeatTimeout(leader); err != nil {
		t.Fatalf("repair healed Follower: %v", err)
	}
	wantState := cluster.State(leader)
	gotState := cluster.State(oldLeader)
	if gotState.LastLogIndex != wantState.LastLogIndex || gotState.CommitIndex != wantState.CommitIndex || gotState.LastApplied != wantState.LastApplied {
		t.Fatalf("healed Follower state = %#v, Leader state = %#v", gotState, wantState)
	}
	if got, want := cluster.AppliedEntries(oldLeader), cluster.AppliedEntries(leader); !reflect.DeepEqual(got, want) {
		t.Fatalf("healed Follower applied entries = %#v, want Leader entries %#v", got, want)
	}

	conflictRejections := 0
	for _, step := range cluster.Trace() {
		for _, action := range step.Actions {
			response, ok := action.(raft.SendAppendEntriesResponse)
			if ok && !response.Response.Success && response.Response.ConflictIndex != 0 {
				conflictRejections++
			}
		}
	}
	if conflictRejections == 0 {
		t.Fatal("repair trace contained no conflict-hint rejection")
	}
}
