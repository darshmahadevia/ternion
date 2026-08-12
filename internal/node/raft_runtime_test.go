package node

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

func TestProcessRestartDoesNotGrantSecondVoteInTerm(t *testing.T) {
	directory := t.TempDir()
	first := runVoteProcess(t, directory, "candidate-a")
	if first != "granted" {
		t.Fatalf("first vote = %q, want granted", first)
	}

	second := runVoteProcess(t, directory, "candidate-b")
	if second != "rejected" {
		t.Fatalf("vote after process restart = %q, want rejected", second)
	}
}

func TestRaftRuntimeRestoresPersistedLog(t *testing.T) {
	cfg := config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1", DataDir: t.TempDir()}}
	peers := []raft.NodeID{"node-2", "node-3"}
	runtime, err := openRaftRuntime(cfg, peers)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	if _, err := runtime.step(raft.ElectionTimeout{}); err != nil {
		t.Fatalf("start pre-vote: %v", err)
	}
	if _, err := runtime.step(raft.PreVoteResponse{From: "node-2", Term: 1, CurrentTerm: 0, Granted: true}); err != nil {
		t.Fatalf("win pre-vote: %v", err)
	}
	if _, err := runtime.step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true}); err != nil {
		t.Fatalf("win election: %v", err)
	}
	if err := runtime.close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}

	reopened, err := openRaftRuntime(cfg, peers)
	if err != nil {
		t.Fatalf("reopen runtime: %v", err)
	}
	defer reopened.close()
	state := reopened.core.State()
	if state.LastLogIndex != 1 || state.LastLogTerm != 1 {
		t.Fatalf("recovered log state = %#v, want index 1 in Term 1", state)
	}
}

func TestRaftRuntimeReplaysOnlyDurableCommittedPrefix(t *testing.T) {
	directory := t.TempDir()
	cfg := config.Config{
		ClusterID:          "cluster-1",
		ActiveSessionLimit: 1,
		Node:               config.Node{ID: "node-1", DataDir: directory},
	}
	store, _, err := wal.Open(directory, wal.Identity{ClusterID: cfg.ClusterID, NodeID: cfg.Node.ID})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	sessionID := [16]byte{1}
	if err := store.SaveLogEntries([]wal.LogEntry{
		{Index: 1, Term: 1, Type: wal.EntryType(raft.EntryOpenSession), SessionID: sessionID},
		{Index: 2, Term: 1, Type: wal.EntryType(raft.EntrySet), SessionID: sessionID, Sequence: 1, Key: "committed", Value: []byte("kept")},
		{Index: 3, Term: 1, Type: wal.EntryType(raft.EntryDelete), SessionID: sessionID, Sequence: 2, Key: "committed"},
		{Index: 4, Term: 1, Type: wal.EntryType(raft.EntrySet), SessionID: sessionID, Sequence: 3, Key: "uncommitted", Value: []byte("ignored")},
	}); err != nil {
		t.Fatalf("save log: %v", err)
	}
	if err := store.SaveCommitIndex(3); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	runtime, err := openRaftRuntime(cfg, []raft.NodeID{"node-2", "node-3"})
	if err != nil {
		t.Fatalf("recover runtime: %v", err)
	}
	defer runtime.close()
	actions, err := runtime.step(raft.RecoverCommitted{})
	if err != nil {
		t.Fatalf("replay committed prefix: %v", err)
	}
	machine := newSessionMachine(cfg.ActiveSessionLimit)
	for _, action := range actions {
		apply, ok := action.(raft.ApplyEntry)
		if !ok {
			t.Fatalf("recovery action = %T, want ApplyEntry", action)
		}
		machine.apply(apply.Entry)
	}
	if _, exists := machine.values["committed"]; exists {
		t.Fatal("recovered committed DELETE left the Key present")
	}
	if _, exists := machine.values["uncommitted"]; exists {
		t.Fatal("uncommitted suffix was applied during recovery")
	}
	if result, shouldApply := machine.evaluateMutation(raft.SessionID(sessionID), 2); shouldApply || result.failure != sessionSucceeded || !result.existed {
		t.Fatalf("retry recovered DELETE = (%#v, apply=%v), want cached existed=true without apply", result, shouldApply)
	}
	if state := runtime.core.State(); state.CommitIndex != 3 || state.LastApplied != 3 || state.LastLogIndex != 4 {
		t.Fatalf("recovered Raft state = %#v, want applied through 3 with log through 4", state)
	}
}

func TestRaftRuntimeRestoresSnapshotThenReplaysCommittedSuffix(t *testing.T) {
	directory := t.TempDir()
	members := map[string]config.Member{"node-1": {}, "node-2": {}, "node-3": {}}
	cfg := config.Config{
		ClusterID: "cluster-1", ActiveSessionLimit: 1,
		Node: config.Node{ID: "node-1", DataDir: directory}, Members: members,
	}
	sessionID := [16]byte{1}
	store, _, err := wal.Open(directory, wal.Identity{ClusterID: cfg.ClusterID, NodeID: cfg.Node.ID})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]wal.LogEntry{
		{Index: 1, Term: 1, Type: wal.EntryType(raft.EntryOpenSession), SessionID: sessionID},
		{Index: 2, Term: 1, Type: wal.EntryType(raft.EntrySet), SessionID: sessionID, Sequence: 1, Key: "deleted", Value: []byte("from Snapshot")},
		{Index: 3, Term: 2, Type: wal.EntryType(raft.EntrySet), SessionID: sessionID, Sequence: 2, Key: "retained", Value: []byte("from suffix")},
		{Index: 4, Term: 2, Type: wal.EntryType(raft.EntryDelete), SessionID: sessionID, Sequence: 3, Key: "deleted"},
	}); err != nil {
		t.Fatalf("save log: %v", err)
	}
	if err := store.SaveCommitIndex(4); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	_, err = snapshot.Save(directory, snapshot.State{
		Identity: snapshotIdentity(cfg), IncludedIndex: 2, IncludedTerm: 1,
		Values:   map[string][]byte{"deleted": []byte("from Snapshot")},
		Sessions: []snapshot.Session{{ID: sessionID, LastSequence: 1}},
	})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}

	runtime, err := openRaftRuntime(cfg, []raft.NodeID{"node-2", "node-3"})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer runtime.close()
	machine := newSessionMachine(cfg.ActiveSessionLimit)
	if err := machine.restore(runtime.recoveredSnapshot); err != nil {
		t.Fatalf("restore Snapshot state: %v", err)
	}
	actions, err := runtime.step(raft.RecoverCommitted{})
	if err != nil {
		t.Fatalf("recover committed suffix: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("recovery emitted %d actions, want two suffix entries", len(actions))
	}
	for _, action := range actions {
		machine.apply(action.(raft.ApplyEntry).Entry)
	}
	if _, found := machine.get("deleted"); found {
		t.Fatal("committed WAL suffix did not delete Snapshot Value")
	}
	if value, found := machine.get("retained"); !found || string(value) != "from suffix" {
		t.Fatalf("retained Value = %q, found %v; want committed suffix Value", value, found)
	}
	if result, apply := machine.evaluateMutation(raft.SessionID(sessionID), 3); apply || !result.existed {
		t.Fatalf("duplicate recovered DELETE = (%#v, apply %v), want cached existed result", result, apply)
	}
}

func TestRaftRuntimePersistsConflictingSuffixReplacement(t *testing.T) {
	cfg := config.Config{
		ClusterID: "cluster-1",
		Node:      config.Node{ID: "node-2", DataDir: t.TempDir()},
	}
	runtime, err := openRaftRuntime(cfg, []raft.NodeID{"node-1", "node-3"})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	if _, err := runtime.step(raft.AppendEntries{
		From: "node-1",
		Term: 1,
		Entries: []raft.LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		},
	}); err != nil {
		t.Fatalf("persist initial suffix: %v", err)
	}
	if _, err := runtime.step(raft.AppendEntries{
		From:         "node-1",
		Term:         2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []raft.LogEntry{{Index: 2, Term: 2}},
	}); err != nil {
		t.Fatalf("persist replacement suffix: %v", err)
	}
	if err := runtime.close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}

	reopened, err := openRaftRuntime(cfg, []raft.NodeID{"node-1", "node-3"})
	if err != nil {
		t.Fatalf("reopen runtime: %v", err)
	}
	defer reopened.close()
	state := reopened.core.State()
	if state.LastLogIndex != 2 || state.LastLogTerm != 2 {
		t.Fatalf("recovered replacement state = %#v, want index 2 in Term 2", state)
	}
}

func runVoteProcess(t *testing.T, directory, candidate string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	command := exec.Command(executable, "-test.run=^TestVoteProcessHelper$")
	command.Env = append(os.Environ(),
		"QUORUMKV_VOTE_HELPER=1",
		"QUORUMKV_DATA_DIR="+directory,
		"QUORUMKV_CANDIDATE="+candidate,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run vote process for %q: %v\n%s", candidate, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestVoteProcessHelper(t *testing.T) {
	if os.Getenv("QUORUMKV_VOTE_HELPER") != "1" {
		return
	}
	runtime, err := openRaftRuntime(config.Config{
		ClusterID: "cluster-1",
		Node: config.Node{
			ID:      "node-1",
			DataDir: os.Getenv("QUORUMKV_DATA_DIR"),
		},
	}, []raft.NodeID{"candidate-a", "candidate-b"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	actions, err := runtime.step(raft.VoteRequest{
		From: raft.NodeID(os.Getenv("QUORUMKV_CANDIDATE")),
		Term: 7,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, action := range actions {
		response, ok := action.(raft.SendVoteResponse)
		if !ok {
			continue
		}
		if response.Response.Granted {
			fmt.Println("granted")
		} else {
			fmt.Println("rejected")
		}
		// os.Exit deliberately skips Close to model abrupt process loss after
		// the response becomes externally visible.
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "vote request produced no response")
	os.Exit(2)
}
