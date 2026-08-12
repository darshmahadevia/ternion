package node

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const mutationProcessTestDeadline = 30 * time.Second

func TestSetSurvivesLeaderCrashAtEveryMutationBoundary(t *testing.T) {
	tests := []struct {
		name     string
		boundary mutationBoundary
	}{
		{name: "before append", boundary: mutationBeforeAppend},
		{name: "after local persistence", boundary: mutationAfterLocalPersistence},
		{name: "after Quorum persistence", boundary: mutationAfterQuorumPersistence},
		{name: "after commitment", boundary: mutationAfterCommitment},
		{name: "after application before response", boundary: mutationAfterApplication},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const key = "survives-leader-crash"
			members := newMutationTestMembers(t)
			configs := make(map[string]config.Config, len(members))
			processes := make(map[string]*mutationNodeProcess, len(members))
			marker := filepath.Join(t.TempDir(), "boundary.reached")
			for id := range members {
				cfg := config.Config{
					Version:            1,
					ClusterID:          "mutation-boundary-" + mutationBoundaryName(test.boundary),
					ActiveSessionLimit: 1,
					Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
					Members:            members,
				}
				configs[id] = cfg
				processes[id] = startMutationNodeProcess(t, cfg, test.boundary, key, marker)
			}
			defer stopMutationNodeProcesses(processes)

			leader := waitForMutationLeader(t, members, nil, mutationProcessTestDeadline)
			addresses := mutationClientAddresses(members, leader)
			requestCtx, cancel := context.WithTimeout(context.Background(), mutationProcessTestDeadline)
			sessionID, err := client.New(addresses...).OpenSession(requestCtx)
			cancel()
			if err != nil {
				t.Fatalf("open Client Session: %v", err)
			}

			setDone := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), mutationProcessTestDeadline)
				defer cancel()
				setDone <- client.New(addresses...).Set(ctx, sessionID, 1, key, []byte("original"))
			}()
			waitForMutationMarker(t, marker, mutationProcessTestDeadline)
			select {
			case err := <-setDone:
				t.Fatalf("SET completed at %s before the scheduled crash: %v", test.name, err)
			default:
			}

			processes[leader].stop()
			delete(processes, leader)
			select {
			case err := <-setDone:
				if err != nil {
					t.Fatalf("official client did not rediscover the Leader after %s: %v", test.name, err)
				}
			case <-time.After(mutationProcessTestDeadline):
				t.Fatalf("official client did not complete after Leader crash at %s", test.name)
			}

			replacement := waitForMutationLeader(t, members, map[string]bool{leader: true}, mutationProcessTestDeadline)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			value, err := client.New(mutationClientAddresses(members, replacement)...).Get(ctx, key)
			cancel()
			if err != nil || !bytes.Equal(value, []byte("original")) {
				t.Fatalf("GET after crash at %s = (%q, %v), want original Value", test.name, value, err)
			}

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			err = client.New(members[replacement].ClientAddress).Set(ctx, sessionID, 1, key, []byte("conflicting retry"))
			cancel()
			if err != nil {
				t.Fatalf("retry cached mutation result after %s: %v", test.name, err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			value, err = client.New(members[replacement].ClientAddress).Get(ctx, key)
			cancel()
			if err != nil || !bytes.Equal(value, []byte("original")) {
				t.Fatalf("GET after conflicting duplicate at %s = (%q, %v), want cached original result", test.name, value, err)
			}

			stopMutationNodeProcesses(processes)
			clear(processes)
			assertDurableMutationHistory(t, configs, sessionID, key, leader, replacement, test.boundary >= mutationAfterQuorumPersistence)
		})
	}
}

func TestMutationNodeProcessHelper(t *testing.T) {
	path := os.Getenv("QUORUMKV_MUTATION_PROCESS_CONFIG")
	if path == "" {
		return
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	boundary, err := parseMutationBoundary(os.Getenv("QUORUMKV_TEST_MUTATION_BOUNDARY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	key := os.Getenv("QUORUMKV_TEST_MUTATION_KEY")
	marker := os.Getenv("QUORUMKV_TEST_MUTATION_MARKER")
	n := New(cfg)
	n.observeMutation = func(actual mutationBoundary, entry raft.LogEntry) {
		if actual != boundary || entry.Key != key || entry.Sequence != 1 {
			return
		}
		file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if os.IsExist(err) {
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if _, err := file.WriteString(mutationBoundaryName(actual)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := file.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		select {}
	}
	if err := n.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

type mutationNodeProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	stopped bool
}

func startMutationNodeProcess(t *testing.T, cfg config.Config, boundary mutationBoundary, key, marker string) *mutationNodeProcess {
	t.Helper()
	contents, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config for %q: %v", cfg.Node.ID, err)
	}
	path := filepath.Join(t.TempDir(), cfg.Node.ID+".yaml")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write config for %q: %v", cfg.Node.ID, err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	process := &mutationNodeProcess{done: make(chan error, 1)}
	process.command = exec.Command(executable, "-test.run=^TestMutationNodeProcessHelper$")
	process.command.Env = append(os.Environ(),
		"QUORUMKV_MUTATION_PROCESS_CONFIG="+path,
		"QUORUMKV_TEST_MUTATION_BOUNDARY="+mutationBoundaryName(boundary),
		"QUORUMKV_TEST_MUTATION_KEY="+key,
		"QUORUMKV_TEST_MUTATION_MARKER="+marker,
	)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start Node %q: %v", cfg.Node.ID, err)
	}
	go func() { process.done <- process.command.Wait() }()
	return process
}

func (p *mutationNodeProcess) stop() {
	if p == nil || p.stopped {
		return
	}
	p.stopped = true
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
	}
}

func newMutationTestMembers(t *testing.T) map[string]config.Member {
	t.Helper()
	members := make(map[string]config.Member, 3)
	for index := 1; index <= 3; index++ {
		members[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedMutationAddress(t),
			ClientAddress: unusedMutationAddress(t),
		}
	}
	return members
}

func unusedMutationAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve process-test address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release process-test address: %v", err)
	}
	return address
}

func mutationClientAddresses(members map[string]config.Member, first string) []string {
	addresses := []string{members[first].ClientAddress}
	for index := 1; index <= len(members); index++ {
		id := fmt.Sprintf("node-%d", index)
		if id != first {
			addresses = append(addresses, members[id].ClientAddress)
		}
	}
	return addresses
}

func stopMutationNodeProcesses(processes map[string]*mutationNodeProcess) {
	for _, process := range processes {
		process.stop()
	}
}

func waitForMutationMarker(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect mutation boundary marker: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Node did not reach mutation boundary marker %q within %v", path, timeout)
}

func waitForMutationLeader(t *testing.T, members map[string]config.Member, excluded map[string]bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		observed := make(map[string]*quorumkvv1.GetStatusResponse)
		for id, member := range members {
			if excluded[id] {
				continue
			}
			if status := fetchMutationStatus(member.ClientAddress); status != nil {
				observed[id] = status
			}
		}
		var leader string
		leaders := 0
		for id, status := range observed {
			if status.Role == quorumkvv1.RaftRole_RAFT_ROLE_LEADER {
				leader = id
				leaders++
			}
		}
		if len(observed) == 3-len(excluded) && leaders == 1 {
			allAgree := true
			for _, status := range observed {
				if status.LeaderId != leader {
					allAgree = false
				}
			}
			if allAgree {
				time.Sleep(1200 * time.Millisecond)
				return leader
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("did not observe one agreed Leader within %v", timeout)
	return ""
}

func fetchMutationStatus(address string) *quorumkvv1.GetStatusResponse {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	response, err := quorumkvv1.NewNodeServiceClient(connection).GetStatus(ctx, &quorumkvv1.GetStatusRequest{})
	if err != nil {
		return nil
	}
	return response
}

func mutationBoundaryName(boundary mutationBoundary) string {
	switch boundary {
	case mutationBeforeAppend:
		return "before-append"
	case mutationAfterLocalPersistence:
		return "after-local-persistence"
	case mutationAfterQuorumPersistence:
		return "after-quorum-persistence"
	case mutationAfterCommitment:
		return "after-commitment"
	case mutationAfterApplication:
		return "after-application"
	default:
		return "unknown"
	}
}

func parseMutationBoundary(name string) (mutationBoundary, error) {
	for _, boundary := range []mutationBoundary{
		mutationBeforeAppend,
		mutationAfterLocalPersistence,
		mutationAfterQuorumPersistence,
		mutationAfterCommitment,
		mutationAfterApplication,
	} {
		if mutationBoundaryName(boundary) == name {
			return boundary, nil
		}
	}
	return 0, fmt.Errorf("unknown test mutation boundary %q", name)
}

func assertDurableMutationHistory(
	t *testing.T,
	configs map[string]config.Config,
	sessionID [16]byte,
	key string,
	crashedLeader string,
	replacementLeader string,
	mustPreserveOriginalEntry bool,
) {
	t.Helper()
	type durableNode struct {
		id     string
		commit uint64
		log    []wal.LogEntry
	}
	nodes := make([]durableNode, 0, len(configs))
	mutationsByNode := make(map[string][]wal.LogEntry, len(configs))
	nodesWithMutation := 0
	for id, cfg := range configs {
		store, recovered, err := wal.Open(cfg.Node.DataDir, wal.Identity{ClusterID: cfg.ClusterID, NodeID: id})
		if err != nil {
			t.Fatalf("recover Node %q WAL: %v", id, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close recovered Node %q WAL: %v", id, err)
		}
		for _, entry := range recovered.Log {
			if entry.Type != wal.EntryType(raft.EntrySet) || entry.SessionID != sessionID || entry.Sequence != 1 {
				continue
			}
			if entry.Key != key || !bytes.Equal(entry.Value, []byte("original")) {
				t.Fatalf("Node %q stored conflicting mutation entry %#v", id, entry)
			}
			mutationsByNode[id] = append(mutationsByNode[id], entry)
		}
		if len(mutationsByNode[id]) > 0 {
			nodesWithMutation++
		}
		nodes = append(nodes, durableNode{id: id, commit: recovered.CommitIndex, log: recovered.Log})
	}
	if nodesWithMutation < 2 {
		t.Fatalf("logical mutation is durable on %d Nodes, want a Quorum", nodesWithMutation)
	}
	if mustPreserveOriginalEntry {
		originalEntries := mutationsByNode[crashedLeader]
		preservedEntries := mutationsByNode[replacementLeader]
		preservedOriginal := false
		for _, original := range originalEntries {
			for _, preserved := range preservedEntries {
				if original.Index == preserved.Index && original.Term == preserved.Term {
					preservedOriginal = true
				}
			}
		}
		if len(originalEntries) == 0 || !preservedOriginal {
			t.Fatalf("replacement Leader %q entries = %#v, crashed Leader %q entries = %#v; want the Quorum-persisted current-Term entry preserved", replacementLeader, preservedEntries, crashedLeader, originalEntries)
		}
	}
	for left := 0; left < len(nodes); left++ {
		for right := left + 1; right < len(nodes); right++ {
			sharedCommit := min(nodes[left].commit, nodes[right].commit)
			for index := uint64(1); index <= sharedCommit; index++ {
				leftEntry := nodes[left].log[index-1]
				rightEntry := nodes[right].log[index-1]
				if leftEntry.Term != rightEntry.Term || leftEntry.Type != rightEntry.Type ||
					leftEntry.SessionID != rightEntry.SessionID || leftEntry.Sequence != rightEntry.Sequence ||
					leftEntry.Key != rightEntry.Key || !bytes.Equal(leftEntry.Value, rightEntry.Value) {
					t.Fatalf("Nodes %q and %q have conflicting committed entries at index %d: %#v != %#v", nodes[left].id, nodes[right].id, index, leftEntry, rightEntry)
				}
			}
		}
	}
}
