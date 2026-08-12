package node_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/cli"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/node"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

const processTestDeadline = 30 * time.Second

func TestLostSetResponseIsDeduplicatedAfterLeaderFailover(t *testing.T) {
	members := make(map[string]config.Member, 3)
	configs := make(map[string]config.Config, 3)
	processes := make(map[string]*nodeProcess, 3)
	for index := 1; index <= 3; index++ {
		members[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedAddress(t),
			ClientAddress: unusedAddress(t),
		}
	}
	for index := 1; index <= 3; index++ {
		id := fmt.Sprintf("node-%d", index)
		cfg := config.Config{
			Version:            1,
			ClusterID:          "deduplication-process-test",
			ActiveSessionLimit: 1,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		}
		configs[id] = cfg
		processes[id] = startNodeProcess(t, cfg)
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
	}()

	leader := waitForStableLeader(t, members, nil, processTestDeadline)
	requestCtx, cancel := context.WithTimeout(context.Background(), processTestDeadline)
	sessionID, err := client.New(members[leader].ClientAddress).OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session: %v", err)
	}

	proxyAddress, stopProxy := startLostSetResponseProxy(t, members[leader].ClientAddress)
	connection, err := grpc.NewClient(proxyAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to response-dropping proxy: %v", err)
	}
	requestCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = quorumkvv1.NewClientServiceClient(connection).Set(requestCtx, &quorumkvv1.SetRequest{
		SessionId: sessionID[:], Sequence: 1, Key: "deduplicated", Value: []byte("original"),
	})
	cancel()
	connection.Close()
	stopProxy()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("SET through response-dropping proxy error = %v, want Unavailable after upstream success", err)
	}

	processes[leader].stop()
	delete(processes, leader)
	replacement := waitForStableLeader(t, members, map[string]bool{leader: true}, processTestDeadline)
	requestCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	err = client.New(members[replacement].ClientAddress).Set(requestCtx, sessionID, 1, "deduplicated", []byte("different retry payload"))
	cancel()
	if err != nil {
		t.Fatalf("retry lost-response SET through replacement Leader: %v", err)
	}

	// With one Follower stopped, sequence 2 remains in flight. Sequence 3 must
	// be rejected immediately instead of creating a second Session mutation.
	remainingFollower := memberOtherThan(t, members, map[string]bool{leader: true, replacement: true})
	remainingFollowerID := memberIDForAddress(t, members, remainingFollower.ClientAddress)
	processes[remainingFollowerID].stop()
	delete(processes, remainingFollowerID)
	firstDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		firstDone <- client.New(members[replacement].ClientAddress).Set(ctx, sessionID, 2, "pending", []byte("value"))
	}()
	time.Sleep(100 * time.Millisecond)
	connection, err = grpc.NewClient(members[replacement].ClientAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to isolated Leader: %v", err)
	}
	requestCtx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = quorumkvv1.NewClientServiceClient(connection).Set(requestCtx, &quorumkvv1.SetRequest{
		SessionId: sessionID[:], Sequence: 3, Key: "must-not-start", Value: []byte("value"),
	})
	cancel()
	connection.Close()
	if status.Code(err) != codes.FailedPrecondition || !hasOutOfOrderSequenceDetail(err, 3, 2) {
		t.Fatalf("second in-flight mutation error = %v, want typed OutOfOrderSequence with next sequence 2", err)
	}
	<-firstDone

	for id, process := range processes {
		process.stop()
		delete(processes, id)
	}
	nodesWithEffect := 0
	for id, cfg := range configs {
		store, recovered, err := wal.Open(cfg.Node.DataDir, wal.Identity{ClusterID: cfg.ClusterID, NodeID: id})
		if err != nil {
			t.Fatalf("recover Node %q WAL: %v", id, err)
		}
		store.Close()
		effects := 0
		for _, entry := range recovered.Log {
			if entry.Type == wal.EntryType(raft.EntrySet) && entry.SessionID == sessionID && entry.Sequence == 1 {
				effects++
			}
		}
		if effects > 1 {
			t.Fatalf("Node %q WAL contains %d entries for the retried logical mutation, want at most one", id, effects)
		}
		if effects == 1 {
			nodesWithEffect++
		}
	}
	if nodesWithEffect < 2 {
		t.Fatalf("retried logical mutation is present on %d Nodes, want a durable Quorum", nodesWithEffect)
	}
}

type lostSetResponseProxy struct {
	quorumkvv1.UnimplementedClientServiceServer
	target string
}

func (p *lostSetResponseProxy) Set(ctx context.Context, request *quorumkvv1.SetRequest) (*quorumkvv1.SetResponse, error) {
	connection, err := grpc.NewClient(p.target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := quorumkvv1.NewClientServiceClient(connection).Set(ctx, request); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unavailable, "test proxy dropped the successful SET response")
}

func startLostSetResponseProxy(t *testing.T, target string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for response-dropping proxy: %v", err)
	}
	server := grpc.NewServer()
	quorumkvv1.RegisterClientServiceServer(server, &lostSetResponseProxy{target: target})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		<-done
	}
}

func hasOutOfOrderSequenceDetail(err error, received, next uint64) bool {
	for _, detail := range status.Convert(err).Details() {
		outOfOrder, ok := detail.(*quorumkvv1.OutOfOrderSequence)
		if ok && outOfOrder.ReceivedSequence == received && outOfOrder.NextSequence == next {
			return true
		}
	}
	return false
}

func TestThreeProcessesSetThroughCLIAndElectReplacementLeader(t *testing.T) {
	members := make(map[string]config.Member, 3)
	for index := 1; index <= 3; index++ {
		members[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedAddress(t),
			ClientAddress: unusedAddress(t),
		}
	}

	processes := make(map[string]*nodeProcess, 3)
	configs := make(map[string]config.Config, 3)
	for index := 1; index <= 3; index++ {
		id := fmt.Sprintf("node-%d", index)
		cfg := config.Config{
			Version:            1,
			ClusterID:          "process-test-cluster",
			ActiveSessionLimit: 1,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		}
		configs[id] = cfg
		processes[id] = startNodeProcess(t, cfg)
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
	}()

	first := waitForSingleLeader(t, members, nil, processTestDeadline)
	initialFollower := memberOtherThan(t, members, map[string]bool{first: true})
	sessionClient := client.New(initialFollower.ClientAddress)
	requestCtx, cancel := context.WithTimeout(context.Background(), processTestDeadline)
	sessionID, err := sessionClient.OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session through Follower: %v", err)
	}
	if sessionID == ([16]byte{}) {
		t.Fatal("OpenSession() returned the zero identity")
	}
	setLeader := waitForStableLeader(t, members, nil, processTestDeadline)
	connection, err := grpc.NewClient(members[setLeader].ClientAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to Leader for public validation check: %v", err)
	}
	requestCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = quorumkvv1.NewClientServiceClient(connection).Set(requestCtx, &quorumkvv1.SetRequest{
		SessionId: sessionID[:], Sequence: 1, Key: "opaque", Value: make([]byte, (1<<20)+1),
	})
	cancel()
	connection.Close()
	if status.Code(err) != codes.InvalidArgument || !hasValidationDetail(err, "value") {
		t.Fatalf("oversized Value error = %v, want typed value validation error", err)
	}
	if output, err := runCLISet(members[setLeader].ClientAddress, sessionID, 1, "opaque", string([]byte{1, 2, 3}), 5*time.Second); err != nil {
		for _, process := range processes {
			process.stop()
		}
		t.Fatalf("SET through CLI: %v\n%s\nNode output: %#v", err, output, nodeProcessOutputs(processes))
	} else if !strings.Contains(output, `"stored":true`) {
		t.Fatalf("SET CLI output = %q, want stored success", output)
	}
	getOutput, err := runCLIGet(initialFollower.ClientAddress, "opaque", 5*time.Second)
	if err != nil {
		t.Fatalf("GET through Follower after completed SET: %v\n%s", err, getOutput)
	}
	var getResult struct {
		Value []byte `json:"value"`
	}
	if err := json.Unmarshal([]byte(getOutput), &getResult); err != nil {
		t.Fatalf("decode GET output %q: %v", getOutput, err)
	}
	if !bytes.Equal(getResult.Value, []byte{1, 2, 3}) {
		t.Fatalf("GET Value = %v, want latest completed SET Value %v", getResult.Value, []byte{1, 2, 3})
	}

	for _, process := range processes {
		process.stop()
	}
	processes = make(map[string]*nodeProcess, 3)
	for id, cfg := range configs {
		processes[id] = startNodeProcess(t, cfg)
	}
	restartedLeader := waitForStableLeader(t, members, nil, processTestDeadline)
	if output, err := runCLISet(members[restartedLeader].ClientAddress, sessionID, 2, "after-restart", "present", 5*time.Second); err != nil {
		t.Fatalf("SET using recovered Client Session after restarting all Nodes: %v\n%s", err, output)
	}
	first = waitForSingleLeader(t, members, nil, processTestDeadline)

	processes[first].stop()
	delete(processes, first)

	replacement := waitForSingleLeader(t, members, map[string]bool{first: true}, processTestDeadline)
	if replacement == first {
		t.Fatalf("replacement Leader = stopped Node %q", first)
	}
	replacementFollower := memberOtherThan(t, members, map[string]bool{first: true, replacement: true})
	if output, err := runCLISet(replacementFollower.ClientAddress, sessionID, 3, "empty", "", 5*time.Second); err != nil {
		t.Fatalf("SET empty Value through replacement Leader: %v\n%s", err, output)
	}
	emptyOutput, err := runCLIGet(replacementFollower.ClientAddress, "empty", 5*time.Second)
	if err != nil {
		t.Fatalf("GET present empty Value: %v\n%s", err, emptyOutput)
	}
	var emptyResult struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.Unmarshal([]byte(emptyOutput), &emptyResult); err != nil || !emptyResult.Found || len(emptyResult.Value) != 0 {
		t.Fatalf("GET empty Value output = %q, decoded=%#v, error=%v; want found empty bytes", emptyOutput, emptyResult, err)
	}
	deleteOutput, err := runCLIDelete(replacementFollower.ClientAddress, sessionID, 4, "empty", 5*time.Second)
	if err != nil || !strings.Contains(deleteOutput, `"existed":true`) {
		t.Fatalf("DELETE existing Key output = %q, error = %v; want existed=true", deleteOutput, err)
	}
	requestCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.New(replacementFollower.ClientAddress).Get(requestCtx, "empty")
	cancel()
	if status.Code(err) != codes.NotFound || !hasKeyNotFoundDetail(err, "empty") {
		t.Fatalf("GET deleted Key error = %v, want typed NotFound", err)
	}
	deleteOutput, err = runCLIDelete(replacementFollower.ClientAddress, sessionID, 5, "empty", 5*time.Second)
	if err != nil || !strings.Contains(deleteOutput, `"existed":false`) {
		t.Fatalf("DELETE missing Key output = %q, error = %v; want existed=false", deleteOutput, err)
	}
	sessionClient = client.New(replacementFollower.ClientAddress)
	requestCtx, cancel = context.WithTimeout(context.Background(), processTestDeadline)
	err = sessionClient.CloseSession(requestCtx, sessionID)
	cancel()
	if err != nil {
		t.Fatalf("close replicated Client Session after Leader change: %v", err)
	}
	deleteOutput, err = runCLIDelete(replacementFollower.ClientAddress, sessionID, 6, "empty", 5*time.Second)
	if status.Code(err) != codes.FailedPrecondition || !hasInvalidSessionDetail(err, quorumkvv1.InvalidSessionReason_INVALID_SESSION_REASON_CLOSED) {
		t.Fatalf("DELETE with closed Client Session output = %q, error = %v; want typed closed Session", deleteOutput, err)
	}

	requestCtx, cancel = context.WithTimeout(context.Background(), processTestDeadline)
	isolationSession, err := sessionClient.OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session for Quorum-loss check: %v", err)
	}
	isolate := memberIDForAddress(t, members, replacementFollower.ClientAddress)
	processes[isolate].stop()
	delete(processes, isolate)
	output, err := runCLISet(members[replacement].ClientAddress, isolationSession, 1, "must-not-commit", "value", 1500*time.Millisecond)
	if err == nil || strings.Contains(output, `"stored":true`) {
		t.Fatalf("SET without Quorum output = %q, error = %v; want timeout or Unavailable without success", output, err)
	}
	requestCtx, cancel = context.WithTimeout(context.Background(), 1500*time.Millisecond)
	_, err = client.New(members[replacement].ClientAddress).Get(requestCtx, "opaque")
	cancel()
	if err == nil {
		t.Fatal("GET from minority partition succeeded, want unavailable or deadline failure")
	}

	for id, process := range processes {
		select {
		case err := <-process.done:
			process.stopped = true
			t.Fatalf("surviving Node %q exited early (%v): %s", id, err, process.output.String())
		default:
		}
	}
}

func nodeProcessOutputs(processes map[string]*nodeProcess) map[string]string {
	outputs := make(map[string]string, len(processes))
	for id, process := range processes {
		outputs[id] = process.output.String()
	}
	return outputs
}

func runCLISet(address string, sessionID [16]byte, sequence uint64, key, value string, timeout time.Duration) (string, error) {
	var output bytes.Buffer
	err := cli.Run([]string{
		"--address", address,
		"--timeout", timeout.String(),
		"set", hex.EncodeToString(sessionID[:]), strconv.FormatUint(sequence, 10), key, value,
	}, &output)
	return output.String(), err
}

func runCLIGet(address, key string, timeout time.Duration) (string, error) {
	var output bytes.Buffer
	err := cli.Run([]string{"--address", address, "--timeout", timeout.String(), "get", key}, &output)
	return output.String(), err
}

func runCLIDelete(address string, sessionID [16]byte, sequence uint64, key string, timeout time.Duration) (string, error) {
	var output bytes.Buffer
	err := cli.Run([]string{
		"--address", address,
		"--timeout", timeout.String(),
		"delete", hex.EncodeToString(sessionID[:]), strconv.FormatUint(sequence, 10), key,
	}, &output)
	return output.String(), err
}

func hasValidationDetail(err error, field string) bool {
	for _, detail := range status.Convert(err).Details() {
		validation, ok := detail.(*quorumkvv1.ValidationError)
		if ok && validation.Field == field {
			return true
		}
	}
	return false
}

func hasKeyNotFoundDetail(err error, key string) bool {
	for _, detail := range status.Convert(err).Details() {
		notFound, ok := detail.(*quorumkvv1.KeyNotFound)
		if ok && notFound.Key == key {
			return true
		}
	}
	return false
}

func hasInvalidSessionDetail(err error, reason quorumkvv1.InvalidSessionReason) bool {
	for _, detail := range status.Convert(err).Details() {
		invalid, ok := detail.(*quorumkvv1.InvalidSession)
		if ok && invalid.Reason == reason {
			return true
		}
	}
	return false
}

func memberIDForAddress(t *testing.T, members map[string]config.Member, address string) string {
	t.Helper()
	for id, member := range members {
		if member.ClientAddress == address {
			return id
		}
	}
	t.Fatalf("no Cluster member has client address %q", address)
	return ""
}

func memberOtherThan(t *testing.T, members map[string]config.Member, excluded map[string]bool) config.Member {
	t.Helper()
	for id, member := range members {
		if !excluded[id] {
			return member
		}
	}
	t.Fatal("no eligible Cluster member")
	return config.Member{}
}

func TestNodeProcessHelper(t *testing.T) {
	path := os.Getenv("QUORUMKV_NODE_PROCESS_CONFIG")
	if path == "" {
		return
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := node.New(cfg).Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

type nodeProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	stopped bool
}

func startNodeProcess(t *testing.T, cfg config.Config) *nodeProcess {
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
	process := &nodeProcess{done: make(chan error, 1)}
	process.command = exec.Command(executable, "-test.run=^TestNodeProcessHelper$")
	process.command.Env = append(os.Environ(), "QUORUMKV_NODE_PROCESS_CONFIG="+path)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start Node %q: %v", cfg.Node.ID, err)
	}
	go func() { process.done <- process.command.Wait() }()
	return process
}

func (p *nodeProcess) stop() {
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

func waitForSingleLeader(t *testing.T, members map[string]config.Member, excluded map[string]bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var observed map[string]*quorumkvv1.GetStatusResponse
	for time.Now().Before(deadline) {
		observed = make(map[string]*quorumkvv1.GetStatusResponse)
		for id, member := range members {
			if excluded[id] {
				continue
			}
			if status := fetchStatus(member.ClientAddress); status != nil {
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
				return leader
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	details := make(map[string]string, len(observed))
	for id, state := range observed {
		details[id] = fmt.Sprintf("role=%s leader=%q term=%d", state.Role, state.LeaderId, state.Term)
	}
	t.Fatalf("did not observe one agreed Leader within %v; last status: %#v", timeout, details)
	return ""
}

func waitForStableLeader(t *testing.T, members map[string]config.Member, excluded map[string]bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		first := waitForSingleLeader(t, members, excluded, time.Until(deadline))
		time.Sleep(checkQuorumObservationWindow)
		second := waitForSingleLeader(t, members, excluded, time.Until(deadline))
		if first == second {
			return first
		}
	}
	t.Fatalf("did not observe stable Leader within %v", timeout)
	return ""
}

const checkQuorumObservationWindow = 1200 * time.Millisecond

func fetchStatus(address string) *quorumkvv1.GetStatusResponse {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	status, err := quorumkvv1.NewNodeServiceClient(connection).GetStatus(ctx, &quorumkvv1.GetStatusRequest{})
	if err != nil {
		return nil
	}
	return status
}
