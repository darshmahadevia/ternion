package node

import (
	"context"
	"reflect"
	"strings"
	"testing"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSessionMachineEnforcesLimitAndPermanentClosure(t *testing.T) {
	machine := newSessionMachine(1)
	first := raft.SessionID{1}
	second := raft.SessionID{2}

	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: first}); result.failure != sessionSucceeded {
		t.Fatalf("open first Client Session = %v, want success", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: second}); result.failure != sessionLimitReached {
		t.Fatalf("open above limit = %v, want limit reached", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: first}); result.failure != sessionSucceeded {
		t.Fatalf("close first Client Session = %v, want success", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: first}); result.failure != sessionAlreadyExists {
		t.Fatalf("reopen closed Client Session = %v, want already used", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: first}); result.failure != sessionClosed {
		t.Fatalf("close closed Client Session = %v, want closed", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: second}); result.failure != sessionUnknown {
		t.Fatalf("close unknown Client Session = %v, want unknown", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: second}); result.failure != sessionSucceeded {
		t.Fatalf("open after releasing capacity = %v, want success", result.failure)
	}
}

func TestFollowerReturnsTypedLeaderHint(t *testing.T) {
	n := New(config.Config{
		ClusterID:          "cluster-1",
		ActiveSessionLimit: 1,
		Node:               config.Node{ID: "node-1"},
		Members: map[string]config.Member{
			"node-1": {ClientAddress: "127.0.0.1:7401"},
			"node-2": {ClientAddress: "127.0.0.1:7402"},
			"node-3": {ClientAddress: "127.0.0.1:7403"},
		},
	})
	n.publishRaftState(raft.State{ID: "node-1", Role: raft.Follower, LeaderID: "node-2", Term: 3})

	_, err := n.OpenSession(context.Background(), &quorumkvv1.OpenSessionRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("OpenSession() error = %v, want FailedPrecondition", err)
	}
	for _, detail := range status.Convert(err).Details() {
		notLeader, ok := detail.(*quorumkvv1.NotLeader)
		if ok {
			if notLeader.LeaderId != "node-2" || notLeader.LeaderAddress != "127.0.0.1:7402" {
				t.Fatalf("NotLeader detail = %#v, want node-2 address", notLeader)
			}
			return
		}
	}
	t.Fatal("OpenSession() error has no typed NotLeader detail")
}

func TestGetFollowerReturnsTypedLeaderHint(t *testing.T) {
	n := New(config.Config{
		ClusterID: "cluster-1",
		Node:      config.Node{ID: "node-1"},
		Members: map[string]config.Member{
			"node-1": {ClientAddress: "127.0.0.1:7401"},
			"node-2": {ClientAddress: "127.0.0.1:7402"},
			"node-3": {ClientAddress: "127.0.0.1:7403"},
		},
	})
	n.publishRaftState(raft.State{ID: "node-1", Role: raft.Follower, LeaderID: "node-2", Term: 3})

	_, err := n.Get(context.Background(), &quorumkvv1.GetRequest{Key: "key"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Get() error = %v, want FailedPrecondition", err)
	}
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatalf("Get() details = %#v, want typed NotLeader", details)
	}
	notLeader, ok := details[0].(*quorumkvv1.NotLeader)
	if !ok || notLeader.LeaderId != "node-2" || notLeader.LeaderAddress != "127.0.0.1:7402" {
		t.Fatalf("Get() NotLeader detail = %#v, want node-2 address", details[0])
	}
}

func TestSessionMachineStoresCopiedOpaqueAndEmptyValuesInSequence(t *testing.T) {
	machine := newSessionMachine(1)
	sessionID := raft.SessionID{1}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: sessionID}); result.failure != sessionSucceeded {
		t.Fatalf("open Client Session = %v, want success", result.failure)
	}

	source := []byte{0, 1, 255}
	if result := machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 1, Key: "opaque", Value: source}); result.failure != sessionSucceeded {
		t.Fatalf("apply opaque SET = %v, want success", result.failure)
	}
	source[0] = 9
	if got := machine.values["opaque"]; !reflect.DeepEqual(got, []byte{0, 1, 255}) {
		t.Fatalf("stored opaque Value = %v, want copied bytes", got)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 2, Key: "empty", Value: []byte{}}); result.failure != sessionSucceeded {
		t.Fatalf("apply empty SET = %v, want success", result.failure)
	}
	if value, exists := machine.values["empty"]; !exists || len(value) != 0 {
		t.Fatalf("stored empty Value = %v, exists=%v; want present empty Value", value, exists)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 2, Key: "duplicate", Value: []byte("must not apply")}); result.failure != sessionSucceeded {
		t.Fatalf("apply duplicate latest SET = %v, want cached success", result.failure)
	}
	if _, exists := machine.values["duplicate"]; exists {
		t.Fatal("duplicate latest SET created a second effect")
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 1, Key: "stale"}); result.failure != sessionStaleSequence {
		t.Fatalf("apply stale SET = %v, want stale sequence", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 4, Key: "gap"}); result.failure != sessionOutOfOrderSequence {
		t.Fatalf("apply skipped SET = %v, want out-of-order sequence", result.failure)
	}
}

func TestSessionMachineDeletesIdempotentlyAndCachesResult(t *testing.T) {
	machine := newSessionMachine(1)
	sessionID := raft.SessionID{1}
	machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: sessionID})
	machine.apply(raft.LogEntry{Type: raft.EntrySet, SessionID: sessionID, Sequence: 1, Key: "key", Value: []byte("value")})

	first := machine.apply(raft.LogEntry{Type: raft.EntryDelete, SessionID: sessionID, Sequence: 2, Key: "key"})
	if first.failure != sessionSucceeded || !first.existed {
		t.Fatalf("DELETE existing Key = %#v, want success with existed=true", first)
	}
	if _, exists := machine.values["key"]; exists {
		t.Fatal("DELETE left the Key present")
	}

	// A different Client Session may recreate the Key before the response is
	// retried. The duplicate must still return the committed original result.
	machine.values["key"] = []byte("replacement")
	duplicate := machine.apply(raft.LogEntry{Type: raft.EntryDelete, SessionID: sessionID, Sequence: 2, Key: "key"})
	if duplicate.failure != sessionSucceeded || !duplicate.existed {
		t.Fatalf("duplicate DELETE = %#v, want cached existed=true", duplicate)
	}
	if got := string(machine.values["key"]); got != "replacement" {
		t.Fatalf("duplicate DELETE changed recreated Value to %q", got)
	}

	missing := machine.apply(raft.LogEntry{Type: raft.EntryDelete, SessionID: sessionID, Sequence: 3, Key: "missing"})
	if missing.failure != sessionSucceeded || missing.existed {
		t.Fatalf("DELETE missing Key = %#v, want success with existed=false", missing)
	}
}

func TestSequenceFailuresHaveDistinctTypedDetails(t *testing.T) {
	n := New(config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1"}})
	tests := []struct {
		name   string
		result proposalResult
		detail any
	}{
		{
			name:   "stale",
			result: proposalResult{failure: sessionStaleSequence, sequence: 1, wantSequence: 2},
			detail: &quorumkvv1.StaleSequence{},
		},
		{
			name:   "out of order",
			result: proposalResult{failure: sessionOutOfOrderSequence, sequence: 4, wantSequence: 3},
			detail: &quorumkvv1.OutOfOrderSequence{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := n.proposalError(test.result)
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("proposal error = %v, want FailedPrecondition", err)
			}
			details := status.Convert(err).Details()
			if len(details) != 1 || reflect.TypeOf(details[0]) != reflect.TypeOf(test.detail) {
				t.Fatalf("proposal details = %#v, want %T", details, test.detail)
			}
		})
	}
}

func TestSetRejectsInvalidInputBeforeProposal(t *testing.T) {
	n := New(config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1"}})
	valid := func() *quorumkvv1.SetRequest {
		return &quorumkvv1.SetRequest{SessionId: make([]byte, 16), Sequence: 1, Key: "key"}
	}
	tests := []struct {
		name   string
		field  string
		change func(*quorumkvv1.SetRequest)
	}{
		{name: "session identity", field: "session_id", change: func(request *quorumkvv1.SetRequest) { request.SessionId = nil }},
		{name: "zero sequence", field: "sequence", change: func(request *quorumkvv1.SetRequest) { request.Sequence = 0 }},
		{name: "empty Key", field: "key", change: func(request *quorumkvv1.SetRequest) { request.Key = "" }},
		{name: "invalid UTF-8 Key", field: "key", change: func(request *quorumkvv1.SetRequest) { request.Key = string([]byte{0xff}) }},
		{name: "oversized Key", field: "key", change: func(request *quorumkvv1.SetRequest) { request.Key = strings.Repeat("k", maxKeyBytes+1) }},
		{name: "oversized Value", field: "value", change: func(request *quorumkvv1.SetRequest) { request.Value = make([]byte, maxValueBytes+1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid()
			test.change(request)
			_, err := n.Set(context.Background(), request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Set() error = %v, want InvalidArgument", err)
			}
			details := status.Convert(err).Details()
			if len(details) != 1 {
				t.Fatalf("Set() error details = %#v, want one ValidationError for %q", details, test.field)
			}
			validation, ok := details[0].(*quorumkvv1.ValidationError)
			if !ok || validation.Field != test.field {
				t.Fatalf("Set() error details = %#v, want ValidationError for %q", details, test.field)
			}
		})
	}
}

func TestDeleteRejectsInvalidInputBeforeProposal(t *testing.T) {
	n := New(config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1"}})
	requests := []*quorumkvv1.DeleteRequest{
		{Sequence: 1, Key: "key"},
		{SessionId: make([]byte, 16), Key: "key"},
		{SessionId: make([]byte, 16), Sequence: 1},
		{SessionId: make([]byte, 16), Sequence: 1, Key: string([]byte{0xff})},
		{SessionId: make([]byte, 16), Sequence: 1, Key: strings.Repeat("k", maxKeyBytes+1)},
	}
	for _, request := range requests {
		if _, err := n.Delete(context.Background(), request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Delete(%#v) error = %v, want InvalidArgument before NotLeader", request, err)
		}
	}
}

func TestGetRejectsInvalidKeyBeforeReadConfirmation(t *testing.T) {
	n := New(config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1"}})
	for _, key := range []string{"", string([]byte{0xff}), strings.Repeat("k", maxKeyBytes+1)} {
		_, err := n.Get(context.Background(), &quorumkvv1.GetRequest{Key: key})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Get(%q) error = %v, want InvalidArgument", key, err)
		}
	}
}
