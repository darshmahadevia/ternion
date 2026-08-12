package linearizability

import (
	"testing"
	"time"
)

func op(kind Kind, session [16]byte, sequence uint64, key string, value []byte, invoke, complete time.Time, outcome Outcome) Operation {
	return Operation{Kind: kind, Session: session, Sequence: sequence, Key: key, Value: value, Invoke: invoke, Complete: complete, Outcome: outcome}
}

func TestCheckModelsEmptyValuesMissingKeysDeletesAndRetries(t *testing.T) {
	base := time.Unix(0, 0)
	session := [16]byte{1}
	history := []Operation{
		op(Set, session, 1, "empty", []byte{}, base, base.Add(time.Second), Outcome{}),
		op(Get, session, 0, "empty", nil, base.Add(2*time.Second), base.Add(3*time.Second), Outcome{Found: true}),
		op(Delete, session, 2, "empty", nil, base.Add(4*time.Second), base.Add(5*time.Second), Outcome{Existed: true}),
		op(Get, session, 0, "empty", nil, base.Add(6*time.Second), base.Add(7*time.Second), Outcome{Error: "not_found"}),
		// The changed payload is not applied: this is a duplicate of sequence 2.
		op(Delete, session, 2, "empty", nil, base.Add(8*time.Second), base.Add(9*time.Second), Outcome{Existed: true}),
	}
	if err := Check(history); err != nil {
		t.Fatalf("Check() = %v", err)
	}
}

func TestCheckMayOmitTimedOutMutationButRetryIsOneLogicalMutation(t *testing.T) {
	base := time.Unix(0, 0)
	session := [16]byte{2}
	history := []Operation{
		op(Set, session, 1, "key", []byte("value"), base, time.Time{}, Outcome{}),
		op(Set, session, 1, "key", []byte("different"), base.Add(time.Second), base.Add(2*time.Second), Outcome{}),
		op(Get, session, 0, "key", nil, base.Add(3*time.Second), base.Add(4*time.Second), Outcome{Found: true, Value: []byte("value")}),
	}
	if err := Check(history); err != nil {
		t.Fatalf("Check() with pending mutation retry = %v", err)
	}
}

func TestCheckModelsOverlappingSessions(t *testing.T) {
	base := time.Unix(0, 0)
	first := [16]byte{5}
	second := [16]byte{6}
	history := []Operation{
		op(Set, first, 1, "shared", []byte("first"), base, base.Add(4*time.Second), Outcome{}),
		op(Set, second, 1, "shared", []byte("second"), base.Add(time.Second), base.Add(2*time.Second), Outcome{}),
		op(Get, first, 0, "shared", nil, base.Add(1500*time.Millisecond), base.Add(3500*time.Millisecond), Outcome{Found: true, Value: []byte("first")}),
	}
	if err := Check(history); err != nil {
		t.Fatalf("Check() for overlapping multi-session history = %v", err)
	}
}

func TestCheckFindsNonLinearizableHistory(t *testing.T) {
	base := time.Unix(0, 0)
	session := [16]byte{3}
	// The SET completed before the GET began, so the GET cannot return missing.
	history := []Operation{
		op(Set, session, 1, "key", []byte("value"), base, base.Add(time.Second), Outcome{}),
		op(Get, session, 0, "key", nil, base.Add(2*time.Second), base.Add(3*time.Second), Outcome{Error: "not_found"}),
	}
	if err := Check(history); err == nil {
		t.Fatal("Check() accepted a deliberately faulty history")
	}
}

func TestHistoryRecordsConcurrentInvocationsWithoutServerTimes(t *testing.T) {
	var history History
	session := [16]byte{4}
	complete := history.Start(Set, session, 1, "key", []byte("value"))
	complete(Outcome{})
	operations := history.Operations()
	if len(operations) != 1 || operations[0].Invoke.IsZero() || operations[0].Complete.IsZero() {
		t.Fatalf("captured operation = %#v, want invocation and completion", operations)
	}
}
