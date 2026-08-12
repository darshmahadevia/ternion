// Package linearizability provides a client-side history recorder and a
// checker for the QuorumKV data contract.
package linearizability

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

type Kind uint8

const (
	Set Kind = iota
	Get
	Delete
)

// Outcome is the observable result of one client command. Error contains a
// stable semantic category, not a server or transport error string.
type Outcome struct {
	Value   []byte
	Found   bool
	Existed bool
	Error   string
}

// Operation is one invocation. A zero Completed time denotes a pending call.
// Invocation and completion times are captured by the client harness only;
// server timestamps are deliberately not part of the model.
type Operation struct {
	Kind     Kind
	Session  [16]byte
	Sequence uint64
	Key      string
	Value    []byte
	Invoke   time.Time
	Complete time.Time
	Outcome  Outcome
}

// History is a concurrently appendable client history.
type History struct {
	mu         sync.Mutex
	operations []Operation
}

// Start records an invocation and returns a completion function. The function
// may be called with a pending outcome (or never called) when a deadline makes
// the mutation's result unknown.
func (h *History) Start(kind Kind, session [16]byte, sequence uint64, key string, value []byte) func(Outcome) {
	op := Operation{Kind: kind, Session: session, Sequence: sequence, Key: key, Value: append([]byte(nil), value...), Invoke: time.Now()}
	h.mu.Lock()
	index := len(h.operations)
	h.operations = append(h.operations, op)
	h.mu.Unlock()
	return func(outcome Outcome) {
		h.mu.Lock()
		defer h.mu.Unlock()
		if index >= len(h.operations) || !h.operations[index].Complete.IsZero() {
			return
		}
		h.operations[index].Complete = time.Now()
		h.operations[index].Outcome = cloneOutcome(outcome)
	}
}

// Operations returns a stable snapshot of the captured history.
func (h *History) Operations() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]Operation, len(h.operations))
	for i, operation := range h.operations {
		result[i] = operation
		result[i].Value = append([]byte(nil), operation.Value...)
		result[i].Outcome = cloneOutcome(operation.Outcome)
	}
	return result
}

// Check reports whether the history has a legal sequential explanation. A
// pending operation may be omitted, which models a timeout whose result is
// unknown. Repeating a Session/Sequence is the same logical mutation from the
// model's perspective and returns the cached mutation result.
func Check(history []Operation) error {
	for _, operation := range history {
		if operation.Invoke.IsZero() {
			return fmt.Errorf("operation has no invocation time")
		}
		if !operation.Complete.IsZero() && operation.Complete.Before(operation.Invoke) {
			return fmt.Errorf("operation completed before invocation")
		}
	}
	model := newModel()
	used := make([]bool, len(history))
	if checkOrder(history, used, model, 0) {
		return nil
	}
	return fmt.Errorf("history is not linearizable")
}

type sessionState struct {
	last   uint64
	result Outcome
}
type model struct {
	values   map[string][]byte
	sessions map[[16]byte]sessionState
}

func newModel() model {
	return model{values: make(map[string][]byte), sessions: make(map[[16]byte]sessionState)}
}

func (m model) clone() model {
	copyModel := newModel()
	for key, value := range m.values {
		copyModel.values[key] = append([]byte(nil), value...)
	}
	for session, state := range m.sessions {
		copyModel.sessions[session] = sessionState{last: state.last, result: cloneOutcome(state.result)}
	}
	return copyModel
}

func checkOrder(history []Operation, used []bool, state model, placed int) bool {
	if placed == len(history) {
		return true
	}
	for index, operation := range history {
		if used[index] || !eligible(index, history, used) {
			continue
		}
		candidate := state.clone()
		actual := candidate.apply(operation)
		if !outcomeEqual(actual, operation.Outcome) {
			continue
		}
		used[index] = true
		if checkOrder(history, used, candidate, placed+1) {
			return true
		}
		used[index] = false
	}
	// A call without a completion is allowed to have no linearization point.
	for index, operation := range history {
		if used[index] || !operation.Complete.IsZero() || !eligible(index, history, used) {
			continue
		}
		used[index] = true
		if checkOrder(history, used, state, placed+1) {
			return true
		}
		used[index] = false
	}
	return false
}

func eligible(index int, history []Operation, used []bool) bool {
	candidate := history[index]
	for prior, operation := range history {
		if used[prior] || prior == index || operation.Complete.IsZero() {
			continue
		}
		// A completed operation that finished before this invocation must be
		// linearized first. Pending operations impose no real-time order.
		if !candidate.Invoke.Before(operation.Complete) {
			return false
		}
	}
	return true
}

func (m model) apply(operation Operation) Outcome {
	if operation.Kind == Set || operation.Kind == Delete {
		state, exists := m.sessions[operation.Session]
		if exists && operation.Sequence == state.last {
			return cloneOutcome(state.result)
		}
		if exists && operation.Sequence <= state.last {
			return Outcome{Error: "stale_sequence"}
		}
		if exists && operation.Sequence != state.last+1 {
			return Outcome{Error: "out_of_order"}
		}
		var result Outcome
		if operation.Kind == Set {
			m.values[operation.Key] = append([]byte(nil), operation.Value...)
		} else {
			_, result.Existed = m.values[operation.Key]
			delete(m.values, operation.Key)
		}
		m.sessions[operation.Session] = sessionState{last: operation.Sequence, result: cloneOutcome(result)}
		return result
	}
	value, ok := m.values[operation.Key]
	if !ok {
		return Outcome{Error: "not_found"}
	}
	return Outcome{Found: true, Value: append([]byte(nil), value...)}
}

func outcomeEqual(left, right Outcome) bool {
	return left.Error == right.Error && left.Found == right.Found && left.Existed == right.Existed && bytes.Equal(left.Value, right.Value)
}

func cloneOutcome(outcome Outcome) Outcome {
	outcome.Value = append([]byte(nil), outcome.Value...)
	return outcome
}
