package simulation

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

// RandomConfig bounds one seeded fault simulation.
type RandomConfig struct {
	Seed  int64
	Steps int
}

// FaultCoverage records the fault classes exercised by a run.
type FaultCoverage struct {
	MessageDelay     bool `json:"message_delay"`
	MessageDrop      bool `json:"message_drop"`
	MessageDuplicate bool `json:"message_duplicate"`
	MessageReorder   bool `json:"message_reorder"`
	Asymmetric       bool `json:"asymmetric_partition"`
	Crash            bool `json:"crash"`
	Restart          bool `json:"restart"`
	PersistenceDelay bool `json:"persistence_delay"`
}

// TraceEvent is one ordered scheduler decision. Detail is deliberately textual
// so a failed CI run remains useful without a custom trace viewer.
type TraceEvent struct {
	Sequence int    `json:"sequence"`
	Kind     string `json:"kind"`
	Node     string `json:"node,omitempty"`
	Detail   string `json:"detail"`
}

// RandomResult contains the seed and complete replay evidence for one run.
type RandomResult struct {
	Seed    int64         `json:"seed"`
	Steps   int           `json:"steps"`
	Faults  FaultCoverage `json:"faults"`
	Trace   []TraceEvent  `json:"trace"`
	Failure string        `json:"failure,omitempty"`
}

// WriteTrace writes a self-contained CI artifact, replacing an earlier replay
// of the same seed on every supported development platform.
func (r RandomResult) WriteTrace(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode simulation trace: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write simulation trace: %w", err)
	}
	return nil
}

type queuedKind uint8

const (
	queuedTimer queuedKind = iota
	queuedMessage
	queuedPersistence
	queuedClient
)

type queuedEvent struct {
	kind        queuedKind
	from        raft.NodeID
	to          raft.NodeID
	event       raft.Event
	persistence raft.Action
	availableAt int
}

type durableNode struct {
	hard   raft.HardState
	log    []raft.LogEntry
	commit uint64
}

type modelSession struct {
	closed       bool
	lastSequence uint64
	deleteResult bool
}

type modelState struct {
	applied  uint64
	values   map[string][]byte
	sessions map[raft.SessionID]modelSession
}

type randomCluster struct {
	seed         int64
	random       *rand.Rand
	nodes        map[raft.NodeID]*raft.Node
	alive        map[raft.NodeID]bool
	connected    map[raft.NodeID]map[raft.NodeID]bool
	durable      map[raft.NodeID]*durableNode
	models       map[raft.NodeID]*modelState
	queue        []queuedEvent
	trace        []TraceEvent
	faults       FaultCoverage
	leaders      map[uint64]raft.NodeID
	committed    map[uint64]raft.LogEntry
	lastState    map[raft.NodeID]raft.State
	incarnation  map[raft.NodeID]uint64
	appliedSeen  map[raft.NodeID]map[uint64]uint64
	invariantErr error
	step         int
}

// RunRandomized executes a deterministic three-Node schedule. The healthy
// prefix establishes an acknowledged mutation; the remaining schedule then
// exercises every allowed fault class while continuing to deliver random work.
func RunRandomized(config RandomConfig) (RandomResult, error) {
	if config.Steps < 1 {
		return RandomResult{}, errors.New("simulation steps must be positive")
	}
	cluster := newRandomCluster(config.Seed)
	if err := cluster.bootstrap(); err != nil {
		return cluster.result(config.Steps, err), err
	}
	if err := cluster.injectFaultFrontier(); err != nil {
		return cluster.result(config.Steps, err), err
	}
	for cluster.step < config.Steps {
		cluster.enqueueRandomWork()
		if !cluster.runOne(true) {
			cluster.enqueueTimer(threeNodeIDs[cluster.random.Intn(len(threeNodeIDs))], raft.ElectionTimeout{})
			continue
		}
		if err := cluster.assertInvariants(); err != nil {
			wrapped := fmt.Errorf("seed %d after trace event %d: %w", config.Seed, len(cluster.trace), err)
			return cluster.result(config.Steps, wrapped), wrapped
		}
	}
	return cluster.result(config.Steps, nil), nil
}

func newRandomCluster(seed int64) *randomCluster {
	c := &randomCluster{
		seed: seed, random: rand.New(rand.NewSource(seed)), // #nosec G404 -- reproducible fault scheduling.
		nodes: make(map[raft.NodeID]*raft.Node), alive: make(map[raft.NodeID]bool),
		connected: make(map[raft.NodeID]map[raft.NodeID]bool), durable: make(map[raft.NodeID]*durableNode),
		models: make(map[raft.NodeID]*modelState), leaders: make(map[uint64]raft.NodeID),
		committed: make(map[uint64]raft.LogEntry), lastState: make(map[raft.NodeID]raft.State),
		incarnation: make(map[raft.NodeID]uint64), appliedSeen: make(map[raft.NodeID]map[uint64]uint64),
	}
	for _, id := range threeNodeIDs {
		var peers []raft.NodeID
		c.connected[id] = make(map[raft.NodeID]bool)
		for _, peer := range threeNodeIDs {
			if peer != id {
				peers = append(peers, peer)
				c.connected[id][peer] = true
			}
		}
		c.nodes[id] = raft.NewNode(id, peers)
		c.alive[id] = true
		c.durable[id] = &durableNode{}
		c.models[id] = newModelState()
		c.incarnation[id] = 1
		c.appliedSeen[id] = make(map[uint64]uint64)
	}
	return c
}

func (c *randomCluster) bootstrap() error {
	// The random first timeout makes the whole schedule seed-dependent while
	// priority delivery lets the initial election complete without relying on luck.
	c.enqueueTimer(threeNodeIDs[c.random.Intn(len(threeNodeIDs))], raft.ElectionTimeout{})
	for attempts := 0; attempts < 5000; attempts++ {
		if leader := c.leader(); leader != "" && c.nodes[leader].State().ReadReady {
			sessionID := raft.SessionID{1}
			c.enqueueClient(leader, raft.ProposeSession{ProposalID: 1, Type: raft.EntryOpenSession, SessionID: sessionID})
			if err := c.drainUntil(func() bool {
				_, exists := c.models[leader].sessions[sessionID]
				return exists
			}); err != nil {
				return err
			}
			c.enqueueClient(leader, raft.ProposeSet{ProposalID: 2, SessionID: sessionID, Sequence: 1, Key: "seed", Value: []byte("acknowledged")})
			return c.drainUntil(func() bool { return string(c.models[leader].values["seed"]) == "acknowledged" })
		}
		if !c.runOne(false) {
			return errors.New("bootstrap scheduler ran out of events before electing a Leader")
		}
		if err := c.assertInvariants(); err != nil {
			return fmt.Errorf("bootstrap invariant: %w", err)
		}
	}
	return errors.New("bootstrap did not elect a read-ready Leader")
}

func (c *randomCluster) drainUntil(done func() bool) error {
	for attempts := 0; attempts < 5000; attempts++ {
		if done() {
			return nil
		}
		if !c.runOne(false) {
			return errors.New("healthy scheduler ran out of events")
		}
		if err := c.assertInvariants(); err != nil {
			return err
		}
	}
	return errors.New("healthy scheduler made no progress")
}

func (c *randomCluster) injectFaultFrontier() error {
	// Delay persistence before allowing it to complete.
	leader := c.leader()
	c.enqueueClient(leader, raft.ProposeSet{ProposalID: 3, SessionID: raft.SessionID{1}, Sequence: 2, Key: "fault", Value: []byte("value")})
	if err := c.runUntilKind(queuedPersistence); err != nil {
		return err
	}
	if event := c.first(queuedPersistence); event != nil {
		event.availableAt = c.step + 4
		c.faults.PersistenceDelay = true
		c.record("persistence-delay", event.to, fmt.Sprintf("delay %T", event.persistence))
	}

	// A heartbeat round supplies independent messages for delay, duplication,
	// reordering, and drop without manufacturing protocol messages.
	if leader == "" {
		leader = c.leader()
	}
	c.enqueueTimer(leader, raft.HeartbeatTimeout{})
	if err := c.runUntilKind(queuedMessage); err != nil {
		return err
	}
	messages := c.indices(queuedMessage)
	if len(messages) > 0 {
		item := c.queue[messages[0]]
		c.queue[messages[0]].availableAt = c.step + 4
		c.faults.MessageDelay = true
		c.record("message-delay", item.to, fmt.Sprintf("%s -> %s %T", item.from, item.to, item.event))
		c.queue = append(c.queue, item)
		c.faults.MessageDuplicate = true
		c.record("message-duplicate", item.to, fmt.Sprintf("%s -> %s %T", item.from, item.to, item.event))
	}
	messages = c.indices(queuedMessage)
	if len(messages) >= 2 {
		i, j := messages[0], messages[1]
		c.queue[i], c.queue[j] = c.queue[j], c.queue[i]
		c.faults.MessageReorder = true
		c.record("message-reorder", "", "swap two pending messages")
	}
	messages = c.indices(queuedMessage)
	if len(messages) > 0 {
		item := c.queue[messages[0]]
		c.remove(messages[0])
		c.faults.MessageDrop = true
		c.record("message-drop", item.to, fmt.Sprintf("%s -> %s %T", item.from, item.to, item.event))
	}

	from := leader
	var to raft.NodeID
	for _, id := range threeNodeIDs {
		if id != from {
			to = id
			break
		}
	}
	c.connected[from][to] = false
	c.faults.Asymmetric = true
	c.record("partition", from, fmt.Sprintf("block only %s -> %s", from, to))
	if err := c.assertInvariants(); err != nil {
		return err
	}
	c.enqueueTimer(from, raft.HeartbeatTimeout{})
	if err := c.deliverThroughPartition(from, to); err != nil {
		return err
	}

	victim := threeNodeIDs[c.random.Intn(len(threeNodeIDs))]
	c.crash(victim)
	if err := c.assertInvariants(); err != nil {
		return err
	}
	c.restart(victim)
	if err := c.assertInvariants(); err != nil {
		return err
	}
	c.connected[from][to] = true
	c.record("heal", from, fmt.Sprintf("restore %s -> %s", from, to))
	return c.assertInvariants()
}

func (c *randomCluster) deliverThroughPartition(from, to raft.NodeID) error {
	for attempts := 0; attempts < 100; attempts++ {
		before := len(c.trace)
		if !c.runOne(false) {
			break
		}
		if err := c.assertInvariants(); err != nil {
			return err
		}
		for _, event := range c.trace[before:] {
			if event.Kind == "message-drop" && event.Node == string(to) {
				return nil
			}
		}
	}
	return fmt.Errorf("asymmetric partition did not block a message from %s to %s", from, to)
}

func (c *randomCluster) runUntilKind(kind queuedKind) error {
	for attempts := 0; attempts < 100 && len(c.indices(kind)) == 0; attempts++ {
		if !c.runOne(false) {
			return nil
		}
		if err := c.assertInvariants(); err != nil {
			return err
		}
	}
	return nil
}

func (c *randomCluster) runOne(randomChoice bool) bool {
	eligible := c.eligible()
	if len(eligible) == 0 {
		c.step++
		return len(c.queue) > 0
	}
	var selected int
	if randomChoice {
		selected = eligible[c.random.Intn(len(eligible))]
	} else {
		selected = c.healthyEvent(eligible)
	}
	item := c.queue[selected]
	c.remove(selected)
	c.step++
	if !c.alive[item.to] {
		c.record("discard", item.to, fmt.Sprintf("target crashed: %T", item.event))
		return true
	}
	if item.kind == queuedMessage && !c.connected[item.from][item.to] {
		c.faults.MessageDrop = true
		c.record("message-drop", item.to, fmt.Sprintf("partition blocked %s -> %s %T", item.from, item.to, item.event))
		return true
	}
	if item.kind == queuedPersistence {
		c.completePersistence(item)
		c.record("persistence", item.to, fmt.Sprintf("complete %T", item.persistence))
	} else {
		c.record(kindName(item.kind), item.to, fmt.Sprintf("deliver %T from %s", item.event, item.from))
	}
	actions := c.nodes[item.to].Step(item.event)
	c.consumeActions(item.to, actions)
	return true
}

func (c *randomCluster) consumeActions(id raft.NodeID, actions []raft.Action) {
	for _, action := range actions {
		switch action := action.(type) {
		case raft.PersistHardState, raft.PersistLogEntries, raft.PersistCommitIndex:
			c.enqueue(queuedEvent{kind: queuedPersistence, to: id, persistence: action, event: persistenceCompletion(action)})
		case raft.SendPreVoteRequest:
			c.enqueueMessage(id, action.To, action.Request)
		case raft.SendPreVoteResponse:
			c.enqueueMessage(id, action.To, action.Response)
		case raft.SendVoteRequest:
			c.enqueueMessage(id, action.To, action.Request)
		case raft.SendVoteResponse:
			c.enqueueMessage(id, action.To, action.Response)
		case raft.SendAppendEntries:
			c.enqueueMessage(id, action.To, action.Request)
		case raft.SendAppendEntriesResponse:
			c.enqueueMessage(id, action.To, action.Response)
		case raft.ResetElectionTimer:
			c.enqueueTimer(id, raft.ElectionTimeout{})
		case raft.ResetHeartbeatTimer:
			c.enqueueTimer(id, raft.HeartbeatTimeout{})
		case raft.ResetCheckQuorumTimer:
			c.enqueueTimer(id, raft.CheckQuorumTimeout{})
		case raft.ApplyEntry:
			c.apply(id, action.Entry)
		case raft.BecameLeader:
			if prior := c.leaders[action.Term]; prior != "" && prior != id {
				// assertInvariants reports this with the complete trace context.
			} else {
				c.leaders[action.Term] = id
			}
		case raft.SendInstallSnapshot, raft.SendInstallSnapshotResponse:
			// Random runs do not compact history, so Snapshot traffic is unreachable.
		}
	}
}

func persistenceCompletion(action raft.Action) raft.Event {
	switch action := action.(type) {
	case raft.PersistHardState:
		return raft.HardStatePersisted{PersistenceID: action.PersistenceID}
	case raft.PersistLogEntries:
		return raft.LogEntriesPersisted{PersistenceID: action.PersistenceID}
	case raft.PersistCommitIndex:
		return raft.CommitIndexPersisted{PersistenceID: action.PersistenceID}
	default:
		panic(fmt.Sprintf("unsupported persistence action %T", action))
	}
}

func (c *randomCluster) completePersistence(item queuedEvent) {
	durable := c.durable[item.to]
	switch action := item.persistence.(type) {
	case raft.PersistHardState:
		durable.hard = raft.HardState{Term: action.Term, VotedFor: action.VotedFor}
	case raft.PersistLogEntries:
		if action.TruncateFrom > 0 {
			cut := sort.Search(len(durable.log), func(i int) bool { return durable.log[i].Index >= action.TruncateFrom })
			durable.log = append([]raft.LogEntry(nil), durable.log[:cut]...)
		}
		for _, entry := range action.Entries {
			if len(durable.log) > 0 && durable.log[len(durable.log)-1].Index >= entry.Index {
				cut := sort.Search(len(durable.log), func(i int) bool { return durable.log[i].Index >= entry.Index })
				durable.log = durable.log[:cut]
			}
			durable.log = append(durable.log, cloneEntry(entry))
		}
	case raft.PersistCommitIndex:
		durable.commit = action.CommitIndex
	}
}

func (c *randomCluster) apply(id raft.NodeID, entry raft.LogEntry) {
	model := c.models[id]
	if entry.Index != model.applied+1 {
		c.invariantErr = fmt.Errorf("ordered application: Node %s applied index %d after %d", id, entry.Index, model.applied)
		return
	}
	incarnation := c.incarnation[id]
	if c.appliedSeen[id][entry.Index] == incarnation {
		c.invariantErr = fmt.Errorf("once-only application: Node %s applied index %d twice in one incarnation", id, entry.Index)
		return
	}
	c.appliedSeen[id][entry.Index] = incarnation
	model.applied = entry.Index
	model.apply(entry)
	if existing, ok := c.committed[entry.Index]; ok && !reflect.DeepEqual(existing, entry) {
		c.invariantErr = fmt.Errorf("committed-prefix agreement: conflicting applied entry at index %d", entry.Index)
		return
	}
	c.committed[entry.Index] = cloneEntry(entry)
}

func newModelState() *modelState {
	return &modelState{values: make(map[string][]byte), sessions: make(map[raft.SessionID]modelSession)}
}

func (m *modelState) apply(entry raft.LogEntry) {
	switch entry.Type {
	case raft.EntryOpenSession:
		if _, exists := m.sessions[entry.SessionID]; !exists {
			m.sessions[entry.SessionID] = modelSession{}
		}
	case raft.EntryCloseSession:
		session, exists := m.sessions[entry.SessionID]
		if exists && !session.closed {
			session.closed = true
			m.sessions[entry.SessionID] = session
		}
	case raft.EntrySet, raft.EntryDelete:
		session, exists := m.sessions[entry.SessionID]
		if !exists || session.closed || entry.Sequence != session.lastSequence+1 {
			return
		}
		if entry.Type == raft.EntrySet {
			m.values[entry.Key] = append([]byte(nil), entry.Value...)
			session.deleteResult = false
		} else {
			_, session.deleteResult = m.values[entry.Key]
			delete(m.values, entry.Key)
		}
		session.lastSequence = entry.Sequence
		m.sessions[entry.SessionID] = session
	}
}

func (c *randomCluster) assertInvariants() error {
	if c.invariantErr != nil {
		return c.invariantErr
	}
	for term, first := range c.leaders {
		for id, node := range c.nodes {
			if c.alive[id] && node.State().Role == raft.Leader && node.State().Term == term && id != first {
				return fmt.Errorf("election safety: Term %d Leaders %s and %s", term, first, id)
			}
		}
	}
	for id, node := range c.nodes {
		if !c.alive[id] {
			continue
		}
		state := node.State()
		if previous, ok := c.lastState[id]; ok {
			if state.Term < previous.Term || state.CommitIndex < previous.CommitIndex || state.LastApplied < previous.LastApplied {
				return fmt.Errorf("monotonic progress: Node %s moved from %#v to %#v", id, previous, state)
			}
		}
		c.lastState[id] = state
		if state.LastApplied > state.CommitIndex || state.LastApplied != c.models[id].applied {
			return fmt.Errorf("ordered application: Node %s applied %d with commit %d and model %d", id, state.LastApplied, state.CommitIndex, c.models[id].applied)
		}
		if state.Role == raft.Leader {
			for index, entry := range c.committed {
				if index > uint64(len(c.durable[id].log)) || !reflect.DeepEqual(c.durable[id].log[index-1], entry) {
					return fmt.Errorf("leader completeness: Node %s lacks committed entry at index %d", id, index)
				}
			}
		}
	}
	for i, left := range threeNodeIDs {
		for _, right := range threeNodeIDs[i+1:] {
			leftDurable, rightDurable := c.durable[left], c.durable[right]
			through := min(leftDurable.commit, rightDurable.commit)
			for index := uint64(1); index <= through; index++ {
				if index > uint64(len(leftDurable.log)) || index > uint64(len(rightDurable.log)) || !reflect.DeepEqual(leftDurable.log[index-1], rightDurable.log[index-1]) {
					return fmt.Errorf("committed-prefix agreement: Nodes %s and %s differ at %d", left, right, index)
				}
			}
			if c.models[left].applied == c.models[right].applied && !equalModel(c.models[left], c.models[right]) {
				return fmt.Errorf("equal applied index %d has unequal state on Nodes %s and %s", c.models[left].applied, left, right)
			}
		}
	}
	for index, entry := range c.committed {
		copies := 0
		for _, id := range threeNodeIDs {
			if index <= uint64(len(c.durable[id].log)) && reflect.DeepEqual(c.durable[id].log[index-1], entry) {
				copies++
			}
		}
		if copies < 2 {
			return fmt.Errorf("durability: acknowledged index %d remains on %d Nodes (durable: %s)", index, copies, c.durableSummary())
		}
	}
	return nil
}

func (c *randomCluster) durableSummary() string {
	var summary string
	for _, id := range threeNodeIDs {
		durable := c.durable[id]
		summary += fmt.Sprintf("%s(term=%d commit=%d log=", id, durable.hard.Term, durable.commit)
		for _, entry := range durable.log {
			summary += fmt.Sprintf("%d/%d,", entry.Index, entry.Term)
		}
		summary += ") "
	}
	return summary
}

func equalModel(left, right *modelState) bool {
	return left.applied == right.applied && reflect.DeepEqual(left.values, right.values) && reflect.DeepEqual(left.sessions, right.sessions)
}

func (c *randomCluster) crash(id raft.NodeID) {
	c.alive[id] = false
	filtered := c.queue[:0]
	for _, item := range c.queue {
		if item.to == id && item.kind != queuedMessage {
			continue
		}
		filtered = append(filtered, item)
	}
	c.queue = filtered
	c.faults.Crash = true
	c.record("crash", id, "stop Node; discard timers and incomplete persistence")
}

func (c *randomCluster) restart(id raft.NodeID) {
	durable := c.durable[id]
	var peers []raft.NodeID
	for _, peer := range threeNodeIDs {
		if peer != id {
			peers = append(peers, peer)
		}
	}
	c.nodes[id] = raft.NewNodeFromRecoveredState(id, peers, raft.RecoveredState{HardState: durable.hard, Log: durable.log, CommitIndex: durable.commit})
	c.alive[id] = true
	c.incarnation[id]++
	c.models[id] = newModelState()
	delete(c.lastState, id)
	c.consumeActions(id, c.nodes[id].Step(raft.RecoverCommitted{}))
	c.enqueueTimer(id, raft.ElectionTimeout{})
	c.faults.Restart = true
	c.record("restart", id, fmt.Sprintf("recover Term %d commit %d", durable.hard.Term, durable.commit))
}

func (c *randomCluster) enqueueRandomWork() {
	if c.step%17 == 0 {
		if leader := c.leader(); leader != "" {
			c.enqueueTimer(leader, raft.HeartbeatTimeout{})
		}
	}
	if c.step%29 == 0 {
		c.enqueueTimer(threeNodeIDs[c.random.Intn(len(threeNodeIDs))], raft.ElectionTimeout{})
	}
}

func (c *randomCluster) leader() raft.NodeID {
	for _, id := range threeNodeIDs {
		if c.alive[id] && c.nodes[id].State().Role == raft.Leader {
			return id
		}
	}
	return ""
}

func (c *randomCluster) enqueueMessage(from, to raft.NodeID, event raft.Event) {
	c.enqueue(queuedEvent{kind: queuedMessage, from: from, to: to, event: event})
}
func (c *randomCluster) enqueueTimer(to raft.NodeID, event raft.Event) {
	filtered := c.queue[:0]
	for _, pending := range c.queue {
		if pending.kind == queuedTimer && pending.to == to && reflect.TypeOf(pending.event) == reflect.TypeOf(event) {
			continue
		}
		filtered = append(filtered, pending)
	}
	c.queue = filtered
	c.enqueue(queuedEvent{kind: queuedTimer, to: to, event: event})
}
func (c *randomCluster) enqueueClient(to raft.NodeID, event raft.Event) {
	c.enqueue(queuedEvent{kind: queuedClient, to: to, event: event})
}
func (c *randomCluster) enqueue(item queuedEvent) {
	c.queue = append(c.queue, item)
}

func (c *randomCluster) healthyEvent(eligible []int) int {
	// Healthy setup drains effects before another timer can preempt them.
	for _, preferred := range []queuedKind{queuedPersistence, queuedMessage, queuedClient, queuedTimer} {
		for _, index := range eligible {
			if c.queue[index].kind == preferred {
				return index
			}
		}
	}
	return eligible[0]
}

func (c *randomCluster) eligible() []int {
	firstPersistence := make(map[raft.NodeID]int)
	for index, item := range c.queue {
		if item.kind == queuedPersistence {
			if _, exists := firstPersistence[item.to]; !exists {
				firstPersistence[item.to] = index
			}
		}
	}

	var result []int
	for index, item := range c.queue {
		if item.availableAt > c.step {
			continue
		}
		if persistence, blocked := firstPersistence[item.to]; blocked && persistence != index {
			// The production runtime finishes each emitted persistence action
			// before delivering another event to that Node. Other Nodes continue,
			// which is the persistence-delay fault this scheduler controls.
			continue
		}
		result = append(result, index)
	}
	return result
}

func (c *randomCluster) first(kind queuedKind) *queuedEvent {
	for index := range c.queue {
		if c.queue[index].kind == kind {
			return &c.queue[index]
		}
	}
	return nil
}
func (c *randomCluster) indices(kind queuedKind) []int {
	var result []int
	for index, item := range c.queue {
		if item.kind == kind {
			result = append(result, index)
		}
	}
	return result
}
func (c *randomCluster) remove(index int) { c.queue = append(c.queue[:index], c.queue[index+1:]...) }

func (c *randomCluster) record(kind string, node raft.NodeID, detail string) {
	c.trace = append(c.trace, TraceEvent{Sequence: len(c.trace) + 1, Kind: kind, Node: string(node), Detail: detail})
}

func (c *randomCluster) result(steps int, failure error) RandomResult {
	result := RandomResult{Seed: c.seed, Steps: steps, Faults: c.faults, Trace: append([]TraceEvent(nil), c.trace...)}
	if failure != nil {
		result.Failure = failure.Error()
	}
	return result
}

func kindName(kind queuedKind) string {
	switch kind {
	case queuedTimer:
		return "timer"
	case queuedMessage:
		return "message"
	case queuedClient:
		return "client"
	default:
		return "event"
	}
}

func cloneEntry(entry raft.LogEntry) raft.LogEntry {
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
