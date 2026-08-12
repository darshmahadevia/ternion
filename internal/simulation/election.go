// Package simulation executes deterministic Raft events without real I/O or clocks.
package simulation

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

var threeNodeIDs = []raft.NodeID{"node-1", "node-2", "node-3"}

// Timing configures the deterministic runtime's Raft timers.
type Timing struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	CheckQuorumWindow  time.Duration
}

// DefaultTiming matches the v1 local timing decisions.
func DefaultTiming() Timing {
	return Timing{
		HeartbeatInterval:  100 * time.Millisecond,
		ElectionTimeoutMin: 500 * time.Millisecond,
		ElectionTimeoutMax: time.Second,
		CheckQuorumWindow:  time.Second,
	}
}

// Validate preserves the ordering that lets heartbeats arrive well before an
// election and prevents check-quorum from being more aggressive than election.
func (t Timing) Validate() error {
	var problems []error
	if t.HeartbeatInterval <= 0 {
		problems = append(problems, errors.New("heartbeat interval must be positive"))
	}
	if t.ElectionTimeoutMin <= t.HeartbeatInterval {
		problems = append(problems, errors.New("minimum election timeout must exceed heartbeat interval"))
	}
	if t.ElectionTimeoutMax < t.ElectionTimeoutMin {
		problems = append(problems, errors.New("maximum election timeout must not be less than minimum"))
	}
	if t.CheckQuorumWindow < t.ElectionTimeoutMin {
		problems = append(problems, errors.New("check-quorum window must not be less than minimum election timeout"))
	}
	return errors.Join(problems...)
}

// ElectionClock supplies randomized election durations without exposing a
// clock or random source to the Raft core.
type ElectionClock interface {
	ElectionTimeout(minimum, maximum time.Duration) time.Duration
}

// SeededClock returns replayable pseudo-random election timeouts.
type SeededClock struct{ random *rand.Rand }

// NewSeededClock creates a deterministic clock for a simulation seed.
func NewSeededClock(seed int64) *SeededClock {
	return &SeededClock{random: rand.New(rand.NewSource(seed))} // #nosec G404 -- reproducibility, not security.
}

// ElectionTimeout chooses an inclusive duration within the configured range.
func (c *SeededClock) ElectionTimeout(minimum, maximum time.Duration) time.Duration {
	span := maximum - minimum
	if span == 0 {
		return minimum
	}
	return minimum + time.Duration(c.random.Int63n(int64(span)+1))
}

// Step records one event and the actions it produced.
type Step struct {
	Node    raft.NodeID
	Event   raft.Event
	Actions []raft.Action
}

// ScheduledElection records the latest injected election timeout for a Node.
type ScheduledElection struct {
	Node  raft.NodeID
	After time.Duration
}

// Cluster is a partitionable deterministic three-Node runtime. It owns event
// delivery and timer choices while each Raft Node owns consensus decisions.
type Cluster struct {
	nodes             map[raft.NodeID]*raft.Node
	connected         map[raft.NodeID]map[raft.NodeID]bool
	clock             ElectionClock
	timing            Timing
	electionTimeouts  map[raft.NodeID]time.Duration
	heartbeatTimeouts map[raft.NodeID]time.Duration
	quorumTimeouts    map[raft.NodeID]time.Duration
	applied           map[raft.NodeID][]raft.LogEntry
	scheduledElection []ScheduledElection
	trace             []Step
}

type scheduledEvent struct {
	node  raft.NodeID
	event raft.Event
}

// NewCluster creates a connected three-Node Cluster with injected timing.
func NewCluster(timing Timing, clock ElectionClock) (*Cluster, error) {
	if err := timing.Validate(); err != nil {
		return nil, fmt.Errorf("validate simulation timing: %w", err)
	}
	if clock == nil {
		return nil, errors.New("create simulation Cluster: election clock is required")
	}

	cluster := &Cluster{
		nodes:             make(map[raft.NodeID]*raft.Node, len(threeNodeIDs)),
		connected:         make(map[raft.NodeID]map[raft.NodeID]bool, len(threeNodeIDs)),
		clock:             clock,
		timing:            timing,
		electionTimeouts:  make(map[raft.NodeID]time.Duration, len(threeNodeIDs)),
		heartbeatTimeouts: make(map[raft.NodeID]time.Duration, len(threeNodeIDs)),
		quorumTimeouts:    make(map[raft.NodeID]time.Duration, len(threeNodeIDs)),
		applied:           make(map[raft.NodeID][]raft.LogEntry, len(threeNodeIDs)),
	}
	for _, id := range threeNodeIDs {
		var peers []raft.NodeID
		cluster.connected[id] = make(map[raft.NodeID]bool, len(threeNodeIDs)-1)
		for _, peer := range threeNodeIDs {
			if peer != id {
				peers = append(peers, peer)
				cluster.connected[id][peer] = true
			}
		}
		cluster.nodes[id] = raft.NewNode(id, peers)
		cluster.resetElectionTimer(id)
	}
	return cluster, nil
}

// FireNextElectionTimeout fires the shortest currently scheduled timeout.
func (c *Cluster) FireNextElectionTimeout() error {
	var selected raft.NodeID
	var shortest time.Duration
	for _, id := range threeNodeIDs {
		delay := c.electionTimeouts[id]
		if selected == "" || delay < shortest || (delay == shortest && id < selected) {
			selected, shortest = id, delay
		}
	}
	return c.FireElectionTimeout(selected)
}

// FireElectionTimeout reports a controlled election timeout for one Node.
func (c *Cluster) FireElectionTimeout(id raft.NodeID) error {
	return c.process(scheduledEvent{node: id, event: raft.ElectionTimeout{}})
}

// FireHeartbeatTimeout asks one Node to run a heartbeat round.
func (c *Cluster) FireHeartbeatTimeout(id raft.NodeID) error {
	return c.process(scheduledEvent{node: id, event: raft.HeartbeatTimeout{}})
}

// FireCheckQuorumTimeout ends one Node's current quorum-contact window.
func (c *Cluster) FireCheckQuorumTimeout(id raft.NodeID) error {
	return c.process(scheduledEvent{node: id, event: raft.CheckQuorumTimeout{}})
}

// ProposeSession injects one deterministic Client Session command.
func (c *Cluster) ProposeSession(id raft.NodeID, proposal raft.ProposeSession) error {
	return c.process(scheduledEvent{node: id, event: proposal})
}

// ProposeSet injects one deterministic SET command.
func (c *Cluster) ProposeSet(id raft.NodeID, proposal raft.ProposeSet) error {
	return c.process(scheduledEvent{node: id, event: proposal})
}

// Partition permits delivery only between Nodes in the same supplied group.
func (c *Cluster) Partition(groups ...[]raft.NodeID) {
	for from := range c.connected {
		for to := range c.connected[from] {
			c.connected[from][to] = false
		}
	}
	for _, group := range groups {
		for _, from := range group {
			for _, to := range group {
				if from != to {
					c.connected[from][to] = true
				}
			}
		}
	}
}

// Heal restores bidirectional delivery between every Node.
func (c *Cluster) Heal() { c.Partition(threeNodeIDs) }

// State returns one Node's current state.
func (c *Cluster) State(id raft.NodeID) raft.State { return c.nodes[id].State() }

// Trace returns a copy of the replayable event/action history.
func (c *Cluster) Trace() []Step { return append([]Step(nil), c.trace...) }

// ScheduledElections returns every randomized timer choice in order.
func (c *Cluster) ScheduledElections() []ScheduledElection {
	return append([]ScheduledElection(nil), c.scheduledElection...)
}

// HeartbeatTimeout returns the configured delay most recently scheduled for a Leader.
func (c *Cluster) HeartbeatTimeout(id raft.NodeID) time.Duration {
	return c.heartbeatTimeouts[id]
}

// CheckQuorumTimeout returns the configured authority-loss window for a Leader.
func (c *Cluster) CheckQuorumTimeout(id raft.NodeID) time.Duration {
	return c.quorumTimeouts[id]
}

// AppliedEntries returns the entries the runtime applied for one Node.
func (c *Cluster) AppliedEntries(id raft.NodeID) []raft.LogEntry {
	return append([]raft.LogEntry(nil), c.applied[id]...)
}

func (c *Cluster) process(initial scheduledEvent) error {
	queue := []scheduledEvent{initial}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		node, ok := c.nodes[next.node]
		if !ok {
			return fmt.Errorf("deliver event to unknown Node %q", next.node)
		}
		actions := node.Step(next.event)
		c.trace = append(c.trace, Step{Node: next.node, Event: next.event, Actions: actions})
		for _, action := range actions {
			switch action := action.(type) {
			case raft.PersistHardState:
				queue = append(queue, scheduledEvent{node: next.node, event: raft.HardStatePersisted{PersistenceID: action.PersistenceID}})
			case raft.PersistLogEntries:
				queue = append(queue, scheduledEvent{node: next.node, event: raft.LogEntriesPersisted{PersistenceID: action.PersistenceID}})
			case raft.PersistCommitIndex:
				queue = append(queue, scheduledEvent{node: next.node, event: raft.CommitIndexPersisted{PersistenceID: action.PersistenceID}})
			case raft.SendPreVoteRequest:
				c.deliver(&queue, next.node, action.To, action.Request)
			case raft.SendPreVoteResponse:
				c.deliver(&queue, next.node, action.To, action.Response)
			case raft.SendVoteRequest:
				c.deliver(&queue, next.node, action.To, action.Request)
			case raft.SendVoteResponse:
				c.deliver(&queue, next.node, action.To, action.Response)
			case raft.SendAppendEntries:
				if !c.deliver(&queue, next.node, action.To, action.Request) {
					queue = append(queue, scheduledEvent{node: next.node, event: raft.AppendEntriesFailed{To: action.To, RequestID: action.Request.RequestID}})
				}
			case raft.SendAppendEntriesResponse:
				c.deliver(&queue, next.node, action.To, action.Response)
			case raft.ResetElectionTimer:
				c.resetElectionTimer(next.node)
			case raft.ResetHeartbeatTimer:
				c.heartbeatTimeouts[next.node] = c.timing.HeartbeatInterval
			case raft.ResetCheckQuorumTimer:
				c.quorumTimeouts[next.node] = c.timing.CheckQuorumWindow
			case raft.ApplyEntry:
				applied := c.applied[next.node]
				wantIndex := uint64(len(applied) + 1)
				if action.Entry.Index != wantIndex {
					return fmt.Errorf("node %q applied index %d after %d entries", next.node, action.Entry.Index, len(applied))
				}
				c.applied[next.node] = append(applied, action.Entry)
			case raft.BecameLeader, raft.LostLeadership, raft.BecameReadReady,
				raft.ProposalAccepted, raft.ProposalRejected:
				// The harness fires these controlled timers explicitly; role changes
				// are already visible through Node.State.
			}
		}
		if err := assertRaftInvariants(c.nodes, c.applied); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) deliver(queue *[]scheduledEvent, from, to raft.NodeID, event raft.Event) bool {
	if c.connected[from][to] {
		*queue = append(*queue, scheduledEvent{node: to, event: event})
		return true
	}
	return false
}

func (c *Cluster) resetElectionTimer(id raft.NodeID) {
	delay := c.clock.ElectionTimeout(c.timing.ElectionTimeoutMin, c.timing.ElectionTimeoutMax)
	c.electionTimeouts[id] = delay
	c.scheduledElection = append(c.scheduledElection, ScheduledElection{Node: id, After: delay})
}

// ElectionResult contains the elected Leader and replayable trace.
type ElectionResult struct {
	Leader raft.NodeID
	Term   uint64
	Trace  []Step
}

// RunElection executes the first controlled election for a seed.
func RunElection(seed int64) (ElectionResult, error) {
	cluster, err := NewCluster(DefaultTiming(), NewSeededClock(seed))
	if err != nil {
		return ElectionResult{}, err
	}
	if err := cluster.FireNextElectionTimeout(); err != nil {
		return ElectionResult{Trace: cluster.Trace()}, err
	}
	leaders := leadersByTerm(cluster.nodes)
	if len(leaders) != 1 {
		return ElectionResult{Trace: cluster.Trace()}, fmt.Errorf("election finished with %d Leader Terms, want 1", len(leaders))
	}
	for term, ids := range leaders {
		return ElectionResult{Leader: ids[0], Term: term, Trace: cluster.Trace()}, nil
	}
	panic("unreachable")
}

func assertRaftInvariants(nodes map[raft.NodeID]*raft.Node, applied map[raft.NodeID][]raft.LogEntry) error {
	for term, leaders := range leadersByTerm(nodes) {
		if len(leaders) > 1 {
			return fmt.Errorf("term %d has multiple Leaders: %v", term, leaders)
		}
	}
	for id, node := range nodes {
		state := node.State()
		if state.LastApplied > state.CommitIndex {
			return fmt.Errorf("node %q applied through %d beyond commit index %d", id, state.LastApplied, state.CommitIndex)
		}
		if uint64(len(applied[id])) != state.LastApplied {
			return fmt.Errorf("node %q recorded %d applied entries, state reports %d", id, len(applied[id]), state.LastApplied)
		}
	}
	return nil
}

func leadersByTerm(nodes map[raft.NodeID]*raft.Node) map[uint64][]raft.NodeID {
	leaders := make(map[uint64][]raft.NodeID)
	for _, node := range nodes {
		state := node.State()
		if state.Role == raft.Leader {
			leaders[state.Term] = append(leaders[state.Term], state.ID)
		}
	}
	for _, termLeaders := range leaders {
		sort.Slice(termLeaders, func(i, j int) bool { return termLeaders[i] < termLeaders[j] })
	}
	return leaders
}
