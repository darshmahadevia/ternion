package node

import (
	"fmt"
	"sort"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

// raftRuntime owns the synchronous boundary between deterministic election
// decisions and durable local effects. Its caller owns event serialization.
type raftRuntime struct {
	core              *raft.Node
	wal               *wal.WAL
	observeMutation   mutationObserver
	metrics           *nodeMetrics
	leaderMutations   map[uint64]raft.LogEntry
	recoveredSnapshot *snapshot.State
}

func openRaftRuntime(cfg config.Config, peers []raft.NodeID) (*raftRuntime, error) {
	store, recovered, err := wal.Open(cfg.Node.DataDir, wal.Identity{
		ClusterID: cfg.ClusterID,
		NodeID:    cfg.Node.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("recover Node %q consensus state: %w", cfg.Node.ID, err)
	}
	logEntries := make([]raft.LogEntry, len(recovered.Log))
	for index, entry := range recovered.Log {
		if entry.Type > wal.EntryType(raft.EntryDelete) {
			store.Close()
			return nil, fmt.Errorf("recover Node %q consensus state: unsupported log entry type %d at index %d", cfg.Node.ID, entry.Type, entry.Index)
		}
		logEntries[index] = raft.LogEntry{Index: entry.Index, Term: entry.Term, Type: raft.EntryType(entry.Type), SessionID: raft.SessionID(entry.SessionID), Sequence: entry.Sequence, Key: entry.Key, Value: append([]byte(nil), entry.Value...)}
	}
	identity := snapshotIdentity(cfg)
	logTerms := make([]uint64, len(recovered.Log))
	for index, entry := range recovered.Log {
		logTerms[index] = entry.Term
	}
	recoveredSnapshot, err := snapshot.LoadNewest(cfg.Node.DataDir, snapshot.Compatibility{
		Identity:      identity,
		CommitIndex:   recovered.CommitIndex,
		LogStartIndex: recovered.SnapshotIndex + 1,
		LogTerms:      logTerms,
		SnapshotIndex: recovered.SnapshotIndex,
		SnapshotTerm:  recovered.SnapshotTerm,
	})
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("recover Node %q Snapshot: %w", cfg.Node.ID, err)
	}
	var snapshotIndex, snapshotTerm uint64
	if recoveredSnapshot != nil {
		snapshotIndex = recoveredSnapshot.IncludedIndex
		snapshotTerm = recoveredSnapshot.IncludedTerm
		firstSuffixEntry := 0
		for firstSuffixEntry < len(logEntries) && logEntries[firstSuffixEntry].Index <= snapshotIndex {
			firstSuffixEntry++
		}
		logEntries = logEntries[firstSuffixEntry:]
	}
	core := raft.NewNodeFromRecoveredState(raft.NodeID(cfg.Node.ID), peers, raft.RecoveredState{
		HardState: raft.HardState{
			Term:     recovered.HardState.Term,
			VotedFor: raft.NodeID(recovered.HardState.VotedFor),
		},
		Log:           logEntries,
		CommitIndex:   recovered.CommitIndex,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
	})
	return &raftRuntime{core: core, wal: store, recoveredSnapshot: recoveredSnapshot}, nil
}

// step executes durability actions synchronously. A completion event is not
// delivered to Raft until the corresponding WAL records have been synced.
func (r *raftRuntime) step(event raft.Event) ([]raft.Action, error) {
	pending := r.core.Step(event)
	var emitted []raft.Action
	for len(pending) > 0 {
		action := pending[0]
		pending = pending[1:]
		switch persist := action.(type) {
		case raft.PersistHardState:
			if err := r.wal.SaveHardState(wal.HardState{
				Term:     persist.Term,
				VotedFor: string(persist.VotedFor),
			}); err != nil {
				return nil, fmt.Errorf("persist Raft hard state: %w", err)
			}
			if r.metrics != nil {
				r.metrics.walSyncs.Add(1)
			}
			pending = append(pending, r.core.Step(raft.HardStatePersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		case raft.PersistLogEntries:
			if persist.TruncateFrom != 0 {
				if err := r.wal.TruncateLog(persist.TruncateFrom); err != nil {
					return nil, fmt.Errorf("persist Raft log truncation: %w", err)
				}
			}
			entries := make([]wal.LogEntry, len(persist.Entries))
			for index, entry := range persist.Entries {
				entries[index] = wal.LogEntry{Index: entry.Index, Term: entry.Term, Type: wal.EntryType(entry.Type), SessionID: [16]byte(entry.SessionID), Sequence: entry.Sequence, Key: entry.Key, Value: append([]byte(nil), entry.Value...)}
			}
			if err := r.wal.SaveLogEntries(entries); err != nil {
				return nil, fmt.Errorf("persist Raft log entries: %w", err)
			}
			if r.metrics != nil {
				r.metrics.walSyncs.Add(1)
			}
			if r.observeMutation != nil && r.core.State().Role == raft.Leader {
				if r.leaderMutations == nil {
					r.leaderMutations = make(map[uint64]raft.LogEntry)
				}
				for _, entry := range persist.Entries {
					if entry.Type != raft.EntrySet && entry.Type != raft.EntryDelete {
						continue
					}
					r.leaderMutations[entry.Index] = entry
					r.observeMutation(mutationAfterLocalPersistence, entry)
				}
			}
			pending = append(pending, r.core.Step(raft.LogEntriesPersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		case raft.PersistCommitIndex:
			var committedMutations []raft.LogEntry
			if r.observeMutation != nil {
				committedMutations = r.committedLeaderMutations(persist.CommitIndex)
			}
			for _, entry := range committedMutations {
				r.observeMutation(mutationAfterQuorumPersistence, entry)
			}
			if err := r.wal.SaveCommitIndex(persist.CommitIndex); err != nil {
				return nil, fmt.Errorf("persist Raft commit index: %w", err)
			}
			if r.metrics != nil {
				r.metrics.walSyncs.Add(1)
			}
			for _, entry := range committedMutations {
				r.observeMutation(mutationAfterCommitment, entry)
				delete(r.leaderMutations, entry.Index)
			}
			pending = append(pending, r.core.Step(raft.CommitIndexPersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		default:
			emitted = append(emitted, action)
		}
	}
	return emitted, nil
}

func (r *raftRuntime) committedLeaderMutations(commitIndex uint64) []raft.LogEntry {
	entries := make([]raft.LogEntry, 0, len(r.leaderMutations))
	for index, entry := range r.leaderMutations {
		if index <= commitIndex {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
	return entries
}

func (r *raftRuntime) close() error {
	return r.wal.Close()
}
