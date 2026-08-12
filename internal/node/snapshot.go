package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

// CreateSnapshot requests a manual Snapshot and waits for its durable
// installation. The apply loop clones state; file I/O proceeds asynchronously.
func (n *Node) CreateSnapshot(ctx context.Context) error {
	result := make(chan error, 1)
	input := raftInput{snapshotResult: result}
	select {
	case n.events <- input:
	case <-n.runtimeDone:
		return errors.New("create Snapshot: Node is stopping")
	case <-ctx.Done():
		return fmt.Errorf("create Snapshot: %w", ctx.Err())
	}
	select {
	case err := <-result:
		return err
	case <-n.runtimeDone:
		return errors.New("create Snapshot: Node stopped before completion")
	case <-ctx.Done():
		return fmt.Errorf("create Snapshot: %w", ctx.Err())
	}
}

func installSnapshot(directory string, state snapshot.State, store *wal.WAL) error {
	if _, err := snapshot.Save(directory, state); err != nil {
		return fmt.Errorf("create Snapshot through index %d: %w", state.IncludedIndex, err)
	}
	if state.IncludedIndex == 0 {
		return nil
	}
	if err := store.Compact(state.IncludedIndex, state.IncludedTerm); err != nil {
		return fmt.Errorf("compact WAL through Snapshot index %d: %w", state.IncludedIndex, err)
	}
	return nil
}

func snapshotIdentity(cfg config.Config) snapshot.Identity {
	memberIDs := make([]string, 0, len(cfg.Members))
	for id := range cfg.Members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)
	return snapshot.Identity{ClusterID: cfg.ClusterID, NodeID: cfg.Node.ID, MemberIDs: memberIDs}
}

func snapshotClusterMatches(got, want snapshot.Identity) bool {
	if got.ClusterID != want.ClusterID || len(got.MemberIDs) != len(want.MemberIDs) {
		return false
	}
	for index := range got.MemberIDs {
		if got.MemberIDs[index] != want.MemberIDs[index] {
			return false
		}
	}
	return true
}

func (m *sessionMachine) snapshot(identity snapshot.Identity, includedIndex, includedTerm uint64) snapshot.State {
	state := snapshot.State{
		Identity:      identity,
		IncludedIndex: includedIndex,
		IncludedTerm:  includedTerm,
		Values:        make(map[string][]byte, len(m.values)),
		Sessions:      make([]snapshot.Session, 0, len(m.sessions)),
	}
	for key, value := range m.values {
		state.Values[key] = append([]byte(nil), value...)
	}
	for id, record := range m.sessions {
		state.Sessions = append(state.Sessions, snapshot.Session{
			ID:                [16]byte(id),
			Closed:            record.state == sessionPermanentlyClosed,
			LastSequence:      record.lastSequence,
			LastResult:        uint8(record.lastMutationResult),
			LastDeleteExisted: record.lastDeleteExisted,
		})
	}
	sort.Slice(state.Sessions, func(i, j int) bool {
		return bytes.Compare(state.Sessions[i].ID[:], state.Sessions[j].ID[:]) < 0
	})
	return state
}

func (m *sessionMachine) restore(state *snapshot.State) error {
	if state == nil {
		return nil
	}
	m.active = 0
	m.sessions = make(map[raft.SessionID]sessionRecord, len(state.Sessions))
	m.values = make(map[string][]byte, len(state.Values))
	for key, value := range state.Values {
		m.values[key] = append([]byte(nil), value...)
	}
	for _, saved := range state.Sessions {
		if saved.LastResult > uint8(sessionOutOfOrderSequence) {
			return fmt.Errorf("stored Snapshot Client Session %x has unknown result %d", saved.ID, saved.LastResult)
		}
		record := sessionRecord{
			state:              sessionActive,
			lastSequence:       saved.LastSequence,
			lastMutationResult: sessionFailure(saved.LastResult),
			lastDeleteExisted:  saved.LastDeleteExisted,
		}
		if saved.Closed {
			record.state = sessionPermanentlyClosed
		} else {
			m.active++
		}
		m.sessions[raft.SessionID(saved.ID)] = record
	}
	if m.active > m.limit {
		return fmt.Errorf("stored Snapshot contains %d active Client Sessions, configured limit is %d", m.active, m.limit)
	}
	return nil
}
