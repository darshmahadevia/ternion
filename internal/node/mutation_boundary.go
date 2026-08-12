package node

import "github.com/Het-Jethva/quorumkv/internal/raft"

// mutationBoundary identifies the durability transitions where a process loss
// changes what a caller can know about a mutation's outcome. The observer is
// private and nil in production; deterministic process tests use it to stop a
// Node at an exact boundary.
type mutationBoundary uint8

const (
	mutationBeforeAppend mutationBoundary = iota
	mutationAfterLocalPersistence
	mutationAfterQuorumPersistence
	mutationAfterCommitment
	mutationAfterApplication
)

type mutationObserver func(mutationBoundary, raft.LogEntry)

func entryForMutation(event raft.Event) (raft.LogEntry, bool) {
	switch event := event.(type) {
	case raft.ProposeSet:
		return raft.LogEntry{
			Type:      raft.EntrySet,
			SessionID: event.SessionID,
			Sequence:  event.Sequence,
			Key:       event.Key,
			Value:     append([]byte(nil), event.Value...),
		}, true
	case raft.ProposeDelete:
		return raft.LogEntry{
			Type:      raft.EntryDelete,
			SessionID: event.SessionID,
			Sequence:  event.Sequence,
			Key:       event.Key,
		}, true
	default:
		return raft.LogEntry{}, false
	}
}
