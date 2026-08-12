package snapshot_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/snapshot"
)

func TestSaveAndLoadNewestCompatibleSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	first := snapshot.State{
		Identity: identity, IncludedIndex: 2, IncludedTerm: 1,
		Values:   map[string][]byte{"key": {0, 1, 2}},
		Sessions: []snapshot.Session{{ID: [16]byte{1}, LastSequence: 2, LastDeleteExisted: true}},
	}
	firstName, err := snapshot.Save(directory, first)
	if err != nil {
		t.Fatalf("save first Snapshot: %v", err)
	}
	second := first
	second.IncludedIndex = 4
	second.IncludedTerm = 2
	second.Values = map[string][]byte{"newer": {3}}
	secondName, err := snapshot.Save(directory, second)
	if err != nil {
		t.Fatalf("save second Snapshot: %v", err)
	}
	if firstName == secondName || !strings.HasSuffix(firstName, ".qsnap") || !strings.HasSuffix(secondName, ".qsnap") {
		t.Fatalf("Snapshot names = %q and %q, want unique immutable final names", firstName, secondName)
	}
	if err := os.WriteFile(filepath.Join(directory, "snapshot-999.tmp"), []byte("incomplete"), 0o600); err != nil {
		t.Fatalf("write incomplete temporary Snapshot: %v", err)
	}

	loaded, err := snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 3, LogTerms: []uint64{1, 1, 1, 2},
	})
	if err != nil {
		t.Fatalf("load newest compatible Snapshot: %v", err)
	}
	if loaded == nil || loaded.IncludedIndex != 2 || string(loaded.Values["key"]) != string([]byte{0, 1, 2}) {
		t.Fatalf("loaded Snapshot = %#v, want first compatible state", loaded)
	}

	loaded, err = snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 4, LogTerms: []uint64{1, 1, 1, 2},
	})
	if err != nil {
		t.Fatalf("load newest Snapshot: %v", err)
	}
	if loaded == nil || loaded.IncludedIndex != 4 || string(loaded.Values["newer"]) != string([]byte{3}) {
		t.Fatalf("loaded Snapshot = %#v, want newest state", loaded)
	}
}

func TestLoadRejectsSnapshotsWithoutCompatibleWALHistory(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	if _, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 2, IncludedTerm: 1, Values: map[string][]byte{},
	}); err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	_, err := snapshot.LoadNewest(directory, snapshot.Compatibility{Identity: identity, CommitIndex: 1, LogTerms: []uint64{1}})
	if err == nil || !strings.Contains(err.Error(), "compatible with the durable WAL") {
		t.Fatalf("load incompatible Snapshot error = %v, want WAL compatibility diagnostic", err)
	}
}

func TestLoadRejectsCorruptFinalSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{Identity: identity, Values: map[string][]byte{}})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read Snapshot: %v", err)
	}
	contents[len(contents)-1] ^= 0xff
	if err := os.WriteFile(name, contents, 0o600); err != nil {
		t.Fatalf("corrupt Snapshot: %v", err)
	}
	_, err = snapshot.LoadNewest(directory, snapshot.Compatibility{Identity: identity})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("load corrupt Snapshot error = %v, want checksum mismatch", err)
	}
}
