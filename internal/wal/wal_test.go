package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestWALRecoversLatestHardStateAcrossSegments(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-2"}
	wal, recovered, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("open new WAL: %v", err)
	}
	if !reflect.DeepEqual(recovered, RecoveredState{Identity: identity}) {
		t.Fatalf("new WAL state = %#v, want identity only", recovered)
	}
	for _, state := range []HardState{
		{Term: 1, VotedFor: "node-1"},
		{Term: 2},
		{Term: 3, VotedFor: "node-3"},
	} {
		if err := wal.SaveHardState(state); err != nil {
			t.Fatalf("save hard state %#v: %v", state, err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	segments, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find segments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("WAL segments = %d, want rollover", len(segments))
	}
	assertSegmentFormat(t, segments[0].path)

	reopened, recovered, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	want := RecoveredState{Identity: identity, HardState: HardState{Term: 3, VotedFor: "node-3"}}
	if !reflect.DeepEqual(recovered, want) {
		t.Fatalf("recovered state = %#v, want %#v", recovered, want)
	}
}

func TestWALSyncsAndValidatesLogEntryOrder(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	wal, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	sessionID := [16]byte{1, 2, 3, 4}
	setValue := []byte{0, 1, 255}
	if err := wal.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1, Type: 0},
		{Index: 2, Term: 1, Type: 1, SessionID: sessionID},
		{Index: 3, Term: 1, Type: 3, SessionID: sessionID, Sequence: 1, Key: "empty-or-opaque", Value: setValue},
	}); err != nil {
		t.Fatalf("save log entry: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	wantLog := []LogEntry{
		{Index: 1, Term: 1, Type: 0},
		{Index: 2, Term: 1, Type: 1, SessionID: sessionID},
		{Index: 3, Term: 1, Type: 3, SessionID: sessionID, Sequence: 1, Key: "empty-or-opaque", Value: []byte{0, 1, 255}},
	}
	if !reflect.DeepEqual(recovered.Log, wantLog) {
		t.Fatalf("recovered log = %#v, want %#v", recovered.Log, wantLog)
	}
	if err := reopened.SaveLogEntries(nil); err == nil || !strings.Contains(err.Error(), "at least one entry") {
		t.Fatalf("SaveLogEntries(nil) error = %v, want non-empty detail", err)
	}
}

func TestWALRecoversCommittedPrefixAcrossTruncationAndReplacement(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}); err != nil {
		t.Fatalf("save initial log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.TruncateLog(2); err == nil || !strings.Contains(err.Error(), "committed index 2") {
		t.Fatalf("truncate committed history error = %v, want rejection", err)
	}
	if err := store.TruncateLog(3); err != nil {
		t.Fatalf("truncate uncommitted suffix: %v", err)
	}
	replacement := LogEntry{Index: 3, Term: 2}
	if err := store.SaveLogEntries([]LogEntry{replacement}); err != nil {
		t.Fatalf("save replacement entry: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.CommitIndex != 2 {
		t.Fatalf("recovered commit index = %d, want 2", recovered.CommitIndex)
	}
	wantLog := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, replacement}
	if !reflect.DeepEqual(recovered.Log, wantLog) {
		t.Fatalf("recovered log = %#v, want %#v", recovered.Log, wantLog)
	}
}

func TestWALCompactionPreservesRecoveryAcrossPartialSegmentDeletion(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := open(directory, identity, 180)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveHardState(HardState{Term: 3, VotedFor: "node-2"}); err != nil {
		t.Fatalf("save hard state: %v", err)
	}
	for index := uint64(1); index <= 7; index++ {
		if err := store.SaveLogEntries([]LogEntry{{Index: index, Term: 3, Key: fmt.Sprintf("key-%d", index), Value: make([]byte, 32)}}); err != nil {
			t.Fatalf("save log entry %d: %v", index, err)
		}
	}
	if err := store.SaveCommitIndex(7); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	before, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find WAL segments before compaction: %v", err)
	}
	backups := make(map[string][]byte, len(before))
	for _, segment := range before {
		backups[filepath.Base(segment.path)] = readFile(t, segment.path)
	}
	if err := store.Compact(5, 3); err != nil {
		t.Fatalf("compact WAL: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted WAL: %v", err)
	}
	after, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find compacted WAL segments: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("WAL segments after compaction = %d, before = %d; want at least one covered segment deleted", len(after), len(before))
	}

	// Restoring one deleted segment models a crash between segment deletions.
	// The synced checkpoint must make both the partial and complete deletion
	// layouts recover to the same retained suffix.
	for name, contents := range backups {
		path := filepath.Join(directory, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err := os.WriteFile(path, contents, 0o600); err != nil {
				t.Fatalf("restore one segment for crash point: %v", err)
			}
			break
		}
	}
	reopened, recovered, err := open(directory, identity, 180)
	if err != nil {
		t.Fatalf("recover partially deleted compacted WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 5 || recovered.SnapshotTerm != 3 || recovered.CommitIndex != 7 || recovered.HardState != (HardState{Term: 3, VotedFor: "node-2"}) {
		t.Fatalf("recovered compacted metadata = %#v, want Snapshot 5/3, commit 7, and hard state", recovered)
	}
	if len(recovered.Log) != 2 || recovered.Log[0].Index != 6 || recovered.Log[1].Index != 7 {
		t.Fatalf("recovered retained suffix = %#v, want indexes 6 and 7", recovered.Log)
	}
}

func TestWALRejectsConfiguredIdentityMismatch(t *testing.T) {
	directory := t.TempDir()
	wal, _, err := Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
	if err != nil {
		t.Fatalf("open new WAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	for _, test := range []struct {
		name     string
		identity Identity
		detail   string
	}{
		{name: "Cluster", identity: Identity{ClusterID: "cluster-2", NodeID: "node-1"}, detail: "configured Cluster \"cluster-2\" Node \"node-1\""},
		{name: "Node", identity: Identity{ClusterID: "cluster-1", NodeID: "node-2"}, detail: "configured Cluster \"cluster-1\" Node \"node-2\""},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := Open(directory, test.identity)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("Open() error = %v, want durable identity mismatch with %q", err, test.detail)
			}
		})
	}
}

func TestWALRejectsUnsupportedVersionAndChecksumMismatch(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		directory := t.TempDir()
		path := createWAL(t, directory)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open segment: %v", err)
		}
		if _, err := file.WriteAt([]byte{0, 0, 0, 2}, int64(len(segmentMagic))); err != nil {
			t.Fatalf("replace version: %v", err)
		}
		file.Close()
		_, _, err = Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
		if err == nil || !strings.Contains(err.Error(), "unsupported format version 2") {
			t.Fatalf("Open() error = %v, want unsupported version", err)
		}
	})

	t.Run("checksum", func(t *testing.T) {
		directory := t.TempDir()
		path := createWAL(t, directory)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open segment: %v", err)
		}
		if _, err := file.WriteAt([]byte{'X'}, segmentHeaderSize+frameHeaderSize+1); err != nil {
			t.Fatalf("corrupt record: %v", err)
		}
		file.Close()
		_, _, err = Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
		for _, detail := range []string{filepath.Base(path), fmt.Sprintf("byte offset %d", segmentHeaderSize), "checksum mismatch"} {
			if err == nil || !strings.Contains(err.Error(), detail) {
				t.Fatalf("Open() error = %v, want interior corruption detail %q", err, detail)
			}
		}
	})
}

func TestWALRecoversInterruptedFinalFrameAtEveryByteBoundary(t *testing.T) {
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	baselineDirectory := t.TempDir()
	baselinePath := createWAL(t, baselineDirectory)
	baseline := readFile(t, baselinePath)

	completeDirectory := t.TempDir()
	store, _, err := Open(completeDirectory, identity)
	if err != nil {
		t.Fatalf("open source WAL: %v", err)
	}
	wantState := HardState{Term: 4, VotedFor: "node-2"}
	if err := store.SaveHardState(wantState); err != nil {
		t.Fatalf("save source hard state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source WAL: %v", err)
	}
	complete := readFile(t, filepath.Join(completeDirectory, filepath.Base(baselinePath)))
	frame := complete[len(baseline):]
	if len(frame) <= int(frameHeaderSize) {
		t.Fatalf("appended frame length = %d, want header and body", len(frame))
	}

	for written := 0; written < len(frame); written++ {
		t.Run(fmt.Sprintf("bytes_%d", written), func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, filepath.Base(baselinePath))
			writeInterruptedSegment(t, path, append(append([]byte(nil), baseline...), frame[:written]...), false)

			reopened, recovered, err := Open(directory, identity)
			if err != nil {
				t.Fatalf("recover after %d frame bytes: %v", written, err)
			}
			if recovered.HardState != (HardState{}) {
				t.Fatalf("recovered hard state = %#v, want no fabricated partial record", recovered.HardState)
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("close recovered WAL: %v", err)
			}
			assertFileSize(t, path, int64(len(baseline)))
		})
	}

	for _, syncComplete := range []bool{false, true} {
		name := "before_sync_completion"
		if syncComplete {
			name = "after_sync_completion"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, filepath.Base(baselinePath))
			writeInterruptedSegment(t, path, complete, syncComplete)

			reopened, recovered, err := Open(directory, identity)
			if err != nil {
				t.Fatalf("recover complete frame: %v", err)
			}
			defer reopened.Close()
			if recovered.HardState != wantState {
				t.Fatalf("recovered hard state = %#v, want %#v", recovered.HardState, wantState)
			}
			assertFileSize(t, path, int64(len(complete)))
		})
	}
}

func TestWALTruncatesChecksumInvalidFinalFrameAndContinues(t *testing.T) {
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	directory := t.TempDir()
	path := createWAL(t, directory)
	baseline := readFile(t, path)

	sourceDirectory := t.TempDir()
	store, _, err := Open(sourceDirectory, identity)
	if err != nil {
		t.Fatalf("open source WAL: %v", err)
	}
	if err := store.SaveHardState(HardState{Term: 1, VotedFor: "node-2"}); err != nil {
		t.Fatalf("save source hard state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source WAL: %v", err)
	}
	contents := readFile(t, filepath.Join(sourceDirectory, filepath.Base(path)))
	contents[len(contents)-1] ^= 0xff
	writeInterruptedSegment(t, path, contents, true)

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("recover checksum-invalid final frame: %v", err)
	}
	if recovered.HardState != (HardState{}) {
		t.Fatalf("recovered hard state = %#v, want invalid tail discarded", recovered.HardState)
	}
	assertFileSize(t, path, int64(len(baseline)))
	want := HardState{Term: 2, VotedFor: "node-3"}
	if err := reopened.SaveHardState(want); err != nil {
		t.Fatalf("append after tail recovery: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close repaired WAL: %v", err)
	}

	reopened, recovered, err = Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen repaired WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.HardState != want {
		t.Fatalf("recovered replacement hard state = %#v, want %#v", recovered.HardState, want)
	}
}

func TestWALRejectsIncompleteFrameOutsideFinalSegment(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	for term := uint64(1); term <= 4; term++ {
		if err := store.SaveHardState(HardState{Term: term, VotedFor: "node-2"}); err != nil {
			t.Fatalf("save hard state for Term %d: %v", term, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	segments, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find WAL segments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("WAL segments = %d, want at least two", len(segments))
	}
	contents := readFile(t, segments[0].path)
	frameOffset := lastFrameOffset(t, contents)
	if err := os.Truncate(segments[0].path, int64(len(contents)-1)); err != nil {
		t.Fatalf("truncate non-final segment: %v", err)
	}

	_, _, err = open(directory, identity, 64)
	for _, detail := range []string{filepath.Base(segments[0].path), fmt.Sprintf("byte offset %d", frameOffset), "body"} {
		if err == nil || !strings.Contains(err.Error(), detail) {
			t.Fatalf("open WAL error = %v, want non-tail corruption detail %q", err, detail)
		}
	}
}

func createWAL(t *testing.T, directory string) string {
	t.Helper()
	wal, _, err := Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	return filepath.Join(directory, "wal-0000000000000001.qwal")
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	return contents
}

func writeInterruptedSegment(t *testing.T, path string, contents []byte, syncComplete bool) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create interrupted segment: %v", err)
	}
	if _, err := file.Write(contents); err != nil {
		file.Close()
		t.Fatalf("write interrupted segment: %v", err)
	}
	if syncComplete {
		if err := file.Sync(); err != nil {
			file.Close()
			t.Fatalf("sync interrupted segment: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close interrupted segment: %v", err)
	}
}

func assertFileSize(t *testing.T, path string, want int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	if info.Size() != want {
		t.Fatalf("%q size = %d, want %d", path, info.Size(), want)
	}
}

func lastFrameOffset(t *testing.T, contents []byte) int64 {
	t.Helper()
	offset := segmentHeaderSize
	for offset < int64(len(contents)) {
		if offset+frameHeaderSize > int64(len(contents)) {
			t.Fatalf("test segment has incomplete frame header at %d", offset)
		}
		length := int64(binary.BigEndian.Uint32(contents[offset : offset+4]))
		next := offset + frameHeaderSize + length
		if next > int64(len(contents)) {
			t.Fatalf("test segment has incomplete frame body at %d", offset)
		}
		if next == int64(len(contents)) {
			return offset
		}
		offset = next
	}
	t.Fatal("test segment has no frames")
	return 0
}

func assertSegmentFormat(t *testing.T, path string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if string(contents[:len(segmentMagic)]) != segmentMagic {
		t.Fatalf("segment magic = %q, want %q", contents[:len(segmentMagic)], segmentMagic)
	}
	if got := binary.BigEndian.Uint32(contents[len(segmentMagic):segmentHeaderSize]); got != formatVersion {
		t.Fatalf("format version = %d, want %d", got, formatVersion)
	}
	length := binary.BigEndian.Uint32(contents[segmentHeaderSize : segmentHeaderSize+4])
	wantChecksum := binary.BigEndian.Uint32(contents[segmentHeaderSize+4 : segmentHeaderSize+8])
	body := contents[segmentHeaderSize+frameHeaderSize : segmentHeaderSize+frameHeaderSize+int64(length)]
	if got := crc32.ChecksumIEEE(body); got != wantChecksum {
		t.Fatalf("frame checksum = %08x, want %08x", got, wantChecksum)
	}
}
