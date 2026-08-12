// Package snapshot stores immutable point-in-time images of replicated state.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	magic          = "QKVSNP01"
	formatVersion  = uint32(1)
	headerSize     = len(magic) + 4 + 8 + 4
	maxPayloadSize = uint64(4 << 30)
)

// Identity binds a Snapshot to one Node and the fixed membership of its Cluster.
type Identity struct {
	ClusterID string   `json:"cluster_id"`
	NodeID    string   `json:"node_id"`
	MemberIDs []string `json:"member_ids"`
}

// Session is the replicated state retained for one Client Session.
type Session struct {
	ID                [16]byte `json:"id"`
	Closed            bool     `json:"closed"`
	LastSequence      uint64   `json:"last_sequence"`
	LastResult        uint8    `json:"last_result"`
	LastDeleteExisted bool     `json:"last_delete_existed"`
}

// State is the complete replicated state through IncludedIndex.
type State struct {
	Identity      Identity          `json:"identity"`
	IncludedIndex uint64            `json:"included_index"`
	IncludedTerm  uint64            `json:"included_term"`
	Values        map[string][]byte `json:"values"`
	Sessions      []Session         `json:"sessions"`
}

// Compatibility describes the durable WAL history a Snapshot must match.
type Compatibility struct {
	Identity      Identity
	CommitIndex   uint64
	LogStartIndex uint64
	LogTerms      []uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Save writes and syncs a temporary file before atomically installing a
// uniquely named immutable Snapshot.
func Save(directory string, state State) (string, error) {
	if err := validateState(state); err != nil {
		return "", fmt.Errorf("save Snapshot: %w", err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("encode Snapshot: %w", err)
	}
	if uint64(len(payload)) > maxPayloadSize {
		return "", fmt.Errorf("save Snapshot: payload size %d exceeds limit %d", len(payload), maxPayloadSize)
	}

	header := make([]byte, headerSize)
	copy(header, magic)
	binary.BigEndian.PutUint32(header[len(magic):], formatVersion)
	binary.BigEndian.PutUint64(header[len(magic)+4:], uint64(len(payload)))
	binary.BigEndian.PutUint32(header[len(magic)+12:], crc32.ChecksumIEEE(payload))

	if err := os.MkdirAll(directory, 0o750); err != nil {
		return "", fmt.Errorf("create Snapshot directory %q: %w", directory, err)
	}
	temporary, err := os.CreateTemp(directory, fmt.Sprintf("snapshot-%020d-%020d-*.tmp", state.IncludedIndex, state.IncludedTerm))
	if err != nil {
		return "", fmt.Errorf("create temporary Snapshot: %w", err)
	}
	temporaryName := temporary.Name()
	installed := false
	defer func() {
		if !installed {
			_ = temporary.Close()
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", fmt.Errorf("set temporary Snapshot permissions: %w", err)
	}
	if _, err := temporary.Write(header); err != nil {
		return "", fmt.Errorf("write Snapshot header: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return "", fmt.Errorf("write Snapshot payload: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("sync temporary Snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close temporary Snapshot: %w", err)
	}

	finalName := strings.TrimSuffix(temporaryName, ".tmp") + ".qsnap"
	if _, err := os.Lstat(finalName); err == nil {
		return "", fmt.Errorf("install Snapshot %q: immutable destination already exists", finalName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect Snapshot destination %q: %w", finalName, err)
	}
	if err := os.Rename(temporaryName, finalName); err != nil {
		return "", fmt.Errorf("install Snapshot %q: %w", finalName, err)
	}
	installed = true
	if err := syncDirectory(directory); err != nil {
		return "", fmt.Errorf("sync Snapshot directory: %w", err)
	}
	return finalName, nil
}

// LoadNewest validates final Snapshot candidates and returns the newest one
// whose included position is present in the durable committed WAL.
func LoadNewest(directory string, compatibility Compatibility) (*State, error) {
	matches, err := filepath.Glob(filepath.Join(directory, "snapshot-*.qsnap"))
	if err != nil {
		return nil, fmt.Errorf("find Snapshots: %w", err)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	var newest *State
	var newestName string
	for _, name := range matches {
		state, err := read(name)
		if err != nil {
			return nil, err
		}
		if !equalIdentity(state.Identity, compatibility.Identity) {
			return nil, fmt.Errorf("stored Snapshot %q identity does not match configured Cluster membership", name)
		}
		if !matchesWAL(*state, compatibility) {
			continue
		}
		if newest == nil || state.IncludedIndex > newest.IncludedIndex ||
			(state.IncludedIndex == newest.IncludedIndex && (state.IncludedTerm > newest.IncludedTerm ||
				(state.IncludedTerm == newest.IncludedTerm && name > newestName))) {
			newest, newestName = state, name
		}
	}
	if len(matches) > 0 && newest == nil {
		return nil, errors.New("no stored Snapshot is compatible with the durable WAL")
	}
	return newest, nil
}

// Encoded returns the newest immutable Snapshot file at the requested
// position. The returned bytes include the versioned, checksummed file header.
func Encoded(directory string, includedIndex, includedTerm uint64) ([]byte, error) {
	pattern := filepath.Join(directory, fmt.Sprintf("snapshot-%020d-%020d-*.qsnap", includedIndex, includedTerm))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("find snapshot at %d/%d: %w", includedIndex, includedTerm, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("snapshot at %d/%d is not installed", includedIndex, includedTerm)
	}
	sort.Strings(matches)
	name := matches[len(matches)-1]
	contents, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read snapshot %q: %w", name, err)
	}
	if _, err := decode(name, contents); err != nil {
		return nil, err
	}
	return contents, nil
}

// Decode validates an encoded Snapshot file before returning its state.
func Decode(contents []byte) (*State, error) {
	return decode("received Snapshot", contents)
}

func read(name string) (*State, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("stat Snapshot %q: %w", name, err)
	}
	if info.Size() > int64(maxPayloadSize)+int64(headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q size %d exceeds limit %d", name, info.Size(), maxPayloadSize+uint64(headerSize))
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read Snapshot %q: %w", name, err)
	}
	return decode(name, contents)
}

func decode(name string, contents []byte) (*State, error) {
	if uint64(len(contents)) > maxPayloadSize+uint64(headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q size %d exceeds limit %d", name, len(contents), maxPayloadSize+uint64(headerSize))
	}
	if len(contents) < headerSize {
		return nil, fmt.Errorf("stored Snapshot %q is shorter than its header", name)
	}
	if string(contents[:len(magic)]) != magic {
		return nil, fmt.Errorf("stored Snapshot %q has invalid magic", name)
	}
	version := binary.BigEndian.Uint32(contents[len(magic):])
	if version != formatVersion {
		return nil, fmt.Errorf("stored Snapshot %q has unsupported format version %d", name, version)
	}
	length := binary.BigEndian.Uint64(contents[len(magic)+4:])
	if length > maxPayloadSize {
		return nil, fmt.Errorf("stored Snapshot %q payload length %d exceeds limit %d", name, length, maxPayloadSize)
	}
	if length != uint64(len(contents)-headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q length is %d, header records %d", name, len(contents)-headerSize, length)
	}
	payload := contents[headerSize:]
	wantChecksum := binary.BigEndian.Uint32(contents[len(magic)+12:])
	if got := crc32.ChecksumIEEE(payload); got != wantChecksum {
		return nil, fmt.Errorf("stored Snapshot %q checksum mismatch: got %08x, want %08x", name, got, wantChecksum)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var state State
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode Snapshot %q: %w", name, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode Snapshot %q: trailing payload data", name)
	}
	if err := validateState(state); err != nil {
		return nil, fmt.Errorf("validate Snapshot %q: %w", name, err)
	}
	return &state, nil
}

func validateState(state State) error {
	if strings.TrimSpace(state.Identity.ClusterID) == "" || strings.TrimSpace(state.Identity.NodeID) == "" {
		return errors.New("missing Snapshot Cluster Identity or Node Identity")
	}
	if len(state.Identity.MemberIDs) != 3 {
		return fmt.Errorf("member identities contain %d Nodes, want 3", len(state.Identity.MemberIDs))
	}
	nodeIsMember := false
	for index, id := range state.Identity.MemberIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("member identity must not be empty")
		}
		if index > 0 && state.Identity.MemberIDs[index-1] >= id {
			return errors.New("member identities must be unique and sorted")
		}
		if id == state.Identity.NodeID {
			nodeIsMember = true
		}
	}
	if !nodeIsMember {
		return errors.New("local Node Identity is absent from member identities")
	}
	if (state.IncludedIndex == 0) != (state.IncludedTerm == 0) {
		return errors.New("included index and Term must both be zero or both be non-zero")
	}
	seen := make(map[[16]byte]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		if _, exists := seen[session.ID]; exists {
			return fmt.Errorf("duplicate Client Session identity %x", session.ID)
		}
		seen[session.ID] = struct{}{}
	}
	return nil
}

func matchesWAL(state State, compatibility Compatibility) bool {
	if state.IncludedIndex > compatibility.CommitIndex {
		return false
	}
	if state.IncludedIndex == 0 {
		return state.IncludedTerm == 0
	}
	if state.IncludedIndex == compatibility.SnapshotIndex {
		return state.IncludedTerm == compatibility.SnapshotTerm
	}
	start := compatibility.LogStartIndex
	if start == 0 {
		start = 1
	}
	if state.IncludedIndex < start {
		return false
	}
	offset := state.IncludedIndex - start
	if offset >= uint64(len(compatibility.LogTerms)) {
		return false
	}
	return compatibility.LogTerms[offset] == state.IncludedTerm
}

func equalIdentity(left, right Identity) bool {
	if left.ClusterID != right.ClusterID || left.NodeID != right.NodeID || len(left.MemberIDs) != len(right.MemberIDs) {
		return false
	}
	for index := range left.MemberIDs {
		if left.MemberIDs[index] != right.MemberIDs[index] {
			return false
		}
	}
	return true
}

func syncDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
