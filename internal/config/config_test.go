package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/config"
)

func TestLoadValidConfig(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "node.yaml")
	contents := `version: 1
cluster_id: test-cluster
active_session_limit: 64
node:
  id: node-1
  data_dir: data/node-1
members:
  node-1:
    peer_address: 127.0.0.1:7301
    client_address: 127.0.0.1:7401
  node-2:
    peer_address: 127.0.0.1:7302
    client_address: 127.0.0.1:7402
  node-3:
    peer_address: 127.0.0.1:7303
    client_address: 127.0.0.1:7403
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ClusterID != "test-cluster" || cfg.Node.ID != "node-1" {
		t.Fatalf("Load() identity = %q/%q, want test-cluster/node-1", cfg.ClusterID, cfg.Node.ID)
	}
	if got := cfg.EffectiveSnapshotThresholdBytes(); got != config.DefaultSnapshotThresholdBytes {
		t.Fatalf("default Snapshot threshold = %d, want %d", got, config.DefaultSnapshotThresholdBytes)
	}
	wantDataDir := filepath.Join(directory, "data", "node-1")
	if cfg.Node.DataDir != wantDataDir {
		t.Fatalf("Load() data dir = %q, want %q", cfg.Node.DataDir, wantDataDir)
	}
}

func TestLoadRejectsUnknownAndInvalidSettings(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "node.yaml")
	contents := `version: 2
cluster_id: ""
unexpected: true
node:
  id: ""
  peer_address: 127.0.0.1:7300
  client_address: 127.0.0.1:7300
  data_dir: ""
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := config.Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want invalid configuration error")
	}
	if !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("Load() error = %q, want unknown-field detail", err)
	}
}

func TestValidateReportsAllInvalidSettings(t *testing.T) {
	t.Parallel()

	err := (config.Config{}).Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want validation errors")
	}
	for _, detail := range []string{"version", "cluster_id", "active_session_limit", "node.id", "node.data_dir", "exactly three Nodes"} {
		if !strings.Contains(err.Error(), detail) {
			t.Errorf("Validate() error = %q, want %q", err, detail)
		}
	}
}

func TestValidateRejectsNegativeSnapshotThreshold(t *testing.T) {
	cfg := configForTest()
	cfg.SnapshotThresholdBytes = -1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "snapshot_threshold_bytes") {
		t.Fatalf("Validate() error = %v, want Snapshot threshold diagnostic", err)
	}
}

func TestValidateRejectsDuplicateMemberAddresses(t *testing.T) {
	cfg := configForTest()
	cfg.Members["node-2"] = config.Member{
		PeerAddress:   cfg.Members["node-1"].PeerAddress,
		ClientAddress: "127.0.0.1:7402",
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("Validate() error = %v, want duplicate address detail", err)
	}
}

func configForTest() config.Config {
	return config.Config{
		Version:            1,
		ClusterID:          "cluster-1",
		ActiveSessionLimit: 64,
		Node:               config.Node{ID: "node-1", DataDir: "data/node-1"},
		Members: map[string]config.Member{
			"node-1": {PeerAddress: "127.0.0.1:7301", ClientAddress: "127.0.0.1:7401"},
			"node-2": {PeerAddress: "127.0.0.1:7302", ClientAddress: "127.0.0.1:7402"},
			"node-3": {PeerAddress: "127.0.0.1:7303", ClientAddress: "127.0.0.1:7403"},
		},
	}
}
