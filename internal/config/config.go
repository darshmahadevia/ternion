package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config contains the static identity and local settings for one node.
const DefaultSnapshotThresholdBytes = int64(64 << 20)

type Config struct {
	Version                int               `yaml:"version"`
	ClusterID              string            `yaml:"cluster_id"`
	ActiveSessionLimit     int               `yaml:"active_session_limit"`
	SnapshotThresholdBytes int64             `yaml:"snapshot_threshold_bytes,omitempty"`
	Node                   Node              `yaml:"node"`
	Members                map[string]Member `yaml:"members"`
}

// Node contains settings owned by this process.
type Node struct {
	ID             string `yaml:"id"`
	DataDir        string `yaml:"data_dir"`
	MetricsAddress string `yaml:"metrics_address,omitempty"`
}

// Member contains the runtime addresses for one configured Cluster member.
type Member struct {
	PeerAddress   string `yaml:"peer_address"`
	ClientAddress string `yaml:"client_address"`
}

// Load reads and validates a node configuration file.
func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	if !filepath.IsAbs(cfg.Node.DataDir) {
		cfg.Node.DataDir = filepath.Clean(filepath.Join(filepath.Dir(path), cfg.Node.DataDir))
	}
	return cfg, nil
}

// Validate rejects ambiguous or incomplete local settings before startup.
func (c Config) Validate() error {
	var problems []error
	if c.Version != 1 {
		problems = append(problems, fmt.Errorf("version must be 1, got %d", c.Version))
	}
	if strings.TrimSpace(c.ClusterID) == "" {
		problems = append(problems, errors.New("cluster_id is required"))
	}
	if c.ActiveSessionLimit < 1 {
		problems = append(problems, errors.New("active_session_limit must be at least 1"))
	} else if uint64(c.ActiveSessionLimit) > uint64(^uint32(0)) {
		problems = append(problems, errors.New("active_session_limit exceeds the v1 protocol limit"))
	}
	if c.SnapshotThresholdBytes < 0 {
		problems = append(problems, errors.New("snapshot_threshold_bytes must not be negative"))
	}
	if strings.TrimSpace(c.Node.ID) == "" {
		problems = append(problems, errors.New("node.id is required"))
	}
	if strings.TrimSpace(c.Node.DataDir) == "" {
		problems = append(problems, errors.New("node.data_dir is required"))
	}
	if c.Node.MetricsAddress != "" {
		if err := validateAddress("node.metrics_address", c.Node.MetricsAddress); err != nil {
			problems = append(problems, err)
		}
	}
	if len(c.Members) != 3 {
		problems = append(problems, fmt.Errorf("members must contain exactly three Nodes, got %d", len(c.Members)))
	}
	if c.Node.ID != "" {
		if _, ok := c.Members[c.Node.ID]; !ok {
			problems = append(problems, fmt.Errorf("node.id %q is not present in members", c.Node.ID))
		}
	}
	addresses := make(map[string]string, len(c.Members)*2)
	for id, member := range c.Members {
		if strings.TrimSpace(id) == "" {
			problems = append(problems, errors.New("member Node Identity must not be empty"))
		}
		for name, address := range map[string]string{
			"peer_address":   member.PeerAddress,
			"client_address": member.ClientAddress,
		} {
			field := fmt.Sprintf("members[%q].%s", id, name)
			if err := validateAddress(field, address); err != nil {
				problems = append(problems, err)
				continue
			}
			if owner, exists := addresses[address]; exists {
				problems = append(problems, fmt.Errorf("%s duplicates %s at %q", field, owner, address))
			} else {
				addresses[address] = field
			}
		}
	}
	return errors.Join(problems...)
}

// LocalMember returns this process's addresses from the shared member map.
func (c Config) LocalMember() Member { return c.Members[c.Node.ID] }

// EffectiveSnapshotThresholdBytes returns the configured automatic Snapshot
// threshold, or the v1 default when the setting is omitted.
func (c Config) EffectiveSnapshotThresholdBytes() int64 {
	if c.SnapshotThresholdBytes == 0 {
		return DefaultSnapshotThresholdBytes
	}
	return c.SnapshotThresholdBytes
}

func validateAddress(name, address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("%s is required", name)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s must be a host:port address: %w", name, err)
	}
	if host == "" {
		return fmt.Errorf("%s host is required", name)
	}
	if port == "" || port == "0" {
		return fmt.Errorf("%s port must be non-zero", name)
	}
	return nil
}
