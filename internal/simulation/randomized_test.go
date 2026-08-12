package simulation_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/simulation"
)

func TestRandomizedFaultScheduleIsReplayableAndCoversAllowedFaults(t *testing.T) {
	config := simulation.RandomConfig{Seed: 20250308, Steps: 400}
	first, err := simulation.RunRandomized(config)
	if err != nil {
		t.Fatalf("run randomized simulation: %v", err)
	}
	second, err := simulation.RunRandomized(config)
	if err != nil {
		t.Fatalf("replay randomized simulation: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed and step count produced different traces")
	}

	faults := first.Faults
	if !faults.MessageDelay || !faults.MessageDrop || !faults.MessageDuplicate || !faults.MessageReorder ||
		!faults.Asymmetric || !faults.Crash || !faults.Restart || !faults.PersistenceDelay {
		t.Fatalf("fault coverage = %#v, want every allowed fault class", faults)
	}

	seen := make(map[string]bool)
	for _, event := range first.Trace {
		seen[event.Kind] = true
	}
	for _, kind := range []string{"timer", "message", "persistence", "crash", "restart"} {
		if !seen[kind] {
			t.Errorf("trace does not contain %q event", kind)
		}
	}
}

func TestTraceFileCanBeReplacedByAnExactReplay(t *testing.T) {
	config := simulation.RandomConfig{Seed: 99, Steps: 200}
	result, err := simulation.RunRandomized(config)
	if err != nil {
		t.Fatalf("run randomized simulation: %v", err)
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := result.WriteTrace(path); err != nil {
		t.Fatalf("write first trace: %v", err)
	}
	if err := result.WriteTrace(path); err != nil {
		t.Fatalf("replace trace: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	var decoded simulation.RandomResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if !reflect.DeepEqual(decoded, result) {
		t.Fatal("stored trace does not match the replay result")
	}
}

func TestRandomizedFaultSchedulesPreserveInvariantsAcrossSeeds(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		seed := seed
		t.Run(string(rune('A'+seed-1)), func(t *testing.T) {
			t.Parallel()
			result, err := simulation.RunRandomized(simulation.RandomConfig{Seed: seed, Steps: 500})
			if err != nil {
				t.Fatalf("run seed %d (replay: go run ./cmd/quorumkvsim -seed %d -steps 500): %v\nlast trace event: %#v", seed, seed, err, lastTrace(result))
			}
		})
	}
}

func lastTrace(result simulation.RandomResult) simulation.TraceEvent {
	if len(result.Trace) == 0 {
		return simulation.TraceEvent{}
	}
	return result.Trace[len(result.Trace)-1]
}
