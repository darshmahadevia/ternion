// quorumkvsim replays one seeded deterministic fault schedule.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Het-Jethva/quorumkv/internal/simulation"
)

func main() {
	seed := flag.Int64("seed", 1, "deterministic simulation seed")
	steps := flag.Int("steps", 1000, "maximum scheduler steps")
	tracePath := flag.String("trace", "", "optional JSON trace output path")
	flag.Parse()

	result, runErr := simulation.RunRandomized(simulation.RandomConfig{Seed: *seed, Steps: *steps})
	if *tracePath != "" {
		if err := os.MkdirAll(filepath.Dir(*tracePath), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "create trace directory: %v\n", err)
			os.Exit(1)
		}
		if err := result.WriteTrace(*tracePath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		for _, event := range result.Trace {
			fmt.Fprintf(os.Stderr, "%04d %-22s %-8s %s\n", event.Sequence, event.Kind, event.Node, event.Detail)
		}
		fmt.Fprintf(os.Stderr, "replay: go run ./cmd/quorumkvsim -seed %d -steps %d\n", *seed, *steps)
		os.Exit(1)
	}
	fmt.Printf("seed %d passed %d scheduler steps (%d trace events)\n", *seed, *steps, len(result.Trace))
}
