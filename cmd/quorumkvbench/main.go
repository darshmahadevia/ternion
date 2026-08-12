// Command quorumkvbench measures the public API of a running three-Node Cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
)

type options struct {
	addresses     []string
	setOperations int
	getOperations int
	concurrency   int
	valueBytes    int
	hardware      string
	output        string
	timeout       time.Duration
}

type report struct {
	FormatVersion int         `json:"format_version"`
	GeneratedAt   time.Time   `json:"generated_at"`
	Environment   environment `json:"environment"`
	Cluster       cluster     `json:"cluster"`
	Parameters    parameters  `json:"parameters"`
	Workloads     []workload  `json:"workloads"`
}

type environment struct {
	Hardware  string `json:"hardware"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	GoVersion string `json:"go_version"`
}

type cluster struct {
	Nodes          int    `json:"nodes"`
	Topology       string `json:"topology"`
	DurableWALSync bool   `json:"durable_wal_sync"`
}

type parameters struct {
	SetOperations int `json:"set_operations"`
	GetOperations int `json:"get_operations"`
	Concurrency   int `json:"concurrency"`
	ValueBytes    int `json:"value_bytes"`
}

type workload struct {
	Command                string  `json:"command"`
	Operations             int     `json:"operations"`
	DurationNanoseconds    int64   `json:"duration_nanoseconds"`
	ThroughputOpsPerSecond float64 `json:"throughput_ops_per_second"`
	P50Nanoseconds         int64   `json:"p50_nanoseconds"`
	P95Nanoseconds         int64   `json:"p95_nanoseconds"`
	P99Nanoseconds         int64   `json:"p99_nanoseconds"`
	LatencySamples         []int64 `json:"latency_samples_nanoseconds"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("quorumkvbench", flag.ContinueOnError)
	addresses := flags.String("addresses", "127.0.0.1:17401,127.0.0.1:17402,127.0.0.1:17403", "comma-separated Node client addresses")
	setOperations := flags.Int("set-operations", 500, "number of durable SET commands")
	getOperations := flags.Int("get-operations", 2000, "number of linearizable GET commands")
	concurrency := flags.Int("concurrency", 8, "concurrent clients")
	valueBytes := flags.Int("value-bytes", 1024, "Value payload size")
	hardware := flags.String("hardware", "", "hardware description recorded in the result (required)")
	output := flags.String("output", "", "result JSON path (default stdout)")
	timeout := flags.Duration("timeout", 10*time.Minute, "overall benchmark timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	opts := options{
		addresses: strings.Split(*addresses, ","), setOperations: *setOperations,
		getOperations: *getOperations, concurrency: *concurrency, valueBytes: *valueBytes,
		hardware: strings.TrimSpace(*hardware), output: *output, timeout: *timeout,
	}
	if err := validateOptions(opts); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	result, err := measure(ctx, opts)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode benchmark report: %w", err)
	}
	encoded = append(encoded, '\n')
	if opts.output == "" {
		_, err = os.Stdout.Write(encoded)
	} else {
		err = os.WriteFile(opts.output, encoded, 0o644)
	}
	if err != nil {
		return fmt.Errorf("write benchmark report: %w", err)
	}
	return nil
}

func validateOptions(opts options) error {
	if len(opts.addresses) != 3 {
		return fmt.Errorf("addresses must contain exactly three Nodes")
	}
	for _, address := range opts.addresses {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("addresses must not contain an empty address")
		}
	}
	if opts.setOperations < 1 || opts.getOperations < 1 {
		return fmt.Errorf("set-operations and get-operations must be positive")
	}
	if opts.concurrency < 1 || opts.concurrency > opts.setOperations {
		return fmt.Errorf("concurrency must be between 1 and set-operations")
	}
	if opts.valueBytes < 0 || opts.valueBytes > 1<<20 {
		return fmt.Errorf("value-bytes must be between 0 and 1048576")
	}
	if opts.hardware == "" {
		return fmt.Errorf("hardware is required so published results remain attributable")
	}
	if opts.timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	return nil
}

func measure(ctx context.Context, opts options) (report, error) {
	clients := make([]*client.Client, opts.concurrency)
	sessions := make([][16]byte, opts.concurrency)
	for index := range clients {
		clients[index] = client.New(opts.addresses...)
		sessionID, err := clients[index].OpenSession(ctx)
		if err != nil {
			return report{}, fmt.Errorf("open Client Session %d: %w", index, err)
		}
		sessions[index] = sessionID
	}

	value := make([]byte, opts.valueBytes)
	for index := range value {
		value[index] = byte(index)
	}
	setSequences := make([]uint64, opts.concurrency)
	setResult, err := runConcurrent(ctx, "SET", opts.setOperations, opts.concurrency, func(worker, operation int) error {
		setSequences[worker]++
		return clients[worker].Set(ctx, sessions[worker], setSequences[worker], benchmarkKey(operation), value)
	})
	if err != nil {
		return report{}, err
	}
	getResult, err := runConcurrent(ctx, "GET", opts.getOperations, opts.concurrency, func(worker, operation int) error {
		_, err := clients[worker].Get(ctx, benchmarkKey(operation%opts.setOperations))
		return err
	})
	if err != nil {
		return report{}, err
	}
	for index := range clients {
		if err := clients[index].CloseSession(ctx, sessions[index]); err != nil {
			return report{}, fmt.Errorf("close Client Session %d: %w", index, err)
		}
	}

	return report{
		FormatVersion: 1,
		GeneratedAt:   time.Now().UTC(),
		Environment:   environment{Hardware: opts.hardware, OS: runtime.GOOS, Arch: runtime.GOARCH, GoVersion: runtime.Version()},
		Cluster:       cluster{Nodes: 3, Topology: "three local OS processes with separate data directories", DurableWALSync: true},
		Parameters:    parameters{SetOperations: opts.setOperations, GetOperations: opts.getOperations, Concurrency: opts.concurrency, ValueBytes: opts.valueBytes},
		Workloads:     []workload{setResult, getResult},
	}, nil
}

func runConcurrent(ctx context.Context, command string, operations, concurrency int, call func(worker, operation int) error) (workload, error) {
	latencies := make([]int64, operations)
	var next atomic.Int64
	var firstErr error
	var errorOnce sync.Once
	var workers sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for {
				operation := int(next.Add(1) - 1)
				if operation >= operations || ctx.Err() != nil {
					return
				}
				calledAt := time.Now()
				if err := call(worker, operation); err != nil {
					errorOnce.Do(func() { firstErr = fmt.Errorf("%s operation %d: %w", command, operation, err) })
					return
				}
				latencies[operation] = time.Since(calledAt).Nanoseconds()
			}
		}(worker)
	}
	workers.Wait()
	duration := time.Since(start)
	if firstErr != nil {
		return workload{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return workload{}, fmt.Errorf("%s workload: %w", command, err)
	}
	return summarize(command, latencies, duration), nil
}

func summarize(command string, samples []int64, duration time.Duration) workload {
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return workload{
		Command: command, Operations: len(samples), DurationNanoseconds: duration.Nanoseconds(),
		ThroughputOpsPerSecond: float64(len(samples)) / duration.Seconds(),
		P50Nanoseconds:         percentile(sorted, 50), P95Nanoseconds: percentile(sorted, 95), P99Nanoseconds: percentile(sorted, 99),
		LatencySamples: samples,
	}
}

func percentile(sorted []int64, percentage float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(percentage/100*float64(len(sorted)))) - 1
	return sorted[max(0, index)]
}

func benchmarkKey(operation int) string {
	return fmt.Sprintf("benchmark-%08d", operation)
}
