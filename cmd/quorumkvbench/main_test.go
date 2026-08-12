package main

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSummarizeReportsNearestRankPercentiles(t *testing.T) {
	result := summarize("GET", []int64{100, 1, 10, 20, 30}, 2*time.Second)

	if result.ThroughputOpsPerSecond != 2.5 {
		t.Fatalf("throughput = %v, want 2.5", result.ThroughputOpsPerSecond)
	}
	if result.P50Nanoseconds != 20 || result.P95Nanoseconds != 100 || result.P99Nanoseconds != 100 {
		t.Fatalf("percentiles = p50:%d p95:%d p99:%d, want 20, 100, 100", result.P50Nanoseconds, result.P95Nanoseconds, result.P99Nanoseconds)
	}
	if !reflect.DeepEqual(result.LatencySamples, []int64{100, 1, 10, 20, 30}) {
		t.Fatalf("raw samples = %v, want acquisition order preserved", result.LatencySamples)
	}
}

func TestRunConcurrentAssignsEveryOperationOnce(t *testing.T) {
	const operations = 101
	seen := make([]bool, operations)
	var lock sync.Mutex
	result, err := runConcurrent(context.Background(), "SET", operations, 8, func(_ int, operation int) error {
		lock.Lock()
		defer lock.Unlock()
		if seen[operation] {
			return errors.New("operation assigned twice")
		}
		seen[operation] = true
		return nil
	})
	if err != nil {
		t.Fatalf("runConcurrent: %v", err)
	}
	if result.Operations != operations {
		t.Fatalf("operations = %d, want %d", result.Operations, operations)
	}
	for operation, wasSeen := range seen {
		if !wasSeen {
			t.Fatalf("operation %d was not assigned", operation)
		}
	}
}

func TestValidateOptionsRequiresPublishableMetadata(t *testing.T) {
	opts := options{
		addresses: []string{"one:1", "two:2", "three:3"}, setOperations: 1,
		getOperations: 1, concurrency: 1, timeout: time.Second,
	}
	if err := validateOptions(opts); err == nil {
		t.Fatal("validateOptions accepted an empty hardware description")
	}
	opts.hardware = "test machine"
	if err := validateOptions(opts); err != nil {
		t.Fatalf("validateOptions rejected valid options: %v", err)
	}
}
