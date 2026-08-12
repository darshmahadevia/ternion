package node

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// nodeMetrics is deliberately small and lock-free: it is observed from the
// gRPC servers while the Raft loop updates it from its own goroutine.
type nodeMetrics struct {
	rpcTotal, rpcErrors                                atomic.Uint64
	elections, raftRPCs                                atomic.Uint64
	proposals, clientErrors, clientRetries             atomic.Uint64
	walSyncs, snapshots, snapshotInstalls, compactions atomic.Uint64
	commitLatencyMicros                                atomic.Uint64
}

func (m *nodeMetrics) observeRPC(err error) {
	m.rpcTotal.Add(1)
	if err != nil {
		m.rpcErrors.Add(1)
	}
}

func (m *nodeMetrics) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetric := func(name string, value uint64) { _, _ = fmt.Fprintf(w, "quorumkv_%s %d\n", name, value) }
		writeMetric("rpc_total", m.rpcTotal.Load())
		writeMetric("rpc_errors_total", m.rpcErrors.Load())
		writeMetric("elections_total", m.elections.Load())
		writeMetric("raft_rpcs_total", m.raftRPCs.Load())
		writeMetric("proposals_total", m.proposals.Load())
		writeMetric("client_errors_total", m.clientErrors.Load())
		writeMetric("client_retries_total", m.clientRetries.Load())
		writeMetric("wal_syncs_total", m.walSyncs.Load())
		writeMetric("snapshots_total", m.snapshots.Load())
		writeMetric("snapshot_installations_total", m.snapshotInstalls.Load())
		writeMetric("snapshot_compactions_total", m.compactions.Load())
		writeMetric("commit_latency_microseconds_total", m.commitLatencyMicros.Load())
	})
}
