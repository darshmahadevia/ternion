package node

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposeOperationalCounters(t *testing.T) {
	var metrics nodeMetrics
	metrics.rpcTotal.Add(3)
	response := httptest.NewRecorder()
	metrics.handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	for _, name := range []string{
		"quorumkv_rpc_total 3",
		"quorumkv_elections_total",
		"quorumkv_raft_rpcs_total",
		"quorumkv_proposals_total",
		"quorumkv_wal_syncs_total",
		"quorumkv_snapshot_installations_total",
		"quorumkv_snapshot_compactions_total",
	} {
		if !strings.Contains(body, name) {
			t.Fatalf("metrics output missing %q:\n%s", name, body)
		}
	}
}
