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
		"ternion_rpc_total 3",
		"ternion_elections_total",
		"ternion_raft_rpcs_total",
		"ternion_proposals_total",
		"ternion_wal_syncs_total",
		"ternion_snapshot_installations_total",
		"ternion_snapshot_compactions_total",
	} {
		if !strings.Contains(body, name) {
			t.Fatalf("metrics output missing %q:\n%s", name, body)
		}
	}
}
