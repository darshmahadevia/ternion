package raft

import "testing"

func TestLogFreshnessComparison(t *testing.T) {
	tests := []struct {
		name           string
		candidateTerm  uint64
		candidateIndex uint64
		localTerm      uint64
		localIndex     uint64
		want           bool
	}{
		{name: "newer Term wins despite shorter log", candidateTerm: 3, candidateIndex: 1, localTerm: 2, localIndex: 10, want: true},
		{name: "older Term loses despite longer log", candidateTerm: 1, candidateIndex: 10, localTerm: 2, localIndex: 1, want: false},
		{name: "same Term requires equal or later index", candidateTerm: 2, candidateIndex: 4, localTerm: 2, localIndex: 5, want: false},
		{name: "identical position is current", candidateTerm: 2, candidateIndex: 5, localTerm: 2, localIndex: 5, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := logIsAtLeastAsUpToDate(test.candidateTerm, test.candidateIndex, test.localTerm, test.localIndex)
			if got != test.want {
				t.Fatalf("log freshness = %t, want %t", got, test.want)
			}
		})
	}
}
