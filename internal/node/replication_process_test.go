package node_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

func TestThreeProcessesRepairFollowerAfterPeerPartitionHeals(t *testing.T) {
	actualMembers := make(map[string]config.Member, 3)
	for index := 1; index <= 3; index++ {
		actualMembers[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedAddress(t),
			ClientAddress: unusedAddress(t),
		}
	}

	proxies := make(map[string]*peerPartitionProxy, 3)
	for id, member := range actualMembers {
		proxies[id] = newPeerPartitionProxy(t, member.PeerAddress)
	}
	processes := make(map[string]*nodeProcess, 3)
	configs := make(map[string]config.Config, 3)
	for index := 1; index <= 3; index++ {
		id := fmt.Sprintf("node-%d", index)
		members := make(map[string]config.Member, 3)
		for memberID, member := range actualMembers {
			if memberID != id {
				member.PeerAddress = proxies[memberID].address()
			}
			members[memberID] = member
		}
		cfg := config.Config{
			Version:            1,
			ClusterID:          "replication-partition-test",
			ActiveSessionLimit: 2,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		}
		configs[id] = cfg
		processes[id] = startNodeProcess(t, cfg)
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
	}()

	leader := waitForStableLeader(t, actualMembers, nil, processTestDeadline)
	stale := memberIDForAddress(t, actualMembers, memberOtherThan(t, actualMembers, map[string]bool{leader: true}).ClientAddress)
	ctx, cancel := context.WithTimeout(context.Background(), processTestDeadline)
	sessionID, err := client.New(actualMembers[leader].ClientAddress).OpenSession(ctx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session: %v", err)
	}

	proxies[stale].setEnabled(false)
	for sequence, value := range []string{"first", "second", "third"} {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		err = client.New(actualMembers[leader].ClientAddress).Set(ctx, sessionID, uint64(sequence+1), "partitioned", []byte(value))
		cancel()
		if err != nil {
			t.Fatalf("SET sequence %d while Follower is partitioned: %v", sequence+1, err)
		}
	}
	if status := fetchStatus(actualMembers[stale].ClientAddress); status == nil {
		t.Fatal("partitioned Follower process stopped serving local status")
	}

	proxies[stale].setEnabled(true)
	if healedLeader := waitForStableLeader(t, actualMembers, nil, processTestDeadline); healedLeader != leader {
		t.Fatalf("Leader changed from %q to %q while ordinary Follower partition healed", leader, healedLeader)
	}

	for id, process := range processes {
		process.stop()
		delete(processes, id)
	}
	leaderStore, leaderRecovered, err := wal.Open(configs[leader].Node.DataDir, wal.Identity{ClusterID: configs[leader].ClusterID, NodeID: leader})
	if err != nil {
		t.Fatalf("recover Leader WAL: %v", err)
	}
	defer leaderStore.Close()
	store, recovered, err := wal.Open(configs[stale].Node.DataDir, wal.Identity{ClusterID: configs[stale].ClusterID, NodeID: stale})
	if err != nil {
		t.Fatalf("recover healed Follower WAL: %v", err)
	}
	defer store.Close()
	if recovered.CommitIndex != leaderRecovered.CommitIndex {
		t.Fatalf("healed Follower durable commit index = %d, Leader = %d", recovered.CommitIndex, leaderRecovered.CommitIndex)
	}
	if recovered.CommitIndex == 0 || uint64(len(recovered.Log)) < recovered.CommitIndex || uint64(len(leaderRecovered.Log)) < leaderRecovered.CommitIndex {
		t.Fatalf("invalid recovered progress: Follower commit/log=%d/%d Leader=%d/%d", recovered.CommitIndex, len(recovered.Log), leaderRecovered.CommitIndex, len(leaderRecovered.Log))
	}
	for index := uint64(0); index < recovered.CommitIndex; index++ {
		if got, want := recovered.Log[index], leaderRecovered.Log[index]; got.Index != want.Index || got.Term != want.Term || got.Type != want.Type || got.Key != want.Key || string(got.Value) != string(want.Value) {
			t.Fatalf("healed Follower committed entry %d = %#v, Leader = %#v", index+1, got, want)
		}
	}
	var values []string
	for _, entry := range recovered.Log {
		if entry.Type == wal.EntryType(raft.EntrySet) && entry.Key == "partitioned" {
			values = append(values, string(entry.Value))
		}
	}
	if got := fmt.Sprint(values); got != "[first second third]" {
		t.Fatalf("healed Follower SET values = %s, want [first second third]", got)
	}
}

type peerPartitionProxy struct {
	listener net.Listener
	target   string

	mu      sync.Mutex
	enabled bool
	closed  bool
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
}

func newPeerPartitionProxy(t *testing.T, target string) *peerPartitionProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for peer partition proxy: %v", err)
	}
	proxy := &peerPartitionProxy{listener: listener, target: target, enabled: true, conns: make(map[net.Conn]struct{})}
	proxy.wg.Add(1)
	go proxy.accept()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *peerPartitionProxy) address() string { return p.listener.Addr().String() }

func (p *peerPartitionProxy) setEnabled(enabled bool) {
	p.mu.Lock()
	p.enabled = enabled
	if !enabled {
		for connection := range p.conns {
			_ = connection.Close()
		}
	}
	p.mu.Unlock()
}

func (p *peerPartitionProxy) accept() {
	defer p.wg.Done()
	for {
		connection, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		enabled := p.enabled && !p.closed
		p.mu.Unlock()
		if !enabled {
			_ = connection.Close()
			continue
		}
		p.wg.Add(1)
		go p.forward(connection)
	}
}

func (p *peerPartitionProxy) forward(downstream net.Conn) {
	defer p.wg.Done()
	upstream, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		_ = downstream.Close()
		return
	}
	p.track(downstream, upstream, true)
	done := make(chan struct{}, 2)
	copyConnection := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		done <- struct{}{}
	}
	go copyConnection(upstream, downstream)
	go copyConnection(downstream, upstream)
	<-done
	_ = downstream.Close()
	_ = upstream.Close()
	<-done
	p.track(downstream, upstream, false)
}

func (p *peerPartitionProxy) track(first, second net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.conns[first] = struct{}{}
		p.conns[second] = struct{}{}
		return
	}
	delete(p.conns, first)
	delete(p.conns, second)
}

func (p *peerPartitionProxy) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	_ = p.listener.Close()
	for connection := range p.conns {
		_ = connection.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
