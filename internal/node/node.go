package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	// LivenessService and ReadinessService are separate health-check names so
	// operators do not mistake local process health for readiness to accept RPCs.
	LivenessService  = "quorumkv.v1.Liveness"
	ReadinessService = "quorumkv.v1.Readiness"

	shutdownGracePeriod = 5 * time.Second
)

// Node owns the local listeners and gRPC servers for one configured process.
// Run must be called at most once.
type Node struct {
	quorumkvv1.UnimplementedNodeServiceServer
	quorumkvv1.UnimplementedClientServiceServer
	quorumkvv1.UnimplementedPeerServiceServer

	config          config.Config
	ready           atomic.Bool
	raftState       atomic.Value
	events          chan raftInput
	runtimeDone     chan struct{}
	nextProposal    atomic.Uint64
	nextRead        atomic.Uint64
	observeMutation mutationObserver
	metrics         nodeMetrics
	logger          *slog.Logger
	lastRole        atomic.Uint32
}

type raftInput struct {
	event          raft.Event
	result         chan proposalResult
	requestContext context.Context
	readResult     chan readResult
	key            string
	snapshotResult chan error
}

// New creates a node from an already validated configuration.
func New(cfg config.Config) *Node {
	n := &Node{
		config:      cfg,
		events:      make(chan raftInput, 256),
		runtimeDone: make(chan struct{}),
	}
	n.logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	n.publishRaftState(raft.State{ID: raft.NodeID(cfg.Node.ID), Role: raft.Follower})
	return n
}

// Run serves the peer and client endpoints until cancellation or a server error.
func (n *Node) Run(ctx context.Context) (runErr error) {
	local := n.config.LocalMember()
	peers := make([]raft.NodeID, 0, len(n.config.Members)-1)
	for id := range n.config.Members {
		if id != n.config.Node.ID {
			peers = append(peers, raft.NodeID(id))
		}
	}
	runtime, err := openRaftRuntime(n.config, peers)
	if err != nil {
		return err
	}
	runtime.observeMutation = n.observeMutation
	runtime.metrics = &n.metrics
	defer func() {
		if err := runtime.close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close Node %q consensus state: %w", n.config.Node.ID, err)
		}
	}()

	peerListener, err := net.Listen("tcp", local.PeerAddress)
	if err != nil {
		return fmt.Errorf("listen on peer address %q: %w", local.PeerAddress, err)
	}
	defer peerListener.Close()

	clientListener, err := net.Listen("tcp", local.ClientAddress)
	if err != nil {
		return fmt.Errorf("listen on client address %q: %w", local.ClientAddress, err)
	}
	defer clientListener.Close()

	var metricsListener net.Listener
	var metricsServer *http.Server
	if n.config.Node.MetricsAddress != "" {
		metricsListener, err = net.Listen("tcp", n.config.Node.MetricsAddress)
		if err != nil {
			return fmt.Errorf("listen on metrics address %q: %w", n.config.Node.MetricsAddress, err)
		}
		defer metricsListener.Close()
		metricsServer = &http.Server{Handler: n.metrics.handler(), ReadHeaderTimeout: 5 * time.Second}
		defer metricsServer.Close()
	}

	peerServer := grpc.NewServer(grpc.UnaryInterceptor(n.metricsInterceptor))
	clientServer := grpc.NewServer(grpc.UnaryInterceptor(n.metricsInterceptor))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(LivenessService, grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpc_health_v1.RegisterHealthServer(clientServer, healthServer)
	quorumkvv1.RegisterNodeServiceServer(clientServer, n)
	quorumkvv1.RegisterClientServiceServer(clientServer, n)
	quorumkvv1.RegisterPeerServiceServer(peerServer, n)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport := newPeerTransport(n.config)
	defer func() {
		if err := transport.close(); err != nil && runErr == nil {
			runErr = err
		}
	}()

	n.ready.Store(true)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_SERVING)

	serveErrors := make(chan error, 3)
	var servers sync.WaitGroup
	serve := func(name string, server *grpc.Server, listener net.Listener) {
		defer servers.Done()
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrors <- fmt.Errorf("serve %s endpoint: %w", name, err)
		}
	}
	servers.Add(2)
	go serve("peer", peerServer, peerListener)
	go serve("client", clientServer, clientListener)
	if metricsServer != nil {
		servers.Add(1)
		go func() {
			defer servers.Done()
			if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErrors <- fmt.Errorf("serve metrics endpoint: %w", err)
			}
		}()
	}
	servers.Add(1)
	go func() {
		defer servers.Done()
		defer close(n.runtimeDone)
		if err := n.runRaft(runCtx, runtime, transport); err != nil {
			serveErrors <- fmt.Errorf("run Raft: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
	case runErr = <-serveErrors:
	}
	cancel()
	if metricsServer != nil {
		_ = metricsServer.Shutdown(context.Background())
	}

	n.ready.Store(false)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	stopServers(peerServer, clientServer)
	servers.Wait()
	return runErr
}

func stopServers(servers ...*grpc.Server) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, server := range servers {
			server.GracefulStop()
		}
	}()

	select {
	case <-done:
	case <-time.After(shutdownGracePeriod):
		for _, server := range servers {
			server.Stop()
		}
		<-done
	}
}

// GetStatus reports only local process state; it makes no Cluster health claim.
func (n *Node) GetStatus(context.Context, *quorumkvv1.GetStatusRequest) (*quorumkvv1.GetStatusResponse, error) {
	state := quorumkvv1.NodeState_NODE_STATE_STARTING
	if n.ready.Load() {
		state = quorumkvv1.NodeState_NODE_STATE_READY
	}
	local := n.config.LocalMember()
	raftState := n.raftState.Load().(raft.State)
	return &quorumkvv1.GetStatusResponse{
		ClusterId:     n.config.ClusterID,
		NodeId:        n.config.Node.ID,
		State:         state,
		PeerAddress:   local.PeerAddress,
		ClientAddress: local.ClientAddress,
		Role:          encodeRole(raftState.Role),
		LeaderId:      string(raftState.LeaderID),
		Term:          raftState.Term,
		LastLogIndex:  raftState.LastLogIndex,
		CommitIndex:   raftState.CommitIndex,
		LastApplied:   raftState.LastApplied,
		SnapshotIndex: raftState.SnapshotIndex,
	}, nil
}

func (n *Node) publishRaftState(state raft.State) {
	n.raftState.Store(state)
	role := uint32(state.Role)
	if n.lastRole.Swap(role) != role && n.logger != nil {
		n.logger.Info("Raft role changed", "node_id", state.ID, "term", state.Term, "role", encodeRole(state.Role).String(), "leader_id", state.LeaderID, "last_log_index", state.LastLogIndex, "commit_index", state.CommitIndex, "last_applied", state.LastApplied, "snapshot_index", state.SnapshotIndex)
	}
}

func (n *Node) metricsInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	response, err := handler(ctx, req)
	n.metrics.observeRPC(err)
	if err != nil {
		n.metrics.clientErrors.Add(1)
	}
	return response, err
}

func encodeRole(role raft.Role) quorumkvv1.RaftRole {
	switch role {
	case raft.Follower:
		return quorumkvv1.RaftRole_RAFT_ROLE_FOLLOWER
	case raft.PreCandidate:
		return quorumkvv1.RaftRole_RAFT_ROLE_PRE_CANDIDATE
	case raft.Candidate:
		return quorumkvv1.RaftRole_RAFT_ROLE_CANDIDATE
	case raft.Leader:
		return quorumkvv1.RaftRole_RAFT_ROLE_LEADER
	default:
		return quorumkvv1.RaftRole_RAFT_ROLE_UNSPECIFIED
	}
}
