package node

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	peerProtocolVersion = uint32(1)
	peerRPCTimeout      = 250 * time.Millisecond
)

func (n *Node) Handshake(_ context.Context, request *quorumkvv1.HandshakeRequest) (*quorumkvv1.HandshakeResponse, error) {
	if err := n.validatePeer(request.ProtocolVersion, request.ClusterId, request.NodeId, request.TargetNodeId); err != nil {
		return nil, err
	}
	if request.ActiveSessionLimit != uint32(n.config.ActiveSessionLimit) {
		return nil, status.Errorf(codes.FailedPrecondition, "peer active Client Session limit %d does not match Node %q limit %d", request.ActiveSessionLimit, n.config.Node.ID, n.config.ActiveSessionLimit)
	}
	return &quorumkvv1.HandshakeResponse{
		ProtocolVersion:    peerProtocolVersion,
		ClusterId:          n.config.ClusterID,
		NodeId:             n.config.Node.ID,
		ActiveSessionLimit: uint32(n.config.ActiveSessionLimit),
	}, nil
}

func (n *Node) Send(ctx context.Context, request *quorumkvv1.SendRequest) (*quorumkvv1.SendResponse, error) {
	if err := n.validatePeer(request.ProtocolVersion, request.ClusterId, request.FromNodeId, request.ToNodeId); err != nil {
		return nil, err
	}
	event, err := decodeRaftMessage(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// A successful peer RPC means the bounded owner queue accepted the message.
	// Waiting for the owner to dequeue it can deadlock Nodes that synchronously
	// exchange reciprocal Raft messages; network loss at shutdown is permitted.
	input := raftInput{event: event}
	select {
	case n.events <- input:
		return &quorumkvv1.SendResponse{}, nil
	case <-n.runtimeDone:
		return nil, status.Error(codes.Unavailable, "target Node is stopping")
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (n *Node) validatePeer(version uint32, clusterID, fromNodeID, targetNodeID string) error {
	if version != peerProtocolVersion {
		return status.Errorf(codes.FailedPrecondition, "peer protocol version %d is incompatible with Node %q; require version %d", version, n.config.Node.ID, peerProtocolVersion)
	}
	if clusterID != n.config.ClusterID {
		return status.Errorf(codes.FailedPrecondition, "peer Cluster Identity %q does not match Node %q Cluster Identity %q", clusterID, n.config.Node.ID, n.config.ClusterID)
	}
	if targetNodeID != n.config.Node.ID {
		return status.Errorf(codes.FailedPrecondition, "peer targeted Node %q but reached Node %q", targetNodeID, n.config.Node.ID)
	}
	if fromNodeID == n.config.Node.ID {
		return status.Errorf(codes.FailedPrecondition, "peer claimed local Node Identity %q", fromNodeID)
	}
	if _, ok := n.config.Members[fromNodeID]; !ok {
		return status.Errorf(codes.FailedPrecondition, "peer Node Identity %q is not a configured Cluster member", fromNodeID)
	}
	return nil
}

type peerClient struct {
	connection *grpc.ClientConn
	client     quorumkvv1.PeerServiceClient
}

type peerConfigurationError struct{ err error }

func (e peerConfigurationError) Error() string { return e.err.Error() }
func (e peerConfigurationError) Unwrap() error { return e.err }

func isPeerConfigurationError(err error) bool {
	var configurationError peerConfigurationError
	return errors.As(err, &configurationError)
}

type peerTransport struct {
	config  config.Config
	clients map[raft.NodeID]*peerClient
}

func newPeerTransport(cfg config.Config) *peerTransport {
	return &peerTransport{config: cfg, clients: make(map[raft.NodeID]*peerClient)}
}

func (t *peerTransport) send(ctx context.Context, action raft.Action) error {
	if snapshotAction, ok := action.(raft.SendInstallSnapshot); ok {
		return t.sendSnapshot(ctx, snapshotAction)
	}
	to, message, err := encodeRaftAction(t.config, action)
	if err != nil {
		return err
	}
	client, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	if _, err := client.client.Send(requestCtx, message); err != nil {
		// Keep the channel: gRPC reconnects it in the background. Closing a
		// failed channel here can stall the single Raft owner during failover.
		sendErr := fmt.Errorf("send Raft message to Node %q: %w", to, err)
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.InvalidArgument {
			return peerConfigurationError{err: sendErr}
		}
		return sendErr
	}
	return nil
}

func (t *peerTransport) sendSnapshot(ctx context.Context, action raft.SendInstallSnapshot) error {
	contents, err := snapshot.Encoded(t.config.Node.DataDir, action.SnapshotIndex, action.SnapshotTerm)
	if err != nil {
		return fmt.Errorf("load snapshot for transfer: %w", err)
	}
	if action.Offset > uint64(len(contents)) {
		return fmt.Errorf("snapshot transfer offset %d exceeds length %d", action.Offset, len(contents))
	}
	const chunkSize = 64 << 10
	end := action.Offset + chunkSize
	if end > uint64(len(contents)) {
		end = uint64(len(contents))
	}
	client, err := t.client(ctx, action.To)
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	_, err = client.client.Send(requestCtx, &quorumkvv1.SendRequest{ProtocolVersion: peerProtocolVersion, ClusterId: t.config.ClusterID, FromNodeId: t.config.Node.ID, ToNodeId: string(action.To), Message: &quorumkvv1.SendRequest_InstallSnapshotRequest{InstallSnapshotRequest: &quorumkvv1.InstallSnapshotRequest{Term: action.Term, RequestId: action.RequestID, SnapshotIndex: action.SnapshotIndex, SnapshotTerm: action.SnapshotTerm, SnapshotLength: uint64(len(contents)), SnapshotChecksum: crc32.ChecksumIEEE(contents), Offset: action.Offset, Data: append([]byte(nil), contents[action.Offset:end]...), Done: end == uint64(len(contents))}}})
	if err != nil {
		return fmt.Errorf("send snapshot chunk to Node %q: %w", action.To, err)
	}
	return nil
}

func (t *peerTransport) client(ctx context.Context, id raft.NodeID) (*peerClient, error) {
	if client := t.clients[id]; client != nil {
		return client, nil
	}
	member, ok := t.config.Members[string(id)]
	if !ok {
		return nil, fmt.Errorf("send to unconfigured Node %q", id)
	}
	connection, err := grpc.NewClient(member.PeerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create peer client for Node %q at %q: %w", id, member.PeerAddress, err)
	}
	client := quorumkvv1.NewPeerServiceClient(connection)
	requestCtx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	response, err := client.Handshake(requestCtx, &quorumkvv1.HandshakeRequest{
		ProtocolVersion:    peerProtocolVersion,
		ClusterId:          t.config.ClusterID,
		NodeId:             t.config.Node.ID,
		TargetNodeId:       string(id),
		ActiveSessionLimit: uint32(t.config.ActiveSessionLimit),
	})
	if err != nil {
		connection.Close()
		handshakeErr := fmt.Errorf("handshake with Node %q at %q: %w", id, member.PeerAddress, err)
		if status.Code(err) == codes.FailedPrecondition {
			return nil, peerConfigurationError{err: handshakeErr}
		}
		return nil, handshakeErr
	}
	if response.ProtocolVersion != peerProtocolVersion || response.ClusterId != t.config.ClusterID || response.NodeId != string(id) || response.ActiveSessionLimit != uint32(t.config.ActiveSessionLimit) {
		connection.Close()
		return nil, peerConfigurationError{err: fmt.Errorf("handshake with Node %q returned protocol %d Cluster %q Node %q active Client Session limit %d", id, response.ProtocolVersion, response.ClusterId, response.NodeId, response.ActiveSessionLimit)}
	}
	peer := &peerClient{connection: connection, client: client}
	t.clients[id] = peer
	return peer, nil
}

func (t *peerTransport) close() error {
	var first error
	for id, client := range t.clients {
		if err := client.connection.Close(); err != nil && first == nil {
			first = fmt.Errorf("close peer connection to Node %q: %w", id, err)
		}
	}
	return first
}

func encodeRaftAction(cfg config.Config, action raft.Action) (raft.NodeID, *quorumkvv1.SendRequest, error) {
	request := &quorumkvv1.SendRequest{
		ProtocolVersion: peerProtocolVersion,
		ClusterId:       cfg.ClusterID,
		FromNodeId:      cfg.Node.ID,
	}
	var to raft.NodeID
	switch action := action.(type) {
	case raft.SendPreVoteRequest:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_PreVoteRequest{PreVoteRequest: &quorumkvv1.PreVoteRequest{Term: action.Request.Term, LastLogIndex: action.Request.LastLogIndex, LastLogTerm: action.Request.LastLogTerm}}
	case raft.SendPreVoteResponse:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_PreVoteResponse{PreVoteResponse: &quorumkvv1.PreVoteResponse{Term: action.Response.Term, CurrentTerm: action.Response.CurrentTerm, Granted: action.Response.Granted}}
	case raft.SendVoteRequest:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_VoteRequest{VoteRequest: &quorumkvv1.VoteRequest{Term: action.Request.Term, LastLogIndex: action.Request.LastLogIndex, LastLogTerm: action.Request.LastLogTerm}}
	case raft.SendVoteResponse:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_VoteResponse{VoteResponse: &quorumkvv1.VoteResponse{Term: action.Response.Term, Granted: action.Response.Granted}}
	case raft.SendAppendEntries:
		to = action.To
		entries := make([]*quorumkvv1.RaftLogEntry, len(action.Request.Entries))
		for index, entry := range action.Request.Entries {
			entries[index] = &quorumkvv1.RaftLogEntry{Index: entry.Index, Term: entry.Term, Type: encodeEntryType(entry.Type), SessionId: entry.SessionID[:], Sequence: entry.Sequence, Key: entry.Key, Value: entry.Value}
		}
		request.Message = &quorumkvv1.SendRequest_AppendEntriesRequest{AppendEntriesRequest: &quorumkvv1.AppendEntriesRequest{Term: action.Request.Term, RequestId: action.Request.RequestID, PreviousLogIndex: action.Request.PrevLogIndex, PreviousLogTerm: action.Request.PrevLogTerm, Entries: entries, LeaderCommit: action.Request.LeaderCommit, ReadId: uint64(action.Request.ReadID)}}
	case raft.SendAppendEntriesResponse:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_AppendEntriesResponse{AppendEntriesResponse: &quorumkvv1.AppendEntriesResponse{Term: action.Response.Term, RequestId: action.Response.RequestID, Success: action.Response.Success, MatchIndex: action.Response.MatchIndex, ConflictTerm: action.Response.ConflictTerm, ConflictIndex: action.Response.ConflictIndex, ReadId: uint64(action.Response.ReadID)}}
	case raft.SendInstallSnapshotResponse:
		to = action.To
		request.Message = &quorumkvv1.SendRequest_InstallSnapshotResponse{InstallSnapshotResponse: &quorumkvv1.InstallSnapshotResponse{Term: action.Response.Term, RequestId: action.Response.RequestID, Success: action.Response.Success, NextOffset: action.Response.NextOffset, SnapshotIndex: action.Response.SnapshotIndex, Done: action.Response.Done}}
	default:
		return "", nil, fmt.Errorf("encode unsupported Raft action %T", action)
	}
	request.ToNodeId = string(to)
	return to, request, nil
}

func decodeRaftMessage(request *quorumkvv1.SendRequest) (raft.Event, error) {
	from := raft.NodeID(request.FromNodeId)
	switch message := request.Message.(type) {
	case *quorumkvv1.SendRequest_PreVoteRequest:
		return raft.PreVoteRequest{From: from, Term: message.PreVoteRequest.Term, LastLogIndex: message.PreVoteRequest.LastLogIndex, LastLogTerm: message.PreVoteRequest.LastLogTerm}, nil
	case *quorumkvv1.SendRequest_PreVoteResponse:
		return raft.PreVoteResponse{From: from, Term: message.PreVoteResponse.Term, CurrentTerm: message.PreVoteResponse.CurrentTerm, Granted: message.PreVoteResponse.Granted}, nil
	case *quorumkvv1.SendRequest_VoteRequest:
		return raft.VoteRequest{From: from, Term: message.VoteRequest.Term, LastLogIndex: message.VoteRequest.LastLogIndex, LastLogTerm: message.VoteRequest.LastLogTerm}, nil
	case *quorumkvv1.SendRequest_VoteResponse:
		return raft.VoteResponse{From: from, Term: message.VoteResponse.Term, Granted: message.VoteResponse.Granted}, nil
	case *quorumkvv1.SendRequest_AppendEntriesRequest:
		entries := make([]raft.LogEntry, len(message.AppendEntriesRequest.Entries))
		for index, entry := range message.AppendEntriesRequest.Entries {
			entryType, err := decodeEntryType(entry.Type)
			if err != nil {
				return nil, err
			}
			if entryType != raft.EntryNoOp && len(entry.SessionId) != 16 {
				return nil, fmt.Errorf("raft session log entry identity is %d bytes, want 16", len(entry.SessionId))
			}
			if entryType == raft.EntryNoOp && len(entry.SessionId) != 0 && len(entry.SessionId) != 16 {
				return nil, fmt.Errorf("raft log entry session identity is %d bytes, want 16", len(entry.SessionId))
			}
			var sessionID raft.SessionID
			copy(sessionID[:], entry.SessionId)
			entries[index] = raft.LogEntry{Index: entry.Index, Term: entry.Term, Type: entryType, SessionID: sessionID, Sequence: entry.Sequence, Key: entry.Key, Value: append([]byte(nil), entry.Value...)}
		}
		return raft.AppendEntries{From: from, Term: message.AppendEntriesRequest.Term, RequestID: message.AppendEntriesRequest.RequestId, PrevLogIndex: message.AppendEntriesRequest.PreviousLogIndex, PrevLogTerm: message.AppendEntriesRequest.PreviousLogTerm, Entries: entries, LeaderCommit: message.AppendEntriesRequest.LeaderCommit, ReadID: raft.ReadID(message.AppendEntriesRequest.ReadId)}, nil
	case *quorumkvv1.SendRequest_AppendEntriesResponse:
		return raft.AppendEntriesResponse{From: from, Term: message.AppendEntriesResponse.Term, RequestID: message.AppendEntriesResponse.RequestId, Success: message.AppendEntriesResponse.Success, MatchIndex: message.AppendEntriesResponse.MatchIndex, ConflictTerm: message.AppendEntriesResponse.ConflictTerm, ConflictIndex: message.AppendEntriesResponse.ConflictIndex, ReadID: raft.ReadID(message.AppendEntriesResponse.ReadId)}, nil
	case *quorumkvv1.SendRequest_InstallSnapshotRequest:
		s := message.InstallSnapshotRequest
		return raft.InstallSnapshot{From: from, Term: s.Term, RequestID: s.RequestId, SnapshotIndex: s.SnapshotIndex, SnapshotTerm: s.SnapshotTerm, Length: s.SnapshotLength, Checksum: s.SnapshotChecksum, Offset: s.Offset, Data: append([]byte(nil), s.Data...), Done: s.Done}, nil
	case *quorumkvv1.SendRequest_InstallSnapshotResponse:
		s := message.InstallSnapshotResponse
		return raft.InstallSnapshotResponse{From: from, Term: s.Term, RequestID: s.RequestId, SnapshotIndex: s.SnapshotIndex, Success: s.Success, NextOffset: s.NextOffset, Done: s.Done}, nil
	default:
		return nil, fmt.Errorf("raft message payload is required")
	}
}

func encodeEntryType(entryType raft.EntryType) quorumkvv1.RaftEntryType {
	switch entryType {
	case raft.EntryNoOp:
		return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_NO_OP
	case raft.EntryOpenSession:
		return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_OPEN_SESSION
	case raft.EntryCloseSession:
		return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_CLOSE_SESSION
	case raft.EntrySet:
		return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_SET
	case raft.EntryDelete:
		return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_DELETE
	}
	return quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_UNSPECIFIED
}

func decodeEntryType(entryType quorumkvv1.RaftEntryType) (raft.EntryType, error) {
	switch entryType {
	case quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_NO_OP:
		return raft.EntryNoOp, nil
	case quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_OPEN_SESSION:
		return raft.EntryOpenSession, nil
	case quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_CLOSE_SESSION:
		return raft.EntryCloseSession, nil
	case quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_SET:
		return raft.EntrySet, nil
	case quorumkvv1.RaftEntryType_RAFT_ENTRY_TYPE_DELETE:
		return raft.EntryDelete, nil
	default:
		return 0, fmt.Errorf("raft entry type %s is unsupported", entryType)
	}
}
