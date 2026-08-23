package node

import (
	"context"

	ternionv1 "github.com/darshmahadevia/ternion/gen/ternion/v1"
	"github.com/darshmahadevia/ternion/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readResult struct {
	value    []byte
	found    bool
	leaderID raft.NodeID
	rejected bool
}

// Get returns a Value only after Raft confirms the Leader's current authority.
func (n *Node) Get(ctx context.Context, request *ternionv1.GetRequest) (*ternionv1.GetResponse, error) {
	if err := validateKey(request.Key); err != nil {
		return nil, err
	}
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}

	results := make(chan readResult, 1)
	readID := raft.ReadID(n.nextRead.Add(1))
	input := readInput{event: raft.ConfirmRead{ReadID: readID}, result: results, ctx: ctx, key: request.Key}
	select {
	case n.inputs <- input:
	case <-n.runtimeDone:
		return nil, status.Error(codes.Unavailable, "Node is stopping")
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case result := <-results:
		if result.rejected {
			return nil, n.proposalError(proposalResult{leaderID: result.leaderID, rejected: true})
		}
		if !result.found {
			base := status.Newf(codes.NotFound, "Key %q was not found", request.Key)
			withDetails, err := base.WithDetails(&ternionv1.KeyNotFound{Key: request.Key})
			if err != nil {
				return nil, base.Err()
			}
			return nil, withDetails.Err()
		}
		return &ternionv1.GetResponse{Value: result.value}, nil
	case <-n.runtimeDone:
		return nil, status.Error(codes.Unavailable, "Node stopped before GET completed")
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
