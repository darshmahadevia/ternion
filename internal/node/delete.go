package node

import (
	"context"

	ternionv1 "github.com/darshmahadevia/ternion/gen/ternion/v1"
	"github.com/darshmahadevia/ternion/internal/raft"
)

// Delete removes a Key after its sequenced mutation is durably committed and
// applied. A duplicate sequence returns the original existed result.
func (n *Node) Delete(ctx context.Context, request *ternionv1.DeleteRequest) (*ternionv1.DeleteResponse, error) {
	if err := validateMutation(request.SessionId, request.Sequence, request.Key); err != nil {
		return nil, err
	}
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	var sessionID raft.SessionID
	copy(sessionID[:], request.SessionId)
	result, err := n.propose(ctx, raft.ProposeDelete{
		ProposalID: raft.ProposalID(n.nextProposal.Add(1)),
		SessionID:  sessionID,
		Sequence:   request.Sequence,
		Key:        request.Key,
	})
	if err != nil {
		return nil, err
	}
	if err := n.proposalError(result); err != nil {
		return nil, err
	}
	return &ternionv1.DeleteResponse{Existed: result.existed}, nil
}
