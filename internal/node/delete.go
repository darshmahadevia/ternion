package node

import (
	"context"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
)

// Delete removes a Key after its sequenced mutation is durably committed and
// applied. A duplicate sequence returns the original existed result.
func (n *Node) Delete(ctx context.Context, request *quorumkvv1.DeleteRequest) (*quorumkvv1.DeleteResponse, error) {
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
	return &quorumkvv1.DeleteResponse{Existed: result.existed}, nil
}
