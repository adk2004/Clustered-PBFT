// intra_cluster.go implements the two local-transition scenarios from the paper.
//
// Scenario A — Pure local (paper Section XI):
//	Client proposes LOCAL → leader skips Vote/VoteReply → PBFT directly.
//
// Scenario B — Proposed GLOBAL, voted LOCAL (paper Section X):
//	Client proposes GLOBAL → replicas vote → majority LOCAL →
//	leader rewrites operation to local variant → PBFT.
//
// Both scenarios terminate with a RunPBFT call on the cluster's own PBFTInstance.
package protocol

import (
	"context"
	"fmt"

	"github.com/adk2004/vehicular-bft/messages"
)

// RunLocalTransition executes a purely local state transition (paper Section XI).
//
// The vote and vote-reply phases are bypassed entirely:
//
//	"When a leader of a cluster receives an intra-cluster request proposing
//	 a local state transition, the protocol simplifies the process by bypassing
//	 the vote and vote-reply phases. Instead, the leader directly sends a
//	 pre-prepare message to all nodes within its cluster."
//
// Parameters:
//
//	pbft      — the cluster's PBFTInstance (Leader + honest Replicas).
//	operation — the operation string to commit.
//	clientID  — the client's ID (used in Reply messages).
//
// Returns the set of Reply envelopes produced by honest nodes that reached
// commit quorum.
func RunLocalTransition(
	ctx context.Context,
	pbft *PBFTInstance,
	operation, clientID string,
) ([]messages.Reply, error) {

	if !pbft.Leader.IsLeader() {
		return nil, fmt.Errorf("RunLocalTransition: node %s is not a leader", pbft.Leader.ID)
	}

	replies, err := pbft.RunPBFT(ctx, operation, clientID)
	if err != nil {
		return nil, fmt.Errorf("RunLocalTransition: PBFT failed: %w", err)
	}
	return replies, nil
}

// RunProposedGlobalButLocal handles the scenario where a client proposed a
// GLOBAL transition but the cluster voted LOCAL (paper Section X).
//
// Steps:
//  1. The vote phase has already decided LOCAL (caller's responsibility).
//  2. The leader constructs a "local variant" of the operation by appending
//     ":local" — in a real system this would be a semantic rewrite; the suffix
//     makes the test assertable.
//  3. The leader runs intra-cluster PBFT with the rewritten operation.
//
// In practice the semantic rewrite depends on the application layer; here we
// adopt the convention from plan Section XI: "the leader constructs a new
// operation o' that represents the local version of the proposed global transition."
func RunProposedGlobalButLocal(
	ctx context.Context,
	pbft *PBFTInstance,
	originalOperation, clientID string,
) ([]messages.Reply, error) {

	if !pbft.Leader.IsLeader() {
		return nil, fmt.Errorf("RunProposedGlobalButLocal: node %s is not a leader", pbft.Leader.ID)
	}

	// Rewrite: append ":local" to signal this is a down-scoped local variant.
	localOperation := originalOperation + ":local"

	replies, err := pbft.RunPBFT(ctx, localOperation, clientID)
	if err != nil {
		return nil, fmt.Errorf("RunProposedGlobalButLocal: PBFT failed: %w", err)
	}
	return replies, nil
}