// vote.go implements the Vote + VoteReply phase that precedes PBFT within each
// cluster. The leader asks replicas whether the proposed state transition should
// be LOCAL (affecting only this cluster) or GLOBAL (all clusters).
//
// Paper Section VII:
//
//	Leader broadcasts Vote to cluster replicas.
//	Replicas respond with VoteReply carrying their decision.
//	Leader tallies: if >= 2f_local+1 replies → decide by majority.
//	GLOBAL → proceed to InterClusterRequest (Section IX).
//	LOCAL  → skip inter-cluster phase, run PBFT directly (Section X/XI).
package protocol

import (
	"fmt"

	"github.com/adk2004/vehicular-bft/messages"
	"github.com/adk2004/vehicular-bft/node"
)

// RunVotePhase tallies a set of VoteReply envelopes collected from cluster
// replicas and returns the decided TransitionType.
//
// Design for testability: callers supply the pre-collected []Envelope slice
// rather than a timeout channel. In production, the caller reads from a
// network.Server.MsgChan until the required count arrives, then calls this.
//
// Tally rules (paper Section VII, plan pitfall #3):
//   - Only distinct senderIDs count (map-dedup; duplicates are silently dropped).
//   - Quorum = 2·f_local + 1 total valid replies.
//   - Majority of GLOBAL votes → decide GLOBAL; otherwise decide LOCAL.
//   - If quorum not reached with the supplied replies, return an error.
//
// The leader authenticates each VoteReply using the known public keys in pbft.
func RunVotePhase(
	pbft *PBFTInstance,
	replies []messages.Envelope,
) (messages.TransitionType, error) {

	if !pbft.Leader.IsLeader() {
		return "", fmt.Errorf("RunVotePhase: node %s is not a leader", pbft.Leader.ID)
	}

	globalCount := 0
	localCount := 0
	seen := make(map[string]bool) // de-duplicate by sender

	for _, env := range replies {
		// Authenticate the sender.
		senderPub, ok := pbft.Leader.KnownKeys[env.SenderID]
		if !ok {
			continue // unknown sender — skip
		}
		if !messages.ValidateEnvelope(env, senderPub) {
			continue // bad signature — skip
		}
		// De-duplicate by sender ID (plan pitfall #3).
		if seen[env.SenderID] {
			continue
		}
		seen[env.SenderID] = true

		var vr messages.VoteReply
		if err := messages.DecodeBody(env, &vr); err != nil {
			continue // malformed body — skip
		}

		switch vr.Transition {
		case messages.GLOBAL:
			globalCount++
		case messages.LOCAL:
			localCount++
		}
	}

	total := globalCount + localCount
	if !node.HasQuorumLocal(total, pbft.FLocal) {
		return "", fmt.Errorf(
			"RunVotePhase: quorum not reached — have %d valid replies, need %d (2f+1, f=%d)",
			total, 2*pbft.FLocal+1, pbft.FLocal,
		)
	}

	// Decide: GLOBAL wins ties (any majority-GLOBAL or equal split → GLOBAL).
	if globalCount >= localCount {
		return messages.GLOBAL, nil
	}
	return messages.LOCAL, nil
}

// BuildVoteReplies is a test helper that has each honest replica in pbft process
// a Vote envelope and return their VoteReply envelopes.
// The leader is excluded (it does not vote for itself in the VoteReply phase).
//
// For Test 3 (injecting LOCAL votes), callers build their own VoteReply envelopes
// directly using messages.NewEnvelope instead of calling this function.
func BuildVoteReplies(pbft *PBFTInstance, voteEnv messages.Envelope) []messages.Envelope {
	voteReplies := make([]messages.Envelope, 0, len(pbft.Replicas))
	for _, r := range pbft.Replicas {
		replyEnv, err := r.HandleVote(voteEnv)
		if err != nil {
			continue // faulty replica or auth failure — skip
		}
		voteReplies = append(voteReplies, replyEnv)
	}
	return voteReplies
}