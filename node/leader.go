// leader.go implements the leader-side message handlers for the vehicular BFT
// protocol. These methods are called on nodes with Role == RoleLeader.
//
// The leader's responsibilities in the protocol:
//  1. Receive IntraClusterRequest from the client.
//  2. Broadcast Vote to cluster replicas.
//  3. Collect VoteReply messages and tally them.
//  4. If GLOBAL: multicast InterClusterRequest to all other cluster leaders,
//     then start PBFT in own cluster.
//  5. If LOCAL: start PBFT directly in own cluster (no inter-cluster phase).
//  6. Drive the PrePrepare → Prepare → Commit sequence within the cluster.
package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/adk2004/vehicular-bft/crypto"
	"github.com/adk2004/vehicular-bft/messages"
)

// ─────────────────────────────────────────────────────────────────────────────
// HandleIntraClusterRequest
// ─────────────────────────────────────────────────────────────────────────────

// HandleIntraClusterRequest processes a client's INTRA-CLUSTER-REQUEST.
//
// Steps (paper Section VII — Vote phase):
//  1. Validate envelope signature using the client's known public key.
//  2. Decode the request.
//  3. Construct a Vote message with the next sequence ID.
//  4. Return a signed Vote envelope to be broadcast to cluster replicas.
//
// The caller (PBFTInstance) is responsible for broadcasting the returned Vote.
func (n *Node) HandleIntraClusterRequest(env messages.Envelope) (messages.Envelope, error) {
	if !n.IsLeader() {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: node %s is not a leader", n.ID)
	}

	// Step 1: authenticate the client.
	n.mu.RLock()
	senderPub, ok := n.KnownKeys[env.SenderID]
	n.mu.RUnlock()
	if !ok {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: unknown sender %q", env.SenderID)
	}
	if !messages.ValidateEnvelope(env, senderPub) {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: invalid signature from %q", env.SenderID)
	}

	// Step 2: decode.
	var req messages.IntraClusterRequest
	if err := messages.DecodeBody(env, &req); err != nil {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: %w", err)
	}

	// Step 3: build the Vote.
	seqID := n.NextSequenceID()

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: marshal request: %w", err)
	}
	digest, err := crypto.Digest(reqBytes)
	if err != nil {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: digest: %w", err)
	}

	n.mu.RLock()
	viewNum := n.ViewNumber
	n.mu.RUnlock()

	vote := messages.Vote{
		ViewNumber: viewNum,
		SequenceID: seqID,
		Digest:     digest,
		Message:    reqBytes,
		Transition: req.Transition,
	}

	// Step 4: sign and return.
	voteEnv, err := messages.NewEnvelope(messages.MsgVote, n.ID, vote, n.PrivKey)
	if err != nil {
		return messages.Envelope{}, fmt.Errorf("node.HandleIntraClusterRequest: sign Vote: %w", err)
	}
	return voteEnv, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// HandleVoteReplies
// ─────────────────────────────────────────────────────────────────────────────

// HandleVoteReplies tallies incoming VoteReply envelopes and returns the
// decided TransitionType once a quorum of 2·f_local+1 is reached.
//
// Per plan pitfall #3: only distinct senders count — duplicate senderIDs are
// silently ignored.
//
// Returns:
//   - (decided TransitionType, next message to send, nil)  when quorum reached.
//   - ("", nil, nil)                                        when quorum not yet reached.
//   - ("", nil, err)                                        on validation failure.
//
// The "next message" is:
//   - An InterClusterRequest envelope  → if decided GLOBAL.
//   - A PrePrepare envelope            → if decided LOCAL (skip inter-cluster phase).
func (n *Node) HandleVoteReplies(
	envs []messages.Envelope,
	fLocal int,
	operation string,
	clientID string,
) (messages.TransitionType, *messages.Envelope, error) {

	if !n.IsLeader() {
		return "", nil, fmt.Errorf("node.HandleVoteReplies: node %s is not a leader", n.ID)
	}

	globalVotes := 0
	localVotes := 0

	for _, env := range envs {
		// Authenticate.
		n.mu.RLock()
		senderPub, ok := n.KnownKeys[env.SenderID]
		n.mu.RUnlock()
		if !ok {
			continue // unknown sender — skip
		}
		if !messages.ValidateEnvelope(env, senderPub) {
			continue // bad signature — skip
		}

		// Decode.
		var vr messages.VoteReply
		if err := messages.DecodeBody(env, &vr); err != nil {
			continue
		}

		// De-duplicate by sender ID (plan pitfall #3).
		count := n.AddVoteReply(env.SenderID, vr)
		_ = count // we tally ourselves below

		switch vr.Transition {
		case messages.GLOBAL:
			globalVotes++
		case messages.LOCAL:
			localVotes++
		}
	}

	total := globalVotes + localVotes
	if !HasQuorumLocal(total, fLocal) {
		return "", nil, nil // wait for more replies
	}

	// Decide by majority.
	if globalVotes >= localVotes {
		// GLOBAL path (paper Section IX Step 6):
		// Build InterClusterRequest to multicast to all other leaders.
		icr := messages.InterClusterRequest{
			Operation:     operation,
			Timestamp:     time.Now().UnixNano(),
			ClientID:      clientID,
			Transition:    messages.GLOBAL,
			OriginCluster: n.ClusterIdx,
		}
		icrEnv, err := messages.NewEnvelope(messages.MsgInterClusterRequest, n.ID, icr, n.PrivKey)
		if err != nil {
			return "", nil, fmt.Errorf("node.HandleVoteReplies: sign InterClusterRequest: %w", err)
		}
		n.ClearVoteReplies()
		return messages.GLOBAL, &icrEnv, nil
	}

	// LOCAL path (paper Section X Step 5):
	// Build a PrePrepare to start intra-cluster PBFT with the local operation.
	ppEnv, err := n.StartPBFT(operation)
	if err != nil {
		return "", nil, fmt.Errorf("node.HandleVoteReplies: start local PBFT: %w", err)
	}
	n.ClearVoteReplies()
	return messages.LOCAL, ppEnv, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// StartPBFT
// ─────────────────────────────────────────────────────────────────────────────

// StartPBFT constructs and returns a signed PrePrepare message for `operation`,
// advancing the leader's SequenceID.
//
// Called by:
//   - HandleVoteReplies when decided LOCAL.
//   - HandleInterClusterRequest at every cluster leader during the GLOBAL path.
//   - RunLocalTransition (Phase 8) which bypasses the Vote phase entirely.
//
// The Message field in PrePrepare is a JSON object {"operation": "..."} so that
// replicas can reconstruct the operation string from the committed log entry
// (see OperationFromPrePrepareMessage in replica.go).
func (n *Node) StartPBFT(operation string) (*messages.Envelope, error) {
	if !n.IsLeader() {
		return nil, fmt.Errorf("node.StartPBFT: node %s is not a leader", n.ID)
	}

	// Encode the operation into the PrePrepare Message field.
	msgPayload := map[string]string{"operation": operation}
	msgBytes, err := json.Marshal(msgPayload)
	if err != nil {
		return nil, fmt.Errorf("node.StartPBFT: marshal operation: %w", err)
	}

	digest, err := crypto.Digest(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("node.StartPBFT: digest: %w", err)
	}

	seqID := n.NextSequenceID()

	n.mu.RLock()
	viewNum := n.ViewNumber
	n.mu.RUnlock()

	pp := messages.PrePrepare{
		ViewNumber: viewNum,
		SequenceID: seqID,
		Digest:     digest,
		Message:    msgBytes,
	}

	ppEnv, err := messages.NewEnvelope(messages.MsgPrePrepare, n.ID, pp, n.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("node.StartPBFT: sign PrePrepare: %w", err)
	}
	return &ppEnv, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// HandleInterClusterRequest
// ─────────────────────────────────────────────────────────────────────────────

// HandleInterClusterRequest is called on a cluster leader when it receives an
// INTER-CLUSTER-REQUEST from the initiating cluster's leader.
//
// Steps (paper Section IX Step 7):
//  1. Validate the envelope signature.
//  2. Decode the InterClusterRequest.
//  3. Immediately start the PBFT process in own cluster by calling StartPBFT.
//  4. Return the resulting PrePrepare envelope.
func (n *Node) HandleInterClusterRequest(env messages.Envelope) (*messages.Envelope, error) {
	if !n.IsLeader() {
		return nil, fmt.Errorf("node.HandleInterClusterRequest: node %s is not a leader", n.ID)
	}

	// Step 1: authenticate the originating leader.
	n.mu.RLock()
	senderPub, ok := n.KnownKeys[env.SenderID]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("node.HandleInterClusterRequest: unknown sender %q", env.SenderID)
	}
	if !messages.ValidateEnvelope(env, senderPub) {
		return nil, fmt.Errorf("node.HandleInterClusterRequest: invalid signature from %q", env.SenderID)
	}

	// Step 2: decode.
	var icr messages.InterClusterRequest
	if err := messages.DecodeBody(env, &icr); err != nil {
		return nil, fmt.Errorf("node.HandleInterClusterRequest: %w", err)
	}

	// Step 3 & 4: kick off PBFT in own cluster.
	ppEnv, err := n.StartPBFT(icr.Operation)
	if err != nil {
		return nil, fmt.Errorf("node.HandleInterClusterRequest: StartPBFT: %w", err)
	}
	return ppEnv, nil
}
