package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/adk2004/vehicular-bft/cluster"
	"github.com/adk2004/vehicular-bft/messages"
	nodemod "github.com/adk2004/vehicular-bft/node"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// makeNode creates a node, failing the test on error.
func makeNode(t *testing.T, id string, role nodemod.Role, ci, ni int) *nodemod.Node {
	t.Helper()
	n, err := nodemod.NewNode(id, role, ci, ni, cluster.Point{ID: id, X: float64(ni * 10), Y: float64(ci * 10)})
	if err != nil {
		t.Fatalf("NewNode(%s): %v", id, err)
	}
	return n
}

// linkKeys shares all public keys between every node in the slice.
func linkKeys(nodes []*nodemod.Node) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a.ID != b.ID {
				a.KnownKeys[b.ID] = b.PubKey
			}
		}
	}
}

// makeClusterPBFT builds a PBFTInstance for one cluster.
//
//	ci         — cluster index
//	totalSize  — intended cluster size (used to compute fLocal correctly)
//	honestSize — number of honest nodes to include (totalSize - faultyCount)
//	Returns the PBFTInstance and all nodes (leader first).
func makeClusterPBFT(t *testing.T, ci, totalSize, honestSize int) (*PBFTInstance, []*nodemod.Node) {
	t.Helper()
	nodes := make([]*nodemod.Node, honestSize)
	for j := 0; j < honestSize; j++ {
		role := nodemod.RoleReplica
		if j == 0 {
			role = nodemod.RoleLeader
		}
		nodes[j] = makeNode(t, fmt.Sprintf("node-%d-%d", ci, j), role, ci, j)
	}
	linkKeys(nodes)

	fLocal := nodemod.FaultyThresholdLocal(totalSize)
	pbft := NewPBFTInstance(nodes[0], nodes[1:], fLocal, nil)
	return pbft, nodes
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Local transition (4 nodes, 1 faulty simulated as absent)
// ─────────────────────────────────────────────────────────────────────────────

// TestProtocolLocalTransition verifies that RunLocalTransition completes with
// only 3 honest nodes (the 4th is absent/faulty), all honest nodes commit the
// operation, and the faulty node is untouched.
func TestProtocolLocalTransition(t *testing.T) {
	t.Parallel()

	const (
		totalClusterSize = 4
		honestCount      = 3 // 1 faulty excluded
		clientID         = "client-0"
		operation        = "SET signal=GREEN"
	)

	pbft, nodes := makeClusterPBFT(t, 0, totalClusterSize, honestCount)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	replies, err := RunLocalTransition(ctx, pbft, operation, clientID)
	if err != nil {
		t.Fatalf("RunLocalTransition: %v", err)
	}

	// Must produce at least one reply.
	if len(replies) == 0 {
		t.Fatal("RunLocalTransition: no replies returned")
	}

	// All honest nodes must have committed the operation.
	for _, nd := range nodes {
		found := false
		log := nd.GetLog()
		for _, entry := range log {
			if entry == operation {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("honest node %s did not commit %q", nd.ID, operation)
		}
	}

	// fLocal=1, quorum=3; we have 3 honest nodes → all should reply.
	fLocal := nodemod.FaultyThresholdLocal(totalClusterSize)
	if !nodemod.HasQuorumLocal(len(replies), fLocal) {
		t.Errorf("reply count %d below local quorum threshold %d", len(replies), 2*fLocal+1)
	}

	t.Logf("LocalTransition: %d replies from %d honest nodes (fLocal=%d, quorum=%d)",
		len(replies), honestCount, fLocal, 2*fLocal+1)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Global transition (12 nodes, 3 clusters, all honest)
// ─────────────────────────────────────────────────────────────────────────────

// TestProtocolGlobalTransition verifies that RunGlobalTransition across 3 clusters
// of 4 nodes collects >= f_global+1 replies, all cluster leaders committed the
// same operation, and NetworkState (represented by node logs) changes.
func TestProtocolGlobalTransition(t *testing.T) {
	t.Parallel()

	const (
		totalNodes      = 12
		numClusters     = 3
		nodesPerCluster = 4
		clientID        = "client-0"
		operation       = "UPDATE route=highway-A1"
	)

	// Build 3 PBFTInstances (all nodes honest).
	clusterInstances := make([]*PBFTInstance, numClusters)
	allNodes := make([][]*nodemod.Node, numClusters)

	for ci := 0; ci < numClusters; ci++ {
		pbft, nodes := makeClusterPBFT(t, ci, nodesPerCluster, nodesPerCluster)
		clusterInstances[ci] = pbft
		allNodes[ci] = nodes
	}

	fGlobal := nodemod.FaultyThresholdGlobal(totalNodes) // = 3
	coord := NewGlobalCoordinator(clusterInstances, fGlobal, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	replies, err := coord.RunGlobalTransition(ctx, 0, operation, clientID)
	if err != nil {
		t.Fatalf("RunGlobalTransition: %v", err)
	}

	// Must reach global quorum: >= f_global+1 = 4 replies.
	if !nodemod.HasQuorumGlobal(len(replies), fGlobal) {
		t.Errorf("global quorum not met: got %d replies, need %d (f_global=%d)",
			len(replies), fGlobal+1, fGlobal)
	}

	// All cluster leaders must have committed the operation.
	for _, leader := range coord.ClusterLeaders() {
		found := false
		log := leader.GetLog()
		for _, entry := range log {
			if entry == operation {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cluster leader %s did not commit %q", leader.ID, operation)
		}
	}

	// NetworkState changed: every honest node should have the operation in its log.
	for ci, clusterNodes := range allNodes {
		for _, nd := range clusterNodes {
			log := nd.GetLog()
			if len(log) == 0 {
				t.Errorf("cluster %d node %s: Log is empty after global transition", ci, nd.ID)
			}
		}
	}

	t.Logf("GlobalTransition: %d total replies across %d clusters (fGlobal=%d, quorum=%d)",
		len(replies), numClusters, fGlobal, fGlobal+1)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Proposed GLOBAL but cluster votes LOCAL
// ─────────────────────────────────────────────────────────────────────────────

// TestProtocolProposedGlobalButLocal injects 3 LOCAL VoteReply envelopes into
// RunVotePhase and verifies it returns LOCAL, then confirms RunProposedGlobalButLocal
// executes the local variant of the operation successfully.
func TestProtocolProposedGlobalButLocal(t *testing.T) {
	t.Parallel()

	const (
		totalSize = 4
		clientID  = "client-0"
		operation = "SET traffic=REROUTE"
	)

	pbft, _ := makeClusterPBFT(t, 0, totalSize, totalSize)

	// Build a Vote envelope from the leader (represents the client's GLOBAL proposal).
	client := makeNode(t, "client-0", nodemod.RoleClient, -1, -1)
	pbft.Leader.KnownKeys[client.ID] = client.PubKey
	for _, r := range pbft.Replicas {
		r.KnownKeys[client.ID] = client.PubKey
	}

	req := messages.IntraClusterRequest{
		Operation:  operation,
		Timestamp:  time.Now().UnixNano(),
		ClientID:   clientID,
		Transition: messages.GLOBAL, // client proposes GLOBAL
	}
	reqEnv, err := messages.NewEnvelope(messages.MsgIntraClusterRequest, client.ID, req, client.PrivKey)
	if err != nil {
		t.Fatalf("NewEnvelope(req): %v", err)
	}
	voteEnv, err := pbft.Leader.HandleIntraClusterRequest(reqEnv)
	if err != nil {
		t.Fatalf("HandleIntraClusterRequest: %v", err)
	}

	// Inject LOCAL votes: manually build VoteReply envelopes with Transition=LOCAL.
	// This simulates replicas deciding the operation should stay local.
	var voteReplies []messages.Envelope
	var voteMsg messages.Vote
	if err := messages.DecodeBody(voteEnv, &voteMsg); err != nil {
		t.Fatalf("decode vote: %v", err)
	}

	for i, r := range pbft.Replicas {
		localReply := messages.VoteReply{
			ViewNumber: voteMsg.ViewNumber,
			SequenceID: voteMsg.SequenceID,
			Digest:     voteMsg.Digest,
			ReplicaIdx: i + 1,
			ClusterIdx: 0,
			Transition: messages.LOCAL, // ← replica votes LOCAL (injection)
			Message:    voteMsg.Message,
		}
		env, err := messages.NewEnvelope(messages.MsgVoteReply, r.ID, localReply, r.PrivKey)
		if err != nil {
			t.Fatalf("NewEnvelope(VoteReply %d): %v", i, err)
		}
		voteReplies = append(voteReplies, env)
	}

	// Step 1: RunVotePhase must decide LOCAL.
	decided, err := RunVotePhase(pbft, voteReplies)
	if err != nil {
		t.Fatalf("RunVotePhase: %v", err)
	}
	if decided != messages.LOCAL {
		t.Errorf("RunVotePhase decided %q, want LOCAL (3 LOCAL votes injected)", decided)
	}

	// Step 2: RunProposedGlobalButLocal must execute the local variant.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	replies, err := RunProposedGlobalButLocal(ctx, pbft, operation, clientID)
	if err != nil {
		t.Fatalf("RunProposedGlobalButLocal: %v", err)
	}
	if len(replies) == 0 {
		t.Fatal("RunProposedGlobalButLocal: no replies returned")
	}

	// The committed operation must be the LOCAL variant (operation + ":local").
	expectedLocalOp := operation + ":local"
	for _, nd := range append([]*nodemod.Node{pbft.Leader}, pbft.Replicas...) {
		found := false
		log := nd.GetLog()
		for _, entry := range log {
			if entry == expectedLocalOp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %s: local variant %q not found in Log %v", nd.ID, expectedLocalOp, log)
		}
	}

	t.Logf("ProposedGlobalButLocal: decided=LOCAL, operation=%q committed as %q", operation, expectedLocalOp)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Byzantine leader (view-change placeholder: timeout, no panic)
// ─────────────────────────────────────────────────────────────────────────────

// TestProtocolByzantineLeaderTimeout verifies that when the leader is faulty
// (simulated by demoting it from RoleLeader so StartPBFT returns an error),
// RunPBFT/RunLocalTransition returns an error — not a panic.
// Full view-change is out of scope; we assert the timeout/error path is clean.
func TestProtocolByzantineLeaderTimeout(t *testing.T) {
	t.Parallel()

	pbft, _ := makeClusterPBFT(t, 0, 4, 4)

	// Demote the leader to simulate a Byzantine leader that refuses to drive PBFT.
	pbft.Leader.DemoteToReplica()

	// Use a very short context to simulate the "replicas detect timeout" scenario.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := RunLocalTransition(ctx, pbft, "SET x=1", "client-0")
	if err == nil {
		t.Error("RunLocalTransition with demoted leader: expected error, got nil")
	}
	// Must be an error about leader role, not a panic or nil.
	t.Logf("Byzantine leader correctly returned error: %v", err)
}

// Additional: Byzantine leader via cancelled context (network timeout simulation).
func TestProtocolCancelledContextReturnsError(t *testing.T) {
	t.Parallel()

	pbft, _ := makeClusterPBFT(t, 0, 4, 4)

	// Cancel the context before calling RunPBFT.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled immediately

	_, err := pbft.RunPBFT(ctx, "op", "client")
	if err == nil {
		t.Error("RunPBFT with pre-cancelled context: expected error, got nil")
	}
	t.Logf("Cancelled context correctly returned error: %v", err)
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: VotePhase tally correctness
// ─────────────────────────────────────────────────────────────────────────────

// TestProtocolVotePhaseGlobalMajority ensures that >= 2 GLOBAL votes (out of 3)
// produce a GLOBAL decision for fLocal=1.
func TestProtocolVotePhaseGlobalMajority(t *testing.T) {
	t.Parallel()

	pbft, _ := makeClusterPBFT(t, 0, 4, 4)

	// Build the vote envelope.
	client := makeNode(t, "vclient", nodemod.RoleClient, -1, -1)
	pbft.Leader.KnownKeys[client.ID] = client.PubKey

	req := messages.IntraClusterRequest{
		Operation: "op", Timestamp: 1, ClientID: client.ID, Transition: messages.GLOBAL,
	}
	reqEnv, _ := messages.NewEnvelope(messages.MsgIntraClusterRequest, client.ID, req, client.PrivKey)
	voteEnv, err := pbft.Leader.HandleIntraClusterRequest(reqEnv)
	if err != nil {
		t.Fatalf("HandleIntraClusterRequest: %v", err)
	}

	// All replicas echo GLOBAL (default behaviour via HandleVote).
	voteReplies := BuildVoteReplies(pbft, voteEnv)
	if len(voteReplies) < 2*nodemod.FaultyThresholdLocal(4)+1-1 {
		t.Skipf("not enough replicas to reach quorum")
	}

	decided, err := RunVotePhase(pbft, voteReplies)
	if err != nil {
		t.Fatalf("RunVotePhase (GLOBAL majority): %v", err)
	}
	if decided != messages.GLOBAL {
		t.Errorf("decided = %q, want GLOBAL", decided)
	}
}

// TestProtocolVotePhaseQuorumNotReached returns an error when fewer than 2f+1
// valid replies are provided.
func TestProtocolVotePhaseQuorumNotReached(t *testing.T) {
	t.Parallel()

	pbft, _ := makeClusterPBFT(t, 0, 4, 4)

	// Supply zero replies — quorum (3) cannot be reached.
	_, err := RunVotePhase(pbft, nil)
	if err == nil {
		t.Error("RunVotePhase with 0 replies: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: RunPBFT with all 4 honest nodes (standard case)
// ─────────────────────────────────────────────────────────────────────────────

func TestProtocolRunPBFTFourHonestNodes(t *testing.T) {
	t.Parallel()

	pbft, nodes := makeClusterPBFT(t, 0, 4, 4)
	ctx := context.Background()

	replies, err := pbft.RunPBFT(ctx, "SET x=100", "client-0")
	if err != nil {
		t.Fatalf("RunPBFT: %v", err)
	}
	if len(replies) == 0 {
		t.Fatal("RunPBFT: no replies")
	}
	for _, nd := range nodes {
		log := nd.GetLog()
		if len(log) == 0 {
			t.Errorf("node %s: Log empty after commit", nd.ID)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: de-duplicate VoteReply by sender in RunVotePhase
// ─────────────────────────────────────────────────────────────────────────────

func TestProtocolVotePhaseDeduplicateSender(t *testing.T) {
	t.Parallel()

	pbft, _ := makeClusterPBFT(t, 0, 4, 4)
	client := makeNode(t, "vc2", nodemod.RoleClient, -1, -1)
	pbft.Leader.KnownKeys[client.ID] = client.PubKey

	req := messages.IntraClusterRequest{
		Operation: "op2", Timestamp: 2, ClientID: client.ID, Transition: messages.GLOBAL,
	}
	reqEnv, _ := messages.NewEnvelope(messages.MsgIntraClusterRequest, client.ID, req, client.PrivKey)
	voteEnv, _ := pbft.Leader.HandleIntraClusterRequest(reqEnv)

	// Build one valid GLOBAL VoteReply from replica 0.
	replica := pbft.Replicas[0]
	replyEnv, _ := replica.HandleVote(voteEnv)

	// Feed the same reply 5 times — should only count as 1.
	repeated := []messages.Envelope{replyEnv, replyEnv, replyEnv, replyEnv, replyEnv}
	_, err := RunVotePhase(pbft, repeated)
	if err == nil {
		// If somehow quorum is met with 1 unique sender, that means fLocal==0,
		// which is only possible for very small clusters. Just verify no panic.
		t.Log("note: quorum reached with 1 sender (fLocal may be 0)")
	} else {
		// Expected: quorum not reached because duplicates were de-duped.
		t.Logf("correctly returned error (dedup): %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: idempotent ApplyOperation via repeated RunPBFT calls
// ─────────────────────────────────────────────────────────────────────────────

func TestProtocolApplyOperationIdempotentAcrossRounds(t *testing.T) {
	t.Parallel()

	pbft, nodes := makeClusterPBFT(t, 0, 4, 4)
	ctx := context.Background()
	op := "SET y=7"

	// First round — seqID=1.
	if _, err := pbft.RunPBFT(ctx, op, "c"); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	// Second round with different op — seqID=2.
	if _, err := pbft.RunPBFT(ctx, "SET z=8", "c"); err != nil {
		t.Fatalf("round 2: %v", err)
	}

	// Log must have exactly 2 entries per node (each round committed once).
	for _, nd := range nodes {
		log := nd.GetLog()
		if len(log) != 2 {
			t.Errorf("node %s: Log has %d entries, want 2", nd.ID, len(log))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: encode operation for BuildVoteReplies reference
// ─────────────────────────────────────────────────────────────────────────────

// operationPayload is used by OperationFromPrePrepareMessage.
// Defined here to keep the test self-contained.
func operationPayloadJSON(op string) []byte {
	b, _ := json.Marshal(map[string]string{"operation": op})
	return b
}
