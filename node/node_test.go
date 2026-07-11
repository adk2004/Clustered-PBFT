package node

import (
	"fmt"
	"testing"

	"github.com/adk2004/vehicular-bft/cluster"
	"github.com/adk2004/vehicular-bft/messages"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

// makeNode creates a node, failing the test on error.
func makeNode(t *testing.T, id string, role Role, ci, ni int) *Node {
	t.Helper()
	n, err := NewNode(id, role, ci, ni, cluster.Point{ID: id, X: float64(ni * 10), Y: float64(ci * 10)})
	if err != nil {
		t.Fatalf("NewNode(%s): %v", id, err)
	}
	return n
}

// linkKeys shares public keys between all nodes in the slice (simulates NodeCA).
func linkKeys(nodes []*Node) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a.ID != b.ID {
				a.KnownKeys[b.ID] = b.PubKey
			}
		}
	}
}

// makeCluster creates m*n nodes, elects index 0 as leader, and links all keys.
func makeCluster(t *testing.T, clusterIdx, n int) (*Node, []*Node) {
	t.Helper()
	all := make([]*Node, n)
	for j := 0; j < n; j++ {
		role := RoleReplica
		if j == 0 {
			role = RoleLeader
		}
		all[j] = makeNode(t, fmt.Sprintf("node-%d-%d", clusterIdx, j), role, clusterIdx, j)
	}
	linkKeys(all)
	return all[0], all[1:]
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — NewNode generates distinct key pairs for two nodes
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeNewNodeDistinctKeyPairs(t *testing.T) {
	t.Parallel()

	n1 := makeNode(t, "node-0-0", RoleLeader, 0, 0)
	n2 := makeNode(t, "node-0-1", RoleReplica, 0, 1)

	if n1.PubKey == nil || n2.PubKey == nil {
		t.Fatal("one or both public keys are nil")
	}
	if n1.PrivKey == nil || n2.PrivKey == nil {
		t.Fatal("one or both private keys are nil")
	}

	// Moduli must differ (otherwise keys are identical, which is a crypto failure).
	if n1.PubKey.N.Cmp(n2.PubKey.N) == 0 {
		t.Error("two distinct nodes share the same RSA modulus — key generation is broken")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — FaultyThresholdLocal: n=4→1, n=7→2, n=10→3
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeFaultyThresholdLocal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		n, want int
	}{
		{4, 1},  // plan required case
		{7, 2},  // plan required case
		{10, 3}, // plan required case
		{1, 0},  // edge: single node
		{3, 0},  // 3f+1=1 → f=0
		{13, 4}, // 13 nodes → f=4
		{0, 0},  // edge: zero
	}

	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("n=%d", tc.n), func(t *testing.T) {
			t.Parallel()
			got := FaultyThresholdLocal(tc.n)
			if got != tc.want {
				t.Errorf("FaultyThresholdLocal(%d) = %d, want %d", tc.n, got, tc.want)
			}
		})
	}
}

func TestNodeFaultyThresholdGlobal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		p, want int
	}{
		{12, 3}, // 3 clusters × 4 nodes, f_global=3
		{4, 1},
		{1, 0},
		{0, 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("p=%d", tc.p), func(t *testing.T) {
			t.Parallel()
			got := FaultyThresholdGlobal(tc.p)
			if got != tc.want {
				t.Errorf("FaultyThresholdGlobal(%d) = %d, want %d", tc.p, got, tc.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — HasQuorumLocal boundary: 2f+1 passes, 2f fails
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeHasQuorumLocalBoundary(t *testing.T) {
	t.Parallel()

	fLocal := 1 // 4-node cluster (MINSIZE)

	// Exactly at quorum threshold (2*1+1 = 3): must return true.
	if !HasQuorumLocal(3, fLocal) {
		t.Error("HasQuorumLocal(3, 1) = false, want true (2f+1 = 3)")
	}
	// One below threshold (2*1 = 2): must return false.
	if HasQuorumLocal(2, fLocal) {
		t.Error("HasQuorumLocal(2, 1) = true, want false (below quorum)")
	}
	// Above threshold: must return true.
	if !HasQuorumLocal(4, fLocal) {
		t.Error("HasQuorumLocal(4, 1) = false, want true")
	}
	// Zero: must return false.
	if HasQuorumLocal(0, fLocal) {
		t.Error("HasQuorumLocal(0, 1) = true, want false")
	}
}

func TestNodeHasQuorumGlobalBoundary(t *testing.T) {
	t.Parallel()

	fGlobal := 3 // 12-node network

	// Exactly at threshold (f+1 = 4): must return true.
	if !HasQuorumGlobal(4, fGlobal) {
		t.Error("HasQuorumGlobal(4, 3) = false, want true (f+1 = 4)")
	}
	// One below (f = 3): must return false.
	if HasQuorumGlobal(3, fGlobal) {
		t.Error("HasQuorumGlobal(3, 3) = true, want false (below f+1)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — ApplyOperation is idempotent for duplicate seqIDs
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeApplyOperationIdempotent(t *testing.T) {
	t.Parallel()

	n := makeNode(t, "node-0-0", RoleLeader, 0, 0)

	n.ApplyOperation(1, "SET signal=RED")
	n.ApplyOperation(1, "SET signal=RED") // duplicate — must be no-op
	n.ApplyOperation(1, "SET signal=RED") // third call — still no-op

	log := n.GetLog()
	if len(log) != 1 {
		t.Errorf("Log length = %d after 3 duplicate ApplyOperation calls, want 1", len(log))
	}
	if log[0] != "SET signal=RED" {
		t.Errorf("Log[0] = %q, want %q", log[0], "SET signal=RED")
	}
}

// Different seqIDs must each appear exactly once in the log.
func TestNodeApplyOperationDistinctSeqIDs(t *testing.T) {
	t.Parallel()

	n := makeNode(t, "node-0-1", RoleReplica, 0, 1)

	ops := []struct {
		seq int
		op  string
	}{
		{1, "SET a=1"},
		{2, "SET b=2"},
		{3, "SET c=3"},
		{2, "SET b=2"}, // duplicate seqID — must be ignored
		{1, "SET a=1"}, // duplicate seqID — must be ignored
	}

	for _, o := range ops {
		n.ApplyOperation(o.seq, o.op)
	}

	log := n.GetLog()
	if len(log) != 3 {
		t.Errorf("Log length = %d, want 3 (duplicates must be idempotent)", len(log))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Only RoleLeader can call StartPBFT; RoleReplica must get an error
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeStartPBFTLeaderOnly(t *testing.T) {
	t.Parallel()

	leader := makeNode(t, "node-0-0", RoleLeader, 0, 0)
	replica := makeNode(t, "node-0-1", RoleReplica, 0, 1)

	// Leader should succeed.
	ppEnv, err := leader.StartPBFT("SET x=1")
	if err != nil {
		t.Errorf("leader.StartPBFT: unexpected error: %v", err)
	}
	if ppEnv == nil {
		t.Error("leader.StartPBFT: returned nil envelope")
	}
	if ppEnv != nil && ppEnv.Type != messages.MsgPrePrepare {
		t.Errorf("leader.StartPBFT: env.Type = %q, want MsgPrePrepare", ppEnv.Type)
	}

	// Replica must return an error, not panic.
	ppEnvR, errR := replica.StartPBFT("SET x=1")
	if errR == nil {
		t.Error("replica.StartPBFT: expected error, got nil")
	}
	if ppEnvR != nil {
		t.Error("replica.StartPBFT: expected nil envelope on error, got non-nil")
	}
}

// HandleIntraClusterRequest must also fail when called on a replica.
func TestNodeHandleIntraClusterRequestLeaderOnly(t *testing.T) {
	t.Parallel()

	replica := makeNode(t, "node-0-1", RoleReplica, 0, 1)
	client := makeNode(t, "client-0", RoleClient, -1, -1)
	replica.KnownKeys[client.ID] = client.PubKey

	req := messages.IntraClusterRequest{
		Operation:  "op",
		Timestamp:  1,
		ClientID:   client.ID,
		Transition: messages.LOCAL,
	}
	env, _ := messages.NewEnvelope(messages.MsgIntraClusterRequest, client.ID, req, client.PrivKey)

	_, err := replica.HandleIntraClusterRequest(env)
	if err == nil {
		t.Error("replica.HandleIntraClusterRequest: expected error for non-leader, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HandleVote / VoteReply round-trip
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeHandleVoteReplyRoundTrip(t *testing.T) {
	t.Parallel()

	leader, replicas := makeCluster(t, 0, 4)
	fLocal := FaultyThresholdLocal(4) // = 1

	// Leader receives a client request and produces a Vote.
	client := makeNode(t, "client-0", RoleClient, -1, -1)
	leader.KnownKeys[client.ID] = client.PubKey
	for _, r := range replicas {
		r.KnownKeys[client.ID] = client.PubKey
	}

	req := messages.IntraClusterRequest{
		Operation:  "SET signal=GREEN",
		Timestamp:  12345,
		ClientID:   client.ID,
		Transition: messages.GLOBAL,
	}
	reqEnv, err := messages.NewEnvelope(messages.MsgIntraClusterRequest, client.ID, req, client.PrivKey)
	if err != nil {
		t.Fatalf("NewEnvelope(request): %v", err)
	}

	voteEnv, err := leader.HandleIntraClusterRequest(reqEnv)
	if err != nil {
		t.Fatalf("HandleIntraClusterRequest: %v", err)
	}
	if voteEnv.Type != messages.MsgVote {
		t.Fatalf("expected MsgVote, got %s", voteEnv.Type)
	}

	// All replicas process the Vote and return VoteReply envelopes.
	voteReplies := make([]messages.Envelope, len(replicas))
	for i, r := range replicas {
		replyEnv, err := r.HandleVote(voteEnv)
		if err != nil {
			t.Fatalf("replica %s HandleVote: %v", r.ID, err)
		}
		if replyEnv.Type != messages.MsgVoteReply {
			t.Fatalf("expected MsgVoteReply from %s, got %s", r.ID, replyEnv.Type)
		}
		voteReplies[i] = replyEnv
	}

	// Leader tallies VoteReplies — should reach quorum (3 replicas > 2f+1=3).
	decided, nextEnv, err := leader.HandleVoteReplies(voteReplies, fLocal, req.Operation, client.ID)
	if err != nil {
		t.Fatalf("HandleVoteReplies: %v", err)
	}
	if decided == "" {
		t.Fatal("HandleVoteReplies: quorum not reached (got empty TransitionType)")
	}
	if nextEnv == nil {
		t.Fatal("HandleVoteReplies: returned nil next envelope after quorum")
	}
	// All replicas voted GLOBAL → decided must be GLOBAL → InterClusterRequest.
	if decided != messages.GLOBAL {
		t.Errorf("decided = %s, want GLOBAL (all replicas echoed GLOBAL)", decided)
	}
	if nextEnv.Type != messages.MsgInterClusterRequest {
		t.Errorf("next env type = %s, want MsgInterClusterRequest", nextEnv.Type)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Full intra-cluster PBFT flow: PrePrepare → Prepare → Commit → Reply
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeIntraClusterPBFTFlow(t *testing.T) {
	t.Parallel()

	const clusterSize = 4
	leader, replicas := makeCluster(t, 0, clusterSize)
	allNodes := append([]*Node{leader}, replicas...)
	fLocal := FaultyThresholdLocal(clusterSize) // = 1

	operation := "SET route=highway-A1"
	clientID := "client-0"

	// Step 1 — Leader starts PBFT.
	ppEnvPtr, err := leader.StartPBFT(operation)
	if err != nil {
		t.Fatalf("StartPBFT: %v", err)
	}
	ppEnv := *ppEnvPtr

	// Step 2 — All nodes (including leader-as-replica) handle PrePrepare.
	prepareEnvs := make([]messages.Envelope, 0, clusterSize)
	for _, nd := range allNodes {
		pEnv, err := nd.HandlePrePrepare(ppEnv)
		if err != nil {
			t.Fatalf("%s HandlePrePrepare: %v", nd.ID, err)
		}
		prepareEnvs = append(prepareEnvs, pEnv)
	}

	// Step 3 — All nodes handle all Prepare messages.
	commitEnvs := make([]messages.Envelope, 0)
	for _, nd := range allNodes {
		for _, pEnv := range prepareEnvs {
			if pEnv.SenderID == nd.ID {
				continue // don't process own message
			}
			cEnvPtr, err := nd.HandlePrepare(pEnv, fLocal)
			if err != nil {
				t.Fatalf("%s HandlePrepare from %s: %v", nd.ID, pEnv.SenderID, err)
			}
			if cEnvPtr != nil {
				commitEnvs = append(commitEnvs, *cEnvPtr)
			}
		}
	}

	if len(commitEnvs) == 0 {
		t.Fatal("no Commit messages produced — quorum not reached in Prepare phase")
	}

	// Step 4 — All nodes handle all Commit messages.
	replyCount := 0
	for _, nd := range allNodes {
		for _, cEnv := range commitEnvs {
			if cEnv.SenderID == nd.ID {
				continue
			}
			rEnvPtr, err := nd.HandleCommit(cEnv, fLocal, operation, clientID)
			if err != nil {
				t.Fatalf("%s HandleCommit from %s: %v", nd.ID, cEnv.SenderID, err)
			}
			if rEnvPtr != nil {
				replyCount++
				if rEnvPtr.Type != messages.MsgReply {
					t.Errorf("expected MsgReply, got %s", rEnvPtr.Type)
				}
			}
		}
	}

	if replyCount == 0 {
		t.Fatal("no Reply messages produced — quorum not reached in Commit phase")
	}

	// Verify every honest node committed the operation.
	for _, nd := range allNodes {
		found := false
		for _, entry := range nd.Log {
			if entry == operation {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %s did not commit operation %q", nd.ID, operation)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// De-duplication: AddPrepare / AddCommit / AddVoteReply
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeAddPrepareDeduplicate(t *testing.T) {
	t.Parallel()
	n := makeNode(t, "n", RoleReplica, 0, 0)

	c1 := n.AddPrepare(1, "sender-A")
	c2 := n.AddPrepare(1, "sender-A") // duplicate — must not increment
	c3 := n.AddPrepare(1, "sender-B") // new sender — must increment

	if c1 != 1 {
		t.Errorf("first AddPrepare = %d, want 1", c1)
	}
	if c2 != 1 {
		t.Errorf("duplicate AddPrepare = %d, want 1 (no change)", c2)
	}
	if c3 != 2 {
		t.Errorf("second sender AddPrepare = %d, want 2", c3)
	}
}

func TestNodeAddCommitDeduplicate(t *testing.T) {
	t.Parallel()
	n := makeNode(t, "n", RoleReplica, 0, 0)

	n.AddCommit(5, "node-X")
	n.AddCommit(5, "node-X") // duplicate
	count := n.AddCommit(5, "node-Y")

	if count != 2 {
		t.Errorf("AddCommit count = %d after 2 distinct senders, want 2", count)
	}
}

func TestNodeAddVoteReplyDeduplicate(t *testing.T) {
	t.Parallel()
	n := makeNode(t, "n", RoleLeader, 0, 0)

	n.AddVoteReply("rep-0", "msg0")
	n.AddVoteReply("rep-0", "msg0") // duplicate sender
	count := n.AddVoteReply("rep-1", "msg1")

	if count != 2 {
		t.Errorf("AddVoteReply count = %d, want 2 (dedup)", count)
	}

	n.ClearVoteReplies()
	if len(n.VoteReplies) != 0 {
		t.Error("ClearVoteReplies did not empty the map")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Role promotion / demotion
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeRolePromotionDemotion(t *testing.T) {
	t.Parallel()
	n := makeNode(t, "n", RoleReplica, 0, 0)

	if n.IsLeader() {
		t.Error("replica should not be leader initially")
	}
	n.PromoteToLeader()
	if !n.IsLeader() {
		t.Error("PromoteToLeader: node should be leader now")
	}
	n.DemoteToReplica()
	if n.IsLeader() {
		t.Error("DemoteToReplica: node should not be leader")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NextSequenceID increments monotonically
// ─────────────────────────────────────────────────────────────────────────────

func TestNodeNextSequenceIDMonotonic(t *testing.T) {
	t.Parallel()
	n := makeNode(t, "n", RoleLeader, 0, 0)

	prev := 0
	for i := 0; i < 10; i++ {
		got := n.NextSequenceID()
		if got <= prev {
			t.Errorf("NextSequenceID call %d: got %d, want > %d (not monotonic)", i, got, prev)
		}
		prev = got
	}
}
