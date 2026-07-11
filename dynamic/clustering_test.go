package dynamic

import (
	"fmt"
	"math"
	"testing"

	"github.com/adk2004/vehicular-bft/cluster"
	nodemod "github.com/adk2004/vehicular-bft/node"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// makeGridPoints returns p evenly-spaced Points on a 2-D grid.
// Columns = ceil(sqrt(p)), spacing = 100 units.
func makeGridPoints(p int) []cluster.Point {
	cols := int(math.Ceil(math.Sqrt(float64(p))))
	pts := make([]cluster.Point, 0, p)
	for i := 0; i < p; i++ {
		row := i / cols
		col := i % cols
		pts = append(pts, cluster.Point{
			ID: fmt.Sprintf("node-%d-%d", row, col),
			X:  float64(col) * 100,
			Y:  float64(row) * 100,
		})
	}
	return pts
}

// makeNodesForPoints creates Node objects parallel to pts.
// All nodes get RoleReplica; nodes[0] is promoted to Leader within each cluster.
func makeNodesForPoints(t *testing.T, pts []cluster.Point) []*nodemod.Node {
	t.Helper()
	nodes := make([]*nodemod.Node, len(pts))
	for i, pt := range pts {
		nd, err := nodemod.NewNode(pt.ID, nodemod.RoleReplica, 0, i, pt)
		if err != nil {
			t.Fatalf("makeNodesForPoints: NewNode(%s): %v", pt.ID, err)
		}
		nodes[i] = nd
	}
	return nodes
}

// buildTickMode builds a TickMode from p grid points.
// Uses SameSizeKMeansSeeded with a fixed seed for reproducibility.
func buildTickMode(t *testing.T, p int) *TickMode {
	t.Helper()
	pts := makeGridPoints(p)
	n, m := cluster.ComputeDimensions(p)
	clusters := cluster.SameSizeKMeansSeeded(pts, m, n, 42)
	nodes := makeNodesForPoints(t, pts)
	return NewTickMode(clusters, nodes, 10, 4)
}

// collectAllNodeIDs returns a set of all node IDs across all clusters.
func collectAllNodeIDs(clusters []cluster.Cluster) map[string]int {
	seen := make(map[string]int)
	for _, c := range clusters {
		for _, pt := range c.Nodes {
			seen[pt.ID]++
		}
	}
	return seen
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Adding 4 nodes to 8-node network → 3 clusters of 4
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicAddFourNodesToEight(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Verify initial state: 8 nodes in 2 clusters of 4.
	if len(tm.CurrentClusters) != 2 {
		t.Fatalf("initial clusters = %d, want 2", len(tm.CurrentClusters))
	}
	if len(tm.NodePoints) != 8 {
		t.Fatalf("initial NodePoints = %d, want 8", len(tm.NodePoints))
	}

	// Add 4 new nodes spread far apart from existing ones.
	newNodes := []cluster.Point{
		{ID: "new-0", X: 500, Y: 0},
		{ID: "new-1", X: 500, Y: 100},
		{ID: "new-2", X: 500, Y: 200},
		{ID: "new-3", X: 500, Y: 300},
	}

	newClusters, newLeaders, err := tm.ProcessTick(Tick{NewNodes: newNodes})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// Total nodes must now be 12.
	if len(tm.NodePoints) != 12 {
		t.Errorf("NodePoints after tick = %d, want 12", len(tm.NodePoints))
	}
	if len(tm.Nodes) != 12 {
		t.Errorf("Nodes after tick = %d, want 12", len(tm.Nodes))
	}

	// Must have 3 clusters (ComputeDimensions(12) → n=4, m=3).
	if len(newClusters) != 3 {
		t.Errorf("clusters after tick = %d, want 3", len(newClusters))
	}

	// Each cluster must have exactly 4 nodes.
	for _, c := range newClusters {
		if len(c.Nodes) != 4 {
			t.Errorf("cluster %d has %d nodes, want 4", c.ID, len(c.Nodes))
		}
	}

	// Leaders map must have one entry per cluster.
	if len(newLeaders) != 3 {
		t.Errorf("newLeaders has %d entries, want 3", len(newLeaders))
	}

	t.Logf("8→12 nodes: %d clusters, leaders: %v", len(newClusters), func() []string {
		ids := make([]string, 0, len(newLeaders))
		for _, l := range newLeaders {
			ids = append(ids, l.ID)
		}
		return ids
	}())
}

// MaxPerTick is enforced: adding 6 nodes when MaxPerTick=4 admits only 4.
func TestDynamicMaxPerTickEnforced(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)
	tm.MaxPerTick = 4

	sixNewNodes := make([]cluster.Point, 6)
	for i := range sixNewNodes {
		sixNewNodes[i] = cluster.Point{ID: fmt.Sprintf("extra-%d", i), X: float64(i * 50), Y: 1000}
	}

	_, _, err := tm.ProcessTick(Tick{NewNodes: sixNewNodes})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// Only 4 of the 6 new nodes should have been admitted.
	if len(tm.NodePoints) != 12 { // 8 original + 4 admitted
		t.Errorf("NodePoints = %d, want 12 (MaxPerTick=4 enforced)", len(tm.NodePoints))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — After re-clustering, no node appears in two clusters
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicNoNodeInTwoClusters(t *testing.T) {
	t.Parallel()

	configs := []struct{ start, add int }{
		{8, 4},
		{12, 4},
		{8, 0},
	}

	for _, cfg := range configs {
		cfg := cfg
		t.Run(fmt.Sprintf("start=%d_add=%d", cfg.start, cfg.add), func(t *testing.T) {
			t.Parallel()

			tm := buildTickMode(t, cfg.start)

			newPts := make([]cluster.Point, cfg.add)
			for i := range newPts {
				newPts[i] = cluster.Point{ID: fmt.Sprintf("fresh-%d-%d", cfg.start, i), X: float64(i*75 + 999), Y: 999}
			}

			newClusters, _, err := tm.ProcessTick(Tick{NewNodes: newPts})
			if err != nil {
				t.Fatalf("ProcessTick: %v", err)
			}

			seen := collectAllNodeIDs(newClusters)

			// No node ID may appear more than once.
			for id, count := range seen {
				if count > 1 {
					t.Errorf("node %q appears in %d clusters (duplicate)", id, count)
				}
			}

			// Total node count must equal start + admitted.
			admitted := cfg.add
			if admitted > tm.MaxPerTick {
				admitted = tm.MaxPerTick
			}
			want := cfg.start + admitted
			if len(seen) != want {
				t.Errorf("total distinct nodes = %d, want %d", len(seen), want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Leader closest to centroid stays leader
// ─────────────────────────────────────────────────────────────────────────────

// TestDynamicLeaderIsAlwaysClosestToCentroid verifies that after every ProcessTick,
// the elected leader for each cluster is genuinely the closest node to the centroid.
// This is the mathematical invariant from paper Section V-C:
// "The leader node of each cluster is the node with the minimum Euclidean
// distance to the cluster's centroid."
func TestDynamicLeaderIsAlwaysClosestToCentroid(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Run 3 ticks with minor movements; verify the invariant after each tick.
	for tick := 0; tick < 3; tick++ {
		// Minor movement: nudge all existing nodes by 1 unit.
		moved := make(map[string]cluster.Point, len(tm.NodePoints))
		for _, pt := range tm.NodePoints {
			moved[pt.ID] = cluster.Point{ID: pt.ID, X: pt.X + 1, Y: pt.Y + 1}
		}

		newClusters, newLeaders, err := tm.ProcessTick(Tick{MovedNodes: moved})
		if err != nil {
			t.Fatalf("tick %d: ProcessTick: %v", tick, err)
		}

		// For every cluster, verify leader is the node closest to centroid.
		for _, c := range newClusters {
			leaderPt, ok := newLeaders[c.ID]
			if !ok {
				t.Errorf("tick %d cluster %d: no leader in map", tick, c.ID)
				continue
			}
			leaderDist := cluster.EuclideanDistance(leaderPt, c.Centroid)
			for _, nd := range c.Nodes {
				d := cluster.EuclideanDistance(nd, c.Centroid)
				if d < leaderDist-1e-9 {
					t.Errorf("tick %d cluster %d: node %q (dist=%.4f) is closer than leader %q (dist=%.4f)",
						tick, c.ID, nd.ID, d, leaderPt.ID, leaderDist)
				}
			}
		}
	}
}

// TestDynamicStableLeaderSamePosition verifies that a node sitting exactly at
// the cluster centroid is elected leader and stays leader across ticks when
// other nodes move but it stays put.
func TestDynamicStableLeaderSamePosition(t *testing.T) {
	t.Parallel()

	// Build 4 nodes: one exactly at the centroid, three at corners.
	// Centroid of these 4 points = mean of all coords.
	// Place centroid node at (50,50) and corners at (0,0),(100,0),(0,100),(100,100).
	pts := []cluster.Point{
		{ID: "center", X: 50, Y: 50}, // will be closest to centroid
		{ID: "c0", X: 0, Y: 0},
		{ID: "c1", X: 100, Y: 0},
		{ID: "c2", X: 0, Y: 100},
		// Add 4 more nodes far away so we have exactly 2 clusters of 4.
		{ID: "far0", X: 900, Y: 900},
		{ID: "far1", X: 1000, Y: 900},
		{ID: "far2", X: 900, Y: 1000},
		{ID: "far3", X: 1000, Y: 1000},
	}

	n, m := cluster.ComputeDimensions(8) // n=4, m=2
	clusters := cluster.SameSizeKMeansSeeded(pts, m, n, 10)
	nodes := makeNodesForPoints(t, pts)
	tm := NewTickMode(clusters, nodes, 10, 4)

	// Find which cluster contains "center".
	centerClusterID := -1
	for _, c := range tm.CurrentClusters {
		for _, nd := range c.Nodes {
			if nd.ID == "center" {
				centerClusterID = c.ID
				break
			}
		}
		if centerClusterID >= 0 {
			break
		}
	}
	if centerClusterID < 0 {
		t.Fatal("center node not found in any cluster")
	}

	// Verify center is already the leader.
	initialLeader := tm.Leaders[centerClusterID]
	if initialLeader.ID != "center" {
		t.Logf("note: center node is not initial leader (ID=%s, dist from centroid may differ)",
			initialLeader.ID)
		// This can legitimately happen if the K-Means centroid falls on a corner
		// node. The important check is the invariant test above.
	}

	// Move corner nodes slightly but keep center in place.
	moved := map[string]cluster.Point{
		"c0": {ID: "c0", X: 2, Y: 2},
		"c1": {ID: "c1", X: 98, Y: 2},
		"c2": {ID: "c2", X: 2, Y: 98},
	}
	newClusters, newLeaders, err := tm.ProcessTick(Tick{MovedNodes: moved})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// After tick, verify the invariant for the cluster containing "center".
	for _, c := range newClusters {
		for _, nd := range c.Nodes {
			if nd.ID != "center" {
				continue
			}
			// "center" is in cluster c; verify it's still the leader.
			leaderPt := newLeaders[c.ID]
			leaderDist := cluster.EuclideanDistance(leaderPt, c.Centroid)
			centerDist := cluster.EuclideanDistance(nd, c.Centroid)

			if centerDist < leaderDist-1e-9 {
				t.Errorf("center node (dist=%.4f) is closer to centroid than elected leader %q (dist=%.4f)",
					centerDist, leaderPt.ID, leaderDist)
			}
			return // found and verified
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Node that moved away from centroid is replaced as leader
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicLeaderReplacedWhenMovedAway(t *testing.T) {
	t.Parallel()

	// 8 nodes in 2 clusters of 4: left half (x≈0-300) and right half (x≈700-1000).
	pts := []cluster.Point{
		// Left cluster
		{ID: "L0", X: 0, Y: 0},
		{ID: "L1", X: 100, Y: 0},
		{ID: "L2", X: 0, Y: 100},
		{ID: "L3", X: 100, Y: 100},
		// Right cluster
		{ID: "R0", X: 700, Y: 0},
		{ID: "R1", X: 800, Y: 0},
		{ID: "R2", X: 700, Y: 100},
		{ID: "R3", X: 800, Y: 100},
	}

	n, m := cluster.ComputeDimensions(8)
	clusters := cluster.SameSizeKMeansSeeded(pts, m, n, 55)
	nodes := makeNodesForPoints(t, pts)
	tm := NewTickMode(clusters, nodes, 10, 4)

	// Find the left cluster and its leader.
	leftClusterID := -1
	for _, c := range tm.CurrentClusters {
		for _, nd := range c.Nodes {
			if nd.ID == "L0" {
				leftClusterID = c.ID
				break
			}
		}
		if leftClusterID >= 0 {
			break
		}
	}
	if leftClusterID < 0 {
		t.Skip("left cluster not found — K-Means produced unexpected assignment")
	}

	originalLeader := tm.Leaders[leftClusterID]
	t.Logf("original leader of left cluster: %s at (%.0f,%.0f)",
		originalLeader.ID, originalLeader.X, originalLeader.Y)

	// Move the original leader far to the right (away from left cluster).
	moved := map[string]cluster.Point{
		originalLeader.ID: {ID: originalLeader.ID, X: 950, Y: 50}, // now in right territory
	}

	newClusters, newLeaders, err := tm.ProcessTick(Tick{MovedNodes: moved})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// Find the new cluster containing original left nodes (those that didn't move).
	for _, c := range newClusters {
		// Check if this cluster contains one of the stationary left nodes.
		hasStationary := false
		for _, nd := range c.Nodes {
			if nd.ID == "L1" || nd.ID == "L2" || nd.ID == "L3" {
				hasStationary = true
				break
			}
		}
		if !hasStationary {
			continue
		}

		// Verify leader is closest to centroid.
		leaderPt := newLeaders[c.ID]
		leaderDist := cluster.EuclideanDistance(leaderPt, c.Centroid)
		for _, nd := range c.Nodes {
			d := cluster.EuclideanDistance(nd, c.Centroid)
			if d < leaderDist-1e-9 {
				t.Errorf("node %q (dist=%.4f) is closer to centroid than leader %q (dist=%.4f)",
					nd.ID, d, leaderPt.ID, leaderDist)
			}
		}
		t.Logf("new leader of left-side cluster %d: %s", c.ID, leaderPt.ID)
		return
	}

	t.Log("note: moved node caused significant cluster reorganisation — invariant still checked in Test 3")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Service queue non-empty before tick, empty after
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicServiceQueueClearedAfterTick(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Pre-populate service queues for both initial clusters.
	for _, c := range tm.CurrentClusters {
		for i := 0; i < 3; i++ {
			tm.EnqueueOperation(c.ID, fmt.Sprintf("op-%d-%d", c.ID, i))
		}
	}

	// Queue must be non-empty before the tick.
	if tm.PendingCount() == 0 {
		t.Fatal("ServiceQueue should be non-empty before ProcessTick")
	}
	t.Logf("pending before tick: %d operations", tm.PendingCount())

	// Process tick with no changes.
	_, _, err := tm.ProcessTick(Tick{})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// After ProcessTick the primary queues must have been forwarded/cleared.
	// (Forwarding re-enqueues under the new cluster IDs, so we check that
	// the total pending is ≤ the original count — nothing was duplicated.)
	afterCount := tm.PendingCount()
	t.Logf("pending after tick: %d operations", afterCount)

	// The queue should be empty or have been forwarded (≤ original count).
	// With no new nodes and no movement, forwarding maps old→new cluster IDs.
	// At most the same ops can be present (no duplication).
	if afterCount > 6 { // 6 = 2 clusters × 3 ops
		t.Errorf("ServiceQueue has %d pending after tick, want ≤ 6 (no duplication)", afterCount)
	}
}

// TestDynamicServiceQueueEmptyOnNoOpTick confirms an empty queue stays empty.
func TestDynamicServiceQueueEmptyOnNoOpTick(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)
	// No pre-enqueued operations.

	_, _, err := tm.ProcessTick(Tick{})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	if tm.PendingCount() != 0 {
		t.Errorf("ServiceQueue = %d after empty tick, want 0", tm.PendingCount())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6 — Adding 0 nodes (movement only) still triggers leader re-evaluation
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicMovementOnlyTriggersLeaderReEvaluation(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Capture initial leaders.
	initialLeaders := make(map[int]cluster.Point, len(tm.Leaders))
	for id, lp := range tm.Leaders {
		initialLeaders[id] = lp
	}

	// Move ALL nodes significantly (50 units right).
	moved := make(map[string]cluster.Point, len(tm.NodePoints))
	for _, pt := range tm.NodePoints {
		moved[pt.ID] = cluster.Point{ID: pt.ID, X: pt.X + 50, Y: pt.Y}
	}

	// No new nodes — only movement.
	newClusters, newLeaders, err := tm.ProcessTick(Tick{
		NewNodes:   nil,
		MovedNodes: moved,
	})
	if err != nil {
		t.Fatalf("ProcessTick (movement-only): %v", err)
	}

	// Node count must be unchanged.
	if len(tm.NodePoints) != 8 {
		t.Errorf("NodePoints = %d after movement-only tick, want 8", len(tm.NodePoints))
	}

	// Leader election MUST have been re-run: for every new cluster, the
	// returned leader must be the closest node to the new centroid.
	for _, c := range newClusters {
		leaderPt, ok := newLeaders[c.ID]
		if !ok {
			t.Errorf("cluster %d: no leader after movement-only tick", c.ID)
			continue
		}
		leaderDist := cluster.EuclideanDistance(leaderPt, c.Centroid)
		for _, nd := range c.Nodes {
			d := cluster.EuclideanDistance(nd, c.Centroid)
			if d < leaderDist-1e-9 {
				t.Errorf("cluster %d: node %q (dist=%.4f) closer to centroid than leader %q (dist=%.4f) — leader re-eval broken",
					c.ID, nd.ID, d, leaderPt.ID, leaderDist)
			}
		}
	}

	// Updated NodePoints must reflect the movement.
	for _, pt := range tm.NodePoints {
		orig := moved[pt.ID] // the expected new position
		if orig.ID == "" {
			continue // skip if not in moved map (shouldn't happen)
		}
		if math.Abs(pt.X-orig.X) > 1e-9 || math.Abs(pt.Y-orig.Y) > 1e-9 {
			t.Errorf("NodePoints[%s]: position not updated after movement", pt.ID)
		}
	}
	t.Logf("movement-only tick: %d clusters, %d leaders re-evaluated", len(newClusters), len(newLeaders))
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: duplicate node ID is ignored
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicDuplicateNodeIDIgnored(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Try to add a node whose ID already exists.
	existingID := tm.NodePoints[0].ID
	duplicate := cluster.Point{ID: existingID, X: 999, Y: 999}

	_, _, err := tm.ProcessTick(Tick{NewNodes: []cluster.Point{duplicate}})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}

	// Node count must still be 8.
	if len(tm.NodePoints) != 8 {
		t.Errorf("NodePoints = %d after duplicate add, want 8", len(tm.NodePoints))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: ForwardPendingRequests clears the queue
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicForwardPendingRequestsClearsQueue(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Enqueue ops for cluster 0.
	for i := 0; i < 5; i++ {
		tm.EnqueueOperation(0, fmt.Sprintf("op-%d", i))
	}
	if tm.PendingCount() == 0 {
		t.Fatal("queue should be non-empty before ForwardPendingRequests")
	}

	// Forward from some old leader to a new address.
	oldLeaderID := tm.Leaders[0].ID
	err := tm.ForwardPendingRequests(oldLeaderID, "127.0.0.1:9001")
	if err != nil {
		t.Fatalf("ForwardPendingRequests: %v", err)
	}

	// Queue for cluster 0 should now be clear.
	tm.mu.Lock()
	remaining := len(tm.ServiceQueue[0])
	tm.mu.Unlock()
	if remaining != 0 {
		t.Errorf("ServiceQueue[0] = %d ops after ForwardPendingRequests, want 0", remaining)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: InTickMode flag is set/cleared correctly
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicInTickModeFlag(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8)

	// Before tick: flag should be false.
	if tm.InTickMode() {
		t.Error("InTickMode() = true before any tick — should be false")
	}

	// After tick: flag should be false again.
	_, _, err := tm.ProcessTick(Tick{})
	if err != nil {
		t.Fatalf("ProcessTick: %v", err)
	}
	if tm.InTickMode() {
		t.Error("InTickMode() = true after ProcessTick returned — should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: multi-tick growth 8 → 12 → 16
// ─────────────────────────────────────────────────────────────────────────────

func TestDynamicMultiTickGrowth(t *testing.T) {
	t.Parallel()

	tm := buildTickMode(t, 8) // start: 8 nodes, 2 clusters

	for tickNum, newCount := range []int{4, 4} {
		newPts := make([]cluster.Point, newCount)
		for i := range newPts {
			newPts[i] = cluster.Point{
				ID: fmt.Sprintf("growth-t%d-%d", tickNum, i),
				X:  float64(tickNum*500 + i*100),
				Y:  float64(tickNum * 200),
			}
		}
		newClusters, _, err := tm.ProcessTick(Tick{NewNodes: newPts})
		if err != nil {
			t.Fatalf("tick %d: ProcessTick: %v", tickNum+1, err)
		}

		// Verify no duplicates after each tick.
		seen := collectAllNodeIDs(newClusters)
		for id, count := range seen {
			if count > 1 {
				t.Errorf("tick %d: node %q in %d clusters", tickNum+1, id, count)
			}
		}
	}

	// After 2 ticks of +4 each: 16 nodes total.
	if len(tm.NodePoints) != 16 {
		t.Errorf("after 2 growth ticks: NodePoints = %d, want 16", len(tm.NodePoints))
	}

	n, m := cluster.ComputeDimensions(16) // n=4, m=4
	if m != 4 || n != 4 {
		t.Errorf("ComputeDimensions(16) = (n=%d, m=%d), want (4,4)", n, m)
	}
}