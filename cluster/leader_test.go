package cluster

import (
	"math"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test fixtures
// ─────────────────────────────────────────────────────────────────────────────

// clusterWithCentroid builds a Cluster with an explicit centroid and a given
// set of nodes. Used to test leader election independently of K-Means.
func clusterWithCentroid(id int, cx, cy float64, nodes []Point) Cluster {
	return Cluster{
		ID:       id,
		Centroid: Point{X: cx, Y: cy},
		Nodes:    nodes,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Leader is the node with minimum distance to centroid
// ─────────────────────────────────────────────────────────────────────────────

func TestLeaderElectLeaderMinDistance(t *testing.T) {
	t.Parallel()

	// Centroid at (5, 5).  Distances computed by hand:
	//   node-A (4,5) → dist = 1.0          ← CLOSEST → expected leader
	//   node-B (8,5) → dist = 3.0
	//   node-C (5,9) → dist = 4.0
	//   node-D (0,0) → dist = √50 ≈ 7.07
	c := clusterWithCentroid(0, 5, 5, []Point{
		{ID: "node-A", X: 4, Y: 5},
		{ID: "node-B", X: 8, Y: 5},
		{ID: "node-C", X: 5, Y: 9},
		{ID: "node-D", X: 0, Y: 0},
	})

	idx := ElectLeader(c)

	if idx != 0 {
		t.Errorf("ElectLeader returned index %d (ID=%q), want index 0 (ID=node-A, dist=1.0)",
			idx, c.Nodes[idx].ID)
	}
	if c.Nodes[idx].ID != "node-A" {
		t.Errorf("elected leader ID = %q, want node-A", c.Nodes[idx].ID)
	}
}

// Verify the manual distance arithmetic is self-consistent.
func TestLeaderElectLeaderDistancesMatchManual(t *testing.T) {
	t.Parallel()

	centroid := Point{X: 10, Y: 10}
	nodes := []Point{
		{ID: "far",    X: 20, Y: 20}, // dist = √200 ≈ 14.14
		{ID: "medium", X: 15, Y: 10}, // dist = 5.0
		{ID: "close",  X: 11, Y: 10}, // dist = 1.0  ← expected
	}
	c := Cluster{ID: 0, Centroid: centroid, Nodes: nodes}

	idx := ElectLeader(c)
	if idx != 2 {
		t.Errorf("want index 2 (ID=close), got index %d (ID=%s)", idx, c.Nodes[idx].ID)
	}

	// Also verify LeaderDistance equals the hand-computed value.
	ld := LeaderDistance(c)
	if math.Abs(ld-1.0) > 1e-9 {
		t.Errorf("LeaderDistance = %f, want 1.0", ld)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — A node exactly at the centroid is elected
// ─────────────────────────────────────────────────────────────────────────────

func TestLeaderNodeAtCentroidIsElected(t *testing.T) {
	t.Parallel()

	// Place one node exactly at the centroid; others are further away.
	cx, cy := 50.0, 50.0
	c := clusterWithCentroid(0, cx, cy, []Point{
		{ID: "far-1",    X: 10,  Y: 10},   // dist ≈ 56.6
		{ID: "at-center",X: cx,  Y: cy},   // dist = 0.0 ← expected
		{ID: "far-2",    X: 90,  Y: 90},   // dist ≈ 56.6
		{ID: "near",     X: 51,  Y: 50},   // dist = 1.0
	})

	idx := ElectLeader(c)
	if c.Nodes[idx].ID != "at-center" {
		t.Errorf("expected node at centroid to be elected, got %q (index %d)",
			c.Nodes[idx].ID, idx)
	}
	dist := LeaderDistance(c)
	if dist != 0.0 {
		t.Errorf("leader distance for node at centroid = %f, want 0.0", dist)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — ElectAllLeaders: one leader per cluster, each in the Nodes slice
// ─────────────────────────────────────────────────────────────────────────────

func TestLeaderElectAllLeadersOnePerCluster(t *testing.T) {
	t.Parallel()

	// Build 3 clusters with hand-placed nodes and centroids so leader selection
	// is unambiguous without relying on K-Means.
	clusters := []Cluster{
		clusterWithCentroid(0, 0, 0, []Point{
			{ID: "c0-close", X: 1, Y: 0},   // dist=1 ← leader
			{ID: "c0-far",   X: 5, Y: 5},   // dist≈7.07
		}),
		clusterWithCentroid(1, 100, 100, []Point{
			{ID: "c1-far",   X: 200, Y: 200}, // dist≈141
			{ID: "c1-close", X: 101, Y: 100}, // dist=1 ← leader
		}),
		clusterWithCentroid(2, 500, 0, []Point{
			{ID: "c2-a",     X: 503, Y: 0}, // dist=3
			{ID: "c2-b",     X: 502, Y: 0}, // dist=2 ← leader
			{ID: "c2-c",     X: 510, Y: 0}, // dist=10
		}),
	}

	leaders := ElectAllLeaders(clusters)

	// Must have exactly one entry per cluster.
	if len(leaders) != len(clusters) {
		t.Fatalf("ElectAllLeaders returned %d entries, want %d", len(leaders), len(clusters))
	}

	// Each elected leader Point must exist verbatim in its cluster's Nodes slice.
	for _, c := range clusters {
		leaderPoint, ok := leaders[c.ID]
		if !ok {
			t.Errorf("cluster %d has no entry in leaders map", c.ID)
			continue
		}
		found := false
		for _, nd := range c.Nodes {
			if nd.ID == leaderPoint.ID && nd.X == leaderPoint.X && nd.Y == leaderPoint.Y {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cluster %d leader %q is not in Cluster.Nodes", c.ID, leaderPoint.ID)
		}
	}

	// Spot-check expected leaders.
	if leaders[0].ID != "c0-close" {
		t.Errorf("cluster 0: expected leader c0-close, got %q", leaders[0].ID)
	}
	if leaders[1].ID != "c1-close" {
		t.Errorf("cluster 1: expected leader c1-close, got %q", leaders[1].ID)
	}
	if leaders[2].ID != "c2-b" {
		t.Errorf("cluster 2: expected leader c2-b, got %q", leaders[2].ID)
	}
}

// ElectAllLeaders on an empty slice must return an empty map (not panic).
func TestLeaderElectAllLeadersEmptyInput(t *testing.T) {
	t.Parallel()
	leaders := ElectAllLeaders([]Cluster{})
	if len(leaders) != 0 {
		t.Errorf("ElectAllLeaders([]) returned %d entries, want 0", len(leaders))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Tie-breaking is deterministic: lowest index wins
// ─────────────────────────────────────────────────────────────────────────────

func TestLeaderTieBreakingLowestIndexWins(t *testing.T) {
	t.Parallel()

	// Centroid at origin. Four nodes all at distance 1.0 from the centroid —
	// one at each cardinal direction. Lowest index (0) must always win.
	c := clusterWithCentroid(0, 0, 0, []Point{
		{ID: "north", X: 0,  Y: 1},  // dist=1.0 — index 0 ← must win
		{ID: "east",  X: 1,  Y: 0},  // dist=1.0 — index 1
		{ID: "south", X: 0,  Y: -1}, // dist=1.0 — index 2
		{ID: "west",  X: -1, Y: 0},  // dist=1.0 — index 3
	})

	idx := ElectLeader(c)
	if idx != 0 {
		t.Errorf("tie-breaking: got index %d (%q), want index 0 (north) — lowest index must win",
			idx, c.Nodes[idx].ID)
	}

	// Also confirm the result is stable across repeated calls (no randomness).
	for i := 0; i < 10; i++ {
		if ElectLeader(c) != 0 {
			t.Errorf("ElectLeader returned different index on call %d — not deterministic", i)
			break
		}
	}
}

// Tie on distance but only among a subset of nodes: lower-index still wins
// even if there is a non-tied node at a greater distance later in the slice.
func TestLeaderTieBreakingPartialTie(t *testing.T) {
	t.Parallel()

	// Centroid at (0,0).
	// node-0: dist=2.0  (tied for min)
	// node-1: dist=5.0
	// node-2: dist=2.0  (tied for min, higher index)
	// node-3: dist=10.0
	c := clusterWithCentroid(0, 0, 0, []Point{
		{ID: "node-0", X: 2, Y: 0},   // dist=2 ← expected winner (lower index)
		{ID: "node-1", X: 5, Y: 0},   // dist=5
		{ID: "node-2", X: 0, Y: 2},   // dist=2, tied with node-0
		{ID: "node-3", X: 10, Y: 0},  // dist=10
	})

	idx := ElectLeader(c)
	if idx != 0 {
		t.Errorf("partial tie: got index %d (%q), want 0 (node-0) — lower index wins",
			idx, c.Nodes[idx].ID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge cases
// ─────────────────────────────────────────────────────────────────────────────

// ElectLeader on an empty cluster must return -1 (not panic).
func TestLeaderEmptyClusterReturnsNegativeOne(t *testing.T) {
	t.Parallel()
	c := Cluster{ID: 0, Centroid: Point{X: 0, Y: 0}, Nodes: []Point{}}
	if idx := ElectLeader(c); idx != -1 {
		t.Errorf("ElectLeader(empty cluster) = %d, want -1", idx)
	}
}

// ElectLeader on a single-node cluster must elect that node (index 0).
func TestLeaderSingleNodeIsAlwaysLeader(t *testing.T) {
	t.Parallel()
	c := clusterWithCentroid(0, 100, 200, []Point{
		{ID: "only-node", X: 999, Y: 999}, // far from centroid, but only option
	})
	idx := ElectLeader(c)
	if idx != 0 {
		t.Errorf("single-node cluster: ElectLeader = %d, want 0", idx)
	}
}

// LeaderDistance on an empty cluster returns MaxFloat64 (no panic).
func TestLeaderDistanceEmptyCluster(t *testing.T) {
	t.Parallel()
	c := Cluster{ID: 0, Centroid: Point{}, Nodes: []Point{}}
	d := LeaderDistance(c)
	if d != math.MaxFloat64 {
		t.Errorf("LeaderDistance(empty) = %v, want MaxFloat64", d)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration: SameSizeKMeans → ElectAllLeaders pipeline
// ─────────────────────────────────────────────────────────────────────────────

// Verify the full pipeline: cluster 12 nodes then elect one leader per cluster.
// Leader must exist in the cluster's Nodes slice and each cluster gets exactly one.
func TestLeaderIntegrationWithKMeans(t *testing.T) {
	t.Parallel()

	nodes := makeGrid(12)
	clusters := SameSizeKMeansSeeded(nodes, 3, 4, 42)
	leaders := ElectAllLeaders(clusters)

	if len(leaders) != 3 {
		t.Fatalf("expected 3 leaders, got %d", len(leaders))
	}

	for _, c := range clusters {
		lp, ok := leaders[c.ID]
		if !ok {
			t.Errorf("no leader for cluster %d", c.ID)
			continue
		}

		// Leader must be a member of the cluster.
		found := false
		for _, nd := range c.Nodes {
			if nd.ID == lp.ID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cluster %d leader %q is not in Nodes", c.ID, lp.ID)
		}

		// Leader must genuinely be the closest node to the centroid.
		leaderDist := EuclideanDistance(lp, c.Centroid)
		for _, nd := range c.Nodes {
			d := EuclideanDistance(nd, c.Centroid)
			if d < leaderDist-1e-9 { // 1e-9 tolerance for floating-point
				t.Errorf("cluster %d: node %q (dist=%.4f) is closer to centroid than leader %q (dist=%.4f)",
					c.ID, nd.ID, d, lp.ID, leaderDist)
			}
		}
	}
}

// ElectAllLeaders is stable: calling it twice on the same clusters gives the
// same result (no hidden randomness).
func TestLeaderElectAllLeadersIsStable(t *testing.T) {
	t.Parallel()

	clusters := SameSizeKMeansSeeded(makeGrid(12), 3, 4, 7)
	l1 := ElectAllLeaders(clusters)
	l2 := ElectAllLeaders(clusters)

	for id, p1 := range l1 {
		p2 := l2[id]
		if p1.ID != p2.ID || p1.X != p2.X || p1.Y != p2.Y {
			t.Errorf("cluster %d: leader changed between calls: %q vs %q", id, p1.ID, p2.ID)
		}
	}
}