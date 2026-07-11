package cluster

import (
	"fmt"
	"math"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test fixtures
// ─────────────────────────────────────────────────────────────────────────────

// makeGrid returns p Points laid out on a uniform integer grid, row-major order.
// E.g. makeGrid(12) → (0,0),(1,0),(2,0),(3,0),(0,1),…  (4 columns, 3 rows).
// All IDs are unique: "p-<row>-<col>".
// Keeping coordinates small and well-separated makes cluster assignments
// predictable regardless of K-Means++ random seed.
func makeGrid(p int) []Point {
	cols := int(math.Ceil(math.Sqrt(float64(p))))
	nodes := make([]Point, 0, p)
	for i := 0; i < p; i++ {
		row := i / cols
		col := i % cols
		nodes = append(nodes, Point{
			ID: fmt.Sprintf("p-%d-%d", row, col),
			X:  float64(col) * 100, // 100-unit spacing to make clusters obvious
			Y:  float64(row) * 100,
		})
	}
	return nodes
}

// makeFourCorners returns 8 nodes placed at the 4 corners of a 1000×1000 square,
// 2 nodes per corner (offset by 1 unit). Perfect for 2-cluster and 4-cluster tests.
func makeFourCorners() []Point {
	return []Point{
		{ID: "a1", X: 0, Y: 0}, {ID: "a2", X: 1, Y: 0},
		{ID: "b1", X: 1000, Y: 0}, {ID: "b2", X: 1001, Y: 0},
		{ID: "c1", X: 0, Y: 1000}, {ID: "c2", X: 1, Y: 1000},
		{ID: "d1", X: 1000, Y: 1000}, {ID: "d2", X: 1001, Y: 1000},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — ComputeDimensions(12) → n=4, m=3
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansComputeDimensions12(t *testing.T) {
	t.Parallel()
	n, m := ComputeDimensions(12)
	if n != 4 {
		t.Errorf("ComputeDimensions(12): n = %d, want 4 (sqrt(12)≈3.46→3 < MINSIZE=4)", n)
	}
	if m != 3 {
		t.Errorf("ComputeDimensions(12): m = %d, want 3 (12/4=3)", m)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — ComputeDimensions(8) → n=4, m=2
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansComputeDimensions8(t *testing.T) {
	t.Parallel()
	n, m := ComputeDimensions(8)
	if n != 4 {
		t.Errorf("ComputeDimensions(8): n = %d, want 4 (sqrt(8)≈2.83→2 < MINSIZE=4)", n)
	}
	if m != 2 {
		t.Errorf("ComputeDimensions(8): m = %d, want 2 (8/4=2)", m)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — ComputeDimensions(20) → n=4, m=5
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansComputeDimensions20(t *testing.T) {
	t.Parallel()
	n, m := ComputeDimensions(20)
	if n != 4 {
		t.Errorf("ComputeDimensions(20): n = %d, want 4 (sqrt(20)≈4.47→4, meets MINSIZE)", n)
	}
	if m != 5 {
		t.Errorf("ComputeDimensions(20): m = %d, want 5 (20/4=5)", m)
	}
}

// Additional dimension checks for completeness.
func TestKMeansComputeDimensionsTable(t *testing.T) {
	t.Parallel()
	type tc struct {
		p, wantN, wantM int
	}
	cases := []tc{
		{4, 4, 1},   // p=4: sqrt=2 < MINSIZE → n=4, m=1
		{8, 4, 2},   // plan test 2
		{9, 4, 2},   // sqrt=3 < MINSIZE → n=4, m=2 (1 leftover)
		{12, 4, 3},  // plan test 1
		{16, 4, 4},  // sqrt=4, m=4
		{20, 4, 5},  // plan test 3
		{25, 5, 5},  // sqrt=5 ≥ MINSIZE → n=5, m=5
		{100, 10, 10}, // sqrt=10, m=10
	}
	for _, c := range cases {
		c := c
		t.Run(fmt.Sprintf("p=%d", c.p), func(t *testing.T) {
			t.Parallel()
			n, m := ComputeDimensions(c.p)
			if n != c.wantN || m != c.wantM {
				t.Errorf("ComputeDimensions(%d) = (%d,%d), want (%d,%d)",
					c.p, n, m, c.wantN, c.wantM)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — SameSizeKMeans with 12 nodes → exactly 3 clusters of 4 nodes
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeans12NodesThreeClustersOf4(t *testing.T) {
	t.Parallel()
	nodes := makeGrid(12)
	const m, n = 3, 4
	clusters := SameSizeKMeansSeeded(nodes, m, n, 42)

	if len(clusters) != m {
		t.Fatalf("got %d clusters, want %d", len(clusters), m)
	}
	for _, c := range clusters {
		if len(c.Nodes) != n {
			t.Errorf("cluster %d has %d nodes, want exactly %d", c.ID, len(c.Nodes), n)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — SameSizeKMeans with 8 nodes → exactly 2 clusters of 4 nodes
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeans8NodesTwoClustersOf4(t *testing.T) {
	t.Parallel()
	nodes := makeFourCorners() // 8 well-separated nodes
	const m, n = 2, 4
	clusters := SameSizeKMeansSeeded(nodes, m, n, 99)

	if len(clusters) != m {
		t.Fatalf("got %d clusters, want %d", len(clusters), m)
	}
	for _, c := range clusters {
		if len(c.Nodes) != n {
			t.Errorf("cluster %d has %d nodes, want exactly %d", c.ID, len(c.Nodes), n)
		}
	}
}

// Also test 16-node and 20-node configurations (paper Section XIV).
func TestKMeans16And20NodesEqualSize(t *testing.T) {
	t.Parallel()
	configs := []struct{ p, m, n int }{{16, 4, 4}, {20, 5, 4}}
	for _, cfg := range configs {
		cfg := cfg
		t.Run(fmt.Sprintf("p=%d_m=%d_n=%d", cfg.p, cfg.m, cfg.n), func(t *testing.T) {
			t.Parallel()
			nodes := makeGrid(cfg.p)
			clusters := SameSizeKMeansSeeded(nodes, cfg.m, cfg.n, 1234)
			if len(clusters) != cfg.m {
				t.Fatalf("got %d clusters, want %d", len(clusters), cfg.m)
			}
			for _, c := range clusters {
				if len(c.Nodes) != cfg.n {
					t.Errorf("cluster %d has %d nodes, want %d", c.ID, len(c.Nodes), cfg.n)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6 — All p nodes appear in exactly one cluster (no duplicates, no missing)
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansNodesPartitionExactly(t *testing.T) {
	t.Parallel()
	configs := []struct{ p, m, n int }{{8, 2, 4}, {12, 3, 4}, {16, 4, 4}, {20, 5, 4}}

	for _, cfg := range configs {
		cfg := cfg
		t.Run(fmt.Sprintf("p=%d", cfg.p), func(t *testing.T) {
			t.Parallel()
			nodes := makeGrid(cfg.p)
			clusters := SameSizeKMeansSeeded(nodes, cfg.m, cfg.n, 77)

			// Build a set of all original node IDs.
			originalIDs := make(map[string]bool, cfg.p)
			for _, nd := range nodes {
				originalIDs[nd.ID] = true
			}

			// Walk all cluster assignments and verify each ID appears exactly once.
			seen := make(map[string]int, cfg.p)
			for _, c := range clusters {
				for _, nd := range c.Nodes {
					seen[nd.ID]++
				}
			}

			// Check for duplicates.
			for id, count := range seen {
				if count > 1 {
					t.Errorf("node %q appears in %d clusters (duplicate)", id, count)
				}
			}

			// Check for missing nodes.
			for id := range originalIDs {
				if seen[id] == 0 {
					t.Errorf("node %q does not appear in any cluster (missing)", id)
				}
			}

			// Total assigned nodes must equal p.
			total := 0
			for _, c := range clusters {
				total += len(c.Nodes)
			}
			if total != cfg.p {
				t.Errorf("total assigned nodes = %d, want %d", total, cfg.p)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7 — Same seed → identical cluster assignments
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansDeterministicWithSameSeed(t *testing.T) {
	t.Parallel()
	nodes := makeGrid(12)
	const m, n, seed = 3, 4, int64(42)

	run1 := SameSizeKMeansSeeded(nodes, m, n, seed)
	run2 := SameSizeKMeansSeeded(nodes, m, n, seed)

	if len(run1) != len(run2) {
		t.Fatalf("cluster count differs: run1=%d, run2=%d", len(run1), len(run2))
	}
	for i := range run1 {
		if len(run1[i].Nodes) != len(run2[i].Nodes) {
			t.Errorf("cluster %d node count differs: run1=%d run2=%d",
				i, len(run1[i].Nodes), len(run2[i].Nodes))
			continue
		}
		for j, n1 := range run1[i].Nodes {
			n2 := run2[i].Nodes[j]
			if n1.ID != n2.ID {
				t.Errorf("cluster %d, node %d: run1 ID=%q, run2 ID=%q", i, j, n1.ID, n2.ID)
			}
		}
	}
}

// Different seeds should generally produce different assignments for non-trivial inputs.
func TestKMeansDifferentSeedsCanDiffer(t *testing.T) {
	t.Parallel()
	// Use 20 spread-out nodes (5 clusters of 4): enough spread for seed to matter.
	nodes := makeGrid(20)
	const m, n = 5, 4

	run1 := SameSizeKMeansSeeded(nodes, m, n, 1)
	run2 := SameSizeKMeansSeeded(nodes, m, n, 999999)

	// Convert both results to a canonical signature string for comparison.
	sig := func(clusters []Cluster) string {
		s := ""
		for _, c := range clusters {
			for _, nd := range c.Nodes {
				s += nd.ID + "|"
			}
			s += ";"
		}
		return s
	}
	// We don't assert they MUST differ (they might converge to the same local
	// optimum), but we log it so test output is informative.
	if sig(run1) == sig(run2) {
		t.Log("note: both seeds converged to the same assignment (acceptable)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster ID assignment correctness
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansClusterIDsAreZeroBased(t *testing.T) {
	t.Parallel()
	nodes := makeGrid(12)
	clusters := SameSizeKMeansSeeded(nodes, 3, 4, 42)

	ids := make(map[int]bool)
	for _, c := range clusters {
		ids[c.ID] = true
	}
	for i := 0; i < 3; i++ {
		if !ids[i] {
			t.Errorf("cluster ID %d missing from result — IDs must be 0-based", i)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EuclideanDistance correctness
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansEuclideanDistance(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b Point
		want float64
	}{
		{Point{X: 0, Y: 0}, Point{X: 3, Y: 4}, 5.0},        // 3-4-5 triangle
		{Point{X: 0, Y: 0}, Point{X: 0, Y: 0}, 0.0},        // same point
		{Point{X: 1, Y: 1}, Point{X: 4, Y: 5}, 5.0},        // (3,4) vector
		{Point{X: -1, Y: -1}, Point{X: 2, Y: 3}, 5.0},      // negative coords
		{Point{X: 0, Y: 0}, Point{X: 1, Y: 1}, math.Sqrt2}, // unit diagonal
	}
	for _, tc := range cases {
		got := EuclideanDistance(tc.a, tc.b)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("EuclideanDistance(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// KMeansPlusPlus: returns exactly m centroids, all from the input set
// ─────────────────────────────────────────────────────────────────────────────

func TestKMeansPlusPlusReturnsMCentroids(t *testing.T) {
	t.Parallel()
	nodes := makeGrid(12)
	centroids := KMeansPlusPlus(nodes, 3)
	if len(centroids) != 3 {
		t.Fatalf("KMeansPlusPlus returned %d centroids, want 3", len(centroids))
	}
	// Each centroid must come from the input node set (coordinates match).
	nodeSet := make(map[[2]float64]bool)
	for _, n := range nodes {
		nodeSet[[2]float64{n.X, n.Y}] = true
	}
	for i, c := range centroids {
		key := [2]float64{c.X, c.Y}
		if !nodeSet[key] {
			t.Errorf("centroid %d (%v) is not a member of the input node set", i, c)
		}
	}
}