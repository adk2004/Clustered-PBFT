// Package cluster implements geographic node clustering for the vehicular BFT protocol.
//
// The core algorithm is the Same-size K-Means method described in paper Section V-B
// (Algorithm 1). It is an adaptation of standard K-Means that enforces an equal
// number of nodes per cluster, which is essential for:
//   - Balanced fault-tolerance thresholds (f_local = floor((n-1)/3) is the same
//     for every cluster).
//   - Fair message-load distribution across clusters.
//
// Centroid initialisation uses the K-Means++ strategy (Arthur & Vassilvitskii 2007,
// paper ref [30]) to improve convergence speed and cluster quality over random seeding.
//
// Public API:
//
//	ComputeDimensions(p)               → (n, m)  — paper eq. (1)
//	KMeansPlusPlus(nodes, m)           → m seed centroids
//	SameSizeKMeans(nodes, m, n)        → m balanced clusters (random seed)
//	SameSizeKMeansSeeded(nodes,m,n,s)  → m balanced clusters (fixed seed, for tests)
//	EuclideanDistance(a, b)            → float64
package cluster

import (
	"math"
	"math/rand"
	"time"

	"github.com/adk2004/vehicular-bft/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// Core data types
// ─────────────────────────────────────────────────────────────────────────────

// Point represents a node's 2-D geographic coordinates in the vehicular network.
// ID must be unique across the entire network and matches node.Node.ID.
type Point struct {
	ID string
	X  float64
	Y  float64
}

// Cluster is a geographic group of nodes sharing the same consensus instance.
// Centroid is the arithmetic mean of all node positions in the cluster (updated
// after each K-Means iteration). Nodes is ordered by assignment time.
type Cluster struct {
	// ID is the cluster index i (0-based), matching ClusterState.ClusterID.
	ID int

	// Centroid is the geometric centre of the cluster, recomputed each iteration.
	// Its ID field is empty — it is a synthetic point, not a real node.
	Centroid Point

	// Nodes are the p/m actual nodes assigned to this cluster.
	Nodes []Point
}

// ─────────────────────────────────────────────────────────────────────────────
// EuclideanDistance
// ─────────────────────────────────────────────────────────────────────────────

// EuclideanDistance returns the straight-line distance between two 2-D points.
// Used by both the K-Means assignment step and the leader election (Phase 4).
func EuclideanDistance(a, b Point) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// ─────────────────────────────────────────────────────────────────────────────
// ComputeDimensions — paper eq. (1)
// ─────────────────────────────────────────────────────────────────────────────

// ComputeDimensions derives (n, m) from the total node count p.
//
// Paper equation (1):
//
//	n = ⌊√p⌋,   m = ⌊p/n⌋
//
// The MINSIZE = 4 constraint from Section V-A is enforced: PBFT requires at least
// 3f+1 nodes, and f_min = 1 → n_min = 4.  If the formula gives n < 4 we clamp n
// to 4 and recompute m accordingly.
//
// Examples (matching plan test cases):
//
//	ComputeDimensions(8)  → n=4, m=2   (√8≈2.8 → 2 < MINSIZE → n=4, m=8/4=2)
//	ComputeDimensions(12) → n=4, m=3   (√12≈3.4 → 3 < MINSIZE → n=4, m=12/4=3)
//	ComputeDimensions(16) → n=4, m=4   (√16=4,  m=16/4=4)
//	ComputeDimensions(20) → n=4, m=5   (√20≈4.4 → 4, m=20/4=5)
func ComputeDimensions(p int) (n int, m int) {
	n = int(math.Floor(math.Sqrt(float64(p))))
	if n < config.MINSIZE {
		n = config.MINSIZE
	}
	m = p / n // integer floor division
	return n, m
}

// ─────────────────────────────────────────────────────────────────────────────
// K-Means++ seeding — paper ref [30]
// ─────────────────────────────────────────────────────────────────────────────

// KMeansPlusPlus selects m diverse seed centroids from nodes using the K-Means++
// probability-weighted strategy (Arthur & Vassilvitskii 2007).
//
// Algorithm:
//  1. Choose the first centroid uniformly at random from nodes.
//  2. For each subsequent centroid, choose node x with probability proportional
//     to D(x)² where D(x) is the minimum distance from x to any chosen centroid.
//  3. Repeat until m centroids are chosen.
func KMeansPlusPlus(nodes []Point, m int) []Point {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return kMeansPlusPlusWithRand(nodes, m, rng)
}

// kMeansPlusPlusWithRand is the internal implementation that accepts a caller-
// supplied *rand.Rand, enabling deterministic unit tests.
func kMeansPlusPlusWithRand(nodes []Point, m int, rng *rand.Rand) []Point {
	if m <= 0 || len(nodes) == 0 {
		return nil
	}
	if m > len(nodes) {
		m = len(nodes)
	}

	centroids := make([]Point, 0, m)

	// Step 1: first centroid chosen uniformly at random.
	centroids = append(centroids, nodes[rng.Intn(len(nodes))])

	// Steps 2..m: each subsequent centroid chosen with probability proportional to D(x)².
	for len(centroids) < m {
		dists := make([]float64, len(nodes))
		total := 0.0
		for i, node := range nodes {
			minDist := math.MaxFloat64
			for _, c := range centroids {
				d := EuclideanDistance(node, c)
				if d < minDist {
					minDist = d
				}
			}
			dists[i] = minDist * minDist
			total += dists[i]
		}

		chosen := len(nodes) - 1 // safe fallback index
		if total > 0 {
			r := rng.Float64() * total
			cumulative := 0.0
			for i, d := range dists {
				cumulative += d
				if cumulative >= r {
					chosen = i
					break
				}
			}
		} else {
			// Degenerate: all distances zero (duplicate node positions).
			// Pick any node not already a centroid.
			usedIDs := make(map[string]bool, len(centroids))
			for _, c := range centroids {
				usedIDs[c.ID] = true
			}
			for i, node := range nodes {
				if !usedIDs[node.ID] {
					chosen = i
					break
				}
			}
		}
		centroids = append(centroids, nodes[chosen])
	}

	return centroids
}

// ─────────────────────────────────────────────────────────────────────────────
// Same-size K-Means — paper Section V-B, Algorithm 1
// ─────────────────────────────────────────────────────────────────────────────

// SameSizeKMeans partitions nodes into m clusters of n nodes each.
// Uses a wall-clock random seed. For reproducible results call SameSizeKMeansSeeded.
func SameSizeKMeans(nodes []Point, m int, n int) []Cluster {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return sameSizeKMeansCore(nodes, m, n, rng)
}

// SameSizeKMeansSeeded is the deterministic variant: the same (nodes, m, n, seed)
// always produces identical cluster assignments (plan test 7).
func SameSizeKMeansSeeded(nodes []Point, m int, n int, seed int64) []Cluster {
	rng := rand.New(rand.NewSource(seed))
	return sameSizeKMeansCore(nodes, m, n, rng)
}

// sameSizeKMeansCore is the shared implementation (Algorithm 1 from the paper).
//
//  1. Seed m centroids with K-Means++ (line 5).
//  2. For each iteration:
//     a. Assign each node to nearest non-full cluster (lines 7-18).
//     b. Recompute centroids as cluster means (lines 19-21).
//     c. Stop if convergence criterion met or KMeansMaxIter exhausted (line 22).
func sameSizeKMeansCore(nodes []Point, m, n int, rng *rand.Rand) []Cluster {
	if len(nodes) == 0 || m == 0 || n == 0 {
		return nil
	}

	// Step 1 — seed centroids.
	centroids := kMeansPlusPlusWithRand(nodes, m, rng)

	var clusters []Cluster

	// Main loop — assign → recompute → convergence check.
	for iter := 0; iter < config.KMeansMaxIter; iter++ {
		clusters = assignNodesToClusters(nodes, centroids, n)
		newCentroids := recomputeCentroids(clusters, centroids)

		if centroidsConverged(centroids, newCentroids) {
			centroids = newCentroids
			break
		}
		centroids = newCentroids
	}

	// Final assignment using the converged centroids so Cluster.Centroid is current.
	clusters = assignNodesToClusters(nodes, centroids, n)
	for i := range clusters {
		clusters[i].ID = i
		clusters[i].Centroid = centroids[i]
	}

	return clusters
}

// ─────────────────────────────────────────────────────────────────────────────
// assignNodesToClusters — Algorithm 1 lines 7–18
// ─────────────────────────────────────────────────────────────────────────────

// assignNodesToClusters assigns each node to the nearest non-full cluster.
// A cluster is "full" when it already holds n nodes; full clusters are excluded
// from further assignment (paper: "Remove (Cj, sj) from C").
//
// When len(nodes) == m*n, each cluster ends up with exactly n nodes.
// When len(nodes) > m*n (remainder nodes), excess nodes fall back to the
// least-loaded cluster so no node is ever lost.
func assignNodesToClusters(nodes []Point, centroids []Point, n int) []Cluster {
	m := len(centroids)
	clusters := make([]Cluster, m)
	for i := range clusters {
		clusters[i] = Cluster{
			ID:       i,
			Centroid: centroids[i],
			Nodes:    make([]Point, 0, n),
		}
	}

	sizes := make([]int, m)

	for _, node := range nodes {
		bestIdx := -1
		bestDist := math.MaxFloat64

		// Find nearest cluster still below capacity (Algorithm 1 lines 8-14).
		for j, c := range centroids {
			if sizes[j] >= n {
				continue // skip full cluster
			}
			d := EuclideanDistance(node, c)
			if d < bestDist {
				bestDist = d
				bestIdx = j
			}
		}

		// Fallback for remainder nodes: assign to cluster with fewest nodes,
		// tie-broken by lower index.
		if bestIdx == -1 {
			minSize := sizes[0]
			bestIdx = 0
			for j := 1; j < m; j++ {
				if sizes[j] < minSize {
					minSize = sizes[j]
					bestIdx = j
				}
			}
		}

		clusters[bestIdx].Nodes = append(clusters[bestIdx].Nodes, node)
		sizes[bestIdx]++
	}

	return clusters
}

// ─────────────────────────────────────────────────────────────────────────────
// recomputeCentroids — Algorithm 1 lines 19–21
// ─────────────────────────────────────────────────────────────────────────────

// recomputeCentroids computes the arithmetic mean position of each cluster's
// nodes. Empty clusters (degenerate input) preserve their previous centroid.
func recomputeCentroids(clusters []Cluster, prevCentroids []Point) []Point {
	newCentroids := make([]Point, len(clusters))
	for i, c := range clusters {
		if len(c.Nodes) == 0 {
			newCentroids[i] = prevCentroids[i] // preserve to avoid zero-division
			continue
		}
		sumX, sumY := 0.0, 0.0
		for _, node := range c.Nodes {
			sumX += node.X
			sumY += node.Y
		}
		count := float64(len(c.Nodes))
		newCentroids[i] = Point{X: sumX / count, Y: sumY / count}
	}
	return newCentroids
}

// ─────────────────────────────────────────────────────────────────────────────
// centroidsConverged
// ─────────────────────────────────────────────────────────────────────────────

// centroidsConverged returns true when every centroid has moved by less than
// config.KMeansConvergenceDelta — the stopping criterion from plan pitfall #6.
func centroidsConverged(oldC, newC []Point) bool {
	for i := range oldC {
		if EuclideanDistance(oldC[i], newC[i]) >= config.KMeansConvergenceDelta {
			return false
		}
	}
	return true
}