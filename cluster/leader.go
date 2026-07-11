// Leader election for the vehicular BFT protocol — paper Section V-C.
//
// "The leader node of each cluster is the node with the minimum Euclidean
// distance to the cluster's centroid."
//
// Design decisions (documented here for the test suite and future phases):
//
//  1. Tie-breaking: when two or more nodes share the minimum distance to the
//     centroid, the node with the lower index in Cluster.Nodes wins. This is
//     deterministic because SameSizeKMeans assigns nodes in a fixed order.
//
//  2. Return value of ElectLeader: the index j into Cluster.Nodes, not the
//     node's own ID or a pointer. Callers can then do c.Nodes[j] to get the
//     Point. Returning -1 signals an empty cluster (guard against panics).
//
//  3. ElectAllLeaders returns map[clusterID]Point rather than map[clusterID]int
//     so callers (protocol layer, dynamic clustering) never need to look up the
//     cluster again — the elected leader Point is immediately usable.
package cluster

import "math"

// ElectLeader returns the index j (into c.Nodes) of the node closest in
// Euclidean distance to c.Centroid (paper Section V-C, Algorithm 1 lines 23-30).
//
// Tie-breaking rule: lowest index in Nodes wins (deterministic, documented above).
// Returns -1 if the cluster has no nodes.
func ElectLeader(c Cluster) int {
	if len(c.Nodes) == 0 {
		return -1
	}

	bestIdx := 0
	bestDist := EuclideanDistance(c.Nodes[0], c.Centroid)

	for j := 1; j < len(c.Nodes); j++ {
		d := EuclideanDistance(c.Nodes[j], c.Centroid)
		// Strict less-than: first (lowest-index) minimum wins on ties.
		if d < bestDist {
			bestDist = d
			bestIdx = j
		}
	}

	return bestIdx
}

// ElectAllLeaders runs ElectLeader for every cluster and returns a map from
// cluster ID to the elected leader's Point.
//
// The returned map always has len(clusters) entries (one per cluster).
// Empty clusters produce no entry in the map (ElectLeader returns -1).
//
// Used by:
//   - Protocol initialisation (Phase 8) to know which node drives pre-prepare.
//   - Dynamic clustering (Phase 9) to detect leader changes after re-clustering.
func ElectAllLeaders(clusters []Cluster) map[int]Point {
	leaders := make(map[int]Point, len(clusters))
	for _, c := range clusters {
		idx := ElectLeader(c)
		if idx < 0 {
			continue // skip empty clusters
		}
		leaders[c.ID] = c.Nodes[idx]
	}
	return leaders
}

// LeaderDistance returns the Euclidean distance from the elected leader to the
// cluster centroid. Useful for logging and assertions in tests.
// Returns math.MaxFloat64 for an empty cluster.
func LeaderDistance(c Cluster) float64 {
	idx := ElectLeader(c)
	if idx < 0 {
		return math.MaxFloat64
	}
	return EuclideanDistance(c.Nodes[idx], c.Centroid)
}