// Package config holds all project-wide constants derived directly from the paper
// "An Efficient and Scalable Byzantine Fault Tolerant Consensus for Vehicular Networks".
package config

const (
	// MINSIZE is the minimum number of nodes per cluster.
	// With f_min = 1, PBFT requires at least 3f+1 = 4 nodes (paper Section V-A).
	MINSIZE = 4

	// MaxFaultyRatio is the upper bound on faulty nodes as a fraction: f <= (n-1)/3.
	// Used as a sanity-check constant; actual f values are computed as floor((n-1)/3).
	MaxFaultyRatio = 1.0 / 3.0

	// TickDefault is the default tick duration in seconds for dynamic clustering
	// (paper Section XII, Algorithm 2).
	TickDefault = 10

	// MaxNodesPerTick (T) is the maximum number of new nodes that may join per tick
	// in the dynamic clustering experiments (paper Section XIV — T=4).
	MaxNodesPerTick = 4

	// RSABits is the key size used for all RSA key pairs issued by NodeCA
	// (paper Section IV-A-2: "All keypairs issued follow the RSA cryptosystem").
	RSABits = 2048

	// TCPBasePort is the base port for TCP node listeners.
	// Node with index i listens on TCPBasePort + i (e.g. node 0 → 9000, node 1 → 9001).
	TCPBasePort = 9000

	// KMeansMaxIter caps the number of iterations in Same-size K-Means to prevent
	// infinite loops on pathological inputs (paper Section V-B: "until convergence").
	KMeansMaxIter = 100

	// KMeansConvergenceDelta is the centroid-movement threshold below which
	// K-Means is declared converged (pitfall note in the plan).
	KMeansConvergenceDelta = 0.001
)