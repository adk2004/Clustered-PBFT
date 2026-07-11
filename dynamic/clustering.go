// Package dynamic implements Algorithm 2 from paper Section XII:
// dynamic clustering for node growth and mobility without system downtime.
//
// Core idea (paper Section XII):
//
//	"To mitigate the impact of re-clustering on system availability, we
//	 introduce a 'tick mode'. During tick mode, cluster leaders continue
//	 to process requests and maintain service queues. Once re-clustering
//	 completes, the system exits tick mode, resuming normal operations."
//
// TickMode manages the full lifecycle:
//
//	tick arrives → enter tick mode → update positions / add nodes →
//	re-run SameSizeKMeans → re-elect leaders → forward queued ops →
//	exit tick mode → resume normal operation
//
// The computational complexity of the clustering process is O(p·k·t) where
// p = total nodes, k = clusters, t = iterations (paper Section XII).
// We use t = 10 (config.KMeansMaxIter defaults to 100; the paper uses 10).
package dynamic

import (
	"fmt"
	"math"
	"sync"

	"github.com/adk2004/vehicular-bft/cluster"
	nodemod "github.com/adk2004/vehicular-bft/node"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tick — one time-period descriptor
// ─────────────────────────────────────────────────────────────────────────────

// Tick describes what happens during one time period in the vehicular network.
type Tick struct {
	// NewNodes lists new vehicles that joined the network this tick.
	// At most MaxPerTick entries are processed; excess are silently dropped.
	// Each Point.ID must be globally unique and not already in the network.
	NewNodes []cluster.Point

	// MovedNodes maps nodeID → updated geographic position for nodes that
	// changed location this tick (e.g. vehicles driving to a new area).
	// Only IDs present in the current node list are updated; unknown IDs are
	// ignored.
	MovedNodes map[string]cluster.Point
}

// ─────────────────────────────────────────────────────────────────────────────
// TickMode — the dynamic clustering manager
// ─────────────────────────────────────────────────────────────────────────────

// TickMode wraps the current cluster state and manages re-clustering across
// ticks. It is the single source of truth for which nodes belong to which
// cluster and who their leader is.
type TickMode struct {
	// CurrentClusters is the most recently computed cluster assignment.
	// Updated atomically at the end of each ProcessTick call.
	CurrentClusters []cluster.Cluster

	// Nodes holds a Node object for every vehicle in the network.
	// Indexed to match NodePoints: Nodes[i] corresponds to NodePoints[i].
	Nodes []*nodemod.Node

	// NodePoints holds the 2-D geographic position for each vehicle,
	// parallel to Nodes. Updated in-place by ProcessTick when nodes move.
	NodePoints []cluster.Point

	// Leaders maps clusterID → the elected leader's Point for the current tick.
	Leaders map[int]cluster.Point

	// TickDuration is the nominal wall-clock duration of one tick (seconds).
	// Informational; ProcessTick does not sleep for this duration.
	TickDuration int

	// MaxPerTick (T) is the upper bound on new nodes admitted per tick.
	// Paper Section XIV: T = 4.
	MaxPerTick int

	// ServiceQueue holds pending operations per cluster that arrived while
	// tick mode was active. Keyed by cluster ID; cleared after forwarding.
	// Callers enqueue via EnqueueOperation; ProcessTick drains the queue.
	ServiceQueue map[int][]string

	// inTickMode is true while a ProcessTick call is executing.
	// Read via InTickMode(); not accessed directly by callers.
	inTickMode bool

	// seed for SameSizeKMeansSeeded; incremented each tick so successive
	// ticks explore different initial centroid placements.
	seed int64

	mu sync.Mutex
}

// NewTickMode constructs a TickMode from an initial cluster assignment.
//
//	clusters     — initial cluster assignment (from SameSizeKMeans).
//	nodes        — Node objects in the same order as the flat point list.
//	               len(nodes) must equal total nodes across all clusters.
//	tickDuration — nominal seconds per tick (informational).
//	maxPerTick   — T, max new nodes admitted per tick (paper T=4).
func NewTickMode(
	clusters []cluster.Cluster,
	nodes []*nodemod.Node,
	tickDuration, maxPerTick int,
) *TickMode {
	// Flatten points in cluster-major, node-index order so NodePoints[i]
	// corresponds to Nodes[i].
	points := flattenPoints(clusters)

	leaders := cluster.ElectAllLeaders(clusters)

	return &TickMode{
		CurrentClusters: clusters,
		Nodes:           nodes,
		NodePoints:      points,
		Leaders:         leaders,
		TickDuration:    tickDuration,
		MaxPerTick:      maxPerTick,
		ServiceQueue:    make(map[int][]string),
		seed:            42,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

// EnqueueOperation adds op to the service queue for clusterID.
// Called by the protocol layer when a new request arrives while tick mode is
// active — the request cannot be processed until re-clustering finishes.
func (tm *TickMode) EnqueueOperation(clusterID int, op string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.ServiceQueue[clusterID] = append(tm.ServiceQueue[clusterID], op)
}

// InTickMode returns true while a ProcessTick call is executing.
// The protocol layer should enqueue requests rather than executing them.
func (tm *TickMode) InTickMode() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.inTickMode
}

// PendingCount returns the total number of operations currently queued.
func (tm *TickMode) PendingCount() int {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	n := 0
	for _, ops := range tm.ServiceQueue {
		n += len(ops)
	}
	return n
}

// ProcessTick executes one tick of Algorithm 2 (paper Section XII).
//
// Steps:
//  1. Enter tick mode  (leaders keep serving queued requests externally).
//  2. Apply node movements  (update NodePoints + Node.Location).
//  3. Admit up to MaxPerTick new nodes.
//  4. Re-run SameSizeKMeans to obtain new cluster assignments.
//  5. Re-elect leaders via ElectAllLeaders.
//  6. Forward pending ServiceQueue operations to their new leaders.
//  7. Commit new state; exit tick mode.
//
// Returns the new cluster assignments and the updated leader map.
// The caller should rebuild PBFTInstances from the returned clusters.
func (tm *TickMode) ProcessTick(t Tick) ([]cluster.Cluster, map[int]cluster.Point, error) {
	// ── Step 1: enter tick mode ────────────────────────────────────────────
	tm.mu.Lock()
	tm.inTickMode = true
	tm.mu.Unlock()

	defer func() {
		tm.mu.Lock()
		tm.inTickMode = false
		tm.mu.Unlock()
	}()

	// ── Step 2: apply node movements ──────────────────────────────────────
	if len(t.MovedNodes) > 0 {
		for i, pt := range tm.NodePoints {
			newPos, moved := t.MovedNodes[pt.ID]
			if !moved {
				continue
			}
			tm.NodePoints[i] = newPos
			// Sync Node.Location so the node object is consistent.
			for _, nd := range tm.Nodes {
				if nd.ID == pt.ID {
					nd.Location = newPos
					break
				}
			}
		}
	}

	// ── Step 3: admit new nodes ────────────────────────────────────────────
	toAdd := t.NewNodes
	if len(toAdd) > tm.MaxPerTick {
		toAdd = toAdd[:tm.MaxPerTick]
	}
	for _, pt := range toAdd {
		// Duplicate check: skip if the ID already exists.
		if tm.hasNode(pt.ID) {
			continue
		}
		tm.NodePoints = append(tm.NodePoints, pt)

		// Create a minimal Node object for the new vehicle.
		// Its cluster/node indices will be reassigned after re-clustering.
		newNode, err := nodemod.NewNode(pt.ID, nodemod.RoleReplica, -1, 0, pt)
		if err != nil {
			return nil, nil, fmt.Errorf("ProcessTick: NewNode(%s): %w", pt.ID, err)
		}
		tm.Nodes = append(tm.Nodes, newNode)
	}

	// ── Step 4: re-cluster ────────────────────────────────────────────────
	p := len(tm.NodePoints)
	n, m := cluster.ComputeDimensions(p)

	newClusters := cluster.SameSizeKMeansSeeded(tm.NodePoints, m, n, tm.seed)

	// ── Step 5: re-elect leaders ──────────────────────────────────────────
	newLeaders := cluster.ElectAllLeaders(newClusters)

	// ── Step 6: forward pending service-queue operations ──────────────────
	tm.mu.Lock()
	pendingToForward := make(map[int][]string, len(tm.ServiceQueue))
	for k, v := range tm.ServiceQueue {
		pendingToForward[k] = v
	}
	// Clear the queue.
	tm.ServiceQueue = make(map[int][]string)
	tm.mu.Unlock()

	// Forward each cluster's pending ops to the new leader of the cluster
	// whose centroid is closest to the old leader's position.
	for oldClusterID, ops := range pendingToForward {
		if len(ops) == 0 {
			continue
		}
		oldLeader, exists := tm.Leaders[oldClusterID]
		if !exists {
			continue
		}
		newClusterID := findClosestCluster(oldLeader, newClusters)
		if newClusterID < 0 {
			continue
		}
		// In production: send each op via network to newLeaders[newClusterID].
		// Here: enqueue under the new cluster ID so the caller can process them.
		tm.mu.Lock()
		tm.ServiceQueue[newClusterID] = append(tm.ServiceQueue[newClusterID], ops...)
		tm.mu.Unlock()
	}

	// ── Step 7: commit new state ──────────────────────────────────────────
	tm.CurrentClusters = newClusters
	tm.Leaders = newLeaders
	tm.seed++ // advance seed for the next tick

	return newClusters, newLeaders, nil
}

// ForwardPendingRequests transfers all pending operations queued under
// oldLeaderID's cluster to the node at newLeaderAddr.
//
// In production this calls network.Send for each operation; here it
// clears the ServiceQueue and returns the count of forwarded ops.
// Used by the protocol layer after a view change or leader migration.
func (tm *TickMode) ForwardPendingRequests(oldLeaderID string, newLeaderAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Find the cluster that oldLeaderID belongs to in the current assignment.
	targetCluster := -1
	for _, c := range tm.CurrentClusters {
		for _, pt := range c.Nodes {
			if pt.ID == oldLeaderID {
				targetCluster = c.ID
				break
			}
		}
		if targetCluster >= 0 {
			break
		}
	}

	if targetCluster < 0 {
		// oldLeaderID not found — may have left the network; clear all queues.
		for k := range tm.ServiceQueue {
			delete(tm.ServiceQueue, k)
		}
		return nil
	}

	ops, exists := tm.ServiceQueue[targetCluster]
	if !exists || len(ops) == 0 {
		return nil
	}

	// Production would call: network.Send(newLeaderAddr, envelope) per op.
	// Simulation: acknowledge and clear.
	_ = newLeaderAddr
	delete(tm.ServiceQueue, targetCluster)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// flattenPoints collects all node points from clusters in cluster-major order.
func flattenPoints(clusters []cluster.Cluster) []cluster.Point {
	pts := make([]cluster.Point, 0)
	for _, c := range clusters {
		pts = append(pts, c.Nodes...)
	}
	return pts
}

// hasNode returns true if a node with the given ID already exists.
func (tm *TickMode) hasNode(id string) bool {
	for _, pt := range tm.NodePoints {
		if pt.ID == id {
			return true
		}
	}
	return false
}

// findClosestCluster returns the ID of the cluster whose centroid is nearest
// to pt. Returns -1 if clusters is empty.
func findClosestCluster(pt cluster.Point, clusters []cluster.Cluster) int {
	best := -1
	bestDist := math.MaxFloat64
	for _, c := range clusters {
		d := cluster.EuclideanDistance(pt, c.Centroid)
		if d < bestDist {
			bestDist = d
			best = c.ID
		}
	}
	return best
}