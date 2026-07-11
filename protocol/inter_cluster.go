// inter_cluster.go implements the global state transition path (paper Section IX).
//
// When the Vote phase decides GLOBAL, the initiating cluster's leader:
//   1. Updates fault tolerance limit from f_local to f_global (Step 5).
//   2. Multicasts an InterClusterRequest to all other cluster leaders (Step 6).
//   3. All leaders (including the initiating one) run PBFT in their clusters
//      concurrently (Step 7).
//   4. The client waits for >= f_global+1 Reply messages (Step 8).
//
// GlobalCoordinator orchestrates steps 2-4 for in-process simulation.
package protocol

import (
	"context"
	"fmt"
	"sync"

	"github.com/adk2004/vehicular-bft/messages"
	"github.com/adk2004/vehicular-bft/node"
)

// ─────────────────────────────────────────────────────────────────────────────
// GlobalCoordinator
// ─────────────────────────────────────────────────────────────────────────────

// GlobalCoordinator orchestrates a global state transition across all clusters.
//
// It holds one PBFTInstance per cluster and runs them concurrently, then
// waits until enough Reply messages arrive to satisfy the global quorum.
type GlobalCoordinator struct {
	// Clusters holds one PBFTInstance per cluster, indexed by cluster ID.
	Clusters []*PBFTInstance

	// FGlobal is the global Byzantine fault threshold:
	//   f_global = floor((totalNodes - 1) / 3)
	// Client waits for f_global + 1 replies (paper Section IX Step 8,
	// plan pitfall #5: this is f+1, NOT 2f+1).
	FGlobal int

	// LeaderAddrs maps cluster index → leader TCP address.
	// Used to multicast InterClusterRequest in production mode.
	// May be nil in simulation mode.
	LeaderAddrs map[int]string
}

// NewGlobalCoordinator constructs a GlobalCoordinator.
//
//	clusters     — one PBFTInstance per cluster, length m.
//	fGlobal      — floor((p-1)/3) where p = total network nodes.
//	leaderAddrs  — clusterIdx → leader address (nil OK for simulation).
func NewGlobalCoordinator(
	clusters []*PBFTInstance,
	fGlobal int,
	leaderAddrs map[int]string,
) *GlobalCoordinator {
	if leaderAddrs == nil {
		leaderAddrs = make(map[int]string)
	}
	return &GlobalCoordinator{
		Clusters:    clusters,
		FGlobal:     fGlobal,
		LeaderAddrs: leaderAddrs,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RunGlobalTransition
// ─────────────────────────────────────────────────────────────────────────────

// RunGlobalTransition executes the global consensus path (paper Section IX).
//
// Concurrency: each cluster runs its own PBFT round in a separate goroutine.
// All goroutines are started before any results are awaited (true parallelism).
//
// Success condition (paper Section IX Step 8):
//
//	The client considers the operation committed when it receives
//	>= f_global + 1 Reply messages (plan pitfall #5).
//
// This function collects ALL replies from all clusters, then returns them.
// The caller can check HasQuorumGlobal on the result count.
//
// Parameters:
//
//	ctx           — cancellation context (deadline propagates to all clusters).
//	originCluster — index of the cluster that initiated the global request.
//	operation     — the operation string to commit network-wide.
//	clientID      — client's ID for Reply messages.
func (gc *GlobalCoordinator) RunGlobalTransition(
	ctx context.Context,
	originCluster int,
	operation, clientID string,
) ([]messages.Reply, error) {

	if len(gc.Clusters) == 0 {
		return nil, fmt.Errorf("RunGlobalTransition: no clusters registered")
	}

	type result struct {
		clusterIdx int
		replies    []messages.Reply
		err        error
	}

	resultCh := make(chan result, len(gc.Clusters))
	var wg sync.WaitGroup

	// Step 7 (paper): all cluster leaders run PBFT concurrently.
	for i, pbft := range gc.Clusters {
		wg.Add(1)
		go func(idx int, inst *PBFTInstance) {
			defer wg.Done()
			replies, err := inst.RunPBFT(ctx, operation, clientID)
			resultCh <- result{clusterIdx: idx, replies: replies, err: err}
		}(i, pbft)
	}

	// Close resultCh once all goroutines finish.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect all replies and errors.
	allReplies := make([]messages.Reply, 0, len(gc.Clusters)*4)
	var errs []error

	for res := range resultCh {
		if res.err != nil {
			errs = append(errs, fmt.Errorf("cluster %d: %w", res.clusterIdx, res.err))
			continue
		}
		allReplies = append(allReplies, res.replies...)
	}

	// Require that at least one cluster succeeded.
	if len(allReplies) == 0 {
		if len(errs) > 0 {
			return nil, fmt.Errorf("RunGlobalTransition: all clusters failed; first error: %w", errs[0])
		}
		return nil, fmt.Errorf("RunGlobalTransition: no replies collected")
	}

	// Step 8 (paper): verify the global quorum is met.
	if !node.HasQuorumGlobal(len(allReplies), gc.FGlobal) {
		return allReplies, fmt.Errorf(
			"RunGlobalTransition: global quorum not met — got %d replies, need %d (f_global+1, f=%d)",
			len(allReplies), gc.FGlobal+1, gc.FGlobal,
		)
	}

	return allReplies, nil
}

// ClusterLeaders returns the leader Node from each PBFTInstance, indexed by
// cluster ID. Useful for asserting that all leaders committed the same operation.
func (gc *GlobalCoordinator) ClusterLeaders() []*node.Node {
	leaders := make([]*node.Node, len(gc.Clusters))
	for i, c := range gc.Clusters {
		leaders[i] = c.Leader
	}
	return leaders
}