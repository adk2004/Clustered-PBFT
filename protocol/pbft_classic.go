// pbft_classic.go implements a classic (non-clustered) PBFT simulator for
// performance comparison against the paper's clustered protocol.
//
// Classic PBFT puts ALL p nodes into a single consensus round:
//
//   - Every phase (PrePrepare, Prepare, Commit) involves ALL p nodes.
//   - Message complexity is O(p²) per consensus round.
//   - Per-phase latency grows linearly with p (more messages to verify/process).
//
// This is used exclusively for generating the "Classic PBFT" baseline in the
// performance comparison graphs (Figures 4-7). The clustered protocol's
// advantage — O(n^1.5) messages instead of O(n²) — becomes visible because
// the clustered variant runs small intra-cluster PBFTs (√p nodes each) in
// parallel, while classic PBFT must process all p nodes sequentially in each
// phase.
package protocol

import (
	"context"
	"fmt"
	"time"

	"github.com/adk2004/vehicular-bft/cluster"
	"github.com/adk2004/vehicular-bft/messages"
	"github.com/adk2004/vehicular-bft/node"
)

// ─────────────────────────────────────────────────────────────────────────────
// ClassicPBFTSimulator
// ─────────────────────────────────────────────────────────────────────────────

// ClassicPBFTSimulator wraps a single PBFTInstance containing ALL p nodes
// to simulate traditional (non-clustered) PBFT consensus.
//
// The key difference from the clustered protocol:
//   - One PBFT round involves ALL p nodes (not just √p per cluster).
//   - Per-phase delay scales with p, not √p.
//   - This naturally produces higher latency and lower throughput at scale.
type ClassicPBFTSimulator struct {
	// Instance is a single PBFTInstance containing ALL nodes.
	Instance *PBFTInstance

	// NodeCount is the total number of nodes (p).
	NodeCount int

	// PerMsgDelayUs is the simulated per-message processing delay in
	// microseconds. Each phase involves NodeCount messages, so the total
	// delay per phase = PerMsgDelayUs × NodeCount.
	// Default: 100µs (0.1ms) per message.
	PerMsgDelayUs int
}

// NewClassicPBFT constructs a ClassicPBFTSimulator from a flat list of nodes.
//
// Parameters:
//
//	allNodes     — all p nodes in the network; allNodes[0] is elected leader.
//	fGlobal      — floor((p-1)/3), the Byzantine fault threshold.
//	perMsgDelayUs — microseconds of simulated processing per message per phase.
//	                Pass 0 to use the default (100µs).
func NewClassicPBFT(
	allNodes []*node.Node,
	fGlobal int,
	perMsgDelayUs int,
) *ClassicPBFTSimulator {
	if perMsgDelayUs <= 0 {
		perMsgDelayUs = 100 // default 0.1ms per message
	}

	leader := allNodes[0]
	replicas := allNodes[1:]

	inst := NewPBFTInstance(leader, replicas, fGlobal, nil)
	// Classic PBFT phase delay scales with ALL nodes, not just a cluster.
	// This is the critical difference: PhaseDelayMs = perMsgDelay × nodeCount.
	inst.PhaseDelayMs = (perMsgDelayUs * len(allNodes)) / 1000
	if inst.PhaseDelayMs < 1 && len(allNodes) > 1 {
		inst.PhaseDelayMs = 1 // ensure at least 1ms at non-trivial sizes
	}

	return &ClassicPBFTSimulator{
		Instance:      inst,
		NodeCount:     len(allNodes),
		PerMsgDelayUs: perMsgDelayUs,
	}
}

// RunConsensus executes one complete classic PBFT round with all p nodes.
//
// The round follows the standard PBFT phases but with phase delays
// proportional to p (the total node count), simulating the O(p²) message
// cost that makes classic PBFT slow at scale.
func (c *ClassicPBFTSimulator) RunConsensus(
	ctx context.Context,
	operation, clientID string,
) ([]messages.Reply, error) {
	return c.Instance.RunPBFT(ctx, operation, clientID)
}

// ─────────────────────────────────────────────────────────────────────────────
// RunClassicPBFTRound — convenience wrapper with timing
// ─────────────────────────────────────────────────────────────────────────────

// RunClassicPBFTRound runs a single consensus round and returns the duration.
// This is a convenience wrapper used by the benchmark harness.
func (c *ClassicPBFTSimulator) RunClassicPBFTRound(
	ctx context.Context,
	operation, clientID string,
) ([]messages.Reply, time.Duration, error) {
	start := time.Now()
	replies, err := c.RunConsensus(ctx, operation, clientID)
	elapsed := time.Since(start)
	return replies, elapsed, err
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder helper
// ─────────────────────────────────────────────────────────────────────────────

// BuildClassicPBFTNodes creates a flat list of p nodes (no clustering) for use
// with NewClassicPBFT. The first node is promoted to leader; the rest are
// replicas. All nodes have each other's public keys registered.
//
// This is separate from buildClusters in main.go because classic PBFT does
// not partition nodes into clusters.
func BuildClassicPBFTNodes(p int) ([]*node.Node, error) {
	if p < 4 {
		return nil, fmt.Errorf("BuildClassicPBFTNodes: need at least 4 nodes, got %d", p)
	}

	nodes := make([]*node.Node, p)
	for i := 0; i < p; i++ {
		role := node.RoleReplica
		if i == 0 {
			role = node.RoleLeader
		}
		nd, err := node.NewNode(
			fmt.Sprintf("classic-%d", i),
			role, 0, i,
			cluster.Point{ID: fmt.Sprintf("classic-%d", i), X: 0, Y: 0},
		)
		if err != nil {
			return nil, fmt.Errorf("BuildClassicPBFTNodes: node %d: %w", i, err)
		}
		nodes[i] = nd
	}

	// Register all public keys (every node knows every other node).
	for _, a := range nodes {
		for _, b := range nodes {
			if a.ID != b.ID {
				a.KnownKeys[b.ID] = b.PubKey
			}
		}
	}

	return nodes, nil
}
