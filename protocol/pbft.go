// Package protocol wires together the node, messages, and network packages into
// a working vehicular BFT consensus protocol.
//
// This file defines PBFTInstance — the engine that drives a single cluster
// through one complete Pre-Prepare → Prepare → Commit → Reply round.
//
// Two execution modes:
//
//  1. Simulation mode (Phase 8 tests): RunPBFT calls node handlers directly,
//     no TCP involved. Nodes are in-process goroutine-safe objects.
//
//  2. Production mode (future): SendFn is wired to network.Send; nodes run
//     as separate processes listening on their Addrs ports.
//
// Self-counting rule (matches PBFT spec, paper Section II-A):
//
//	A node counts its own Prepare/Commit toward the 2f+1 quorum.
//	Without this, n=3 honest nodes (from a 4-node cluster with 1 faulty)
//	can only collect 2 foreign Prepares, which falls short of quorum=3.
//	Self-counting is added explicitly after HandlePrePrepare/HandlePrepare.
package protocol

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/adk2004/vehicular-bft/messages"
	"github.com/adk2004/vehicular-bft/node"
)

const defaultPBFTTimeout = 10 * time.Second

// SendFn is the pluggable message-delivery function.
//   - Production: wraps network.Send(addr, env).
//   - Tests: nil (RunPBFT delivers directly to node handlers).
type SendFn func(addr string, env messages.Envelope) error

// ─────────────────────────────────────────────────────────────────────────────
// PBFTInstance
// ─────────────────────────────────────────────────────────────────────────────

// PBFTInstance manages one cluster's PBFT state machine for a single operation
// round. It is safe to call RunPBFT from multiple goroutines concurrently
// (each call is an independent round; opLog is mutex-protected).
type PBFTInstance struct {
	// Leader drives PrePrepare and processes VoteReplies.
	Leader *node.Node

	// Replicas are the honest replicas for this round.
	// Faulty nodes are simply absent — they receive no messages.
	Replicas []*node.Node

	// FLocal is the Byzantine fault threshold for this cluster.
	// MUST be set to FaultyThresholdLocal(totalClusterSize), NOT len(Replicas),
	// so excluding faulty nodes does not distort the 2f+1 quorum.
	FLocal int

	// Addrs maps nodeID → TCP address (for production SendFn).
	Addrs map[string]string

	// SendFn delivers messages (nil → simulation mode, delivery via RunPBFT).
	SendFn SendFn

	// Timeout caps each RunPBFT call.
	Timeout time.Duration

	// PhaseDelayMs injects an artificial sleep between PBFT phases to simulate
	// V2V/V2I message propagation latency on a single machine.
	// Set by the simulation package;
	PhaseDelayMs int

	// opLog maps sequence ID → operation string, used by the step-by-step API.
	opLog map[int]string
	mu    sync.Mutex

	// ── Reputation-Weighted Voting (RWV) extension ───────────────────────────
	// UseReputation activates reputation-weighted quorum when true.
	// When false the exact legacy path is used (zero-breakage guarantee).
	UseReputation bool

	// TotalClusterReputation is the sum of all node reputation scores in this cluster.
	// Required for HasReputationQuorum threshold computation.
	TotalClusterReputation int
}

// NewPBFTInstance creates a PBFTInstance.
//
//	fLocal  — floor((totalClusterSize-1)/3), e.g. 1 for a 4-node cluster.
//	addrs   — nodeID → "host:port"; may be nil in simulation mode.
func NewPBFTInstance(
	leader *node.Node,
	replicas []*node.Node,
	fLocal int,
	addrs map[string]string,
) *PBFTInstance {
	if addrs == nil {
		addrs = make(map[string]string)
	}
	return &PBFTInstance{
		Leader:   leader,
		Replicas: replicas,
		FLocal:   fLocal,
		Addrs:    addrs,
		Timeout:  defaultPBFTTimeout,
		opLog:    make(map[int]string),
	}
}

// allNodes returns [Leader] ++ Replicas.
func (p *PBFTInstance) allNodes() []*node.Node {
	all := make([]*node.Node, 0, 1+len(p.Replicas))
	all = append(all, p.Leader)
	all = append(all, p.Replicas...)
	return all
}

func (p *PBFTInstance) storeOp(seqID int, op string) {
	p.mu.Lock()
	p.opLog[seqID] = op
	p.mu.Unlock()
}

func (p *PBFTInstance) loadOp(seqID int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.opLog[seqID]
}

// ─────────────────────────────────────────────────────────────────────────────
// RunPBFT — full synchronous round (simulation mode)
// ─────────────────────────────────────────────────────────────────────────────

// RunPBFT executes a complete PBFT round for operation in simulation mode.
//
// Phases (paper Section II-A-1):
//
//  1. Leader calls StartPBFT → PrePrepare.
//  2. Every honest node HandlePrePrepare → Prepare + self-count own Prepare.
//  3. Every honest node processes peers' Prepares → Commit once 2f+1 reached.
//     Own Commit is self-counted immediately on production.
//  4. Every honest node processes peers' Commits → Reply once 2f+1 reached.
//
// Context cancellation is checked between phases so callers can enforce
// timeouts (used in the Byzantine-leader test).
//
// Returns: the set of Reply messages from nodes that reached commit quorum.
// An error is returned if any phase fails to produce messages or ctx fires.
func (p *PBFTInstance) RunPBFT(ctx context.Context, operation, clientID string) ([]messages.Reply, error) {
	all := p.allNodes()

	// ── Phase 1: PrePrepare ───────────────────────────────────────────────
	ppEnvPtr, err := p.Leader.StartPBFT(operation)
	if err != nil {
		return nil, fmt.Errorf("RunPBFT[%s]: StartPBFT failed: %w", p.Leader.ID, err)
	}
	ppEnv := *ppEnvPtr

	var pp messages.PrePrepare
	if err := messages.DecodeBody(ppEnv, &pp); err != nil {
		return nil, fmt.Errorf("RunPBFT: decode PrePrepare: %w", err)
	}
	seqID := pp.SequenceID
	p.storeOp(seqID, operation)

	// Simulate message propagation delay after PrePrepare broadcast.
	p.injectPhaseDelay()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("RunPBFT: cancelled before PrePrepare delivery: %w", err)
	}

	// ── Phase 2: PrePrepare → Prepare (with self-count) ───────────────────
	prepareEnvs := make([]messages.Envelope, 0, len(all))
	for _, nd := range all {
		pEnv, err := nd.HandlePrePrepare(ppEnv)
		if err != nil {
			continue // node rejected (mismatched view, bad digest, etc.)
		}
		// Self-count: this node's own Prepare counts toward its 2f+1 quorum.
		nd.AddPrepare(seqID, nd.ID)
		prepareEnvs = append(prepareEnvs, pEnv)
	}
	if len(prepareEnvs) == 0 {
		return nil, fmt.Errorf("RunPBFT: no Prepare produced — all nodes rejected PrePrepare")
	}

	// Simulate message propagation delay after Prepare broadcast.
	p.injectPhaseDelay()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("RunPBFT: cancelled after Prepare phase: %w", err)
	}

	// ── Phase 3: Prepare → Commit (cross-node, skip own = already counted) ─
	commitEnvs := make([]messages.Envelope, 0, len(all))
	commitProduced := make(map[string]bool)

	if p.UseReputation {
		// Reputation path — accumulate weight; produce commit once weighted quorum met.
		for _, nd := range all {
			for _, pEnv := range prepareEnvs {
				if pEnv.SenderID == nd.ID {
					continue // skip own — already self-counted in Phase 2
				}
				var prepare messages.Prepare
				messages.DecodeBody(pEnv, &prepare)
				nd.AddPrepare(prepare.SequenceID, pEnv.SenderID)
				if commitProduced[nd.ID] {
					continue
				}
				w := nd.GetPrepareWeight(prepare.SequenceID)
				if !node.HasReputationQuorum(w, p.TotalClusterReputation) {
					continue
				}
				// Weighted quorum met — try the standard HandlePrepare path first.
				cPtr, cerr := nd.HandlePrepare(pEnv, p.FLocal)
				if cerr != nil || cPtr == nil {
					// HandlePrepare returns nil when raw count < 2f+1; build commit directly.
					seqID := prepare.SequenceID
					commit := messages.Commit{
						SequenceID: seqID,
						ViewNumber: nd.GetViewNumber(),
						Digest:     prepare.Digest,
						NodeID:     nd.ID,
					}
					env, err := messages.NewEnvelope(messages.MsgCommit, nd.ID, commit, nd.PrivKey)
					if err != nil {
						continue
					}
					// Self-count own commit.
					nd.AddCommit(seqID, nd.ID)
					commitEnvs = append(commitEnvs, env)
					commitProduced[nd.ID] = true
					continue
				}
				// HandlePrepare produced a commit via count path — self-count it.
				var selfC messages.Commit
				if err := messages.DecodeBody(*cPtr, &selfC); err == nil {
					nd.AddCommit(selfC.SequenceID, nd.ID)
				}
				commitEnvs = append(commitEnvs, *cPtr)
				commitProduced[nd.ID] = true
			}
		}
	} else {
		// Legacy path — exact original logic, unchanged.
		for _, nd := range all {
			for _, pEnv := range prepareEnvs {
				if pEnv.SenderID == nd.ID {
					continue // skip own — already self-counted in Phase 2
				}
				cPtr, err := nd.HandlePrepare(pEnv, p.FLocal)
				if err != nil {
					continue
				}
				if cPtr == nil || commitProduced[nd.ID] {
					continue // quorum not yet reached for this node, or already committed
				}
				// Self-count own Commit immediately (mirrors the Prepare self-count).
				var selfC messages.Commit
				if err := messages.DecodeBody(*cPtr, &selfC); err == nil {
					nd.AddCommit(selfC.SequenceID, nd.ID)
				}
				commitEnvs = append(commitEnvs, *cPtr)
				commitProduced[nd.ID] = true
			}
		}
	}
	if len(commitEnvs) == 0 {
		return nil, fmt.Errorf("RunPBFT: no Commit produced — Prepare quorum not reached (%d prepares, need %d)",
			len(prepareEnvs), 2*p.FLocal+1)
	}

	// Simulate message propagation delay after Commit broadcast.
	p.injectPhaseDelay()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("RunPBFT: cancelled after Commit phase: %w", err)
	}

	// ── Phase 4: Commit → Reply (cross-node, skip own = already counted) ──
	replies := make([]messages.Reply, 0, len(all))
	replyProduced := make(map[string]bool)

	if p.UseReputation {
		// Reputation path — accumulate commit weight; produce reply once weighted quorum met.
		for _, nd := range all {
			for _, cEnv := range commitEnvs {
				if cEnv.SenderID == nd.ID {
					continue // skip own — self-counted when commit was produced
				}
				var commit messages.Commit
				messages.DecodeBody(cEnv, &commit)
				nd.AddCommit(commit.SequenceID, cEnv.SenderID)
				if replyProduced[nd.ID] {
					continue
				}
				w := nd.GetCommitWeight(commit.SequenceID)
				if !node.HasReputationQuorum(w, p.TotalClusterReputation) {
					continue
				}
				// Weighted commit quorum met — try standard HandleCommit first.
				rPtr, rerr := nd.HandleCommit(cEnv, p.FLocal, operation, clientID)
				if rerr != nil || rPtr == nil {
					// Build reply directly (HandleCommit gated on count quorum).
					nd.ApplyOperation(commit.SequenceID, operation)
					reply := messages.Reply{
						ViewNumber: commit.ViewNumber,
						Timestamp:  int64(commit.SequenceID),
						ClientID:   clientID,
						NodeID:     nd.ID,
						Result:     fmt.Sprintf("OK:%s", operation),
					}
					replies = append(replies, reply)
					replyProduced[nd.ID] = true
					continue
				}
				var r messages.Reply
				if err := messages.DecodeBody(*rPtr, &r); err == nil {
					replies = append(replies, r)
					replyProduced[nd.ID] = true
				}
			}
		}
	} else {
		// Legacy path — exact original logic, unchanged.
		for _, nd := range all {
			for _, cEnv := range commitEnvs {
				if cEnv.SenderID == nd.ID {
					continue // skip own — self-counted in Phase 3
				}
				rPtr, err := nd.HandleCommit(cEnv, p.FLocal, operation, clientID)
				if err != nil {
					continue
				}
				if rPtr == nil || replyProduced[nd.ID] {
					continue
				}
				var r messages.Reply
				if err := messages.DecodeBody(*rPtr, &r); err == nil {
					replies = append(replies, r)
					replyProduced[nd.ID] = true
				}
			}
		}
	}
	if len(replies) == 0 {
		return nil, fmt.Errorf("RunPBFT: no Reply produced — Commit quorum not reached (%d commits, need %d)",
			len(commitEnvs), 2*p.FLocal+1)
	}

	return replies, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Step-by-step API (production TCP path)
// ─────────────────────────────────────────────────────────────────────────────

// RunPrePrepare has the leader create a PrePrepare and dispatch it to all nodes.
// In production: message is sent via SendFn → network.Send.
func (p *PBFTInstance) RunPrePrepare(operation string) error {
	ppEnvPtr, err := p.Leader.StartPBFT(operation)
	if err != nil {
		return fmt.Errorf("RunPrePrepare: %w", err)
	}
	var pp messages.PrePrepare
	if err := messages.DecodeBody(*ppEnvPtr, &pp); err != nil {
		return fmt.Errorf("RunPrePrepare: decode: %w", err)
	}
	p.storeOp(pp.SequenceID, operation)
	return p.sendToAll(*ppEnvPtr)
}

// ReceivePrePrepare handles a PrePrepare at replicaID and broadcasts its Prepare.
func (p *PBFTInstance) ReceivePrePrepare(replicaID string, env messages.Envelope) error {
	nd := p.findNode(replicaID)
	if nd == nil {
		return fmt.Errorf("ReceivePrePrepare: unknown node %s", replicaID)
	}
	pEnv, err := nd.HandlePrePrepare(env)
	if err != nil {
		return fmt.Errorf("ReceivePrePrepare(%s): %w", replicaID, err)
	}
	return p.sendToAll(pEnv)
}

// ReceivePrepare handles a Prepare at receiverID.
// Broadcasts a Commit when the 2f+1 quorum is reached.
func (p *PBFTInstance) ReceivePrepare(receiverID string, env messages.Envelope) error {
	nd := p.findNode(receiverID)
	if nd == nil {
		return fmt.Errorf("ReceivePrepare: unknown node %s", receiverID)
	}
	cPtr, err := nd.HandlePrepare(env, p.FLocal)
	if err != nil {
		return fmt.Errorf("ReceivePrepare(%s): %w", receiverID, err)
	}
	if cPtr != nil {
		return p.sendToAll(*cPtr)
	}
	return nil
}

// ReceiveCommit handles a Commit at receiverID.
// Executes the operation and returns a Reply when 2f+1 commits received.
func (p *PBFTInstance) ReceiveCommit(receiverID string, env messages.Envelope, clientID string) (*messages.Reply, error) {
	nd := p.findNode(receiverID)
	if nd == nil {
		return nil, fmt.Errorf("ReceiveCommit: unknown node %s", receiverID)
	}
	var commit messages.Commit
	if err := messages.DecodeBody(env, &commit); err != nil {
		return nil, fmt.Errorf("ReceiveCommit: decode: %w", err)
	}
	op := p.loadOp(commit.SequenceID)
	rPtr, err := nd.HandleCommit(env, p.FLocal, op, clientID)
	if err != nil {
		return nil, fmt.Errorf("ReceiveCommit(%s): %w", receiverID, err)
	}
	if rPtr == nil {
		return nil, nil
	}
	var r messages.Reply
	if err := messages.DecodeBody(*rPtr, &r); err != nil {
		return nil, fmt.Errorf("ReceiveCommit: decode reply: %w", err)
	}
	return &r, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────


// injectPhaseDelay sleeps for PhaseDelayMs if set, simulating network propagation.
func (p *PBFTInstance) injectPhaseDelay() {
	if p.PhaseDelayMs > 0 {
		time.Sleep(time.Duration(p.PhaseDelayMs) * time.Millisecond)
	}
}


// sendToAll sends env to every node via SendFn (no-op if SendFn is nil).
func (p *PBFTInstance) sendToAll(env messages.Envelope) error {
	if p.SendFn == nil {
		return nil // simulation mode — RunPBFT delivers directly
	}
	for _, nd := range p.allNodes() {
		addr, ok := p.Addrs[nd.ID]
		if !ok {
			continue
		}
		_ = p.SendFn(addr, env) // best-effort; individual errors don't stop broadcast
	}
	return nil
}

// findNode returns the Node with the given ID, searching Leader then Replicas.
func (p *PBFTInstance) findNode(id string) *node.Node {
	if p.Leader.ID == id {
		return p.Leader
	}
	for _, r := range p.Replicas {
		if r.ID == id {
			return r
		}
	}
	return nil
}