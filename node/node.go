// Package node defines the Node type representing a single vehicle/replica in
// the vehicular BFT network, along with fault-threshold helpers used throughout
// the protocol.
//
// Every node holds:
//   - Its RSA identity (generated at construction via NodeCA).
//   - A role: CLIENT, REPLICA, or LEADER.
//   - Its position within the cluster hierarchy (ClusterIdx i, NodeIdx j).
//   - A local copy of the protocol state: view number, sequence counter,
//     in-flight vote/prepare/commit tallies, and the committed operation log.
//   - A directory of all peer public keys (for signature verification).
package node

import (
	"crypto/rsa"
	"fmt"
	"math"
	"sync"

	"github.com/adk2004/vehicular-bft/cluster"
	"github.com/adk2004/vehicular-bft/crypto"
	"github.com/adk2004/vehicular-bft/state"
)

// ─────────────────────────────────────────────────────────────────────────────
// Role
// ─────────────────────────────────────────────────────────────────────────────

// Role classifies a node's function in the consensus protocol.
type Role string

const (
	// RoleClient is an external entity that submits requests.
	// Clients do not participate in voting or PBFT phases.
	RoleClient Role = "CLIENT"

	// RoleReplica is a standard participant that votes, prepares, and commits.
	RoleReplica Role = "REPLICA"

	// RoleLeader is a replica that additionally drives the pre-prepare phase
	// and coordinates the Vote / VoteReply exchange within its cluster.
	// Leader status is rotated (round-robin) on view change.
	RoleLeader Role = "LEADER"
)

// ─────────────────────────────────────────────────────────────────────────────
// Node
// ─────────────────────────────────────────────────────────────────────────────

// Node represents one vehicle/node in the vehicular BFT network.
//
// Thread-safety: Node uses RWMutex (mu) to protect concurrent access to mutable state:
//   - All reads/writes to ViewNumber, SequenceID, Role, Location use the mutex.
//   - All map operations (VoteReplies, Prepares, Commits, KnownKeys, executedSeqs)
//     use the mutex to prevent race conditions.
//   - LocalState access is protected during state mutations.
//
// Use RLock for read-only operations and Lock for modifications.
type Node struct {
	// ── Identity ────────────────────────────────────────────────────────────
	// ID is the globally unique node identifier (e.g. "node-0-2" = cluster 0, index 2).
	ID string

	// Role is the node's current function: CLIENT, REPLICA, or LEADER.
	Role Role

	// ── Cluster position ───────────────────────────────────────────────────
	// ClusterIdx is the cluster index i (0-based).
	ClusterIdx int

	// NodeIdx is the position j within the cluster (0-based).
	// The pair (ClusterIdx, NodeIdx) uniquely identifies a node within a network.
	NodeIdx int

	// Location is the node's current 2-D geographic coordinate.
	// Updated by the dynamic clustering phase when the vehicle moves.
	Location cluster.Point

	// ── Cryptographic identity ─────────────────────────────────────────────
	// PrivKey is the node's RSA private key — never shared.
	PrivKey *rsa.PrivateKey

	// PubKey is the node's RSA public key — distributed to all peers via NodeCA.
	PubKey *rsa.PublicKey

	// KnownKeys maps every other node ID to its RSA public key.
	// Populated during initialisation from the NodeCA key directory.
	// Used by ValidateEnvelope before processing any incoming message.
	KnownKeys map[string]*rsa.PublicKey

	// ── Protocol state ──────────────────────────────────────────────────────
	// ViewNumber is the current PBFT view (incremented on leader rotation).
	ViewNumber int

	// SequenceID is the monotonically increasing request counter.
	// Only the leader increments this; replicas track the highest seen.
	SequenceID int

	// LocalState is this node's individual state σ_ij (paper Section VI).
	LocalState state.NodeState

	// ── In-flight message tallies ──────────────────────────────────────────
	// VoteReplies accumulates VoteReply messages received during the Vote phase.
	// Keyed by sender node ID to prevent double-counting (plan pitfall #3).
	VoteReplies map[string]interface{}

	// Prepares accumulates Prepare messages keyed by sequenceID → senderID set.
	// Once len(Prepares[seq]) >= 2f+1 the node advances to Commit.
	Prepares map[int]map[string]bool

	// Commits accumulates Commit messages keyed by sequenceID → senderID set.
	// Once len(Commits[seq]) >= 2f+1 the node executes the operation.
	Commits map[int]map[string]bool

	// ── Committed log ──────────────────────────────────────────────────────
	// Log is the ordered list of committed operations applied to LocalState.
	// Append-only; never truncated during normal operation.
	Log []string

	// executedSeqs tracks which sequence IDs have already been executed so
	// that ApplyOperation is idempotent (plan test 4 in Phase 6).
	executedSeqs map[int]bool
	mu           sync.RWMutex

	// ── Dynamic Reputation-Driven PBFT (Algorithm 1) ─────────────────────────
	// Reputation (R_i) is this node's current dynamic trust score.
	Reputation float64

	// BaseReputation (R_init) is the immutable starting score used in
	// the logarithmic reward formula: ΔR_inc = α / (ln(1 + R_i - R_init) + β).
	BaseReputation float64

	// KnownReputations maps peer node IDs to their current reputation scores.
	// Updated each epoch via SyncReputations (Algorithm 1, Phase IV).
	KnownReputations map[string]float64
}

// ─────────────────────────────────────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────────────────────────────────────

// NewNode constructs and initialises a Node, generating a fresh RSA-2048 key
// pair automatically (simulating NodeCA key issuance, paper Section IV-A-2).
//
// Parameters:
//
//	id         – globally unique node identifier (e.g. "node-2-3")
//	role       – CLIENT, REPLICA, or LEADER
//	clusterIdx – cluster index i
//	nodeIdx    – position j within the cluster
//	loc        – 2-D geographic location
func NewNode(id string, role Role, clusterIdx int, nodeIdx int, loc cluster.Point) (*Node, error) {
	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("node.NewNode(%s): key generation failed: %w", id, err)
	}

	n := &Node{
		ID:           id,
		Role:         role,
		ClusterIdx:   clusterIdx,
		NodeIdx:      nodeIdx,
		Location:     loc,
		PrivKey:      priv,
		PubKey:       pub,
		KnownKeys:    make(map[string]*rsa.PublicKey),
		ViewNumber:   0,
		SequenceID:   0,
		LocalState:   state.NewNodeState(id, clusterIdx, nodeIdx),
		VoteReplies:  make(map[string]interface{}),
		Prepares:     make(map[int]map[string]bool),
		Commits:      make(map[int]map[string]bool),
		Log:          make([]string, 0),
		executedSeqs: make(map[int]bool),
	}

	// Register own public key in KnownKeys so self-verification works.
	n.KnownKeys[id] = pub

	return n, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Fault-threshold helpers  (paper Section II-A-1 and Section IV-A-3)
// ─────────────────────────────────────────────────────────────────────────────

// FaultyThresholdLocal returns f_local = ⌊(n-1)/3⌋ for a cluster of size n.
//
// This is the maximum number of Byzantine nodes tolerated within one cluster.
// PBFT requires n ≥ 3f+1, so f_local ≤ ⌊(n-1)/3⌋.
//
// Examples:  n=4 → 1,  n=7 → 2,  n=10 → 3,  n=13 → 4
func FaultyThresholdLocal(n int) int {
	if n <= 0 {
		return 0
	}
	return (n - 1) / 3
}

// FaultyThresholdGlobal returns f_global = ⌊(p-1)/3⌋ for a network of p nodes.
//
// This is the maximum number of Byzantine nodes tolerated across the entire
// network. Used when checking the client's reply quorum (paper Section IX Step 8).
func FaultyThresholdGlobal(p int) int {
	if p <= 0 {
		return 0
	}
	return (p - 1) / 3
}

// HasQuorumLocal returns true when count ≥ 2·f_local + 1.
//
// Used to decide when the leader has enough VoteReply or Prepare/Commit
// messages to advance the protocol within a cluster (paper Section VII).
func HasQuorumLocal(count int, fLocal int) bool {
	return count >= 2*fLocal+1
}

// HasQuorumGlobal returns true when count ≥ f_global + 1.
//
// Used by the client to decide when enough Reply messages have arrived to
// consider the global operation successfully executed (paper Section IX Step 8,
// plan pitfall #5: this is f+1, NOT 2f+1).
func HasQuorumGlobal(count int, fGlobal int) bool {
	return count >= fGlobal+1
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation execution
// ─────────────────────────────────────────────────────────────────────────────

// ApplyOperation deterministically applies operation op to the node's LocalState
// and appends it to the Log.
//
// Idempotency: if seqID has already been executed, the call is a no-op.
// This prevents double-execution when a Commit message is delivered twice
// (plan test 4 in Phase 6).
//
// The operation string is stored verbatim in the log. The protocol layer
// (Phase 8) is responsible for parsing it into concrete state mutations.
// Thread-safe: holds mu.Lock during the entire operation.
func (n *Node) ApplyOperation(seqID int, operation string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.executedSeqs[seqID] {
		return // idempotent — already applied
	}
	n.Log = append(n.Log, operation)
	n.LocalState.Data["last_op"] = operation
	n.LocalState.Data["log_len"] = len(n.Log)
	n.executedSeqs[seqID] = true
}

// ─────────────────────────────────────────────────────────────────────────────
// Tally helpers — used by Phase 8 handlers
// ─────────────────────────────────────────────────────────────────────────────

// AddPrepare records a Prepare from senderID for seqID.
// Returns the current distinct-sender count for that seqID.
// Duplicate senders are ignored (plan pitfall #3).
// Thread-safe: holds mu.Lock during the operation.
func (n *Node) AddPrepare(seqID int, senderID string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Prepares[seqID] == nil {
		n.Prepares[seqID] = make(map[string]bool)
	}
	n.Prepares[seqID][senderID] = true
	return len(n.Prepares[seqID])
}

// AddCommit records a Commit from senderID for seqID.
// Returns the current distinct-sender count for that seqID.
// Thread-safe: holds mu.Lock during the operation.
func (n *Node) AddCommit(seqID int, senderID string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Commits[seqID] == nil {
		n.Commits[seqID] = make(map[string]bool)
	}
	n.Commits[seqID][senderID] = true
	return len(n.Commits[seqID])
}

// AddVoteReply records a VoteReply from senderID.
// The value stored is the raw message (interface{}) for the leader to inspect.
// Returns the current distinct-sender count.
// Thread-safe: holds mu.Lock during the operation.
func (n *Node) AddVoteReply(senderID string, msg interface{}) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.VoteReplies[senderID] = msg
	return len(n.VoteReplies)
}

// ClearVoteReplies resets the VoteReply tally. Called after the Vote phase
// concludes so the next request starts clean.
// Thread-safe: holds mu.Lock during the operation.
func (n *Node) ClearVoteReplies() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.VoteReplies = make(map[string]interface{})
}

// NextSequenceID increments and returns the leader's sequence counter.
// Only the leader should call this.
// Thread-safe: holds mu.Lock for read-modify-write of SequenceID.
func (n *Node) NextSequenceID() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.SequenceID++
	return n.SequenceID
}

// IsLeader returns true when the node currently holds the LEADER role.
// Thread-safe: holds mu.RLock during role check.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == RoleLeader
}

// PromoteToLeader sets the node's role to LEADER.
// Used during leader election and view-change rotation.
// Thread-safe: holds mu.Lock during role update.
func (n *Node) PromoteToLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Role = RoleLeader
}

// DemoteToReplica sets the node's role to REPLICA.
// Used when this node steps down from leadership.
// Thread-safe: holds mu.Lock during role update.
func (n *Node) DemoteToReplica() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Role = RoleReplica
}

// ─────────────────────────────────────────────────────────────────────────────
// Getter methods for thread-safe read access
// ─────────────────────────────────────────────────────────────────────────────

// GetViewNumber returns the current view number.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetViewNumber() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ViewNumber
}

// GetSequenceID returns the current sequence ID.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetSequenceID() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.SequenceID
}

// GetLog returns a copy of the committed operation log.
// Thread-safe: holds mu.RLock and returns a copy to prevent external modification.
func (n *Node) GetLog() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	logCopy := make([]string, len(n.Log))
	copy(logCopy, n.Log)
	return logCopy
}

// GetRole returns the current node role.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetRole() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role
}

// GetLocation returns the current geographic location.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetLocation() cluster.Point {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Location
}

// SetLocation updates the node's geographic location (called during clustering).
// Thread-safe: holds mu.Lock during write.
func (n *Node) SetLocation(loc cluster.Point) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Location = loc
}

// ─────────────────────────────────────────────────────────────────────────────
// Reputation-Weighted Voting (RWV) — additive extension
// ─────────────────────────────────────────────────────────────────────────────

// InitReputation sets this node's starting reputation (R_init) and its initial
// knowledge of peer reputations. Must be called after NewNode, before RunPBFT.
// Thread-safe: holds mu.Lock during write.
func (n *Node) InitReputation(rep float64, clusterReps map[string]float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Reputation = rep
	n.BaseReputation = rep // immutable baseline for the reward formula
	n.KnownReputations = make(map[string]float64, len(clusterReps))
	for k, v := range clusterReps {
		n.KnownReputations[k] = v
	}
}

// reputationOf returns the reputation score for a given node ID.
// Returns 1.0 (not 0) for unknown nodes to avoid accidental exclusion.
// NOTE: called only from GetPrepareWeight/GetCommitWeight which already hold
// mu.RLock, so KnownReputations is accessed directly without re-locking.
func (n *Node) reputationOf(id string) float64 {
	if r, ok := n.KnownReputations[id]; ok {
		return r
	}
	return 1.0
}

// GetPrepareWeight returns the total reputation weight (Ω_prepare) of all
// senders that submitted a Prepare for seqID. Used in Phase I quorum check.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetPrepareWeight(seqID int) float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	senders, ok := n.Prepares[seqID]
	if !ok {
		return 0.0
	}
	var total float64
	for id := range senders {
		total += n.reputationOf(id)
	}
	return total
}

// GetCommitWeight returns the total reputation weight (Ω_commit) of all
// senders that submitted a Commit for seqID. Used in Phase I quorum check.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetCommitWeight(seqID int) float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	senders, ok := n.Commits[seqID]
	if !ok {
		return 0.0
	}
	var total float64
	for id := range senders {
		total += n.reputationOf(id)
	}
	return total
}

// HasReputationQuorum checks Algorithm 1, Phase I threshold:
//
//	τ = ⎪2Ω/3⎪ + 1
//
// Returns true when the accumulated vote weight (W_valid) meets or exceeds τ.
func HasReputationQuorum(accumulatedWeight, totalClusterWeight float64) bool {
	threshold := math.Floor(2.0*totalClusterWeight/3.0) + 1.0
	return accumulatedWeight >= threshold
}

// ─────────────────────────────────────────────────────────────────────────────
// Algorithm 1 — Phase III & IV: Dynamic update + synchronisation
// ─────────────────────────────────────────────────────────────────────────────

// UpdateReputation applies Algorithm 1 Phase III to this node's R_i.
//
// Honest node (participated in consensus):
//
//	ΔR_inc = α / (ln(1 + R_i − R_init) + β)
//	R_i    ← R_i + ΔR_inc
//
// Byzantine/absent node:
//
//	R_i ← R_i × γ   (multiplicative decay, 0 < γ < 1)
//
// Floor: R_i is clamped to 0 if it drops negative.
// Thread-safe: holds mu.Lock during write.
func (n *Node) UpdateReputation(honest bool, alpha, beta, gamma float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if honest {
		// Diminishing-returns reward: generous for newcomers, logarithmically
		// smaller for already-trusted nodes, preventing reputation inflation.
		excess := math.Max(n.Reputation-n.BaseReputation, 0.0)
		delta := alpha / (math.Log(1.0+excess) + beta)
		n.Reputation += delta
	} else {
		// Multiplicative penalty: faster decay for severely misbehaving nodes.
		n.Reputation *= gamma
	}
	if n.Reputation < 0 {
		n.Reputation = 0
	}
}

// GetReputation returns this node's current reputation score.
// Thread-safe: holds mu.RLock during read.
func (n *Node) GetReputation() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Reputation
}

// SyncReputations updates this node's KnownReputations map with fresh scores
// received from peers (Algorithm 1, Phase IV: reputation synchronisation).
// Does NOT overwrite this node's own Reputation (that is managed locally).
// Thread-safe: holds mu.Lock during write.
func (n *Node) SyncReputations(reps map[string]float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, r := range reps {
		if id == n.ID {
			continue // own reputation is authoritative — don't overwrite
		}
		n.KnownReputations[id] = r
	}
}
