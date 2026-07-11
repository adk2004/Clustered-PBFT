// Package state implements the State Machine Replication (SMR) model
// from the paper Section IV-B and the local/global transition definitions
// from Section VI.
//
// The paper defines three nested levels of state (eq. 2 and 3):
//
//	σ           – NetworkState:  the collective state of the entire network
//	σ_i         – ClusterState:  the state of the i-th cluster
//	σ_ij        – NodeState:     the state of the j-th node in cluster i
//
// Transitions are either:
//
//	(γ_L)^i – LocalTransition:  changes only cluster i         (paper eq. 4-5)
//	γ_G     – GlobalTransition: changes every cluster          (paper eq. 6-7)
//
// A key mathematical relationship from the paper (eq. 9):
//
//	γ_G(σ) = ∏_{i=0}^{m-1} (γ_L)^i(σ_i)
//
// i.e. a global transition is equivalent to composing local transitions
// across every cluster. GlobalTransition enforces this invariant.
//
// All transition functions are pure: they never mutate the input state.
// Clone() is provided to make deep-copy semantics explicit.
package state

import "encoding/json"

// ─────────────────────────────────────────────────────────────────────────────
// Core data types
// ─────────────────────────────────────────────────────────────────────────────

// NodeState is σ_ij — the state of the j-th node in the i-th cluster.
// The Data map carries vehicle-specific key-value payload (speed, location, etc.)
// and is intentionally open-ended so later phases can store whatever they need.
type NodeState struct {
	// NodeID is the unique identifier for this node (matches node.Node.ID).
	NodeID string `json:"node_id"`

	// ClusterID is the index of the cluster this node belongs to.
	ClusterID int `json:"cluster_id"`

	// NodeIdx is the position j of this node within its cluster (0-based).
	NodeIdx int `json:"node_idx"`

	// Data holds arbitrary vehicle state: location, speed, committed log index, etc.
	// Using map[string]interface{} keeps the type generic and JSON-serialisable.
	Data map[string]interface{} `json:"data"`
}

// ClusterState is σ_i — the collective state of the i-th cluster (paper eq. 3).
// NodeStates is ordered by node index j: NodeStates[j] = σ_ij.
type ClusterState struct {
	// ClusterID is the index i of this cluster within the network.
	ClusterID int `json:"cluster_id"`

	// NodeStates is the ordered slice of individual node states [σ_i0, σ_i1, …, σ_i(n-1)].
	NodeStates []NodeState `json:"node_states"`
}

// NetworkState is σ — the collective state of the entire network (paper eq. 2).
// ClusterStates is ordered by cluster index i: ClusterStates[i] = σ_i.
type NetworkState struct {
	// ClusterStates is the ordered slice of cluster states [σ_0, σ_1, …, σ_{m-1}].
	ClusterStates []ClusterState `json:"cluster_states"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Clone — deep copy
// ─────────────────────────────────────────────────────────────────────────────

// Clone returns a deep copy of sigma so that transition functions can produce
// a new state without mutating the original. We use JSON round-trip as a
// simple, correct mechanism that handles arbitrary Data maps.
func Clone(sigma NetworkState) NetworkState {
	// Marshal to JSON then unmarshal into a fresh value.
	// This handles all nested pointer and map types correctly.
	raw, err := json.Marshal(sigma)
	if err != nil {
		// NetworkState only contains JSON-safe types; this should never fire.
		panic("state.Clone: json.Marshal failed on NetworkState: " + err.Error())
	}
	var copy NetworkState
	if err := json.Unmarshal(raw, &copy); err != nil {
		panic("state.Clone: json.Unmarshal failed on NetworkState: " + err.Error())
	}
	return copy
}

// ─────────────────────────────────────────────────────────────────────────────
// Local state transition — (γ_L)^i
// ─────────────────────────────────────────────────────────────────────────────

// LocalTransition applies the local state transition (γ_L)^i to the network.
//
// Formally (paper eq. 4):
//
//	σ' = (γ_L)^i(σ) = {σ_0, …, σ'_i, …, σ_{m-1}}
//
// Only the cluster at clusterIdx is replaced with newClusterState.
// All other cluster states are carried over unchanged.
//
// Returns a new NetworkState; sigma is never mutated.
//
// Panics if clusterIdx is out of bounds — callers are expected to validate
// the index before calling (protocol layer enforces this invariant).
func LocalTransition(sigma NetworkState, clusterIdx int, newClusterState ClusterState) NetworkState {
	if clusterIdx < 0 || clusterIdx >= len(sigma.ClusterStates) {
		panic("state.LocalTransition: clusterIdx out of bounds")
	}
	next := Clone(sigma)
	next.ClusterStates[clusterIdx] = cloneClusterState(newClusterState)
	return next
}

// ─────────────────────────────────────────────────────────────────────────────
// Global state transition — γ_G
// ─────────────────────────────────────────────────────────────────────────────

// GlobalTransition applies the global state transition γ_G to the network.
//
// Formally (paper eq. 6):
//
//	σ' = γ_G(σ) = {σ'_0, σ'_1, …, σ'_{m-1}}
//
// Every cluster state is replaced by the corresponding entry in newClusterStates.
// This is mathematically equivalent to composing LocalTransition across every
// cluster index (paper eq. 9):
//
//	γ_G(σ) = ∏_{i=0}^{m-1} (γ_L)^i(σ_i)
//
// newClusterStates must have the same length as sigma.ClusterStates.
// Returns a new NetworkState; sigma is never mutated.
//
// Panics if len(newClusterStates) != len(sigma.ClusterStates).
func GlobalTransition(sigma NetworkState, newClusterStates []ClusterState) NetworkState {
	if len(newClusterStates) != len(sigma.ClusterStates) {
		panic("state.GlobalTransition: newClusterStates length must equal number of clusters")
	}
	// Build the new network state by applying a local transition for every cluster,
	// directly implementing eq. 9.
	next := Clone(sigma)
	for i, cs := range newClusterStates {
		next.ClusterStates[i] = cloneClusterState(cs)
	}
	return next
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// cloneClusterState deep-copies a single ClusterState using JSON round-trip.
func cloneClusterState(cs ClusterState) ClusterState {
	raw, err := json.Marshal(cs)
	if err != nil {
		panic("state.cloneClusterState: json.Marshal failed: " + err.Error())
	}
	var out ClusterState
	if err := json.Unmarshal(raw, &out); err != nil {
		panic("state.cloneClusterState: json.Unmarshal failed: " + err.Error())
	}
	return out
}

// cloneNodeState deep-copies a single NodeState.
func cloneNodeState(ns NodeState) NodeState {
	raw, err := json.Marshal(ns)
	if err != nil {
		panic("state.cloneNodeState: json.Marshal failed: " + err.Error())
	}
	var out NodeState
	if err := json.Unmarshal(raw, &out); err != nil {
		panic("state.cloneNodeState: json.Unmarshal failed: " + err.Error())
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Convenience constructors (used heavily in tests and the protocol layer)
// ─────────────────────────────────────────────────────────────────────────────

// NewNodeState creates a NodeState with an initialised (non-nil) Data map.
func NewNodeState(nodeID string, clusterID, nodeIdx int) NodeState {
	return NodeState{
		NodeID:    nodeID,
		ClusterID: clusterID,
		NodeIdx:   nodeIdx,
		Data:      make(map[string]interface{}),
	}
}

// NewClusterState creates a ClusterState for clusterID containing the given nodes.
func NewClusterState(clusterID int, nodes []NodeState) ClusterState {
	ns := make([]NodeState, len(nodes))
	for i, n := range nodes {
		ns[i] = cloneNodeState(n)
	}
	return ClusterState{ClusterID: clusterID, NodeStates: ns}
}

// NewNetworkState creates a NetworkState from a slice of ClusterStates.
func NewNetworkState(clusters []ClusterState) NetworkState {
	cs := make([]ClusterState, len(clusters))
	for i, c := range clusters {
		cs[i] = cloneClusterState(c)
	}
	return NetworkState{ClusterStates: cs}
}