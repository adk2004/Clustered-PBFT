package state

import (
	"encoding/json"
	"reflect"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

// makeNetwork builds a deterministic NetworkState with m clusters of n nodes each.
// Node IDs have the form "node-i-j" and carry a seed value in Data["seed"].
func makeNetwork(m, n int) NetworkState {
	clusters := make([]ClusterState, m)
	for i := 0; i < m; i++ {
		nodes := make([]NodeState, n)
		for j := 0; j < n; j++ {
			ns := NewNodeState(nodeID(i, j), i, j)
			ns.Data["seed"] = i*100 + j
			nodes[j] = ns
		}
		clusters[i] = NewClusterState(i, nodes)
	}
	return NewNetworkState(clusters)
}

func nodeID(i, j int) string {
	return "node-" + itoa(i) + "-" + itoa(j)
}

// itoa avoids importing strconv (keeps the test file dependency-free).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// clusterJSON serialises a ClusterState to JSON for byte-level comparison.
func clusterJSON(t *testing.T, cs ClusterState) string {
	t.Helper()
	b, err := json.Marshal(cs)
	if err != nil {
		t.Fatalf("clusterJSON marshal: %v", err)
	}
	return string(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — LocalTransition changes only cluster i; all others unchanged
// ─────────────────────────────────────────────────────────────────────────────

func TestLocalTransitionOnlyChangesTargetCluster(t *testing.T) {
	const m, n = 3, 4
	original := makeNetwork(m, n)

	// For each cluster index, apply a local transition and verify the invariant.
	for targetIdx := 0; targetIdx < m; targetIdx++ {
		targetIdx := targetIdx
		t.Run("cluster_"+itoa(targetIdx), func(t *testing.T) {
			t.Parallel()

			// Build a replacement cluster with distinctly different Data.
			newNodes := make([]NodeState, n)
			for j := 0; j < n; j++ {
				ns := NewNodeState("new-"+nodeID(targetIdx, j), targetIdx, j)
				ns.Data["replaced"] = true
				ns.Data["seed"] = 9999
				newNodes[j] = ns
			}
			newCluster := NewClusterState(targetIdx, newNodes)

			result := LocalTransition(original, targetIdx, newCluster)

			// The target cluster must have changed.
			if reflect.DeepEqual(result.ClusterStates[targetIdx], original.ClusterStates[targetIdx]) {
				t.Errorf("cluster %d was NOT changed by LocalTransition", targetIdx)
			}

			// Every OTHER cluster must be byte-identical to the original.
			for i := 0; i < m; i++ {
				if i == targetIdx {
					continue
				}
				origJSON := clusterJSON(t, original.ClusterStates[i])
				resJSON := clusterJSON(t, result.ClusterStates[i])
				if origJSON != resJSON {
					t.Errorf("cluster %d was unexpectedly modified by LocalTransition on cluster %d:\n  original: %s\n  result:   %s",
						i, targetIdx, origJSON, resJSON)
				}
			}
		})
	}
}

// LocalTransition must not mutate the original sigma.
func TestLocalTransitionDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	original := makeNetwork(3, 4)
	originalJSON, _ := json.Marshal(original)

	newCluster := NewClusterState(1, []NodeState{NewNodeState("x", 1, 0)})
	_ = LocalTransition(original, 1, newCluster)

	afterJSON, _ := json.Marshal(original)
	if string(originalJSON) != string(afterJSON) {
		t.Error("LocalTransition mutated the original NetworkState")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — GlobalTransition changes every cluster
// ─────────────────────────────────────────────────────────────────────────────

func TestGlobalTransitionChangesEveryCluster(t *testing.T) {
	t.Parallel()

	const m, n = 3, 4
	original := makeNetwork(m, n)

	// Build a completely new set of cluster states.
	newClusters := make([]ClusterState, m)
	for i := 0; i < m; i++ {
		nodes := make([]NodeState, n)
		for j := 0; j < n; j++ {
			ns := NewNodeState("global-"+nodeID(i, j), i, j)
			ns.Data["global_update"] = true
			ns.Data["seed"] = -1
			nodes[j] = ns
		}
		newClusters[i] = NewClusterState(i, nodes)
	}

	result := GlobalTransition(original, newClusters)

	// Every cluster must have changed.
	for i := 0; i < m; i++ {
		origJSON := clusterJSON(t, original.ClusterStates[i])
		resJSON := clusterJSON(t, result.ClusterStates[i])
		if origJSON == resJSON {
			t.Errorf("cluster %d was NOT changed by GlobalTransition", i)
		}
	}
}

// GlobalTransition must not mutate the original sigma.
func TestGlobalTransitionDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	const m, n = 3, 4
	original := makeNetwork(m, n)
	originalJSON, _ := json.Marshal(original)

	newClusters := make([]ClusterState, m)
	for i := 0; i < m; i++ {
		newClusters[i] = NewClusterState(i, []NodeState{NewNodeState("x", i, 0)})
	}
	_ = GlobalTransition(original, newClusters)

	afterJSON, _ := json.Marshal(original)
	if string(originalJSON) != string(afterJSON) {
		t.Error("GlobalTransition mutated the original NetworkState")
	}
}

// GlobalTransition with wrong-length newClusters must panic.
func TestGlobalTransitionPanicsOnLengthMismatch(t *testing.T) {
	t.Parallel()

	original := makeNetwork(3, 4)
	defer func() {
		if r := recover(); r == nil {
			t.Error("GlobalTransition should panic when newClusterStates has wrong length, but did not")
		}
	}()
	_ = GlobalTransition(original, []ClusterState{}) // length 0, original has 3
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Clone produces a true deep copy
// ─────────────────────────────────────────────────────────────────────────────

func TestCloneIsDeepCopy(t *testing.T) {
	t.Parallel()

	original := makeNetwork(3, 4)

	cloned := Clone(original)

	// Structural equality before mutation.
	origJSON, _ := json.Marshal(original)
	cloneJSON, _ := json.Marshal(cloned)
	if string(origJSON) != string(cloneJSON) {
		t.Fatal("Clone produced a structurally different state")
	}

	// Mutate the clone's Data map — must not affect the original.
	cloned.ClusterStates[0].NodeStates[0].Data["injected"] = "evil"

	origAfterJSON, _ := json.Marshal(original)
	if string(origJSON) != string(origAfterJSON) {
		t.Error("Mutating the clone's Data map affected the original — Clone is not a deep copy")
	}

	// Mutate the clone's NodeID — must not affect the original.
	cloned.ClusterStates[1].NodeStates[2].NodeID = "mutated-id"

	origAfterJSON2, _ := json.Marshal(original)
	if string(origJSON) != string(origAfterJSON2) {
		t.Error("Mutating the clone's NodeID affected the original — Clone is not a deep copy")
	}
}

// Clone of a Clone must also be independent.
func TestCloneOfCloneIsIndependent(t *testing.T) {
	t.Parallel()

	original := makeNetwork(2, 4)
	clone1 := Clone(original)
	clone2 := Clone(clone1)

	// Mutate clone1.
	clone1.ClusterStates[0].NodeStates[0].Data["x"] = 42

	origJSON, _ := json.Marshal(original)
	clone2JSON, _ := json.Marshal(clone2)

	origAfterJSON, _ := json.Marshal(original)
	if string(origJSON) != string(origAfterJSON) {
		t.Error("Mutating clone1 affected original")
	}
	if string(origJSON) != string(clone2JSON) {
		t.Error("clone2 does not match the original (mutation leaked through chain)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — GlobalTransition ≡ composing LocalTransitions (paper eq. 9)
// ─────────────────────────────────────────────────────────────────────────────

func TestGlobalEqualsComposedLocalTransitions(t *testing.T) {
	t.Parallel()

	const m, n = 3, 4
	original := makeNetwork(m, n)

	// Create one updated cluster state per cluster.
	newClusters := make([]ClusterState, m)
	for i := 0; i < m; i++ {
		nodes := make([]NodeState, n)
		for j := 0; j < n; j++ {
			ns := NewNodeState("eq9-"+nodeID(i, j), i, j)
			ns.Data["eq9"] = i*10 + j
			nodes[j] = ns
		}
		newClusters[i] = NewClusterState(i, nodes)
	}

	// Path A: single GlobalTransition (paper eq. 6).
	globalResult := GlobalTransition(original, newClusters)

	// Path B: compose LocalTransitions for every cluster (paper eq. 9).
	//   γ_G(σ) = ∏_{i=0}^{m-1} (γ_L)^i(σ)
	// Note: each local transition receives the ORIGINAL sigma, and we merge
	// the results by constructing the final state cluster by cluster —
	// matching the mathematical definition where each (γ_L)^i acts on its
	// own cluster of the collective state independently.
	composed := Clone(original)
	for i := 0; i < m; i++ {
		composed = LocalTransition(composed, i, newClusters[i])
	}

	// The two results must be structurally identical (paper eq. 9 invariant).
	globalJSON, _ := json.Marshal(globalResult)
	composedJSON, _ := json.Marshal(composed)

	if string(globalJSON) != string(composedJSON) {
		t.Errorf("GlobalTransition result does not equal composed LocalTransitions:\n  global:   %s\n  composed: %s",
			globalJSON, composedJSON)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: NewNodeState / NewClusterState / NewNetworkState constructors
// ─────────────────────────────────────────────────────────────────────────────

func TestNewNodeStateHasNonNilData(t *testing.T) {
	t.Parallel()

	ns := NewNodeState("node-0-0", 0, 0)
	if ns.Data == nil {
		t.Error("NewNodeState returned a NodeState with nil Data map")
	}
	// Should be writable without panic.
	ns.Data["key"] = "value"
}

func TestNewNetworkStatePreservesOrder(t *testing.T) {
	t.Parallel()

	const m, n = 4, 4
	net := makeNetwork(m, n)

	for i := 0; i < m; i++ {
		if net.ClusterStates[i].ClusterID != i {
			t.Errorf("ClusterState[%d].ClusterID = %d, want %d",
				i, net.ClusterStates[i].ClusterID, i)
		}
		for j := 0; j < n; j++ {
			got := net.ClusterStates[i].NodeStates[j].NodeID
			want := nodeID(i, j)
			if got != want {
				t.Errorf("NodeStates[%d][%d].NodeID = %q, want %q", i, j, got, want)
			}
		}
	}
}