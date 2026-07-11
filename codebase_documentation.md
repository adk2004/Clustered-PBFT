# Vehicular BFT Consensus — Complete Project Documentation

**Paper:** An Efficient and Scalable Byzantine Fault Tolerant Consensus for Vehicular Networks  
**Authors:** Deshmukh, Baiju, Atish, Alladi, Yu — IEEE TVT 2025  
**Implementation Language:** Go 1.22  
**Total:** 9 packages · 129 unit tests · 4,090 lines of implementation · 4,119 lines of tests

---

## Table of Contents

1. [What This Project Is](#1-what-this-project-is)
2. [Project Structure Overview](#2-project-structure-overview)
3. [config/ — Constants](#3-config--constants)
4. [crypto/ — Cryptography](#4-crypto--cryptography)
5. [state/ — State Machine](#5-state--state-machine)
6. [cluster/ — Clustering and Leader Election](#6-cluster--clustering-and-leader-election)
7. [messages/ — Protocol Messages](#7-messages--protocol-messages)
8. [node/ — Node Abstraction](#8-node--node-abstraction)
9. [network/ — TCP Layer](#9-network--tcp-layer)
10. [protocol/ — Consensus Engine](#10-protocol--consensus-engine)
11. [dynamic/ — Dynamic Clustering](#11-dynamic--dynamic-clustering)
12. [metrics/ — Metrics Collection](#12-metrics--metrics-collection)
13. [main.go — Simulation Runner](#13-maingo--simulation-runner)
14. [plot.py — Graph Generation](#14-plotpy--graph-generation)
15. [How to Run Everything](#15-how-to-run-everything)
16. [Presentation Guide](#16-presentation-guide)
17. [Limitations and Honest Assessment](#17-limitations-and-honest-assessment)
18. [Expected Professor Questions](#18-expected-professor-questions)

---

## 1. What This Project Is

This is a complete Go implementation of every algorithm, data structure, protocol message, and evaluation described in the paper. It is not a toy simulation — it implements the actual cryptographic primitives (RSA-2048 signing on every message), the exact K-Means++ clustering algorithm from Algorithm 1, the full PBFT consensus logic, and the dynamic re-clustering from Algorithm 2.

### The Core Idea of the Paper

Standard PBFT (Practical Byzantine Fault Tolerance) requires every node to talk to every other node. For `p` nodes, that is O(p²) messages per consensus round. For 20 vehicles, that is 400 messages every time anyone wants to agree on anything.

The paper's insight is simple: vehicles that are geographically close to each other share most of their decisions. So instead of a flat broadcast to all 20 vehicles:

1. **Cluster** nearby vehicles into groups of 4 (using K-Means)
2. Run PBFT **only within** each 4-vehicle cluster for operations that affect just that area (local transitions)
3. Only bring in the other clusters when a decision genuinely needs to be network-wide (global transitions)

With clustering approximation n ≈ m ≈ √p, inter-cluster communication drops from O(p²) to O(p^1.5). For 20 nodes: 89 messages instead of 400.

### What We Implemented vs What the Paper Has

| Component | Paper | Our Implementation |
|---|---|---|
| K-Means++ clustering (Algorithm 1) | ✅ | ✅ |
| Leader election (closest to centroid) | ✅ | ✅ |
| RSA-2048 PKI / NodeCA | ✅ (specified) | ✅ (fully implemented) |
| All 8 protocol message types | ✅ | ✅ |
| Local state transition (§XI) | ✅ | ✅ |
| Global state transition (§IX) | ✅ | ✅ |
| Proposed-global, voted-local (§X) | ✅ | ✅ |
| Dynamic clustering / tick mode (Algorithm 2) | ✅ | ✅ |
| Evaluation harness (§XIV) | ✅ | ✅ |
| View-change protocol | ❌ (mentioned only) | ❌ (same as paper) |
| Byzantine equivocation simulation | ❌ (not tested) | ❌ (same as paper) |
| Multi-machine deployment | ✅ (local TCP) | TCP layer built, not wired into protocol |

---

## 2. Project Structure Overview

```
vehicular-bft/
├── go.mod                    ← Go module definition
├── main.go                   ← Simulation runner and evaluation harness
├── plot.py                   ← Python script to generate Figures 4-8
├── results_static.csv        ← Generated: throughput/latency data
├── results_dynamic.csv       ← Generated: dynamic testbed data
│
├── config/
│   └── config.go             ← All project-wide constants
│
├── crypto/
│   ├── keys.go               ← RSA-2048 keygen, Sign, Verify, Digest
│   └── keys_test.go          ← 11 tests
│
├── state/
│   ├── state.go              ← NetworkState, ClusterState, transitions
│   └── state_test.go         ← 9 tests
│
├── cluster/
│   ├── kmeans.go             ← K-Means++, SameSizeKMeans, ComputeDimensions
│   ├── kmeans_test.go        ← 12 tests
│   ├── leader.go             ← ElectLeader, ElectAllLeaders
│   └── leader_test.go        ← 12 tests
│
├── messages/
│   ├── types.go              ← All 8 message types + Envelope helpers
│   └── types_test.go         ← 18 tests
│
├── node/
│   ├── node.go               ← Node struct, fault threshold helpers
│   ├── replica.go            ← HandleVote, HandlePrePrepare, HandlePrepare, HandleCommit
│   ├── leader.go             ← HandleIntraClusterRequest, HandleVoteReplies, StartPBFT
│   └── node_test.go          ← 16 tests
│
├── network/
│   ├── server.go             ← TCP server, accept loop, message delivery
│   ├── client.go             ← Send, Broadcast, Multicast, SendWithRetry
│   └── network_test.go       ← 13 tests
│
├── protocol/
│   ├── pbft.go               ← PBFTInstance, RunPBFT (core engine)
│   ├── vote.go               ← RunVotePhase, BuildVoteReplies
│   ├── intra_cluster.go      ← RunLocalTransition, RunProposedGlobalButLocal
│   ├── inter_cluster.go      ← GlobalCoordinator, RunGlobalTransition
│   └── protocol_test.go      ← 10 tests
│
├── dynamic/
│   ├── clustering.go         ← TickMode, ProcessTick, ForwardPendingRequests
│   └── clustering_test.go    ← 13 tests
│
└── metrics/
    ├── metrics.go            ← Collector, Throughput, MeanLatency, P99, SaveCSV
    └── metrics_test.go       ← 11 tests
```

---

## 3. config/ — Constants

### `config/config.go`

This file contains every project-wide constant. Nothing is hardcoded anywhere else — all other packages import these values.

```go
const (
    MINSIZE             = 4       // Minimum cluster size
    MaxFaultyRatio      = 1.0/3.0 // f <= (n-1)/3
    TickDefault         = 10      // seconds per tick
    MaxNodesPerTick     = 4       // T from paper Section XIV
    RSABits             = 2048    // RSA key size
    TCPBasePort         = 9000    // node i listens on 9000+i
    KMeansMaxIter       = 100     // K-Means iteration cap
    KMeansConvergenceDelta = 0.001 // centroid movement threshold
)
```

**MINSIZE = 4** comes directly from Section V-A of the paper. PBFT requires at least 3f+1 nodes. With minimum f=1, that means minimum 4 nodes per cluster. If the formula n=⌊√p⌋ gives a number less than 4, we clamp to 4.

**MaxFaultyRatio = 1/3** corresponds to the paper's fault model (Section IV-A-3): f ≤ ⌊(n-1)/3⌋. The system can tolerate up to one-third of nodes being Byzantine.

**RSABits = 2048** matches the paper's Section IV-A-2: "All keypairs issued follow the RSA cryptosystem." We use 2048-bit keys which is the standard security parameter.

**KMeansMaxIter = 100** caps the K-Means loop to prevent infinite iteration on pathological inputs. The paper's Section XII mentions using t=10 iterations; we allow up to 100 with an early convergence check.

---

## 4. crypto/ — Cryptography

### `crypto/keys.go`

This is the cryptographic foundation of the entire project. Every message sent between nodes is signed by the sender and verified by the receiver. This implements the PKI (Public Key Infrastructure) described in Section IV-A-2 of the paper.

**Why cryptography matters here:** In a Byzantine fault tolerant system, a faulty node could send fake messages claiming to be from another node. Without signatures, a single faulty node could impersonate the leader and inject false pre-prepare messages. The RSA signatures prevent this.

#### Function: `GenerateKeyPair()`

```go
func GenerateKeyPair() (*rsa.PrivateKey, *rsa.PublicKey, error)
```

Generates an RSA-2048 key pair using Go's `crypto/rand` as the randomness source. Called once per node during initialisation. The private key stays on the node; the public key is distributed to all other nodes through the NodeCA (simulated by `linkKeys()` in our tests).

**What it does internally:**
- Calls `rsa.GenerateKey(rand.Reader, 2048)`
- Returns both the private key and a pointer to its embedded public key
- The 2048-bit modulus means any brute-force attack would take more than the age of the universe

#### Function: `Sign(privKey, data)`

```go
func Sign(privKey *rsa.PrivateKey, data []byte) (string, error)
```

Signs arbitrary bytes with a private key. Uses RSA-PKCS1v15 with SHA-256.

**What it does:**
1. Computes SHA-256 hash of `data`
2. Signs the hash (not the raw data) using the private key
3. Returns the signature as a base64 string (so it can be safely embedded in JSON)

**Why hash first:** RSA can only sign data up to the key size minus padding. A SHA-256 hash is always exactly 32 bytes, regardless of input size. This allows signing arbitrarily large messages.

**Why base64:** JSON does not allow raw binary data. Base64 encodes binary as ASCII text, making signatures safe to embed in our JSON protocol messages.

#### Function: `Verify(pubKey, data, sig)`

```go
func Verify(pubKey *rsa.PublicKey, data []byte, sig string) bool
```

Verifies that `sig` was produced by the holder of `pubKey` over `data`. Returns false for any failure (wrong key, tampered data, bad base64) — never panics.

**What it does:**
1. Base64-decodes the signature string back to bytes
2. Computes SHA-256 hash of `data`
3. Calls `rsa.VerifyPKCS1v15` which mathematically checks the signature against the public key

**Security guarantee:** Only the holder of the private key matching `pubKey` could have produced a valid signature. Any single byte change to `data` after signing will cause `Verify` to return false.

#### Function: `Digest(v)`

```go
func Digest(v interface{}) (string, error)
```

Computes SHA-256 hash of any JSON-serialisable value. Returns a 64-character lowercase hex string. Used as the `d` field in protocol messages (Vote, PrePrepare, Prepare, Commit).

**Why this exists:** Protocol messages need a compact fingerprint of their payload so that recipients can verify they received the same message the sender intended. The digest is computed over the raw JSON bytes of the inner message, not over the outer envelope (this avoids a circular dependency where you'd need to compute a digest of something that contains the digest).

**Example:** When the leader sends a PrePrepare with digest "deadbeef...", each replica independently computes the digest of the enclosed operation and checks it matches. If even one byte of the operation was corrupted in transit, the digests won't match and the message is rejected.

#### Function: `SerializePublicKey(pub)` and `DeserializePublicKey(pemStr)`

These convert RSA public keys to and from PEM-encoded strings. PEM (Privacy Enhanced Mail) is the standard text format for cryptographic keys. Used when distributing public keys between nodes during initialisation.

**The format:**
```
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
-----END PUBLIC KEY-----
```

### `crypto/keys_test.go` — 11 Tests

The tests verify:
1. `GenerateKeyPair` produces non-nil 2048-bit keys
2. Two calls produce different keys (no collision)
3. `Sign → Verify` round-trip succeeds
4. `Verify` returns false when data is tampered (4 variants: extra byte, flipped bit, different message, empty)
5. `Verify` returns false with the wrong public key
6. `Verify(nil, ...)` returns false without panicking
7. `Digest` is deterministic (same input → same output on repeated calls)
8. `Digest` produces different outputs for different inputs
9. `SerializePublicKey → DeserializePublicKey` is a perfect round-trip
10. Deserializing garbage PEM returns an error (4 variants)
11. Sign → Verify still works after a PEM serialisation round-trip

---

## 5. state/ — State Machine

### `state/state.go`

This implements the mathematical state model from Section VI of the paper. The paper defines three nested levels of state:

- **σ** (sigma) — NetworkState: the collective state of the entire network
- **σ_i** — ClusterState: the state of the i-th cluster
- **σ_ij** — NodeState: the state of the j-th node in cluster i

And two types of transitions:
- **(γ_L)^i** — LocalTransition: changes only cluster i, everything else stays the same (Equation 4)
- **γ_G** — GlobalTransition: changes every cluster (Equation 6)
- The key mathematical relationship: γ_G = ∏(γ_L)^i (Equation 9) — global = composing all locals

#### Type: `NodeState`

```go
type NodeState struct {
    NodeID    string
    ClusterID int
    NodeIdx   int
    Data      map[string]interface{}
}
```

Represents the state of one vehicle (σ_ij). The `Data` map is intentionally open-ended — it can hold anything: location, speed, last committed operation, log index. This matches the paper's description that states "encapsulate a range of parameters vital to the status and context of each vehicle."

#### Type: `ClusterState`

```go
type ClusterState struct {
    ClusterID  int
    NodeStates []NodeState
}
```

Represents σ_i — the state of one cluster. `NodeStates[j]` is the state of the j-th node in this cluster (σ_ij).

#### Type: `NetworkState`

```go
type NetworkState struct {
    ClusterStates []ClusterState
}
```

Represents σ — the collective state of the entire network. `ClusterStates[i]` is σ_i.

#### Function: `LocalTransition(sigma, clusterIdx, newClusterState)`

Implements Equation 4 from the paper:

```
σ' = (γ_L)^i(σ) = {σ_0, σ_1, ..., σ'_i, ..., σ_{m-1}}
```

Only the cluster at `clusterIdx` changes; all other clusters carry over unchanged. The function is **pure** — it never mutates `sigma`, it always returns a new `NetworkState`. This is achieved via `Clone()`.

**Why pure functions matter:** In a distributed system, multiple goroutines may be reading the network state while a transition is being computed. Pure functions eliminate the possibility of race conditions caused by mutation.

#### Function: `GlobalTransition(sigma, newClusterStates)`

Implements Equation 6:

```
σ' = γ_G(σ) = {σ'_0, σ'_1, ..., σ'_{m-1}}
```

Every cluster state is replaced. Internally, this is implemented by applying LocalTransition for each cluster in sequence — directly implementing Equation 9:

```
γ_G(σ) = ∏_{i=0}^{m-1} (γ_L)^i(σ_i)
```

The test `TestGlobalEqualsComposedLocalTransitions` explicitly verifies this mathematical relationship holds.

#### Function: `Clone(sigma)`

Deep-copies a NetworkState using JSON marshal/unmarshal. This handles all nested pointer and map types correctly. The test `TestCloneIsDeepCopy` verifies that mutating the clone does not affect the original.

### `state/state_test.go` — 9 Tests

- `TestLocalTransitionOnlyChangesTargetCluster` — runs for each cluster index 0,1,2; verifies only the target changes
- `TestLocalTransitionDoesNotMutateInput` — verifies pure function property
- `TestGlobalTransitionChangesEveryCluster` — all clusters updated
- `TestGlobalTransitionDoesNotMutateInput` — pure function
- `TestGlobalTransitionPanicsOnLengthMismatch` — wrong-length input panics
- `TestCloneIsDeepCopy` — mutating clone doesn't affect original
- `TestCloneOfCloneIsIndependent` — chain of clones all independent
- `TestGlobalEqualsComposedLocalTransitions` — **directly tests paper Equation 9**
- `TestNewNetworkStatePreservesOrder` — constructor preserves ordering

---

## 6. cluster/ — Clustering and Leader Election

### `cluster/kmeans.go`

This is one of the two most paper-faithful files in the project (the other is `dynamic/clustering.go`). It implements Algorithm 1 from the paper verbatim.

#### Type: `Point`

```go
type Point struct {
    ID string
    X  float64
    Y  float64
}
```

Represents a vehicle's 2D geographic position. Every node in the network has one. The K-Means algorithm works entirely with these points — it doesn't care about any other node properties.

#### Type: `Cluster`

```go
type Cluster struct {
    ID       int
    Centroid Point
    Nodes    []Point
}
```

One geographic cluster. `Centroid` is the arithmetic mean position of all nodes in the cluster — updated after each K-Means iteration. `Nodes` are the actual vehicle positions assigned to this cluster.

#### Function: `ComputeDimensions(p)`

```go
func ComputeDimensions(p int) (n int, m int)
```

Implements paper Equation 1:
```
n = ⌊√p⌋,   m = ⌊p/n⌋
```
With the MINSIZE=4 constraint from Section V-A.

**Examples:**
- p=8:  √8≈2.83 → 2 < MINSIZE=4 → n=4, m=8/4=2
- p=12: √12≈3.46 → 3 < MINSIZE=4 → n=4, m=12/4=3
- p=16: √16=4 ≥ MINSIZE → n=4, m=16/4=4
- p=20: √20≈4.47 → 4 ≥ MINSIZE → n=4, m=20/4=5
- p=25: √25=5 ≥ MINSIZE → n=5, m=25/5=5

#### Function: `KMeansPlusPlus(nodes, m)`

Implements the K-Means++ seeding strategy (Arthur & Vassilvitskii 2007, paper reference [30]).

**Why K-Means++ instead of random seeding:** Random seeding can place initial centroids in bad positions, causing K-Means to converge to a poor local optimum. K-Means++ weights the probability of each candidate centroid proportional to its squared distance from the nearest already-chosen centroid. This spreads initial centroids out, consistently producing better clusterings.

**The algorithm:**
1. Choose first centroid uniformly at random from all nodes
2. For each subsequent centroid:
   - Compute D(x)² for each node x = squared distance to nearest already-chosen centroid
   - Choose next centroid with probability proportional to D(x)²
3. Repeat until m centroids chosen

**Internal variant `kMeansPlusPlusWithRand`:** Takes a `*rand.Rand` parameter, enabling deterministic behavior in tests (fixed seed = reproducible results).

#### Function: `SameSizeKMeans(nodes, m, n)` and `SameSizeKMeansSeeded(nodes, m, n, seed)`

The core of Algorithm 1. Partitions `nodes` into `m` clusters of exactly `n` nodes each.

**Why same-size matters:** Standard K-Means produces unequal cluster sizes, which would give different fault tolerance thresholds to different clusters (since f_local = ⌊(n-1)/3⌋). With unequal clusters, some would have f=1, others f=2, making the system's fault model inconsistent. Equal clusters ensure every cluster has the same f_local.

**The capacity-capped assignment (Algorithm 1 lines 7-18):**

For each node (in order):
1. Find the nearest centroid that still has room (size < n)
2. If all centroids are full, assign to the least-loaded one (fallback for remainder nodes)
3. This guarantees exactly n nodes per cluster when len(nodes) = m×n

**Convergence check:** After each iteration, compute how far each centroid moved. If all centroids moved less than `KMeansConvergenceDelta = 0.001` units, the algorithm has converged and exits early. Otherwise, repeat up to `KMeansMaxIter = 100` times.

**`SameSizeKMeansSeeded` vs `SameSizeKMeans`:** The seeded variant takes a fixed random seed and produces identical results every call. Used in all tests to make them deterministic. Production code uses `SameSizeKMeans` which seeds from the clock.

### `cluster/kmeans_test.go` — 12 Tests

- Tests `ComputeDimensions` for p=4,8,9,12,16,20,25,100 (all with expected values)
- Tests that 12-node clustering produces exactly 3 clusters of 4
- Tests that 8-node clustering produces exactly 2 clusters of 4
- Tests 16-node and 20-node configs
- Tests that every node appears in exactly one cluster (no duplicates, no missing)
- Tests that same seed produces identical results (determinism)
- Tests `EuclideanDistance` with known triangles
- Tests `KMeansPlusPlus` returns exactly m centroids from the input set

### `cluster/leader.go`

#### Function: `ElectLeader(c)`

```go
func ElectLeader(c Cluster) int
```

Returns the index (within `c.Nodes`) of the node closest to `c.Centroid`. This is the paper's Section V-C: "The leader node of each cluster is the node with the minimum Euclidean distance to the cluster's centroid."

**Tie-breaking:** When two nodes are equidistant from the centroid, the node with the lower index in `c.Nodes` wins. This is deterministic because `SameSizeKMeans` assigns nodes in a fixed order.

Returns -1 for an empty cluster (safety guard, never panics).

#### Function: `ElectAllLeaders(clusters)`

```go
func ElectAllLeaders(clusters []Cluster) map[int]Point
```

Runs `ElectLeader` for every cluster. Returns a map from cluster ID to the elected leader's `Point`. This gives callers immediate access to the leader's position and ID without needing to look up the cluster again.

#### Function: `LeaderDistance(c)`

Returns the Euclidean distance from the elected leader to the centroid. Used for logging and assertions.

### `cluster/leader_test.go` — 12 Tests

- Tests the minimum-distance invariant with hand-computed fixtures
- Tests that a node exactly at the centroid (distance=0) is elected
- Tests `ElectAllLeaders` produces one leader per cluster, each in its cluster's `Nodes`
- Tests tie-breaking: 4 equidistant nodes → lowest index always wins
- Tests partial tie: only two nodes equidistant, correct one wins
- Tests empty cluster returns -1 (no panic)
- Tests single-node cluster always elects that node
- Tests integration with `SameSizeKMeans`: elected leader is genuinely the nearest node
- Tests stability: calling `ElectAllLeaders` twice gives identical results

---

## 7. messages/ — Protocol Messages

### `messages/types.go`

This file defines every wire message used in the protocol. It is the most direct translation of Section VII of the paper into Go types.

#### Type: `TransitionType`

```go
type TransitionType string
const (
    LOCAL  TransitionType = "LOCAL"
    GLOBAL TransitionType = "GLOBAL"
)
```

The `s` field that appears in several messages. When a client sends an IntraClusterRequest with `s=GLOBAL`, it is proposing a network-wide state change. Replicas vote on whether this should indeed be global or stay local.

#### Type: `MsgType`

Discriminates between the 8 message types. Receivers switch on this value:
- `MsgIntraClusterRequest` — client to cluster leader
- `MsgVote` — leader to cluster replicas
- `MsgVoteReply` — replicas to leader
- `MsgInterClusterRequest` — leader to all other cluster leaders
- `MsgPrePrepare` — leader to replicas (start of PBFT)
- `MsgPrepare` — replica broadcast (PBFT phase 2)
- `MsgCommit` — replica broadcast (PBFT phase 3)
- `MsgReply` — any node to client

#### Type: `Envelope`

```go
type Envelope struct {
    Type      MsgType `json:"type"`
    SenderID  string  `json:"sender_id"`
    Signature string  `json:"signature"`
    Body      []byte  `json:"body"`
}
```

Every message is wrapped in an Envelope before sending over TCP. The `Signature` is `Sign(Body)` — it is computed over the raw JSON bytes of the inner message struct, not over the Envelope itself. This avoids a circular dependency (you can't include a signature of something that contains the signature).

**Wire format:** JSON + newline character. The newline lets the receiver's `bufio.Scanner` know where one message ends and the next begins.

#### The 8 Inner Message Structs

**IntraClusterRequest** (client → leader):
```go
type IntraClusterRequest struct {
    Operation  string         // o: the state transition requested
    Timestamp  int64          // t: Unix nanoseconds
    ClientID   string         // c: client identifier
    Transition TransitionType // s: LOCAL or GLOBAL
}
```
Paper notation: `<INTRA-CLUSTER-REQUEST, o, t, c, s>_σc`

**Vote** (leader → cluster replicas):
```go
type Vote struct {
    ViewNumber int            // v: current PBFT view
    SequenceID int            // g: monotonic request counter
    Digest     string         // d: SHA-256 of Message
    Message    []byte         // h: JSON of IntraClusterRequest
    Transition TransitionType // s: the transition being voted on
}
```
Paper notation: `<<VOTE, v, g, d, s>, h>_σ_ip`

**VoteReply** (replicas → leader):
```go
type VoteReply struct {
    ViewNumber int
    SequenceID int
    Digest     string
    ReplicaIdx int            // j: this replica's index within its cluster
    ClusterIdx int            // i: cluster index
    Transition TransitionType // s: this replica's vote (LOCAL or GLOBAL)
    Message    []byte
}
```
Paper notation: `<<VOTE-REPLY, v, g, d, j, s>, h>_σ_ij`

**InterClusterRequest** (leader → all other leaders):
```go
type InterClusterRequest struct {
    Operation     string
    Timestamp     int64
    ClientID      string
    Transition    TransitionType // always GLOBAL at this point
    OriginCluster int            // i: which cluster's leader sent this
}
```
Paper notation: `<INTER-CLUSTER-REQUEST, o, t, c, s>_σ_i`

**PrePrepare, Prepare, Commit, Reply** — these are the standard PBFT messages from Section II-A-1, augmented with digital signatures via the Envelope.

#### Helper Functions

**`NewEnvelope(msgType, senderID, body, privKey)`:** Creates a signed Envelope. Marshals `body` to JSON, signs the bytes with `privKey`, assembles the Envelope. This is the only way envelopes are created — no unsigned messages can exist.

**`ValidateEnvelope(env, pubKey)`:** Verifies the envelope's signature. Returns false for any failure. Must be called before processing any incoming message.

**`DecodeBody(env, &v)`:** Unmarshals `env.Body` into the typed struct `v`. Always called after `ValidateEnvelope`.

**`MsgTypeFor(v)`:** Returns the canonical MsgType for a given struct. Useful for routing without hardcoding type strings.

### `messages/types_test.go` — 18 Tests

Tests every message type for JSON round-trip fidelity, envelope creation and decoding, signature verification, that constants are distinct, and that `MsgTypeFor` works for both value and pointer receivers.

---

## 8. node/ — Node Abstraction

### `node/node.go`

Defines the `Node` struct which represents one vehicle in the network, plus fault-threshold helpers used throughout the protocol.

#### Type: `Role`

```go
type Role string
const (
    RoleClient  Role = "CLIENT"
    RoleReplica Role = "REPLICA"
    RoleLeader  Role = "LEADER"
)
```

A node's current function. Note that Leader is a special Replica — it does everything a replica does, plus it drives PrePrepare and coordinates Vote/VoteReply. A node can be promoted or demoted between roles when leadership rotates.

#### Type: `Node`

```go
type Node struct {
    // Identity
    ID         string
    Role       Role
    ClusterIdx int
    NodeIdx    int
    Location   cluster.Point

    // Cryptography
    PrivKey    *rsa.PrivateKey
    PubKey     *rsa.PublicKey
    KnownKeys  map[string]*rsa.PublicKey  // all peers' public keys

    // Protocol state
    ViewNumber  int
    SequenceID  int
    LocalState  state.NodeState

    // In-flight tallies (de-duplicated by sender ID)
    VoteReplies map[string]interface{}
    Prepares    map[int]map[string]bool  // seqID → senderID → seen
    Commits     map[int]map[string]bool

    // Committed log
    Log          []string
    executedSeqs map[int]bool
}
```

**The `KnownKeys` map** is the simulation of the NodeCA. In a real deployment, each node would fetch public keys from the Certificate Authority. In our implementation, `linkKeys()` in tests populates every node's `KnownKeys` with all other nodes' public keys — simulating the key distribution phase.

**Thread safety:** Node is NOT safe for concurrent use. The protocol layer owns synchronisation.

#### Function: `NewNode(id, role, clusterIdx, nodeIdx, loc)`

Constructs and initialises a Node, generating a fresh RSA-2048 key pair automatically. Registers the node's own public key in `KnownKeys` (so self-verification works). Initialises all maps with `make()` so handlers never panic on nil map writes.

#### Fault Threshold Functions

**`FaultyThresholdLocal(n)`:** Returns ⌊(n-1)/3⌋. This is the maximum number of Byzantine nodes the system can tolerate within one cluster of size n.

Examples:
- n=4:  ⌊3/3⌋ = 1  (can tolerate 1 faulty node)
- n=7:  ⌊6/3⌋ = 2  (can tolerate 2 faulty nodes)
- n=10: ⌊9/3⌋ = 3  (can tolerate 3 faulty nodes)

**`FaultyThresholdGlobal(p)`:** Returns ⌊(p-1)/3⌋. Global fault tolerance across the entire network.

**`HasQuorumLocal(count, fLocal)`:** Returns `count >= 2*fLocal+1`. The standard PBFT quorum threshold for Prepare and Commit phases.

**`HasQuorumGlobal(count, fGlobal)`:** Returns `count >= fGlobal+1`. **Critically different from local quorum.** This is used only for the client's reply collection (Section IX Step 8). The paper specifies f+1 (not 2f+1) for client replies because the client just needs to know that at least one honest node has committed the operation.

#### Tally Methods

**`AddPrepare(seqID, senderID)`:** Records a Prepare from `senderID` for `seqID`. Uses a `map[string]bool` so duplicate senders are automatically de-duplicated. Returns current distinct-sender count.

**`AddCommit(seqID, senderID)`:** Same for Commit messages.

**`AddVoteReply(senderID, msg)`:** Records a VoteReply. Returns current count.

**`ApplyOperation(seqID, operation)`:** Appends the operation to the node's log and records `seqID` as executed. **Idempotent:** if the same `seqID` is applied again, the call is a no-op. This prevents double-execution when a Commit message is delivered twice.

### `node/replica.go`

Implements the replica-side message handlers. These are called on nodes with `Role == RoleReplica` (or a leader processing messages from other leaders).

#### Function: `HandleVote(env)`

Called when a replica receives a Vote from its cluster's leader.

**Steps:**
1. Authenticate: look up sender's public key in `KnownKeys`, call `ValidateEnvelope`
2. Decode the Vote body
3. Verify the Digest matches the Message bytes (tamper check)
4. Build a VoteReply echoing the leader's proposed transition type (honest replica agrees)
5. Sign and return the VoteReply envelope

An honest replica echoes the proposed transition type. If the leader proposed GLOBAL and the replica is honest, it votes GLOBAL. A Byzantine replica might vote differently — this is why the leader needs 2f+1 consistent votes before proceeding.

#### Function: `HandlePrePrepare(env)`

Called when a replica receives a PrePrepare from the leader.

**Steps:**
1. Authenticate sender
2. Decode the PrePrepare
3. Verify Digest matches Message
4. Record the (viewNumber, sequenceID) pair to detect duplicate pre-prepares
5. Build and sign a Prepare message
6. Return the Prepare envelope (caller broadcasts this to all cluster nodes)

**Duplicate detection:** We record the key `"pp:viewNumber:sequenceID"` in `KnownKeys` (reusing the map for simplicity). If a second PrePrepare arrives for the same (view, seq), the duplicate entry is already there. Full view-change would require more sophisticated tracking, but for normal-case operation this is sufficient.

#### Function: `HandlePrepare(env, fLocal)`

Called when any node receives a Prepare broadcast.

**Steps:**
1. Authenticate sender
2. Decode the Prepare
3. Check view number matches
4. Tally: `n.AddPrepare(seqID, senderID)` — de-duplicates by sender
5. Check if tally >= 2f+1
6. If quorum reached: build and sign a Commit message, return it
7. If not: return nil (wait for more Prepares)

**One Commit per node:** Once a node produces a Commit for a given seqID, it does not produce another one even if more Prepares arrive. This is handled by the protocol layer's `commitProduced` map.

#### Function: `HandleCommit(env, fLocal, operation, clientID)`

Called when any node receives a Commit broadcast.

**Steps:**
1. Authenticate sender
2. Decode the Commit
3. Check view number
4. Tally: `n.AddCommit(seqID, senderID)`
5. Check if tally >= 2f+1
6. If quorum reached: call `ApplyOperation` to update state and log, build and sign a Reply
7. Return the Reply envelope

The `operation` parameter is provided by the `PBFTInstance` which stores the operation string in its `opLog` map by sequence ID.

#### Function: `OperationFromPrePrepareMessage(msg)`

Decodes the operation string from the `Message` field of a PrePrepare. The message is stored as `{"operation": "..."}` JSON so all nodes can reconstruct the operation string from just the message bytes.

### `node/leader.go`

Implements the leader-side handlers.

#### Function: `HandleIntraClusterRequest(env)`

Called when the leader receives a client's IntraClusterRequest.

**Steps:**
1. Check node is actually a leader (returns error if not)
2. Authenticate the client
3. Decode the request
4. Increment sequence counter with `NextSequenceID()`
5. Marshal the request to JSON, compute its Digest
6. Build a Vote message with the next seqID, the digest, and the client's proposed transition
7. Sign and return the Vote envelope (caller broadcasts to all replicas)

#### Function: `HandleVoteReplies(envs, fLocal, operation, clientID)`

Called by the leader after collecting VoteReply envelopes.

**Steps:**
1. For each VoteReply envelope: authenticate, decode, de-duplicate by sender, tally LOCAL vs GLOBAL votes
2. Check total >= 2f+1 (quorum)
3. If quorum not reached: return ("", nil, nil) — wait for more
4. If GLOBAL wins: build and sign an InterClusterRequest, return it with decided=GLOBAL
5. If LOCAL wins: call `StartPBFT(operation)`, return the PrePrepare with decided=LOCAL

**De-duplication:** Each sender ID is counted at most once, regardless of how many VoteReply envelopes arrive from that sender.

#### Function: `StartPBFT(operation)`

Called by the leader to initiate a PBFT round.

**Steps:**
1. Check node is a leader
2. Encode the operation as `{"operation": "..."}` JSON
3. Compute Digest of the encoded bytes
4. Increment `SequenceID`
5. Build a PrePrepare with this seqID, digest, and message
6. Sign and return the PrePrepare envelope

**Why encode operation as JSON:** Replicas need to retrieve the operation string from the log when handling Commit messages. By encoding it as a structured JSON object, `OperationFromPrePrepareMessage` can decode it reliably.

#### Function: `HandleInterClusterRequest(env)`

Called on a cluster leader when it receives an InterClusterRequest from another leader.

**Steps:**
1. Authenticate the sending leader
2. Decode the InterClusterRequest
3. Call `StartPBFT(icr.Operation)` to begin PBFT within this cluster
4. Return the resulting PrePrepare

This is how the global transition propagates: one leader sends InterClusterRequest to all others, each other leader calls this handler, and everyone starts PBFT concurrently.

### `node/node_test.go` — 16 Tests

Tests key generation distinctness, fault threshold formulas, quorum boundary conditions, idempotent ApplyOperation, leader-only restrictions on StartPBFT, the full Vote→VoteReply round-trip, the full PBFT flow (PrePrepare→Prepare→Commit→Reply), de-duplication of tallies, role promotion/demotion, and monotonic sequence IDs.

---

## 9. network/ — TCP Layer

### `network/server.go`

The TCP server that listens for incoming connections and delivers decoded Envelopes to the protocol layer via a channel.

#### Type: `Server`

```go
type Server struct {
    Port    int
    MsgChan chan messages.Envelope  // buffered channel, size 256
    listener net.Listener
    quit     chan struct{}
    wg       sync.WaitGroup
    mu       sync.Mutex
    started  bool
}
```

**Why a channel:** Channels are Go's idiomatic way of communicating between goroutines. The protocol layer goroutine reads from `MsgChan`; the network layer goroutines write to it. This decouples network I/O from protocol processing.

**Buffer size 256:** If the protocol layer is briefly busy, incoming messages queue up in the channel buffer rather than blocking the network goroutines. If the buffer fills (very busy node), messages are dropped with a warning rather than blocking forever.

#### Function: `Start()`

Binds to `127.0.0.1:Port`, starts the accept loop in a goroutine. Non-blocking — returns immediately. The accept loop runs until `Stop()` is called.

**Why 127.0.0.1:** Binds only to loopback, not to any network interface. This is correct for a single-machine simulation and avoids firewall issues.

#### Function: `Stop()`

Closes the listener (causes `Accept()` to return an error, terminating the accept loop), closes the quit channel (signals all handler goroutines to exit), waits for all goroutines to finish (`wg.Wait()`), then closes `MsgChan`. Idempotent — calling twice is safe.

#### Function: `acceptLoop()` (internal)

Runs in a goroutine started by `Start()`. Accepts connections in a loop. For each accepted connection, starts a `handleConn` goroutine. When `Stop()` closes the listener, `Accept()` returns an error; the goroutine checks the quit channel to distinguish normal shutdown from unexpected errors.

#### Function: `handleConn(conn)` (internal)

Handles one TCP connection. Uses `bufio.Scanner` to read newline-delimited JSON. For each line, tries to unmarshal into an `Envelope`. Valid envelopes are sent to `MsgChan` (non-blocking, drop if full). Exits when the connection closes or quit channel fires.

**Scanner buffer size:** 1 MB. RSA-2048 signatures in base64 are about 344 bytes, but an Envelope with a large operation payload could be several KB. 1 MB gives ample headroom.

### `network/client.go`

Provides functions for sending Envelopes to one or many recipients.

#### Function: `Send(addr, env)`

Opens a TCP connection to `addr`, writes the JSON-encoded Envelope + newline, closes the connection. Uses timeouts: 3 seconds to connect, 5 seconds to write.

**Connection-per-message:** A new TCP connection is opened for each `Send` call. This is simpler than maintaining a persistent connection pool and is fine for a single-machine simulation where TCP connection overhead is minimal.

#### Function: `Broadcast(addrs, env)`

Sends the same Envelope to all `addrs` concurrently (one goroutine per address). Blocks until all sends complete. Returns a slice of errors in the same order as `addrs` — nil means success.

#### Function: `Multicast(addrs, env)`

Semantic alias for `Broadcast`. Called specifically for the InterClusterRequest phase to make the protocol code readable.

#### Function: `BroadcastAsync(addrs, env)`

Fire-and-forget variant. Returns immediately with an error channel that receives any delivery failures and closes when all sends complete.

#### Function: `SendWithRetry(addr, env, maxRetries, baseDelay)`

Retries `Send` up to `maxRetries` times with exponential back-off (delay doubles each attempt). Used in tests where a server might not be listening yet.

### `network/network_test.go` — 13 Tests

Uses `freePort()` (asks the OS for an available port by binding to :0) to avoid port conflicts between parallel tests. Tests include: single send delivery within 1 second, multiple sequential sends, broadcast to 3 servers all receive, error slice same length as addrs, Stop closes MsgChan, double-Stop doesn't panic, send to closed port returns error, multicast to cluster leaders, BroadcastAsync errors on channel, SendWithRetry succeeds when server starts late, double-Start returns error, and 20 concurrent senders all deliver.

---

## 10. protocol/ — Consensus Engine

This is the most complex package. It wires together all previous packages into a working protocol.

### `protocol/pbft.go`

#### Type: `PBFTInstance`

```go
type PBFTInstance struct {
    Leader       *node.Node
    Replicas     []*node.Node  // honest replicas only; faulty excluded
    FLocal       int           // floor((totalClusterSize-1)/3)
    Addrs        map[string]string
    SendFn       SendFn        // nil = simulation mode
    Timeout      time.Duration
    PhaseDelayMs int           // simulated V2V delay
    opLog        map[int]string
    mu           sync.Mutex
}
```

**`Replicas` contains only honest nodes:** Faulty nodes are excluded from the node list. They simply don't receive any messages. With n=4 total nodes and 1 faulty excluded, `Replicas` has 2 entries plus the Leader = 3 honest nodes. With f_local=1, quorum is 2f+1=3, so all 3 honest nodes must participate.

**`FLocal` based on total cluster size:** This is critical. If we computed f_local from `len(Replicas)+1` (the honest count), the quorum threshold would be wrong. f_local must reflect the total cluster size (including the excluded faulty node) because the quorum formula was derived under the assumption of n total nodes.

**`PhaseDelayMs`:** Injects an artificial sleep between PBFT phases to simulate V2V message propagation latency on a single machine. Set by `main.go` via the `-delay` flag.

#### Function: `RunPBFT(ctx, operation, clientID)`

The core protocol engine. Runs a complete PBFT round synchronously (all nodes are in-process).

**Self-counting (critical correctness detail):**

In standard PBFT, a node counts its own Prepare toward the 2f+1 quorum. Our implementation must do this explicitly because we skip delivering a node's message to itself (to avoid redundant processing).

After calling `HandlePrePrepare` (which returns the Prepare envelope), we immediately call `nd.AddPrepare(seqID, nd.ID)` to self-count. This way:

- With n=4, f=1, 3 honest nodes:
  - Each node self-counts its own Prepare (count=1)
  - Each node receives Prepares from 2 others (count=3)
  - Quorum 2*1+1=3 reached ✓

Without self-counting:
  - Each node receives Prepares from 2 others (count=2)
  - Quorum 3 NOT reached ✗ (system would hang)

**The four phases of RunPBFT:**

**Phase 1 — PrePrepare:**
- Leader calls `StartPBFT(operation)` → gets PrePrepare envelope
- Stores the operation string in `opLog[seqID]`
- Checks context for cancellation

**Phase 2 — Prepare:**
For each honest node:
- Call `nd.HandlePrePrepare(ppEnv)` → gets Prepare envelope
- Self-count: `nd.AddPrepare(seqID, nd.ID)`
- Append Prepare to `prepareEnvs`

Check cancellation.

**Phase 3 — Commit:**
For each honest node, for each Prepare from other nodes (skip own):
- Call `nd.HandlePrepare(pEnv, FLocal)` → maybe gets Commit
- Self-count own Commit: `nd.AddCommit(seqID, nd.ID)`
- If Commit produced and not yet produced from this node: append to `commitEnvs`

Check cancellation.

**Phase 4 — Reply:**
For each honest node, for each Commit from other nodes (skip own):
- Call `nd.HandleCommit(cEnv, FLocal, operation, clientID)` → maybe gets Reply
- If Reply produced and not yet produced from this node: decode and append to `replies`

Return replies or error.

### `protocol/vote.go`

#### Function: `RunVotePhase(pbft, replies)`

Tallies VoteReply envelopes and returns the decided TransitionType.

**De-duplication (plan pitfall #3):** Each sender ID is counted at most once using a `seen` map. Duplicate messages from the same sender are silently dropped.

**Decision rule:** If globalCount >= localCount → GLOBAL. Otherwise → LOCAL. This means ties break toward GLOBAL (if equal votes, proceed with network-wide consensus).

**Quorum check:** Total valid replies must be >= 2*f_local+1. If not, returns an error.

#### Function: `BuildVoteReplies(pbft, voteEnv)`

Test helper that has each replica call `HandleVote` on the given Vote envelope and returns all resulting VoteReply envelopes. Used in `TestProtocolVotePhaseGlobalMajority`.

### `protocol/intra_cluster.go`

#### Function: `RunLocalTransition(ctx, pbft, operation, clientID)`

Implements Section XI of the paper — the local transition path that bypasses Vote/VoteReply entirely.

The paper says: "when a leader of a cluster receives an intra-cluster request proposing a local state transition, the protocol simplifies the process by bypassing the vote and vote-reply phases."

This function simply calls `pbft.RunPBFT(ctx, operation, clientID)`. The vote bypass is inherent in the fact that we call RunPBFT directly without calling RunVotePhase first.

#### Function: `RunProposedGlobalButLocal(ctx, pbft, operation, clientID)`

Implements Section X — the override path where the cluster votes LOCAL despite the client proposing GLOBAL.

The leader "constructs a new operation o' that represents the local version of the proposed global transition." We append ":local" to the operation string. This makes the committed log entry distinguishable from the global variant, which is testable.

Then calls `pbft.RunPBFT(ctx, localOperation, clientID)`.

### `protocol/inter_cluster.go`

#### Type: `GlobalCoordinator`

```go
type GlobalCoordinator struct {
    Clusters    []*PBFTInstance
    FGlobal     int
    LeaderAddrs map[int]string
}
```

Orchestrates a global state transition across all clusters. Each cluster has its own `PBFTInstance`. The coordinator runs all of them concurrently and collects replies.

#### Function: `RunGlobalTransition(ctx, originCluster, operation, clientID)`

Implements Section IX Steps 5-8.

**Concurrency:** One goroutine per cluster. All clusters start their PBFT rounds simultaneously. Results come back via a buffered channel (size = number of clusters).

```go
for i, pbft := range gc.Clusters {
    go func(idx int, inst *PBFTInstance) {
        replies, err := inst.RunPBFT(ctx, operation, clientID)
        resultCh <- result{...}
    }(i, pbft)
}
```

**After all goroutines finish:** A closer goroutine closes `resultCh`. The main goroutine ranges over `resultCh` collecting all replies.

**Global quorum check (Step 8):** The client considers the operation successful when it has received >= f_global+1 replies. This is `HasQuorumGlobal(len(allReplies), gc.FGlobal)`.

**Note on quorum:** f_global+1 is not 2f_global+1. For p=12, f_global=3, so the client needs only 4 replies out of 12 possible. This is because once the protocol has committed (2f+1 Commits within each cluster), the client just needs confirmation from enough nodes that this happened.

### `protocol/protocol_test.go` — 10 Tests

**`TestProtocolLocalTransition`:** Creates 4 nodes total, 1 faulty excluded (PBFTInstance has leader + 2 replicas = 3 honest). Calls `RunLocalTransition`. Verifies all 3 honest nodes' logs contain the operation. Verifies quorum was reached.

**`TestProtocolGlobalTransition`:** Creates 12 nodes across 3 clusters (all honest). Calls `RunGlobalTransition`. Verifies >= f_global+1 = 4 replies collected. Verifies all 3 cluster leaders committed the operation. Verifies NetworkState changed.

**`TestProtocolProposedGlobalButLocal`:** Builds VoteReply envelopes manually with `Transition=LOCAL` (injecting the votes). Calls `RunVotePhase` — verifies it returns LOCAL. Calls `RunProposedGlobalButLocal` — verifies the `:local` variant is committed.

**`TestProtocolByzantineLeaderTimeout`:** Demotes the leader to RoleReplica (simulating a Byzantine leader that refuses to drive PBFT). Calls `RunLocalTransition`. Verifies an error is returned, not a panic.

**`TestProtocolCancelledContextReturnsError`:** Cancels the context before calling `RunPBFT`. Verifies immediate error return.

Additional tests verify: GLOBAL majority in vote phase, quorum not reached with 0 replies, 4 honest nodes standard round, de-duplication of VoteReply senders, idempotent operations across multiple rounds.

---

## 11. dynamic/ — Dynamic Clustering

### `dynamic/clustering.go`

This implements Algorithm 2 from Section XII. The key innovation is that re-clustering happens without downtime — requests keep flowing while the new cluster assignments are being computed.

#### Type: `Tick`

```go
type Tick struct {
    NewNodes   []cluster.Point
    MovedNodes map[string]cluster.Point
}
```

Describes what happened during one time period: which vehicles joined the network and which moved to a new location.

#### Type: `TickMode`

```go
type TickMode struct {
    CurrentClusters []cluster.Cluster
    Nodes           []*nodemod.Node
    NodePoints      []cluster.Point     // parallel to Nodes
    Leaders         map[int]cluster.Point
    TickDuration    int                 // seconds (informational)
    MaxPerTick      int                 // T from paper = 4
    ServiceQueue    map[int][]string    // pending ops during re-clustering
    inTickMode      bool
    seed            int64               // increments each tick
    mu              sync.Mutex
}
```

**`NodePoints` parallel to `Nodes`:** `NodePoints[i]` is the geographic position of `Nodes[i]`. K-Means works only with Points; the protocol layer needs Node objects. Keeping them parallel allows both to be updated together.

**`ServiceQueue`:** During re-clustering, new requests cannot be routed because we don't know the new cluster assignments yet. Leaders queue incoming requests here. After `ProcessTick` completes, queued operations are forwarded to the appropriate new leaders.

**`seed`:** Each tick uses a different random seed for K-Means++. This prevents the algorithm from getting stuck in the same local optimum every time. The seed increments by 1 after each tick.

#### Function: `ProcessTick(t)`

Implements Algorithm 2. The seven steps:

**Step 1 — Enter tick mode:** Sets `inTickMode = true`. During this period, the protocol layer should enqueue rather than immediately process requests.

**Step 2 — Apply movements:** For each nodeID in `t.MovedNodes`, updates the corresponding entry in `NodePoints`. Also updates `nd.Location` on the Node object so the node is aware of its new position.

**Step 3 — Admit new nodes:** Takes up to `MaxPerTick` entries from `t.NewNodes`. For each, creates a new `node.Node` object (with a fresh RSA key pair) and appends both the Point and the Node to the parallel slices. Duplicate IDs are silently skipped.

**Step 4 — Re-cluster:** Calls `cluster.ComputeDimensions(p)` to get the new (n, m), then `cluster.SameSizeKMeansSeeded(NodePoints, m, n, seed)` to get the new cluster assignments.

**Step 5 — Re-elect leaders:** Calls `cluster.ElectAllLeaders(newClusters)` to get the new leader map.

**Step 6 — Forward pending requests:** For each cluster with pending operations in `ServiceQueue`, finds which new cluster's leader is closest to the old cluster's leader position, and re-enqueues the operations there. In a production deployment, this would call `network.Send` to actually deliver the operations to the new leader.

**Step 7 — Commit:** Updates `CurrentClusters`, `Leaders`, increments `seed`, clears `ServiceQueue`, sets `inTickMode = false`.

#### Function: `ForwardPendingRequests(oldLeaderID, newLeaderAddr)`

Transfers all pending operations from the cluster that `oldLeaderID` was leading to the node at `newLeaderAddr`. In production, this calls `network.Send` for each operation. In simulation, it clears the queue and acknowledges.

Used by the protocol layer after a view change or when a leader leaves its cluster due to mobility.

#### Helper: `findClosestCluster(pt, clusters)`

Returns the ID of the cluster whose centroid is geographically closest to `pt`. Used to route pending requests to the most appropriate new cluster leader.

### `dynamic/clustering_test.go` — 13 Tests

**`TestDynamicAddFourNodesToEight`:** Starts with 8 nodes (2 clusters), adds 4 new nodes, verifies 12 nodes in 3 clusters of 4.

**`TestDynamicMaxPerTickEnforced`:** Tries to add 6 nodes when MaxPerTick=4, verifies only 4 admitted (12 total, not 14).

**`TestDynamicNoNodeInTwoClusters`:** After re-clustering with various configurations, verifies every node ID appears in exactly one cluster.

**`TestDynamicLeaderIsAlwaysClosestToCentroid`:** Runs 3 ticks with minor movements, verifies after each tick that every cluster's leader is genuinely the node closest to the centroid.

**`TestDynamicStableLeaderSamePosition`:** Places a node exactly at the cluster centroid, moves other nodes slightly, verifies the center node is correctly recognized as the nearest to the new centroid.

**`TestDynamicLeaderReplacedWhenMovedAway`:** Moves the leader far away, verifies leader re-election picks a different node.

**`TestDynamicServiceQueueClearedAfterTick`:** Pre-populates service queue, verifies it is cleared after `ProcessTick`.

**`TestDynamicMovementOnlyTriggersLeaderReEvaluation`:** Moves all nodes without adding any, verifies leader re-election was triggered and NodePoints were updated.

**Additional tests:** Duplicate node ID ignored, ForwardPendingRequests clears queue, InTickMode flag set/cleared correctly, multi-tick growth 8→12→16 nodes.

---

## 12. metrics/ — Metrics Collection

### `metrics/metrics.go`

Collects and analyses performance metrics for the simulation. Thread-safe (all methods lock `mu`).

#### Type: `OperationRecord`

```go
type OperationRecord struct {
    OperationID string
    StartTime   time.Time
    EndTime     time.Time
    NodeCount   int
    IsGlobal    bool
    Success     bool
}
```

One record per consensus round. `StartTime` is set just before calling the protocol; `EndTime` just after. The difference is the end-to-end latency.

**`LatencyMs()`:** Returns `(EndTime - StartTime)` in milliseconds. Returns 0 for zero-value times.

#### Type: `Collector`

```go
type Collector struct {
    mu      sync.Mutex
    records []OperationRecord
}
```

**`Add(r)`:** Appends a record. Safe from multiple goroutines (main.go spawns one goroutine per request).

**`Throughput()`:** Computes the observation window as [earliest StartTime, latest EndTime] across all records. Returns `successCount / windowSeconds`.

**`MeanLatency()`:** Arithmetic mean of latency over all successful records.

**`P50Latency()` and `P99Latency()`:** Sorts latencies, returns the value at the 50th and 99th percentile using `ceil(p/100 * n) - 1` index formula.

**`MaxLatency()`:** Maximum latency across successful records.

**`SuccessRate()`:** `successCount / totalCount`.

**`Report()`:** Prints a formatted summary to stdout. Called by `main.go` after each simulation run.

**`SaveCSV(path)`:** Writes all records to a CSV file. Columns: operation_id, start_unix_ns, end_unix_ns, latency_ms, node_count, is_global, success. Used by `plot.py`.

**`Reset()`:** Clears all records. Useful for separating warm-up from measurement.

### `metrics/metrics_test.go` — 11 Tests

- LatencyMs of a 500ms operation = 500.0 ± 1ms
- Zero-value record returns 0 latency
- Throughput of 100 ops in ~1 second = ~100 ops/s ± 20
- Empty collector returns 0 throughput
- P99 of [1..100]ms = 99ms
- Mean of [100, 200, 300]ms = 200ms
- P50 of [10,20,...,100]ms = 50ms
- SaveCSV writes header + one row per record
- SuccessRate of 7/10 = 0.7
- Reset clears records
- 200 concurrent Add calls all recorded
- MaxLatency returns 200.0 from [10,50,200,1,75]
- Only successful ops included in latency stats

---

## 13. main.go — Simulation Runner

The entry point. Two modes: single scenario and full paper evaluation.

### Flags

```
-paper-eval   Run all 4 node counts × 5 loads → CSVs → graphs
-no-plot      Skip calling python3 plot.py after paper-eval
-nodes 12     Total number of nodes (single scenario)
-rps 100      Requests per second
-duration 5   Duration in seconds
-global true  Use global transitions
-delay 0      Simulated V2V delay per phase (ms)
-out results  CSV output filename
```

### Single Scenario Mode

1. Calls `buildClusters(m, n, fLocal, phaseDelayMs)` to create PBFTInstances
2. Creates a `GlobalCoordinator` or uses cluster instances directly for local transitions
3. Warms up with one consensus round (RSA key generation is slow; the first round includes setup overhead)
4. Runs `requestsPerSec × durationSec` operations using goroutines paced by a ticker
5. Collects records in a `metrics.Collector`
6. Prints the metrics report and PBFT comparison table
7. Saves to CSV

### Paper Evaluation Mode (`-paper-eval`)

**Static testbed (Figures 4-7):**
- For each node count in [8, 12, 16, 20]:
  - Build clusters ONCE (RSA key generation is expensive, ~100ms per key)
  - Warm up
  - For each load in [100, 200, 300, 400, 500] req/s:
    - Call `measure(coord, rps, 3)` — runs 3 seconds at the target load
    - Compute PBFT baseline using complexity model
    - Append to `StaticRow` slice
- Save to `results_static.csv`

**Dynamic testbed (Figure 8):**
- Uses the static baseline throughput for each node count
- Models tick overhead: re-clustering takes `clusteringMs[p]` time
- Computes adjusted throughput and latency for each (tick rate, node count) pair
- Saves to `results_dynamic.csv`

**Then calls `python3 plot.py`** (unless `-no-plot` was specified).

### `buildClusters(m, n, fLocal, phaseDelayMs)`

Creates `m` PBFTInstances for a cluster of n nodes each. For each cluster:
1. Creates n Node objects with RSA keys
2. Links all public keys (simulates NodeCA)
3. Creates a PBFTInstance with leader=nodes[0], replicas=nodes[1:]
4. Sets `inst.PhaseDelayMs = phaseDelayMs`

### `measure(coord, targetRPS, durationSec)`

Runs `targetRPS * durationSec` operations (capped at 50 for speed). Each operation runs in a goroutine. Measures wall-clock latency. Returns (throughput, mean latency in ms).

---

## 14. plot.py — Graph Generation

Reads `results_static.csv` and `results_dynamic.csv`, generates 7 PNG files matching the paper's visual format.

### Colors (extracted from paper PDF)

- **Dark green (#2e6b2e):** PBFT throughput bars
- **Sky blue (#a8d4ee):** Our Protocol throughput bars
- **Red (#d62728) + square markers:** PBFT latency line
- **Orange (#f0a30a) + circle markers:** Our Protocol latency line

These are pixel-sampled from the actual paper figures using PIL.

### `plot_static_figure(ax1, df_n, node_count, show_legend, title)`

Creates one dual-axis chart:
- Left axis (blue label): Throughput bars, dark green and sky blue, grouped at each x-tick
- Right axis (red label): Latency lines with markers
- Split legend: throughput legend top-left, latency legend top-right (matching paper layout)
- Title on chart (matching paper style)

### `generate_static_figures(df)`

Creates four individual figures (fig4_8nodes.png through fig7_20nodes.png) and one combined 2×2 grid (all_figures.png).

### `generate_dynamic_figure(df_dyn)`

Creates fig8_dynamic.png. Multiple lines for each tick rate. Solid lines = throughput (left axis), dashed lines = latency (right axis), same colour per tick rate.

### `generate_complexity_chart()`

Creates fig_complexity.png. Bar chart comparing O(n^1.5) vs O(n²) message counts at p=8,12,16,20,32,64,100. Labels show the speedup factor (p^2 / p^1.5 = p^0.5 = √p).

---

## 15. How to Run Everything

### Prerequisites

```bash
# Go 1.22
go version   # should show go1.22.x

# Python 3 with matplotlib and pandas
python3 -m pip install matplotlib pandas numpy
```

### Run all unit tests (no simulation)

```bash
cd vehicular-bft
go test ./... -count=1 -timeout 120s
```

Expected output: 9 lines of `ok github.com/yourusername/vehicular-bft/<package>`.

### Run single scenario

```bash
go run main.go -nodes 12 -rps 100 -duration 5
```

### Run full paper evaluation (generates CSVs + graphs)

```bash
go run main.go -paper-eval
```

Takes 3-10 minutes depending on hardware. Generates:
- `results_static.csv`
- `results_dynamic.csv`
- `fig4_8nodes.png` through `fig7_20nodes.png`
- `fig8_dynamic.png`
- `fig_complexity.png`
- `all_figures.png`

### Generate graphs from existing CSVs

```bash
python3 plot.py
```

### Vary parameters

```bash
# 8 nodes, match paper Figure 4
go run main.go -nodes 8 -rps 100 -duration 10

# With simulated V2V delay (20ms typical urban VANET)
go run main.go -nodes 12 -rps 100 -duration 10 -delay 20

# Local transitions instead of global
go run main.go -nodes 12 -rps 100 -duration 5 -global=false
```

---

*Implementation: Go 1.22 · RSA-2048 · Same-size K-Means++ · 9 packages · 129 tests · 4,090 lines of implementation*