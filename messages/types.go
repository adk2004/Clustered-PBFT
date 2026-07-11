// Package messages defines every wire message used by the vehicular BFT protocol.
//
// The paper (Section VII) specifies the following message flow:
//
//	Client → Leader:       IntraClusterRequest
//	Leader → Replicas:     Vote
//	Replicas → Leader:     VoteReply
//	Leader → Leaders:      InterClusterRequest        (global path only)
//	Leader → Replicas:     PrePrepare                 (standard PBFT)
//	Replicas → All:        Prepare
//	Replicas → All:        Commit
//	Nodes → Client:        Reply
//
// All messages are transmitted inside a signed Envelope so that any receiver
// can authenticate the sender before processing (plan pitfall #1).
//
// Signature convention (pitfall #2):
//   - The Signature field is Sign(bodyBytes) where bodyBytes = json.Marshal(inner struct).
//   - It is computed BEFORE the struct is placed in Envelope.Body, so there is
//     no circular dependency.
//   - The 'd' (Digest) field that appears inside Vote, VoteReply, PrePrepare etc.
//     is a separate SHA-256 hex digest of the *inner* payload — it is part of the
//     PBFT protocol logic, not the transport authentication.
//
// Usage:
//
//	env, err := messages.NewEnvelope(messages.MsgVote, "node-0-1", vote, privKey)
//	ok        := messages.ValidateEnvelope(env, senderPubKey)
//	var v messages.Vote
//	err = messages.DecodeBody(env, &v)
package messages

import (
	"encoding/json"
	"fmt"

	"github.com/adk2004/vehicular-bft/crypto"

	gocrypto "crypto/rsa"
)

// ─────────────────────────────────────────────────────────────────────────────
// TransitionType — LOCAL vs GLOBAL
// ─────────────────────────────────────────────────────────────────────────────

// TransitionType encodes whether a requested state transition affects only the
// local cluster (LOCAL) or the entire network (GLOBAL) — paper Section VII.
type TransitionType string

const (
	// LOCAL means only the cluster receiving the IntraClusterRequest is affected.
	// The Vote / VoteReply and Inter-Cluster phases are skipped (paper Section XI).
	LOCAL TransitionType = "LOCAL"

	// GLOBAL means the transition must propagate to all clusters via the
	// InterClusterRequest phase (paper Section IX).
	GLOBAL TransitionType = "GLOBAL"
)

// ─────────────────────────────────────────────────────────────────────────────
// MsgType — message kind discriminator
// ─────────────────────────────────────────────────────────────────────────────

// MsgType is the string tag embedded in every Envelope.Type field.
// Receivers switch on this value to route the Envelope to the correct handler.
type MsgType string

const (
	MsgIntraClusterRequest MsgType = "INTRA-CLUSTER-REQUEST"
	MsgVote                MsgType = "VOTE"
	MsgVoteReply           MsgType = "VOTE-REPLY"
	MsgInterClusterRequest MsgType = "INTER-CLUSTER-REQUEST"
	MsgPrePrepare          MsgType = "PRE-PREPARE"
	MsgPrepare             MsgType = "PREPARE"
	MsgCommit              MsgType = "COMMIT"
	MsgReply               MsgType = "REPLY"
)

// ─────────────────────────────────────────────────────────────────────────────
// Envelope — outer transport wrapper
// ─────────────────────────────────────────────────────────────────────────────

// Envelope wraps any protocol message body for transmission over TCP.
// Every field except Body is set by NewEnvelope; Body is the JSON encoding
// of one of the typed structs below.
//
// Wire format: JSON + newline (network/server.go appends '\n').
type Envelope struct {
	// Type identifies which inner struct lives in Body.
	Type MsgType `json:"type"`

	// SenderID is the ID of the node that created and signed this envelope.
	SenderID string `json:"sender_id"`

	// Signature is Sign(bodyBytes) produced with the sender's RSA private key.
	// Receivers call ValidateEnvelope to check it before processing Body.
	Signature string `json:"signature"`

	// Body is json.Marshal(inner message struct). Decode with DecodeBody.
	Body []byte `json:"body"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-message structs — paper Section VII
// ─────────────────────────────────────────────────────────────────────────────

// IntraClusterRequest is sent by a client to the leader of its cluster.
//
// Paper notation: <INTRA-CLUSTER-REQUEST, o, t, c, s>_σc
type IntraClusterRequest struct {
	// Operation (o) is the requested state transition payload.
	Operation string `json:"o"`

	// Timestamp (t) is a Unix nanosecond timestamp used to detect replays.
	Timestamp int64 `json:"t"`

	// ClientID (c) uniquely identifies the requesting client.
	ClientID string `json:"c"`

	// Transition (s) is the client's proposed transition type: LOCAL or GLOBAL.
	// The cluster's Vote phase may override this to LOCAL (paper Section X).
	Transition TransitionType `json:"s"`
}

// Vote is broadcast by the cluster leader to all replicas after receiving an
// IntraClusterRequest, asking them to vote on the transition type.
//
// Paper notation: <<VOTE, v, g, d, s>, h>_σ_ip
type Vote struct {
	// ViewNumber (v) — current PBFT view (incremented on leader rotation).
	ViewNumber int `json:"v"`

	// SequenceID (g) — monotonically increasing request counter for this leader.
	SequenceID int `json:"g"`

	// Digest (d) — SHA-256 hex digest of Message (the IntraClusterRequest body).
	Digest string `json:"d"`

	// Message (h) — the raw JSON bytes of the IntraClusterRequest being voted on.
	Message []byte `json:"h"`

	// Transition (s) — the client's proposed transition type being put to a vote.
	Transition TransitionType `json:"s"`
}

// VoteReply is sent by each replica to the leader in response to a Vote.
//
// Paper notation: <<VOTE-REPLY, v, g, d, j, s>, h>_σ_ij
type VoteReply struct {
	// ViewNumber (v) — must match the Vote's ViewNumber.
	ViewNumber int `json:"v"`

	// SequenceID (g) — must match the Vote's SequenceID.
	SequenceID int `json:"g"`

	// Digest (d) — must match the Vote's Digest (replica verified it).
	Digest string `json:"d"`

	// ReplicaIdx (j) — index of this replica within its cluster (0-based).
	ReplicaIdx int `json:"j"`

	// ClusterIdx (i) — index of the cluster this replica belongs to.
	ClusterIdx int `json:"i"`

	// Transition (s) — the replica's vote: LOCAL or GLOBAL.
	Transition TransitionType `json:"s"`

	// Message (h) — echo of the Vote's Message field for cross-checking.
	Message []byte `json:"h"`
}

// InterClusterRequest is multicast by the initiating cluster's leader to all
// other cluster leaders when the Vote phase produces a GLOBAL result.
//
// Paper notation: <INTER-CLUSTER-REQUEST, o, t, c, s>_σi
type InterClusterRequest struct {
	// Operation (o) — same operation as the originating IntraClusterRequest.
	Operation string `json:"o"`

	// Timestamp (t) — forwarded from the original client request.
	Timestamp int64 `json:"t"`

	// ClientID (c) — forwarded from the original client request.
	ClientID string `json:"c"`

	// Transition (s) — always GLOBAL at this point.
	Transition TransitionType `json:"s"`

	// OriginCluster (i) — index of the cluster whose leader sent this message.
	OriginCluster int `json:"i"`
}

// PrePrepare is the first phase of in-cluster PBFT, sent by the leader.
//
// Paper notation: <<PRE-PREPARE, v, n, d>, m>
type PrePrepare struct {
	// ViewNumber (v) — current view number.
	ViewNumber int `json:"v"`

	// SequenceID (n) — sequence number for ordering within the view.
	SequenceID int `json:"n"`

	// Digest (d) — SHA-256 hex of Message m.
	Digest string `json:"d"`

	// Message (m) — JSON bytes of the operation being proposed.
	Message []byte `json:"m"`
}

// Prepare is broadcast by a replica after accepting a PrePrepare.
//
// Paper notation: <PREPARE, v, n, d, i>
type Prepare struct {
	// ViewNumber (v) — must match the PrePrepare's ViewNumber.
	ViewNumber int `json:"v"`

	// SequenceID (n) — must match the PrePrepare's SequenceID.
	SequenceID int `json:"n"`

	// Digest (d) — must match the PrePrepare's Digest.
	Digest string `json:"d"`

	// NodeID (i) — ID of the replica sending this Prepare.
	NodeID string `json:"i"`
}

// Commit is broadcast by a replica after collecting 2f+1 Prepare messages.
//
// Paper notation: <COMMIT, v, n, d, i>
type Commit struct {
	// ViewNumber (v) — current view number.
	ViewNumber int `json:"v"`

	// SequenceID (n) — sequence number being committed.
	SequenceID int `json:"n"`

	// Digest (d) — digest of the committed operation.
	Digest string `json:"d"`

	// NodeID (i) — ID of the node sending this Commit.
	NodeID string `json:"i"`
}

// Reply is sent by each node to the client after executing the committed operation.
// The client waits for >= f_global + 1 matching replies (paper Section IX, Step 8).
//
// Paper notation: <REPLY, v, t, c, i, r>
type Reply struct {
	// ViewNumber (v) — view in which the operation was executed.
	ViewNumber int `json:"v"`

	// Timestamp (t) — timestamp from the original client request (for matching).
	Timestamp int64 `json:"t"`

	// ClientID (c) — ID of the client this reply is addressed to.
	ClientID string `json:"c"`

	// NodeID (i) — ID of the node sending this reply.
	NodeID string `json:"i"`

	// Result (r) — the outcome of executing the operation.
	Result string `json:"r"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Envelope helpers
// ─────────────────────────────────────────────────────────────────────────────

// NewEnvelope creates a signed Envelope ready for transmission.
//
// Steps:
//  1. JSON-marshal body into Body bytes.
//  2. Sign the Body bytes with privKey (RSA-PKCS1v15 + SHA-256).
//  3. Return the assembled Envelope.
//
// The Signature is over Body so that receivers can verify it with only the raw
// Body bytes — no re-serialization round-trip needed (avoids pitfall #2).
func NewEnvelope(msgType MsgType, senderID string, body interface{}, privKey *gocrypto.PrivateKey) (Envelope, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return Envelope{}, fmt.Errorf("messages.NewEnvelope: marshal body: %w", err)
	}

	sig, err := crypto.Sign(privKey, bodyBytes)
	if err != nil {
		return Envelope{}, fmt.Errorf("messages.NewEnvelope: sign: %w", err)
	}

	return Envelope{
		Type:      msgType,
		SenderID:  senderID,
		Signature: sig,
		Body:      bodyBytes,
	}, nil
}

// ValidateEnvelope verifies that env.Signature was produced by the holder of
// pubKey over env.Body. Returns false on any failure (wrong key, tampered body,
// bad base64). Receivers MUST call this before DecodeBody (plan pitfall #1).
func ValidateEnvelope(env Envelope, pubKey *gocrypto.PublicKey) bool {
	return crypto.Verify(pubKey, env.Body, env.Signature)
}

// DecodeBody JSON-unmarshals env.Body into v.
// v must be a non-nil pointer to the expected message struct.
//
// Example:
//
//	var vote messages.Vote
//	if err := messages.DecodeBody(env, &vote); err != nil { ... }
func DecodeBody(env Envelope, v interface{}) error {
	if err := json.Unmarshal(env.Body, v); err != nil {
		return fmt.Errorf("messages.DecodeBody (type=%s): %w", env.Type, err)
	}
	return nil
}

// MsgTypeFor returns the canonical MsgType for a given message struct value.
// Useful for constructing envelopes without hard-coding the type string at
// every call site. Returns an empty string for unknown types.
func MsgTypeFor(v interface{}) MsgType {
	switch v.(type) {
	case IntraClusterRequest, *IntraClusterRequest:
		return MsgIntraClusterRequest
	case Vote, *Vote:
		return MsgVote
	case VoteReply, *VoteReply:
		return MsgVoteReply
	case InterClusterRequest, *InterClusterRequest:
		return MsgInterClusterRequest
	case PrePrepare, *PrePrepare:
		return MsgPrePrepare
	case Prepare, *Prepare:
		return MsgPrepare
	case Commit, *Commit:
		return MsgCommit
	case Reply, *Reply:
		return MsgReply
	default:
		return ""
	}
}