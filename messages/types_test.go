package messages

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/adk2004/vehicular-bft/crypto"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// mustGenerateKey creates a key pair for tests, failing the test on error.
func mustGenerateKey(t *testing.T) interface{ Private() interface{} } {
	t.Helper()
	// We return the raw types; just use crypto directly in each test.
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Every message type round-trips through JSON without data loss
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesJSONRoundTripIntraClusterRequest(t *testing.T) {
	t.Parallel()
	original := IntraClusterRequest{
		Operation:  "SET signal=RED",
		Timestamp:  time.Now().UnixNano(),
		ClientID:   "client-99",
		Transition: GLOBAL,
	}
	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded IntraClusterRequest
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if original != decoded {
		t.Errorf("round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripVote(t *testing.T) {
	t.Parallel()
	inner := IntraClusterRequest{Operation: "op-42", Timestamp: 1000, ClientID: "c1", Transition: LOCAL}
	innerBytes, _ := json.Marshal(inner)
	original := Vote{
		ViewNumber: 3,
		SequenceID: 7,
		Digest:     "deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234",
		Message:    innerBytes,
		Transition: GLOBAL,
	}
	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded Vote
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.ViewNumber != original.ViewNumber ||
		decoded.SequenceID != original.SequenceID ||
		decoded.Digest != original.Digest ||
		decoded.Transition != original.Transition ||
		string(decoded.Message) != string(original.Message) {
		t.Errorf("Vote round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripVoteReply(t *testing.T) {
	t.Parallel()
	original := VoteReply{
		ViewNumber: 2,
		SequenceID: 5,
		Digest:     "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
		ReplicaIdx: 2,
		ClusterIdx: 1,
		Transition: LOCAL,
		Message:    []byte(`{"o":"test","t":1,"c":"c0","s":"GLOBAL"}`),
	}
	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded VoteReply
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.ViewNumber != original.ViewNumber ||
		decoded.SequenceID != original.SequenceID ||
		decoded.Digest != original.Digest ||
		decoded.ReplicaIdx != original.ReplicaIdx ||
		decoded.ClusterIdx != original.ClusterIdx ||
		decoded.Transition != original.Transition ||
		string(decoded.Message) != string(original.Message) {
		t.Errorf("VoteReply round-trip mismatch")
	}
}

func TestMessagesJSONRoundTripInterClusterRequest(t *testing.T) {
	t.Parallel()
	original := InterClusterRequest{
		Operation:     "UPDATE route=highway-A1",
		Timestamp:     123456789,
		ClientID:      "client-7",
		Transition:    GLOBAL,
		OriginCluster: 2,
	}
	b, _ := json.Marshal(original)
	var decoded InterClusterRequest
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if original != decoded {
		t.Errorf("InterClusterRequest round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripPrePrepare(t *testing.T) {
	t.Parallel()
	original := PrePrepare{
		ViewNumber: 1,
		SequenceID: 42,
		Digest:     "1111111111111111111111111111111111111111111111111111111111111111",
		Message:    []byte(`{"operation":"noop"}`),
	}
	b, _ := json.Marshal(original)
	var decoded PrePrepare
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.ViewNumber != original.ViewNumber ||
		decoded.SequenceID != original.SequenceID ||
		decoded.Digest != original.Digest ||
		string(decoded.Message) != string(original.Message) {
		t.Errorf("PrePrepare round-trip mismatch")
	}
}

func TestMessagesJSONRoundTripPrepare(t *testing.T) {
	t.Parallel()
	original := Prepare{
		ViewNumber: 1,
		SequenceID: 42,
		Digest:     "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
		NodeID:     "node-0-2",
	}
	b, _ := json.Marshal(original)
	var decoded Prepare
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if original != decoded {
		t.Errorf("Prepare round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripCommit(t *testing.T) {
	t.Parallel()
	original := Commit{
		ViewNumber: 1,
		SequenceID: 42,
		Digest:     "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
		NodeID:     "node-1-3",
	}
	b, _ := json.Marshal(original)
	var decoded Commit
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if original != decoded {
		t.Errorf("Commit round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripReply(t *testing.T) {
	t.Parallel()
	original := Reply{
		ViewNumber: 1,
		Timestamp:  987654321,
		ClientID:   "client-3",
		NodeID:     "node-2-0",
		Result:     "OK:signal=RED",
	}
	b, _ := json.Marshal(original)
	var decoded Reply
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if original != decoded {
		t.Errorf("Reply round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestMessagesJSONRoundTripEnvelope(t *testing.T) {
	t.Parallel()
	original := Envelope{
		Type:      MsgPrePrepare,
		SenderID:  "node-0-0",
		Signature: "c2lnbmF0dXJl", // base64 placeholder
		Body:      []byte(`{"v":1,"n":5,"d":"abcd","m":"dGVzdA=="}`),
	}
	b, _ := json.Marshal(original)
	var decoded Envelope
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Type != original.Type ||
		decoded.SenderID != original.SenderID ||
		decoded.Signature != original.Signature ||
		string(decoded.Body) != string(original.Body) {
		t.Errorf("Envelope round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Envelope wrapping a Vote round-trips: Type checked, body decoded
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesEnvelopeWrapsAndDecodesVote(t *testing.T) {
	t.Parallel()

	priv, _, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	// Build the inner Vote.
	inner := IntraClusterRequest{Operation: "SET x=1", Timestamp: 42, ClientID: "c0", Transition: GLOBAL}
	innerBytes, _ := json.Marshal(inner)

	vote := Vote{
		ViewNumber: 1,
		SequenceID: 3,
		Digest:     "cafebabe00000000cafebabe00000000cafebabe00000000cafebabe00000000",
		Message:    innerBytes,
		Transition: GLOBAL,
	}

	// Wrap in a signed envelope.
	env, err := NewEnvelope(MsgVote, "node-0-0", vote, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	// Check the Type discriminator.
	if env.Type != MsgVote {
		t.Errorf("env.Type = %q, want %q", env.Type, MsgVote)
	}
	if env.SenderID != "node-0-0" {
		t.Errorf("env.SenderID = %q, want node-0-0", env.SenderID)
	}

	// Decode the body back into a Vote.
	var decoded Vote
	if err := DecodeBody(env, &decoded); err != nil {
		t.Fatalf("DecodeBody: %v", err)
	}
	if decoded.ViewNumber != vote.ViewNumber ||
		decoded.SequenceID != vote.SequenceID ||
		decoded.Digest != vote.Digest ||
		decoded.Transition != vote.Transition {
		t.Errorf("decoded Vote mismatch:\n  original: %+v\n  decoded:  %+v", vote, decoded)
	}
}

// Test that each message type can be wrapped and decoded via NewEnvelope+DecodeBody.
func TestMessagesEnvelopeRoundTripAllTypes(t *testing.T) {
	t.Parallel()

	priv, _, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	payloads := []struct {
		msgType MsgType
		body    interface{}
		decode  func(Envelope) error
	}{
		{
			MsgIntraClusterRequest,
			IntraClusterRequest{Operation: "op", Timestamp: 1, ClientID: "c", Transition: LOCAL},
			func(env Envelope) error {
				var v IntraClusterRequest
				return DecodeBody(env, &v)
			},
		},
		{
			MsgVote,
			Vote{ViewNumber: 1, SequenceID: 1, Digest: "d", Message: []byte("m"), Transition: GLOBAL},
			func(env Envelope) error {
				var v Vote
				return DecodeBody(env, &v)
			},
		},
		{
			MsgVoteReply,
			VoteReply{ViewNumber: 1, SequenceID: 1, Digest: "d", ReplicaIdx: 0, ClusterIdx: 0, Transition: LOCAL, Message: []byte("m")},
			func(env Envelope) error {
				var v VoteReply
				return DecodeBody(env, &v)
			},
		},
		{
			MsgInterClusterRequest,
			InterClusterRequest{Operation: "op", Timestamp: 2, ClientID: "c", Transition: GLOBAL, OriginCluster: 1},
			func(env Envelope) error {
				var v InterClusterRequest
				return DecodeBody(env, &v)
			},
		},
		{
			MsgPrePrepare,
			PrePrepare{ViewNumber: 1, SequenceID: 5, Digest: "d", Message: []byte("m")},
			func(env Envelope) error {
				var v PrePrepare
				return DecodeBody(env, &v)
			},
		},
		{
			MsgPrepare,
			Prepare{ViewNumber: 1, SequenceID: 5, Digest: "d", NodeID: "n"},
			func(env Envelope) error {
				var v Prepare
				return DecodeBody(env, &v)
			},
		},
		{
			MsgCommit,
			Commit{ViewNumber: 1, SequenceID: 5, Digest: "d", NodeID: "n"},
			func(env Envelope) error {
				var v Commit
				return DecodeBody(env, &v)
			},
		},
		{
			MsgReply,
			Reply{ViewNumber: 1, Timestamp: 99, ClientID: "c", NodeID: "n", Result: "ok"},
			func(env Envelope) error {
				var v Reply
				return DecodeBody(env, &v)
			},
		},
	}

	for _, p := range payloads {
		p := p
		t.Run(string(p.msgType), func(t *testing.T) {
			t.Parallel()
			env, err := NewEnvelope(p.msgType, "sender", p.body, priv)
			if err != nil {
				t.Fatalf("NewEnvelope(%s): %v", p.msgType, err)
			}
			if env.Type != p.msgType {
				t.Errorf("env.Type = %q, want %q", env.Type, p.msgType)
			}
			if err := p.decode(env); err != nil {
				t.Errorf("DecodeBody(%s): %v", p.msgType, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — TransitionType constants are exactly "LOCAL" and "GLOBAL"
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesTransitionTypeConstants(t *testing.T) {
	t.Parallel()

	if string(LOCAL) != "LOCAL" {
		t.Errorf("LOCAL = %q, want \"LOCAL\"", LOCAL)
	}
	if string(GLOBAL) != "GLOBAL" {
		t.Errorf("GLOBAL = %q, want \"GLOBAL\"", GLOBAL)
	}
	// Must be distinct.
	if LOCAL == GLOBAL {
		t.Error("LOCAL == GLOBAL — they must be different constants")
	}
}

// TransitionType round-trips through JSON correctly.
func TestMessagesTransitionTypeJSONRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []TransitionType{LOCAL, GLOBAL} {
		tt := tt
		t.Run(string(tt), func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt)
			if err != nil {
				t.Fatalf("Marshal(%s): %v", tt, err)
			}
			var decoded TransitionType
			if err := json.Unmarshal(b, &decoded); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tt, err)
			}
			if decoded != tt {
				t.Errorf("TransitionType round-trip: got %q, want %q", decoded, tt)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ValidateEnvelope — signature verification
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesValidateEnvelopeAcceptsValidSignature(t *testing.T) {
	t.Parallel()

	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	env, err := NewEnvelope(MsgCommit, "node-1-0",
		Commit{ViewNumber: 2, SequenceID: 8, Digest: "d", NodeID: "node-1-0"}, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	if !ValidateEnvelope(env, pub) {
		t.Error("ValidateEnvelope returned false for a legitimately signed envelope")
	}
}

func TestMessagesValidateEnvelopeRejectsTamperedBody(t *testing.T) {
	t.Parallel()

	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	env, err := NewEnvelope(MsgPrepare, "node-0-1",
		Prepare{ViewNumber: 1, SequenceID: 3, Digest: "d", NodeID: "node-0-1"}, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	// Tamper with the body after signing.
	env.Body = append(env.Body, []byte("TAMPERED")...)

	if ValidateEnvelope(env, pub) {
		t.Error("ValidateEnvelope returned true for a tampered body — should be false")
	}
}

func TestMessagesValidateEnvelopeRejectsWrongKey(t *testing.T) {
	t.Parallel()

	priv, _, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair (signer): %v", err)
	}
	_, wrongPub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair (wrong): %v", err)
	}

	env, err := NewEnvelope(MsgReply, "node-2-0",
		Reply{ViewNumber: 1, Timestamp: 10, ClientID: "c", NodeID: "node-2-0", Result: "ok"}, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	if ValidateEnvelope(env, wrongPub) {
		t.Error("ValidateEnvelope returned true with the wrong public key — should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MsgType constants — all 8 must be non-empty and distinct
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesMsgTypeConstantsAreDistinct(t *testing.T) {
	t.Parallel()

	allTypes := []MsgType{
		MsgIntraClusterRequest,
		MsgVote,
		MsgVoteReply,
		MsgInterClusterRequest,
		MsgPrePrepare,
		MsgPrepare,
		MsgCommit,
		MsgReply,
	}

	seen := make(map[MsgType]bool)
	for _, mt := range allTypes {
		if mt == "" {
			t.Errorf("a MsgType constant is empty string")
		}
		if seen[mt] {
			t.Errorf("duplicate MsgType value: %q", mt)
		}
		seen[mt] = true
	}
	if len(seen) != 8 {
		t.Errorf("expected 8 distinct MsgType constants, got %d", len(seen))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MsgTypeFor helper
// ─────────────────────────────────────────────────────────────────────────────

func TestMessagesMsgTypeForAllStructs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		v    interface{}
		want MsgType
	}{
		{IntraClusterRequest{}, MsgIntraClusterRequest},
		{&IntraClusterRequest{}, MsgIntraClusterRequest},
		{Vote{}, MsgVote},
		{&Vote{}, MsgVote},
		{VoteReply{}, MsgVoteReply},
		{&VoteReply{}, MsgVoteReply},
		{InterClusterRequest{}, MsgInterClusterRequest},
		{&InterClusterRequest{}, MsgInterClusterRequest},
		{PrePrepare{}, MsgPrePrepare},
		{&PrePrepare{}, MsgPrePrepare},
		{Prepare{}, MsgPrepare},
		{&Prepare{}, MsgPrepare},
		{Commit{}, MsgCommit},
		{&Commit{}, MsgCommit},
		{Reply{}, MsgReply},
		{&Reply{}, MsgReply},
		{"unknown", MsgType("")}, // unknown type → empty string
	}

	for _, tc := range cases {
		got := MsgTypeFor(tc.v)
		if got != tc.want {
			t.Errorf("MsgTypeFor(%T) = %q, want %q", tc.v, got, tc.want)
		}
	}
}