package crypto

import (
	"strings"
	"testing"
)

// ── Test 1 ────────────────────────────────────────────────────────────────────
// GenerateKeyPair must return non-nil keys, no error, and a 2048-bit modulus.

func TestGenerateKeyPair(t *testing.T) {
	t.Parallel()

	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() returned unexpected error: %v", err)
	}
	if priv == nil {
		t.Fatal("GenerateKeyPair() returned nil private key")
	}
	if pub == nil {
		t.Fatal("GenerateKeyPair() returned nil public key")
	}
	// Validate RSA key size — must be 2048 bits (config.RSABits).
	if bits := priv.N.BitLen(); bits != 2048 {
		t.Errorf("expected 2048-bit key, got %d-bit key", bits)
	}
	// Public key embedded in the private key must match the returned public key.
	if priv.PublicKey.N.Cmp(pub.N) != 0 || priv.PublicKey.E != pub.E {
		t.Error("public key embedded in private key does not match returned public key")
	}
}

// Two independently generated key pairs must differ (birthday-problem probability
// of collision is negligible with 2048-bit keys, so this test is deterministic in
// practice).
func TestGenerateKeyPairDistinct(t *testing.T) {
	t.Parallel()

	_, pub1, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("first GenerateKeyPair: %v", err)
	}
	_, pub2, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("second GenerateKeyPair: %v", err)
	}
	if pub1.N.Cmp(pub2.N) == 0 {
		t.Error("two independently generated key pairs share the same modulus — highly suspicious")
	}
}

// ── Test 2 ────────────────────────────────────────────────────────────────────
// Sign → Verify round-trip must succeed with the matching key pair.

func TestSignVerifyRoundTrip(t *testing.T) {
	t.Parallel()

	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	data := []byte("test message: PBFT pre-prepare for sequence 42")

	sig, err := Sign(priv, data)
	if err != nil {
		t.Fatalf("Sign() returned unexpected error: %v", err)
	}
	if sig == "" {
		t.Fatal("Sign() returned empty signature")
	}
	// Signature is base64 — must not contain raw binary.
	if strings.ContainsAny(sig, "\x00\n\r") {
		t.Error("Sign() returned a signature with control characters — expected clean base64")
	}

	if !Verify(pub, data, sig) {
		t.Fatal("Verify() returned false for a valid (priv, data, sig) triple — should be true")
	}
}

// ── Test 3 ────────────────────────────────────────────────────────────────────
// Verify must return false when the signed data is altered (tamper-detection).

func TestVerifyReturnsFalseOnTamperedData(t *testing.T) {
	t.Parallel()

	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	original := []byte("legitimate vote: GLOBAL")
	sig, err := Sign(priv, original)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	tests := []struct {
		name    string
		tampered []byte
	}{
		{"extra byte appended", append(original, 0xFF)},
		{"one byte flipped", func() []byte {
			cp := make([]byte, len(original))
			copy(cp, original)
			cp[0] ^= 0x01
			return cp
		}()},
		{"completely different message", []byte("malicious vote: LOCAL")},
		{"empty message", []byte{}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if Verify(pub, tc.tampered, sig) {
				t.Errorf("Verify() returned true for tampered data (%s) — should be false", tc.name)
			}
		})
	}
}

// ── Test 4 ────────────────────────────────────────────────────────────────────
// Verify must return false when the public key does not match the signing key.

func TestVerifyReturnsFalseWithWrongKey(t *testing.T) {
	t.Parallel()

	// Legitimate signer.
	signerPriv, _, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair (signer): %v", err)
	}
	// Unrelated key pair (attacker/wrong node).
	_, wrongPub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair (wrong): %v", err)
	}

	data := []byte("commit message for seq 7")
	sig, err := Sign(signerPriv, data)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	if Verify(wrongPub, data, sig) {
		t.Fatal("Verify() returned true with the wrong public key — should be false")
	}
}

// Nil key must not panic — Verify should return false gracefully.
func TestVerifyNilKeyReturnsFalse(t *testing.T) {
	t.Parallel()

	priv, _, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	data := []byte("any data")
	sig, _ := Sign(priv, data)

	if Verify(nil, data, sig) {
		t.Error("Verify(nil key, ...) should return false, not true")
	}
}

// ── Test 5 ────────────────────────────────────────────────────────────────────
// Digest must be deterministic: same input → identical 64-char hex string.

func TestDigestDeterministic(t *testing.T) {
	t.Parallel()

	type pbftMsg struct {
		NodeID     string
		ViewNumber int
		SeqID      int
		Operation  string
	}
	v := pbftMsg{NodeID: "node-3", ViewNumber: 2, SeqID: 17, Operation: "SET traffic_signal=GREEN"}

	d1, err := Digest(v)
	if err != nil {
		t.Fatalf("Digest (first call): %v", err)
	}
	d2, err := Digest(v)
	if err != nil {
		t.Fatalf("Digest (second call): %v", err)
	}

	if d1 != d2 {
		t.Errorf("Digest is not deterministic:\n  call 1: %q\n  call 2: %q", d1, d2)
	}
	// SHA-256 produces 32 bytes → 64 hex characters.
	if len(d1) != 64 {
		t.Errorf("Digest length = %d, want 64 (SHA-256 hex)", len(d1))
	}
	// Must be lowercase hex only.
	for i, c := range d1 {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("Digest character at position %d (%q) is not lowercase hex", i, c)
			break
		}
	}
}

// Distinct inputs must produce distinct digests (collision sanity check).
func TestDigestDistinctInputs(t *testing.T) {
	t.Parallel()

	d1, err := Digest(map[string]int{"seq": 1})
	if err != nil {
		t.Fatalf("Digest (input 1): %v", err)
	}
	d2, err := Digest(map[string]int{"seq": 2})
	if err != nil {
		t.Fatalf("Digest (input 2): %v", err)
	}
	if d1 == d2 {
		t.Error("Digest produced the same hash for different inputs")
	}
}

// ── Test 6 ────────────────────────────────────────────────────────────────────
// SerializePublicKey → DeserializePublicKey must be a perfect round-trip.

func TestSerializeDeserializePublicKeyRoundTrip(t *testing.T) {
	t.Parallel()

	_, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	pemStr, err := SerializePublicKey(pub)
	if err != nil {
		t.Fatalf("SerializePublicKey: %v", err)
	}
	if pemStr == "" {
		t.Fatal("SerializePublicKey returned an empty string")
	}
	// Must begin with PEM header.
	if !strings.HasPrefix(pemStr, "-----BEGIN PUBLIC KEY-----") {
		t.Errorf("SerializePublicKey output does not start with PEM header:\n%s", pemStr)
	}

	restored, err := DeserializePublicKey(pemStr)
	if err != nil {
		t.Fatalf("DeserializePublicKey: %v", err)
	}
	if restored == nil {
		t.Fatal("DeserializePublicKey returned a nil key")
	}

	// N (modulus) must match exactly.
	if pub.N.Cmp(restored.N) != 0 {
		t.Error("public key modulus (N) changed after round-trip serialization")
	}
	// E (public exponent) must match.
	if pub.E != restored.E {
		t.Errorf("public key exponent (E) changed after round-trip: original=%d restored=%d",
			pub.E, restored.E)
	}
}

// Deserializing garbage PEM must return a non-nil error, not panic.
func TestDeserializeInvalidPEMReturnsError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"random bytes", "not-pem-at-all"},
		{"truncated PEM", "-----BEGIN PUBLIC KEY-----\naGVsbG8=\n"},
		{"wrong PEM type", "-----BEGIN CERTIFICATE-----\naGVsbG8=\n-----END CERTIFICATE-----\n"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			key, err := DeserializePublicKey(tc.input)
			if err == nil {
				t.Errorf("DeserializePublicKey(%q) returned nil error — expected an error", tc.name)
			}
			if key != nil {
				t.Errorf("DeserializePublicKey(%q) returned a non-nil key alongside an error", tc.name)
			}
		})
	}
}

// Sign + Verify must work end-to-end after a public key round-trip through PEM.
// This validates that serialized keys are functionally equivalent to originals.
func TestSignVerifyAfterKeySerializationRoundTrip(t *testing.T) {
	t.Parallel()

	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	// Serialize and restore the public key.
	pemStr, err := SerializePublicKey(pub)
	if err != nil {
		t.Fatalf("SerializePublicKey: %v", err)
	}
	restoredPub, err := DeserializePublicKey(pemStr)
	if err != nil {
		t.Fatalf("DeserializePublicKey: %v", err)
	}

	data := []byte("inter-cluster-request for global transition")
	sig, err := Sign(priv, data)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// Verify with the *restored* public key (simulates a peer receiving the serialized key).
	if !Verify(restoredPub, data, sig) {
		t.Fatal("Verify failed after public key PEM round-trip — serialization broke the key")
	}
}