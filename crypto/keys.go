// Package crypto provides all cryptographic primitives used by the vehicular BFT protocol.
//
// Every node in the network is assigned an RSA-2048 key pair issued by a central
// authority (NodeCA). All protocol messages are signed by their sender and verified
// by receivers before processing (paper Section IV-A-2, pitfall #1 in the plan).
//
// Design decisions:
//   - Signing: RSA-PKCS1v15 with SHA-256. Deterministic and widely supported.
//   - Digest: SHA-256 over the JSON encoding of the message body (not the envelope),
//     avoiding a circular dependency when computing the 'd' field (pitfall #2).
//   - PEM: standard x509/PKIX encoding for public key exchange and storage.
package crypto

import (
	gocrypto "crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/adk2004/vehicular-bft/config"
)

// GenerateKeyPair generates an RSA key pair of size config.RSABits (2048) for a node.
// The private key is kept by the node; the public key is shared with all peers
// through the NodeCA infrastructure (paper Section IV-A-2).
//
// Returns (privateKey, publicKey, error).
func GenerateKeyPair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, config.RSABits)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto.GenerateKeyPair: failed to generate %d-bit RSA key: %w",
			config.RSABits, err)
	}
	return privKey, &privKey.PublicKey, nil
}

// Sign produces an RSA-PKCS1v15 signature over the SHA-256 hash of data.
// The result is base64 (standard encoding) so it can be embedded safely in JSON.
//
// Every protocol message envelope carries a Signature field produced by this function
// (plan Section V messages, pitfall #1).
func Sign(privKey *rsa.PrivateKey, data []byte) (string, error) {
	if privKey == nil {
		return "", errors.New("crypto.Sign: private key is nil")
	}
	hash := sha256.Sum256(data)
	sigBytes, err := rsa.SignPKCS1v15(rand.Reader, privKey, gocrypto.SHA256, hash[:])
	if err != nil {
		return "", fmt.Errorf("crypto.Sign: RSA signing failed: %w", err)
	}
	return base64.StdEncoding.EncodeToString(sigBytes), nil
}

// Verify checks that sig (base64-encoded RSA-PKCS1v15 signature) was produced by
// the holder of pubKey over data. Returns false on any error (bad base64, wrong key,
// tampered data) so callers can treat the boolean as a safe gate.
func Verify(pubKey *rsa.PublicKey, data []byte, sig string) bool {
	if pubKey == nil || sig == "" {
		return false
	}
	sigBytes, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		return false
	}
	hash := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pubKey, gocrypto.SHA256, hash[:], sigBytes)
	return err == nil
}

// Digest returns the SHA-256 hex digest (64-char lowercase string) of v after
// JSON marshalling. This is used as the 'd' field in all protocol messages
// (Vote, PrePrepare, Prepare, Commit — paper Section VII).
//
// IMPORTANT: always call Digest on the inner message body struct, NOT the
// outer Envelope. Computing the digest after adding the signature field would
// create a circular dependency (pitfall #2 in the implementation plan).
func Digest(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("crypto.Digest: JSON marshal failed: %w", err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// SerializePublicKey encodes pub as a PEM block with type "PUBLIC KEY" using the
// standard PKIX/DER format (x509.MarshalPKIXPublicKey). The returned string is
// safe for storage, logging, and JSON embedding.
//
// Used to distribute node public keys through the NodeCA (paper Section IV-A-2).
func SerializePublicKey(pub *rsa.PublicKey) (string, error) {
	if pub == nil {
		return "", errors.New("crypto.SerializePublicKey: public key is nil")
	}
	derBytes, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("crypto.SerializePublicKey: DER marshal failed: %w", err)
	}
	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derBytes,
	}
	return string(pem.EncodeToMemory(block)), nil
}

// DeserializePublicKey parses a PEM-encoded RSA public key produced by SerializePublicKey.
// Returns an error if the PEM block is missing, malformed, or is not an RSA key.
func DeserializePublicKey(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("crypto.DeserializePublicKey: no valid PEM block found")
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("crypto.DeserializePublicKey: unexpected PEM type %q, want \"PUBLIC KEY\"",
			block.Type)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("crypto.DeserializePublicKey: PKIX parse failed: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("crypto.DeserializePublicKey: parsed key is %T, not *rsa.PublicKey", pub)
	}
	return rsaPub, nil
}