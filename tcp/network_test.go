package network

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adk2004/vehicular-bft/crypto"
	"github.com/adk2004/vehicular-bft/messages"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// freePort asks the OS for an available TCP port by binding to :0.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// startServer creates, starts, and registers a cleanup for a Server.
func startServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(freePort(t))
	if err := s.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	t.Cleanup(s.Stop)
	return s
}

// makeTestEnvelope builds a signed Commit envelope for use in tests.
func makeTestEnvelope(t *testing.T, seqID int) messages.Envelope {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	commit := messages.Commit{
		ViewNumber: 1,
		SequenceID: seqID,
		Digest:     fmt.Sprintf("digest-%d", seqID),
		NodeID:     fmt.Sprintf("node-0-%d", seqID),
	}
	env, err := messages.NewEnvelope(messages.MsgCommit, commit.NodeID, commit, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return env
}

// receiveWithTimeout waits up to timeout for one message on ch.
func receiveWithTimeout(ch <-chan messages.Envelope, timeout time.Duration) (messages.Envelope, bool) {
	select {
	case env, ok := <-ch:
		return env, ok
	case <-time.After(timeout):
		return messages.Envelope{}, false
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Start + Send delivers one message to MsgChan within 1 second
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkSendDelivers(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	env := makeTestEnvelope(t, 1)

	if err := Send(s.Addr(), env); err != nil {
		t.Fatalf("Send: %v", err)
	}

	received, ok := receiveWithTimeout(s.MsgChan, time.Second)
	if !ok {
		t.Fatal("timed out waiting for message on MsgChan (1 second)")
	}
	if received.Type != messages.MsgCommit {
		t.Errorf("received Type = %q, want %q", received.Type, messages.MsgCommit)
	}
	if received.SenderID != env.SenderID {
		t.Errorf("received SenderID = %q, want %q", received.SenderID, env.SenderID)
	}
}

// Multiple sequential sends to the same server all arrive.
func TestNetworkMultipleSendsAllDeliver(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	const count = 5

	for i := 0; i < count; i++ {
		env := makeTestEnvelope(t, i)
		if err := Send(s.Addr(), env); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}

	for i := 0; i < count; i++ {
		_, ok := receiveWithTimeout(s.MsgChan, time.Second)
		if !ok {
			t.Fatalf("timed out waiting for message %d/%d", i+1, count)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Broadcast to 3 servers delivers to all 3
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkBroadcastToThreeServers(t *testing.T) {
	t.Parallel()

	const serverCount = 3
	servers := make([]*Server, serverCount)
	addrs := make([]string, serverCount)

	for i := 0; i < serverCount; i++ {
		servers[i] = startServer(t)
		addrs[i] = servers[i].Addr()
	}

	env := makeTestEnvelope(t, 42)
	errs := Broadcast(addrs, env)

	// All sends must succeed.
	for i, err := range errs {
		if err != nil {
			t.Errorf("Broadcast to server %d (%s): %v", i, addrs[i], err)
		}
	}

	// All 3 servers must receive the message.
	var wg sync.WaitGroup
	for i, srv := range servers {
		wg.Add(1)
		go func(idx int, s *Server) {
			defer wg.Done()
			received, ok := receiveWithTimeout(s.MsgChan, time.Second)
			if !ok {
				t.Errorf("server %d: timed out — message not delivered", idx)
				return
			}
			if received.SenderID != env.SenderID {
				t.Errorf("server %d: SenderID = %q, want %q", idx, received.SenderID, env.SenderID)
			}
		}(i, srv)
	}
	wg.Wait()
}

// Broadcast returns one error per address in the same order.
func TestNetworkBroadcastErrorSliceSameLength(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	addrs := []string{s.Addr(), "127.0.0.1:1"} // second addr is unreachable

	env := makeTestEnvelope(t, 7)
	errs := Broadcast(addrs, env)

	if len(errs) != len(addrs) {
		t.Errorf("Broadcast returned %d errors for %d addresses", len(errs), len(addrs))
	}
	// First send should succeed.
	if errs[0] != nil {
		t.Errorf("Broadcast[0] to valid server: unexpected error: %v", errs[0])
	}
	// Second send to unreachable port should fail.
	if errs[1] == nil {
		t.Error("Broadcast[1] to unreachable port: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Stop shuts down the server cleanly (no goroutine leak)
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkStopClosesChannelAndPreventsNewReceives(t *testing.T) {
	t.Parallel()

	s := NewServer(freePort(t))
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send one message to confirm the server is working.
	env := makeTestEnvelope(t, 99)
	if err := Send(s.Addr(), env); err != nil {
		t.Fatalf("Send before Stop: %v", err)
	}
	_, ok := receiveWithTimeout(s.MsgChan, time.Second)
	if !ok {
		t.Fatal("message before Stop not received")
	}

	// Stop the server.
	s.Stop()

	// MsgChan must be closed — range-loop or receive returns immediately.
	select {
	case _, chanOpen := <-s.MsgChan:
		if chanOpen {
			t.Error("MsgChan still open after Stop()")
		}
		// channel closed — correct
	case <-time.After(time.Second):
		t.Error("MsgChan was not closed within 1 second of Stop()")
	}
}

// Stop is idempotent — calling it twice must not panic.
func TestNetworkStopIdempotent(t *testing.T) {
	t.Parallel()

	s := NewServer(freePort(t))
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should not panic on double-stop.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Stop() panicked: %v", r)
		}
	}()
	s.Stop()
	s.Stop() // second call — must be safe
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Sending to a closed port returns an error, not a panic
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkSendToClosedPortReturnsError(t *testing.T) {
	t.Parallel()

	// Use a port that is definitely not listening.
	// Port 1 is reserved and unreachable on Linux without root.
	err := Send("127.0.0.1:1", makeTestEnvelope(t, 0))
	if err == nil {
		t.Error("Send to closed port: expected error, got nil")
	}
}

// Send to an invalid address (bad format) also returns an error.
func TestNetworkSendBadAddressReturnsError(t *testing.T) {
	t.Parallel()

	err := Send("not-a-valid-addr", makeTestEnvelope(t, 0))
	if err == nil {
		t.Error("Send to invalid address: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: Multicast is identical to Broadcast
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkMulticastDeliversToAllLeaders(t *testing.T) {
	t.Parallel()

	// Simulate 3 cluster leaders.
	leaders := make([]*Server, 3)
	addrs := make([]string, 3)
	for i := range leaders {
		leaders[i] = startServer(t)
		addrs[i] = leaders[i].Addr()
	}

	icr := messages.InterClusterRequest{
		Operation:     "UPDATE route=A1",
		Timestamp:     time.Now().UnixNano(),
		ClientID:      "client-0",
		Transition:    messages.GLOBAL,
		OriginCluster: 0,
	}
	priv, _, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	env, err := messages.NewEnvelope(messages.MsgInterClusterRequest, "node-0-0", icr, priv)
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	errs := Multicast(addrs, env)
	for i, e := range errs {
		if e != nil {
			t.Errorf("Multicast to leader %d: %v", i, e)
		}
	}

	// Every leader must receive the InterClusterRequest.
	for i, l := range leaders {
		received, ok := receiveWithTimeout(l.MsgChan, time.Second)
		if !ok {
			t.Errorf("leader %d: multicast message not received within 1s", i)
			continue
		}
		if received.Type != messages.MsgInterClusterRequest {
			t.Errorf("leader %d: Type = %q, want MsgInterClusterRequest", i, received.Type)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BroadcastAsync sends fire-and-forget and errors arrive on the channel
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkBroadcastAsync(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	// One valid, one invalid.
	addrs := []string{s.Addr(), "127.0.0.1:1"}

	env := makeTestEnvelope(t, 55)
	errCh := BroadcastAsync(addrs, env)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	// Exactly one failure (port 1).
	if len(errs) != 1 {
		t.Errorf("BroadcastAsync: expected 1 error, got %d", len(errs))
	}

	// Valid server still receives the message.
	_, ok := receiveWithTimeout(s.MsgChan, time.Second)
	if !ok {
		t.Error("BroadcastAsync: valid server did not receive the message")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SendWithRetry succeeds on the second attempt
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkSendWithRetrySucceedsWhenServerStartsLate(t *testing.T) {
	t.Parallel()

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	env := makeTestEnvelope(t, 77)

	// Use a channel to pass the server back safely (no shared variable race).
	serverCh := make(chan *Server, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		srv := NewServer(port)
		if err := srv.Start(); err != nil {
			serverCh <- nil
			return
		}
		serverCh <- srv
	}()

	// SendWithRetry should eventually succeed once the server starts.
	err := SendWithRetry(addr, env, 10, 50*time.Millisecond)
	if err != nil {
		t.Errorf("SendWithRetry: expected success, got: %v", err)
	}

	// Clean up the server (receive from channel to synchronise).
	if srv := <-serverCh; srv != nil {
		srv.Stop()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Server rejects double-Start
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkServerDoubleStartReturnsError(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	err := s.Start() // second Start — should return error
	if err == nil {
		t.Error("Server.Start() called twice: expected error, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent sends from multiple goroutines all arrive
// ─────────────────────────────────────────────────────────────────────────────

func TestNetworkConcurrentSends(t *testing.T) {
	t.Parallel()

	s := startServer(t)
	const senders = 20

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := makeTestEnvelope(t, idx)
			if err := Send(s.Addr(), env); err != nil {
				t.Errorf("concurrent Send[%d]: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	// All senders messages must arrive (within 3 seconds).
	received := 0
	deadline := time.After(3 * time.Second)
	for received < senders {
		select {
		case <-s.MsgChan:
			received++
		case <-deadline:
			t.Errorf("timed out: received %d/%d messages", received, senders)
			return
		}
	}
}