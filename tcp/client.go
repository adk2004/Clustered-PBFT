// client.go provides functions for sending Envelopes to one or many peers
// over TCP. All functions are safe to call from multiple goroutines.
//
// Wire format: JSON-encoded Envelope followed by a newline character ('\n').
// This matches the line-delimited framing that Server.handleConn expects.
//
// Connection policy: a new TCP connection is opened per Send call and closed
// immediately after the message is written. This keeps the implementation
// stateless and avoids connection-management complexity for Phase 7.
// A persistent connection pool can be layered on top in a later phase without
// changing the message format.
package network

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/adk2004/vehicular-bft/messages"
)

const (
	// dialTimeout is the maximum time allowed to establish a TCP connection.
	dialTimeout = 3 * time.Second

	// writeTimeout is the maximum time allowed to write a single message.
	writeTimeout = 5 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// Send
// ─────────────────────────────────────────────────────────────────────────────

// Send opens a TCP connection to addr, writes one JSON-encoded Envelope
// followed by '\n', then closes the connection.
//
// addr format: "host:port"  (e.g. "127.0.0.1:9001")
//
// Returns an error if the connection cannot be established, the marshal fails,
// or the write times out. The caller may retry or consider the peer faulty.
func Send(addr string, env messages.Envelope) error {
	// Establish connection with timeout.
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return fmt.Errorf("network.Send: dial %s: %w", addr, err)
	}
	defer conn.Close() //nolint:errcheck

	// Encode the envelope.
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("network.Send: marshal envelope: %w", err)
	}

	// Append newline delimiter so the server's bufio.Scanner can frame it.
	data = append(data, '\n')

	// Apply write deadline.
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("network.Send: set write deadline: %w", err)
	}

	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("network.Send: write to %s: %w", addr, err)
	}

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Broadcast
// ─────────────────────────────────────────────────────────────────────────────

// Broadcast sends the same Envelope to every address in addrs concurrently.
//
// One goroutine is spawned per address. Returns a slice of errors in the same
// order as addrs; nil entries indicate success. The function blocks until all
// goroutines complete.
//
// Example:
//
//	errs := network.Broadcast([]string{"127.0.0.1:9001", "127.0.0.1:9002"}, env)
//	for i, err := range errs {
//	    if err != nil { log.Printf("send to %s failed: %v", addrs[i], err) }
//	}
func Broadcast(addrs []string, env messages.Envelope) []error {
	errs := make([]error, len(addrs))
	var wg sync.WaitGroup

	for i, addr := range addrs {
		wg.Add(1)
		go func(idx int, a string) {
			defer wg.Done()
			errs[idx] = Send(a, env)
		}(i, addr)
	}

	wg.Wait()
	return errs
}

// ─────────────────────────────────────────────────────────────────────────────
// Multicast
// ─────────────────────────────────────────────────────────────────────────────

// Multicast is a semantic alias for Broadcast used specifically for the
// InterClusterRequest phase (paper Section VII), where the initiating cluster
// leader multicasts to all other cluster leaders.
//
// Behaviour is identical to Broadcast; the separate name improves readability
// at the protocol call site in Phase 8.
func Multicast(addrs []string, env messages.Envelope) []error {
	return Broadcast(addrs, env)
}

// ─────────────────────────────────────────────────────────────────────────────
// BroadcastAsync
// ─────────────────────────────────────────────────────────────────────────────

// BroadcastAsync sends the Envelope to all addrs concurrently and returns
// immediately without waiting. Errors are sent on the returned channel,
// which is closed once all sends complete.
//
// Useful when the caller does not need to block on delivery (e.g. fire-and-
// forget Vote broadcasts during the voting phase).
func BroadcastAsync(addrs []string, env messages.Envelope) <-chan error {
	errCh := make(chan error, len(addrs))
	var wg sync.WaitGroup

	for _, addr := range addrs {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			if err := Send(a, env); err != nil {
				errCh <- err
			}
		}(addr)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	return errCh
}

// ─────────────────────────────────────────────────────────────────────────────
// SendWithRetry
// ─────────────────────────────────────────────────────────────────────────────

// SendWithRetry attempts Send up to maxRetries times with an exponential
// back-off starting at baseDelay. Useful for transient network failures during
// testing with rapidly-starting servers.
//
// Returns nil as soon as one attempt succeeds; returns the last error if all
// attempts fail.
func SendWithRetry(addr string, env messages.Envelope, maxRetries int, baseDelay time.Duration) error {
	var lastErr error
	delay := baseDelay
	for i := 0; i < maxRetries; i++ {
		if err := Send(addr, env); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(delay)
		delay *= 2 // exponential back-off
	}
	return fmt.Errorf("network.SendWithRetry: all %d attempts failed, last error: %w", maxRetries, lastErr)
}