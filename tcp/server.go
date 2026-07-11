// Package network provides the TCP transport layer for the vehicular BFT protocol.
//
// Design decisions:
//   - Wire format: one JSON-encoded Envelope per line (\n delimited).
//     This keeps framing simple and human-readable for debugging.
//   - Server is non-blocking: Start() spawns a goroutine; the caller reads
//     from MsgChan.
//   - Each accepted TCP connection is handled in its own goroutine so slow
//     readers do not stall other senders.
//   - Stop() closes the listener and closes MsgChan so range-loops on the
//     channel terminate naturally.
//   - The server recovers from individual connection panics but lets fatal
//     errors (like the listener itself failing) propagate via MsgChan close.
package network

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/adk2004/vehicular-bft/messages"
)

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

// Server listens on a TCP port and delivers decoded Envelopes to MsgChan.
//
// Lifecycle:
//
//	s := network.NewServer(9001)
//	if err := s.Start(); err != nil { ... }
//	defer s.Stop()
//	for env := range s.MsgChan { ... }
type Server struct {
	// Port is the TCP port this server listens on.
	Port int

	// MsgChan receives every valid Envelope delivered to this server.
	// The channel is closed when Stop() is called.
	MsgChan chan messages.Envelope

	// listener is the underlying TCP listener. Nil until Start() is called.
	listener net.Listener

	// quit is closed by Stop() to signal the accept loop to exit.
	quit chan struct{}

	// wg tracks all connection-handler goroutines so Stop() can wait for them.
	wg sync.WaitGroup

	// mu guards listener and the "started" state.
	mu sync.Mutex

	// started prevents double-Start.
	started bool
}

// NewServer creates a Server that will listen on port. It does NOT start
// listening until Start() is called.
func NewServer(port int) *Server {
	return &Server{
		Port:    port,
		MsgChan: make(chan messages.Envelope, 256), // buffered to avoid sender blocks
		quit:    make(chan struct{}),
	}
}

// Start begins accepting TCP connections on s.Port. It is non-blocking:
// the accept loop runs in a background goroutine.
//
// Returns an error if the port cannot be bound (e.g. already in use).
// Calling Start() more than once returns an error without re-binding.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("network.Server: already started on port %d", s.Port)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port))
	if err != nil {
		return fmt.Errorf("network.Server.Start: listen on port %d: %w", s.Port, err)
	}

	s.listener = ln
	s.started = true

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop shuts down the listener, signals all handler goroutines to exit, and
// closes MsgChan. After Stop() returns, no more messages will be delivered.
// Calling Stop() more than once is safe (idempotent).
func (s *Server) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	close(s.quit)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Unlock()

	// Wait for the accept loop and all connection handlers to finish.
	s.wg.Wait()
	close(s.MsgChan)
}

// Addr returns the server's bound address (e.g. "127.0.0.1:9001").
// Returns an empty string if the server is not started.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal: accept loop and connection handler
// ─────────────────────────────────────────────────────────────────────────────

// acceptLoop runs in a goroutine started by Start().
// It accepts incoming connections until the quit channel is closed or the
// listener returns an error (which happens when Stop() closes the listener).
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check whether we were stopped intentionally.
			select {
			case <-s.quit:
				return // normal shutdown
			default:
				// Unexpected listener error — exit the loop.
				return
			}
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// handleConn reads newline-delimited JSON Envelopes from conn and forwards
// valid ones to MsgChan. It exits when:
//   - The connection is closed by the remote end (EOF).
//   - The quit channel is closed (Stop() was called).
//   - An unrecoverable read error occurs.
func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close() //nolint:errcheck

	scanner := bufio.NewScanner(conn)
	// Increase the scanner buffer to 1 MB to handle large RSA-signed envelopes.
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		// Check for shutdown before processing.
		select {
		case <-s.quit:
			return
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var env messages.Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			// Malformed JSON — skip this message, keep the connection alive.
			continue
		}

		// Non-blocking send to MsgChan; drop if the buffer is full to prevent
		// a misbehaving sender from blocking the accept loop.
		select {
		case s.MsgChan <- env:
		case <-s.quit:
			return
		default:
			// Channel full — drop the message (caller can increase buffer size).
		}
	}
}