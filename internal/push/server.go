package push

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Notification is the JSON object pushed to subscribers when a new message
// is published. It carries the message_seq cursor so subscribers can detect
// gaps after a disconnect and recover via the pull-based messages.by_run
// RPC.
type Notification struct {
	Type       string `json:"type"` // always "message"
	RunID      string `json:"run_id"`
	MessageSeq int64  `json:"message_seq"`
	EventID    string `json:"event_id"`
	Timestamp  string `json:"timestamp"`
}

// SubscriptionRequest is sent by a subscriber immediately after connecting.
// An empty RunIDs slice means "subscribe to all run_ids".
type SubscriptionRequest struct {
	RunIDs []string `json:"run_ids"`
}

// Subscriber is one connected push client. Each subscriber has a bounded
// buffered channel so that a slow subscriber does not block the publisher;
// notifications are dropped (with a log line) when the buffer is full.
type Subscriber struct {
	conn      net.Conn
	runIDs    map[string]bool // empty = all run_ids
	encoder   *json.Encoder
	ch        chan Notification
	done      chan struct{} // closed when the write goroutine exits
	closeOnce sync.Once
}

// matches reports whether the subscriber wants notifications for runID.
// An empty runIDs map means "all runs".
func (s *Subscriber) matches(runID string) bool {
	if len(s.runIDs) == 0 {
		return true
	}
	return s.runIDs[runID]
}

// Server is the push server. It listens on a Unix socket (separate from the
// main RPC socket), accepts subscriber connections, and pushes
// message-published notifications to matching subscribers.
//
// The server implements the Pusher interface via NotifyMessage. It is
// optional: when not started, the service uses a NoopPusher and the system
// falls back to pull-based cursor polling.
type Server struct {
	socketPath string
	mu         sync.RWMutex
	subs       map[string][]*Subscriber // run_id -> subscribers
	listener   net.Listener
	logger     *log.Logger

	// allSubs tracks subscribers with an empty runIDs filter (subscribe to
	// all runs). Kept separate so NotifyMessage can fan out without
	// iterating every run_id key.
	allSubs []*Subscriber

	closeOnce sync.Once
	closed    chan struct{}
}

// NewServer creates a push server that will listen on socketPath when
// Start is called. The socket path must be separate from the main RPC
// socket (e.g. pantheond-push.sock).
func NewServer(socketPath string, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(os.Stderr, "push: ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Server{
		socketPath: socketPath,
		subs:       make(map[string][]*Subscriber),
		logger:     logger,
		closed:     make(chan struct{}),
	}
}

// Start binds the Unix socket and runs the accept loop until ctx is
// cancelled. It returns immediately with an error if the socket cannot be
// created. The caller is expected to cancel ctx (or call Close) to shut
// down the server.
func (s *Server) Start(ctx context.Context) error {
	// Remove stale socket file if it exists.
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("push: remove stale socket %q: %w", s.socketPath, err)
	}
	// Ensure parent directory exists.
	if dir := filepath.Dir(s.socketPath); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("push: create socket dir: %w", err)
		}
	}
	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("push: listen %q: %w", s.socketPath, err)
	}
	s.listener = ln
	s.logger.Printf("push: serving on Unix socket %s", s.socketPath)

	// Accept loop. Closes the listener when ctx is cancelled.
	go func() {
		<-ctx.Done()
		s.Close()
	}()

	go s.acceptLoop()
	return nil
}

// acceptLoop accepts subscriber connections. Each connection is handled in
// its own goroutine: read the subscription request, register the
// subscriber, then spawn a write goroutine that drains its notification
// channel.
func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closed:
				// Shutdown in progress.
				return
			default:
			}
			s.logger.Printf("push: accept: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// handleConn reads the subscription request, registers the subscriber, and
// runs the write loop. When the connection ends (EOF, error, or shutdown)
// the subscriber is unregistered and the connection closed.
func (s *Server) handleConn(conn net.Conn) {
	// Read the subscription request (one JSON line).
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 4096), 64*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			s.logger.Printf("push: read subscription: %v", err)
		}
		conn.Close()
		return
	}
	var req SubscriptionRequest
	if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
		s.logger.Printf("push: parse subscription: %v", err)
		conn.Close()
		return
	}

	runIDs := make(map[string]bool, len(req.RunIDs))
	for _, r := range req.RunIDs {
		if r != "" {
			runIDs[r] = true
		}
	}

	sub := &Subscriber{
		conn:    conn,
		runIDs:  runIDs,
		encoder: json.NewEncoder(conn),
		ch:      make(chan Notification, notifyBufferSize),
		done:    make(chan struct{}),
	}
	s.register(sub)
	defer s.unregister(sub)

	// Write loop: drain the notification channel and write each
	// notification as a JSON line. Exits when Close is called (channel
	// closed) or a write fails.
	for n := range sub.ch {
		if err := sub.encoder.Encode(&n); err != nil {
			s.logger.Printf("push: write notification: %v", err)
			return
		}
	}
}

// notifyBufferSize is the per-subscriber notification buffer. When full,
// new notifications are dropped (best-effort push — subscribers recover via
// the pull RPC cursor).
const notifyBufferSize = 64

// register adds a subscriber to the subscriber map under a write lock.
func (s *Server) register(sub *Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(sub.runIDs) == 0 {
		s.allSubs = append(s.allSubs, sub)
		return
	}
	for r := range sub.runIDs {
		s.subs[r] = append(s.subs[r], sub)
	}
}

// unregister removes a subscriber from the map and closes its channel so
// the write loop exits.
func (s *Server) unregister(sub *Subscriber) {
	s.mu.Lock()
	if len(sub.runIDs) == 0 {
		s.allSubs = removeSub(s.allSubs, sub)
	} else {
		for r := range sub.runIDs {
			s.subs[r] = removeSub(s.subs[r], sub)
			if len(s.subs[r]) == 0 {
				delete(s.subs, r)
			}
		}
	}
	s.mu.Unlock()
	sub.close()
}

// close closes the subscriber's connection and notification channel once.
func (sub *Subscriber) close() {
	sub.closeOnce.Do(func() {
		sub.conn.Close()
		close(sub.ch)
		close(sub.done)
	})
}

func removeSub(slice []*Subscriber, sub *Subscriber) []*Subscriber {
	for i, v := range slice {
		if v == sub {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// NotifyMessage implements Pusher. It builds a notification and fans it out
// to all matching subscribers. The fan-out is non-blocking: each
// subscriber has a bounded buffer; a full buffer means the notification is
// dropped (the subscriber recovers via the pull RPC cursor).
//
// This method is safe for concurrent use and never blocks the caller on
// subscriber I/O.
func (s *Server) NotifyMessage(runID string, messageSeq int64, eventID string) {
	n := Notification{
		Type:       "message",
		RunID:      runID,
		MessageSeq: messageSeq,
		EventID:    eventID,
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	s.mu.RLock()
	targets := make([]*Subscriber, 0, len(s.allSubs)+len(s.subs[runID]))
	targets = append(targets, s.allSubs...)
	targets = append(targets, s.subs[runID]...)
	s.mu.RUnlock()

	for _, sub := range targets {
		if !sub.matches(runID) {
			continue
		}
		select {
		case sub.ch <- n:
		default:
			// Buffer full — drop. Subscriber recovers via pull cursor.
			s.logger.Printf("push: drop notification for run %s seq %d (subscriber buffer full)", runID, messageSeq)
		}
	}
}

// Close shuts down the push server: closes the listener and all subscriber
// connections. It is safe to call multiple times.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.listener != nil {
			s.listener.Close()
		}
		close(s.closed)
		// Close all subscribers so their write loops exit.
		s.mu.Lock()
		for _, subs := range s.subs {
			for _, sub := range subs {
				sub.close()
			}
		}
		for _, sub := range s.allSubs {
			sub.close()
		}
		s.subs = make(map[string][]*Subscriber)
		s.allSubs = nil
		s.mu.Unlock()
		// Best-effort socket file cleanup.
		_ = os.Remove(s.socketPath)
	})
}

// SocketPath returns the configured Unix socket path.
func (s *Server) SocketPath() string { return s.socketPath }
