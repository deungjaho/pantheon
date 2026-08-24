package push

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// newTestServer starts a push server on a temp Unix socket and returns it
// along with a cancel func to shut it down. The socket path is kept short
// to stay within the macOS Unix socket path length limit (~104 chars).
func newTestServer(t *testing.T) (*Server, context.CancelFunc) {
	t.Helper()
	socketPath := fmt.Sprintf("/tmp/pantheon-push-%d.sock", time.Now().UnixNano())
	srv := NewServer(socketPath, nil)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("push server start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	return srv, cancel
}

// subReader wraps a push subscriber connection with a background goroutine
// that reads notification lines and forwards them to a channel. This avoids
// the bufio.Scanner limitation where a read-deadline timeout permanently
// kills the scanner.
type subReader struct {
	conn net.Conn
	ch   chan Notification
	done chan struct{}
}

// dialSubscriber connects to the push server, sends a subscription request,
// and returns a subReader whose Recv method reads notifications with a
// bounded wait.
func dialSubscriber(t *testing.T, socketPath string, runIDs []string) *subReader {
	t.Helper()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial push socket: %v", err)
	}
	req, err := json.Marshal(SubscriptionRequest{RunIDs: runIDs})
	if err != nil {
		conn.Close()
		t.Fatalf("marshal subscription: %v", err)
	}
	if _, err := conn.Write(append(req, '\n')); err != nil {
		conn.Close()
		t.Fatalf("write subscription: %v", err)
	}
	sr := &subReader{
		conn: conn,
		ch:   make(chan Notification, 16),
		done: make(chan struct{}),
	}
	go sr.readLoop()
	return sr
}

// readLoop reads notification lines until the connection closes.
func (sr *subReader) readLoop() {
	defer close(sr.done)
	scanner := bufio.NewScanner(sr.conn)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	for scanner.Scan() {
		var n Notification
		if err := json.Unmarshal(scanner.Bytes(), &n); err != nil {
			continue
		}
		select {
		case sr.ch <- n:
		case <-sr.done:
			return
		}
	}
}

// Recv reads one notification with a timeout. Returns the notification and
// whether one was received before the deadline.
func (sr *subReader) Recv(timeout time.Duration) (Notification, bool) {
	select {
	case n := <-sr.ch:
		return n, true
	case <-time.After(timeout):
		return Notification{}, false
	}
}

// Close closes the subscriber connection.
func (sr *subReader) Close() {
	sr.conn.Close()
	<-sr.done
}

// assertNoNotification verifies that no notification arrives within the
// given window.
func assertNoNotification(t *testing.T, sr *subReader, window time.Duration) {
	t.Helper()
	if _, ok := sr.Recv(window); ok {
		t.Fatalf("received unexpected notification")
	}
}

// TestPushServerNotify starts a server, connects a subscriber, publishes a
// message, and verifies the notification is received with the correct
// run_id, message_seq, and event_id.
func TestPushServerNotify(t *testing.T) {
	srv, _ := newTestServer(t)
	sr := dialSubscriber(t, srv.SocketPath(), []string{"run_a"})
	defer sr.Close()

	// Allow the server to register the subscriber before notifying.
	time.Sleep(50 * time.Millisecond)

	srv.NotifyMessage("run_a", 1, "evt_abc")

	n, ok := sr.Recv(2 * time.Second)
	if !ok {
		t.Fatal("did not receive notification")
	}
	if n.Type != "message" {
		t.Errorf("type = %q, want %q", n.Type, "message")
	}
	if n.RunID != "run_a" {
		t.Errorf("run_id = %q, want run_a", n.RunID)
	}
	if n.MessageSeq != 1 {
		t.Errorf("message_seq = %d, want 1", n.MessageSeq)
	}
	if n.EventID != "evt_abc" {
		t.Errorf("event_id = %q, want evt_abc", n.EventID)
	}
	if n.Timestamp == "" {
		t.Error("timestamp is empty")
	}
}

// TestPushServerMultipleSubscribers connects two subscribers for the same
// run and verifies both receive the notification.
func TestPushServerMultipleSubscribers(t *testing.T) {
	srv, _ := newTestServer(t)
	sr1 := dialSubscriber(t, srv.SocketPath(), []string{"run_x"})
	defer sr1.Close()
	sr2 := dialSubscriber(t, srv.SocketPath(), []string{"run_x"})
	defer sr2.Close()

	time.Sleep(50 * time.Millisecond)

	srv.NotifyMessage("run_x", 7, "evt_multi")

	for i, sr := range []*subReader{sr1, sr2} {
		n, ok := sr.Recv(2 * time.Second)
		if !ok {
			t.Fatalf("subscriber %d: did not receive notification", i+1)
		}
		if n.RunID != "run_x" || n.MessageSeq != 7 {
			t.Errorf("subscriber %d: notification = %+v, want run_x/7", i+1, n)
		}
	}
}

// TestPushServerFilterByRunID connects a subscriber for run A, publishes to
// run B, and verifies no notification is delivered to the run-A subscriber.
func TestPushServerFilterByRunID(t *testing.T) {
	srv, _ := newTestServer(t)
	sr := dialSubscriber(t, srv.SocketPath(), []string{"run_a"})
	defer sr.Close()

	time.Sleep(50 * time.Millisecond)

	// Publish to a different run — the subscriber should not receive it.
	srv.NotifyMessage("run_b", 1, "evt_b1")
	assertNoNotification(t, sr, 200*time.Millisecond)

	// Publish to the subscribed run — now it should arrive.
	srv.NotifyMessage("run_a", 1, "evt_a1")
	n, ok := sr.Recv(2 * time.Second)
	if !ok {
		t.Fatal("did not receive notification for run_a")
	}
	if n.RunID != "run_a" {
		t.Errorf("run_id = %q, want run_a", n.RunID)
	}
}

// TestPushServerReconnect verifies the cursor-fallback recovery pattern:
// a subscriber receives a notification, disconnects (missing a subsequent
// publish), reconnects, and receives the next notification. The message_seq
// in each notification lets the client detect the gap and recover via the
// pull-based messages.by_run RPC.
func TestPushServerReconnect(t *testing.T) {
	srv, _ := newTestServer(t)

	// First session: receive seq=1.
	sr1 := dialSubscriber(t, srv.SocketPath(), []string{"run_r"})
	time.Sleep(50 * time.Millisecond)
	srv.NotifyMessage("run_r", 1, "evt_r1")
	n1, ok := sr1.Recv(2 * time.Second)
	if !ok {
		sr1.Close()
		t.Fatal("first session: did not receive notification")
	}
	if n1.MessageSeq != 1 {
		t.Fatalf("first session: message_seq = %d, want 1", n1.MessageSeq)
	}
	sr1.Close()

	// While disconnected, a message is published (seq=2). The subscriber
	// misses it — this is the gap the cursor fallback must recover.
	time.Sleep(50 * time.Millisecond)
	srv.NotifyMessage("run_r", 2, "evt_r2")
	time.Sleep(50 * time.Millisecond)

	// Reconnect. The client would first pull messages.by_run with
	// cursor=1 to recover seq=2, then subscribe for new pushes.
	sr2 := dialSubscriber(t, srv.SocketPath(), []string{"run_r"})
	defer sr2.Close()
	time.Sleep(50 * time.Millisecond)

	// Publish seq=3 — the reconnected subscriber should receive it.
	srv.NotifyMessage("run_r", 3, "evt_r3")
	n3, ok := sr2.Recv(2 * time.Second)
	if !ok {
		t.Fatal("second session: did not receive notification")
	}
	if n3.MessageSeq != 3 {
		t.Errorf("second session: message_seq = %d, want 3", n3.MessageSeq)
	}
	// The client observes seq jumped from 1 to 3, indicating a gap (seq=2)
	// that must be recovered via the pull RPC with cursor=1.
	if n3.MessageSeq-n1.MessageSeq != 2 {
		t.Errorf("expected gap of 2 (seq 1 -> 3), got %d -> %d", n1.MessageSeq, n3.MessageSeq)
	}
}

// TestPushServerSubscribeAll verifies that a subscriber with an empty
// run_ids filter receives notifications for any run.
func TestPushServerSubscribeAll(t *testing.T) {
	srv, _ := newTestServer(t)
	sr := dialSubscriber(t, srv.SocketPath(), nil) // empty = all
	defer sr.Close()

	time.Sleep(50 * time.Millisecond)

	srv.NotifyMessage("run_any", 1, "evt_any")
	n, ok := sr.Recv(2 * time.Second)
	if !ok {
		t.Fatal("did not receive notification")
	}
	if n.RunID != "run_any" {
		t.Errorf("run_id = %q, want run_any", n.RunID)
	}
}

// TestPushServerNoopPusher verifies the NoopPusher does nothing and is safe
// to use as the default when push is disabled.
func TestPushServerNoopPusher(t *testing.T) {
	var p Pusher = &NoopPusher{}
	// Must not panic and must not block.
	p.NotifyMessage("run_x", 1, "evt_x")
}

// TestPushServerClose verifies that Close shuts down the server and removes
// the socket file.
func TestPushServerClose(t *testing.T) {
	socketPath := fmt.Sprintf("/tmp/pantheon-push-close-%d.sock", time.Now().UnixNano())
	srv := NewServer(socketPath, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv.Close()

	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Errorf("socket file still exists after Close")
	}
}
