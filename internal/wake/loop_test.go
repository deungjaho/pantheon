package wake

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// fakeStore implements EventStore for testing.
type fakeStore struct {
	mu       sync.Mutex
	cursor   int64
	events   []domain.Event
	cursorOK bool
}

func (s *fakeStore) EventsSince(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Event
	for _, e := range s.events {
		if e.Seq > cursor {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *fakeStore) LoadWakeCursor(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, nil
}

func (s *fakeStore) SaveWakeCursor(ctx context.Context, cursor int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = cursor
	s.cursorOK = true
	return nil
}

func (s *fakeStore) LastEventSeq(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max int64
	for _, e := range s.events {
		if e.Seq > max {
			max = e.Seq
		}
	}
	return max, nil
}

func (s *fakeStore) addEvent(seq int64, eventType string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, domain.Event{
		Seq:       seq,
		EventID:   "evt_" + string(rune('a'+int(seq))),
		EventType: eventType,
	})
}

func (s *fakeStore) getCursor() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

func TestEmptyPollZeroCost(t *testing.T) {
	store := &fakeStore{}
	var handlerCalls atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		handlerCalls.Add(1)
		return nil
	}

	loop := New(store, handler, Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for a few poll cycles.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Handler should never be called — no events.
	if handlerCalls.Load() > 0 {
		t.Errorf("handler called %d times on empty store, want 0", handlerCalls.Load())
	}
}

func TestProcessEventAndSaveCursor(t *testing.T) {
	store := &fakeStore{}
	store.addEvent(1, "message")
	store.addEvent(2, "message.ack")

	var processed []int64
	var mu sync.Mutex
	handler := func(ctx context.Context, events []domain.Event) error {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			processed = append(processed, e.Seq)
		}
		return nil
	}

	loop := New(store, handler, Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for at least one poll.
	time.Sleep(200 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 2 {
		t.Fatalf("processed %d events, want 2", len(processed))
	}
	if processed[0] != 1 || processed[1] != 2 {
		t.Errorf("processed seqs = %v, want [1 2]", processed)
	}

	// Cursor should be saved to 2.
	if got := store.getCursor(); got != 2 {
		t.Errorf("cursor = %d, want 2", got)
	}
}

func TestCrashRecoveryFromCursor(t *testing.T) {
	// Simulate: store has cursor=2 (crash happened after processing 2 events).
	// New events 3, 4 arrive. Loop should start from cursor=2, process 3+4.
	store := &fakeStore{cursor: 2}
	store.addEvent(1, "message")
	store.addEvent(2, "message")
	store.addEvent(3, "message")
	store.addEvent(4, "message")

	var processed []int64
	var mu sync.Mutex
	handler := func(ctx context.Context, events []domain.Event) error {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			processed = append(processed, e.Seq)
		}
		return nil
	}

	loop := New(store, handler, Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	// Should only process events 3 and 4 (cursor was 2).
	if len(processed) != 2 {
		t.Fatalf("processed %d events, want 2", len(processed))
	}
	if processed[0] != 3 || processed[1] != 4 {
		t.Errorf("processed seqs = %v, want [3 4]", processed)
	}
	if got := store.getCursor(); got != 4 {
		t.Errorf("cursor = %d, want 4", got)
	}
}

func TestPollOnce(t *testing.T) {
	store := &fakeStore{}
	store.addEvent(1, "message")
	store.addEvent(2, "message")

	var processed atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		processed.Add(int64(len(events)))
		return nil
	}

	loop := New(store, handler, Config{BatchSize: 10})

	n, err := loop.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if n != 2 {
		t.Errorf("PollOnce processed %d, want 2", n)
	}
	if processed.Load() != 2 {
		t.Errorf("handler processed %d, want 2", processed.Load())
	}
	if got := store.getCursor(); got != 2 {
		t.Errorf("cursor = %d, want 2", got)
	}

	// Second poll — no new events.
	n, err = loop.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce 2: %v", err)
	}
	if n != 0 {
		t.Errorf("PollOnce 2 processed %d, want 0", n)
	}
}

func TestInitCursorSkipsExisting(t *testing.T) {
	store := &fakeStore{}
	store.addEvent(1, "message")
	store.addEvent(2, "message")
	store.addEvent(3, "message")

	var processed atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		processed.Add(int64(len(events)))
		return nil
	}

	loop := New(store, handler, Config{BatchSize: 10})

	// Init cursor to 3 — skip all existing events.
	if err := loop.InitCursor(context.Background()); err != nil {
		t.Fatalf("InitCursor: %v", err)
	}

	// PollOnce — no new events.
	n, err := loop.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("PollOnce processed %d after InitCursor, want 0", n)
	}
	if processed.Load() != 0 {
		t.Errorf("handler called after InitCursor, want 0")
	}
}

func TestStartTwiceFails(t *testing.T) {
	store := &fakeStore{}
	handler := func(ctx context.Context, events []domain.Event) error { return nil }

	loop := New(store, handler, Config{PollInterval: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := loop.Start(ctx); err == nil {
		t.Fatal("second Start should fail")
	}
}

func TestBatchSizeLimit(t *testing.T) {
	store := &fakeStore{}
	for i := int64(1); i <= 10; i++ {
		store.addEvent(i, "message")
	}

	var processed atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		processed.Add(int64(len(events)))
		return nil
	}

	loop := New(store, handler, Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	cancel()

	// With batch=3 and 10 events, need 4 polls (3+3+3+1).
	// In 300ms with 50ms interval, should get ~6 polls, enough for all 10.
	if got := processed.Load(); got != 10 {
		t.Errorf("processed %d, want 10", got)
	}
}

func TestHandlerErrorDoesNotAdvanceCursor(t *testing.T) {
	store := &fakeStore{}
	store.addEvent(1, "message")

	callCount := atomic.Int64{}
	handler := func(ctx context.Context, events []domain.Event) error {
		callCount.Add(1)
		return fmt.Errorf("handler error")
	}

	loop := New(store, handler, Config{BatchSize: 10})

	_, err := loop.PollOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from handler failure")
	}

	// Cursor should NOT advance — event 1 is still unprocessed.
	if got := store.getCursor(); got != 0 {
		t.Errorf("cursor = %d, want 0 (handler failed)", got)
	}
}

func TestLoopStopsOnContextCancel(t *testing.T) {
	store := &fakeStore{}
	handler := func(ctx context.Context, events []domain.Event) error { return nil }

	loop := New(store, handler, Config{PollInterval: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())

	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !loop.IsRunning() {
		t.Fatal("loop should be running")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if loop.IsRunning() {
		t.Fatal("loop should stop after context cancel")
	}
}
