// Package wake implements the event-driven wake loop (C-003).
//
// The wake loop reads new events from the SQLite event journal using a
// persisted cursor. It only processes events when new ones exist — empty
// polls consume zero resources and do not invoke any handler. The cursor
// is persisted to the meta table after each batch, enabling crash recovery:
// after a restart, the loop resumes from the last-processed seq.
//
// This implements ADR-0013's target architecture: event-driven wake,
// cursor-based recovery, no tmux pane text dependency.
package wake

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// EventStore is the subset of store.Store required by the wake loop.
// Defined at the consumer side per AGENTS.md convention.
type EventStore interface {
	EventsSince(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error)
	LoadWakeCursor(ctx context.Context) (int64, error)
	SaveWakeCursor(ctx context.Context, cursor int64) error
	LastEventSeq(ctx context.Context) (int64, error)
}

// Handler processes a batch of new events. It is called by the wake loop
// when new events are detected. The handler must be idempotent — the same
// event may be delivered after a crash recovery if the cursor was not
// saved before the crash.
//
// The handler returns the number of events it successfully processed.
// The wake loop saves the cursor up to the last successfully processed event.
type Handler func(ctx context.Context, events []domain.Event) error

// Config controls the wake loop behavior.
type Config struct {
	// PollInterval is how often to check for new events. Default 5s.
	PollInterval time.Duration
	// BatchSize is the max events per poll. Default 100.
	BatchSize int
	// Logger receives diagnostic messages. If nil, uses log.Default().
	Logger *log.Logger
}

// Loop is the event-driven wake loop (C-003).
type Loop struct {
	store   EventStore
	handler Handler
	cfg     Config
	logger  *log.Logger

	mu      sync.Mutex
	running bool
	cursor  int64 // last-processed seq, loaded from store on Start
}

// New creates a wake loop backed by the given store and handler.
func New(store EventStore, handler Handler, cfg Config) *Loop {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Loop{
		store:   store,
		handler: handler,
		cfg:     cfg,
		logger:  logger,
	}
}

// Start begins the wake loop in a background goroutine. The loop runs
// until ctx is cancelled. On start, it loads the persisted cursor from
// the store (crash recovery). Returns an error if the loop is already
// running or if the cursor cannot be loaded.
func (l *Loop) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return fmt.Errorf("wake: already running")
	}
	l.mu.Unlock()

	// Load persisted cursor for crash recovery.
	cursor, err := l.store.LoadWakeCursor(ctx)
	if err != nil {
		return fmt.Errorf("wake: load cursor: %w", err)
	}
	l.mu.Lock()
	l.cursor = cursor
	l.running = true
	l.mu.Unlock()

	l.logger.Printf("wake: starting (cursor=%d, poll=%s, batch=%d)",
		cursor, l.cfg.PollInterval, l.cfg.BatchSize)

	go l.run(ctx)
	return nil
}

// Cursor returns the current cursor (last-processed seq).
func (l *Loop) Cursor() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cursor
}

// IsRunning reports whether the loop is currently running.
func (l *Loop) IsRunning() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.running
}

// run is the main loop. It polls for new events at PollInterval. When
// new events are found, it calls the handler and saves the cursor. When
// no new events exist, it does nothing (zero-cost empty poll).
func (l *Loop) run(ctx context.Context) {
	defer func() {
		l.mu.Lock()
		l.running = false
		l.mu.Unlock()
		l.logger.Printf("wake: stopped (cursor=%d)", l.cursor)
	}()

	ticker := time.NewTicker(l.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.poll(ctx); err != nil {
				l.logger.Printf("wake: poll error: %v", err)
			}
		}
	}
}

// poll reads new events since the cursor, calls the handler, and saves
// the cursor. If no new events exist, returns immediately (empty poll).
func (l *Loop) poll(ctx context.Context) error {
	l.mu.Lock()
	cursor := l.cursor
	l.mu.Unlock()

	events, err := l.store.EventsSince(ctx, "", cursor, l.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("events since: %w", err)
	}

	if len(events) == 0 {
		// Empty poll — zero cost, no handler call.
		return nil
	}

	// Call the handler with the new events.
	if err := l.handler(ctx, events); err != nil {
		return fmt.Errorf("handler: %w", err)
	}

	// Save cursor to the last event's seq.
	newCursor := events[len(events)-1].Seq
	if err := l.store.SaveWakeCursor(ctx, newCursor); err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}

	l.mu.Lock()
	l.cursor = newCursor
	l.mu.Unlock()

	l.logger.Printf("wake: processed %d events, cursor %d → %d",
		len(events), cursor, newCursor)

	return nil
}

// PollOnce performs a single poll cycle without running the full loop.
// This is useful for testing and for systemd timer-based invocation
// (where the timer triggers a one-shot poll rather than a long-lived loop).
// Returns the number of events processed.
func (l *Loop) PollOnce(ctx context.Context) (int, error) {
	l.mu.Lock()
	cursor := l.cursor
	l.mu.Unlock()

	events, err := l.store.EventsSince(ctx, "", cursor, l.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("events since: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	if err := l.handler(ctx, events); err != nil {
		return 0, fmt.Errorf("handler: %w", err)
	}

	newCursor := events[len(events)-1].Seq
	if err := l.store.SaveWakeCursor(ctx, newCursor); err != nil {
		return 0, fmt.Errorf("save cursor: %w", err)
	}

	l.mu.Lock()
	l.cursor = newCursor
	l.mu.Unlock()

	return len(events), nil
}

// InitCursor sets the cursor to the current last event seq. This is used
// when starting fresh — skip all existing events and only process new ones.
func (l *Loop) InitCursor(ctx context.Context) error {
	lastSeq, err := l.store.LastEventSeq(ctx)
	if err != nil {
		return fmt.Errorf("last event seq: %w", err)
	}
	if err := l.store.SaveWakeCursor(ctx, lastSeq); err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	l.mu.Lock()
	l.cursor = lastSeq
	l.mu.Unlock()
	return nil
}
