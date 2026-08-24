package wake

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/store"
)

// fakeContStore implements both ContinuationStore and OrphanedRunStore for
// unit testing the reconcile tick. It tracks continuations and published
// messages in memory.
type fakeContStore struct {
	mu            sync.Mutex
	continuations map[string]*domain.Continuation
	published     []publishedMsg
	orphaned      []domain.OrphanedRun
}

type publishedMsg struct {
	runID, senderRole, recipientInstance, body, idempotencyKey string
}

func newFakeContStore() *fakeContStore {
	return &fakeContStore{continuations: make(map[string]*domain.Continuation)}
}

func (s *fakeContStore) ListPendingContinuations(ctx context.Context) ([]*domain.Continuation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Continuation
	for _, c := range s.continuations {
		if c.State == domain.ContinuationPending || c.State == domain.ContinuationNotified {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *fakeContStore) MarkContinuationNotified(ctx context.Context, continuationID, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.continuations[continuationID]
	if !ok {
		return domain.ErrNotFound("continuation not found: " + continuationID)
	}
	now := time.Now().UTC()
	c.State = domain.ContinuationNotified
	c.WakeSentAt = &now
	c.WakeCount++
	return nil
}

func (s *fakeContStore) UpdateWakeSentAt(ctx context.Context, continuationID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.continuations[continuationID]
	if !ok {
		return domain.ErrNotFound("continuation not found: " + continuationID)
	}
	c.WakeSentAt = &now
	c.WakeCount++
	return nil
}

func (s *fakeContStore) PublishMessageEnvelope(ctx context.Context, msg *domain.Message) (seq, messageSeq int64, messageID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, publishedMsg{
		runID:             msg.RunID,
		senderRole:        string(msg.Sender.Role),
		recipientInstance: msg.Recipient.Instance,
		body:              msg.PayloadRef.Inline,
		idempotencyKey:    msg.IdempotencyKey,
	})
	return int64(len(s.published)), int64(len(s.published)), msg.MessageID, nil
}

func (s *fakeContStore) ListOrphanedRuns(ctx context.Context) ([]domain.OrphanedRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.OrphanedRun, len(s.orphaned))
	copy(out, s.orphaned)
	return out, nil
}

func (s *fakeContStore) getPublished() []publishedMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]publishedMsg, len(s.published))
	copy(out, s.published)
	return out
}

func (s *fakeContStore) addContinuation(c *domain.Continuation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.continuations[c.ContinuationID] = c
}

func (s *fakeContStore) setOrphaned(runs []domain.OrphanedRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orphaned = runs
}

func TestTick_FirstNotification(t *testing.T) {
	fs := newFakeContStore()
	fs.addContinuation(&domain.Continuation{
		ContinuationID:     "cont_1",
		RunID:              "run_1",
		SuccessorObjective: "finish feature",
		State:              domain.ContinuationPending,
		Owner:              "pm",
	})

	rec := NewReconciler(fs, nil, nil, time.Hour)
	result, err := rec.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if result.Checked != 1 {
		t.Fatalf("checked = %d, want 1", result.Checked)
	}
	if result.Notified != 1 {
		t.Fatalf("notified = %d, want 1", result.Notified)
	}
	if result.Skipped != 0 {
		t.Fatalf("skipped = %d, want 0", result.Skipped)
	}

	// A wake.continuation message should have been published.
	msgs := fs.getPublished()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}
	if msgs[0].senderRole != "metis" {
		t.Fatalf("senderRole = %q, want metis", msgs[0].senderRole)
	}
	if msgs[0].recipientInstance != "pm" {
		t.Fatalf("recipientInstance = %q, want pm", msgs[0].recipientInstance)
	}

	// The continuation should now be notified with wake_count=1.
	fs.mu.Lock()
	c := fs.continuations["cont_1"]
	fs.mu.Unlock()
	if c.State != domain.ContinuationNotified {
		t.Fatalf("state = %q, want notified", c.State)
	}
	if c.WakeCount != 1 {
		t.Fatalf("wake_count = %d, want 1", c.WakeCount)
	}
}

func TestTick_Dedup(t *testing.T) {
	fs := newFakeContStore()
	// Already notified, wake_sent_at is recent (within wake_gap).
	recent := time.Now().UTC()
	fs.addContinuation(&domain.Continuation{
		ContinuationID:     "cont_2",
		RunID:              "run_2",
		SuccessorObjective: "obj",
		State:              domain.ContinuationNotified,
		WakeSentAt:         &recent,
		WakeCount:          1,
		Owner:              "pm",
	})

	rec := NewReconciler(fs, nil, nil, time.Hour)
	result, err := rec.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if result.Notified != 0 {
		t.Fatalf("notified = %d, want 0 (dedup)", result.Notified)
	}
	if result.ReNotified != 0 {
		t.Fatalf("re_notified = %d, want 0 (dedup)", result.ReNotified)
	}
	if result.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", result.Skipped)
	}

	// No new message should have been published.
	msgs := fs.getPublished()
	if len(msgs) != 0 {
		t.Fatalf("published %d messages, want 0 (dedup)", len(msgs))
	}

	// wake_count should NOT have incremented.
	fs.mu.Lock()
	c := fs.continuations["cont_2"]
	fs.mu.Unlock()
	if c.WakeCount != 1 {
		t.Fatalf("wake_count = %d, want 1 (no increment on dedup)", c.WakeCount)
	}
}

func TestTick_ReNotify(t *testing.T) {
	fs := newFakeContStore()
	// Notified, but wake_sent_at is older than wake_gap.
	stale := time.Now().UTC().Add(-2 * time.Hour)
	fs.addContinuation(&domain.Continuation{
		ContinuationID:     "cont_3",
		RunID:              "run_3",
		SuccessorObjective: "obj",
		State:              domain.ContinuationNotified,
		WakeSentAt:         &stale,
		WakeCount:          1,
		Owner:              "pm",
	})

	rec := NewReconciler(fs, nil, nil, time.Hour)
	result, err := rec.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if result.ReNotified != 1 {
		t.Fatalf("re_notified = %d, want 1", result.ReNotified)
	}
	if result.Notified != 0 {
		t.Fatalf("notified = %d, want 0", result.Notified)
	}
	if result.Skipped != 0 {
		t.Fatalf("skipped = %d, want 0", result.Skipped)
	}

	// A re-notification message should have been published.
	msgs := fs.getPublished()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}

	// wake_count should have incremented (re-notification).
	fs.mu.Lock()
	c := fs.continuations["cont_3"]
	fs.mu.Unlock()
	if c.WakeCount != 2 {
		t.Fatalf("wake_count = %d, want 2 (incremented on re-notify)", c.WakeCount)
	}
}

func TestTick_RestartIdempotent(t *testing.T) {
	// Use a real store to verify that continuations persist across a
	// close/reopen and that re-running the tick produces no duplicates.
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	ctx := context.Background()

	s1, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	// Register a pending continuation.
	cid := "cont_restart_1"
	c := &domain.Continuation{
		ContinuationID:     cid,
		RunID:              "run_restart",
		ProjectID:          "prj",
		Owner:              "pm",
		SuccessorObjective: "obj",
	}
	if err := s1.RegisterContinuation(ctx, c, "evt_reg_1"); err != nil {
		t.Fatalf("RegisterContinuation: %v", err)
	}

	// First tick: should notify (pending → notified).
	rec1 := NewReconciler(s1, nil, nil, time.Hour)
	r1, err := rec1.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	if r1.Notified != 1 {
		t.Fatalf("Tick 1 notified = %d, want 1", r1.Notified)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen — continuation state must persist.
	s2, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetContinuation(ctx, cid)
	if err != nil {
		t.Fatalf("GetContinuation after restart: %v", err)
	}
	if got.State != domain.ContinuationNotified {
		t.Fatalf("state after restart = %q, want notified", got.State)
	}

	// Second tick: should skip (within wake_gap, dedup).
	rec2 := NewReconciler(s2, nil, nil, time.Hour)
	r2, err := rec2.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	if r2.Notified != 0 {
		t.Fatalf("Tick 2 notified = %d, want 0 (dedup after restart)", r2.Notified)
	}
	if r2.Skipped != 1 {
		t.Fatalf("Tick 2 skipped = %d, want 1", r2.Skipped)
	}
}

func TestTick_OrphanedRun(t *testing.T) {
	fs := newFakeContStore()
	fs.setOrphaned([]domain.OrphanedRun{
		{
			RunID:    "run_orphan_1",
			State:    domain.RunV2Running,
			Owner:    "pm",
			AgentID:  "agt_dead",
			AgentPID: 999999,
		},
	})

	rec := NewReconciler(fs, fs, nil, time.Hour)
	result, err := rec.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(result.OrphanedRuns) != 1 {
		t.Fatalf("orphaned runs = %d, want 1", len(result.OrphanedRuns))
	}
	if result.OrphanedRuns[0].RunID != "run_orphan_1" {
		t.Fatalf("orphaned run id = %q, want run_orphan_1", result.OrphanedRuns[0].RunID)
	}

	// A wake.continuation message should have been published for the orphan.
	msgs := fs.getPublished()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}
	if msgs[0].senderRole != "metis" {
		t.Fatalf("senderRole = %q, want metis", msgs[0].senderRole)
	}
	// The body must mention the orphaned run and "PM attention required".
	if !strings.Contains(msgs[0].body, "run_orphan_1") {
		t.Fatalf("body does not mention run id: %q", msgs[0].body)
	}
	if !strings.Contains(msgs[0].body, "PM attention required") {
		t.Fatalf("body does not mention PM attention: %q", msgs[0].body)
	}
}

func TestTick_NoAutoSuccessor(t *testing.T) {
	fs := newFakeContStore()
	// A pending continuation + an orphaned run.
	fs.addContinuation(&domain.Continuation{
		ContinuationID:     "cont_noauto",
		RunID:              "run_noauto",
		SuccessorObjective: "obj",
		State:              domain.ContinuationPending,
		Owner:              "pm",
	})
	fs.setOrphaned([]domain.OrphanedRun{
		{RunID: "run_noauto", State: domain.RunV2Running, Owner: "pm"},
	})

	rec := NewReconciler(fs, fs, nil, time.Hour)
	result, err := rec.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	// The continuation should be notified but NOT fulfilled.
	fs.mu.Lock()
	c := fs.continuations["cont_noauto"]
	fs.mu.Unlock()
	if c.State != domain.ContinuationNotified {
		t.Fatalf("continuation state = %q, want notified (not fulfilled)", c.State)
	}
	if c.SuccessorRunID != "" {
		t.Fatalf("successor_run_id = %q, want empty (no auto-successor)", c.SuccessorRunID)
	}

	// The orphaned run should be surfaced but no successor created.
	if len(result.OrphanedRuns) != 1 {
		t.Fatalf("orphaned runs = %d, want 1", len(result.OrphanedRuns))
	}

	// No continuation should be fulfilled — only wake messages published.
	// All published messages should be wake.continuation (notify/orphan),
	// none should indicate fulfillment.
	msgs := fs.getPublished()
	for _, m := range msgs {
		if m.senderRole != "metis" {
			t.Fatalf("unexpected senderRole %q — no auto-successor actions", m.senderRole)
		}
	}
}
