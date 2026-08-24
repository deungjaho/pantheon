package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/tangtszho/pantheon/internal/domain"
)

// registerTestContinuation is a helper that registers a continuation with
// sensible defaults and returns the resulting record.
func registerTestContinuation(t *testing.T, s *Store, runID, objective string) *domain.Continuation {
	t.Helper()
	ctx := context.Background()
	cid := mustNewID(t, "cont_")
	c := &domain.Continuation{
		ContinuationID:     cid,
		RunID:              runID,
		ProjectID:          "prj_test",
		Owner:              "pm",
		SuccessorObjective: objective,
	}
	if err := s.RegisterContinuation(ctx, c, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterContinuation: %v", err)
	}
	return c
}

func TestRegisterContinuation_Idempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID := "run_idem_1"

	c1 := registerTestContinuation(t, s, runID, "finish the feature")
	c2 := registerTestContinuation(t, s, runID, "finish the feature")

	// Same run_id + objective → same continuation_id (idempotent).
	if c1.ContinuationID != c2.ContinuationID {
		t.Fatalf("idempotent register returned different IDs: %q vs %q",
			c1.ContinuationID, c2.ContinuationID)
	}

	// Only one continuation row should exist.
	list, err := s.ListContinuations(ctx, "all")
	if err != nil {
		t.Fatalf("ListContinuations: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 continuation after idempotent register, got %d", len(list))
	}

	// A different objective for the same run creates a new continuation.
	c3 := registerTestContinuation(t, s, runID, "different objective")
	if c3.ContinuationID == c1.ContinuationID {
		t.Fatal("different objective should create a new continuation")
	}
	list, err = s.ListContinuations(ctx, "all")
	if err != nil {
		t.Fatalf("ListContinuations 2: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 continuations, got %d", len(list))
	}
}

func TestListPendingContinuations(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// pending
	c1 := registerTestContinuation(t, s, "run_a", "obj-a")
	// pending → notified
	c2 := registerTestContinuation(t, s, "run_b", "obj-b")
	if err := s.MarkContinuationNotified(ctx, c2.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("MarkContinuationNotified: %v", err)
	}
	// notified → fulfilled (should NOT appear in pending list)
	c3 := registerTestContinuation(t, s, "run_c", "obj-c")
	if err := s.MarkContinuationNotified(ctx, c3.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("MarkContinuationNotified c3: %v", err)
	}
	if err := s.FulfillContinuation(ctx, c3.ContinuationID, "run_successor", mustNewID(t, "evt_")); err != nil {
		t.Fatalf("FulfillContinuation: %v", err)
	}
	// pending → cancelled (should NOT appear in pending list)
	c4 := registerTestContinuation(t, s, "run_d", "obj-d")
	if err := s.CancelContinuation(ctx, c4.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CancelContinuation: %v", err)
	}

	pending, err := s.ListPendingContinuations(ctx)
	if err != nil {
		t.Fatalf("ListPendingContinuations: %v", err)
	}
	// Only c1 (pending) and c2 (notified) should appear.
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending continuations, got %d", len(pending))
	}
	ids := map[string]bool{}
	for _, c := range pending {
		ids[c.ContinuationID] = true
	}
	if !ids[c1.ContinuationID] || !ids[c2.ContinuationID] {
		t.Fatalf("pending list missing expected IDs; got %v", ids)
	}
	if ids[c3.ContinuationID] || ids[c4.ContinuationID] {
		t.Fatal("fulfilled/cancelled continuations should not appear in pending list")
	}
}

func TestMarkContinuationNotified(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := registerTestContinuation(t, s, "run_notify", "obj")

	// Before: state=pending, wake_count=0, wake_sent_at=nil.
	got, err := s.GetContinuation(ctx, c.ContinuationID)
	if err != nil {
		t.Fatalf("GetContinuation: %v", err)
	}
	if got.State != domain.ContinuationPending {
		t.Fatalf("initial state = %q, want pending", got.State)
	}
	if got.WakeCount != 0 {
		t.Fatalf("initial wake_count = %d, want 0", got.WakeCount)
	}
	if got.WakeSentAt != nil {
		t.Fatalf("initial wake_sent_at = %v, want nil", got.WakeSentAt)
	}

	// Mark notified.
	if err := s.MarkContinuationNotified(ctx, c.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("MarkContinuationNotified: %v", err)
	}

	got, err = s.GetContinuation(ctx, c.ContinuationID)
	if err != nil {
		t.Fatalf("GetContinuation after notify: %v", err)
	}
	if got.State != domain.ContinuationNotified {
		t.Fatalf("state after notify = %q, want notified", got.State)
	}
	if got.WakeCount != 1 {
		t.Fatalf("wake_count after notify = %d, want 1", got.WakeCount)
	}
	if got.WakeSentAt == nil {
		t.Fatal("wake_sent_at should be set after notify")
	}
}

func TestFulfillContinuation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := registerTestContinuation(t, s, "run_fulfill", "obj")
	if err := s.MarkContinuationNotified(ctx, c.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("MarkContinuationNotified: %v", err)
	}

	successorRunID := "run_successor_1"
	if err := s.FulfillContinuation(ctx, c.ContinuationID, successorRunID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("FulfillContinuation: %v", err)
	}

	got, err := s.GetContinuation(ctx, c.ContinuationID)
	if err != nil {
		t.Fatalf("GetContinuation after fulfill: %v", err)
	}
	if got.State != domain.ContinuationFulfilled {
		t.Fatalf("state = %q, want fulfilled", got.State)
	}
	if got.SuccessorRunID != successorRunID {
		t.Fatalf("successor_run_id = %q, want %q", got.SuccessorRunID, successorRunID)
	}
	if got.FulfilledAt == nil {
		t.Fatal("fulfilled_at should be set")
	}

	// Fulfilling an already-fulfilled continuation should fail (terminal state).
	err = s.FulfillContinuation(ctx, c.ContinuationID, "run_other", mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("fulfilling an already-fulfilled continuation should fail")
	}
}

func TestCancelContinuation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := registerTestContinuation(t, s, "run_cancel", "obj")

	if err := s.CancelContinuation(ctx, c.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CancelContinuation: %v", err)
	}

	got, err := s.GetContinuation(ctx, c.ContinuationID)
	if err != nil {
		t.Fatalf("GetContinuation after cancel: %v", err)
	}
	if got.State != domain.ContinuationCancelled {
		t.Fatalf("state = %q, want cancelled", got.State)
	}

	// Cancelling an already-cancelled continuation should fail (terminal state).
	err = s.CancelContinuation(ctx, c.ContinuationID, mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("cancelling an already-cancelled continuation should fail")
	}
}

func TestRestartReconcile(t *testing.T) {
	// Verify that continuations persist across a close/reopen of the store
	// (simulating a daemon restart). All continuation state is in SQLite.
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")

	ctx := context.Background()
	s1, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	c := registerTestContinuation(t, s1, "run_restart", "obj-restart")
	if err := s1.MarkContinuationNotified(ctx, c.ContinuationID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("MarkContinuationNotified: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen — continuations must persist.
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetContinuation(ctx, c.ContinuationID)
	if err != nil {
		t.Fatalf("GetContinuation after restart: %v", err)
	}
	if got == nil {
		t.Fatal("continuation not found after restart")
	}
	if got.State != domain.ContinuationNotified {
		t.Fatalf("state after restart = %q, want notified", got.State)
	}
	if got.WakeCount != 1 {
		t.Fatalf("wake_count after restart = %d, want 1", got.WakeCount)
	}
	if got.SuccessorObjective != "obj-restart" {
		t.Fatalf("successor_objective after restart = %q, want obj-restart", got.SuccessorObjective)
	}

	// The pending list should still include it.
	pending, err := s2.ListPendingContinuations(ctx)
	if err != nil {
		t.Fatalf("ListPendingContinuations after restart: %v", err)
	}
	if len(pending) != 1 || pending[0].ContinuationID != c.ContinuationID {
		t.Fatalf("pending list after restart = %v, want [%s]", pending, c.ContinuationID)
	}
}
