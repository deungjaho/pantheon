// Package integration contains end-to-end integration tests for the
// Pantheon communication system (C-005). These tests exercise the full
// message chain: publish → subscribe → ack/nack → deadline → retry →
// crash recovery, using a real SQLite store.
package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/store"
	"github.com/tangtszho/pantheon/internal/wake"
)

// newStore creates a fresh SQLite store in a temp directory.
func newStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	s, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustNewID(t *testing.T, prefix string) string {
	t.Helper()
	id, err := domain.NewID(prefix)
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	return id
}

func mustPublish(t *testing.T, s *store.Store, msg *domain.Message) (seq, msgSeq int64, msgID string) {
	t.Helper()
	// Auto-fill idempotency_key if not set (matches service behavior).
	if msg.IdempotencyKey == "" {
		msg.IdempotencyKey = msg.MessageID
	}
	seq, msgSeq, msgID, err := s.PublishMessageEnvelope(context.Background(), msg)
	if err != nil {
		t.Fatalf("PublishMessageEnvelope: %v", err)
	}
	return seq, msgSeq, msgID
}

// mustNewIDNoT generates an ID without a *testing.T (for use in goroutines).
func mustNewIDNoT(prefix string) string {
	id, err := domain.NewID(prefix)
	if err != nil {
		panic(err)
	}
	return id
}

// publishNoT publishes a message without a *testing.T (for use in goroutines).
// Auto-fills idempotency_key if not set.
func publishNoT(ctx context.Context, s *store.Store, msg *domain.Message) error {
	if msg.IdempotencyKey == "" {
		msg.IdempotencyKey = msg.MessageID
	}
	_, _, _, err := s.PublishMessageEnvelope(ctx, msg)
	return err
}

// TestFullMessageChain tests the complete PM→Worker→Verifier message
// chain: directive → report → complete → verify → ack (C-005).
func TestFullMessageChain(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	// 1. PM sends directive to Worker.
	directive := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgDirective,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		Recipient:   domain.MessageEndpoint{AgentID: "worker-001", Role: domain.RoleWorker},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "Implement feature X."},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}
	_, dirMsgSeq, dirMsgID := mustPublish(t, s, directive)
	if dirMsgSeq != 1 {
		t.Errorf("directive msg_seq=%d, want 1", dirMsgSeq)
	}

	// 2. Worker reports progress.
	report := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgReport,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "worker-001", Role: domain.RoleWorker},
		Recipient:   domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "50% done."},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}
	_, rptMsgSeq, _ := mustPublish(t, s, report)
	if rptMsgSeq != 2 {
		t.Errorf("report msg_seq=%d, want 2", rptMsgSeq)
	}

	// 3. Worker sends complete (claim, not fact).
	complete := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgComplete,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "worker-001", Role: domain.RoleWorker},
		Recipient:   domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "Done."},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}
	_, cmpMsgSeq, _ := mustPublish(t, s, complete)
	if cmpMsgSeq != 3 {
		t.Errorf("complete msg_seq=%d, want 3", cmpMsgSeq)
	}

	// 4. PM sends verify to Verifier.
	verify := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgVerify,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		Recipient:   domain.MessageEndpoint{AgentID: "verifier-001", Role: domain.RoleVerifier},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "Verify run."},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}
	_, vrfMsgSeq, _ := mustPublish(t, s, verify)
	if vrfMsgSeq != 4 {
		t.Errorf("verify msg_seq=%d, want 4", vrfMsgSeq)
	}

	// 5. Verifier acks the directive.
	if err := s.AckMessage(ctx, dirMsgID, "verifier-001"); err != nil {
		t.Fatalf("AckMessage: %v", err)
	}

	// 6. Verify all 4 messages are readable via MessagesByRun.
	events, err := s.MessagesByRun(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("MessagesByRun: %v", err)
	}
	if len(events) < 4 {
		t.Errorf("expected at least 4 message events, got %d", len(events))
	}

	// 7. Verify the directive is acked.
	status, err := s.GetMessageStatus(ctx, dirMsgID)
	if err != nil {
		t.Fatalf("GetMessageStatus: %v", err)
	}
	if status.AckState != domain.AckStateAcked {
		t.Errorf("directive ack_state=%s, want acked", status.AckState)
	}
}

// TestConcurrentPublishNoSeqConflict tests that concurrent publish
// operations do not produce seq conflicts or message loss (C-005).
func TestConcurrentPublishNoSeqConflict(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	const goroutines = 10
	const perGoroutine = 20
	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				msg := &domain.Message{
					MessageID: mustNewIDNoT("msg_"),
					Type:      domain.MsgReport,
					RunID:     runID,
					Sender: domain.MessageEndpoint{
						AgentID: fmt.Sprintf("worker-%03d", workerID),
						Role:    domain.RoleWorker,
					},
					Recipient: domain.MessageEndpoint{
						AgentID: "pm-001",
						Role:    domain.RoleController,
					},
					PayloadRef: domain.PayloadRef{
						Kind:   domain.PayloadKindInline,
						Inline: fmt.Sprintf("report %d-%d", workerID, j),
					},
					Sensitivity: domain.SensNormal,
					CreatedAt:   time.Now().UTC(),
				}
				if err := publishNoT(ctx, s, msg); err != nil {
					errors.Add(1)
					t.Errorf("publish %d-%d: %v", workerID, j, err)
				}
			}
		}(i)
	}
	wg.Wait()

	if errors.Load() > 0 {
		t.Fatalf("%d publish errors", errors.Load())
	}

	// Verify all messages are readable.
	events, err := s.MessagesByRun(ctx, runID, 0, 1000)
	if err != nil {
		t.Fatalf("MessagesByRun: %v", err)
	}
	expected := goroutines * perGoroutine
	if len(events) != expected {
		t.Errorf("expected %d message events, got %d", expected, len(events))
	}

	// Verify message_seq values are unique and contiguous (1..N).
	seqs := make(map[int64]bool, expected)
	for _, e := range events {
		if e.MessageSeq > 0 {
			if seqs[e.MessageSeq] {
				t.Errorf("duplicate message_seq: %d", e.MessageSeq)
			}
			seqs[e.MessageSeq] = true
		}
	}
	if len(seqs) != expected {
		t.Errorf("unique message_seq count=%d, want %d", len(seqs), expected)
	}
}

// TestCrashRecoveryCursorResume tests that after a "crash" (close + reopen
// store), the wake loop resumes from the persisted cursor (C-005).
func TestCrashRecoveryCursorResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	ctx := context.Background()

	// Phase 1: open store, publish events, run wake loop, close.
	s1, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}

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

	loop1 := wake.New(s1, handler, wake.Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    100,
	})

	// Publish 3 events.
	for i := 0; i < 3; i++ {
		msg := &domain.Message{
			MessageID:   mustNewIDNoT("msg_"),
			Type:        domain.MsgDirective,
			RunID:       "run_crash",
			Sender:      domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
			Recipient:   domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
			PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msg"},
			Sensitivity: domain.SensNormal,
			CreatedAt:   time.Now().UTC(),
		}
		if err := publishNoT(ctx, s1, msg); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	// Start loop, let it process.
	loopCtx1, cancel1 := context.WithTimeout(ctx, 300*time.Millisecond)
	if err := loop1.Start(loopCtx1); err != nil {
		t.Fatalf("loop1 Start: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	cancel1()
	_ = s1.Close()

	mu.Lock()
	phase1Count := len(processed)
	mu.Unlock()
	if phase1Count != 3 {
		t.Fatalf("phase 1 processed %d events, want 3", phase1Count)
	}

	// Phase 2: reopen store (simulate crash recovery), publish 2 more events.
	s2, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer s2.Close()

	for i := 0; i < 2; i++ {
		msg := &domain.Message{
			MessageID:   mustNewIDNoT("msg_"),
			Type:        domain.MsgReport,
			RunID:       "run_crash",
			Sender:      domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
			Recipient:   domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
			PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "rpt"},
			Sensitivity: domain.SensNormal,
			CreatedAt:   time.Now().UTC(),
		}
		if err := publishNoT(ctx, s2, msg); err != nil {
			t.Fatalf("publish 2: %v", err)
		}
	}

	// Start new wake loop — should resume from persisted cursor.
	var phase2Processed []int64
	var mu2 sync.Mutex
	handler2 := func(ctx context.Context, events []domain.Event) error {
		mu2.Lock()
		defer mu2.Unlock()
		for _, e := range events {
			phase2Processed = append(phase2Processed, e.Seq)
		}
		return nil
	}

	loop2 := wake.New(s2, handler2, wake.Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    100,
	})

	loopCtx2, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel2()
	if err := loop2.Start(loopCtx2); err != nil {
		t.Fatalf("loop2 Start: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	cancel2()

	mu2.Lock()
	defer mu2.Unlock()
	if len(phase2Processed) != 2 {
		t.Errorf("phase 2 processed %d events, want 2 (crash recovery)", len(phase2Processed))
	}

	// Verify no overlap: phase 2 events should have higher seq than phase 1.
	if len(phase2Processed) > 0 && len(processed) > 0 {
		if phase2Processed[0] <= processed[len(processed)-1] {
			t.Errorf("phase 2 seq %d should be > phase 1 last seq %d",
				phase2Processed[0], processed[len(processed)-1])
		}
	}
}

// TestIdempotencyDedupEndToEnd tests that publishing the same message
// with the same idempotency_key does not produce duplicates (C-005).
func TestIdempotencyDedupEndToEnd(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	idempotencyKey := "idem-key-001"
	msgID := mustNewID(t, "msg_")

	// First publish.
	msg := &domain.Message{
		MessageID:      msgID,
		IdempotencyKey: idempotencyKey,
		Type:           domain.MsgDirective,
		RunID:          runID,
		Sender:         domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
		Recipient:      domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "test"},
		Sensitivity:    domain.SensNormal,
		CreatedAt:      time.Now().UTC(),
	}
	seq1, msgSeq1, retMsgID1 := mustPublish(t, s, msg)

	// Second publish with same idempotency_key — should dedup.
	msg2 := &domain.Message{
		MessageID:      mustNewID(t, "msg_"), // different message_id
		IdempotencyKey: idempotencyKey,       // same key
		Type:           domain.MsgDirective,
		RunID:          runID,
		Sender:         domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
		Recipient:      domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "test"},
		Sensitivity:    domain.SensNormal,
		CreatedAt:      time.Now().UTC(),
	}
	seq2, msgSeq2, retMsgID2 := mustPublish(t, s, msg2)

	// Should return same seq and message_id (deduped).
	if seq1 != seq2 {
		t.Errorf("seq: first=%d, second=%d (should be same, deduped)", seq1, seq2)
	}
	if msgSeq1 != msgSeq2 {
		t.Errorf("msg_seq: first=%d, second=%d (should be same, deduped)", msgSeq1, msgSeq2)
	}
	if retMsgID1 != retMsgID2 {
		t.Errorf("message_id: first=%s, second=%s (should be same, deduped)", retMsgID1, retMsgID2)
	}

	// Verify only 1 message event in the run.
	events, err := s.MessagesByRun(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("MessagesByRun: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 message event (deduped), got %d", len(events))
	}
}

// TestDeadlineExpiryEndToEnd tests that TTL-expired messages are
// correctly marked as expired (C-005).
func TestDeadlineExpiryEndToEnd(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	// Publish a message with 1 second TTL.
	msg := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgDirective,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
		Recipient:   domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "urgent"},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
		TTL:         1, // 1 second
	}
	_, _, msgID := mustPublish(t, s, msg)

	// Wait for TTL to expire.
	time.Sleep(2 * time.Second)

	// Run ExpireMessages.
	expired, err := s.ExpireMessages(ctx, time.Now())
	if err != nil {
		t.Fatalf("ExpireMessages: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired message, got %d", len(expired))
	}
	if expired[0] != msgID {
		t.Errorf("expired message_id=%s, want %s", expired[0], msgID)
	}

	// Verify status is expired.
	status, err := s.GetMessageStatus(ctx, msgID)
	if err != nil {
		t.Fatalf("GetMessageStatus: %v", err)
	}
	if status.AckState != domain.AckStateExpired {
		t.Errorf("ack_state=%s, want expired", status.AckState)
	}
	if !status.IsExpired {
		t.Error("is_expired=false, want true")
	}
}

// TestRetryToDeadEndToEnd tests that 3 nacks mark a message as dead (C-005).
func TestRetryToDeadEndToEnd(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	msg := &domain.Message{
		MessageID:   mustNewID(t, "msg_"),
		Type:        domain.MsgDirective,
		RunID:       runID,
		Sender:      domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
		Recipient:   domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
		PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "test"},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}
	_, _, msgID := mustPublish(t, s, msg)

	// Nack 3 times — should mark dead on the 3rd.
	for i := 1; i <= 3; i++ {
		retryCount, finalState, err := s.NackMessage(ctx, msgID, "worker", "failed")
		if err != nil {
			t.Fatalf("NackMessage %d: %v", i, err)
		}
		if i < 3 {
			if finalState == domain.AckStateDead {
				t.Errorf("nack %d: final_state=dead, want not dead yet", i)
			}
		} else {
			if finalState != domain.AckStateDead {
				t.Errorf("nack %d: final_state=%s, want dead", i, finalState)
			}
		}
		_ = retryCount
	}

	// Verify status is dead.
	status, err := s.GetMessageStatus(ctx, msgID)
	if err != nil {
		t.Fatalf("GetMessageStatus: %v", err)
	}
	if status.AckState != domain.AckStateDead {
		t.Errorf("ack_state=%s, want dead", status.AckState)
	}
	if !status.IsDead {
		t.Error("is_dead=false, want true")
	}
	if status.RetryCount != 3 {
		t.Errorf("retry_count=%d, want 3", status.RetryCount)
	}
}

// TestWakeLoopEmptyPollZeroCost tests that the wake loop does not call
// the handler when there are no new events (C-005).
func TestWakeLoopEmptyPollZeroCost(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	var handlerCalls atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		handlerCalls.Add(1)
		return nil
	}

	loop := wake.New(s, handler, wake.Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    100,
	})

	loopCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := loop.Start(loopCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	cancel()

	if handlerCalls.Load() > 0 {
		t.Errorf("handler called %d times on empty store, want 0", handlerCalls.Load())
	}
}

// TestWakeLoopProcessesNewEvents tests that the wake loop processes
// events published after the loop starts (C-005).
func TestWakeLoopProcessesNewEvents(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	runID := mustNewID(t, "run_")

	var processed atomic.Int64
	handler := func(ctx context.Context, events []domain.Event) error {
		processed.Add(int64(len(events)))
		return nil
	}

	loop := wake.New(s, handler, wake.Config{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    100,
	})

	loopCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := loop.Start(loopCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Publish 5 events after loop starts.
	time.Sleep(100 * time.Millisecond) // let loop do a few empty polls
	for i := 0; i < 5; i++ {
		msg := &domain.Message{
			MessageID:   mustNewIDNoT("msg_"),
			Type:        domain.MsgDirective,
			RunID:       runID,
			Sender:      domain.MessageEndpoint{AgentID: "pm", Role: domain.RoleController},
			Recipient:   domain.MessageEndpoint{AgentID: "worker", Role: domain.RoleWorker},
			PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msg"},
			Sensitivity: domain.SensNormal,
			CreatedAt:   time.Now().UTC(),
		}
		if err := publishNoT(ctx, s, msg); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond) // let loop process
	cancel()

	if processed.Load() != 5 {
		t.Errorf("processed %d events, want 5", processed.Load())
	}
}
