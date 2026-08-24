package store

import (
	"context"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func publishTestMessage(t *testing.T, s *Store, messageID, runID string, ttl int) (seq, msgSeq int64) {
	t.Helper()
	ctx := context.Background()
	msg := &domain.Message{
		MessageID:      messageID,
		RunID:          runID,
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_" + messageID,
		TTL:            ttl,
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "test"},
	}
	seq, msgSeq, _, err := s.PublishMessageEnvelope(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessageEnvelope: %v", err)
	}
	return seq, msgSeq
}

func TestAckMessageBasic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_ack1", "run_ack1", 0)

	err := s.AckMessage(ctx, "msg_ack1", "agent_worker1")
	if err != nil {
		t.Fatalf("AckMessage: %v", err)
	}

	st, err := s.GetMessageStatus(ctx, "msg_ack1")
	if err != nil {
		t.Fatalf("GetMessageStatus: %v", err)
	}
	if st.AckState != domain.AckStateAcked {
		t.Errorf("ack_state = %q, want acked", st.AckState)
	}
}

func TestAckMessageIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_ack2", "run_ack2", 0)

	if err := s.AckMessage(ctx, "msg_ack2", "agent1"); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	// Second ack should be a no-op (idempotent).
	if err := s.AckMessage(ctx, "msg_ack2", "agent1"); err != nil {
		t.Fatalf("second ack (idempotent): %v", err)
	}

	st, _ := s.GetMessageStatus(ctx, "msg_ack2")
	if st.AckState != domain.AckStateAcked {
		t.Errorf("ack_state = %q, want acked", st.AckState)
	}
}

func TestAckMessageNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	err := s.AckMessage(ctx, "msg_nonexistent", "agent1")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

func TestAckMessageAfterNack(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_ack3", "run_ack3", 0)

	// Nack first.
	_, _, err := s.NackMessage(ctx, "msg_ack3", "agent1", "busy")
	if err != nil {
		t.Fatalf("NackMessage: %v", err)
	}

	// Ack should fail — message is nacked.
	err = s.AckMessage(ctx, "msg_ack3", "agent1")
	if err == nil {
		t.Fatal("expected error when acking a nacked message")
	}
}

func TestNackMessageBasic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_nack1", "run_nack1", 0)

	retryCount, finalState, err := s.NackMessage(ctx, "msg_nack1", "agent1", "error")
	if err != nil {
		t.Fatalf("NackMessage: %v", err)
	}
	if retryCount != 1 {
		t.Errorf("retry_count = %d, want 1", retryCount)
	}
	if finalState != domain.AckStateNacked {
		t.Errorf("final_state = %q, want nacked", finalState)
	}

	st, _ := s.GetMessageStatus(ctx, "msg_nack1")
	if st.RetryCount != 1 {
		t.Errorf("status retry_count = %d, want 1", st.RetryCount)
	}
}

func TestNackMessageRetryToDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_nack2", "run_nack2", 0)

	// Nack MaxRetries times — last one should mark as dead.
	var finalState domain.AckState
	var retryCount int
	var err error
	for i := 0; i < domain.MaxRetries; i++ {
		retryCount, finalState, err = s.NackMessage(ctx, "msg_nack2", "agent1", "retry")
		if err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}

	if retryCount != domain.MaxRetries {
		t.Errorf("retry_count = %d, want %d", retryCount, domain.MaxRetries)
	}
	if finalState != domain.AckStateDead {
		t.Errorf("final_state = %q, want dead", finalState)
	}

	st, _ := s.GetMessageStatus(ctx, "msg_nack2")
	if !st.IsDead {
		t.Error("IsDead should be true after MaxRetries nacks")
	}
}

func TestNackMessageAfterDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_nack3", "run_nack3", 0)

	// Nack to dead.
	for i := 0; i < domain.MaxRetries; i++ {
		_, _, err := s.NackMessage(ctx, "msg_nack3", "agent1", "retry")
		if err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}

	// Further nack should fail.
	_, _, err := s.NackMessage(ctx, "msg_nack3", "agent1", "retry")
	if err == nil {
		t.Fatal("expected error when nacking a dead message")
	}
}

func TestNackMessageAfterAcked(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_nack4", "run_nack4", 0)

	if err := s.AckMessage(ctx, "msg_nack4", "agent1"); err != nil {
		t.Fatalf("AckMessage: %v", err)
	}

	_, _, err := s.NackMessage(ctx, "msg_nack4", "agent1", "late")
	if err == nil {
		t.Fatal("expected error when nacking an acked message")
	}
}

func TestExpireMessages(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Publish a message with TTL=1 second.
	publishTestMessage(t, s, "msg_exp1", "run_exp1", 1)
	// Publish a message with no TTL.
	publishTestMessage(t, s, "msg_exp2", "run_exp2", 0)

	// Check immediately — nothing should expire.
	expired, err := s.ExpireMessages(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ExpireMessages (immediate): %v", err)
	}
	if len(expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(expired))
	}

	// Wait 2 seconds, then check.
	time.Sleep(2 * time.Second)
	expired, err = s.ExpireMessages(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ExpireMessages (after TTL): %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0] != "msg_exp1" {
		t.Errorf("expired[0] = %q, want msg_exp1", expired[0])
	}

	// Verify the message is marked expired.
	st, _ := s.GetMessageStatus(ctx, "msg_exp1")
	if !st.IsExpired {
		t.Error("msg_exp1 should be expired")
	}

	// The no-TTL message should still be pending.
	st2, _ := s.GetMessageStatus(ctx, "msg_exp2")
	if st2.AckState != domain.AckStatePending {
		t.Errorf("msg_exp2 ack_state = %q, want pending", st2.AckState)
	}
}

func TestExpireMessagesAlreadyAcked(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Publish and immediately ack a message with TTL=1.
	publishTestMessage(t, s, "msg_exp3", "run_exp3", 1)
	if err := s.AckMessage(ctx, "msg_exp3", "agent1"); err != nil {
		t.Fatalf("AckMessage: %v", err)
	}

	time.Sleep(2 * time.Second)
	expired, err := s.ExpireMessages(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ExpireMessages: %v", err)
	}
	// Acked message should not be expired.
	for _, id := range expired {
		if id == "msg_exp3" {
			t.Fatal("acked message should not be expired")
		}
	}
}

func TestGetMessageStatusNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, err := s.GetMessageStatus(ctx, "msg_nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

func TestPublishEnvelopeSetsPendingAckState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	publishTestMessage(t, s, "msg_pending", "run_pending", 0)

	st, err := s.GetMessageStatus(ctx, "msg_pending")
	if err != nil {
		t.Fatalf("GetMessageStatus: %v", err)
	}
	if st.AckState != domain.AckStatePending {
		t.Errorf("ack_state = %q, want pending", st.AckState)
	}
	if st.RetryCount != 0 {
		t.Errorf("retry_count = %d, want 0", st.RetryCount)
	}
}
