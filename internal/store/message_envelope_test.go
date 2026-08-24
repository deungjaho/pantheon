package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func TestPublishMessageEnvelopeBasic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	msg := &domain.Message{
		MessageID:      "msg_testbasic",
		RunID:          "run_testbasic",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		CreatedAt:      now,
		IdempotencyKey: "req_testbasic",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "do the thing"},
	}

	seq, msgSeq, msgID, err := s.PublishMessageEnvelope(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessageEnvelope: %v", err)
	}
	if seq == 0 {
		t.Fatal("expected non-zero global seq")
	}
	if msgSeq != 1 {
		t.Errorf("message_seq = %d, want 1 (first message in run)", msgSeq)
	}
	if msgID != "msg_testbasic" {
		t.Errorf("message_id = %q, want msg_testbasic", msgID)
	}
}

func TestPublishMessageEnvelopePerRunSeq(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		msg := &domain.Message{
			MessageID:      "msg_seqtest" + string(rune('A'+i)),
			RunID:          "run_seqtest",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			CreatedAt:      time.Now().UTC(),
			IdempotencyKey: "req_seqtest" + string(rune('A'+i)),
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msg"},
		}
		_, msgSeq, _, err := s.PublishMessageEnvelope(ctx, msg)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if msgSeq != int64(i+1) {
			t.Errorf("message %d: seq = %d, want %d", i, msgSeq, i+1)
		}
	}
}

func TestPublishMessageEnvelopeIdempotencyDedup(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	msg := &domain.Message{
		MessageID:      "msg_deduptest",
		RunID:          "run_deduptest",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_deduptest",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "dedup me"},
	}

	seq1, msgSeq1, _, err := s.PublishMessageEnvelope(ctx, msg)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// Same idempotency_key, different message_id — should dedup.
	msg2 := *msg
	msg2.MessageID = "msg_deduptest2"
	seq2, msgSeq2, msgID2, err := s.PublishMessageEnvelope(ctx, &msg2)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}

	if seq1 != seq2 {
		t.Errorf("dedup: seq changed %d → %d", seq1, seq2)
	}
	if msgSeq1 != msgSeq2 {
		t.Errorf("dedup: message_seq changed %d → %d", msgSeq1, msgSeq2)
	}
	// The returned message_id should be the original (from the first insert).
	if msgID2 != "msg_deduptest" {
		t.Errorf("dedup: message_id = %q, want msg_deduptest (original)", msgID2)
	}
}

func TestPublishMessageEnvelopeValidation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	t.Run("missing_message_id", func(t *testing.T) {
		msg := &domain.Message{
			RunID:          "run_v",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			CreatedAt:      time.Now().UTC(),
			IdempotencyKey: "req_v",
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "x"},
		}
		_, _, _, err := s.PublishMessageEnvelope(ctx, msg)
		if err == nil {
			t.Fatal("expected error for missing message_id")
		}
	})

	t.Run("inline_too_large", func(t *testing.T) {
		msg := &domain.Message{
			MessageID:      "msg_v",
			RunID:          "run_v",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			CreatedAt:      time.Now().UTC(),
			IdempotencyKey: "req_v",
			PayloadRef: domain.PayloadRef{
				Kind:   domain.PayloadKindInline,
				Inline: strings.Repeat("x", domain.MaxInlinePayload+1),
			},
		}
		_, _, _, err := s.PublishMessageEnvelope(ctx, msg)
		if err == nil {
			t.Fatal("expected error for oversized inline payload")
		}
	})
}

func TestMessagesByRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Publish 3 messages to run_A, 1 to run_B.
	for i := 0; i < 3; i++ {
		msg := &domain.Message{
			MessageID:      "msg_byrunA" + string(rune('A'+i)),
			RunID:          "run_byrunA",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			CreatedAt:      time.Now().UTC(),
			IdempotencyKey: "req_byrunA" + string(rune('A'+i)),
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msg"},
		}
		if _, _, _, err := s.PublishMessageEnvelope(ctx, msg); err != nil {
			t.Fatalf("publish A%d: %v", i, err)
		}
	}
	msgB := &domain.Message{
		MessageID:      "msg_byrunB",
		RunID:          "run_byrunB",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgReport,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_byrunB",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msgB"},
	}
	if _, _, _, err := s.PublishMessageEnvelope(ctx, msgB); err != nil {
		t.Fatalf("publish B: %v", err)
	}

	// Query run_A — should get 3, ordered by message_seq.
	events, err := s.MessagesByRun(ctx, "run_byrunA", 0, 10)
	if err != nil {
		t.Fatalf("MessagesByRun: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(events))
	}
	for i, e := range events {
		if e.MessageSeq != int64(i+1) {
			t.Errorf("event %d: message_seq = %d, want %d", i, e.MessageSeq, i+1)
		}
		if e.MessageID == "" {
			t.Errorf("event %d: message_id is empty", i)
		}
		if e.IdempotencyKey == "" {
			t.Errorf("event %d: idempotency_key is empty", i)
		}
	}

	// Query run_A with cursor=2 — should get 1 (message_seq=3).
	events, err = s.MessagesByRun(ctx, "run_byrunA", 2, 10)
	if err != nil {
		t.Fatalf("MessagesByRun cursor: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 message after cursor=2, got %d", len(events))
	}
	if events[0].MessageSeq != 3 {
		t.Errorf("message_seq = %d, want 3", events[0].MessageSeq)
	}

	// Query run_B — should get 1.
	events, err = s.MessagesByRun(ctx, "run_byrunB", 0, 10)
	if err != nil {
		t.Fatalf("MessagesByRun B: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 message for run_B, got %d", len(events))
	}
}

func TestPublishMessageEnvelopeIndependentRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Publish to run_X, then run_Y — both should get message_seq=1.
	msgX := &domain.Message{
		MessageID:      "msg_indepX",
		RunID:          "run_indepX",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_indepX",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "x"},
	}
	msgY := &domain.Message{
		MessageID:      "msg_indepY",
		RunID:          "run_indepY",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_indepY",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "y"},
	}

	_, seqX, _, err := s.PublishMessageEnvelope(ctx, msgX)
	if err != nil {
		t.Fatalf("publish X: %v", err)
	}
	_, seqY, _, err := s.PublishMessageEnvelope(ctx, msgY)
	if err != nil {
		t.Fatalf("publish Y: %v", err)
	}
	if seqX != 1 || seqY != 1 {
		t.Errorf("independent runs: seqX=%d seqY=%d, both want 1", seqX, seqY)
	}
}
