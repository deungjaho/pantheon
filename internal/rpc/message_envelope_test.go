package rpc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tangtszho/pantheon/internal/domain"
)

func TestMessagePublishEnvelope(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		MessageID:      "msg_rpctest1",
		RunID:          "run_rpctest1",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "req_rpctest1",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "hello from RPC"},
	})
	if resp.Error != nil {
		t.Fatalf("message.publish.envelope error: %v", resp.Error)
	}
	var result MessagePublishEnvelopeResult
	json.Unmarshal(resp.Result, &result)
	if result.Seq == 0 {
		t.Fatal("expected non-zero seq")
	}
	if result.MessageSeq != 1 {
		t.Errorf("message_seq = %d, want 1", result.MessageSeq)
	}
	if result.MessageID != "msg_rpctest1" {
		t.Errorf("message_id = %q, want msg_rpctest1", result.MessageID)
	}
	if result.Deduped {
		t.Error("first publish should not be deduped")
	}
}

func TestMessagePublishEnvelopeDedup(t *testing.T) {
	svc, _ := newTestService(t)

	p := MessagePublishEnvelopeParams{
		MessageID:      "msg_deduprpc1",
		RunID:          "run_deduprpc",
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "req_deduprpc",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "dedup"},
	}
	resp1 := callRPC(t, svc, "message.publish.envelope", p)
	var result1 MessagePublishEnvelopeResult
	json.Unmarshal(resp1.Result, &result1)

	// Same idempotency_key, different message_id.
	p.MessageID = "msg_deduprpc2"
	resp2 := callRPC(t, svc, "message.publish.envelope", p)
	var result2 MessagePublishEnvelopeResult
	json.Unmarshal(resp2.Result, &result2)

	if result1.Seq != result2.Seq {
		t.Errorf("dedup: seq changed %d → %d", result1.Seq, result2.Seq)
	}
	if !result2.Deduped {
		t.Error("second publish with same idempotency_key should be deduped")
	}
}

func TestMessagePublishEnvelopeValidation(t *testing.T) {
	svc, _ := newTestService(t)

	t.Run("missing_run_id", func(t *testing.T) {
		resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      "msg_v",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			IdempotencyKey: "req_v",
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "x"},
		})
		if resp.Error == nil {
			t.Fatal("expected error for missing run_id")
		}
	})

	t.Run("invalid_type", func(t *testing.T) {
		resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      "msg_v",
			RunID:          "run_v",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MessageType("bogus"),
			IdempotencyKey: "req_v",
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "x"},
		})
		if resp.Error == nil {
			t.Fatal("expected error for invalid type")
		}
	})

	t.Run("inline_too_large", func(t *testing.T) {
		resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      "msg_v",
			RunID:          "run_v",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			IdempotencyKey: "req_v",
			PayloadRef: domain.PayloadRef{
				Kind:   domain.PayloadKindInline,
				Inline: strings.Repeat("x", domain.MaxInlinePayload+1),
			},
		})
		if resp.Error == nil {
			t.Fatal("expected error for oversized inline payload")
		}
	})
}

func TestMessagesByRun(t *testing.T) {
	svc, _ := newTestService(t)

	// Publish 3 messages to run_rpcbyrun.
	for i := 0; i < 3; i++ {
		callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      "msg_rpcbyrun" + string(rune('A'+i)),
			RunID:          "run_rpcbyrun",
			Sender:         domain.MessageEndpoint{Role: domain.RolePM},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgDirective,
			IdempotencyKey: "req_rpcbyrun" + string(rune('A'+i)),
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "msg"},
		})
	}

	resp := callRPC(t, svc, "messages.by_run", MessagesByRunParams{
		RunID: "run_rpcbyrun",
	})
	if resp.Error != nil {
		t.Fatalf("messages.by_run error: %v", resp.Error)
	}
	var result MessagesByRunResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(result.Messages))
	}
	for i, m := range result.Messages {
		if m.MessageSeq != int64(i+1) {
			t.Errorf("message %d: message_seq = %d, want %d", i, m.MessageSeq, i+1)
		}
	}
	if result.NextCursor != 3 {
		t.Errorf("next_cursor = %d, want 3", result.NextCursor)
	}

	// Query with cursor=1 — should get 2 (seq 2 and 3).
	resp = callRPC(t, svc, "messages.by_run", MessagesByRunParams{
		RunID:  "run_rpcbyrun",
		Cursor: 1,
	})
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 2 {
		t.Fatalf("expected 2 messages after cursor=1, got %d", len(result.Messages))
	}
}

func TestMessagesByRunValidation(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "messages.by_run", MessagesByRunParams{})
	if resp.Error == nil {
		t.Fatal("expected error for missing run_id")
	}
}

func TestMessagePublishEnvelopeAutoGenerateID(t *testing.T) {
	svc, _ := newTestService(t)

	// No message_id provided — should auto-generate.
	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		RunID:      "run_autogen",
		Sender:     domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:  domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:       domain.MsgDirective,
		PayloadRef: domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "auto"},
	})
	if resp.Error != nil {
		t.Fatalf("auto-gen ID error: %v", resp.Error)
	}
	var result MessagePublishEnvelopeResult
	json.Unmarshal(resp.Result, &result)
	if result.MessageID == "" {
		t.Fatal("expected auto-generated message_id")
	}
	if !strings.HasPrefix(result.MessageID, "msg_") {
		t.Errorf("auto-generated message_id = %q, want msg_ prefix", result.MessageID)
	}
}
