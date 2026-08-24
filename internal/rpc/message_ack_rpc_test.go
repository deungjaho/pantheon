package rpc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func newAckTestService(t *testing.T) *Service {
	t.Helper()
	svc, _ := newTestService(t)
	return svc
}

func publishForAckTest(t *testing.T, svc *Service, messageID, runID string, ttl int) {
	t.Helper()
	params := MessagePublishEnvelopeParams{
		MessageID:      messageID,
		RunID:          runID,
		Sender:         domain.MessageEndpoint{Role: domain.RolePM},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		TTL:            ttl,
		IdempotencyKey: "req_" + messageID,
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "test"},
	}
	raw, _ := json.Marshal(params)
	_, err := svc.handleMessagePublishEnvelope(context.Background(), raw)
	if err != nil {
		t.Fatalf("publish envelope: %v", err)
	}
}

func TestRPCMessageAck(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_ack1", "run_rpc_ack1", 0)

	params, _ := json.Marshal(MessageAckParams{
		MessageID: "msg_rpc_ack1",
		AgentID:   "agent_worker1",
	})
	res, err := svc.handleMessageAck(ctx, params)
	if err != nil {
		t.Fatalf("handleMessageAck: %v", err)
	}
	ackRes := res.(*MessageAckResult)
	if !ackRes.Acked {
		t.Error("acked should be true")
	}

	// Verify via status.
	statusParams, _ := json.Marshal(MessagesStatusParams{MessageID: "msg_rpc_ack1"})
	statusRes, err := svc.handleMessagesStatus(ctx, statusParams)
	if err != nil {
		t.Fatalf("handleMessagesStatus: %v", err)
	}
	st := statusRes.(*MessagesStatusResult)
	if st.AckState != domain.AckStateAcked {
		t.Errorf("ack_state = %q, want acked", st.AckState)
	}
}

func TestRPCMessageAckIdempotent(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_ack2", "run_rpc_ack2", 0)

	params, _ := json.Marshal(MessageAckParams{MessageID: "msg_rpc_ack2"})

	// First ack.
	_, err := svc.handleMessageAck(ctx, params)
	if err != nil {
		t.Fatalf("first ack: %v", err)
	}
	// Second ack — should be idempotent (no error).
	_, err = svc.handleMessageAck(ctx, params)
	if err != nil {
		t.Fatalf("second ack (idempotent): %v", err)
	}
}

func TestRPCMessageAckMissingMessageID(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	params, _ := json.Marshal(MessageAckParams{})
	_, err := svc.handleMessageAck(ctx, params)
	if err == nil {
		t.Fatal("expected error for missing message_id")
	}
}

func TestRPCMessageNack(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_nack1", "run_rpc_nack1", 0)

	params, _ := json.Marshal(MessageNackParams{
		MessageID: "msg_rpc_nack1",
		AgentID:   "agent_worker1",
		Reason:    "processing error",
	})
	res, err := svc.handleMessageNack(ctx, params)
	if err != nil {
		t.Fatalf("handleMessageNack: %v", err)
	}
	nackRes := res.(*MessageNackResult)
	if nackRes.RetryCount != 1 {
		t.Errorf("retry_count = %d, want 1", nackRes.RetryCount)
	}
	if nackRes.FinalState != domain.AckStateNacked {
		t.Errorf("final_state = %q, want nacked", nackRes.FinalState)
	}
}

func TestRPCMessageNackToDead(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_nack2", "run_rpc_nack2", 0)

	params, _ := json.Marshal(MessageNackParams{
		MessageID: "msg_rpc_nack2",
		Reason:    "retry",
	})

	var lastRes *MessageNackResult
	for i := 0; i < domain.MaxRetries; i++ {
		res, err := svc.handleMessageNack(ctx, params)
		if err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
		lastRes = res.(*MessageNackResult)
	}
	if lastRes.FinalState != domain.AckStateDead {
		t.Errorf("final_state = %q, want dead", lastRes.FinalState)
	}
	if lastRes.RetryCount != domain.MaxRetries {
		t.Errorf("retry_count = %d, want %d", lastRes.RetryCount, domain.MaxRetries)
	}
}

func TestRPCMessagesDeadlineCheck(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()

	// Publish with TTL=1 second.
	publishForAckTest(t, svc, "msg_rpc_exp1", "run_rpc_exp1", 1)
	// Publish with no TTL.
	publishForAckTest(t, svc, "msg_rpc_exp2", "run_rpc_exp2", 0)

	// Immediate check — nothing expired.
	params, _ := json.Marshal(MessagesDeadlineCheckParams{})
	res, err := svc.handleMessagesDeadlineCheck(ctx, params)
	if err != nil {
		t.Fatalf("deadline check (immediate): %v", err)
	}
	dlRes := res.(*MessagesDeadlineCheckResult)
	if len(dlRes.ExpiredMessageIDs) != 0 {
		t.Errorf("expected 0 expired, got %d", len(dlRes.ExpiredMessageIDs))
	}

	// Wait 2 seconds.
	time.Sleep(2 * time.Second)

	res, err = svc.handleMessagesDeadlineCheck(ctx, params)
	if err != nil {
		t.Fatalf("deadline check (after TTL): %v", err)
	}
	dlRes = res.(*MessagesDeadlineCheckResult)
	if len(dlRes.ExpiredMessageIDs) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(dlRes.ExpiredMessageIDs))
	}
	if dlRes.ExpiredMessageIDs[0] != "msg_rpc_exp1" {
		t.Errorf("expired[0] = %q, want msg_rpc_exp1", dlRes.ExpiredMessageIDs[0])
	}
}

func TestRPCMessagesStatus(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_status1", "run_rpc_status1", 0)

	params, _ := json.Marshal(MessagesStatusParams{MessageID: "msg_rpc_status1"})
	res, err := svc.handleMessagesStatus(ctx, params)
	if err != nil {
		t.Fatalf("handleMessagesStatus: %v", err)
	}
	st := res.(*MessagesStatusResult)
	if st.AckState != domain.AckStatePending {
		t.Errorf("ack_state = %q, want pending", st.AckState)
	}
	if st.RetryCount != 0 {
		t.Errorf("retry_count = %d, want 0", st.RetryCount)
	}
	if st.IsDead || st.IsExpired {
		t.Error("new message should not be dead or expired")
	}
}

func TestRPCMessagesStatusNotFound(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	params, _ := json.Marshal(MessagesStatusParams{MessageID: "msg_nonexistent"})
	_, err := svc.handleMessagesStatus(ctx, params)
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

func TestRPCAckAfterNackFails(t *testing.T) {
	svc := newAckTestService(t)
	ctx := context.Background()
	publishForAckTest(t, svc, "msg_rpc_acknack", "run_rpc_acknack", 0)

	// Nack first.
	nackParams, _ := json.Marshal(MessageNackParams{MessageID: "msg_rpc_acknack"})
	_, err := svc.handleMessageNack(ctx, nackParams)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Ack should fail.
	ackParams, _ := json.Marshal(MessageAckParams{MessageID: "msg_rpc_acknack"})
	_, err = svc.handleMessageAck(ctx, ackParams)
	if err == nil {
		t.Fatal("expected error when acking a nacked message")
	}
}
