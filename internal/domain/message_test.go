package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMessageTypeValid(t *testing.T) {
	valid := []MessageType{MsgDirective, MsgReport, MsgState, MsgBlock, MsgComplete, MsgVerify, MsgAck, MsgNack}
	for _, mt := range valid {
		if !mt.Valid() {
			t.Errorf("MessageType %q should be valid", mt)
		}
	}
	if MessageType("unknown").Valid() {
		t.Error("unknown message type should be invalid")
	}
}

func TestValidMessageRole(t *testing.T) {
	valid := []AgentRole{RoleController, RoleWorker, RoleVerifier, RoleMetis, RolePM}
	for _, r := range valid {
		if !ValidMessageRole(r) {
			t.Errorf("AgentRole %q should be valid for messages", r)
		}
	}
	if ValidMessageRole(AgentRole("unknown")) {
		t.Error("unknown role should be invalid")
	}
}

func TestSensitivityValid(t *testing.T) {
	if !SensNormal.Valid() {
		t.Error("SensNormal should be valid")
	}
	if !SensRestricted.Valid() {
		t.Error("SensRestricted should be valid")
	}
	if Sensitivity("secret").Valid() {
		t.Error("unknown sensitivity should be invalid")
	}
}

func TestMessageValidate(t *testing.T) {
	base := Message{
		MessageID:      "msg_test123",
		RunID:          "run_abc",
		Sender:         MessageEndpoint{Role: RolePM},
		Recipient:      MessageEndpoint{Role: RoleWorker},
		Type:           MsgDirective,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: "req_test123",
		PayloadRef:     PayloadRef{Kind: PayloadKindInline, Inline: "do something"},
	}

	t.Run("valid", func(t *testing.T) {
		m := base
		if err := m.Validate(); err != nil {
			t.Fatalf("valid message failed: %v", err)
		}
	})

	t.Run("missing_message_id", func(t *testing.T) {
		m := base
		m.MessageID = ""
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "message_id") {
			t.Fatalf("expected message_id error, got: %v", err)
		}
	})

	t.Run("missing_run_id", func(t *testing.T) {
		m := base
		m.RunID = ""
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "run_id") {
			t.Fatalf("expected run_id error, got: %v", err)
		}
	})

	t.Run("invalid_type", func(t *testing.T) {
		m := base
		m.Type = MessageType("bogus")
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "type") {
			t.Fatalf("expected type error, got: %v", err)
		}
	})

	t.Run("invalid_sender_role", func(t *testing.T) {
		m := base
		m.Sender = MessageEndpoint{Role: AgentRole("bogus")}
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "sender role") {
			t.Fatalf("expected sender role error, got: %v", err)
		}
	})

	t.Run("missing_idempotency_key", func(t *testing.T) {
		m := base
		m.IdempotencyKey = ""
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "idempotency_key") {
			t.Fatalf("expected idempotency_key error, got: %v", err)
		}
	})

	t.Run("inline_too_large", func(t *testing.T) {
		m := base
		m.PayloadRef.Inline = strings.Repeat("x", MaxInlinePayload+1)
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "exceeds max") {
			t.Fatalf("expected exceeds max error, got: %v", err)
		}
	})

	t.Run("artifact_missing_sha256", func(t *testing.T) {
		m := base
		m.PayloadRef = PayloadRef{Kind: PayloadKindArtifact, ArtifactID: "art_123"}
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "sha256") {
			t.Fatalf("expected sha256 error, got: %v", err)
		}
	})

	t.Run("missing_created_at", func(t *testing.T) {
		m := base
		m.CreatedAt = time.Time{}
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "created_at") {
			t.Fatalf("expected created_at error, got: %v", err)
		}
	})
}

func TestParseMessageV11Envelope(t *testing.T) {
	original := Message{
		MessageID:      "msg_abc",
		RunID:          "run_123",
		Type:           MsgDirective,
		Sender:         MessageEndpoint{Role: RolePM},
		Recipient:      MessageEndpoint{Role: RoleWorker},
		IdempotencyKey: "req_abc",
		PayloadRef:     PayloadRef{Kind: PayloadKindInline, Inline: "hello"},
		CreatedAt:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := ParseMessage(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.MessageID != "msg_abc" {
		t.Errorf("message_id = %q, want msg_abc", parsed.MessageID)
	}
	if parsed.Type != MsgDirective {
		t.Errorf("type = %q, want directive", parsed.Type)
	}
	if parsed.PayloadRef.Inline != "hello" {
		t.Errorf("inline = %q, want hello", parsed.PayloadRef.Inline)
	}
}

func TestParseMessageLegacy(t *testing.T) {
	legacy := `{"topic":"directive.pantheon","body":"start working","from":"metis","to":"worker"}`
	parsed, err := ParseMessage(json.RawMessage(legacy))
	if err != nil {
		t.Fatalf("parse legacy: %v", err)
	}
	if parsed.Type != MsgDirective {
		t.Errorf("type = %q, want directive", parsed.Type)
	}
	if parsed.PayloadRef.Inline != "start working" {
		t.Errorf("inline = %q, want 'start working'", parsed.PayloadRef.Inline)
	}
	if parsed.Sender.Role != RoleMetis {
		t.Errorf("sender role = %q, want metis", parsed.Sender.Role)
	}
	if parsed.Recipient.Role != RoleWorker {
		t.Errorf("recipient role = %q, want worker", parsed.Recipient.Role)
	}
}

func TestParseMessageLegacyUnknownTopic(t *testing.T) {
	legacy := `{"topic":"random.topic","body":"data","from":"agent1"}`
	parsed, err := ParseMessage(json.RawMessage(legacy))
	if err != nil {
		t.Fatalf("parse legacy: %v", err)
	}
	// Unknown prefix defaults to "report".
	if parsed.Type != MsgReport {
		t.Errorf("type = %q, want report (default)", parsed.Type)
	}
}

func TestInferTypeFromTopic(t *testing.T) {
	cases := []struct {
		topic string
		want  MessageType
	}{
		{"directive.pantheon", MsgDirective},
		{"report.hydra", MsgReport},
		{"verify.run123", MsgVerify},
		{"state.worker1", MsgState},
		{"block.task1", MsgBlock},
		{"complete.task2", MsgComplete},
		{"ack.msg1", MsgAck},
		{"nack.msg2", MsgNack},
		{"unknown.topic", MsgReport}, // default
		{"nodothereport", MsgReport}, // no dot, default
	}
	for _, c := range cases {
		got := inferTypeFromTopic(c.topic)
		if got != c.want {
			t.Errorf("inferTypeFromTopic(%q) = %q, want %q", c.topic, got, c.want)
		}
	}
}
