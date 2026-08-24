package notify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func TestFileInboxProjector_Project(t *testing.T) {
	tmp := t.TempDir()
	inboxDir := filepath.Join(tmp, "inbox")
	outboxDir := filepath.Join(tmp, "outbox")
	projector := NewFileInboxProjector(inboxDir, outboxDir)

	msg := &domain.Message{
		MessageID: "msg_test_001",
		Type:      domain.MsgDirective,
		RunID:     "run_001",
		Seq:       1,
		Sender: domain.MessageEndpoint{
			AgentID: "pm-001",
			Role:    domain.RoleController,
		},
		Recipient: domain.MessageEndpoint{
			AgentID: "worker-001",
			Role:    domain.RoleWorker,
		},
		PayloadRef: domain.PayloadRef{
			Kind:   domain.PayloadKindInline,
			Inline: "Please implement feature X.",
		},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
	}

	if err := projector.Project(context.Background(), msg); err != nil {
		t.Fatalf("Project: %v", err)
	}

	// Check inbox file.
	inboxPath := filepath.Join(inboxDir, "worker-001.md")
	inboxContent, err := os.ReadFile(inboxPath)
	if err != nil {
		t.Fatalf("read inbox: %v", err)
	}
	if !strings.Contains(string(inboxContent), "msg_test_001") {
		t.Errorf("inbox missing message_id: %s", inboxContent)
	}
	if !strings.Contains(string(inboxContent), "Please implement feature X.") {
		t.Errorf("inbox missing payload: %s", inboxContent)
	}
	if !strings.Contains(string(inboxContent), "worker-001") {
		t.Errorf("inbox missing recipient: %s", inboxContent)
	}

	// Check outbox file.
	outboxPath := filepath.Join(outboxDir, "pm-001.md")
	outboxContent, err := os.ReadFile(outboxPath)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if !strings.Contains(string(outboxContent), "msg_test_001") {
		t.Errorf("outbox missing message_id: %s", outboxContent)
	}
	if !strings.Contains(string(outboxContent), "pm-001") {
		t.Errorf("outbox missing sender: %s", outboxContent)
	}
}

func TestFileInboxProjector_ArtifactPayload(t *testing.T) {
	tmp := t.TempDir()
	projector := NewFileInboxProjector(
		filepath.Join(tmp, "inbox"),
		filepath.Join(tmp, "outbox"),
	)

	msg := &domain.Message{
		MessageID: "msg_artifact_001",
		Type:      domain.MsgReport,
		Sender:    domain.MessageEndpoint{AgentID: "worker-001", Role: domain.RoleWorker},
		Recipient: domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		PayloadRef: domain.PayloadRef{
			Kind:       domain.PayloadKindArtifact,
			ArtifactID: "art_abc123",
		},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}

	if err := projector.Project(context.Background(), msg); err != nil {
		t.Fatalf("Project: %v", err)
	}

	inboxPath := filepath.Join(tmp, "inbox", "pm-001.md")
	content, err := os.ReadFile(inboxPath)
	if err != nil {
		t.Fatalf("read inbox: %v", err)
	}
	if !strings.Contains(string(content), "art_abc123") {
		t.Errorf("inbox missing artifact ID: %s", content)
	}
}

func TestFileInboxProjector_RoleOnlyNoAgentID(t *testing.T) {
	tmp := t.TempDir()
	projector := NewFileInboxProjector(
		filepath.Join(tmp, "inbox"),
		filepath.Join(tmp, "outbox"),
	)

	msg := &domain.Message{
		MessageID: "msg_role_only",
		Type:      domain.MsgDirective,
		Sender:    domain.MessageEndpoint{Role: domain.RoleController},
		Recipient: domain.MessageEndpoint{Role: domain.RoleWorker},
		PayloadRef: domain.PayloadRef{
			Kind:   domain.PayloadKindInline,
			Inline: "Role-only message.",
		},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}

	if err := projector.Project(context.Background(), msg); err != nil {
		t.Fatalf("Project: %v", err)
	}

	// Should use role name as filename.
	inboxPath := filepath.Join(tmp, "inbox", string(domain.RoleWorker)+".md")
	if _, err := os.Stat(inboxPath); err != nil {
		t.Errorf("inbox file by role not created: %v", err)
	}

	outboxPath := filepath.Join(tmp, "outbox", string(domain.RoleController)+".md")
	if _, err := os.Stat(outboxPath); err != nil {
		t.Errorf("outbox file by role not created: %v", err)
	}
}

func TestFileInboxProjector_AppendsMultipleMessages(t *testing.T) {
	tmp := t.TempDir()
	projector := NewFileInboxProjector(
		filepath.Join(tmp, "inbox"),
		filepath.Join(tmp, "outbox"),
	)

	for i := 0; i < 3; i++ {
		msg := &domain.Message{
			MessageID:   "msg_multi_" + string(rune('a'+i)),
			Type:        domain.MsgDirective,
			Sender:      domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
			Recipient:   domain.MessageEndpoint{AgentID: "worker-001", Role: domain.RoleWorker},
			PayloadRef:  domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "message"},
			Sensitivity: domain.SensNormal,
			CreatedAt:   time.Now().UTC(),
		}
		if err := projector.Project(context.Background(), msg); err != nil {
			t.Fatalf("Project %d: %v", i, err)
		}
	}

	inboxPath := filepath.Join(tmp, "inbox", "worker-001.md")
	content, err := os.ReadFile(inboxPath)
	if err != nil {
		t.Fatalf("read inbox: %v", err)
	}

	// Should contain 3 message entries.
	count := strings.Count(string(content), "## directive")
	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
}

func TestFileInboxProjector_NoRecipientNoInbox(t *testing.T) {
	tmp := t.TempDir()
	inboxDir := filepath.Join(tmp, "inbox")
	outboxDir := filepath.Join(tmp, "outbox")
	projector := NewFileInboxProjector(inboxDir, outboxDir)

	// Message with no recipient — should only project to outbox.
	msg := &domain.Message{
		MessageID: "msg_no_recipient",
		Type:      domain.MsgDirective,
		Sender:    domain.MessageEndpoint{AgentID: "pm-001", Role: domain.RoleController},
		PayloadRef: domain.PayloadRef{
			Kind:   domain.PayloadKindInline,
			Inline: "Broadcast message.",
		},
		Sensitivity: domain.SensNormal,
		CreatedAt:   time.Now().UTC(),
	}

	if err := projector.Project(context.Background(), msg); err != nil {
		t.Fatalf("Project: %v", err)
	}

	// Inbox dir should not have any .md files.
	entries, err := os.ReadDir(inboxDir)
	if err == nil {
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".md") {
				t.Errorf("inbox should be empty, found %s", e.Name())
			}
		}
	}

	// Outbox should have the message.
	outboxPath := filepath.Join(outboxDir, "pm-001.md")
	if _, err := os.Stat(outboxPath); err != nil {
		t.Errorf("outbox file not created: %v", err)
	}
}
