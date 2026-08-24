// Package notify implements notification adapters for the Pantheon message bus.
//
// TmuxNotifier encapsulates tmux send-keys as a semantic notify(agent_id, msg)
// operation. InboxNotifier projects messages to inbox/*.md files for
// human-readable fallback.
package notify

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/tangtszho/pantheon/internal/domain"
)

// AgentLookup is the interface for finding an agent by ID.
// Implemented by *store.Store.
type AgentLookup interface {
	GetAgent(ctx context.Context, agentID string) (*domain.Agent, error)
}

// TmuxNotifier sends notifications to agents via tmux send-keys.
// It looks up the agent's tmux_session from the agent registry and
// sends a message to that session.
type TmuxNotifier struct {
	store   AgentLookup
	tmuxBin string
	mu      sync.Mutex
	execFn  func(cmd *exec.Cmd) error
}

// NewTmuxNotifier creates a TmuxNotifier that uses the given store for
// agent lookups. The tmux binary defaults to "tmux" on PATH.
func NewTmuxNotifier(store AgentLookup) *TmuxNotifier {
	return &TmuxNotifier{
		store:   store,
		tmuxBin: "tmux",
		execFn: func(cmd *exec.Cmd) error {
			return cmd.Run()
		},
	}
}

// Notify sends a message to the agent's tmux session.
// If the agent has no tmux_session, or tmux is not available, the
// notification is silently dropped (non-fatal).
func (n *TmuxNotifier) Notify(ctx context.Context, agentID, message string) error {
	agent, err := n.store.GetAgent(ctx, agentID)
	if err != nil {
		return fmt.Errorf("lookup agent %s: %w", agentID, err)
	}
	if agent == nil {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.TmuxSession == "" {
		// No tmux session registered — silently skip.
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	cmd := exec.CommandContext(ctx, n.tmuxBin, "send-keys", "-t", agent.TmuxSession, message, "Enter")
	cmd.Stderr = os.Stderr
	return n.execFn(cmd)
}

// SetExecFn overrides the exec function for testing.
func (n *TmuxNotifier) SetExecFn(fn func(cmd *exec.Cmd) error) {
	n.execFn = fn
}

// InboxNotifier appends messages to inbox/{name}.md files for
// human-readable fallback. This preserves the existing inbox/outbox
// contract during the transition to the message bus.
type InboxNotifier struct {
	inboxDir string
	mu       sync.Mutex
}

// NewInboxNotifier creates an InboxNotifier that writes to the given
// inbox directory.
func NewInboxNotifier(inboxDir string) *InboxNotifier {
	return &InboxNotifier{inboxDir: inboxDir}
}

// Write appends a message to inbox/{name}.md.
func (n *InboxNotifier) Write(name, message string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := os.MkdirAll(n.inboxDir, 0o755); err != nil {
		return fmt.Errorf("create inbox dir: %w", err)
	}
	path := filepath.Join(n.inboxDir, name+".md")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open inbox file: %w", err)
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s\n\n", message)
	return err
}

// FileInboxProjector projects v1.1 message envelopes to inbox/outbox files
// (C-004). It implements rpc.InboxProjector. Projection is best-effort:
// errors are returned but the caller (publish handler) ignores them.
//
// Messages are projected to:
//   - inbox/{recipient}.md — for the recipient to read
//   - outbox/{sender}.md — for the sender's sent log
//
// Restricted messages (sensitivity=restricted) are NOT projected — the
// caller filters them before calling Project.
type FileInboxProjector struct {
	inboxDir  string
	outboxDir string
	mu        sync.Mutex
}

// NewFileInboxProjector creates a FileInboxProjector that writes to the
// given inbox and outbox directories.
func NewFileInboxProjector(inboxDir, outboxDir string) *FileInboxProjector {
	return &FileInboxProjector{
		inboxDir:  inboxDir,
		outboxDir: outboxDir,
	}
}

// Project writes a message envelope to inbox/{recipient}.md and
// outbox/{sender}.md (C-004). Best-effort: returns error but caller
// ignores it.
func (p *FileInboxProjector) Project(ctx context.Context, msg *domain.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Format the message as a human-readable markdown entry.
	entry := formatMessageEntry(msg)

	// Project to inbox (recipient).
	if msg.Recipient.AgentID != "" || msg.Recipient.Role != "" {
		recipientName := msg.Recipient.AgentID
		if recipientName == "" {
			recipientName = string(msg.Recipient.Role)
		}
		if err := p.writeToDir(p.inboxDir, recipientName, entry); err != nil {
			return fmt.Errorf("project inbox: %w", err)
		}
	}

	// Project to outbox (sender).
	if msg.Sender.AgentID != "" || msg.Sender.Role != "" {
		senderName := msg.Sender.AgentID
		if senderName == "" {
			senderName = string(msg.Sender.Role)
		}
		if err := p.writeToDir(p.outboxDir, senderName, entry); err != nil {
			return fmt.Errorf("project outbox: %w", err)
		}
	}

	return nil
}

// writeToDir appends an entry to {dir}/{name}.md.
func (p *FileInboxProjector) writeToDir(dir, name, entry string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}
	path := filepath.Join(dir, name+".md")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s\n\n", entry)
	return err
}

// formatMessageEntry formats a message envelope as a markdown entry.
func formatMessageEntry(msg *domain.Message) string {
	var body string
	if msg.PayloadRef.Kind == domain.PayloadKindInline {
		body = msg.PayloadRef.Inline
	} else {
		body = "[artifact: " + msg.PayloadRef.ArtifactID + "]"
	}
	return fmt.Sprintf("## %s [%s]\n\n- **From:** %s (%s)\n- **To:** %s (%s)\n- **Run:** %s\n- **Seq:** %d\n- **Time:** %s\n\n%s\n",
		msg.Type,
		msg.MessageID,
		msg.Sender.AgentID, msg.Sender.Role,
		msg.Recipient.AgentID, msg.Recipient.Role,
		msg.RunID,
		msg.Seq,
		msg.CreatedAt.Format("2006-01-02 15:04:05"),
		body,
	)
}
