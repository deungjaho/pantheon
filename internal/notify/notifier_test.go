package notify

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/store"
)

// fakeStore implements AgentLookup for testing.
type fakeStore struct {
	agents map[string]*domain.Agent
}

func (f *fakeStore) GetAgent(ctx context.Context, agentID string) (*domain.Agent, error) {
	a, ok := f.agents[agentID]
	if !ok {
		return nil, nil
	}
	return a, nil
}

func TestTmuxNotifier_NotifyWithSession(t *testing.T) {
	fs := &fakeStore{
		agents: map[string]*domain.Agent{
			"agt_1": {AgentID: "agt_1", TmuxSession: "test-session"},
		},
	}
	n := NewTmuxNotifier(fs)

	var capturedCmd string
	n.SetExecFn(func(cmd *exec.Cmd) error {
		capturedCmd = strings.Join(cmd.Args, " ")
		return nil
	})

	if err := n.Notify(context.Background(), "agt_1", "hello from test"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if !strings.Contains(capturedCmd, "tmux send-keys -t test-session") {
		t.Fatalf("cmd = %q, want tmux send-keys -t test-session", capturedCmd)
	}
	if !strings.Contains(capturedCmd, "hello from test") {
		t.Fatalf("cmd = %q, want message 'hello from test'", capturedCmd)
	}
}

func TestTmuxNotifier_NoSession(t *testing.T) {
	fs := &fakeStore{
		agents: map[string]*domain.Agent{
			"agt_2": {AgentID: "agt_2", TmuxSession: ""},
		},
	}
	n := NewTmuxNotifier(fs)

	called := false
	n.SetExecFn(func(cmd *exec.Cmd) error {
		called = true
		return nil
	})

	if err := n.Notify(context.Background(), "agt_2", "hello"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if called {
		t.Fatal("should not call tmux when no session")
	}
}

func TestTmuxNotifier_AgentNotFound(t *testing.T) {
	fs := &fakeStore{agents: map[string]*domain.Agent{}}
	n := NewTmuxNotifier(fs)

	err := n.Notify(context.Background(), "nonexistent", "hello")
	if err == nil {
		t.Fatal("expected error for nonexistent agent")
	}
}

func TestTmuxNotifier_WithRealStore(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(context.Background(), filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	// Create project + workspace + run + agent with tmux_session.
	eid := "evt_test_1"
	_ = s.RegisterProject(ctx, &domain.Project{
		ProjectID: "prj_t", Name: "test", RepoPath: "/x", BaseRef: "main",
		RegisteredAt: time.Now().UTC(),
	}, eid)
	eid = "evt_test_2"
	_ = s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_t", ProjectID: "prj_t", Name: "test", Objective: "test",
		State: domain.WorkspaceActive, Owner: "test", Host: "test", CreatedAt: time.Now().UTC(),
	}, eid)
	eid = "evt_test_3"
	_ = s.CreateRun(ctx, &domain.Run{
		RunID: "run_t", WorkspaceID: "ws_t", BaseCommit: "abc", Budget: 3600,
		State: domain.RunV2Running,
	}, eid)
	eid = "evt_test_4"
	_ = s.RegisterAgent(ctx, &domain.Agent{
		AgentID: "agt_t", RunID: "run_t", Role: domain.RoleWorker, Runtime: "devin",
		PID: 12345, State: domain.AgentRunning, TmuxSession: "my-session",
		StartedAt: time.Now().UTC(),
	}, eid)

	n := NewTmuxNotifier(s)
	var capturedCmd string
	n.SetExecFn(func(cmd *exec.Cmd) error {
		capturedCmd = strings.Join(cmd.Args, " ")
		return nil
	})

	if err := n.Notify(ctx, "agt_t", "test message"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if !strings.Contains(capturedCmd, "my-session") {
		t.Fatalf("cmd = %q, want session 'my-session'", capturedCmd)
	}
}

func TestInboxNotifier_Write(t *testing.T) {
	dir := t.TempDir()
	n := NewInboxNotifier(dir)

	if err := n.Write("test-project", "First message"); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := n.Write("test-project", "Second message"); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	data, err := readFile(filepath.Join(dir, "test-project.md"))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(string(data), "First message") {
		t.Fatal("missing first message")
	}
	if !strings.Contains(string(data), "Second message") {
		t.Fatal("missing second message")
	}
}

func TestInboxNotifier_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir", "inbox")
	n := NewInboxNotifier(dir)

	if err := n.Write("test", "hello"); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
