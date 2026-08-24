package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

func TestClaudeAdapter_Start_RequiresWorktreePath(t *testing.T) {
	c := NewClaudeAdapter(WithClaudeRunner(&fakeRunner{}))
	_, err := c.Start(context.Background(), rpc.RuntimeStartParams{
		Objective: "test",
	})
	if err == nil {
		t.Fatal("expected error for empty worktree_path")
	}
}

func TestClaudeAdapter_Start_RequiresObjective(t *testing.T) {
	c := NewClaudeAdapter(WithClaudeRunner(&fakeRunner{}))
	_, err := c.Start(context.Background(), rpc.RuntimeStartParams{
		WorktreePath: "/tmp/test",
	})
	if err == nil {
		t.Fatal("expected error for empty objective")
	}
}

func TestClaudeAdapter_CreatedWithOptions(t *testing.T) {
	c := NewClaudeAdapter(
		WithClaudeBin("/custom/claude"),
		WithClaudeModel("claude-sonnet-4"),
		WithClaudePermissionMode("dangerous"),
		WithClaudePidDir("/tmp/pids"),
		WithClaudeRunner(&fakeRunner{}),
	)
	if c.ClaudeBin != "/custom/claude" {
		t.Fatalf("ClaudeBin = %q, want /custom/claude", c.ClaudeBin)
	}
	if c.Model != "claude-sonnet-4" {
		t.Fatalf("Model = %q, want claude-sonnet-4", c.Model)
	}
	if c.PermissionMode != "dangerous" {
		t.Fatalf("PermissionMode = %q, want dangerous", c.PermissionMode)
	}
	if c.PidDir != "/tmp/pids" {
		t.Fatalf("PidDir = %q, want /tmp/pids", c.PidDir)
	}
}

func TestClaudeAdapter_Start_GeneratesNonEmptyPrompt(t *testing.T) {
	fr := &fakeRunner{}
	dir, err := os.MkdirTemp("", "pantheon-claude-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	c := NewClaudeAdapter(
		WithClaudeRunner(fr),
		WithClaudePidDir(dir),
		WithClaudeModel("claude-sonnet-4"),
	)

	h, err := c.Start(context.Background(), rpc.RuntimeStartParams{
		RunID:        "run_test1",
		TaskID:       "tsk_test1",
		WorktreePath: "/tmp/test-wt",
		Objective:    "fix the login bug",
		Scope: domain.TaskScope{
			Include: []string{"auth/"},
			Exclude: []string{"vendor/"},
		},
		Budget: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if h.AgentID == "" {
		t.Fatal("agent_id is empty")
	}
	if !startsWith(h.AgentID, "agt_") {
		t.Fatalf("agent_id should start with agt_, got %s", h.AgentID)
	}
	if h.SessionID == "" {
		t.Fatal("session_id should be derived and non-empty")
	}
	if len(fr.startedCmds) != 1 {
		t.Fatalf("expected 1 started cmd, got %d", len(fr.startedCmds))
	}

	// Verify pidfile was written.
	pidfile := filepath.Join(dir, "pantheon-claude-"+h.AgentID+".pid")
	data, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatalf("pidfile not written: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("pidfile is empty")
	}

	// Verify prompt file was written and is non-empty.
	promptFile := filepath.Join(dir, "pantheon-claude-"+h.AgentID+".prompt")
	promptData, err := os.ReadFile(promptFile)
	if err != nil {
		t.Fatalf("prompt file not written: %v", err)
	}
	if len(promptData) == 0 {
		t.Fatal("prompt file is empty")
	}
	if !contains(string(promptData), "fix the login bug") {
		t.Fatal("prompt should contain the objective")
	}

	// Verify the command args include --dangerously-skip-permissions and -p.
	args := fr.startedCmds[0].Args
	if !contains(join(args), "--dangerously-skip-permissions") {
		t.Fatal("args should contain --dangerously-skip-permissions")
	}
	if !contains(join(args), "-p") {
		t.Fatal("args should contain -p (print mode)")
	}
	if !contains(join(args), "--model") || !contains(join(args), "claude-sonnet-4") {
		t.Fatal("args should contain --model claude-sonnet-4")
	}

	// Give the background goroutine time to write the exit file, then clean up.
	time.Sleep(50 * time.Millisecond)
	_ = os.RemoveAll(dir)
}

func TestClaudeAdapter_Start_StartError(t *testing.T) {
	fr := &fakeRunner{startErr: errFake}
	c := NewClaudeAdapter(WithClaudeRunner(fr), WithClaudePidDir(t.TempDir()))
	_, err := c.Start(context.Background(), rpc.RuntimeStartParams{
		WorktreePath: "/tmp/test",
		Objective:    "test",
	})
	if err == nil {
		t.Fatal("expected error from Start failure")
	}
}

func TestClaudeAdapter_Stop_InvalidPID(t *testing.T) {
	c := NewClaudeAdapter(WithClaudeRunner(&fakeRunner{}))
	err := c.Stop(context.Background(), rpc.RuntimeHandle{PID: 0}, 1*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid PID")
	}
}

func TestClaudeAdapter_Stop_SendsSIGTERM(t *testing.T) {
	fr := &fakeRunner{}
	c := NewClaudeAdapter(WithClaudeRunner(fr), WithClaudePidDir(t.TempDir()))

	err := c.Stop(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_test",
		PID:     999999,
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(fr.signals) == 0 && len(fr.killed) == 0 {
		t.Fatal("expected at least one signal or kill attempt")
	}
}

func TestClaudeAdapter_Inspect_DeadProcess(t *testing.T) {
	c := NewClaudeAdapter(WithClaudePidDir(t.TempDir()))

	status, err := c.Inspect(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_nonexistent",
		PID:     999999,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if status.State == domain.AgentRunning {
		t.Fatal("dead process should not be AgentRunning")
	}
}

func TestClaudeAdapter_Inspect_LiveProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signals work differently on Windows")
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	pid := cmd.Process.Pid
	dir := t.TempDir()
	c := NewClaudeAdapter(WithClaudePidDir(dir))

	status, err := c.Inspect(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_live",
		PID:     pid,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if status.State != domain.AgentRunning {
		t.Fatalf("live process should be AgentRunning, got %s", status.State)
	}
	if status.ExitCode != nil {
		t.Fatalf("running process should have nil ExitCode, got %d", *status.ExitCode)
	}
}

func TestClaudeAdapter_Stop_KillsLiveProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signals work differently on Windows")
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid

	c := NewClaudeAdapter(WithClaudePidDir(t.TempDir()))
	err := c.Stop(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_stop",
		PID:     pid,
	}, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		// Process exited — success.
	case <-time.After(2 * time.Second):
		t.Fatal("process did not exit within 2s after Stop")
	}
}

func TestClaudeAdapter_PidfileExitfileLifecycle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signals work differently on Windows")
	}
	dir, err := os.MkdirTemp("", "pantheon-claude-lifecycle-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	c := NewClaudeAdapter(WithClaudePidDir(dir))

	// Spawn a real sleep process as the "agent".
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	agentID := "agt_lifecycle"

	// Write pidfile manually (simulating what Start does).
	pidfile := filepath.Join(dir, "pantheon-claude-"+agentID+".pid")
	if err := os.WriteFile(pidfile, []byte(itoa(pid)), 0o600); err != nil {
		t.Fatalf("write pidfile: %v", err)
	}

	// Inspect should report running.
	status, err := c.Inspect(context.Background(), rpc.RuntimeHandle{
		AgentID: agentID,
		PID:     pid,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if status.State != domain.AgentRunning {
		t.Fatalf("expected AgentRunning, got %s", status.State)
	}

	// Stop should kill the process and clean up pidfile.
	err = c.Stop(context.Background(), rpc.RuntimeHandle{
		AgentID: agentID,
		PID:     pid,
	}, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Wait for process to exit.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("process did not exit within 2s after Stop")
	}

	// Pidfile should be cleaned up.
	if _, err := os.Stat(pidfile); !os.IsNotExist(err) {
		t.Fatalf("pidfile should be removed after Stop, got err=%v", err)
	}
	_ = os.RemoveAll(dir)
}

func TestDeriveSessionID(t *testing.T) {
	id1 := deriveSessionID("/tmp/worktree-1")
	id2 := deriveSessionID("/tmp/worktree-1")
	id3 := deriveSessionID("/tmp/worktree-2")
	if id1 != id2 {
		t.Fatal("same path should produce same session ID")
	}
	if id1 == id3 {
		t.Fatal("different paths should produce different session IDs")
	}
	if !startsWith(id1, "claude-") {
		t.Fatalf("session ID should start with claude-, got %s", id1)
	}
}

// itoa is a local helper to avoid importing strconv in the test file.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
