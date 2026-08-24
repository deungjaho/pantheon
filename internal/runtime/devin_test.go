package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

// fakeRunner is a test CommandRunner that records calls and simulates
// process lifecycle without actually spawning devin.
type fakeRunner struct {
	startedCmds []*exec.Cmd
	startErr    error
	waitCalled  bool
	signals     []signalRecord
	killed      []int
}

type signalRecord struct {
	pid int
	sig os.Signal
}

func (f *fakeRunner) Start(cmd *exec.Cmd) error {
	if f.startErr != nil {
		return f.startErr
	}
	// Simulate a process by assigning a fake PID.
	// exec.Cmd.Start sets cmd.Process; since we don't call it, we
	// create a fake process with a high PID that won't collide.
	cmd.Process = &os.Process{Pid: 100000 + len(f.startedCmds)}
	f.startedCmds = append(f.startedCmds, cmd)
	return nil
}

func (f *fakeRunner) Wait(cmd *exec.Cmd) error {
	f.waitCalled = true
	return nil
}

func (f *fakeRunner) Signal(pid int, sig os.Signal) error {
	f.signals = append(f.signals, signalRecord{pid: pid, sig: sig})
	return nil
}

func (f *fakeRunner) FindProcess(pid int) (*os.Process, error) {
	return os.FindProcess(pid)
}

func (f *fakeRunner) Kill(pid int) error {
	f.killed = append(f.killed, pid)
	return nil
}

func TestDevinAdapter_Start_RequiresWorktreePath(t *testing.T) {
	d := NewDevinAdapter(WithRunner(&fakeRunner{}))
	_, err := d.Start(context.Background(), rpc.RuntimeStartParams{
		Objective: "test",
	})
	if err == nil {
		t.Fatal("expected error for empty worktree_path")
	}
}

func TestDevinAdapter_Start_RequiresObjective(t *testing.T) {
	d := NewDevinAdapter(WithRunner(&fakeRunner{}))
	_, err := d.Start(context.Background(), rpc.RuntimeStartParams{
		WorktreePath: "/tmp/test",
	})
	if err == nil {
		t.Fatal("expected error for empty objective")
	}
}

func TestDevinAdapter_Start_BuildsScopedPrompt(t *testing.T) {
	fr := &fakeRunner{}
	// Use a non-TempDir directory to avoid cleanup races with the
	// background goroutine that writes the exit file. The directory
	// is created manually and left for the OS to clean up in /tmp.
	dir, err := os.MkdirTemp("", "pantheon-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	d := NewDevinAdapter(
		WithRunner(fr),
		WithPidDir(dir),
		WithModel("glm-5-2"),
	)

	h, err := d.Start(context.Background(), rpc.RuntimeStartParams{
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
	if len(fr.startedCmds) != 1 {
		t.Fatalf("expected 1 started cmd, got %d", len(fr.startedCmds))
	}

	// Verify pidfile was written.
	pidfile := filepath.Join(dir, "pantheon-agent-"+h.AgentID+".pid")
	data, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatalf("pidfile not written: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("pidfile is empty")
	}

	// Give the background goroutine time to write the exit file,
	// then clean up manually (not via t.Cleanup to avoid races).
	time.Sleep(50 * time.Millisecond)
	_ = os.RemoveAll(dir)
}

func TestDevinAdapter_Start_StartError(t *testing.T) {
	fr := &fakeRunner{startErr: errFake}
	d := NewDevinAdapter(WithRunner(fr), WithPidDir(t.TempDir()))
	_, err := d.Start(context.Background(), rpc.RuntimeStartParams{
		WorktreePath: "/tmp/test",
		Objective:    "test",
	})
	if err == nil {
		t.Fatal("expected error from Start failure")
	}
}

func TestDevinAdapter_Stop_InvalidPID(t *testing.T) {
	d := NewDevinAdapter(WithRunner(&fakeRunner{}))
	err := d.Stop(context.Background(), rpc.RuntimeHandle{PID: 0}, 1*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid PID")
	}
}

func TestDevinAdapter_Stop_SendsSIGTERM(t *testing.T) {
	fr := &fakeRunner{}
	d := NewDevinAdapter(WithRunner(fr), WithPidDir(t.TempDir()))

	// Use a PID that won't exist (high number).
	err := d.Stop(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_test",
		PID:     999999,
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Should have attempted SIGTERM (either via pgid or direct).
	if len(fr.signals) == 0 && len(fr.killed) == 0 {
		t.Fatal("expected at least one signal or kill attempt")
	}
}

func TestDevinAdapter_Inspect_DeadProcess(t *testing.T) {
	d := NewDevinAdapter(WithPidDir(t.TempDir()))

	// PID 999999 almost certainly doesn't exist.
	status, err := d.Inspect(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_nonexistent",
		PID:     999999,
	})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	// A non-existent process should be reported as exited (not running).
	if status.State == domain.AgentRunning {
		t.Fatal("dead process should not be AgentRunning")
	}
}

func TestDevinAdapter_Inspect_LiveProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signals work differently on Windows")
	}
	// Spawn a real long-running process.
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
	}()

	pid := cmd.Process.Pid
	dir := t.TempDir()
	d := NewDevinAdapter(WithPidDir(dir))

	status, err := d.Inspect(context.Background(), rpc.RuntimeHandle{
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

func TestDevinAdapter_Stop_KillsLiveProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signals work differently on Windows")
	}
	// Spawn a real process that will respond to SIGTERM.
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid

	d := NewDevinAdapter(WithPidDir(t.TempDir()))
	err := d.Stop(context.Background(), rpc.RuntimeHandle{
		AgentID: "agt_stop",
		PID:     pid,
	}, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Wait for the process to exit (cmd.Wait will return once it's killed).
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		// Process exited — success.
	case <-time.After(2 * time.Second):
		t.Fatal("process did not exit within 2s after Stop")
	}
}

func TestBuildPrompt_IncludesScope(t *testing.T) {
	prompt := buildPrompt(rpc.RuntimeStartParams{
		RunID:     "run_123",
		TaskID:    "tsk_456",
		Objective: "fix bug",
		Scope: domain.TaskScope{
			Include: []string{"auth/"},
			Exclude: []string{"vendor/"},
		},
		Budget: 1 * time.Hour,
	})
	if !contains(prompt, "fix bug") {
		t.Fatal("prompt should contain objective")
	}
	if !contains(prompt, "run_123") {
		t.Fatal("prompt should contain run ID")
	}
	if !contains(prompt, "auth/") {
		t.Fatal("prompt should contain scope include")
	}
	if !contains(prompt, "vendor/") {
		t.Fatal("prompt should contain scope exclude")
	}
}

func TestBuildArgs_ModelAndPermission(t *testing.T) {
	d := NewDevinAdapter(WithModel("glm-5-2"), WithPermissionMode("dangerous"))
	args, err := d.buildArgs(rpc.RuntimeStartParams{
		Objective: "test objective",
	}, "agt_test")
	if err != nil {
		t.Fatalf("buildArgs error: %v", err)
	}
	if !contains(join(args), "--model") || !contains(join(args), "glm-5-2") {
		t.Fatal("args should contain --model glm-5-2")
	}
	if !contains(join(args), "--permission-mode") || !contains(join(args), "dangerous") {
		t.Fatal("args should contain --permission-mode dangerous")
	}
	if !contains(join(args), "--prompt-file") {
		t.Fatal("args should contain --prompt-file")
	}
	// Verify the prompt file was actually written.
	promptFile := d.promptFilePath("agt_test")
	data, err := os.ReadFile(promptFile)
	if err != nil {
		t.Fatalf("prompt file not written: %v", err)
	}
	if !contains(string(data), "test objective") {
		t.Fatal("prompt file should contain the objective")
	}
	// Cleanup.
	_ = os.Remove(promptFile)
}

func TestFormatBudget(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{name: "zero", d: 0, want: "0s"},
		{name: "negative", d: -5 * time.Second, want: "0s"},
		{name: "milliseconds", d: 500 * time.Millisecond, want: "500ms"},
		{name: "seconds_only", d: 45 * time.Second, want: "45s"},
		{name: "minutes_only", d: 15 * time.Minute, want: "15m"},
		{name: "minutes_and_seconds", d: 15*time.Minute + 30*time.Second, want: "15m 30s"},
		{name: "hours_only", d: 2 * time.Hour, want: "2h"},
		{name: "hours_and_minutes", d: 90 * time.Minute, want: "1h 30m"},
		{name: "task_budget", d: 15 * time.Minute, want: "15m"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatBudget(tc.d)
			if got != tc.want {
				t.Errorf("formatBudget(%v) = %q, want %q", tc.d, got, tc.want)
			}
		})
	}
}

func TestBuildPrompt_Continuation(t *testing.T) {
	// When a PANTHEON_PROGRESS.md exists in the worktree, the prompt
	// should include the continuation preamble and the progress content.
	dir := t.TempDir()
	progressContent := "## Subtasks\n- [x] step 1\n- [ ] step 2\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(progressContent), 0o600); err != nil {
		t.Fatal(err)
	}
	prompt := buildPrompt(rpc.RuntimeStartParams{
		WorktreePath: dir,
		Objective:    "do the thing",
		RunID:        "run_abc",
	})
	if !contains(prompt, "continuing a previous run") {
		t.Fatal("continuation prompt should contain preamble")
	}
	if !contains(prompt, "step 1") || !contains(prompt, "step 2") {
		t.Fatal("continuation prompt should contain progress content")
	}
	if !contains(prompt, "do the thing") {
		t.Fatal("continuation prompt should still contain objective")
	}
}

func TestBuildPrompt_NoContinuation(t *testing.T) {
	// Without a progress file, the prompt should NOT contain the
	// continuation preamble.
	dir := t.TempDir()
	prompt := buildPrompt(rpc.RuntimeStartParams{
		WorktreePath: dir,
		Objective:    "fresh task",
	})
	if contains(prompt, "continuing a previous run") {
		t.Fatal("non-continuation prompt should not contain preamble")
	}
	if !contains(prompt, "fresh task") {
		t.Fatal("prompt should contain objective")
	}
	if !contains(prompt, "PANTHEON_PROGRESS.md") {
		t.Fatal("prompt should contain progress tracking instruction")
	}
}

// --- helpers ---

var errFake = &fakeError{"fake start error"}

type fakeError struct{ msg string }

func (e *fakeError) Error() string { return e.msg }

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func join(args []string) string {
	result := ""
	for _, a := range args {
		result += a + " "
	}
	return result
}

// Ensure syscall import is used (for build constraints).
var _ = syscall.SIGTERM
