// Package runtime implements the RuntimeAdapter interface for Pantheon Phase 1.
//
// The Devin adapter spawns the `devin` CLI in a git worktree with a clean
// session, a scoped prompt derived from the task objective, and a bounded
// budget. Process state is derived from the OS (PID liveness via signal 0,
// exit status from a pidfile), never from tmux pane text (ADR-0006).
package runtime

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

// CommandRunner abstracts exec.CommandContext for testability.
type CommandRunner interface {
	Start(cmd *exec.Cmd) error
	Wait(cmd *exec.Cmd) error
	Signal(pid int, sig os.Signal) error
	FindProcess(pid int) (*os.Process, error)
	Kill(pid int) error
}

// DefaultRunner uses os/exec and os package functions.
type DefaultRunner struct{}

func (DefaultRunner) Start(cmd *exec.Cmd) error { return cmd.Start() }
func (DefaultRunner) Wait(cmd *exec.Cmd) error  { return cmd.Wait() }
func (DefaultRunner) Signal(pid int, sig os.Signal) error {
	return syscall.Kill(pid, sig.(syscall.Signal))
}
func (DefaultRunner) FindProcess(pid int) (*os.Process, error) { return os.FindProcess(pid) }
func (DefaultRunner) Kill(pid int) error                       { return syscall.Kill(pid, syscall.SIGKILL) }

// DevinAdapter implements rpc.RuntimeAdapter using the devin CLI.
type DevinAdapter struct {
	// DevinBin is the path to the devin executable. Defaults to "devin".
	DevinBin string

	// PidDir is where pidfiles are stored. Defaults to os.TempDir().
	PidDir string

	// Runner is injectable for tests. Defaults to DefaultRunner.
	Runner CommandRunner

	// Model specifies the Devin model to use. If empty, devin's default.
	Model string

	// PermissionMode is passed as --permission-mode. Defaults to "dangerous".
	PermissionMode string
}

// NewDevinAdapter creates a DevinAdapter with sensible defaults.
// The devin binary is resolved in this order:
//  1. PANTHEON_DEVIN_BIN env var (explicit override)
//  2. "devin" on PATH (normal case)
//  3. ~/.local/bin/devin (common install location, not always on SSH PATH)
func NewDevinAdapter(opts ...Option) *DevinAdapter {
	devinBin := "devin"
	if v := os.Getenv("PANTHEON_DEVIN_BIN"); v != "" {
		devinBin = v
	} else if _, err := exec.LookPath("devin"); err != nil {
		// devin not on PATH — try ~/.local/bin/devin
		if home, hErr := os.UserHomeDir(); hErr == nil {
			candidate := filepath.Join(home, ".local", "bin", "devin")
			if _, sErr := os.Stat(candidate); sErr == nil {
				devinBin = candidate
			}
		}
	}
	d := &DevinAdapter{
		DevinBin:       devinBin,
		PidDir:         os.TempDir(),
		Runner:         DefaultRunner{},
		PermissionMode: "dangerous",
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Option configures a DevinAdapter.
type Option func(*DevinAdapter)

// WithDevinBin sets the devin executable path.
func WithDevinBin(bin string) Option {
	return func(d *DevinAdapter) { d.DevinBin = bin }
}

// WithPidDir sets the pidfile directory.
func WithPidDir(dir string) Option {
	return func(d *DevinAdapter) { d.PidDir = dir }
}

// WithRunner sets a custom CommandRunner (for tests).
func WithRunner(r CommandRunner) Option {
	return func(d *DevinAdapter) { d.Runner = r }
}

// WithModel sets the Devin model.
func WithModel(model string) Option {
	return func(d *DevinAdapter) { d.Model = model }
}

// WithPermissionMode sets the --permission-mode flag.
func WithPermissionMode(mode string) Option {
	return func(d *DevinAdapter) { d.PermissionMode = mode }
}

// Start spawns a devin CLI process in the worktree with a scoped prompt.
// The process runs asynchronously; Start returns immediately with a handle
// containing the PID and a generated agent ID.
func (d *DevinAdapter) Start(ctx context.Context, p rpc.RuntimeStartParams) (rpc.RuntimeHandle, error) {
	if p.WorktreePath == "" {
		return rpc.RuntimeHandle{}, domain.ErrInvalidInput("worktree_path is required")
	}
	if p.Objective == "" {
		return rpc.RuntimeHandle{}, domain.ErrInvalidInput("objective is required")
	}

	agentID, err := domain.NewID("agt_")
	if err != nil {
		return rpc.RuntimeHandle{}, domain.ErrInternal("agent id: " + err.Error())
	}

	args, err := d.buildArgs(p, agentID)
	if err != nil {
		return rpc.RuntimeHandle{}, domain.ErrInternal("build args: " + err.Error())
	}
	log.Printf("devin-adapter: starting devin bin=%s args=%v dir=%s agent=%s", d.DevinBin, args, p.WorktreePath, agentID)
	cmd := exec.CommandContext(ctx, d.DevinBin, args...)
	cmd.Dir = p.WorktreePath
	// stdout/stderr go to a log file (not inherited) so that cmd.Wait()
	// does not block on child processes (e.g. devin acp) that inherit
	// the file descriptors. stdin is not connected; devin runs
	// autonomously with the prompt.
	logFile, err := os.OpenFile(d.logFilePath(agentID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return rpc.RuntimeHandle{}, domain.ErrInternal("open log file: " + err.Error())
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil

	// Set process group so we can signal the whole group on stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := d.Runner.Start(cmd); err != nil {
		_ = logFile.Close()
		return rpc.RuntimeHandle{}, domain.ErrInternal("start devin: " + err.Error())
	}

	pid := cmd.Process.Pid

	// Write pidfile for Inspect to read later. The first line is the PID.
	// When the process exits, the exit code is written to a separate
	// exitfile to avoid racing with the pidfile write.
	pidfile := d.pidfilePath(agentID)
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(pid)), 0o600); err != nil {
		// Non-fatal: Inspect can still use the in-memory PID.
		_ = err
	}

	// Wait for the process in a goroutine so Start doesn't block.
	// The exit code is recorded in a separate exitfile for Inspect to read.
	go func() {
		_ = d.Runner.Wait(cmd)
		_ = logFile.Close()
		exitCode := 0
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		log.Printf("devin-adapter: devin exited agent=%s pid=%d exitCode=%d", agentID, pid, exitCode)
		exitfile := d.exitfilePath(agentID)
		_ = os.WriteFile(exitfile, []byte(strconv.Itoa(exitCode)), 0o600)
	}()

	return rpc.RuntimeHandle{
		AgentID:   agentID,
		PID:       pid,
		SessionID: p.SessionID,
	}, nil
}

// buildArgs constructs the devin CLI arguments from the start params.
// The prompt is written to a temp file and passed via --prompt-file,
// because the devin CLI does not support --prompt as a direct argument.
func (d *DevinAdapter) buildArgs(p rpc.RuntimeStartParams, agentID string) ([]string, error) {
	args := []string{}
	if d.Model != "" {
		args = append(args, "--model", d.Model)
	}
	if d.PermissionMode != "" {
		args = append(args, "--permission-mode", d.PermissionMode)
	}
	// Write the prompt to a file and use --prompt-file.
	prompt := buildPrompt(p)
	promptFile := d.promptFilePath(agentID)
	if err := os.WriteFile(promptFile, []byte(prompt), 0o600); err != nil {
		return nil, fmt.Errorf("write prompt file: %w", err)
	}
	// -p (print mode) runs devin non-interactively: process the prompt and exit.
	// Without it devin tries to enter the TUI, which fails in non-TTY (daemon) contexts.
	args = append(args, "-p", "--prompt-file", promptFile)
	return args, nil
}

// progressFileName is the convention file agent writes to track sub-task
// progress. Pantheon reads it when building continuation prompts.
const progressFileName = "PANTHEON_PROGRESS.md"

// buildPrompt creates a scoped prompt from the task objective and scope.
// If the worktree already contains a PANTHEON_PROGRESS.md (continuation
// scenario), its content is prepended so the agent resumes from where
// the previous run left off.
func buildPrompt(p rpc.RuntimeStartParams) string {
	var b strings.Builder

	// Continuation: if a progress file exists in the worktree, inject it.
	if p.WorktreePath != "" {
		if prog, err := os.ReadFile(filepath.Join(p.WorktreePath, progressFileName)); err == nil && len(prog) > 0 {
			b.WriteString("You are continuing a previous run. A PANTHEON_PROGRESS.md file")
			b.WriteString(" from the previous run is in the worktree root. Here is its content:\n\n")
			b.Write(prog)
			b.WriteString("\n\nContinue from where the previous run left off. Do NOT redo")
			b.WriteString(" completed subtasks. Pick up the first remaining subtask.\n\n---\n\n")
		}
	}

	b.WriteString(p.Objective)
	if p.RunID != "" {
		fmt.Fprintf(&b, "\n\nRun ID: %s", p.RunID)
	}
	if p.TaskID != "" {
		fmt.Fprintf(&b, "\nTask ID: %s", p.TaskID)
	}
	if len(p.Scope.Include) > 0 {
		fmt.Fprintf(&b, "\nScope includes: %s", strings.Join(p.Scope.Include, ", "))
	}
	if len(p.Scope.Exclude) > 0 {
		fmt.Fprintf(&b, "\nScope excludes: %s", strings.Join(p.Scope.Exclude, ", "))
	}
	if p.Budget > 0 {
		fmt.Fprintf(&b, "\nBudget: %s", formatBudget(p.Budget))
	}

	// Progress tracking instruction: agent must maintain PANTHEON_PROGRESS.md.
	b.WriteString(progressInstruction)

	return b.String()
}

// progressInstruction is appended to every prompt so the agent maintains
// a structured progress file that Pantheon can read for continuations.
const progressInstruction = `

--- Progress tracking (mandatory) ---

Before starting work, create PANTHEON_PROGRESS.md in the worktree root
with this format:

## Subtasks
- [ ] <subtask 1 description>
- [ ] <subtask 2 description>
- [ ] <subtask 3 description>

## Notes
<any context useful for a continuation: what was tried, what worked, what failed>

As you complete each subtask, update the file:
- Change [ ] to [x] for completed subtasks
- Add notes about decisions made or approaches tried
- If you discover new subtasks, add them to the list

If you run out of budget or context, the file must reflect the current
state so a continuation run can pick up from here. Do NOT delete the
file when you finish — leave it with all subtasks checked off.`

// formatBudget converts a time.Duration into a concise human-readable
// string suitable for embedding in an agent prompt. It collapses the
// duration to its two most significant non-zero units (e.g. "1h 30m",
// "15m", "45s", "500ms"). A zero or negative duration returns "0s".
func formatBudget(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	}
	hours := int64(d / time.Hour)
	d -= time.Duration(hours) * time.Hour
	minutes := int64(d / time.Minute)
	d -= time.Duration(minutes) * time.Minute
	seconds := int64(d / time.Second)

	var parts []string
	switch {
	case hours > 0:
		parts = append(parts, strconv.FormatInt(hours, 10)+"h")
		if minutes > 0 {
			parts = append(parts, strconv.FormatInt(minutes, 10)+"m")
		}
	case minutes > 0:
		parts = append(parts, strconv.FormatInt(minutes, 10)+"m")
		if seconds > 0 {
			parts = append(parts, strconv.FormatInt(seconds, 10)+"s")
		}
	default:
		parts = append(parts, strconv.FormatInt(seconds, 10)+"s")
	}
	return strings.Join(parts, " ")
}

// Stop sends SIGTERM to the process, waits for grace duration, then SIGKILL.
func (d *DevinAdapter) Stop(ctx context.Context, h rpc.RuntimeHandle, grace time.Duration) error {
	if h.PID <= 0 {
		return domain.ErrInvalidInput("invalid PID")
	}

	// Send SIGTERM directly to the process. We use the PID, not the
	// process group, to avoid accidentally signaling unrelated processes
	// that happen to share the same pgid (e.g. the test process itself).
	// The Devin adapter sets Setpgid=true on Start, so the devin process
	// is in its own group, but Stop is called with just a PID from the
	// handle — we don't track the pgid separately. Signaling the PID
	// directly is safe and sufficient for Phase 1 single-worker.
	if err := d.Runner.Signal(h.PID, syscall.SIGTERM); err != nil {
		// Process may have already exited — not an error.
		if isNoSuchProcess(err) {
			d.cleanupPidfile(h.AgentID)
			return nil
		}
		return domain.ErrInternal("SIGTERM: " + err.Error())
	}

	// Wait for grace period.
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	// Check if process is still alive.
	alive, err := d.isAlive(h.PID)
	if err != nil {
		return domain.ErrInternal("check liveness: " + err.Error())
	}
	if !alive {
		d.cleanupPidfile(h.AgentID)
		return nil
	}

	// SIGKILL the process.
	if err := d.Runner.Kill(h.PID); err != nil {
		if isNoSuchProcess(err) {
			d.cleanupPidfile(h.AgentID)
			return nil
		}
		return domain.ErrInternal("SIGKILL: " + err.Error())
	}
	d.cleanupPidfile(h.AgentID)
	return nil
}

// isNoSuchProcess returns true if the error indicates the process doesn't exist.
func isNoSuchProcess(err error) bool {
	if err == nil {
		return false
	}
	return err == os.ErrProcessDone || strings.Contains(err.Error(), "no such process")
}

// Inspect checks PID liveness and reads exit status from the pidfile.
func (d *DevinAdapter) Inspect(ctx context.Context, h rpc.RuntimeHandle) (rpc.RuntimeStatus, error) {
	alive, err := d.isAlive(h.PID)
	if err != nil {
		return rpc.RuntimeStatus{}, domain.ErrInternal("inspect liveness: " + err.Error())
	}

	if alive {
		return rpc.RuntimeStatus{
			State:    domain.AgentRunning,
			ExitCode: nil,
		}, nil
	}

	// Process not alive: read exit code from pidfile.
	exitCode := d.readExitCode(h.AgentID)
	return rpc.RuntimeStatus{
		State:    domain.AgentExited,
		ExitCode: exitCode,
	}, nil
}

// isAlive checks if a PID is alive via signal 0.
func (d *DevinAdapter) isAlive(pid int) (bool, error) {
	proc, err := d.Runner.FindProcess(pid)
	if err != nil {
		return false, err
	}
	// signal 0 checks existence without actually sending a signal.
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		// ESRCH = no such process (not alive).
		// EPERM = permission denied (alive but not ours).
		if err == os.ErrProcessDone {
			return false, nil
		}
		// On Linux, signal 0 to a dead process returns "os: process already finished".
		return false, nil
	}
	return true, nil
}

// pidfilePath returns the path to the pidfile for an agent.
func (d *DevinAdapter) pidfilePath(agentID string) string {
	return filepath.Join(d.PidDir, fmt.Sprintf("pantheon-agent-%s.pid", agentID))
}

// promptFilePath returns the path to the prompt file for an agent.
func (d *DevinAdapter) promptFilePath(agentID string) string {
	return filepath.Join(d.PidDir, fmt.Sprintf("pantheon-agent-%s.prompt", agentID))
}

// readExitCode reads the exit code from the exitfile.
func (d *DevinAdapter) readExitCode(agentID string) *int {
	data, err := os.ReadFile(d.exitfilePath(agentID))
	if err != nil {
		return nil
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return nil
	}
	return &code
}

// exitfilePath returns the path to the exitfile for an agent.
func (d *DevinAdapter) exitfilePath(agentID string) string {
	return filepath.Join(d.PidDir, fmt.Sprintf("pantheon-agent-%s.exit", agentID))
}

// logFilePath returns the path to the log file for an agent's stdout/stderr.
func (d *DevinAdapter) logFilePath(agentID string) string {
	return filepath.Join(d.PidDir, fmt.Sprintf("pantheon-agent-%s.log", agentID))
}

// cleanupPidfile removes the pidfile, exitfile, prompt file, and log file.
func (d *DevinAdapter) cleanupPidfile(agentID string) {
	_ = os.Remove(d.pidfilePath(agentID))
	_ = os.Remove(d.exitfilePath(agentID))
	_ = os.Remove(d.promptFilePath(agentID))
	_ = os.Remove(d.logFilePath(agentID))
}
