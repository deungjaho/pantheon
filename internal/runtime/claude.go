// ClaudeAdapter implements rpc.RuntimeAdapter using the Claude Code CLI.
//
// The adapter spawns the `claude` CLI in a git worktree with a scoped prompt
// derived from the task objective. It mirrors the DevinAdapter's pidfile/
// exitfile lifecycle: process state is derived from the OS (PID liveness via
// signal 0, exit status from an exitfile), never from tmux pane text (ADR-0006).
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// ClaudeAdapter implements rpc.RuntimeAdapter using the Claude Code CLI.
type ClaudeAdapter struct {
	// ClaudeBin is the path to the claude executable. Defaults to "claude".
	ClaudeBin string

	// PidDir is where pidfiles are stored. Defaults to os.TempDir().
	PidDir string

	// Runner is injectable for tests. Defaults to DefaultRunner.
	Runner CommandRunner

	// Model specifies the Claude model to use. If empty, claude's default.
	Model string

	// PermissionMode controls the --dangerously-skip-permissions flag.
	// Defaults to "dangerous" which enables the flag.
	PermissionMode string
}

// ClaudeOption configures a ClaudeAdapter.
type ClaudeOption func(*ClaudeAdapter)

// WithClaudeBin sets the claude executable path.
func WithClaudeBin(bin string) ClaudeOption {
	return func(c *ClaudeAdapter) { c.ClaudeBin = bin }
}

// WithClaudePidDir sets the pidfile directory.
func WithClaudePidDir(dir string) ClaudeOption {
	return func(c *ClaudeAdapter) { c.PidDir = dir }
}

// WithClaudeRunner sets a custom CommandRunner (for tests).
func WithClaudeRunner(r CommandRunner) ClaudeOption {
	return func(c *ClaudeAdapter) { c.Runner = r }
}

// WithClaudeModel sets the Claude model.
func WithClaudeModel(model string) ClaudeOption {
	return func(c *ClaudeAdapter) { c.Model = model }
}

// WithClaudePermissionMode sets the permission mode.
// A non-empty, non-"default" mode enables --dangerously-skip-permissions.
func WithClaudePermissionMode(mode string) ClaudeOption {
	return func(c *ClaudeAdapter) { c.PermissionMode = mode }
}

// NewClaudeAdapter creates a ClaudeAdapter with sensible defaults.
// The claude binary is resolved in this order:
//  1. PANTHEON_CLAUDE_BIN env var (explicit override)
//  2. "claude" on PATH (normal case)
//  3. ~/.local/bin/claude (common install location, not always on SSH PATH)
func NewClaudeAdapter(opts ...ClaudeOption) *ClaudeAdapter {
	claudeBin := "claude"
	if v := os.Getenv("PANTHEON_CLAUDE_BIN"); v != "" {
		claudeBin = v
	} else if _, err := exec.LookPath("claude"); err != nil {
		if home, hErr := os.UserHomeDir(); hErr == nil {
			candidate := filepath.Join(home, ".local", "bin", "claude")
			if _, sErr := os.Stat(candidate); sErr == nil {
				claudeBin = candidate
			}
		}
	}
	c := &ClaudeAdapter{
		ClaudeBin:      claudeBin,
		PidDir:         os.TempDir(),
		Runner:         DefaultRunner{},
		PermissionMode: "dangerous",
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start spawns a claude CLI process in the worktree with a scoped prompt.
// The process runs asynchronously; Start returns immediately with a handle
// containing the PID and a generated agent ID.
func (c *ClaudeAdapter) Start(ctx context.Context, p rpc.RuntimeStartParams) (rpc.RuntimeHandle, error) {
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

	args, err := c.buildArgs(p, agentID)
	if err != nil {
		return rpc.RuntimeHandle{}, domain.ErrInternal("build args: " + err.Error())
	}
	log.Printf("claude-adapter: starting claude bin=%s args=%v dir=%s agent=%s", c.ClaudeBin, args, p.WorktreePath, agentID)
	cmd := exec.CommandContext(ctx, c.ClaudeBin, args...)
	cmd.Dir = p.WorktreePath
	// stdout/stderr go to a log file (not inherited) so that cmd.Wait()
	// does not block on child processes that inherit the file descriptors.
	// stdin is not connected; claude runs autonomously with the prompt.
	logFile, err := os.OpenFile(c.logFilePath(agentID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return rpc.RuntimeHandle{}, domain.ErrInternal("open log file: " + err.Error())
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil

	// Set process group so we can signal the whole group on stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := c.Runner.Start(cmd); err != nil {
		_ = logFile.Close()
		return rpc.RuntimeHandle{}, domain.ErrInternal("start claude: " + err.Error())
	}

	pid := cmd.Process.Pid

	// Write pidfile for Inspect to read later.
	pidfile := c.pidfilePath(agentID)
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(pid)), 0o600); err != nil {
		_ = err // Non-fatal: Inspect can still use the in-memory PID.
	}

	// Derive session ID from worktree path hash if not provided.
	sessionID := p.SessionID
	if sessionID == "" {
		sessionID = deriveSessionID(p.WorktreePath)
	}

	// Wait for the process in a goroutine so Start doesn't block.
	// The exit code is recorded in a separate exitfile for Inspect to read.
	go func() {
		_ = c.Runner.Wait(cmd)
		_ = logFile.Close()
		exitCode := 0
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		log.Printf("claude-adapter: claude exited agent=%s pid=%d exitCode=%d", agentID, pid, exitCode)
		exitfile := c.exitfilePath(agentID)
		_ = os.WriteFile(exitfile, []byte(strconv.Itoa(exitCode)), 0o600)
	}()

	return rpc.RuntimeHandle{
		AgentID:   agentID,
		PID:       pid,
		SessionID: sessionID,
	}, nil
}

// buildArgs constructs the claude CLI arguments from the start params.
// The prompt is written to a temp file (for inspection/debugging) and also
// passed directly via -p, because the claude CLI accepts the prompt as the
// argument to -p (print mode).
func (c *ClaudeAdapter) buildArgs(p rpc.RuntimeStartParams, agentID string) ([]string, error) {
	args := []string{}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	// --dangerously-skip-permissions enables autonomous (non-interactive) mode.
	if c.PermissionMode != "" && c.PermissionMode != "default" {
		args = append(args, "--dangerously-skip-permissions")
	}
	// Write the prompt to a file for inspection/debugging.
	prompt := buildPrompt(p)
	promptFile := c.promptFilePath(agentID)
	if err := os.WriteFile(promptFile, []byte(prompt), 0o600); err != nil {
		return nil, fmt.Errorf("write prompt file: %w", err)
	}
	// -p (print mode) runs claude non-interactively: process the prompt and exit.
	args = append(args, "-p", prompt)
	return args, nil
}

// Stop sends SIGTERM to the process, waits for grace duration, then SIGKILL.
func (c *ClaudeAdapter) Stop(ctx context.Context, h rpc.RuntimeHandle, grace time.Duration) error {
	if h.PID <= 0 {
		return domain.ErrInvalidInput("invalid PID")
	}

	if err := c.Runner.Signal(h.PID, syscall.SIGTERM); err != nil {
		if isNoSuchProcess(err) {
			c.cleanupPidfile(h.AgentID)
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
	alive, err := c.isAlive(h.PID)
	if err != nil {
		return domain.ErrInternal("check liveness: " + err.Error())
	}
	if !alive {
		c.cleanupPidfile(h.AgentID)
		return nil
	}

	// SIGKILL the process.
	if err := c.Runner.Kill(h.PID); err != nil {
		if isNoSuchProcess(err) {
			c.cleanupPidfile(h.AgentID)
			return nil
		}
		return domain.ErrInternal("SIGKILL: " + err.Error())
	}
	c.cleanupPidfile(h.AgentID)
	return nil
}

// Inspect checks PID liveness and reads exit status from the exitfile.
func (c *ClaudeAdapter) Inspect(ctx context.Context, h rpc.RuntimeHandle) (rpc.RuntimeStatus, error) {
	alive, err := c.isAlive(h.PID)
	if err != nil {
		return rpc.RuntimeStatus{}, domain.ErrInternal("inspect liveness: " + err.Error())
	}

	if alive {
		return rpc.RuntimeStatus{
			State:    domain.AgentRunning,
			ExitCode: nil,
		}, nil
	}

	exitCode := c.readExitCode(h.AgentID)
	return rpc.RuntimeStatus{
		State:    domain.AgentExited,
		ExitCode: exitCode,
	}, nil
}

// isAlive checks if a PID is alive via signal 0.
func (c *ClaudeAdapter) isAlive(pid int) (bool, error) {
	proc, err := c.Runner.FindProcess(pid)
	if err != nil {
		return false, err
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		if err == os.ErrProcessDone {
			return false, nil
		}
		return false, nil
	}
	return true, nil
}

// pidfilePath returns the path to the pidfile for an agent.
func (c *ClaudeAdapter) pidfilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-claude-%s.pid", agentID))
}

// promptFilePath returns the path to the prompt file for an agent.
func (c *ClaudeAdapter) promptFilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-claude-%s.prompt", agentID))
}

// readExitCode reads the exit code from the exitfile.
func (c *ClaudeAdapter) readExitCode(agentID string) *int {
	data, err := os.ReadFile(c.exitfilePath(agentID))
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
func (c *ClaudeAdapter) exitfilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-claude-%s.exit", agentID))
}

// logFilePath returns the path to the log file for an agent's stdout/stderr.
func (c *ClaudeAdapter) logFilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-claude-%s.log", agentID))
}

// cleanupPidfile removes the pidfile, exitfile, prompt file, and log file.
func (c *ClaudeAdapter) cleanupPidfile(agentID string) {
	_ = os.Remove(c.pidfilePath(agentID))
	_ = os.Remove(c.exitfilePath(agentID))
	_ = os.Remove(c.promptFilePath(agentID))
	_ = os.Remove(c.logFilePath(agentID))
}

// deriveSessionID computes a stable session ID from the worktree path.
// Claude Code tracks sessions under ~/.claude/projects/ keyed by path; we
// derive a short hash so the handle carries a deterministic identifier.
func deriveSessionID(worktreePath string) string {
	sum := sha256.Sum256([]byte(worktreePath))
	return "claude-" + hex.EncodeToString(sum[:])[:16]
}
