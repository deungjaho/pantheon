// CodexAdapter implements rpc.RuntimeAdapter using the Codex CLI.
//
// The adapter spawns the `codex` CLI in a git worktree with a scoped prompt
// derived from the task objective. It mirrors the DevinAdapter's pidfile/
// exitfile lifecycle: process state is derived from the OS (PID liveness via
// signal 0, exit status from an exitfile), never from tmux pane text (ADR-0006).
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

// CodexAdapter implements rpc.RuntimeAdapter using the Codex CLI.
type CodexAdapter struct {
	// CodexBin is the path to the codex executable. Defaults to "codex".
	CodexBin string

	// PidDir is where pidfiles are stored. Defaults to os.TempDir().
	PidDir string

	// Runner is injectable for tests. Defaults to DefaultRunner.
	Runner CommandRunner

	// Model specifies the Codex model to use. If empty, codex's default.
	Model string

	// PermissionMode controls the --dangerous-bypass-approvals-and-sandbox
	// flag. Defaults to "dangerous" which enables the flag.
	PermissionMode string
}

// CodexOption configures a CodexAdapter.
type CodexOption func(*CodexAdapter)

// WithCodexBin sets the codex executable path.
func WithCodexBin(bin string) CodexOption {
	return func(c *CodexAdapter) { c.CodexBin = bin }
}

// WithCodexPidDir sets the pidfile directory.
func WithCodexPidDir(dir string) CodexOption {
	return func(c *CodexAdapter) { c.PidDir = dir }
}

// WithCodexRunner sets a custom CommandRunner (for tests).
func WithCodexRunner(r CommandRunner) CodexOption {
	return func(c *CodexAdapter) { c.Runner = r }
}

// WithCodexModel sets the Codex model.
func WithCodexModel(model string) CodexOption {
	return func(c *CodexAdapter) { c.Model = model }
}

// WithCodexPermissionMode sets the permission mode.
// A non-empty, non-"default" mode enables --dangerous-bypass-approvals-and-sandbox.
func WithCodexPermissionMode(mode string) CodexOption {
	return func(c *CodexAdapter) { c.PermissionMode = mode }
}

// NewCodexAdapter creates a CodexAdapter with sensible defaults.
// The codex binary is resolved in this order:
//  1. PANTHEON_CODEX_BIN env var (explicit override)
//  2. "codex" on PATH (normal case)
//  3. ~/.local/bin/codex (common install location, not always on SSH PATH)
func NewCodexAdapter(opts ...CodexOption) *CodexAdapter {
	codexBin := "codex"
	if v := os.Getenv("PANTHEON_CODEX_BIN"); v != "" {
		codexBin = v
	} else if _, err := exec.LookPath("codex"); err != nil {
		if home, hErr := os.UserHomeDir(); hErr == nil {
			candidate := filepath.Join(home, ".local", "bin", "codex")
			if _, sErr := os.Stat(candidate); sErr == nil {
				codexBin = candidate
			}
		}
	}
	c := &CodexAdapter{
		CodexBin:       codexBin,
		PidDir:         os.TempDir(),
		Runner:         DefaultRunner{},
		PermissionMode: "dangerous",
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start spawns a codex CLI process in the worktree with a scoped prompt.
// The process runs asynchronously; Start returns immediately with a handle
// containing the PID and a generated agent ID.
func (c *CodexAdapter) Start(ctx context.Context, p rpc.RuntimeStartParams) (rpc.RuntimeHandle, error) {
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
	log.Printf("codex-adapter: starting codex bin=%s args=%v dir=%s agent=%s", c.CodexBin, args, p.WorktreePath, agentID)
	cmd := exec.CommandContext(ctx, c.CodexBin, args...)
	cmd.Dir = p.WorktreePath
	// stdout/stderr go to a log file (not inherited) so that cmd.Wait()
	// does not block on child processes that inherit the file descriptors.
	// stdin is not connected; codex runs autonomously with the prompt.
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
		return rpc.RuntimeHandle{}, domain.ErrInternal("start codex: " + err.Error())
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
		log.Printf("codex-adapter: codex exited agent=%s pid=%d exitCode=%d", agentID, pid, exitCode)
		exitfile := c.exitfilePath(agentID)
		_ = os.WriteFile(exitfile, []byte(strconv.Itoa(exitCode)), 0o600)
	}()

	return rpc.RuntimeHandle{
		AgentID:   agentID,
		PID:       pid,
		SessionID: sessionID,
	}, nil
}

// buildArgs constructs the codex CLI arguments from the start params.
// The prompt is written to a temp file (for inspection/debugging) and passed
// as a positional argument, because the codex CLI accepts the objective as
// a trailing positional argument.
func (c *CodexAdapter) buildArgs(p rpc.RuntimeStartParams, agentID string) ([]string, error) {
	args := []string{}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	// --dangerous-bypass-approvals-and-sandbox enables autonomous mode.
	if c.PermissionMode != "" && c.PermissionMode != "default" {
		args = append(args, "--dangerous-bypass-approvals-and-sandbox")
	}
	// Write the prompt to a file for inspection/debugging.
	prompt := buildPrompt(p)
	promptFile := c.promptFilePath(agentID)
	if err := os.WriteFile(promptFile, []byte(prompt), 0o600); err != nil {
		return nil, fmt.Errorf("write prompt file: %w", err)
	}
	// The objective/prompt is passed as a trailing positional argument.
	args = append(args, prompt)
	return args, nil
}

// Stop sends SIGTERM to the process, waits for grace duration, then SIGKILL.
func (c *CodexAdapter) Stop(ctx context.Context, h rpc.RuntimeHandle, grace time.Duration) error {
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
func (c *CodexAdapter) Inspect(ctx context.Context, h rpc.RuntimeHandle) (rpc.RuntimeStatus, error) {
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
func (c *CodexAdapter) isAlive(pid int) (bool, error) {
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
func (c *CodexAdapter) pidfilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-codex-%s.pid", agentID))
}

// promptFilePath returns the path to the prompt file for an agent.
func (c *CodexAdapter) promptFilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-codex-%s.prompt", agentID))
}

// readExitCode reads the exit code from the exitfile.
func (c *CodexAdapter) readExitCode(agentID string) *int {
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
func (c *CodexAdapter) exitfilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-codex-%s.exit", agentID))
}

// logFilePath returns the path to the log file for an agent's stdout/stderr.
func (c *CodexAdapter) logFilePath(agentID string) string {
	return filepath.Join(c.PidDir, fmt.Sprintf("pantheon-codex-%s.log", agentID))
}

// cleanupPidfile removes the pidfile, exitfile, prompt file, and log file.
func (c *CodexAdapter) cleanupPidfile(agentID string) {
	_ = os.Remove(c.pidfilePath(agentID))
	_ = os.Remove(c.exitfilePath(agentID))
	_ = os.Remove(c.promptFilePath(agentID))
	_ = os.Remove(c.logFilePath(agentID))
}
