// Package workspace manages git worktree lifecycle for Pantheon tasks.
//
// Invariants:
//   - Never mutates the user's main working tree.
//   - One worktree per task; never shared.
//   - Worktree path is derived from task ID, not user input.
//   - Dirty base repos return SNAPSHOT_REQUIRED; Pantheon never auto-commits.
//   - Cleanup is bounded: a retention window keeps recent worktrees; older
//     ones are removed.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// CommandRunner abstracts exec.CommandContext for testability.
type CommandRunner interface {
	Run(ctx context.Context, dir string, name string, args ...string) (stdout, stderr string, exitCode int, err error)
}

// DefaultRunner uses os/exec.
type DefaultRunner struct{}

func (DefaultRunner) Run(ctx context.Context, dir string, name string, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return stdout.String(), stderr.String(), -1, err
		}
	}
	return stdout.String(), stderr.String(), exitCode, nil
}

// Manager creates and cleans up git worktrees for tasks.
type Manager struct {
	runner    CommandRunner
	baseDir   string // parent directory for worktrees
	retention time.Duration
}

// Option configures a Manager.
type Option func(*Manager)

func WithRunner(r CommandRunner) Option    { return func(m *Manager) { m.runner = r } }
func WithBaseDir(d string) Option          { return func(m *Manager) { m.baseDir = d } }
func WithRetention(d time.Duration) Option { return func(m *Manager) { m.retention = d } }

// NewManager creates a workspace Manager. Defaults: DefaultRunner,
// $PANTHEON_HOME/worktrees, 24h retention.
func NewManager(opts ...Option) *Manager {
	m := &Manager{
		runner:    DefaultRunner{},
		baseDir:   defaultBaseDir(),
		retention: 24 * time.Hour,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

func defaultBaseDir() string {
	home := os.Getenv("PANTHEON_HOME")
	if home == "" {
		home = filepath.Join(os.Getenv("HOME"), ".local", "share", "pantheon")
	}
	return filepath.Join(home, "worktrees")
}

// ResolveBaseCommit resolves a git ref to a concrete commit SHA. Returns
// BASE_REF_MISSING if the ref does not exist.
func (m *Manager) ResolveBaseCommit(ctx context.Context, repoPath, baseRef string) (string, error) {
	if repoPath == "" {
		return "", domain.ErrInvalidInput("repo_path is required")
	}
	if baseRef == "" {
		return "", domain.ErrInvalidInput("base_ref is required")
	}
	stdout, _, code, err := m.runner.Run(ctx, repoPath, "git", "rev-parse", "--verify", baseRef+"^{commit}")
	if err != nil {
		return "", domain.ErrInternal(fmt.Sprintf("git rev-parse: %v", err))
	}
	if code != 0 {
		return "", domain.ErrBaseRefMissing(fmt.Sprintf("base ref %q does not resolve in %s", baseRef, repoPath))
	}
	return strings.TrimSpace(stdout), nil
}

// IsDirty reports whether the repo's working tree has uncommitted changes.
func (m *Manager) IsDirty(ctx context.Context, repoPath string) (bool, error) {
	_, _, code, err := m.runner.Run(ctx, repoPath, "git", "diff", "--quiet", "HEAD")
	if err != nil {
		return false, domain.ErrInternal(fmt.Sprintf("git diff: %v", err))
	}
	// exit 0 = clean, exit 1 = dirty, other = error
	if code == 0 {
		return false, nil
	}
	if code == 1 {
		return true, nil
	}
	return false, domain.ErrInternal(fmt.Sprintf("git diff exited %d", code))
}

// PrepareWorktree creates an independent git worktree at the base commit for
// the given task. If the repo is dirty, returns SNAPSHOT_REQUIRED.
//
// The worktree path is derived from the task ID, never from user input.
func (m *Manager) PrepareWorktree(ctx context.Context, repoPath, baseCommit, taskID string) (string, error) {
	if repoPath == "" || baseCommit == "" || taskID == "" {
		return "", domain.ErrInvalidInput("repo_path, base_commit, task_id are required")
	}

	// Check dirty state. Pantheon never auto-commits the user's tree.
	dirty, err := m.IsDirty(ctx, repoPath)
	if err != nil {
		return "", err
	}
	if dirty {
		return "", domain.ErrSnapshotRequired(
			"repo working tree is dirty; snapshot or commit your changes before starting a managed run",
		)
	}

	worktreePath := filepath.Join(m.baseDir, taskID)
	if err := os.MkdirAll(m.baseDir, 0o700); err != nil {
		return "", domain.ErrInternal(fmt.Sprintf("mkdir worktree base: %v", err))
	}

	// Check if worktree path already exists (shouldn't with crypto/rand IDs,
	// but be explicit).
	if _, err := os.Stat(worktreePath); err == nil {
		return "", domain.ErrWorktreeConflict(fmt.Sprintf("worktree path already exists: %s", worktreePath))
	}

	_, stderr, code, err := m.runner.Run(ctx, repoPath,
		"git", "worktree", "add", "--detach", worktreePath, baseCommit)
	if err != nil {
		return "", domain.ErrInternal(fmt.Sprintf("git worktree add: %v", err))
	}
	if code != 0 {
		return "", domain.ErrInternal(fmt.Sprintf("git worktree add failed (exit %d): %s", code, stderr))
	}

	return worktreePath, nil
}

// CleanupWorktree removes a worktree. This is the explicit cleanup path:
// --force is correct here because the caller has decided the worktree's
// task is done. Normal operation never calls this method.
//
// Safety invariants:
//   - worktreePath is validated as a cleaned, absolute path contained under
//     m.baseDir before any destructive action. Out-of-tree, empty, ..
//     traversal, and non-Pantheon paths are rejected with ErrInvalidInput.
//   - git worktree remove is run from the owning repo (the git common dir),
//     not from inside the worktree being removed.
//   - No os.RemoveAll fallback: if git worktree remove fails, the real error
//     is surfaced. State is never asserted as clean on unknown outcome.
//   - Removal is verified via git worktree list so no orphaned metadata
//     remains.
func (m *Manager) CleanupWorktree(ctx context.Context, worktreePath string) error {
	clean, err := m.validateWorktreePath(worktreePath)
	if err != nil {
		return err
	}

	// F5: resolve the owning repo's common dir from the worktree, then run
	// git worktree remove from there — not from inside the worktree itself.
	// This is non-destructive discovery (rev-parse does not mutate state).
	commonDir, _, code, err := m.runner.Run(ctx, clean, "git", "rev-parse", "--git-common-dir")
	if err != nil {
		return domain.ErrInternal(fmt.Sprintf("resolve git common dir: %v", err))
	}
	if code != 0 {
		return domain.ErrInvalidInput(fmt.Sprintf("not a git worktree: %s", clean))
	}
	commonDir = filepath.Clean(strings.TrimSpace(commonDir))

	// Run git worktree remove from the owning repo.
	_, stderr, code, err := m.runner.Run(ctx, commonDir, "git", "worktree", "remove", "--force", clean)
	if err != nil {
		return domain.ErrInternal(fmt.Sprintf("git worktree remove: %v", err))
	}
	if code != 0 {
		// No os.RemoveAll fallback. Surface the real error so the registry
		// and journal never record a clean cleanup on unknown state.
		return domain.ErrInternal(fmt.Sprintf("git worktree remove failed (exit %d): %s", code, strings.TrimSpace(stderr)))
	}

	// F4: verify the worktree is gone from git worktree list so no orphaned
	// metadata remains under $GIT_COMMON_DIR/worktrees/<name>.
	if err := m.verifyWorktreeRemoved(ctx, commonDir, clean); err != nil {
		return err
	}
	return nil
}

// validateWorktreePath checks that worktreePath is a cleaned, absolute path
// contained under m.baseDir. It rejects empty input, relative paths, ..
// traversal, and any path that escapes the worktree base directory. No
// destructive action may run before this check passes.
func (m *Manager) validateWorktreePath(worktreePath string) (string, error) {
	if worktreePath == "" {
		return "", domain.ErrInvalidInput("worktree_path is required")
	}
	clean := filepath.Clean(worktreePath)
	if !filepath.IsAbs(clean) {
		return "", domain.ErrInvalidInput(fmt.Sprintf("worktree_path must be absolute: %s", worktreePath))
	}
	if clean == "/" {
		return "", domain.ErrInvalidInput("worktree_path must not be root")
	}
	base := filepath.Clean(m.baseDir)
	rel, err := filepath.Rel(base, clean)
	if err != nil {
		return "", domain.ErrInvalidInput(fmt.Sprintf("worktree_path outside base dir: %s", worktreePath))
	}
	if rel == "." || strings.HasPrefix(rel, "..") {
		return "", domain.ErrInvalidInput(fmt.Sprintf("worktree_path outside base dir: %s", worktreePath))
	}
	return clean, nil
}

// verifyWorktreeRemoved confirms that clean no longer appears in
// git worktree list, so no orphaned administrative metadata remains.
func (m *Manager) verifyWorktreeRemoved(ctx context.Context, commonDir, clean string) error {
	stdout, _, code, err := m.runner.Run(ctx, commonDir, "git", "worktree", "list", "--porcelain")
	if err != nil {
		return domain.ErrInternal(fmt.Sprintf("git worktree list: %v", err))
	}
	if code != 0 {
		return domain.ErrInternal(fmt.Sprintf("git worktree list exited %d", code))
	}
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(line, "worktree ") {
			if filepath.Clean(strings.TrimPrefix(line, "worktree ")) == clean {
				return domain.ErrInternal(fmt.Sprintf("worktree still registered after remove: %s", clean))
			}
		}
	}
	return nil
}

// CleanupExpired removes worktrees older than the retention window. Returns
// the list of removed paths and an aggregated error joining all per-worktree
// cleanup failures. Errors are never silently skipped: if any CleanupWorktree
// call fails, the error is collected and surfaced to the caller.
func (m *Manager) CleanupExpired(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, domain.ErrInternal(fmt.Sprintf("read worktree dir: %v", err))
	}
	var removed []string
	var errs []error
	cutoff := time.Now().Add(-m.retention)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			errs = append(errs, fmt.Errorf("stat %s: %w", entry.Name(), err))
			continue
		}
		if info.ModTime().Before(cutoff) {
			path := filepath.Join(m.baseDir, entry.Name())
			if err := m.CleanupWorktree(ctx, path); err != nil {
				errs = append(errs, fmt.Errorf("cleanup %s: %w", path, err))
				continue
			}
			removed = append(removed, path)
		}
	}
	return removed, errors.Join(errs...)
}

// ListWorktrees returns metadata about existing worktrees.
type WorktreeInfo struct {
	Path      string
	TaskID    string
	CreatedAt time.Time
}

func (m *Manager) ListWorktrees() ([]WorktreeInfo, error) {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, domain.ErrInternal(fmt.Sprintf("read worktree dir: %v", err))
	}
	var out []WorktreeInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, WorktreeInfo{
			Path:      filepath.Join(m.baseDir, entry.Name()),
			TaskID:    entry.Name(),
			CreatedAt: info.ModTime(),
		})
	}
	return out, nil
}
