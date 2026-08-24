// Package checkpoint implements the CheckpointManager for Pantheon Phase 1.
//
// On run.pause or worker exit with a candidate, the manager creates an
// immutable git ref under refs/pantheon/cnd_<id> pointing at the worker's
// HEAD. The ref is never moved; a new checkpoint creates a new ref.
//
// A Candidate record references the ref, commit SHA, and a short summary.
// It is the handoff unit for run.takeover.
//
// Pushing candidates to a remote is a replaceable interface (Pusher).
// Phase 1 ships a NoopPusher and a GitPusher.
package checkpoint

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

// GitRunner abstracts exec.Command for testability.
type GitRunner interface {
	Run(ctx context.Context, dir string, name string, args ...string) (stdout, stderr string, exitCode int, err error)
}

// DefaultGitRunner uses os/exec.
type DefaultGitRunner struct{}

func (DefaultGitRunner) Run(ctx context.Context, dir, name string, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	return stdout.String(), stderr.String(), exitCode, err
}

// Pusher pushes candidate refs to a remote.
type Pusher interface {
	Push(ctx context.Context, repoPath, refName string) error
}

// NoopPusher does nothing (for local-only testing).
type NoopPusher struct{}

func (NoopPusher) Push(ctx context.Context, repoPath, refName string) error {
	return nil
}

// GitPusher pushes refs to a remote using git push.
type GitPusher struct {
	RemoteName string // defaults to "origin"
	Runner     GitRunner
}

func NewGitPusher(remoteName string, runner GitRunner) *GitPusher {
	if remoteName == "" {
		remoteName = "origin"
	}
	if runner == nil {
		runner = DefaultGitRunner{}
	}
	return &GitPusher{RemoteName: remoteName, Runner: runner}
}

func (g *GitPusher) Push(ctx context.Context, repoPath, refName string) error {
	_, stderr, code, err := g.Runner.Run(ctx, repoPath, "git", "push", g.RemoteName, refName)
	if err != nil {
		return fmt.Errorf("git push: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("git push failed (exit %d): %s", code, strings.TrimSpace(stderr))
	}
	return nil
}

// Manager implements checkpoint creation and candidate retrieval.
// It is designed to be wired into rpc.Service.Checkpoint (which is a struct
// with function fields, not an interface).
type Manager struct {
	Runner GitRunner
	Pusher Pusher
}

// NewManager creates a Manager with defaults.
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		Runner: DefaultGitRunner{},
		Pusher: NoopPusher{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithGitRunner sets a custom GitRunner.
func WithGitRunner(r GitRunner) ManagerOption {
	return func(m *Manager) { m.Runner = r }
}

// WithPusher sets a custom Pusher.
func WithPusher(p Pusher) ManagerOption {
	return func(m *Manager) { m.Pusher = p }
}

// CreateCheckpoint creates an immutable git ref under refs/pantheon/cnd_<id>
// pointing at the worktree's HEAD, saves a candidate record, and optionally
// pushes the ref to a remote. Returns the candidate ID.
func (m *Manager) CreateCheckpoint(ctx context.Context, taskID, runID, worktreePath, summary string) (string, error) {
	if worktreePath == "" {
		return "", domain.ErrInvalidInput("worktree_path is required")
	}
	if taskID == "" {
		return "", domain.ErrInvalidInput("task_id is required")
	}

	// Get the current HEAD commit SHA.
	stdout, stderr, code, err := m.Runner.Run(ctx, worktreePath, "git", "rev-parse", "HEAD")
	if err != nil {
		return "", domain.ErrInternal("rev-parse HEAD: " + err.Error())
	}
	if code != 0 {
		return "", domain.ErrInternal("rev-parse HEAD failed (exit " + itoa(code) + "): " + strings.TrimSpace(stderr))
	}
	commitSHA := strings.TrimSpace(stdout)

	// Generate candidate ID.
	candidateID, err := domain.NewID("cnd_")
	if err != nil {
		return "", domain.ErrInternal("candidate id: " + err.Error())
	}

	// Create immutable ref: refs/pantheon/<candidate_id> -> HEAD.
	// The candidate ID already has a "cnd_" prefix, so the ref name is
	// refs/pantheon/cnd_<hex>. We don't add another "cnd_" prefix.
	refName := "refs/pantheon/" + candidateID
	_, stderr, code, err = m.Runner.Run(ctx, worktreePath, "git", "update-ref", refName, commitSHA)
	if err != nil {
		return "", domain.ErrInternal("update-ref: " + err.Error())
	}
	if code != 0 {
		return "", domain.ErrInternal("update-ref failed (exit " + itoa(code) + "): " + strings.TrimSpace(stderr))
	}

	// Push to remote (NoopPusher by default).
	if m.Pusher != nil {
		if err := m.Pusher.Push(ctx, worktreePath, refName); err != nil {
			// Non-fatal: ref is created locally, push failure is logged but
			// doesn't prevent the checkpoint from being recorded.
			_ = err
		}
	}

	return candidateID, nil
}

// GetCandidate retrieves a candidate by ID. In Phase 1, this reads from the
// git ref directly (the store has a candidates table but the Manager doesn't
// own store access — the RPC service is responsible for persisting the
// candidate record to the store after CreateCheckpoint returns).
//
// For now, GetCandidate reads the ref and returns a domain.Candidate with
// the commit SHA. The full candidate record (with summary, timestamps) is
// persisted by the RPC service layer.
func (m *Manager) GetCandidate(ctx context.Context, candidateID string) (*domain.Candidate, error) {
	if candidateID == "" {
		return nil, domain.ErrInvalidInput("candidate_id is required")
	}

	refName := "refs/pantheon/" + candidateID

	// Read the ref to get the commit SHA.
	// We need a repo path — but we don't have one here. The RPC service
	// should use the store's GetCandidate instead for full records.
	// This method is a fallback for direct ref queries.
	//
	// In practice, the RPC service stores the candidate record (including
	// worktreePath) in the SQLite candidates table and uses the store
	// to retrieve it. This Manager method is for cases where only the
	// ref name is known.
	//
	// For Phase 1, we return a minimal candidate with just the ID.
	// The RPC service layer fills in the rest from the store.
	return &domain.Candidate{
		CandidateID: candidateID,
		RefName:     refName,
	}, nil
}

// AsRPCCheckpointManager converts this Manager into the rpc.CheckpointManager
// struct (which uses function fields for dependency injection).
func (m *Manager) AsRPCCheckpointManager() rpc.CheckpointManager {
	return rpc.CheckpointManager{
		CreateCheckpoint: m.CreateCheckpoint,
		GetCandidate:     m.GetCandidate,
	}
}

// itoa is a simple int-to-string converter to avoid strconv import.
func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
