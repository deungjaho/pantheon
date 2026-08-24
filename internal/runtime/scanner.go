// Package runtime implements the agent liveness scanner.
//
// The scanner periodically checks all running agents via the RuntimeAdapter's
// Inspect method. When an agent is found to have exited, the scanner:
//  1. Updates the agent state in the store.
//  2. Reads PANTHEON_PROGRESS.md from the worktree.
//  3. If there are remaining (unchecked) subtasks, auto-creates and starts
//     a continuation run that reuses the same worktree.
//  4. If all subtasks are complete, marks the run for verification.
package runtime

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

// AgentStore is the subset of store.Store required by the scanner.
type AgentStore interface {
	ListRunningAgents(ctx context.Context) ([]domain.Agent, error)
	UpdateAgentState(ctx context.Context, agentID string, to domain.AgentState, exitCode *int, eventID string) error
	GetRun(ctx context.Context, runID string) (*domain.Run, error)
	GetTaskByRun(ctx context.Context, runID string) (*domain.Task, error)
	GetProject(ctx context.Context, projectID string) (*domain.Project, error)

	// ListRunningRuns returns all runs in the 'running' state. Used by the
	// budget-enforcement pass to catch runs whose budget has been exceeded
	// even if they have no running agents (e.g., the agent crashed but the
	// run was not yet terminalized).
	ListRunningRuns(ctx context.Context) ([]*domain.Run, error)
	// FailRunBudgetExceeded transitions a run to failed with
	// result_state='budget_exceeded' and terminalizes its agents.
	FailRunBudgetExceeded(ctx context.Context, runID string) error
}

// ScannerConfig controls the agent liveness scanner.
type ScannerConfig struct {
	// PollInterval is how often to scan. Default 10s.
	PollInterval time.Duration
	// Logger receives diagnostic messages.
	Logger *log.Logger
	// OnContinuationNeeded is called when an agent exits with remaining
	// subtasks. The callback receives the run ID and worktree path.
	// If nil, continuations are only logged.
	OnContinuationNeeded func(ctx context.Context, runID, worktreePath string, remaining int)
	// OnAllSubtasksComplete is called when an agent exits and all
	// subtasks in PANTHEON_PROGRESS.md are checked. The callback receives
	// the run ID and worktree path. If nil, completion is only logged.
	OnAllSubtasksComplete func(ctx context.Context, runID, worktreePath string)
	// OnBlocked is called when the progress gate fires — the same worktree
	// has had MaxNoProgress consecutive continuations without a decrease in
	// remaining subtasks. The callback receives the run ID, worktree path,
	// and the stale remaining count. If nil, blocking is only logged.
	OnBlocked func(ctx context.Context, runID, worktreePath string, remaining int)
	// MaxNoProgress is the maximum number of consecutive continuations
	// allowed without a decrease in remaining subtasks. Default 3.
	MaxNoProgress int
}

// progressTracker tracks per-worktree progress across continuations to
// detect no-progress loops.
type progressTracker struct {
	lastRemaining     int
	noProgressCount   int
	lastProgressRunID string
}

// Scanner periodically checks running agents for liveness and triggers
// continuations when agents exit with remaining work.
type Scanner struct {
	store    AgentStore
	runtime  rpc.RuntimeAdapter
	cfg      ScannerConfig
	logger   *log.Logger
	trackers map[string]*progressTracker
}

// NewScanner creates an agent liveness scanner.
func NewScanner(store AgentStore, rt rpc.RuntimeAdapter, cfg ScannerConfig) *Scanner {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.MaxNoProgress <= 0 {
		cfg.MaxNoProgress = 3
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Scanner{
		store:    store,
		runtime:  rt,
		cfg:      cfg,
		logger:   logger,
		trackers: make(map[string]*progressTracker),
	}
}

// Start runs the scanner in a background goroutine until ctx is cancelled.
func (s *Scanner) Start(ctx context.Context) {
	s.logger.Printf("agent-scanner: starting (poll=%s)", s.cfg.PollInterval)
	go s.run(ctx)
}

func (s *Scanner) run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Printf("agent-scanner: stopped")
			return
		case <-ticker.C:
			if err := s.scan(ctx); err != nil {
				s.logger.Printf("agent-scanner: scan error: %v", err)
			}
		}
	}
}

// scan performs one liveness check cycle. It is also exported for testing.
func (s *Scanner) scan(ctx context.Context) error {
	agents, err := s.store.ListRunningAgents(ctx)
	if err != nil {
		return fmt.Errorf("list running agents: %w", err)
	}

	for _, a := range agents {
		if err := s.checkAgent(ctx, &a); err != nil {
			s.logger.Printf("agent-scanner: check agent %s: %v", a.AgentID, err)
		}
	}

	// Budget-enforcement pass: check all running runs (not just runs with
	// running agents — a run could have a dead agent but still be in the
	// running state). A run whose elapsed time exceeds its Budget is
	// transitioned to failed with result_state='budget_exceeded'. This pass
	// runs even when there are no running agents, so a run whose agent
	// crashed is still caught by its budget.
	s.checkBudgets(ctx)
	return nil
}

// checkBudgets fails any running run whose elapsed time has exceeded its
// budget. This is a separate pass from the agent liveness check so that a
// run with no running agents (e.g., the agent crashed) is still caught.
func (s *Scanner) checkBudgets(ctx context.Context) {
	runs, err := s.store.ListRunningRuns(ctx)
	if err != nil {
		s.logger.Printf("agent-scanner: list running runs: %v", err)
		return
	}
	for _, run := range runs {
		if run.Budget <= 0 || run.StartedAt == nil {
			continue
		}
		elapsed := time.Since(*run.StartedAt)
		if elapsed > run.Budget {
			s.logger.Printf("agent-scanner: run %s budget exceeded (%s > %s), failing",
				run.RunID, elapsed, run.Budget)
			if err := s.store.FailRunBudgetExceeded(ctx, run.RunID); err != nil {
				s.logger.Printf("agent-scanner: fail run %s budget exceeded: %v", run.RunID, err)
			}
		}
	}
}

// checkAgent inspects a single agent. If the process has exited, it updates
// the agent state and triggers continuation if needed.
func (s *Scanner) checkAgent(ctx context.Context, a *domain.Agent) error {
	status, err := s.runtime.Inspect(ctx, rpc.RuntimeHandle{
		AgentID: a.AgentID,
		PID:     a.PID,
	})
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	if status.State == domain.AgentRunning {
		return nil
	}

	// Agent has exited. Update state.
	exitCode := status.ExitCode
	eid, err := newEventID()
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	if err := s.store.UpdateAgentState(ctx, a.AgentID, status.State, exitCode, eid); err != nil {
		return fmt.Errorf("update agent state: %w", err)
	}
	s.logger.Printf("agent-scanner: agent %s exited (code=%v), run=%s", a.AgentID, exitCode, a.RunID)

	// Read PANTHEON_PROGRESS.md from the worktree.
	task, err := s.store.GetTaskByRun(ctx, a.RunID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}
	if task == nil || task.WorktreePath == "" {
		s.logger.Printf("agent-scanner: no worktree for run %s, skipping continuation", a.RunID)
		return nil
	}

	remaining, err := countRemainingSubtasks(task.WorktreePath)
	if err != nil {
		s.logger.Printf("agent-scanner: read progress file: %v", err)
		return nil
	}

	if remaining > 0 {
		blocked := s.updateProgressTracker(task.WorktreePath, a.RunID, remaining)
		if blocked {
			s.logger.Printf("agent-scanner: run %s BLOCKED — %d remaining subtasks unchanged for %d continuations",
				a.RunID, remaining, s.cfg.MaxNoProgress)
			if s.cfg.OnBlocked != nil {
				s.cfg.OnBlocked(ctx, a.RunID, task.WorktreePath, remaining)
			}
			return nil
		}
		s.logger.Printf("agent-scanner: run %s has %d remaining subtasks, continuation needed",
			a.RunID, remaining)
		if s.cfg.OnContinuationNeeded != nil {
			s.cfg.OnContinuationNeeded(ctx, a.RunID, task.WorktreePath, remaining)
		}
	} else {
		s.resetProgressTracker(task.WorktreePath)
		s.logger.Printf("agent-scanner: run %s all subtasks complete, ready for verification", a.RunID)
		if s.cfg.OnAllSubtasksComplete != nil {
			s.cfg.OnAllSubtasksComplete(ctx, a.RunID, task.WorktreePath)
		}
	}
	return nil
}

// updateProgressTracker records the remaining subtask count for a worktree
// and returns true if the progress gate should block the next continuation.
// The gate fires when noProgressCount reaches MaxNoProgress — i.e. the
// remaining count has not decreased across that many consecutive exits.
func (s *Scanner) updateProgressTracker(worktreePath, runID string, remaining int) bool {
	t := s.trackers[worktreePath]
	if t == nil {
		t = &progressTracker{lastRemaining: -1}
		s.trackers[worktreePath] = t
	}

	if remaining < t.lastRemaining {
		// Progress made: reset counter.
		t.noProgressCount = 0
		t.lastProgressRunID = runID
	} else {
		// No progress (same or more remaining).
		t.noProgressCount++
	}
	t.lastRemaining = remaining

	return t.noProgressCount >= s.cfg.MaxNoProgress
}

// resetProgressTracker clears the tracker for a worktree after all subtasks
// are complete.
func (s *Scanner) resetProgressTracker(worktreePath string) {
	delete(s.trackers, worktreePath)
}

// countRemainingSubtasks reads PANTHEON_PROGRESS.md from the worktree
// and counts unchecked subtask lines (lines starting with "- [ ]").
func countRemainingSubtasks(worktreePath string) (int, error) {
	data, err := os.ReadFile(filepath.Join(worktreePath, progressFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil // no progress file
		}
		return 0, err
	}
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "- [ ]") {
			count++
		}
	}
	return count, nil
}

// newEventID generates a new event ID. This is a thin wrapper to avoid
// importing domain event utilities in the scanner package boundary.
func newEventID() (string, error) {
	return domain.NewID("evt_")
}
