// Package auditor implements the Global Auditor (Phase 4): a periodic
// analyzer of run history that produces structured findings —
// recommendations, memory candidates, policy proposals, and risk findings.
//
// The auditor does NOT auto-modify anything. All findings start in the
// pending state and require human acceptance before they become versioned
// policies/docs (ROADMAP Phase 4: "人工接受后变成 versioned policy/doc",
// "不允许自修改"). Findings are stored locally in SQLite; Mnemos integration
// (memory candidates) is deferred.
//
// The auditor is optional — when not configured (nil on the Service), the
// auditor.* RPC methods return domain.ErrUnsupported("auditor not configured").
package auditor

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// AuditorStore is the consumer-side interface for the auditor's data needs.
// It is satisfied by a thin adapter over *store.Store (see StoreAdapter).
type AuditorStore interface {
	// ListRuns returns all (or recent) runs for analysis.
	ListRuns(ctx context.Context) ([]*domain.Run, error)
	// ListFindings returns findings, optionally filtered by status.
	ListFindings(ctx context.Context, status string, limit int) ([]*domain.Finding, error)
	// CreateFinding persists a new finding.
	CreateFinding(ctx context.Context, f *domain.Finding) error
	// GetEventsByRun returns events for a run after the given cursor.
	GetEventsByRun(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error)
}

// Auditor periodically analyzes run history and produces findings. It is
// safe for concurrent use (the store serializes writes). The auditor never
// auto-modifies state — it only creates pending findings for human review.
type Auditor struct {
	store  AuditorStore
	logger *log.Logger
}

// NewAuditor constructs an Auditor backed by the given store. logger may be
// nil (defaults to log.Default).
func NewAuditor(store AuditorStore, logger *log.Logger) *Auditor {
	if logger == nil {
		logger = log.Default()
	}
	return &Auditor{store: store, logger: logger}
}

// AuditResult is the outcome of one audit cycle.
type AuditResult struct {
	RunsAnalyzed     int
	FindingsProduced int
	Findings         []*domain.Finding
}

// Audit analyzes recent runs and produces findings for detected patterns.
// It is idempotent within a short window: a finding with the same title and
// type that is already pending is not re-created (so running audit twice in
// quick succession does not produce duplicates).
func (a *Auditor) Audit(ctx context.Context) (*AuditResult, error) {
	runs, err := a.store.ListRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("auditor: list runs: %w", err)
	}
	result := &AuditResult{RunsAnalyzed: len(runs)}
	if len(runs) == 0 {
		return result, nil
	}

	// Load existing pending findings so we can dedupe by title+type.
	existing, err := a.store.ListFindings(ctx, string(domain.FindingPending), 1000)
	if err != nil {
		return nil, fmt.Errorf("auditor: list existing findings: %w", err)
	}
	pendingKeys := make(map[string]bool, len(existing))
	for _, f := range existing {
		pendingKeys[findingDedupeKey(f.Type, f.Title)] = true
	}

	var findings []*domain.Finding
	makeFinding := func(typ domain.FindingType, sev domain.FindingSeverity, title, desc, action string, runIDs []string, evidence []string) {
		if pendingKeys[findingDedupeKey(typ, title)] {
			return // already a pending finding for this pattern
		}
		f := newFinding(typ, sev, title, desc, action, runIDs, evidence)
		if err := a.store.CreateFinding(ctx, f); err != nil {
			a.logger.Printf("auditor: create finding %q: %v", title, err)
			return
		}
		pendingKeys[findingDedupeKey(typ, title)] = true
		findings = append(findings, f)
	}

	// Classify runs by state and result.
	var (
		failedRuns         []*domain.Run
		budgetExceededRuns []*domain.Run
		blockedRuns        []*domain.Run
		verifyFailedRuns   []*domain.Run
		staleRuns          []*domain.Run
	)
	now := time.Now().UTC()
	staleThreshold := 24 * time.Hour
	for i := range runs {
		r := runs[i]
		switch r.State {
		case domain.RunV2Failed:
			failedRuns = append(failedRuns, r)
			if r.ResultState == domain.ResultBudgetExceeded {
				budgetExceededRuns = append(budgetExceededRuns, r)
			} else if r.ResultState == domain.ResultFailed {
				verifyFailedRuns = append(verifyFailedRuns, r)
			}
		case domain.RunV2Blocked:
			blockedRuns = append(blockedRuns, r)
		case domain.RunV2Requested, domain.RunV2Planning, domain.RunV2Ready,
			domain.RunV2Running, domain.RunV2Verifying:
			if r.StartedAt != nil && now.Sub(*r.StartedAt) > staleThreshold {
				staleRuns = append(staleRuns, r)
			} else if r.StartedAt == nil {
				// No started_at yet but created long ago — use a heuristic:
				// if the run has been in a non-terminal state with no
				// started_at, surface it as potentially stale.
				staleRuns = append(staleRuns, r)
			}
		}
	}

	// 1. Failed runs pattern: repeated failures may indicate a systematic
	//    issue (same root cause, environment problem, etc.).
	if len(failedRuns) >= failedRunThreshold {
		runIDs := runIDSlice(failedRuns)
		makeFinding(
			domain.FindingRiskFinding,
			domain.FindingSeverityWarning,
			"Repeated run failures detected",
			fmt.Sprintf("%d runs have failed. This may indicate a systematic issue (shared root cause, environment problem, or scope mismatch). Review the failed runs and their root causes before launching new work.", len(failedRuns)),
			"Review failed run root causes; consider blocking new runs in the affected workspace until the pattern is understood.",
			runIDs,
			nil,
		)
	}

	// 2. Budget usage pattern: runs consistently exceeding budget may need
	//    a budget adjustment or scope reduction.
	if len(budgetExceededRuns) >= budgetThreshold {
		runIDs := runIDSlice(budgetExceededRuns)
		makeFinding(
			domain.FindingPolicyProposal,
			domain.FindingSeverityWarning,
			"Runs consistently exceeding budget",
			fmt.Sprintf("%d runs ended with result_state=budget_exceeded. The configured budget may be too low for the task scope, or the tasks need to be decomposed into smaller pieces.", len(budgetExceededRuns)),
			"Propose a policy change: increase the default budget for this workspace/project, or require task decomposition when the estimated effort exceeds the budget.",
			runIDs,
			nil,
		)
	}

	// 3. Circuit breaker trips: runs blocked by the same-root-cause breaker
	//    may indicate a systematic issue that needs human intervention.
	if len(blockedRuns) >= circuitBreakerThreshold {
		runIDs := runIDSlice(blockedRuns)
		makeFinding(
			domain.FindingRiskFinding,
			domain.FindingSeverityCritical,
			"Frequent circuit breaker trips",
			fmt.Sprintf("%d runs are in the blocked state. Frequent circuit breaker trips suggest a systematic issue (repeated same-root-cause failures) that the auto-continuation loop cannot resolve on its own.", len(blockedRuns)),
			"Manually review the blocked runs and their root causes; decide whether to supersede, cancel, or adjust the task scope.",
			runIDs,
			nil,
		)
	}

	// 4. Verification failures: runs failing verification may indicate scope
	//    or task quality issues.
	if len(verifyFailedRuns) >= verifyFailThreshold {
		runIDs := runIDSlice(verifyFailedRuns)
		makeFinding(
			domain.FindingRecommendation,
			domain.FindingSeverityWarning,
			"Runs failing verification",
			fmt.Sprintf("%d runs failed verification (result_state=failed). This may indicate that the task scope is too ambitious, the acceptance criteria are unclear, or the runtime is producing low-quality output.", len(verifyFailedRuns)),
			"Review the verify.failed events and acceptance criteria; consider tightening scope or improving the task spec for future runs.",
			runIDs,
			nil,
		)
	}

	// 5. Stale runs: runs stuck in non-terminal states for too long.
	if len(staleRuns) > 0 {
		runIDs := runIDSlice(staleRuns)
		makeFinding(
			domain.FindingRiskFinding,
			domain.FindingSeverityWarning,
			"Stale runs in non-terminal states",
			fmt.Sprintf("%d runs have been in a non-terminal state (requested/planning/ready/running/verifying) for longer than %s. They may be stuck due to a crashed agent, a lost lease, or a missing PM decision.", len(staleRuns), staleThreshold),
			"Run reconcile.terminal_state and reconcile.crash to surface and resolve stuck runs; terminalize stale agents.",
			runIDs,
			nil,
		)
	}

	// 6. Memory candidate: if there are failed runs with a common root cause
	//    pattern, surface a memory candidate (deferred Mnemos integration —
	//    stored locally until Mnemos is available).
	if len(failedRuns) >= memoryCandidateThreshold {
		runIDs := runIDSlice(failedRuns)
		makeFinding(
			domain.FindingMemoryCandidate,
			domain.FindingSeverityInfo,
			"Failure pattern worth remembering",
			fmt.Sprintf("%d failed runs share a failure pattern. Recording this as a memory candidate so future runs can avoid the same root cause (Mnemos integration deferred — stored locally for now).", len(failedRuns)),
			"Accept this finding to persist the lesson as a versioned memory entry once Mnemos is available.",
			runIDs,
			nil,
		)
	}

	result.FindingsProduced = len(findings)
	result.Findings = findings
	return result, nil
}

// Analysis thresholds. A finding is produced when the count of matching
// runs reaches the threshold. These are conservative defaults that avoid
// noise on small workspaces while surfacing real patterns.
const (
	failedRunThreshold       = 3
	budgetThreshold          = 2
	circuitBreakerThreshold  = 2
	verifyFailThreshold      = 2
	memoryCandidateThreshold = 3
)

// findingDedupeKey is the dedup key for pending findings: type + title.
// Two audits producing the same pattern (same type + title) do not create
// duplicate findings while a pending one already exists.
func findingDedupeKey(typ domain.FindingType, title string) string {
	return string(typ) + "|" + title
}

// newFinding constructs a Finding with a fresh ID and pending status.
func newFinding(typ domain.FindingType, sev domain.FindingSeverity, title, desc, action string, runIDs, evidence []string) *domain.Finding {
	id, _ := domain.NewID("fnd_")
	if runIDs == nil {
		runIDs = []string{}
	}
	if evidence == nil {
		evidence = []string{}
	}
	return &domain.Finding{
		FindingID:      id,
		Type:           typ,
		Severity:       sev,
		Title:          title,
		Description:    desc,
		RunIDs:         runIDs,
		Evidence:       evidence,
		ProposedAction: action,
		Status:         domain.FindingPending,
		CreatedAt:      time.Now().UTC(),
	}
}

// runIDSlice extracts the run IDs from a slice of runs.
func runIDSlice(runs []*domain.Run) []string {
	ids := make([]string, 0, len(runs))
	for _, r := range runs {
		ids = append(ids, r.RunID)
	}
	return ids
}

// StoreAdapter adapts *store.Store to the AuditorStore interface. The store's
// ListRuns takes a status filter; the adapter passes "" to list all runs.
// The store's EventsSince is exposed as GetEventsByRun.
type StoreAdapter struct {
	ListRunsFn      func(ctx context.Context, statusFilter string) ([]*domain.Run, error)
	ListFindingsFn  func(ctx context.Context, status string, limit int) ([]*domain.Finding, error)
	CreateFindingFn func(ctx context.Context, f *domain.Finding) error
	EventsSinceFn   func(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error)
}

// ListRuns lists all runs (no status filter).
func (a StoreAdapter) ListRuns(ctx context.Context) ([]*domain.Run, error) {
	return a.ListRunsFn(ctx, "")
}

// ListFindings lists findings, optionally filtered by status.
func (a StoreAdapter) ListFindings(ctx context.Context, status string, limit int) ([]*domain.Finding, error) {
	return a.ListFindingsFn(ctx, status, limit)
}

// CreateFinding persists a new finding.
func (a StoreAdapter) CreateFinding(ctx context.Context, f *domain.Finding) error {
	return a.CreateFindingFn(ctx, f)
}

// GetEventsByRun returns events for a run after the given cursor.
func (a StoreAdapter) GetEventsByRun(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error) {
	return a.EventsSinceFn(ctx, runID, cursor, limit)
}
