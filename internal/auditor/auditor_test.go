package auditor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// fakeStore is an in-memory AuditorStore for auditor unit tests. It holds
// runs and findings; CreateFinding persists to the findings slice so the
// idempotency check (ListFindings of pending) works across audit cycles.
type fakeStore struct {
	mu       sync.Mutex
	runs     []*domain.Run
	findings []*domain.Finding
}

func (f *fakeStore) ListRuns(ctx context.Context) ([]*domain.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*domain.Run, len(f.runs))
	copy(out, f.runs)
	return out, nil
}

func (f *fakeStore) ListFindings(ctx context.Context, status string, limit int) ([]*domain.Finding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*domain.Finding
	for _, fd := range f.findings {
		if status == "" || string(fd.Status) == status {
			out = append(out, fd)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeStore) CreateFinding(ctx context.Context, fd *domain.Finding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findings = append(f.findings, fd)
	return nil
}

func (f *fakeStore) GetEventsByRun(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error) {
	return nil, nil
}

// makeRun constructs a run in the given state with the given result state.
func makeRun(state domain.RunStateV2, result domain.ResultState, startedAgo time.Duration) *domain.Run {
	r := &domain.Run{
		RunID:       "run_" + string(state) + "_" + string(result),
		Budget:      time.Hour,
		State:       state,
		ResultState: result,
	}
	if startedAgo > 0 {
		t := time.Now().UTC().Add(-startedAgo)
		r.StartedAt = &t
	}
	return r
}

func TestAuditorEmptyStore(t *testing.T) {
	st := &fakeStore{}
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if result.RunsAnalyzed != 0 {
		t.Fatalf("runs_analyzed = %d, want 0", result.RunsAnalyzed)
	}
	if result.FindingsProduced != 0 {
		t.Fatalf("findings_produced = %d, want 0", result.FindingsProduced)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %v, want empty", result.Findings)
	}
}

func TestAuditorDetectsFailedRuns(t *testing.T) {
	st := &fakeStore{}
	// 3 failed runs (threshold = 3) with result_state=failed (verify fail).
	for i := 0; i < 3; i++ {
		st.runs = append(st.runs, makeRun(domain.RunV2Failed, domain.ResultFailed, time.Hour))
	}
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if result.RunsAnalyzed != 3 {
		t.Fatalf("runs_analyzed = %d, want 3", result.RunsAnalyzed)
	}
	if result.FindingsProduced == 0 {
		t.Fatal("expected at least one finding for failed runs")
	}
	// Should produce a risk_finding for repeated failures.
	var hasRiskFinding bool
	for _, f := range result.Findings {
		if f.Type == domain.FindingRiskFinding && f.Status == domain.FindingPending {
			hasRiskFinding = true
		}
	}
	if !hasRiskFinding {
		t.Fatal("expected a pending risk_finding for repeated failures")
	}
}

func TestAuditorDetectsBudgetExceeded(t *testing.T) {
	st := &fakeStore{}
	// 2 budget_exceeded runs (threshold = 2).
	for i := 0; i < 2; i++ {
		st.runs = append(st.runs, makeRun(domain.RunV2Failed, domain.ResultBudgetExceeded, time.Hour))
	}
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	var hasBudgetFinding bool
	for _, f := range result.Findings {
		if f.Type == domain.FindingPolicyProposal && f.Title == "Runs consistently exceeding budget" {
			hasBudgetFinding = true
		}
	}
	if !hasBudgetFinding {
		t.Fatal("expected a policy_proposal for budget exceeded")
	}
}

func TestAuditorDetectsCircuitBreakerTrips(t *testing.T) {
	st := &fakeStore{}
	// 2 blocked runs (circuit breaker threshold = 2).
	for i := 0; i < 2; i++ {
		st.runs = append(st.runs, makeRun(domain.RunV2Blocked, domain.ResultNotStarted, time.Hour))
	}
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	var hasCircuitBreakerFinding bool
	for _, f := range result.Findings {
		if f.Type == domain.FindingRiskFinding && f.Title == "Frequent circuit breaker trips" {
			hasCircuitBreakerFinding = true
		}
	}
	if !hasCircuitBreakerFinding {
		t.Fatal("expected a risk_finding for circuit breaker trips")
	}
}

func TestAuditorDetectsStaleRuns(t *testing.T) {
	st := &fakeStore{}
	// A run stuck in running for 48h (> 24h stale threshold).
	st.runs = append(st.runs, makeRun(domain.RunV2Running, domain.ResultNotStarted, 48*time.Hour))
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	var hasStaleFinding bool
	for _, f := range result.Findings {
		if f.Title == "Stale runs in non-terminal states" {
			hasStaleFinding = true
		}
	}
	if !hasStaleFinding {
		t.Fatal("expected a finding for stale runs")
	}
}

func TestAuditorIdempotent(t *testing.T) {
	st := &fakeStore{}
	// 3 failed runs to trigger the repeated-failures finding.
	for i := 0; i < 3; i++ {
		st.runs = append(st.runs, makeRun(domain.RunV2Failed, domain.ResultFailed, time.Hour))
	}
	a := NewAuditor(st, nil)
	ctx := context.Background()

	r1, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit 1: %v", err)
	}
	if r1.FindingsProduced == 0 {
		t.Fatal("first audit should produce findings")
	}
	firstCount := r1.FindingsProduced

	// Second audit on the same run set should not produce duplicate findings.
	r2, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit 2: %v", err)
	}
	if r2.FindingsProduced != 0 {
		t.Fatalf("second audit produced %d findings, want 0 (idempotent)", r2.FindingsProduced)
	}

	// The store should have exactly firstCount findings (no duplicates).
	all, err := st.ListFindings(ctx, "", 1000)
	if err != nil {
		t.Fatalf("ListFindings: %v", err)
	}
	if len(all) != firstCount {
		t.Fatalf("stored findings = %d, want %d (no duplicates)", len(all), firstCount)
	}
}

func TestAuditorBelowThresholdNoFindings(t *testing.T) {
	st := &fakeStore{}
	// Only 1 failed run (threshold = 3) — no finding expected.
	st.runs = append(st.runs, makeRun(domain.RunV2Failed, domain.ResultFailed, time.Hour))
	a := NewAuditor(st, nil)
	ctx := context.Background()

	result, err := a.Audit(ctx)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if result.FindingsProduced != 0 {
		t.Fatalf("findings_produced = %d, want 0 (below threshold)", result.FindingsProduced)
	}
}
