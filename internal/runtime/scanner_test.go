package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/rpc"
)

// fakeAgentStore is a test implementation of AgentStore.
type fakeAgentStore struct {
	agents  []domain.Agent
	runs    map[string]*domain.Run
	tasks   map[string]*domain.Task
	projs   map[string]*domain.Project
	updated map[string]domain.AgentState

	// Budget-enforcement state.
	failedRuns     map[string]bool // runIDs failed by FailRunBudgetExceeded
	listRunningErr error
}

func (s *fakeAgentStore) ListRunningAgents(ctx context.Context) ([]domain.Agent, error) {
	var result []domain.Agent
	for _, a := range s.agents {
		if a.State == domain.AgentRunning {
			result = append(result, a)
		}
	}
	return result, nil
}

func (s *fakeAgentStore) UpdateAgentState(ctx context.Context, agentID string, to domain.AgentState, exitCode *int, eventID string) error {
	if s.updated == nil {
		s.updated = make(map[string]domain.AgentState)
	}
	s.updated[agentID] = to
	return nil
}

func (s *fakeAgentStore) GetRun(ctx context.Context, runID string) (*domain.Run, error) {
	return s.runs[runID], nil
}

func (s *fakeAgentStore) GetTaskByRun(ctx context.Context, runID string) (*domain.Task, error) {
	return s.tasks[runID], nil
}

func (s *fakeAgentStore) GetProject(ctx context.Context, projectID string) (*domain.Project, error) {
	return s.projs[projectID], nil
}

// ListRunningRuns returns all runs in the 'running' state from the fake
// store's runs map.
func (s *fakeAgentStore) ListRunningRuns(ctx context.Context) ([]*domain.Run, error) {
	if s.listRunningErr != nil {
		return nil, s.listRunningErr
	}
	var out []*domain.Run
	for _, r := range s.runs {
		if r.State == domain.RunV2Running {
			out = append(out, r)
		}
	}
	return out, nil
}

// FailRunBudgetExceeded records that a run was failed due to budget
// exhaustion and updates the in-memory run state.
func (s *fakeAgentStore) FailRunBudgetExceeded(ctx context.Context, runID string) error {
	if s.failedRuns == nil {
		s.failedRuns = make(map[string]bool)
	}
	s.failedRuns[runID] = true
	if r, ok := s.runs[runID]; ok {
		r.State = domain.RunV2Failed
		r.ResultState = domain.ResultBudgetExceeded
	}
	return nil
}

// fakeRuntime is a test RuntimeAdapter that returns a fixed status.
type fakeRuntime struct {
	status rpc.RuntimeStatus
	err    error
}

func (f *fakeRuntime) Start(ctx context.Context, p rpc.RuntimeStartParams) (rpc.RuntimeHandle, error) {
	return rpc.RuntimeHandle{}, nil
}

func (f *fakeRuntime) Stop(ctx context.Context, h rpc.RuntimeHandle, grace time.Duration) error {
	return nil
}

func (f *fakeRuntime) Inspect(ctx context.Context, h rpc.RuntimeHandle) (rpc.RuntimeStatus, error) {
	return f.status, f.err
}

func TestScanner_DetectsExitedAgent(t *testing.T) {
	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: "/tmp/nonexistent"},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	scanner := NewScanner(store, rt, ScannerConfig{PollInterval: 50 * time.Millisecond})
	err := scanner.scan(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got := store.updated["agt_1"]; got != domain.AgentExited {
		t.Fatalf("agent state = %v, want exited", got)
	}
}

func TestScanner_TriggersContinuation(t *testing.T) {
	// Create a worktree with a progress file that has remaining subtasks.
	dir := t.TempDir()
	prog := "## Subtasks\n- [x] step 1\n- [ ] step 2\n- [ ] step 3\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: dir},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	var called bool
	var gotRemaining int
	scanner := NewScanner(store, rt, ScannerConfig{
		PollInterval: 50 * time.Millisecond,
		OnContinuationNeeded: func(ctx context.Context, runID, worktreePath string, remaining int) {
			called = true
			gotRemaining = remaining
			if runID != "run_1" {
				t.Errorf("runID = %q, want run_1", runID)
			}
			if worktreePath != dir {
				t.Errorf("worktreePath = %q, want %s", worktreePath, dir)
			}
		},
	})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !called {
		t.Fatal("OnContinuationNeeded should have been called")
	}
	if gotRemaining != 2 {
		t.Fatalf("remaining = %d, want 2", gotRemaining)
	}
}

func TestScanner_NoContinuationWhenAllDone(t *testing.T) {
	dir := t.TempDir()
	prog := "## Subtasks\n- [x] step 1\n- [x] step 2\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: dir},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	var called bool
	scanner := NewScanner(store, rt, ScannerConfig{
		PollInterval: 50 * time.Millisecond,
		OnContinuationNeeded: func(ctx context.Context, runID, worktreePath string, remaining int) {
			called = true
		},
	})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if called {
		t.Fatal("OnContinuationNeeded should NOT have been called when all subtasks done")
	}
}

func TestScanner_AllSubtasksCompleteCallback(t *testing.T) {
	dir := t.TempDir()
	prog := "## Subtasks\n- [x] step 1\n- [x] step 2\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: dir},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	var completeCalled bool
	var gotRunID, gotPath string
	scanner := NewScanner(store, rt, ScannerConfig{
		PollInterval: 50 * time.Millisecond,
		OnAllSubtasksComplete: func(ctx context.Context, runID, worktreePath string) {
			completeCalled = true
			gotRunID = runID
			gotPath = worktreePath
		},
	})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !completeCalled {
		t.Fatal("OnAllSubtasksComplete should have been called")
	}
	if gotRunID != "run_1" {
		t.Errorf("runID = %q, want run_1", gotRunID)
	}
	if gotPath != dir {
		t.Errorf("worktreePath = %q, want %s", gotPath, dir)
	}
}

func TestScanner_ProgressGateBlocks(t *testing.T) {
	dir := t.TempDir()
	// 2 remaining subtasks — will stay the same across scans.
	prog := "## Subtasks\n- [x] done\n- [ ] a\n- [ ] b\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: dir},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	var contCalled, blockedCalled bool
	var blockedRemaining int
	scanner := NewScanner(store, rt, ScannerConfig{
		PollInterval:  50 * time.Millisecond,
		MaxNoProgress: 3,
		OnContinuationNeeded: func(ctx context.Context, runID, worktreePath string, remaining int) {
			contCalled = true
		},
		OnBlocked: func(ctx context.Context, runID, worktreePath string, remaining int) {
			blockedCalled = true
			blockedRemaining = remaining
		},
	})

	// Scan 1: first exit, noProgressCount=1 → continuation.
	store.agents = []domain.Agent{{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning}}
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !contCalled {
		t.Fatal("scan 1: continuation should fire")
	}
	contCalled = false

	// Scan 2: same remaining, noProgressCount=2 → continuation.
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !contCalled {
		t.Fatal("scan 2: continuation should fire")
	}
	contCalled = false

	// Scan 3: same remaining, noProgressCount=3 → BLOCKED.
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if contCalled {
		t.Fatal("scan 3: continuation should NOT fire (blocked)")
	}
	if !blockedCalled {
		t.Fatal("scan 3: OnBlocked should fire")
	}
	if blockedRemaining != 2 {
		t.Fatalf("blocked remaining = %d, want 2", blockedRemaining)
	}
}

func TestScanner_ProgressGateResetsOnProgress(t *testing.T) {
	dir := t.TempDir()

	writeProgress := func(remaining int) {
		var prog string
		prog = "## Subtasks\n- [x] done\n"
		for i := 0; i < remaining; i++ {
			prog += "- [ ] task\n"
		}
		if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_1", RunID: "run_1", TaskID: "tsk_1", PID: 99999, State: domain.AgentRunning},
		},
		tasks: map[string]*domain.Task{
			"run_1": {TaskID: "tsk_1", RunID: "run_1", WorktreePath: dir},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentExited, ExitCode: intPtr(0)}}

	var blockedCalled bool
	scanner := NewScanner(store, rt, ScannerConfig{
		PollInterval:  50 * time.Millisecond,
		MaxNoProgress: 3,
		OnBlocked: func(ctx context.Context, runID, worktreePath string, remaining int) {
			blockedCalled = true
		},
	})

	// Scan 1: 3 remaining, noProgressCount=1.
	writeProgress(3)
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Scan 2: 3 remaining, noProgressCount=2.
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Scan 3: progress! 1 remaining, noProgressCount resets to 0.
	writeProgress(1)
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if blockedCalled {
		t.Fatal("should not be blocked after progress")
	}

	// Scan 4: 1 remaining, noProgressCount=1 (fresh count).
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if blockedCalled {
		t.Fatal("should not be blocked after only 1 no-progress since reset")
	}
}

func TestScanner_SkipsRunningAgent(t *testing.T) {
	store := &fakeAgentStore{
		agents: []domain.Agent{
			{AgentID: "agt_2", RunID: "run_2", State: domain.AgentRunning},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentRunning}}

	scanner := NewScanner(store, rt, ScannerConfig{PollInterval: 50 * time.Millisecond})
	err := scanner.scan(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if _, changed := store.updated["agt_2"]; changed {
		t.Fatal("running agent should not be updated")
	}
}

func TestScanner_RemainingSubtasks(t *testing.T) {
	dir := t.TempDir()
	prog := "## Subtasks\n- [x] done task\n- [ ] remaining task\n- [ ] another\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}
	remaining, err := countRemainingSubtasks(dir)
	if err != nil {
		t.Fatalf("countRemainingSubtasks: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("remaining = %d, want 2", remaining)
	}
}

func TestScanner_AllSubtasksComplete(t *testing.T) {
	dir := t.TempDir()
	prog := "## Subtasks\n- [x] task 1\n- [x] task 2\n- [x] task 3\n"
	if err := os.WriteFile(filepath.Join(dir, progressFileName), []byte(prog), 0o600); err != nil {
		t.Fatal(err)
	}
	remaining, err := countRemainingSubtasks(dir)
	if err != nil {
		t.Fatalf("countRemainingSubtasks: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining = %d, want 0", remaining)
	}
}

func TestScanner_NoProgressFile(t *testing.T) {
	dir := t.TempDir()
	remaining, err := countRemainingSubtasks(dir)
	if err != nil {
		t.Fatalf("countRemainingSubtasks: %v", err)
	}
	if remaining != -1 {
		t.Fatalf("remaining = %d, want -1 (no file)", remaining)
	}
}

func TestScannerBudgetExceeded(t *testing.T) {
	// A run with a very short budget that has already elapsed. The scanner
	// should fail it with result_state='budget_exceeded'.
	startedAt := time.Now().Add(-50 * time.Millisecond)
	store := &fakeAgentStore{
		runs: map[string]*domain.Run{
			"run_budget": {
				RunID:       "run_budget",
				State:       domain.RunV2Running,
				ResultState: domain.ResultNotStarted,
				Budget:      1 * time.Millisecond, // already exceeded
				StartedAt:   &startedAt,
			},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentRunning}}

	scanner := NewScanner(store, rt, ScannerConfig{PollInterval: 50 * time.Millisecond})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !store.failedRuns["run_budget"] {
		t.Fatal("FailRunBudgetExceeded should have been called for run_budget")
	}
	run := store.runs["run_budget"]
	if run.State != domain.RunV2Failed {
		t.Fatalf("run state = %q, want failed", run.State)
	}
	if run.ResultState != domain.ResultBudgetExceeded {
		t.Fatalf("result state = %q, want budget_exceeded", run.ResultState)
	}
}

func TestScannerBudgetNotExceeded(t *testing.T) {
	// A run with a generous budget that has not yet elapsed. The scanner
	// should leave it running.
	startedAt := time.Now()
	store := &fakeAgentStore{
		runs: map[string]*domain.Run{
			"run_ok": {
				RunID:       "run_ok",
				State:       domain.RunV2Running,
				ResultState: domain.ResultNotStarted,
				Budget:      8 * time.Hour,
				StartedAt:   &startedAt,
			},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentRunning}}

	scanner := NewScanner(store, rt, ScannerConfig{PollInterval: 50 * time.Millisecond})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if store.failedRuns["run_ok"] {
		t.Fatal("FailRunBudgetExceeded should NOT have been called for run_ok")
	}
	run := store.runs["run_ok"]
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running", run.State)
	}
}

func TestScannerBudgetExceededNoRunningAgents(t *testing.T) {
	// A run with no running agents but still in the running state — the
	// budget pass must still catch it (the early-return on zero agents was
	// removed for this reason).
	startedAt := time.Now().Add(-50 * time.Millisecond)
	store := &fakeAgentStore{
		agents: nil, // no running agents
		runs: map[string]*domain.Run{
			"run_noagent": {
				RunID:       "run_noagent",
				State:       domain.RunV2Running,
				ResultState: domain.ResultNotStarted,
				Budget:      1 * time.Millisecond,
				StartedAt:   &startedAt,
			},
		},
	}
	rt := &fakeRuntime{status: rpc.RuntimeStatus{State: domain.AgentRunning}}

	scanner := NewScanner(store, rt, ScannerConfig{PollInterval: 50 * time.Millisecond})
	if err := scanner.scan(context.Background()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !store.failedRuns["run_noagent"] {
		t.Fatal("FailRunBudgetExceeded should have been called for run_noagent even with no running agents")
	}
}

func intPtr(v int) *int { return &v }
