package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustNewID(t *testing.T, prefix string) string {
	t.Helper()
	id, err := domain.NewID(prefix)
	if err != nil {
		t.Fatalf("NewID(%q): %v", prefix, err)
	}
	return id
}

func TestOpenCreatesFileWith0600(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestMigrationIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	s1, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	s1.Close()
	s2, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer s2.Close()
	// Schema version should be the latest after both opens.
	var v string
	err = s2.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v)
	if err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if v != "12" {
		t.Fatalf("schema_version = %q, want 12", v)
	}
}

func TestRegisterProjectAtomic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	p := &domain.Project{
		ProjectID:    pid,
		Name:         "argus",
		RepoPath:     "/home/camt/Work/Argus",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}
	eid := mustNewID(t, "evt_")
	if err := s.RegisterProject(ctx, p, eid); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	got, err := s.GetProject(ctx, pid)
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got == nil {
		t.Fatal("project not found after insert")
	}
	if got.Name != "argus" {
		t.Fatalf("name = %q, want argus", got.Name)
	}
	// Event should be in the journal.
	events, err := s.EventsSince(ctx, "", 0, 10)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].EventType != "project.registered" {
		t.Fatalf("event type = %q, want project.registered", events[0].EventType)
	}
}

func TestDuplicateEventIDIsNoop(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	p := &domain.Project{
		ProjectID:    pid,
		Name:         "argus",
		RepoPath:     "/home/camt/Work/Argus",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}
	eid := mustNewID(t, "evt_")
	if err := s.RegisterProject(ctx, p, eid); err != nil {
		t.Fatalf("RegisterProject 1: %v", err)
	}
	// Re-register with same event_id should not duplicate the event.
	p2 := &domain.Project{
		ProjectID:    mustNewID(t, "prj_"),
		Name:         "hydra",
		RepoPath:     "/home/camt/Work/Hydra",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}
	// Use the same event_id but a different project; the event append should
	// be a no-op for the event, but the project insert still happens.
	if err := s.RegisterProject(ctx, p2, eid); err != nil {
		t.Fatalf("RegisterProject 2: %v", err)
	}
	events, err := s.EventsSince(ctx, "", 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	// Only one event because the second had a duplicate event_id.
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1 (duplicate event_id should be no-op)", len(events))
	}
}

func TestRunStateTransitionValidated(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	rid := mustNewID(t, "run_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "argus", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceCreated, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: rid, WorkspaceID: wid, BaseCommit: "abc", Budget: 8 * time.Hour,
		State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Valid: requested -> planning
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Planning, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("requested -> planning: %v", err)
	}
	// Valid: planning -> ready
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Ready, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("planning -> ready: %v", err)
	}
	// Valid: ready -> running
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Running, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("ready -> running: %v", err)
	}
	// Invalid: running -> requested
	err := s.UpdateRunState(ctx, rid, domain.RunV2Requested, mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("running -> pending should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("error code = %q, want CONFLICT", de.Code)
	}
}

func TestIdempotencyCache(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	reqID := mustNewID(t, "req_")
	resp := json.RawMessage(`{"result":"ok"}`)
	if _, ok, err := s.GetCachedResponse(ctx, reqID); err != nil || ok {
		t.Fatalf("expected miss, got ok=%v err=%v", ok, err)
	}
	if err := s.CacheResponse(ctx, reqID, resp); err != nil {
		t.Fatalf("CacheResponse: %v", err)
	}
	got, ok, err := s.GetCachedResponse(ctx, reqID)
	if err != nil || !ok {
		t.Fatalf("expected hit, got ok=%v err=%v", ok, err)
	}
	if string(got) != string(resp) {
		t.Fatalf("cached = %q, want %q", got, resp)
	}
}

func TestReconcileAfterCrash(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	rid := mustNewID(t, "run_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "argus", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: rid, WorkspaceID: wid, BaseCommit: "abc", Budget: 8 * time.Hour,
		State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Planning, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> planning: %v", err)
	}
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Ready, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> ready: %v", err)
	}
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Running, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> running: %v", err)
	}
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: mustNewID(t, "agt_"), RunID: rid, Role: domain.RoleWorker,
		Runtime: "devin", PID: 12345, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	reconciled, err := s.ReconcileAfterCrash(ctx)
	if err != nil {
		t.Fatalf("ReconcileAfterCrash: %v", err)
	}
	if len(reconciled) != 2 {
		t.Fatalf("reconciled count = %d, want 2: %v", len(reconciled), reconciled)
	}
	run, err := s.GetRun(ctx, rid)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.State != domain.RunV2Failed {
		t.Fatalf("run state = %q, want failed", run.State)
	}
	if run.ResultState != domain.ResultUnknown {
		t.Fatalf("result state = %q, want result_unknown", run.ResultState)
	}
}

func TestEventsSinceCursor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "a", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	wid := mustNewID(t, "ws_")
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceCreated, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	rid := mustNewID(t, "run_")
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: rid, WorkspaceID: wid, BaseCommit: "abc", Budget: 1 * time.Hour,
		State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// 3 events so far: project.registered, workspace.created, run.created
	all, err := s.EventsSince(ctx, "", 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("total events = %d, want 3", len(all))
	}
	lastSeq := all[len(all)-1].Seq
	// Query with cursor = lastSeq should return nothing.
	after, err := s.EventsSince(ctx, "", lastSeq, 100)
	if err != nil {
		t.Fatalf("EventsSince cursor: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("events after cursor = %d, want 0", len(after))
	}
	// Add one more and verify cursor resume.
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Planning, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("UpdateRunState: %v", err)
	}
	resumed, err := s.EventsSince(ctx, "", lastSeq, 100)
	if err != nil {
		t.Fatalf("EventsSince resume: %v", err)
	}
	if len(resumed) != 1 {
		t.Fatalf("resumed events = %d, want 1", len(resumed))
	}
}

func TestConcurrentWritersSerialize(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const goroutines = 16
	const perG = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				pid := mustNewID(t, "prj_")
				p := &domain.Project{
					ProjectID:    pid,
					Name:         fmt.Sprintf("p-%d-%d", idx, i),
					RepoPath:     "/x",
					BaseRef:      "main",
					RegisteredAt: time.Now().UTC(),
				}
				if err := s.RegisterProject(ctx, p, mustNewID(t, "evt_")); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write error: %v", err)
	}
	// Verify all projects and events landed.
	events, err := s.EventsSince(ctx, "", 0, goroutines*perG*2+10)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	// Each RegisterProject appends 1 event.
	if len(events) != goroutines*perG {
		t.Fatalf("event count = %d, want %d", len(events), goroutines*perG)
	}
}

func TestListRuns(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(context.Background(), dir+"/pantheon.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	// Create project + workspace for FK constraints.
	eid := mustNewID(t, "evt_")
	_ = s.RegisterProject(ctx, &domain.Project{
		ProjectID:    "prj_list",
		Name:         "test",
		RepoPath:     "/x",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}, eid)
	eid = mustNewID(t, "evt_")
	_ = s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_list",
		ProjectID:   "prj_list",
		Name:        "test",
		Objective:   "test",
		State:       domain.WorkspaceActive,
		Owner:       "test",
		Host:        "test",
		CreatedAt:   time.Now().UTC(),
	}, eid)

	// Create 3 runs with different states.
	for _, rs := range []struct {
		id    string
		state domain.RunStateV2
	}{
		{"run_a", domain.RunV2Running},
		{"run_b", domain.RunV2Blocked},
		{"run_c", domain.RunV2Running},
	} {
		eid = mustNewID(t, "evt_")
		if err := s.CreateRun(ctx, &domain.Run{
			RunID:       rs.id,
			WorkspaceID: "ws_list",
			BaseCommit:  "abc",
			Budget:      time.Hour,
			State:       rs.state,
		}, eid); err != nil {
			t.Fatalf("CreateRun %s: %v", rs.id, err)
		}
	}

	// List all.
	runs, err := s.ListRuns(ctx, "")
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}

	// List running only.
	runs, err = s.ListRuns(ctx, "running")
	if err != nil {
		t.Fatalf("ListRuns(running): %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 running runs, got %d", len(runs))
	}
	for _, r := range runs {
		if r.State != domain.RunV2Running {
			t.Fatalf("state = %q, want running", r.State)
		}
	}

	// List paused only.
	runs, err = s.ListRuns(ctx, "paused")
	if err != nil {
		t.Fatalf("ListRuns(paused): %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 paused run, got %d", len(runs))
	}
	if runs[0].RunID != "run_b" {
		t.Fatalf("run_id = %q, want run_b", runs[0].RunID)
	}

	// List empty result.
	runs, err = s.ListRuns(ctx, "failed")
	if err != nil {
		t.Fatalf("ListRuns(failed): %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 failed runs, got %d", len(runs))
	}
}

// TestLastEventPopulated verifies that every state-changing store method
// populates the last_event field on the run projection (D4).
func TestLastEventPopulated(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	rid := mustNewID(t, "run_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "a", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	// CreateRun → last_event must be the run.created event_id.
	createEID := mustNewID(t, "evt_")
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: rid, WorkspaceID: wid, BaseCommit: "abc", Budget: time.Hour,
		State: domain.RunV2Requested,
	}, createEID); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	run, _ := s.GetRun(ctx, rid)
	if run.LastEvent != createEID {
		t.Fatalf("after CreateRun: last_event = %q, want %q", run.LastEvent, createEID)
	}

	// UpdateRunState → last_event must be the state_changed event_id.
	planEID := mustNewID(t, "evt_")
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Planning, planEID); err != nil {
		t.Fatalf("UpdateRunState planning: %v", err)
	}
	run, _ = s.GetRun(ctx, rid)
	if run.LastEvent != planEID {
		t.Fatalf("after UpdateRunState(planning): last_event = %q, want %q", run.LastEvent, planEID)
	}

	readyEID := mustNewID(t, "evt_")
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Ready, readyEID); err != nil {
		t.Fatalf("UpdateRunState ready: %v", err)
	}
	run, _ = s.GetRun(ctx, rid)
	if run.LastEvent != readyEID {
		t.Fatalf("after UpdateRunState(ready): last_event = %q, want %q", run.LastEvent, readyEID)
	}

	runningEID := mustNewID(t, "evt_")
	if err := s.UpdateRunState(ctx, rid, domain.RunV2Running, runningEID); err != nil {
		t.Fatalf("UpdateRunState running: %v", err)
	}
	run, _ = s.GetRun(ctx, rid)
	if run.LastEvent != runningEID {
		t.Fatalf("after UpdateRunState(running): last_event = %q, want %q", run.LastEvent, runningEID)
	}

	// RegisterAgent → last_event must be the agent.registered event_id.
	agentEID := mustNewID(t, "evt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: mustNewID(t, "agt_"), RunID: rid, Role: domain.RoleWorker,
		Runtime: "devin", PID: 12345, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, agentEID); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	run, _ = s.GetRun(ctx, rid)
	if run.LastEvent != agentEID {
		t.Fatalf("after RegisterAgent: last_event = %q, want %q", run.LastEvent, agentEID)
	}

	// VerifyRun (PASS) → last_event must be the verify.passed event_id.
	verifyEID := mustNewID(t, "evt_")
	// Register a verifier with a real agent_id (D5: no forged verifier).
	verifierID := mustNewID(t, "agt_")
	verifierEID := mustNewID(t, "evt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: verifierID, RunID: rid, Role: domain.RoleVerifier,
		Runtime: "devin", PID: 0, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, verifierEID); err != nil {
		t.Fatalf("RegisterAgent verifier: %v", err)
	}
	// Use a real event_id from the journal as evidence_ref (D5: no fake evidence).
	events, err := s.EventsSince(ctx, rid, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var evidenceRef string
	for i := range events {
		if events[i].EventType == "agent.registered" {
			evidenceRef = events[i].EventID
			break
		}
	}
	if evidenceRef == "" {
		t.Fatal("no agent.registered event found for evidence_ref")
	}
	finalState, err := s.VerifyRun(ctx, rid, "PASS", verifierID, evidenceRef, verifyEID, "")
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	if finalState != domain.RunV2Completed {
		t.Fatalf("finalState = %q, want completed", finalState)
	}
	run, _ = s.GetRun(ctx, rid)
	if run.LastEvent != verifyEID {
		t.Fatalf("after VerifyRun: last_event = %q, want %q (verify.passed event)", run.LastEvent, verifyEID)
	}
}

// createRunForTest is a helper that creates a project, workspace, and run,
// then drives the run through to the given V2 state. Returns the run ID.
func createRunForTest(t *testing.T, s *Store, ctx context.Context, runID string, budget time.Duration, toState domain.RunStateV2) string {
	t.Helper()
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "test", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: runID, WorkspaceID: wid, BaseCommit: "abc", Budget: budget,
		State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Drive to the target state via the §8.1 state machine.
	if toState == domain.RunV2Requested {
		return runID
	}
	if err := s.UpdateRunState(ctx, runID, domain.RunV2Planning, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> planning: %v", err)
	}
	if toState == domain.RunV2Planning {
		return runID
	}
	if err := s.UpdateRunState(ctx, runID, domain.RunV2Ready, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> ready: %v", err)
	}
	if toState == domain.RunV2Ready {
		return runID
	}
	if err := s.UpdateRunState(ctx, runID, domain.RunV2Running, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("-> running: %v", err)
	}
	if toState == domain.RunV2Running {
		return runID
	}
	if toState == domain.RunV2Blocked {
		if err := s.UpdateRunState(ctx, runID, domain.RunV2Blocked, mustNewID(t, "evt_")); err != nil {
			t.Fatalf("-> blocked: %v", err)
		}
		return runID
	}
	if toState == domain.RunV2Failed {
		if err := s.UpdateRunState(ctx, runID, domain.RunV2Failed, mustNewID(t, "evt_")); err != nil {
			t.Fatalf("-> failed: %v", err)
		}
		return runID
	}
	t.Fatalf("createRunForTest: unsupported target state %q", toState)
	return ""
}

func TestListRunningRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Create runs in various states.
	createRunForTest(t, s, ctx, "run_running_a", time.Hour, domain.RunV2Running)
	createRunForTest(t, s, ctx, "run_running_b", 2*time.Hour, domain.RunV2Running)
	createRunForTest(t, s, ctx, "run_blocked", time.Hour, domain.RunV2Blocked)
	createRunForTest(t, s, ctx, "run_failed", time.Hour, domain.RunV2Failed)
	createRunForTest(t, s, ctx, "run_requested", time.Hour, domain.RunV2Requested)

	runs, err := s.ListRunningRuns(ctx)
	if err != nil {
		t.Fatalf("ListRunningRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 running runs, got %d", len(runs))
	}
	gotIDs := map[string]bool{}
	for _, r := range runs {
		if r.State != domain.RunV2Running {
			t.Fatalf("state = %q, want running", r.State)
		}
		gotIDs[r.RunID] = true
	}
	if !gotIDs["run_running_a"] || !gotIDs["run_running_b"] {
		t.Fatalf("expected run_running_a and run_running_b, got %v", gotIDs)
	}
	// Verify budget is populated from budget_seconds.
	for _, r := range runs {
		if r.Budget <= 0 {
			t.Fatalf("run %s budget = %v, want > 0", r.RunID, r.Budget)
		}
		if r.StartedAt == nil {
			t.Fatalf("run %s started_at is nil", r.RunID)
		}
	}
}

func TestListRunningRunsEmpty(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runs, err := s.ListRunningRuns(ctx)
	if err != nil {
		t.Fatalf("ListRunningRuns: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(runs))
	}
}

func TestFailRunBudgetExceeded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := createRunForTest(t, s, ctx, "run_budget", 8*time.Hour, domain.RunV2Running)

	// Register a running agent for this run.
	agentID := mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: agentID, RunID: rid, Role: domain.RoleWorker,
		Runtime: "devin", PID: 12345, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}

	if err := s.FailRunBudgetExceeded(ctx, rid); err != nil {
		t.Fatalf("FailRunBudgetExceeded: %v", err)
	}

	// Verify run state.
	run, err := s.GetRun(ctx, rid)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.State != domain.RunV2Failed {
		t.Fatalf("run state = %q, want failed", run.State)
	}
	if run.ResultState != domain.ResultBudgetExceeded {
		t.Fatalf("result_state = %q, want budget_exceeded", run.ResultState)
	}
	if run.EndedAt == nil {
		t.Fatal("ended_at should be set")
	}

	// Verify agent was terminalized to 'lost'.
	agent, err := s.GetAgent(ctx, agentID)
	if err != nil {
		t.Fatalf("GetAgent: %v", err)
	}
	if agent.State != domain.AgentLost {
		t.Fatalf("agent state = %q, want lost", agent.State)
	}
	if agent.ExitedAt == nil {
		t.Fatal("agent exited_at should be set")
	}

	// Verify events were appended: a run.state_changed with reason
	// budget_exceeded and an agent.state_changed for the terminalized agent.
	events, err := s.EventsSince(ctx, rid, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var foundRunStateChanged, foundAgentStateChanged bool
	for _, e := range events {
		if e.EventType == "run.state_changed" {
			var p map[string]string
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("unmarshal run.state_changed payload: %v", err)
			}
			if p["reason"] == "budget_exceeded" {
				foundRunStateChanged = true
				if p["to"] != string(domain.RunV2Failed) {
					t.Fatalf("run.state_changed to = %q, want failed", p["to"])
				}
				if p["budget"] == "" {
					t.Fatal("run.state_changed budget field should be populated")
				}
			}
		}
		if e.EventType == "agent.state_changed" && e.AgentID == agentID {
			var p map[string]string
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("unmarshal agent.state_changed payload: %v", err)
			}
			if p["to"] == string(domain.AgentLost) && p["reason"] == "budget_exceeded" {
				foundAgentStateChanged = true
			}
		}
	}
	if !foundRunStateChanged {
		t.Fatal("no run.state_changed event with reason budget_exceeded found")
	}
	if !foundAgentStateChanged {
		t.Fatal("no agent.state_changed event with reason budget_exceeded found")
	}
}

func TestFailRunBudgetExceededTerminalRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// A run already in the failed state — FailRunBudgetExceeded should
	// refuse (it is already terminal).
	rid := createRunForTest(t, s, ctx, "run_already_failed", time.Hour, domain.RunV2Failed)
	err := s.FailRunBudgetExceeded(ctx, rid)
	if err == nil {
		t.Fatal("FailRunBudgetExceeded on a terminal run should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("error code = %q, want CONFLICT", de.Code)
	}
}

func TestFailRunBudgetExceededNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	err := s.FailRunBudgetExceeded(ctx, "run_nonexistent")
	if err == nil {
		t.Fatal("FailRunBudgetExceeded on a nonexistent run should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeNotFound {
		t.Fatalf("error code = %q, want NOT_FOUND", de.Code)
	}
}

// recordChainLink is a helper that records a fulfilled auto-continuation
// linking prevRunID -> successorRunID with the given root cause.
func recordChainLink(t *testing.T, s *Store, ctx context.Context, prevRunID, successorRunID, rootCause string) {
	t.Helper()
	cid := mustNewID(t, "cont_")
	c := &domain.Continuation{
		ContinuationID:     cid,
		RunID:              prevRunID,
		ProjectID:          "prj_test",
		Owner:              "pm",
		SuccessorObjective: "obj",
		RootCause:          rootCause,
	}
	if err := s.RecordAutoContinuation(ctx, c, successorRunID, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RecordAutoContinuation %s->%s: %v", prevRunID, successorRunID, err)
	}
}

func TestCountRootCauseInChain(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Build a chain: run1 -> run2 -> run3 -> run4
	//   run1->run2: exit_code_1
	//   run2->run3: exit_code_1
	//   run3->run4: exit_code_0_incomplete
	recordChainLink(t, s, ctx, "run_chain_1", "run_chain_2", "exit_code_1")
	recordChainLink(t, s, ctx, "run_chain_2", "run_chain_3", "exit_code_1")
	recordChainLink(t, s, ctx, "run_chain_3", "run_chain_4", "exit_code_0_incomplete")

	// Chain ending at run4 (walked back via successor_run_id):
	//   run4 <- run3 (exit_code_0_incomplete) <- run2 (exit_code_1) <- run1 (exit_code_1)
	// exit_code_1 count = 2.
	n, err := s.CountRootCauseInChain(ctx, "run_chain_4", "exit_code_1")
	if err != nil {
		t.Fatalf("CountRootCauseInChain: %v", err)
	}
	if n != 2 {
		t.Fatalf("count exit_code_1 in chain ending run4 = %d, want 2", n)
	}

	// exit_code_0_incomplete count = 1.
	n, err = s.CountRootCauseInChain(ctx, "run_chain_4", "exit_code_0_incomplete")
	if err != nil {
		t.Fatalf("CountRootCauseInChain: %v", err)
	}
	if n != 1 {
		t.Fatalf("count exit_code_0_incomplete = %d, want 1", n)
	}

	// Chain ending at run3: run3 <- run2 (exit_code_1) <- run1 (exit_code_1) = 2.
	n, err = s.CountRootCauseInChain(ctx, "run_chain_3", "exit_code_1")
	if err != nil {
		t.Fatalf("CountRootCauseInChain: %v", err)
	}
	if n != 2 {
		t.Fatalf("count exit_code_1 in chain ending run3 = %d, want 2", n)
	}

	// A run with no predecessor continuations: count = 0.
	n, err = s.CountRootCauseInChain(ctx, "run_chain_1", "exit_code_1")
	if err != nil {
		t.Fatalf("CountRootCauseInChain: %v", err)
	}
	if n != 0 {
		t.Fatalf("count for run with no predecessors = %d, want 0", n)
	}

	// A root cause that never occurred: count = 0.
	n, err = s.CountRootCauseInChain(ctx, "run_chain_4", "exit_code_42")
	if err != nil {
		t.Fatalf("CountRootCauseInChain: %v", err)
	}
	if n != 0 {
		t.Fatalf("count for non-existent root cause = %d, want 0", n)
	}
}

func TestRecordAutoContinuation_StoresRootCause(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	cid := mustNewID(t, "cont_")
	c := &domain.Continuation{
		ContinuationID:     cid,
		RunID:              "run_rc_1",
		ProjectID:          "prj_test",
		Owner:              "pm",
		SuccessorObjective: "obj-rc",
		RootCause:          "exit_code_1",
	}
	if err := s.RecordAutoContinuation(ctx, c, "run_rc_2", mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RecordAutoContinuation: %v", err)
	}

	got, err := s.GetContinuation(ctx, cid)
	if err != nil {
		t.Fatalf("GetContinuation: %v", err)
	}
	if got.RootCause != "exit_code_1" {
		t.Fatalf("root_cause = %q, want exit_code_1", got.RootCause)
	}
	if got.State != domain.ContinuationFulfilled {
		t.Fatalf("state = %q, want fulfilled", got.State)
	}
	if got.SuccessorRunID != "run_rc_2" {
		t.Fatalf("successor_run_id = %q, want run_rc_2", got.SuccessorRunID)
	}
}

// --- TaskSpec / risk-graded verification tests ---

// createTaskTestRun creates a project, workspace, and run for task tests.
func createTaskTestRun(t *testing.T, s *Store, ctx context.Context, runID string) {
	t.Helper()
	pid := mustNewID(t, "prj_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "tasktest", RepoPath: "/x", BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	wid := mustNewID(t, "ws_")
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: runID, WorkspaceID: wid, ProjectID: pid, Owner: "u", BaseCommit: "abc",
		Budget: 1 * time.Hour, State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
}

// TestCreateTaskWithTaskSpec verifies that the TaskSpec fields
// (acceptance_criteria, constraints, deliverables, risk_level) round-trip
// through CreateTask/GetTask.
func TestCreateTaskWithTaskSpec(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	tid := mustNewID(t, "tsk_")
	task := &domain.Task{
		TaskID:             tid,
		RunID:              rid,
		Objective:          "task spec test",
		WorktreePath:       "/tmp/wt",
		State:              domain.TaskReady,
		CreatedAt:          time.Now().UTC(),
		AcceptanceCriteria: []string{"tests pass", "no breaking changes"},
		Constraints:        []string{"no API removal", "keep backwards compat"},
		Deliverables:       []string{"patch", "test results"},
		RiskLevel:          domain.RiskR1,
	}
	if err := s.CreateTask(ctx, task, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, err := s.GetTask(ctx, tid)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got == nil {
		t.Fatal("task not found")
	}
	if len(got.AcceptanceCriteria) != 2 || got.AcceptanceCriteria[0] != "tests pass" {
		t.Fatalf("acceptance_criteria = %v, want [tests pass, no breaking changes]", got.AcceptanceCriteria)
	}
	if len(got.Constraints) != 2 || got.Constraints[0] != "no API removal" {
		t.Fatalf("constraints = %v, want [no API removal, keep backwards compat]", got.Constraints)
	}
	if len(got.Deliverables) != 2 || got.Deliverables[0] != "patch" {
		t.Fatalf("deliverables = %v, want [patch, test results]", got.Deliverables)
	}
	if got.RiskLevel != domain.RiskR1 {
		t.Fatalf("risk_level = %q, want R1", got.RiskLevel)
	}

	// Also verify via GetTaskByRun.
	got2, err := s.GetTaskByRun(ctx, rid)
	if err != nil {
		t.Fatalf("GetTaskByRun: %v", err)
	}
	if got2 == nil || got2.TaskID != tid {
		t.Fatalf("GetTaskByRun returned %v, want task %s", got2, tid)
	}
	if got2.RiskLevel != domain.RiskR1 {
		t.Fatalf("GetTaskByRun risk_level = %q, want R1", got2.RiskLevel)
	}
}

// TestCreateTaskDefaultRiskLevel verifies that a task created with an empty
// RiskLevel defaults to R2 (medium) in the store.
func TestCreateTaskDefaultRiskLevel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	tid := mustNewID(t, "tsk_")
	task := &domain.Task{
		TaskID:       tid,
		RunID:        rid,
		Objective:    "default risk test",
		WorktreePath: "/tmp/wt",
		State:        domain.TaskReady,
		CreatedAt:    time.Now().UTC(),
		// RiskLevel intentionally left empty — store must default to R2.
	}
	if err := s.CreateTask(ctx, task, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	got, err := s.GetTask(ctx, tid)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.RiskLevel != domain.RiskR2 {
		t.Fatalf("default risk_level = %q, want R2", got.RiskLevel)
	}
}

// TestMigrateV10_TaskColumns verifies that the v10 migration added the
// TaskSpec columns to the tasks table with the correct defaults.
func TestMigrateV10_TaskColumns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	// Insert a task via CreateTask (which writes the v10 columns) and
	// read it back via a raw query to confirm the columns exist.
	tid := mustNewID(t, "tsk_")
	if err := s.CreateTask(ctx, &domain.Task{
		TaskID: tid, RunID: rid, Objective: "raw", WorktreePath: "/x",
		State: domain.TaskReady, CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	var ac, cons, del, risk string
	err := s.db.QueryRowContext(ctx,
		`SELECT acceptance_criteria, constraints, deliverables, risk_level FROM tasks WHERE task_id = ?`,
		tid,
	).Scan(&ac, &cons, &del, &risk)
	if err != nil {
		t.Fatalf("raw query v10 columns: %v", err)
	}
	if risk != "R2" {
		t.Fatalf("raw risk_level = %q, want R2 (default)", risk)
	}
	if ac != "[]" {
		t.Fatalf("raw acceptance_criteria = %q, want []", ac)
	}
	if cons != "[]" {
		t.Fatalf("raw constraints = %q, want []", cons)
	}
	if del != "[]" {
		t.Fatalf("raw deliverables = %q, want []", del)
	}
}

// TestVerifyRunApprovalRequired verifies that VerifyRunApprovalRequired
// transitions a running run to verifying (not completed), sets
// next_action=approval_required, and does NOT terminalize agents.
func TestVerifyRunApprovalRequired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	// Drive the run to running.
	for _, st := range []domain.RunStateV2{
		domain.RunV2Planning, domain.RunV2Ready, domain.RunV2Running,
	} {
		if err := s.UpdateRunState(ctx, rid, st, mustNewID(t, "evt_")); err != nil {
			t.Fatalf("UpdateRunState %s: %v", st, err)
		}
	}

	// Register a verifier agent so the run has a nonterminal agent.
	aid := mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: aid, RunID: rid, Role: domain.RoleVerifier, Runtime: "devin",
		PID: 0, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}

	finalState, err := s.VerifyRunApprovalRequired(ctx, rid, "PASS", aid, "evt_evidence", mustNewID(t, "evt_"))
	if err != nil {
		t.Fatalf("VerifyRunApprovalRequired: %v", err)
	}
	if finalState != domain.RunV2Verifying {
		t.Fatalf("finalState = %q, want verifying", finalState)
	}

	run, _ := s.GetRun(ctx, rid)
	if run.State != domain.RunV2Verifying {
		t.Fatalf("run state = %q, want verifying", run.State)
	}
	if run.NextAction != domain.NextActionApprovalRequired {
		t.Fatalf("next_action = %q, want approval_required", run.NextAction)
	}
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted", run.ResultState)
	}

	// Agents must NOT be terminalized (run is not terminal).
	agent, _ := s.GetAgent(ctx, aid)
	if agent == nil {
		t.Fatal("agent not found")
	}
	if agent.State == domain.AgentExited {
		t.Fatal("agent was terminalized but run is not terminal (verifying)")
	}
}

// TestApproveRun verifies that ApproveRun transitions a verifying run to
// completed with result_state=approved and terminalizes agents.
func TestApproveRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	// Drive to running, then to verifying via VerifyRunApprovalRequired.
	for _, st := range []domain.RunStateV2{
		domain.RunV2Planning, domain.RunV2Ready, domain.RunV2Running,
	} {
		if err := s.UpdateRunState(ctx, rid, st, mustNewID(t, "evt_")); err != nil {
			t.Fatalf("UpdateRunState %s: %v", st, err)
		}
	}
	aid := mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: aid, RunID: rid, Role: domain.RoleVerifier, Runtime: "devin",
		PID: 0, State: domain.AgentRunning, StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	if _, err := s.VerifyRunApprovalRequired(ctx, rid, "PASS", aid, "evt_ev", mustNewID(t, "evt_")); err != nil {
		t.Fatalf("VerifyRunApprovalRequired: %v", err)
	}

	// Now approve.
	finalState, err := s.ApproveRun(ctx, rid, "human-pm", "evt_approve", mustNewID(t, "evt_"))
	if err != nil {
		t.Fatalf("ApproveRun: %v", err)
	}
	if finalState != domain.RunV2Completed {
		t.Fatalf("finalState = %q, want completed", finalState)
	}
	run, _ := s.GetRun(ctx, rid)
	if run.State != domain.RunV2Completed {
		t.Fatalf("run state = %q, want completed", run.State)
	}
	if run.ResultState != domain.ResultApproved {
		t.Fatalf("result_state = %q, want approved", run.ResultState)
	}
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("next_action = %q, want none", run.NextAction)
	}

	// Agents must be terminalized.
	agent, _ := s.GetAgent(ctx, aid)
	if agent == nil {
		t.Fatal("agent not found")
	}
	if agent.State != domain.AgentExited {
		t.Fatalf("agent state = %q, want exited (terminalized on approval)", agent.State)
	}

	// A run.approved event must be in the journal.
	events, err := s.EventsSince(ctx, rid, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var foundApproved bool
	for i := range events {
		if events[i].EventType == "run.approved" {
			foundApproved = true
		}
	}
	if !foundApproved {
		t.Fatal("no run.approved event in journal")
	}
}

// TestApproveRunWrongState verifies that ApproveRun rejects a run that is
// not in the verifying state.
func TestApproveRunWrongState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := mustNewID(t, "run_")
	createTaskTestRun(t, s, ctx, rid)

	// Run is in requested state — approve must fail.
	_, err := s.ApproveRun(ctx, rid, "human-pm", "", mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("ApproveRun on a non-verifying run should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("error code = %q, want CONFLICT", de.Code)
	}
}
