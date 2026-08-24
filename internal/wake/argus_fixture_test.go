package wake

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/store"
)

// TestArgusP2BFixture reproduces the Argus P2-B failure mode end-to-end using
// the real store and reconciler. The real-world case is Argus run
// run_77b243950d58dee9781f26531e39d4c7: orphaned-running (state=running since
// 2026-08-04, PID 560506 dead, no tmux/devin). The reconcile tick must surface
// it as attention-required without auto-creating a successor. The PM then
// explicitly registers a continuation, the tick notifies it, and the PM
// explicitly fulfills it with a successor_run_id.
func TestArgusP2BFixture(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/pantheon.db"
	ctx := context.Background()

	s, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// --- Setup: project, workspace, run in running state, dead agent ---
	pid := "prj_argus"
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "argus", RepoPath: "/home/camt/Work/Argus",
		BaseRef: "main", RegisteredAt: time.Now().UTC(),
	}, "evt_argus_prj"); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	wid := "ws_argus_p2b"
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "argus-p2b",
		Objective: "P2-B browser tool", State: domain.WorkspaceActive,
		Owner: "portfolio-pm", Host: "omarchy", CreatedAt: time.Now().UTC(),
	}, "evt_argus_ws"); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	// The real Argus run_id is used as the fixture name to document the
	// real-world case this test represents.
	runID := "run_77b243950d58dee9781f26531e39d4c7"
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: runID, WorkspaceID: wid, ProjectID: pid, Owner: "portfolio-pm",
		BaseCommit: "abc123", Budget: 8 * time.Hour, State: domain.RunV2Requested,
	}, "evt_argus_run"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Drive to running: requested → planning → ready → running.
	for _, to := range []domain.RunStateV2{
		domain.RunV2Planning, domain.RunV2Ready, domain.RunV2Running,
	} {
		if err := s.UpdateRunState(ctx, runID, to, "evt_argus_"+string(to)); err != nil {
			t.Fatalf("UpdateRunState %s: %v", to, err)
		}
	}

	// Register a worker agent with PID=999999 (does not exist). This is the
	// Argus P2-B condition: the agent record says running, but the PID is dead.
	agentID := "agt_argus_dead"
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: agentID, RunID: runID, Role: domain.RoleWorker,
		Runtime: "devin", PID: 999999, State: domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, "evt_argus_agent"); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}

	// --- Step 1: Run the reconcile tick ---
	rec := NewReconciler(s, s, nil, time.Hour)
	result, err := rec.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 1: %v", err)
	}

	// Verify: orphaned run detected.
	if len(result.OrphanedRuns) != 1 {
		t.Fatalf("orphaned runs = %d, want 1", len(result.OrphanedRuns))
	}
	if result.OrphanedRuns[0].RunID != runID {
		t.Fatalf("orphaned run id = %q, want %q", result.OrphanedRuns[0].RunID, runID)
	}
	if result.OrphanedRuns[0].State != domain.RunV2Running {
		t.Fatalf("orphaned run state = %q, want running", result.OrphanedRuns[0].State)
	}

	// Verify: a wake.continuation message was published for the orphan.
	msgs := messagesByType(t, s, "directive")
	if len(msgs) != 1 {
		t.Fatalf("wake.continuation messages = %d, want 1", len(msgs))
	}
	if !strings.Contains(msgs[0], "orphaned run") {
		t.Fatalf("message does not mention orphaned run: %q", msgs[0])
	}
	if !strings.Contains(msgs[0], runID) {
		t.Fatalf("message does not mention run id: %q", msgs[0])
	}

	// Verify: NO successor run was created (no new run besides the original).
	runs, err := s.ListRuns(ctx, "")
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs after tick = %d, want 1 (no auto-successor)", len(runs))
	}

	// Verify: the run is still in running state (not auto-failed).
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.State != domain.RunV2Running {
		t.Fatalf("run state after tick = %q, want running (not auto-failed)", run.State)
	}

	// --- Step 2: PM explicitly registers a continuation for the orphaned run ---
	contC := &domain.Continuation{
		ContinuationID:     "cont_argus_p2b",
		RunID:              runID,
		ProjectID:          pid,
		Owner:              "portfolio-pm",
		SuccessorObjective: "resume Argus P2-B browser tool work",
	}
	if err := s.RegisterContinuation(ctx, contC, "evt_argus_cont_reg"); err != nil {
		t.Fatalf("RegisterContinuation: %v", err)
	}

	// --- Step 3: Run the tick again — continuation should be notified ---
	result2, err := rec.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	if result2.Notified != 1 {
		t.Fatalf("Tick 2 notified = %d, want 1", result2.Notified)
	}

	// The continuation should now be in notified state.
	gotCont, err := s.GetContinuation(ctx, "cont_argus_p2b")
	if err != nil {
		t.Fatalf("GetContinuation: %v", err)
	}
	if gotCont.State != domain.ContinuationNotified {
		t.Fatalf("continuation state = %q, want notified", gotCont.State)
	}
	if gotCont.WakeCount != 1 {
		t.Fatalf("wake_count = %d, want 1", gotCont.WakeCount)
	}

	// A wake.continuation message for the continuation notification should
	// have been published (in addition to the orphan notifications, which
	// are re-surfaced on each tick since orphans have no dedup). Verify that
	// at least one message mentions the continuation.
	msgs2 := messagesByType(t, s, "directive")
	foundContMsg := false
	for _, m := range msgs2 {
		if strings.Contains(m, "cont_argus_p2b") || strings.Contains(m, "requires successor") {
			foundContMsg = true
			break
		}
	}
	if !foundContMsg {
		t.Fatalf("no continuation notification message found among %d messages", len(msgs2))
	}

	// --- Step 4: PM explicitly fulfills the continuation with a successor ---
	successorRunID := "run_argus_successor_1"
	// Create the successor run (the PM would do this via run.create).
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: successorRunID, WorkspaceID: wid, ProjectID: pid,
		Owner: "portfolio-pm", BaseCommit: "abc123", Budget: 8 * time.Hour,
		State: domain.RunV2Requested,
	}, "evt_argus_successor"); err != nil {
		t.Fatalf("CreateRun successor: %v", err)
	}
	if err := s.FulfillContinuation(ctx, "cont_argus_p2b", successorRunID, "evt_argus_cont_fulfill"); err != nil {
		t.Fatalf("FulfillContinuation: %v", err)
	}

	// Verify: continuation state=fulfilled, successor linked.
	gotCont2, err := s.GetContinuation(ctx, "cont_argus_p2b")
	if err != nil {
		t.Fatalf("GetContinuation after fulfill: %v", err)
	}
	if gotCont2.State != domain.ContinuationFulfilled {
		t.Fatalf("continuation state = %q, want fulfilled", gotCont2.State)
	}
	if gotCont2.SuccessorRunID != successorRunID {
		t.Fatalf("successor_run_id = %q, want %q", gotCont2.SuccessorRunID, successorRunID)
	}
	if gotCont2.FulfilledAt == nil {
		t.Fatal("fulfilled_at should be set")
	}

	// Verify: the continuation no longer appears in the pending list.
	pending, err := s.ListPendingContinuations(ctx)
	if err != nil {
		t.Fatalf("ListPendingContinuations: %v", err)
	}
	for _, c := range pending {
		if c.ContinuationID == "cont_argus_p2b" {
			t.Fatal("fulfilled continuation should not appear in pending list")
		}
	}
}

// messagesByType reads all "message" events from the store and returns the
// inline payloads of those whose typed envelope message_type matches.
func messagesByType(t *testing.T, s *store.Store, msgType string) []string {
	t.Helper()
	events, err := s.EventsSince(context.Background(), "", 0, 1000)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var bodies []string
	for _, e := range events {
		if e.EventType != "message" {
			continue
		}
		var msg domain.Message
		if err := json.Unmarshal(e.Payload, &msg); err != nil {
			continue
		}
		if string(msg.Type) == msgType {
			bodies = append(bodies, msg.PayloadRef.Inline)
		}
	}
	return bodies
}
