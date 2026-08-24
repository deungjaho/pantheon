package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// setupRunWithVerifier creates a run in 'running' state, registers a verifier
// agent, and returns (runID, verifierAgentID, evidenceRef). The evidenceRef
// is a real event_id from the journal.
func setupRunWithVerifier(t *testing.T, s *Store) (runID, verifierID, evidenceRef string) {
	t.Helper()
	ctx := context.Background()
	runID = seedRunForTerminal(t, s, domain.RunV2Requested)
	// Transition to running.
	for _, st := range []domain.RunStateV2{
		domain.RunV2Planning, domain.RunV2Ready, domain.RunV2Running,
	} {
		if err := s.UpdateRunState(ctx, runID, st, mustNewID(t, "evt_")); err != nil {
			t.Fatalf("-> %s: %v", st, err)
		}
	}
	// Register a verifier agent.
	verifierID = mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: verifierID, RunID: runID, Role: domain.RoleVerifier,
		Runtime: "devin", PID: 0, State: domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent verifier: %v", err)
	}
	// Register a worker agent too (to test terminalization of non-verifiers).
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: mustNewID(t, "agt_"), RunID: runID, Role: domain.RoleWorker,
		Runtime: "devin", PID: 99999, State: domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent worker: %v", err)
	}
	// Find a real event_id for evidence_ref.
	events, err := s.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	for i := range events {
		if events[i].EventType == "agent.registered" {
			evidenceRef = events[i].EventID
			break
		}
	}
	if evidenceRef == "" && len(events) > 0 {
		evidenceRef = events[0].EventID
	}
	if evidenceRef == "" {
		t.Fatal("no events found for evidence_ref")
	}
	return runID, verifierID, evidenceRef
}

func TestVerifyRun_PASS_SetsResultStateAccepted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	finalState, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	if finalState != domain.RunV2Completed {
		t.Fatalf("finalState = %q, want completed", finalState)
	}
	run, _ := s.GetRun(ctx, runID)
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("C1 DEFECT: result_state = %q, want accepted", run.ResultState)
	}
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("C4: next_action = %q, want none (default for PASS)", run.NextAction)
	}
}

func TestVerifyRun_FAIL_SetsResultStateFailed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	_, err := s.VerifyRun(ctx, runID, "FAIL", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	run, _ := s.GetRun(ctx, runID)
	if run.ResultState != domain.ResultFailed {
		t.Fatalf("C1 DEFECT: result_state = %q, want failed", run.ResultState)
	}
	if run.NextAction != domain.NextActionBlocked {
		t.Fatalf("C4: next_action = %q, want blocked (default for FAIL)", run.NextAction)
	}
}

func TestVerifyRun_ExplicitNextAction(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), domain.NextActionContinuation)
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	run, _ := s.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionContinuation {
		t.Fatalf("next_action = %q, want continuation", run.NextAction)
	}
}

func TestVerifyRun_InvalidNextActionRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "bogus")
	if err == nil {
		t.Fatal("invalid next_action should be rejected")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", de.Code)
	}
	// Run must NOT have transitioned.
	run, _ := s.GetRun(ctx, runID)
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running (no transition on rejected next_action)", run.State)
	}
}

func TestVerifyRun_TerminalizesAgents(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	// All agents of the run must now be 'exited'.
	agent, _ := s.GetAgentByRun(ctx, runID)
	if agent == nil {
		t.Fatal("no agent found after verify")
	}
	// GetAgentByRun returns the most recent; check all agents via a direct
	// query by listing events for agent.terminalized.
	events, err := s.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var termCount int
	for i := range events {
		if events[i].EventType == "agent.terminalized" {
			termCount++
		}
	}
	// Two agents were registered (verifier + worker), both running → both terminalized.
	if termCount != 2 {
		t.Fatalf("C2: expected 2 agent.terminalized events, got %d", termCount)
	}
	// The most recent agent (worker, registered second) should be exited.
	if agent.State != domain.AgentExited {
		t.Fatalf("C2: agent state = %q, want exited", agent.State)
	}
	if agent.ExitedAt == nil {
		t.Fatal("C2: exited_at should be set")
	}
}

func TestVerifyRun_AgentsAlreadyExitedNotReTerminalized(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	// Exit the verifier before verify (simulating it already stopped).
	if err := s.UpdateAgentState(ctx, verifierID, domain.AgentExited, nil, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("UpdateAgentState: %v", err)
	}
	_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err != nil {
		t.Fatalf("VerifyRun: %v", err)
	}
	events, err := s.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var termCount int
	for i := range events {
		if events[i].EventType == "agent.terminalized" {
			termCount++
		}
	}
	// Only the worker (still running) should be terminalized; the verifier
	// was already exited.
	if termCount != 1 {
		t.Fatalf("expected 1 agent.terminalized event (worker only), got %d", termCount)
	}
}

func TestSetNextAction(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID := seedRunForTerminal(t, s, domain.RunV2Completed)

	// Set next_action.
	if err := s.SetNextAction(ctx, runID, domain.NextActionContinuation, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("SetNextAction 1: %v", err)
	}
	run, _ := s.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionContinuation {
		t.Fatalf("next_action = %q, want continuation", run.NextAction)
	}

	// Idempotent: calling twice updates the value.
	if err := s.SetNextAction(ctx, runID, domain.NextActionNone, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("SetNextAction 2: %v", err)
	}
	run, _ = s.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("next_action = %q, want none (updated)", run.NextAction)
	}

	// A run.next_action_set event should be in the journal.
	events, err := s.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var found int
	for i := range events {
		if events[i].EventType == "run.next_action_set" {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("expected 2 run.next_action_set events, got %d", found)
	}
}

func TestSetNextAction_InvalidRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID := seedRunForTerminal(t, s, domain.RunV2Completed)

	err := s.SetNextAction(ctx, runID, "bogus", mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("invalid next_action should be rejected")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", de.Code)
	}
}

func TestSetNextAction_NonexistentRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	err := s.SetNextAction(ctx, "run_nonexistent", domain.NextActionNone, mustNewID(t, "evt_"))
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", de.Code)
	}
}

func TestTerminalizeAgents_Standalone(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID := seedRunForTerminal(t, s, domain.RunV2Completed)

	// Register a running agent.
	agentID := mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: agentID, RunID: runID, Role: domain.RoleWorker,
		Runtime: "devin", PID: 12345, State: domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	// Terminalize.
	if err := s.TerminalizeAgents(ctx, runID, "run_canceled", "evt_evidence"); err != nil {
		t.Fatalf("TerminalizeAgents: %v", err)
	}
	agent, _ := s.GetAgent(ctx, agentID)
	if agent.State != domain.AgentExited {
		t.Fatalf("agent state = %q, want exited", agent.State)
	}
	if agent.ExitedAt == nil {
		t.Fatal("exited_at should be set")
	}
	if agent.ExitCode != nil {
		t.Fatalf("exit_code = %v, want nil (unknown for terminalized)", agent.ExitCode)
	}
	// An agent.terminalized event should be in the journal.
	events, err := s.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var found bool
	for i := range events {
		if events[i].EventType == "agent.terminalized" {
			found = true
		}
	}
	if !found {
		t.Fatal("agent.terminalized event not found")
	}
}

func TestListMissingNextAction(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// A completed run with no next_action (empty) → should be surfaced.
	rid1 := seedRunForTerminal(t, s, domain.RunV2Completed)
	// A failed run with no next_action → should be surfaced.
	_ = seedRunForTerminal(t, s, domain.RunV2Failed)
	// A running run → should NOT be surfaced (not terminal).
	_ = seedRunForTerminal(t, s, domain.RunV2Running)
	// Set next_action on rid1 → should no longer be surfaced.
	if err := s.SetNextAction(ctx, rid1, domain.NextActionNone, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("SetNextAction: %v", err)
	}

	missing, err := s.ListMissingNextAction(ctx)
	if err != nil {
		t.Fatalf("ListMissingNextAction: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing next_action run, got %d: %+v", len(missing), missing)
	}
	if missing[0].State != domain.RunV2Failed {
		t.Fatalf("state = %q, want failed", missing[0].State)
	}
}

func TestListTerminalRunsWithStaleAgents(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := seedRunForTerminal(t, s, domain.RunV2Completed)
	// Register a running agent on the completed run (stale).
	agentID := mustNewID(t, "agt_")
	if err := s.RegisterAgent(ctx, &domain.Agent{
		AgentID: agentID, RunID: rid, Role: domain.RoleWorker,
		Runtime: "devin", PID: 12345, State: domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	stale, err := s.ListTerminalRunsWithStaleAgents(ctx)
	if err != nil {
		t.Fatalf("ListTerminalRunsWithStaleAgents: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale-agent run, got %d", len(stale))
	}
	if stale[0].RunID != rid {
		t.Fatalf("run_id = %q, want %q", stale[0].RunID, rid)
	}
	if len(stale[0].AgentIDs) != 1 || stale[0].AgentIDs[0] != agentID {
		t.Fatalf("agent_ids = %v, want [%s]", stale[0].AgentIDs, agentID)
	}
	// After terminalization, the run should no longer be surfaced.
	if err := s.TerminalizeAgents(ctx, rid, "reconcile", ""); err != nil {
		t.Fatalf("TerminalizeAgents: %v", err)
	}
	stale, _ = s.ListTerminalRunsWithStaleAgents(ctx)
	if len(stale) != 0 {
		t.Fatalf("expected 0 stale-agent runs after terminalization, got %d", len(stale))
	}
}

// TestVerifyRun_ConcurrentOneWins verifies that two goroutines calling
// VerifyRun on the same run — one wins (transitions to completed), the other
// gets a conflict error (the run is no longer in a transitionable state).
func TestVerifyRun_ConcurrentOneWins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	var wg sync.WaitGroup
	var success, conflict int64
	const goroutines = 2
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
			if err == nil {
				atomic.AddInt64(&success, 1)
			} else {
				de := domain.AsError(err)
				if de.Code == domain.CodeConflict {
					atomic.AddInt64(&conflict, 1)
				}
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatalf("expected exactly 1 success, got %d (conflict=%d)", success, conflict)
	}
	if conflict != 1 {
		t.Fatalf("expected exactly 1 conflict, got %d (success=%d)", conflict, success)
	}
	// The run must be completed with result_state=accepted exactly once.
	run, _ := s.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("run state = %q, want completed", run.State)
	}
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted", run.ResultState)
	}
}

// TestVerifyRun_CrashRollback simulates a transaction failure during VerifyRun
// (after the state transition but before commit) and verifies that the run
// state and result_state both roll back. We force the failure by closing the
// underlying db connection mid-transaction is not safe; instead we verify the
// atomicity guarantee by checking that a rejected next_action (which returns
// an error BEFORE runInTx) leaves the run untouched, and that a conflict
// (run already terminal) leaves result_state untouched.
func TestVerifyRun_RejectedLeavesRunUntouched(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunWithVerifier(t, s)

	// First, reject with invalid next_action — run must stay running.
	_, err := s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "bogus")
	if err == nil {
		t.Fatal("expected error for invalid next_action")
	}
	run, _ := s.GetRun(ctx, runID)
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running (rollback)", run.State)
	}
	// result_state was not set by CreateRun (zero value ""); a rejected verify
	// must not change it.
	if run.ResultState != "" {
		t.Fatalf("result_state = %q, want empty (rollback — not changed)", run.ResultState)
	}

	// Now verify PASS successfully.
	_, err = s.VerifyRun(ctx, runID, "PASS", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err != nil {
		t.Fatalf("VerifyRun PASS: %v", err)
	}
	run, _ = s.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("run state = %q, want completed", run.State)
	}
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted", run.ResultState)
	}

	// A second verify on the now-terminal run must fail (conflict) and NOT
	// change result_state or next_action.
	_, err = s.VerifyRun(ctx, runID, "FAIL", verifierID, evidenceRef, mustNewID(t, "evt_"), "")
	if err == nil {
		t.Fatal("expected conflict on second verify of terminal run")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("code = %q, want CONFLICT", de.Code)
	}
	run, _ = s.GetRun(ctx, runID)
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted (unchanged after conflict)", run.ResultState)
	}
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("next_action = %q, want none (unchanged after conflict)", run.NextAction)
	}
}
