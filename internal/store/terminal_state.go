package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// terminalizeAgentsInTx closes all nonterminal agents (registered/starting/
// running) of a run within the given transaction (ADR-0018, C2). Each closed
// agent gets:
//   - state = 'exited'
//   - exited_at = now
//   - exit_code = NULL (the real exit code is unknown)
//   - an agent.terminalized event appended with
//     {agent_id, run_id, reason, run_state, evidence_ref}
//
// This is the low-level tx-scoped helper used by VerifyRun (same tx) so the
// run state transition and agent terminalization are atomic.
func terminalizeAgentsInTx(tx *sql.Tx, runID, reason, evidenceRef string, runState domain.RunStateV2) error {
	rows, err := tx.Query(
		`SELECT agent_id FROM agents WHERE run_id = ? AND state IN ('registered','starting','running')`,
		runID,
	)
	if err != nil {
		return fmt.Errorf("query nonterminal agents: %w", err)
	}
	var agentIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan agent id: %w", err)
		}
		agentIDs = append(agentIDs, id)
	}
	rows.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, id := range agentIDs {
		// Close the agent: state=exited, exited_at=now, exit_code=NULL.
		// exit_code is set to NULL explicitly (COALESCE would preserve an
		// existing value; we want to clear it since the real code is
		// unknown for a terminalized agent).
		if _, err := tx.Exec(
			`UPDATE agents SET state = 'exited', exited_at = ?, exit_code = NULL WHERE agent_id = ?`,
			now, id,
		); err != nil {
			return fmt.Errorf("update agent %s: %w", id, err)
		}
		// Append an agent.terminalized event for auditability.
		evtID, err := domain.NewID("evt_")
		if err != nil {
			return fmt.Errorf("new terminalize event id: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   evtID,
			RunID:     runID,
			AgentID:   id,
			EventType: "agent.terminalized",
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"agent_id":     id,
				"run_id":       runID,
				"reason":       reason,
				"run_state":    string(runState),
				"evidence_ref": evidenceRef,
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append agent.terminalized event: %w", err)
		}
	}
	// Update last_event on the run projection if any agents were closed.
	// (The last terminalize event is the latest; we leave last_event as the
	// verify event set by the caller, since the verify event is the
	// semantically latest event for the run.)
	return nil
}

// TerminalizeAgents closes all nonterminal agents (registered/starting/
// running) of a run in one transaction (ADR-0018, C2). This is the
// standalone, public entry point used by handleRunCancel and any other
// path that transitions a run to a terminal state outside VerifyRun.
//
// Each closed agent gets state='exited', exited_at=now, exit_code=NULL, and
// an agent.terminalized event with {agent_id, run_id, reason, run_state,
// evidence_ref}. The runState is read from the run projection inside the
// same transaction.
func (s *Store) TerminalizeAgents(ctx context.Context, runID, reason, evidenceRef string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var stateStr string
		err := tx.QueryRow(`SELECT state FROM runs WHERE run_id = ?`, runID).Scan(&stateStr)
		if err != nil {
			if err == sql.ErrNoRows {
				return domain.ErrNotFound("run not found: " + runID)
			}
			return fmt.Errorf("select run state: %w", err)
		}
		return terminalizeAgentsInTx(tx, runID, reason, evidenceRef, domain.RunStateV2(stateStr))
	})
}

// SetNextAction sets the next_action decision on a run (ADR-0018, C4). This
// is an explicit PM action — it allows the PM to set or change the
// next_action after verify (e.g., when a continuation is created later).
// Idempotent: calling it twice updates the value. Appends a
// run.next_action_set event.
func (s *Store) SetNextAction(ctx context.Context, runID string, action domain.NextAction, eventID string) error {
	if !domain.ValidNextAction(action) {
		return domain.ErrInvalidInput("invalid next_action: " + string(action))
	}
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var tmp string
		err := tx.QueryRow(`SELECT run_id FROM runs WHERE run_id = ?`, runID).Scan(&tmp)
		if err != nil {
			if err == sql.ErrNoRows {
				return domain.ErrNotFound("run not found: " + runID)
			}
			return fmt.Errorf("select run: %w", err)
		}
		if _, err := tx.Exec(
			`UPDATE runs SET next_action = ?, last_event = ? WHERE run_id = ?`,
			string(action), eventID, runID,
		); err != nil {
			return fmt.Errorf("update next_action: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "run.next_action_set",
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"run_id":      runID,
				"next_action": string(action),
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append run.next_action_set event: %w", err)
		}
		return nil
	})
}

// MissingNextActionRun is a terminal run (completed/failed/canceled) with an
// empty next_action — the "missing decision" case the reconcile tick surfaces
// (ADR-0018, C4).
type MissingNextActionRun struct {
	RunID       string             `json:"run_id"`
	State       domain.RunStateV2  `json:"state"`
	ResultState domain.ResultState `json:"result_state"`
	Owner       string             `json:"owner"`
	ProjectID   string             `json:"project_id"`
}

// ListMissingNextAction returns terminal runs (completed/failed/canceled)
// with an empty next_action (ADR-0018, C4). These are runs where the PM has
// not recorded an explicit next-action decision. The reconcile tick surfaces
// them so the portfolio never goes silently idle.
func (s *Store) ListMissingNextAction(ctx context.Context) ([]MissingNextActionRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		`SELECT run_id, state, result_state, owner, project_id
		 FROM runs
		 WHERE state IN ('completed','failed','canceled') AND (next_action = '' OR next_action IS NULL)
		 ORDER BY run_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list missing next_action: %w", err)
	}
	defer rows.Close()
	var out []MissingNextActionRun
	for rows.Next() {
		var r MissingNextActionRun
		if err := rows.Scan(&r.RunID, &r.State, &r.ResultState, &r.Owner, &r.ProjectID); err != nil {
			return nil, fmt.Errorf("scan missing next_action: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// StaleAgentRun is a terminal run (completed/failed/canceled) that still has
// nonterminal agents (registered/starting/running) — the "stale agents" case
// the reconcile tick surfaces (ADR-0018, C2).
type StaleAgentRun struct {
	RunID    string            `json:"run_id"`
	State    domain.RunStateV2 `json:"state"`
	AgentIDs []string          `json:"agent_ids"`
}

// ListTerminalRunsWithStaleAgents returns terminal runs that still have
// nonterminal agents (ADR-0018, C2). These are runs where agent
// terminalization did not happen (e.g., a run marked failed by crash
// reconciliation without going through VerifyRun). The reconcile tick
// surfaces them so the PM can terminalize the agents.
func (s *Store) ListTerminalRunsWithStaleAgents(ctx context.Context) ([]StaleAgentRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		`SELECT r.run_id, r.state, a.agent_id
		 FROM runs r
		 JOIN agents a ON a.run_id = r.run_id
		 WHERE r.state IN ('completed','failed','canceled')
		   AND a.state IN ('registered','starting','running')
		 ORDER BY r.run_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list terminal runs with stale agents: %w", err)
	}
	defer rows.Close()
	byRun := make(map[string]*StaleAgentRun)
	var order []string
	for rows.Next() {
		var runID, state, agentID string
		if err := rows.Scan(&runID, &state, &agentID); err != nil {
			return nil, fmt.Errorf("scan stale agent: %w", err)
		}
		r, ok := byRun[runID]
		if !ok {
			r = &StaleAgentRun{RunID: runID, State: domain.RunStateV2(state)}
			byRun[runID] = r
			order = append(order, runID)
		}
		r.AgentIDs = append(r.AgentIDs, agentID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	out := make([]StaleAgentRun, 0, len(order))
	for _, id := range order {
		out = append(out, *byRun[id])
	}
	return out, nil
}

// SupersededRun is a run that has been superseded by a successor
// (ADR-0018, C3). The reconcile tick surfaces these so the PM can verify the
// old run is terminalized and the successor is progressing.
type SupersededRun struct {
	OldRunID       string `json:"old_run_id"`
	SuccessorRunID string `json:"successor_run_id"`
	OldRunState    string `json:"old_run_state"`
	Reason         string `json:"reason"`
}

// ListSupersededRuns returns all supersede records joined with the old run's
// current state (ADR-0018, C3). The reconcile tick surfaces these so the PM
// can verify the old run is terminalized and the successor is progressing.
func (s *Store) ListSupersededRuns(ctx context.Context) ([]SupersededRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		`SELECT sup.old_run_id, sup.successor_run_id, r.state, sup.reason
		 FROM supersedes sup
		 JOIN runs r ON r.run_id = sup.old_run_id
		 ORDER BY sup.created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list superseded runs: %w", err)
	}
	defer rows.Close()
	var out []SupersededRun
	for rows.Next() {
		var r SupersededRun
		if err := rows.Scan(&r.OldRunID, &r.SuccessorRunID, &r.OldRunState, &r.Reason); err != nil {
			return nil, fmt.Errorf("scan superseded run: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
