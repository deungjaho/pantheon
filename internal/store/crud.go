package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// --- Projects ---

// RegisterProject inserts a project and appends a project.registered event
// in the same transaction.
func (s *Store) RegisterProject(ctx context.Context, p *domain.Project, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO projects(project_id, name, repo_path, base_ref, registered_at)
			 VALUES(?, ?, ?, ?, ?)`,
			p.ProjectID, p.Name, p.RepoPath, p.BaseRef, p.RegisteredAt.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert project: %w", err)
		}
		_, err := appendEvent(tx, &domain.Event{
			EventID:   eventID,
			EventType: "project.registered",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"project_id": p.ProjectID, "name": p.Name}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// GetProject returns a project by ID.
func (s *Store) GetProject(ctx context.Context, projectID string) (*domain.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p domain.Project
	var registeredAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT project_id, name, repo_path, base_ref, registered_at FROM projects WHERE project_id = ?`,
		projectID,
	).Scan(&p.ProjectID, &p.Name, &p.RepoPath, &p.BaseRef, &registeredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	p.RegisteredAt, err = time.Parse(time.RFC3339Nano, registeredAt)
	if err != nil {
		return nil, fmt.Errorf("parse registered_at: %w", err)
	}
	return &p, nil
}

// ListProjects returns all projects ordered by registered_at ascending.
func (s *Store) ListProjects(ctx context.Context) ([]*domain.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx,
		`SELECT project_id, name, repo_path, base_ref, registered_at FROM projects ORDER BY registered_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	var out []*domain.Project
	for rows.Next() {
		var p domain.Project
		var registeredAt string
		if err := rows.Scan(&p.ProjectID, &p.Name, &p.RepoPath, &p.BaseRef, &registeredAt); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		p.RegisteredAt, err = time.Parse(time.RFC3339Nano, registeredAt)
		if err != nil {
			return nil, fmt.Errorf("parse registered_at: %w", err)
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

// --- Workspaces ---

// CreateWorkspace inserts a workspace and appends a workspace.created event.
func (s *Store) CreateWorkspace(ctx context.Context, w *domain.Workspace, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO workspaces(workspace_id, project_id, name, objective, state, owner, host, created_at)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			w.WorkspaceID, w.ProjectID, w.Name, w.Objective, string(w.State),
			w.Owner, w.Host, w.CreatedAt.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert workspace: %w", err)
		}
		_, err := appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     "",
			EventType: "workspace.created",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"workspace_id": w.WorkspaceID, "name": w.Name}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// GetWorkspace returns a workspace by ID.
func (s *Store) GetWorkspace(ctx context.Context, workspaceID string) (*domain.Workspace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var w domain.Workspace
	var createdAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT workspace_id, project_id, name, objective, state, owner, host, created_at
		 FROM workspaces WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&w.WorkspaceID, &w.ProjectID, &w.Name, &w.Objective, &w.State, &w.Owner, &w.Host, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get workspace: %w", err)
	}
	w.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	return &w, nil
}

// --- Runs ---

// CreateRun inserts a run and appends a run.created event.
func (s *Store) CreateRun(ctx context.Context, r *domain.Run, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		evidence := r.Evidence
		if evidence == nil {
			evidence = []string{}
		}
		evBytes, err := json.Marshal(evidence)
		if err != nil {
			return fmt.Errorf("marshal evidence: %w", err)
		}
		if _, err := tx.Exec(
			`INSERT INTO runs(run_id, workspace_id, project_id, owner, base_commit, budget_seconds, state, result_state,
				epoch, lease_holder, lease_renew_deadline, last_event, checkpoint, evidence, next_action)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.RunID, r.WorkspaceID, r.ProjectID, r.Owner, r.BaseCommit, int64(r.Budget.Seconds()),
			string(r.State), string(r.ResultState),
			r.Epoch, r.Lease.Holder, r.Lease.RenewDeadline, eventID, r.Checkpoint, string(evBytes),
			string(r.NextAction),
		); err != nil {
			return fmt.Errorf("insert run: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     r.RunID,
			EventType: "run.created",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"run_id": r.RunID, "base_commit": r.BaseCommit}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// UpdateRunState transitions a run's state, validates the transition via
// the §8.1 state machine (CheckRunTransitionV2 — the authoritative path),
// and appends a run.state_changed event in one transaction. The to state
// is a V2 state (requested/planning/ready/running/verifying/completed/
// blocked/failed/canceled).
func (s *Store) UpdateRunState(ctx context.Context, runID string, to domain.RunStateV2, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr string
		err := tx.QueryRow(`SELECT state FROM runs WHERE run_id = ?`, runID).Scan(&fromStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run state: %w", err)
		}
		from := domain.RunStateV2(fromStr)
		if err := domain.CheckRunTransitionV2(from, to); err != nil {
			return domain.ErrConflict(err.Error())
		}
		var startedAt, endedAt *string
		now := time.Now().UTC().Format(time.RFC3339Nano)
		switch to {
		case domain.RunV2Running:
			if from == domain.RunV2Planning || from == domain.RunV2Ready {
				s := now
				startedAt = &s
			}
		case domain.RunV2Completed, domain.RunV2Failed, domain.RunV2Canceled:
			e := now
			endedAt = &e
		}
		if _, err := tx.Exec(
			`UPDATE runs SET state = ?, started_at = COALESCE(?, started_at), ended_at = COALESCE(?, ended_at), last_event = ? WHERE run_id = ?`,
			string(to), startedAt, endedAt, eventID, runID,
		); err != nil {
			return fmt.Errorf("update run state: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "run.state_changed",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"from": string(from), "to": string(to)}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// AcquireRunLease sets the lease holder and renew deadline on a run and
// increments the epoch, in one transaction with an event append. This is
// used by agent.register to claim a run.
func (s *Store) AcquireRunLease(ctx context.Context, runID, holder string, renewDeadline int64, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var runIDCheck string
		err := tx.QueryRow(`SELECT run_id FROM runs WHERE run_id = ?`, runID).Scan(&runIDCheck)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run for lease: %w", err)
		}
		if _, err := tx.Exec(
			`UPDATE runs SET lease_holder = ?, lease_renew_deadline = ?, epoch = epoch + 1, last_event = ? WHERE run_id = ?`,
			holder, renewDeadline, eventID, runID,
		); err != nil {
			return fmt.Errorf("acquire lease: %w", err)
		}
		// Record the new epoch on the holder agent so run.verify can
		// reject stale verifiers whose epoch no longer matches the run.
		// The holder is the agent_id (set by handleAgentRegister). If the
		// holder is not a registered agent, this UPDATE affects 0 rows.
		var newEpoch int
		if err := tx.QueryRow(`SELECT epoch FROM runs WHERE run_id = ?`, runID).Scan(&newEpoch); err != nil {
			return fmt.Errorf("select new epoch: %w", err)
		}
		if _, err := tx.Exec(`UPDATE agents SET epoch = ? WHERE agent_id = ?`, newEpoch, holder); err != nil {
			return fmt.Errorf("update agent epoch: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "run.lease_acquired",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]any{"holder": holder, "renew_deadline": renewDeadline}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// RenewRunLease extends the renew deadline on a run's existing lease and
// appends a heartbeat event. Used by agent.heartbeat.
func (s *Store) RenewRunLease(ctx context.Context, runID string, renewDeadline int64, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var holder string
		err := tx.QueryRow(`SELECT lease_holder FROM runs WHERE run_id = ?`, runID).Scan(&holder)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select lease: %w", err)
		}
		if _, err := tx.Exec(
			`UPDATE runs SET lease_renew_deadline = ?, last_event = ? WHERE run_id = ?`,
			renewDeadline, eventID, runID,
		); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "agent.heartbeat",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]any{"holder": holder, "renew_deadline": renewDeadline}),
			Timestamp: time.Now().UTC(),
		})
		return err
	})
}

// SetRunLastEvent records the last event ID on a run projection. This
// is a low-level helper; state-changing methods (UpdateRunState,
// VerifyRun, RegisterAgent, etc.) set last_event inline within their
// transactions. This method is retained for callers that append events
// outside the standard state-change paths.
func (s *Store) SetRunLastEvent(ctx context.Context, runID, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET last_event = ? WHERE run_id = ?`, eventID, runID)
	if err != nil {
		return fmt.Errorf("set last_event: %w", err)
	}
	return nil
}

// VerifyRun transitions a run to a terminal state based on a typed verdict
// and appends a verify.passed or verify.failed event with the verifier
// identity and evidence reference in the same transaction (control-plane
// §3.3, §8.1, acceptance-contract G3-VERIFY).
//
// On PASS: the run transitions through the §8.1 verifying state to
// completed. From running: running → verifying → completed. From blocked:
// blocked → running → verifying → completed (resume first, since §8.1
// does not allow blocked → completed directly). The verify.passed event
// records the verdict, verifier, and evidence reference.
//
// On FAIL: the run transitions to failed. From running/verifying: direct.
// From blocked: blocked → failed (§8.1 allows this). The verify.failed
// event records the verdict, verifier, and evidence reference.
//
// The evidence reference is also appended to the run's evidence slice in the
// projection so the verdict trail is queryable from the run record.
//
// completed is only set by an explicit PASS verdict here — never manufactured
// by a worker's self-report or a stub.
//
// ADR-0018 (C1, C2, C4): VerifyRun now atomically projects:
//   - result_state: PASS → 'accepted', FAIL → 'failed' (C1)
//   - next_action: if nextAction is non-empty it is used; otherwise PASS
//     defaults to 'none' and FAIL defaults to 'blocked' (C4)
//   - agent terminalization: all nonterminal agents of the run are closed
//     with state='exited', exited_at=now, exit_code=NULL, and an
//     agent.terminalized event (C2)
//
// All of the above happen in the same transaction as the state transition,
// so the run state, result_state, next_action, and agent states can never
// diverge after a verify.
func (s *Store) VerifyRun(ctx context.Context, runID string, verdict string, verifierAgentID, evidenceRef, eventID string, nextAction domain.NextAction) (domain.RunStateV2, error) {
	var finalState domain.RunStateV2
	var eventType string
	var resultState domain.ResultState
	switch verdict {
	case "PASS":
		finalState = domain.RunV2Completed
		eventType = "verify.passed"
		resultState = domain.ResultAccepted
		if nextAction == "" {
			nextAction = domain.NextActionNone
		}
	case "FAIL":
		finalState = domain.RunV2Failed
		eventType = "verify.failed"
		resultState = domain.ResultFailed
		if nextAction == "" {
			nextAction = domain.NextActionBlocked
		}
	default:
		return "", domain.ErrInvalidInput("invalid verdict: " + verdict + " (must be PASS or FAIL)")
	}
	if nextAction != "" && !domain.ValidNextAction(nextAction) {
		return "", domain.ErrInvalidInput("invalid next_action: " + string(nextAction))
	}

	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr string
		err := tx.QueryRow(`SELECT state FROM runs WHERE run_id = ?`, runID).Scan(&fromStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run state: %w", err)
		}
		from := domain.RunStateV2(fromStr)

		// Helper to apply a single V2 transition within this tx.
		applyTransition := func(to domain.RunStateV2, setEnded bool) error {
			if err := domain.CheckRunTransitionV2(from, to); err != nil {
				return domain.ErrConflict(err.Error())
			}
			// Generate the event_id first so we can set last_event in the
			// same UPDATE that changes the state.
			seID, err := domain.NewID("evt_")
			if err != nil {
				return fmt.Errorf("new event id: %w", err)
			}
			if setEnded {
				now := time.Now().UTC().Format(time.RFC3339Nano)
				if _, err := tx.Exec(
					`UPDATE runs SET state = ?, ended_at = COALESCE(?, ended_at), last_event = ? WHERE run_id = ?`,
					string(to), now, seID, runID,
				); err != nil {
					return fmt.Errorf("update run state: %w", err)
				}
			} else {
				if _, err := tx.Exec(
					`UPDATE runs SET state = ?, last_event = ? WHERE run_id = ?`,
					string(to), seID, runID,
				); err != nil {
					return fmt.Errorf("update run state: %w", err)
				}
			}
			// Append a run.state_changed event for auditability.
			_, err = appendEvent(tx, &domain.Event{
				EventID:   seID,
				RunID:     runID,
				EventType: "run.state_changed",
				Severity:  domain.SeverityInfo,
				Payload:   mustMarshal(map[string]string{"from": string(from), "to": string(to)}),
				Timestamp: time.Now().UTC(),
			})
			if err != nil {
				return fmt.Errorf("append state_changed event: %w", err)
			}
			from = to
			return nil
		}

		if verdict == "PASS" {
			// PASS: drive through the §8.1 verifying state to completed.
			// From blocked: blocked → running → verifying → completed.
			// From running: running → verifying → completed.
			if from == domain.RunV2Blocked {
				if err := applyTransition(domain.RunV2Running, false); err != nil {
					return err
				}
			}
			if err := applyTransition(domain.RunV2Verifying, false); err != nil {
				return err
			}
			if err := applyTransition(domain.RunV2Completed, true); err != nil {
				return err
			}
		} else {
			// FAIL: from → failed (§8.1 allows running/verifying/blocked → failed).
			if err := applyTransition(domain.RunV2Failed, true); err != nil {
				return err
			}
		}

		// Append the evidence reference to the run's evidence slice.
		if evidenceRef != "" {
			var evidenceJSON string
			err := tx.QueryRow(`SELECT evidence FROM runs WHERE run_id = ?`, runID).Scan(&evidenceJSON)
			if err != nil {
				return fmt.Errorf("select evidence: %w", err)
			}
			var evidence []string
			if evidenceJSON != "" {
				_ = json.Unmarshal([]byte(evidenceJSON), &evidence)
			}
			evidence = append(evidence, evidenceRef)
			evBytes, err := json.Marshal(evidence)
			if err != nil {
				return fmt.Errorf("marshal evidence: %w", err)
			}
			if _, err := tx.Exec(`UPDATE runs SET evidence = ? WHERE run_id = ?`, string(evBytes), runID); err != nil {
				return fmt.Errorf("update evidence: %w", err)
			}
		}

		// Append the verify event with verdict, verifier, and evidence.
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: eventType,
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"verifier_agent_id": verifierAgentID,
				"verdict":           verdict,
				"evidence_ref":      evidenceRef,
				"result_state":      string(resultState),
				"next_action":       string(nextAction),
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append verify event: %w", err)
		}
		// ADR-0018 C1/C4: atomically project result_state and next_action
		// in the same transaction as the state transition.
		if _, err := tx.Exec(
			`UPDATE runs SET result_state = ?, next_action = ?, last_event = ? WHERE run_id = ?`,
			string(resultState), string(nextAction), eventID, runID,
		); err != nil {
			return fmt.Errorf("update result_state/next_action: %w", err)
		}

		// ADR-0018 C2: terminalize all nonterminal agents of this run in
		// the same transaction. The run is now in a terminal state; any
		// agent still in registered/starting/running must be closed with
		// an explicit reason and evidence.
		if err := terminalizeAgentsInTx(tx, runID, "run_terminalized", evidenceRef, finalState); err != nil {
			return fmt.Errorf("terminalize agents: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return finalState, nil
}

// VerifyRunApprovalRequired records a PASS verdict for a high-risk (R2/R3)
// run but does NOT transition to completed. Instead it drives the §8.1
// state machine only as far as the verifying state (running → verifying,
// or blocked → running → verifying) and atomically projects:
//   - result_state = 'accepted' (the verify itself passed)
//   - next_action = 'approval_required' (human sign-off still needed)
//   - a verify.passed event with the verdict, verifier, and evidence
//   - the evidence reference appended to the run's evidence slice
//
// Agents are NOT terminalized — the run is not yet terminal. The run stays
// in verifying until run.approve (or run.verify with an approval flag)
// transitions it to completed via ApproveRun.
//
// This is the risk-graded-verification gate: R0/R1 auto-accept via VerifyRun;
// R2/R3 stop at verifying and require human approval.
func (s *Store) VerifyRunApprovalRequired(ctx context.Context, runID string, verdict string, verifierAgentID, evidenceRef, eventID string) (domain.RunStateV2, error) {
	if verdict != "PASS" {
		return "", domain.ErrInvalidInput("VerifyRunApprovalRequired only accepts PASS verdict, got: " + verdict)
	}
	finalState := domain.RunV2Verifying
	eventType := "verify.passed"
	resultState := domain.ResultAccepted
	nextAction := domain.NextActionApprovalRequired

	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr string
		err := tx.QueryRow(`SELECT state FROM runs WHERE run_id = ?`, runID).Scan(&fromStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run state: %w", err)
		}
		from := domain.RunStateV2(fromStr)

		applyTransition := func(to domain.RunStateV2) error {
			if err := domain.CheckRunTransitionV2(from, to); err != nil {
				return domain.ErrConflict(err.Error())
			}
			seID, err := domain.NewID("evt_")
			if err != nil {
				return fmt.Errorf("new event id: %w", err)
			}
			if _, err := tx.Exec(
				`UPDATE runs SET state = ?, last_event = ? WHERE run_id = ?`,
				string(to), seID, runID,
			); err != nil {
				return fmt.Errorf("update run state: %w", err)
			}
			_, err = appendEvent(tx, &domain.Event{
				EventID:   seID,
				RunID:     runID,
				EventType: "run.state_changed",
				Severity:  domain.SeverityInfo,
				Payload:   mustMarshal(map[string]string{"from": string(from), "to": string(to)}),
				Timestamp: time.Now().UTC(),
			})
			if err != nil {
				return fmt.Errorf("append state_changed event: %w", err)
			}
			from = to
			return nil
		}

		// PASS but approval-required: drive only to verifying (not completed).
		// From blocked: blocked → running → verifying.
		// From running: running → verifying.
		if from == domain.RunV2Blocked {
			if err := applyTransition(domain.RunV2Running); err != nil {
				return err
			}
		}
		if err := applyTransition(domain.RunV2Verifying); err != nil {
			return err
		}

		// Append the evidence reference to the run's evidence slice.
		if evidenceRef != "" {
			var evidenceJSON string
			err := tx.QueryRow(`SELECT evidence FROM runs WHERE run_id = ?`, runID).Scan(&evidenceJSON)
			if err != nil {
				return fmt.Errorf("select evidence: %w", err)
			}
			var evidence []string
			if evidenceJSON != "" {
				_ = json.Unmarshal([]byte(evidenceJSON), &evidence)
			}
			evidence = append(evidence, evidenceRef)
			evBytes, err := json.Marshal(evidence)
			if err != nil {
				return fmt.Errorf("marshal evidence: %w", err)
			}
			if _, err := tx.Exec(`UPDATE runs SET evidence = ? WHERE run_id = ?`, string(evBytes), runID); err != nil {
				return fmt.Errorf("update evidence: %w", err)
			}
		}

		// Append the verify.passed event with verdict, verifier, and evidence.
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: eventType,
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"verifier_agent_id": verifierAgentID,
				"verdict":           verdict,
				"evidence_ref":      evidenceRef,
				"result_state":      string(resultState),
				"next_action":       string(nextAction),
				"approval_required": "true",
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append verify event: %w", err)
		}
		// Atomically project result_state and next_action.
		if _, err := tx.Exec(
			`UPDATE runs SET result_state = ?, next_action = ?, last_event = ? WHERE run_id = ?`,
			string(resultState), string(nextAction), eventID, runID,
		); err != nil {
			return fmt.Errorf("update result_state/next_action: %w", err)
		}
		// Agents are NOT terminalized here — the run is not terminal yet.
		return nil
	})
	if err != nil {
		return "", err
	}
	return finalState, nil
}

// ApproveRun transitions a run from the verifying state (with
// next_action=approval_required) to completed, recording human approval
// (risk-graded verification, R2/R3). It atomically:
//   - validates the run is in the verifying state
//   - transitions verifying → completed (sets ended_at)
//   - projects result_state = 'approved' and next_action = 'none'
//   - appends a run.approved event with the approver identity and evidence
//   - terminalizes all nonterminal agents of the run
//
// Approver is the human identifier (not an agent_id). EvidenceRef is
// optional supporting evidence for the approval decision.
func (s *Store) ApproveRun(ctx context.Context, runID, approver, evidenceRef, eventID string) (domain.RunStateV2, error) {
	finalState := domain.RunV2Completed
	resultState := domain.ResultApproved
	nextAction := domain.NextActionNone

	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr string
		err := tx.QueryRow(`SELECT state FROM runs WHERE run_id = ?`, runID).Scan(&fromStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run state: %w", err)
		}
		from := domain.RunStateV2(fromStr)
		if from != domain.RunV2Verifying {
			return domain.ErrConflict(fmt.Sprintf("run.approve requires verifying state, got %s", from))
		}
		// verifying → completed (§8.1 allows this).
		if err := domain.CheckRunTransitionV2(from, finalState); err != nil {
			return domain.ErrConflict(err.Error())
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.Exec(
			`UPDATE runs SET state = ?, ended_at = COALESCE(?, ended_at), last_event = ? WHERE run_id = ?`,
			string(finalState), now, eventID, runID,
		); err != nil {
			return fmt.Errorf("update run state: %w", err)
		}
		// Generate a distinct event_id for the state_changed event so it
		// does not collide with the run.approved event (appendEvent is
		// idempotent on event_id — a duplicate would be a silent no-op).
		seID, err := domain.NewID("evt_")
		if err != nil {
			return fmt.Errorf("new state_changed event id: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   seID,
			RunID:     runID,
			EventType: "run.state_changed",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"from": string(from), "to": string(finalState)}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append state_changed event: %w", err)
		}
		// Append the run.approved event with the approver and evidence.
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "run.approved",
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"approver":     approver,
				"evidence_ref": evidenceRef,
				"result_state": string(resultState),
				"next_action":  string(nextAction),
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append run.approved event: %w", err)
		}
		// Atomically project result_state and next_action.
		if _, err := tx.Exec(
			`UPDATE runs SET result_state = ?, next_action = ?, last_event = ? WHERE run_id = ?`,
			string(resultState), string(nextAction), eventID, runID,
		); err != nil {
			return fmt.Errorf("update result_state/next_action: %w", err)
		}
		// Terminalize all nonterminal agents — the run is now terminal.
		if err := terminalizeAgentsInTx(tx, runID, "run_approved", evidenceRef, finalState); err != nil {
			return fmt.Errorf("terminalize agents: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return finalState, nil
}

// GetRun returns a run by ID.
func (s *Store) GetRun(ctx context.Context, runID string) (*domain.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanRun(s.db.QueryRowContext(ctx,
		`SELECT run_id, workspace_id, project_id, owner, base_commit, budget_seconds, state, result_state, started_at, ended_at, exit_code,
			epoch, lease_holder, lease_renew_deadline, last_event, checkpoint, evidence, next_action
		 FROM runs WHERE run_id = ?`, runID))
}

// ListRuns returns all runs ordered by started_at descending. If statusFilter
// is non-empty, only runs with that state are returned. The statusFilter
// accepts either a V2 state string (requested/planning/ready/running/
// verifying/completed/blocked/failed/canceled) or a legacy state string
// (pending/preparing/running/paused/resuming/stopping/stopped/failed/
// canceled); legacy strings are translated to V2 via LegacyRunStateMap.
func (s *Store) ListRuns(ctx context.Context, statusFilter string) ([]*domain.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	const runCols = `run_id, workspace_id, project_id, owner, base_commit, budget_seconds, state, result_state, started_at, ended_at, exit_code,
		epoch, lease_holder, lease_renew_deadline, last_event, checkpoint, evidence, next_action`
	var query string
	var args []any
	if statusFilter != "" {
		// Translate legacy state filter to V2 (the DB stores V2 states).
		v2Filter := statusFilter
		if v2, ok := domain.LegacyRunStateMap[domain.RunState(statusFilter)]; ok {
			v2Filter = string(v2)
		}
		query = `SELECT ` + runCols + `
			 FROM runs WHERE state = ? ORDER BY started_at DESC`
		args = []any{v2Filter}
	} else {
		query = `SELECT ` + runCols + `
			 FROM runs ORDER BY started_at DESC`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	var runs []*domain.Run
	for rows.Next() {
		r, err := scanRunRow(rows)
		if err != nil {
			return nil, fmt.Errorf("list runs scan: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// ListRunningRuns returns all runs in the 'running' state ordered by
// started_at ascending. Used by the agent liveness scanner to enforce run
// budgets — a run may be in the running state even if it has no running
// agents (e.g., the agent crashed but the run was not yet terminalized).
func (s *Store) ListRunningRuns(ctx context.Context) ([]*domain.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	const runCols = `run_id, workspace_id, project_id, owner, base_commit, budget_seconds, state, result_state, started_at, ended_at, exit_code,
		epoch, lease_holder, lease_renew_deadline, last_event, checkpoint, evidence, next_action`
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+runCols+`
		 FROM runs WHERE state = 'running' ORDER BY started_at`)
	if err != nil {
		return nil, fmt.Errorf("list running runs: %w", err)
	}
	defer rows.Close()
	var runs []*domain.Run
	for rows.Next() {
		r, err := scanRunRow(rows)
		if err != nil {
			return nil, fmt.Errorf("list running runs scan: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// FailRunBudgetExceeded transitions a run to the failed state with
// result_state='budget_exceeded' when the run's budget has been exceeded.
// In a single transaction it:
//   - validates the run is currently in a non-terminal state
//   - updates the run: state='failed', result_state='budget_exceeded',
//     ended_at=now
//   - appends a run.state_changed event with reason "budget_exceeded"
//   - terminalizes all non-terminal agents (registered/starting/running)
//     by setting state='lost' and exited_at=now
//   - appends an agent.state_changed event for each terminalized agent
//
// This is the budget-enforcement counterpart to VerifyRun; the run never
// reaches the verifying state because the budget is a hard limit, not a
// verdict.
func (s *Store) FailRunBudgetExceeded(ctx context.Context, runID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr string
		var budgetSec int64
		err := tx.QueryRow(`SELECT state, budget_seconds FROM runs WHERE run_id = ?`, runID).
			Scan(&fromStr, &budgetSec)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("run not found: " + runID)
		}
		if err != nil {
			return fmt.Errorf("select run for budget check: %w", err)
		}
		from := domain.RunStateV2(fromStr)
		if domain.IsTerminalRunState(from) {
			return domain.ErrConflict("run already terminal: " + runID)
		}
		if err := domain.CheckRunTransitionV2(from, domain.RunV2Failed); err != nil {
			return domain.ErrConflict(err.Error())
		}

		eventID, err := domain.NewID("evt_")
		if err != nil {
			return fmt.Errorf("new event id: %w", err)
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.Exec(
			`UPDATE runs SET state = ?, result_state = ?, ended_at = COALESCE(?, ended_at), last_event = ? WHERE run_id = ?`,
			string(domain.RunV2Failed), string(domain.ResultBudgetExceeded), now, eventID, runID,
		); err != nil {
			return fmt.Errorf("update run budget exceeded: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			EventType: "run.state_changed",
			Severity:  domain.SeverityWarn,
			Payload: mustMarshal(map[string]string{
				"from":   string(from),
				"to":     string(domain.RunV2Failed),
				"reason": "budget_exceeded",
				"budget": (time.Duration(budgetSec) * time.Second).String(),
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append budget_exceeded event: %w", err)
		}

		// Terminalize all non-terminal agents for this run. Budget
		// exhaustion is a forced closure — agents are set to 'lost'
		// (not 'exited') since the real exit code is unknown and the
		// process may still be running. The §6 agent state machine
		// allows running → lost; for registered/starting agents we set
		// 'lost' directly as a forced terminalization outside the
		// normal state machine (same precedent as terminalizeAgentsInTx
		// setting 'exited' for non-running agents).
		agentRows, err := tx.Query(
			`SELECT agent_id, state FROM agents WHERE run_id = ? AND state IN ('registered','starting','running')`,
			runID,
		)
		if err != nil {
			return fmt.Errorf("query nonterminal agents for budget: %w", err)
		}
		type agentInfo struct {
			id   string
			from string
		}
		var agents []agentInfo
		for agentRows.Next() {
			var ai agentInfo
			if err := agentRows.Scan(&ai.id, &ai.from); err != nil {
				agentRows.Close()
				return fmt.Errorf("scan agent for budget: %w", err)
			}
			agents = append(agents, ai)
		}
		agentRows.Close()

		for _, ai := range agents {
			if _, err := tx.Exec(
				`UPDATE agents SET state = 'lost', exited_at = ?, exit_code = NULL WHERE agent_id = ?`,
				now, ai.id,
			); err != nil {
				return fmt.Errorf("update agent %s lost: %w", ai.id, err)
			}
			aeID, err := domain.NewID("evt_")
			if err != nil {
				return fmt.Errorf("new agent event id: %w", err)
			}
			_, err = appendEvent(tx, &domain.Event{
				EventID:   aeID,
				RunID:     runID,
				AgentID:   ai.id,
				EventType: "agent.state_changed",
				Severity:  domain.SeverityWarn,
				Payload: mustMarshal(map[string]string{
					"agent_id": ai.id,
					"from":     ai.from,
					"to":       string(domain.AgentLost),
					"reason":   "budget_exceeded",
				}),
				Timestamp: time.Now().UTC(),
			})
			if err != nil {
				return fmt.Errorf("append agent.state_changed event: %w", err)
			}
		}
		return nil
	})
}

// runScanner is the common scan target for run rows. Both *sql.Row and
// *sql.Rows satisfy the scanner interface.
type runScanner interface {
	Scan(dest ...any) error
}

// scanRunColumns scans the 18 run columns (11 legacy + 6 §8.2 + next_action)
// into a domain.Run. Used by both scanRun (*sql.Row) and scanRunRow (*sql.Rows).
func scanRunColumns(sc runScanner) (*domain.Run, error) {
	var r domain.Run
	var budgetSec int64
	var startedAt, endedAt sql.NullString
	var exitCode sql.NullInt64
	var leaseHolder string
	var leaseRenewDeadline int64
	var evidenceJSON, nextAction string
	err := sc.Scan(
		&r.RunID, &r.WorkspaceID, &r.ProjectID, &r.Owner, &r.BaseCommit, &budgetSec, &r.State, &r.ResultState, &startedAt, &endedAt, &exitCode,
		&r.Epoch, &leaseHolder, &leaseRenewDeadline, &r.LastEvent, &r.Checkpoint, &evidenceJSON, &nextAction,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan run: %w", err)
	}
	r.Budget = time.Duration(budgetSec) * time.Second
	r.Lease = domain.RunLease{Holder: leaseHolder, RenewDeadline: leaseRenewDeadline}
	r.NextAction = domain.NextAction(nextAction)
	if evidenceJSON != "" {
		_ = json.Unmarshal([]byte(evidenceJSON), &r.Evidence)
	}
	if startedAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, startedAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse started_at: %w", err)
		}
		r.StartedAt = &t
	}
	if endedAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, endedAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse ended_at: %w", err)
		}
		r.EndedAt = &t
	}
	if exitCode.Valid {
		c := int(exitCode.Int64)
		r.ExitCode = &c
	}
	return &r, nil
}

func scanRun(row *sql.Row) (*domain.Run, error) {
	return scanRunColumns(row)
}

// scanRunRow scans a run from a *sql.Rows cursor.
func scanRunRow(rows *sql.Rows) (*domain.Run, error) {
	return scanRunColumns(rows)
}

// --- Tasks ---

// CreateTask inserts a task and appends a task.created event.
func (s *Store) CreateTask(ctx context.Context, t *domain.Task, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		inc, _ := json.Marshal(t.Scope.Include)
		exc, _ := json.Marshal(t.Scope.Exclude)
		ac := marshalStringSlice(t.AcceptanceCriteria)
		cons := marshalStringSlice(t.Constraints)
		del := marshalStringSlice(t.Deliverables)
		risk := string(t.RiskLevel)
		if risk == "" {
			risk = string(domain.RiskR2) // safe default
		}
		if _, err := tx.Exec(
			`INSERT INTO tasks(task_id, run_id, objective, scope_include, scope_exclude, worktree_path, state, created_at,
			   acceptance_criteria, constraints, deliverables, risk_level)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			t.TaskID, t.RunID, t.Objective, string(inc), string(exc), t.WorktreePath,
			string(t.State), t.CreatedAt.UTC().Format(time.RFC3339Nano),
			string(ac), string(cons), string(del), risk,
		); err != nil {
			return fmt.Errorf("insert task: %w", err)
		}
		_, err := appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     t.RunID,
			TaskID:    t.TaskID,
			EventType: "task.created",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"task_id": t.TaskID, "objective": t.Objective}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append event: %w", err)
		}
		// Update last_event on the run projection.
		if t.RunID != "" {
			if _, err := tx.Exec(`UPDATE runs SET last_event = ? WHERE run_id = ?`, eventID, t.RunID); err != nil {
				return fmt.Errorf("update run last_event: %w", err)
			}
		}
		return nil
	})
}

// marshalStringSlice marshals a string slice to JSON, returning "[]" for a
// nil slice (instead of "null") so the stored value always matches the
// column default and round-trips cleanly.
func marshalStringSlice(s []string) string {
	if s == nil {
		return "[]"
	}
	b, _ := json.Marshal(s)
	return string(b)
}

// scanTaskColumns scans the full task column set (including the v10
// TaskSpec/risk columns) from a row or rows scanner into a domain.Task.
// The JSON-array columns (scope_include/exclude, acceptance_criteria,
// constraints, deliverables) are parsed best-effort — a malformed value
// leaves the slice nil rather than failing the read.
func scanTaskColumns(scanner interface {
	Scan(dest ...any) error
}) (*domain.Task, error) {
	var t domain.Task
	var scopeInc, scopeExc, ac, cons, del, createdAt, risk string
	err := scanner.Scan(
		&t.TaskID, &t.RunID, &t.Objective, &scopeInc, &scopeExc, &t.WorktreePath, &t.State, &createdAt,
		&ac, &cons, &del, &risk,
	)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(scopeInc), &t.Scope.Include)
	_ = json.Unmarshal([]byte(scopeExc), &t.Scope.Exclude)
	_ = json.Unmarshal([]byte(ac), &t.AcceptanceCriteria)
	_ = json.Unmarshal([]byte(cons), &t.Constraints)
	_ = json.Unmarshal([]byte(del), &t.Deliverables)
	t.RiskLevel = domain.RiskLevel(risk)
	t.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	return &t, nil
}

// taskQueryCols is the canonical task column list (including v10 columns).
const taskQueryCols = `task_id, run_id, objective, scope_include, scope_exclude, worktree_path, state, created_at,
	acceptance_criteria, constraints, deliverables, risk_level`

// GetTask returns a task by ID.
func (s *Store) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := scanTaskColumns(s.db.QueryRowContext(ctx,
		`SELECT `+taskQueryCols+` FROM tasks WHERE task_id = ?`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}
	return t, nil
}

// GetTaskByRun returns the task associated with a run, or nil if not found.
func (s *Store) GetTaskByRun(ctx context.Context, runID string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := scanTaskColumns(s.db.QueryRowContext(ctx,
		`SELECT `+taskQueryCols+` FROM tasks WHERE run_id = ?`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get task by run: %w", err)
	}
	return t, nil
}

// --- Agents ---

// RegisterAgent inserts an agent and appends an agent.registered event.
func (s *Store) RegisterAgent(ctx context.Context, a *domain.Agent, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO agents(agent_id, run_id, task_id, role, runtime, pid, state, session_id, tmux_session, started_at, epoch)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.AgentID, a.RunID, a.TaskID, string(a.Role), a.Runtime, a.PID,
			string(a.State), a.SessionID, a.TmuxSession, a.StartedAt.UTC().Format(time.RFC3339Nano), a.Epoch,
		); err != nil {
			return fmt.Errorf("insert agent: %w", err)
		}
		_, err := appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     a.RunID,
			TaskID:    a.TaskID,
			AgentID:   a.AgentID,
			EventType: "agent.registered",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"agent_id": a.AgentID, "role": string(a.Role)}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append event: %w", err)
		}
		// Update last_event on the run projection.
		if a.RunID != "" {
			if _, err := tx.Exec(`UPDATE runs SET last_event = ? WHERE run_id = ?`, eventID, a.RunID); err != nil {
				return fmt.Errorf("update run last_event: %w", err)
			}
		}
		return nil
	})
}

// UpdateAgentState transitions an agent's state and appends an event.
func (s *Store) UpdateAgentState(ctx context.Context, agentID string, to domain.AgentState, exitCode *int, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var fromStr, runID string
		err := tx.QueryRow(`SELECT state, run_id FROM agents WHERE agent_id = ?`, agentID).Scan(&fromStr, &runID)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("agent not found: " + agentID)
		}
		if err != nil {
			return fmt.Errorf("select agent state: %w", err)
		}
		from := domain.AgentState(fromStr)
		if !domain.ValidAgentTransition(from, to) {
			return domain.ErrConflict(fmt.Sprintf("invalid agent transition %s -> %s", from, to))
		}
		var exitedAt *string
		var exitVal *int64
		if to == domain.AgentExited {
			now := time.Now().UTC().Format(time.RFC3339Nano)
			exitedAt = &now
			if exitCode != nil {
				v := int64(*exitCode)
				exitVal = &v
			}
		}
		if _, err := tx.Exec(
			`UPDATE agents SET state = ?, exited_at = COALESCE(?, exited_at), exit_code = COALESCE(?, exit_code) WHERE agent_id = ?`,
			string(to), exitedAt, exitVal, agentID,
		); err != nil {
			return fmt.Errorf("update agent state: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     runID,
			AgentID:   agentID,
			EventType: "agent.state_changed",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"from": string(from), "to": string(to)}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append event: %w", err)
		}
		// Update last_event on the run projection.
		if runID != "" {
			if _, err := tx.Exec(`UPDATE runs SET last_event = ? WHERE run_id = ?`, eventID, runID); err != nil {
				return fmt.Errorf("update run last_event: %w", err)
			}
		}
		return nil
	})
}

// --- Events ---

// EventsSince returns events with seq > cursor, up to limit, for a run.
// If runID is empty, returns events across all runs.
func (s *Store) EventsSince(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	var (
		query string
		args  []any
	)
	if runID == "" {
		query = `SELECT seq, event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp
			 FROM events WHERE seq > ? ORDER BY seq ASC LIMIT ?`
		args = []any{cursor, limit}
	} else {
		query = `SELECT seq, event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp
			 FROM events WHERE run_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`
		args = []any{runID, cursor, limit}
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var ts string
		var payload []byte
		var severity string
		var runID, taskID, agentID sql.NullString
		if err := rows.Scan(&e.Seq, &e.EventID, &runID, &taskID, &agentID, &e.EventType, &severity, &payload, &ts); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.RunID = runID.String
		e.TaskID = taskID.String
		e.AgentID = agentID.String
		e.Severity = domain.Severity(severity)
		e.Payload = payload
		e.Timestamp, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, fmt.Errorf("parse event timestamp: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return out, nil
}

// PublishMessageEnvelope writes a v1.1 typed message envelope to the event
// journal. It performs:
//  1. Idempotency dedup: if idempotency_key already exists, returns the
//     existing seq and message_seq without inserting a new row.
//  2. Per-Run message_seq assignment: queries the max message_seq for the
//     given run_id and assigns max+1 (or 1 if none).
//  3. Inline payload bound check: rejects payloads exceeding
//     domain.MaxInlinePayload.
//
// Returns (globalSeq, messageSeq, messageID, error).
func (s *Store) PublishMessageEnvelope(ctx context.Context, msg *domain.Message) (seq, messageSeq int64, messageID string, err error) {
	if err := msg.Validate(); err != nil {
		return 0, 0, "", domain.ErrInvalidInput("validate message: " + err.Error())
	}
	err = s.runInTx(ctx, func(tx *sql.Tx) error {
		// 1. Idempotency: check if idempotency_key already exists.
		var existingSeq, existingMsgSeq int64
		var existingMsgID sql.NullString
		row := tx.QueryRow(
			`SELECT seq, COALESCE(message_seq, 0), message_id FROM events WHERE idempotency_key = ? LIMIT 1`,
			msg.IdempotencyKey,
		)
		if qerr := row.Scan(&existingSeq, &existingMsgSeq, &existingMsgID); qerr == nil {
			// Dedup: return existing seqs and original message_id.
			seq = existingSeq
			messageSeq = existingMsgSeq
			messageID = existingMsgID.String
			return nil
		} else if !errors.Is(qerr, sql.ErrNoRows) {
			return fmt.Errorf("check idempotency_key: %w", qerr)
		}

		// 2. Assign per-Run message_seq.
		var maxMsgSeq sql.NullInt64
		if err := tx.QueryRow(
			`SELECT MAX(message_seq) FROM events WHERE run_id = ? AND message_seq IS NOT NULL`,
			msg.RunID,
		).Scan(&maxMsgSeq); err != nil {
			return fmt.Errorf("query max message_seq: %w", err)
		}
		messageSeq = 1
		if maxMsgSeq.Valid {
			messageSeq = maxMsgSeq.Int64 + 1
		}

		// 3. Marshal envelope as payload.
		payload := mustMarshal(msg)

		// 4. Generate event_id if not provided (use message_id as event_id
		//    base for traceability — but event_id must be unique, so prefix).
		eventID := "evt_" + msg.MessageID
		if strings.HasPrefix(msg.MessageID, "msg_") {
			eventID = "evt_" + msg.MessageID[4:]
		}

		// 5. Insert event with envelope columns. Set ack_state='pending'
		//    for C-002 delivery tracking.
		ts := msg.CreatedAt.UTC().Format(time.RFC3339Nano)
		res, err := tx.Exec(
			`INSERT INTO events(event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp,
			   message_id, idempotency_key, message_seq, ack_state, retry_count)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			eventID, msg.RunID, msg.TaskID, msg.Sender.AgentID, "message", string(domain.SeverityInfo),
			[]byte(payload), ts,
			msg.MessageID, msg.IdempotencyKey, messageSeq,
			string(domain.AckStatePending), 0,
		)
		if err != nil {
			return fmt.Errorf("insert message event: %w", err)
		}
		seq, err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("last insert id: %w", err)
		}
		messageID = msg.MessageID
		return nil
	})
	return seq, messageSeq, messageID, err
}

// MessagesByRun returns message events for the given run_id with message_seq
// > cursor, ordered by message_seq ascending, up to limit. Only events with
// a non-null message_seq are returned (i.e. v1.1 envelope messages).
func (s *Store) MessagesByRun(ctx context.Context, runID string, cursor int64, limit int) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT seq, event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp,
			   message_id, idempotency_key, message_seq, ack_state, retry_count
			  FROM events WHERE run_id = ? AND message_seq > ? AND message_seq IS NOT NULL
			  ORDER BY message_seq ASC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, runID, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages by run: %w", err)
	}
	defer rows.Close()
	return scanMessageEvents(rows)
}

// scanMessageEvents scans rows into domain.Event, including the v1.1
// message envelope columns.
func scanMessageEvents(rows *sql.Rows) ([]domain.Event, error) {
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var ts string
		var payload []byte
		var severity string
		var runID, taskID, agentID, msgID, idempKey, ackState sql.NullString
		var msgSeq sql.NullInt64
		var retryCount sql.NullInt64
		if err := rows.Scan(
			&e.Seq, &e.EventID, &runID, &taskID, &agentID, &e.EventType, &severity, &payload, &ts,
			&msgID, &idempKey, &msgSeq, &ackState, &retryCount,
		); err != nil {
			return nil, fmt.Errorf("scan message event: %w", err)
		}
		e.RunID = runID.String
		e.TaskID = taskID.String
		e.AgentID = agentID.String
		e.Severity = domain.Severity(severity)
		e.Payload = payload
		e.MessageID = msgID.String
		e.IdempotencyKey = idempKey.String
		e.MessageSeq = msgSeq.Int64
		e.AckState = ackState.String
		e.RetryCount = int(retryCount.Int64)
		var err error
		e.Timestamp, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, fmt.Errorf("parse event timestamp: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return out, nil
}

// EventsByTopic returns message events whose payload topic matches the
// given topic prefix, with seq > cursor, up to limit.
// Topic matching is prefix-based: "directive" matches "directive.hydra",
// "directive.mnemos", etc.
func (s *Store) EventsByTopic(ctx context.Context, topicPrefix string, cursor int64, limit int) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	// Filter by event_type='message' and payload containing the topic prefix.
	// SQLite json_extract is available in modernc.org/sqlite.
	query := `SELECT seq, event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp
		  FROM events WHERE event_type = 'message' AND seq > ?
		  AND json_extract(payload, '$.topic') LIKE ? ESCAPE '\'
		  ORDER BY seq ASC LIMIT ?`
	escapedPrefix := strings.ReplaceAll(topicPrefix, `\`, `\\`)
	escapedPrefix = strings.ReplaceAll(escapedPrefix, `%`, `\%`)
	escapedPrefix = strings.ReplaceAll(escapedPrefix, `_`, `\_`)
	pattern := escapedPrefix + "%"
	rows, err := s.db.QueryContext(ctx, query, cursor, pattern, limit)
	if err != nil {
		return nil, fmt.Errorf("query events by topic: %w", err)
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var ts string
		var payload []byte
		var severity string
		var runID, taskID, agentID sql.NullString
		if err := rows.Scan(&e.Seq, &e.EventID, &runID, &taskID, &agentID, &e.EventType, &severity, &payload, &ts); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.RunID = runID.String
		e.TaskID = taskID.String
		e.AgentID = agentID.String
		e.Severity = domain.Severity(severity)
		e.Payload = payload
		e.Timestamp, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, fmt.Errorf("parse event timestamp: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- Helpers ---

// EventExists reports whether an event with the given event_id exists in
// the journal for the given run. Read-only check, no transaction needed.
// Used by run.verify to validate that evidence_ref resolves to a real
// event (control-plane §3.3, acceptance-contract G3-VERIFY.1).
func (s *Store) EventExists(ctx context.Context, runID, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM events WHERE event_id = ? AND run_id = ?`,
		eventID, runID,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("event exists: %w", err)
	}
	return true, nil
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}

// ReconcileAfterCrash marks runs/agents that were left in transient states.
// Called once at daemon start.
func (s *Store) ReconcileAfterCrash(ctx context.Context) ([]string, error) {
	var reconciled []string
	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		// Mark running agents with no live PID as lost (caller checks PID;
		// here we just mark any still-running/preparing as failed since the
		// daemon restarted).
		rows, err := tx.Query(`SELECT agent_id, state FROM agents WHERE state IN ('registered','starting','running')`)
		if err != nil {
			return fmt.Errorf("query live agents: %w", err)
		}
		type pair struct{ id, state string }
		var live []pair
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.id, &p.state); err != nil {
				rows.Close()
				return fmt.Errorf("scan agent: %w", err)
			}
			live = append(live, p)
		}
		rows.Close()
		for _, p := range live {
			if _, err := tx.Exec(`UPDATE agents SET state = 'lost' WHERE agent_id = ?`, p.id); err != nil {
				return fmt.Errorf("mark agent lost: %w", err)
			}
			reconciled = append(reconciled, "agent:"+p.id+" -> lost")
		}
		// Mark planning/ready/running/verifying runs as failed with result_unknown.
		rows2, err := tx.Query(`SELECT run_id FROM runs WHERE state IN ('planning','ready','running','verifying')`)
		if err != nil {
			return fmt.Errorf("query transient runs: %w", err)
		}
		var runs []string
		for rows2.Next() {
			var id string
			if err := rows2.Scan(&id); err != nil {
				rows2.Close()
				return fmt.Errorf("scan run: %w", err)
			}
			runs = append(runs, id)
		}
		rows2.Close()
		for _, id := range runs {
			// Append a run.state_changed event and set last_event.
			seID, err := domain.NewID("evt_")
			if err != nil {
				return fmt.Errorf("new event id: %w", err)
			}
			if _, err := tx.Exec(
				`UPDATE runs SET state = 'failed', result_state = 'result_unknown', ended_at = ?, last_event = ? WHERE run_id = ?`,
				time.Now().UTC().Format(time.RFC3339Nano), seID, id,
			); err != nil {
				return fmt.Errorf("mark run failed: %w", err)
			}
			_, err = appendEvent(tx, &domain.Event{
				EventID:   seID,
				RunID:     id,
				EventType: "run.state_changed",
				Severity:  domain.SeverityWarn,
				Payload:   mustMarshal(map[string]string{"from": "running", "to": "failed", "reason": "crash reconciliation"}),
				Timestamp: time.Now().UTC(),
			})
			if err != nil {
				return fmt.Errorf("append reconcile event: %w", err)
			}
			reconciled = append(reconciled, "run:"+id+" -> failed (result_unknown)")
		}
		return nil
	})
	return reconciled, err
}

// --- Candidates ---

// SaveCandidate persists a candidate record to the candidates table.
func (s *Store) SaveCandidate(ctx context.Context, c *domain.Candidate, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO candidates (candidate_id, task_id, run_id, ref_name, commit_sha, summary, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			c.CandidateID, c.TaskID, c.RunID, c.RefName, c.CommitSHA, c.Summary,
			c.CreatedAt.Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert candidate: %w", err)
		}
		if _, err := tx.Exec(
			`INSERT INTO events (event_id, run_id, task_id, event_type, severity, payload, timestamp)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			eventID, c.RunID, c.TaskID, "candidate_created", "info",
			mustMarshal(map[string]string{
				"candidate_id": c.CandidateID,
				"ref_name":     c.RefName,
				"commit_sha":   c.CommitSHA,
				"summary":      c.Summary,
			}),
			c.CreatedAt.Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert candidate event: %w", err)
		}
		// Update last_event on the run projection.
		if c.RunID != "" {
			if _, err := tx.Exec(`UPDATE runs SET last_event = ? WHERE run_id = ?`, eventID, c.RunID); err != nil {
				return fmt.Errorf("update run last_event: %w", err)
			}
		}
		return nil
	})
}

// GetAgentByRun returns the most recently registered agent for a run.
// In Phase 1 (single worker), there is at most one agent per run.
func (s *Store) GetAgentByRun(ctx context.Context, runID string) (*domain.Agent, error) {
	var a domain.Agent
	var startedAt string
	var exitedAt sql.NullString
	var exitCode sql.NullInt64
	var sessionID, tmuxSession sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT agent_id, run_id, task_id, role, runtime, pid, state, session_id, tmux_session, started_at, exited_at, exit_code, epoch
		 FROM agents WHERE run_id = ? ORDER BY started_at DESC LIMIT 1`, runID,
	).Scan(&a.AgentID, &a.RunID, &a.TaskID, &a.Role, &a.Runtime, &a.PID, &a.State,
		&sessionID, &tmuxSession, &startedAt, &exitedAt, &exitCode, &a.Epoch)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get agent by run: %w", err)
	}
	a.SessionID = sessionID.String
	a.TmuxSession = tmuxSession.String
	a.StartedAt, err = time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return nil, fmt.Errorf("parse started_at: %w", err)
	}
	if exitedAt.Valid && exitedAt.String != "" {
		t, err := time.Parse(time.RFC3339Nano, exitedAt.String)
		if err == nil {
			a.ExitedAt = &t
		}
	}
	if exitCode.Valid {
		code := int(exitCode.Int64)
		a.ExitCode = &code
	}
	return &a, nil
}

// GetAgent returns an agent by ID, or nil if not found.
func (s *Store) GetAgent(ctx context.Context, agentID string) (*domain.Agent, error) {
	var a domain.Agent
	var startedAt string
	var exitedAt sql.NullString
	var exitCode sql.NullInt64
	var sessionID, tmuxSession sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT agent_id, run_id, task_id, role, runtime, pid, state, session_id, tmux_session, started_at, exited_at, exit_code, epoch
		 FROM agents WHERE agent_id = ?`, agentID,
	).Scan(&a.AgentID, &a.RunID, &a.TaskID, &a.Role, &a.Runtime, &a.PID, &a.State,
		&sessionID, &tmuxSession, &startedAt, &exitedAt, &exitCode, &a.Epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get agent: %w", err)
	}
	a.SessionID = sessionID.String
	a.TmuxSession = tmuxSession.String
	a.StartedAt, err = time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return nil, fmt.Errorf("parse started_at: %w", err)
	}
	if exitedAt.Valid && exitedAt.String != "" {
		t, err := time.Parse(time.RFC3339Nano, exitedAt.String)
		if err == nil {
			a.ExitedAt = &t
		}
	}
	if exitCode.Valid {
		code := int(exitCode.Int64)
		a.ExitCode = &code
	}
	return &a, nil
}

// ListRunningAgents returns all agents in the 'running' state.
// Used by the agent liveness scanner to detect exited processes.
func (s *Store) ListRunningAgents(ctx context.Context) ([]domain.Agent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT agent_id, run_id, task_id, role, runtime, pid, state, session_id, tmux_session, started_at, exited_at, exit_code, epoch
		 FROM agents WHERE state = 'running' ORDER BY started_at`)
	if err != nil {
		return nil, fmt.Errorf("list running agents: %w", err)
	}
	defer rows.Close()

	var agents []domain.Agent
	for rows.Next() {
		var a domain.Agent
		var startedAt string
		var exitedAt sql.NullString
		var exitCode sql.NullInt64
		var sessionID, tmuxSession sql.NullString
		if err := rows.Scan(&a.AgentID, &a.RunID, &a.TaskID, &a.Role, &a.Runtime, &a.PID, &a.State,
			&sessionID, &tmuxSession, &startedAt, &exitedAt, &exitCode, &a.Epoch); err != nil {
			return nil, fmt.Errorf("scan agent: %w", err)
		}
		a.SessionID = sessionID.String
		a.TmuxSession = tmuxSession.String
		a.StartedAt, err = time.Parse(time.RFC3339Nano, startedAt)
		if err != nil {
			return nil, fmt.Errorf("parse started_at: %w", err)
		}
		if exitedAt.Valid && exitedAt.String != "" {
			t, err := time.Parse(time.RFC3339Nano, exitedAt.String)
			if err == nil {
				a.ExitedAt = &t
			}
		}
		if exitCode.Valid {
			code := int(exitCode.Int64)
			a.ExitCode = &code
		}
		agents = append(agents, a)
	}
	return agents, nil
}

// GetCandidate retrieves a candidate by ID from the candidates table.
func (s *Store) GetCandidate(ctx context.Context, candidateID string) (*domain.Candidate, error) {
	var c domain.Candidate
	var createdAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT candidate_id, task_id, run_id, ref_name, commit_sha, summary, created_at
		 FROM candidates WHERE candidate_id = ?`, candidateID,
	).Scan(&c.CandidateID, &c.TaskID, &c.RunID, &c.RefName, &c.CommitSHA, &c.Summary, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get candidate: %w", err)
	}
	c.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	return &c, nil
}

// AckMessage marks a message as acked (C-002). It is idempotent: if the
// message is already acked, returns the existing state without writing a
// new event. If the message is nacked/expired/dead, returns an error.
// Writes an event_type="message.ack" event to the journal.
func (s *Store) AckMessage(ctx context.Context, messageID, agentID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		// 1. Check current ack_state.
		var ackState sql.NullString
		err := tx.QueryRow(
			`SELECT ack_state FROM events WHERE message_id = ? AND message_seq IS NOT NULL LIMIT 1`,
			messageID,
		).Scan(&ackState)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrNotFound("ack: message not found: " + messageID)
			}
			return fmt.Errorf("query ack_state: %w", err)
		}

		state := domain.AckState(ackState.String)
		if state == domain.AckStateAcked {
			return nil // idempotent: already acked
		}
		if state == domain.AckStateNacked || state == domain.AckStateExpired || state == domain.AckStateDead {
			return domain.ErrInvalidInput("ack: message is in terminal state " + string(state))
		}

		// 2. Update ack_state to 'acked'.
		if _, err := tx.Exec(
			`UPDATE events SET ack_state = ? WHERE message_id = ? AND message_seq IS NOT NULL`,
			string(domain.AckStateAcked), messageID,
		); err != nil {
			return fmt.Errorf("update ack_state: %w", err)
		}

		// 3. Write ack event.
		ackEventID, err := domain.NewID("evt_")
		if err != nil {
			return domain.ErrInternal("ack event id: " + err.Error())
		}
		payload := mustMarshal(map[string]string{
			"message_id": messageID,
			"acked_by":   agentID,
		})
		_, err = tx.Exec(
			`INSERT INTO events(event_id, event_type, severity, payload, timestamp, message_id, ack_state)
			 VALUES(?, ?, ?, ?, ?, ?, ?)`,
			ackEventID, "message.ack", string(domain.SeverityInfo),
			[]byte(payload), time.Now().UTC().Format(time.RFC3339Nano),
			messageID, string(domain.AckStateAcked),
		)
		if err != nil {
			return fmt.Errorf("insert ack event: %w", err)
		}
		return nil
	})
}

// NackMessage marks a message as nacked (C-002). It is idempotent: if the
// message is already in a terminal state (acked/expired/dead), returns an
// error. Increments retry_count. If retry_count >= MaxRetries, marks the
// message as 'dead' instead of 'nacked'. Writes an event_type="message.nack"
// event to the journal.
func (s *Store) NackMessage(ctx context.Context, messageID, agentID, reason string) (newRetryCount int, finalState domain.AckState, err error) {
	err = s.runInTx(ctx, func(tx *sql.Tx) error {
		// 1. Check current state and retry_count.
		var ackState sql.NullString
		var retryCount sql.NullInt64
		err := tx.QueryRow(
			`SELECT ack_state, retry_count FROM events WHERE message_id = ? AND message_seq IS NOT NULL LIMIT 1`,
			messageID,
		).Scan(&ackState, &retryCount)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrNotFound("nack: message not found: " + messageID)
			}
			return fmt.Errorf("query ack_state: %w", err)
		}

		state := domain.AckState(ackState.String)
		if state == domain.AckStateAcked {
			return domain.ErrInvalidInput("nack: message already acked")
		}
		if state == domain.AckStateExpired || state == domain.AckStateDead {
			return domain.ErrInvalidInput("nack: message is in terminal state " + string(state))
		}

		newRetryCount = int(retryCount.Int64) + 1
		finalState = domain.AckStateNacked
		if newRetryCount >= domain.MaxRetries {
			finalState = domain.AckStateDead
		}

		// 2. Update ack_state and retry_count.
		if _, err := tx.Exec(
			`UPDATE events SET ack_state = ?, retry_count = ? WHERE message_id = ? AND message_seq IS NOT NULL`,
			string(finalState), newRetryCount, messageID,
		); err != nil {
			return fmt.Errorf("update ack_state: %w", err)
		}

		// 3. Write nack event.
		nackEventID, err := domain.NewID("evt_")
		if err != nil {
			return domain.ErrInternal("nack event id: " + err.Error())
		}
		payload := mustMarshal(map[string]any{
			"message_id":  messageID,
			"nacked_by":   agentID,
			"reason":      reason,
			"retry_count": newRetryCount,
			"final_state": string(finalState),
		})
		_, err = tx.Exec(
			`INSERT INTO events(event_id, event_type, severity, payload, timestamp, message_id, ack_state, retry_count)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			nackEventID, "message.nack", string(domain.SeverityWarn),
			[]byte(payload), time.Now().UTC().Format(time.RFC3339Nano),
			messageID, string(finalState), newRetryCount,
		)
		if err != nil {
			return fmt.Errorf("insert nack event: %w", err)
		}
		return nil
	})
	return newRetryCount, finalState, err
}

// ExpireMessages finds messages with ack_state='pending' (or NULL) whose
// TTL has expired and marks them as 'expired' (C-002). Returns the list of
// expired message_ids. Writes an event_type="message.expired" event for each.
// This is the deadline check — should be called periodically by the daemon
// (C-003 will make this event-driven).
func (s *Store) ExpireMessages(ctx context.Context, now time.Time) ([]string, error) {
	var expired []string
	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		// Find pending messages with TTL > 0 that have expired.
		// We need to parse the TTL from the JSON payload, so we query
		// all pending messages and check in Go.
		rows, err := tx.Query(
			`SELECT seq, message_id, payload, timestamp FROM events
			 WHERE ack_state = ? AND message_seq IS NOT NULL AND message_id IS NOT NULL`,
			string(domain.AckStatePending),
		)
		if err != nil {
			return fmt.Errorf("query pending messages: %w", err)
		}
		defer rows.Close()

		type pendingMsg struct {
			seq       int64
			messageID string
			payload   []byte
			ts        string
		}
		var pending []pendingMsg
		for rows.Next() {
			var pm pendingMsg
			if err := rows.Scan(&pm.seq, &pm.messageID, &pm.payload, &pm.ts); err != nil {
				return fmt.Errorf("scan pending: %w", err)
			}
			pending = append(pending, pm)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows err: %w", err)
		}

		for _, pm := range pending {
			var msg domain.Message
			if err := json.Unmarshal(pm.payload, &msg); err != nil {
				continue // skip unparseable
			}
			if msg.TTL <= 0 {
				continue // no expiry
			}
			createdAt, err := time.Parse(time.RFC3339Nano, pm.ts)
			if err != nil {
				continue
			}
			if now.Sub(createdAt) <= time.Duration(msg.TTL)*time.Second {
				continue // not expired yet
			}

			// Mark as expired.
			if _, err := tx.Exec(
				`UPDATE events SET ack_state = ? WHERE seq = ?`,
				string(domain.AckStateExpired), pm.seq,
			); err != nil {
				return fmt.Errorf("update expired: %w", err)
			}

			// Write expired event.
			expEventID, err := domain.NewID("evt_")
			if err != nil {
				return domain.ErrInternal("expire event id: " + err.Error())
			}
			payload := mustMarshal(map[string]string{
				"message_id": pm.messageID,
				"expired_at": now.UTC().Format(time.RFC3339Nano),
			})
			_, err = tx.Exec(
				`INSERT INTO events(event_id, event_type, severity, payload, timestamp, message_id, ack_state)
				 VALUES(?, ?, ?, ?, ?, ?, ?)`,
				expEventID, "message.expired", string(domain.SeverityWarn),
				[]byte(payload), now.UTC().Format(time.RFC3339Nano),
				pm.messageID, string(domain.AckStateExpired),
			)
			if err != nil {
				return fmt.Errorf("insert expired event: %w", err)
			}
			expired = append(expired, pm.messageID)
		}
		return nil
	})
	return expired, err
}

// MessageStatus returns the delivery state of a message (C-002).
type MessageStatus struct {
	MessageID  string
	AckState   domain.AckState
	RetryCount int
	IsDead     bool
	IsExpired  bool
}

// GetMessageStatus returns the current delivery state of a message.
func (s *Store) GetMessageStatus(ctx context.Context, messageID string) (*MessageStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ackState sql.NullString
	var retryCount sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT ack_state, retry_count FROM events WHERE message_id = ? AND message_seq IS NOT NULL LIMIT 1`,
		messageID,
	).Scan(&ackState, &retryCount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound("status: message not found: " + messageID)
		}
		return nil, fmt.Errorf("query status: %w", err)
	}

	st := &MessageStatus{
		MessageID:  messageID,
		AckState:   domain.AckState(ackState.String),
		RetryCount: int(retryCount.Int64),
		IsDead:     domain.AckState(ackState.String) == domain.AckStateDead,
		IsExpired:  domain.AckState(ackState.String) == domain.AckStateExpired,
	}
	return st, nil
}

// SaveWakeCursor persists the last-processed event seq for the wake loop
// (C-003). This enables crash recovery: after a restart, the wake loop
// reads this cursor and resumes from where it left off.
func (s *Store) SaveWakeCursor(ctx context.Context, cursor int64) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT OR REPLACE INTO meta(key, value) VALUES ('wake_cursor', ?)`,
			strconv.FormatInt(cursor, 10),
		)
		if err != nil {
			return fmt.Errorf("save wake cursor: %w", err)
		}
		return nil
	})
}

// LoadWakeCursor returns the last-processed event seq for the wake loop
// (C-003). Returns 0 if no cursor has been saved yet (first run).
func (s *Store) LoadWakeCursor(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var v string
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM meta WHERE key = 'wake_cursor'`,
	).Scan(&v)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil // first run, no cursor
		}
		return 0, fmt.Errorf("load wake cursor: %w", err)
	}
	cursor, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse wake cursor %q: %w", v, err)
	}
	return cursor, nil
}

// LastEventSeq returns the highest seq in the events table (C-003).
// Used by the wake loop to detect new events without polling the full table.
func (s *Store) LastEventSeq(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxSeq sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT MAX(seq) FROM events`,
	).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("query last event seq: %w", err)
	}
	if !maxSeq.Valid {
		return 0, nil // empty table
	}
	return maxSeq.Int64, nil
}

// RecordMetric writes a metric event to the event journal (C-006).
// The metric is stored as an event with event_type="metric" and a JSON
// payload containing the metric name, value, and optional tags.
func (s *Store) RecordMetric(ctx context.Context, name string, value float64, tags map[string]string) error {
	eventID, err := domain.NewID("metric_")
	if err != nil {
		return fmt.Errorf("generate metric event_id: %w", err)
	}
	payload := map[string]any{
		"metric": name,
		"value":  value,
	}
	for k, v := range tags {
		payload[k] = v
	}
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO events (event_id, event_type, severity, payload, timestamp)
			 VALUES (?, 'metric', 'info', ?, ?)`,
			eventID,
			mustMarshal(payload),
			time.Now().UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			return fmt.Errorf("insert metric event: %w", err)
		}
		return nil
	})
}
