package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// migrateV8 adds the continuations table for ADR-0017 (durable wake/continuation).
func migrateV8(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS continuations (
		continuation_id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		project_id TEXT NOT NULL,
		owner TEXT NOT NULL DEFAULT '',
		successor_objective TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'pending',
		wake_sent_at TEXT,
		wake_count INTEGER NOT NULL DEFAULT 0,
		successor_run_id TEXT,
		created_at TEXT NOT NULL,
		fulfilled_at TEXT
	)`)
	if err != nil {
		return fmt.Errorf("create continuations: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_continuations_state ON continuations(state)`)
	if err != nil {
		return fmt.Errorf("index continuations state: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_continuations_run ON continuations(run_id)`)
	if err != nil {
		return fmt.Errorf("index continuations run: %w", err)
	}
	return nil
}

// migrateContinuationRootCause adds the root_cause column to the
// continuations table for the same-root-cause circuit breaker (ROADMAP
// "同根因 3 次熔断"). The column records the derived reason a continuation
// was needed so the breaker can count repeated occurrences across a run
// chain.
//
// Additive (ALTER TABLE ADD COLUMN) and idempotent (tolerates "duplicate
// column name"). Existing continuations get the default empty string.
func migrateContinuationRootCause(tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE continuations ADD COLUMN root_cause TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("alter continuations root_cause: %w", err)
		}
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_continuations_successor ON continuations(successor_run_id) WHERE successor_run_id IS NOT NULL`); err != nil {
		return fmt.Errorf("index continuations successor: %w", err)
	}
	return nil
}

// RegisterContinuation creates a continuation record for a completed/blocked
// run that requires an explicit successor. This is an explicit PM/operator
// action — the system never auto-creates continuations.
func (s *Store) RegisterContinuation(ctx context.Context, c *domain.Continuation, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		// Check for duplicate (idempotent: same run_id + successor_objective).
		var existingID string
		err := tx.QueryRow(
			`SELECT continuation_id FROM continuations WHERE run_id = ? AND successor_objective = ? AND state IN ('pending','notified')`,
			c.RunID, c.SuccessorObjective,
		).Scan(&existingID)
		if err == nil {
			// Already exists and is active — return the existing one (idempotent).
			c.ContinuationID = existingID
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check duplicate continuation: %w", err)
		}

		c.CreatedAt = time.Now().UTC()
		c.State = domain.ContinuationPending

		_, err = tx.Exec(
			`INSERT INTO continuations (continuation_id, run_id, project_id, owner, successor_objective, state, wake_count, created_at, root_cause)
			 VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
			c.ContinuationID, c.RunID, c.ProjectID, c.Owner, c.SuccessorObjective,
			string(c.State), c.CreatedAt.Format(time.RFC3339Nano), c.RootCause,
		)
		if err != nil {
			return fmt.Errorf("insert continuation: %w", err)
		}

		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     c.RunID,
			EventType: "continuation.registered",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"continuation_id": c.ContinuationID, "successor_objective": c.SuccessorObjective}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append continuation.registered event: %w", err)
		}
		return nil
	})
}

// ListPendingContinuations returns all continuations in pending or notified
// state. Used by the reconcile tick to detect runs needing PM attention.
func (s *Store) ListPendingContinuations(ctx context.Context) ([]*domain.Continuation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.QueryContext(ctx,
		`SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
		 FROM continuations WHERE state IN ('pending','notified') ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("query pending continuations: %w", err)
	}
	defer rows.Close()

	var list []*domain.Continuation
	for rows.Next() {
		c, err := scanContinuation(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, c)
	}
	return list, rows.Err()
}

// MarkContinuationNotified sets wake_sent_at to now and increments wake_count.
// This is the dedup marker — the reconcile tick checks wake_sent_at before
// sending a notification.
func (s *Store) MarkContinuationNotified(ctx context.Context, continuationID, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var stateStr string
		err := tx.QueryRow(`SELECT state FROM continuations WHERE continuation_id = ?`, continuationID).Scan(&stateStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("continuation not found: " + continuationID)
		}
		if err != nil {
			return fmt.Errorf("select continuation state: %w", err)
		}
		from := domain.ContinuationState(stateStr)
		if err := domain.CheckContinuationTransition(from, domain.ContinuationNotified); err != nil {
			return domain.ErrConflict(err.Error())
		}

		now := time.Now().UTC()
		if _, err := tx.Exec(
			`UPDATE continuations SET state = 'notified', wake_sent_at = ?, wake_count = wake_count + 1 WHERE continuation_id = ?`,
			now.Format(time.RFC3339Nano), continuationID,
		); err != nil {
			return fmt.Errorf("update continuation notified: %w", err)
		}

		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			EventType: "continuation.notified",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"continuation_id": continuationID}),
			Timestamp: now,
		})
		if err != nil {
			return fmt.Errorf("append continuation.notified event: %w", err)
		}
		return nil
	})
}

// FulfillContinuation marks a continuation as fulfilled and links the successor
// run. This is an explicit PM/operator action — the system never auto-fulfills.
func (s *Store) FulfillContinuation(ctx context.Context, continuationID, successorRunID, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var stateStr string
		err := tx.QueryRow(`SELECT state FROM continuations WHERE continuation_id = ?`, continuationID).Scan(&stateStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("continuation not found: " + continuationID)
		}
		if err != nil {
			return fmt.Errorf("select continuation state: %w", err)
		}
		from := domain.ContinuationState(stateStr)
		if err := domain.CheckContinuationTransition(from, domain.ContinuationFulfilled); err != nil {
			return domain.ErrConflict(err.Error())
		}

		now := time.Now().UTC()
		if _, err := tx.Exec(
			`UPDATE continuations SET state = 'fulfilled', successor_run_id = ?, fulfilled_at = ? WHERE continuation_id = ?`,
			successorRunID, now.Format(time.RFC3339Nano), continuationID,
		); err != nil {
			return fmt.Errorf("update continuation fulfilled: %w", err)
		}

		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			EventType: "continuation.fulfilled",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"continuation_id": continuationID, "successor_run_id": successorRunID}),
			Timestamp: now,
		})
		if err != nil {
			return fmt.Errorf("append continuation.fulfilled event: %w", err)
		}
		return nil
	})
}

// CancelContinuation marks a continuation as cancelled (PM decided no successor needed).
func (s *Store) CancelContinuation(ctx context.Context, continuationID, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		var stateStr string
		err := tx.QueryRow(`SELECT state FROM continuations WHERE continuation_id = ?`, continuationID).Scan(&stateStr)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound("continuation not found: " + continuationID)
		}
		if err != nil {
			return fmt.Errorf("select continuation state: %w", err)
		}
		from := domain.ContinuationState(stateStr)
		if err := domain.CheckContinuationTransition(from, domain.ContinuationCancelled); err != nil {
			return domain.ErrConflict(err.Error())
		}

		if _, err := tx.Exec(
			`UPDATE continuations SET state = 'cancelled' WHERE continuation_id = ?`,
			continuationID,
		); err != nil {
			return fmt.Errorf("update continuation cancelled: %w", err)
		}

		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			EventType: "continuation.cancelled",
			Severity:  domain.SeverityInfo,
			Payload:   mustMarshal(map[string]string{"continuation_id": continuationID}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append continuation.cancelled event: %w", err)
		}
		return nil
	})
}

// GetContinuation returns a continuation by ID.
func (s *Store) GetContinuation(ctx context.Context, continuationID string) (*domain.Continuation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanContinuationRow(s.db.QueryRowContext(ctx,
		`SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
		 FROM continuations WHERE continuation_id = ?`, continuationID))
}

// ListContinuations returns continuations filtered by state. An empty
// stateFilter returns all continuations. The special filter "pending" returns
// only pending+notified (the active set the reconcile tick examines).
func (s *Store) ListContinuations(ctx context.Context, stateFilter string) ([]*domain.Continuation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var query string
	var args []any
	switch stateFilter {
	case "", "all":
		query = `SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
			 FROM continuations ORDER BY created_at ASC`
	case "pending":
		query = `SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
			 FROM continuations WHERE state IN ('pending','notified') ORDER BY created_at ASC`
	default:
		query = `SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
			 FROM continuations WHERE state = ? ORDER BY created_at ASC`
		args = []any{stateFilter}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query continuations: %w", err)
	}
	defer rows.Close()

	var list []*domain.Continuation
	for rows.Next() {
		c, err := scanContinuation(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, c)
	}
	return list, rows.Err()
}

// GetContinuationsByRun returns all continuations for a run.
func (s *Store) GetContinuationsByRun(ctx context.Context, runID string) ([]*domain.Continuation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.QueryContext(ctx,
		`SELECT continuation_id, run_id, project_id, owner, successor_objective, state, wake_sent_at, wake_count, successor_run_id, created_at, fulfilled_at, root_cause
		 FROM continuations WHERE run_id = ? ORDER BY created_at ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("query continuations by run: %w", err)
	}
	defer rows.Close()

	var list []*domain.Continuation
	for rows.Next() {
		c, err := scanContinuation(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, c)
	}
	return list, rows.Err()
}

// continuationScanner is the common scan interface for *sql.Rows and *sql.Row.
type continuationScanner interface {
	Scan(dest ...any) error
}

func scanContinuationRow(row *sql.Row) (*domain.Continuation, error) {
	return scanContinuation(row)
}

func scanContinuation(sc continuationScanner) (*domain.Continuation, error) {
	var c domain.Continuation
	var stateStr string
	var wakeSentAt sql.NullString
	var successorRunID sql.NullString
	var fulfilledAt sql.NullString
	var createdAtStr string

	err := sc.Scan(
		&c.ContinuationID, &c.RunID, &c.ProjectID, &c.Owner, &c.SuccessorObjective,
		&stateStr, &wakeSentAt, &c.WakeCount, &successorRunID, &createdAtStr, &fulfilledAt,
		&c.RootCause,
	)
	if err != nil {
		return nil, err
	}
	c.State = domain.ContinuationState(stateStr)
	if wakeSentAt.Valid && wakeSentAt.String != "" {
		t, err := time.Parse(time.RFC3339Nano, wakeSentAt.String)
		if err == nil {
			c.WakeSentAt = &t
		}
	}
	if successorRunID.Valid {
		c.SuccessorRunID = successorRunID.String
	}
	if fulfilledAt.Valid && fulfilledAt.String != "" {
		t, err := time.Parse(time.RFC3339Nano, fulfilledAt.String)
		if err == nil {
			c.FulfilledAt = &t
		}
	}
	if createdAtStr != "" {
		t, err := time.Parse(time.RFC3339Nano, createdAtStr)
		if err == nil {
			c.CreatedAt = t
		}
	}
	return &c, nil
}

// UpdateWakeSentAt is a low-level method used by the reconcile tick to update
// the wake_sent_at timestamp without going through the full state transition
// (for re-notifying already-notified continuations). It does NOT increment
// wake_count — that is only done on the first pending→notified transition.
func (s *Store) UpdateWakeSentAt(ctx context.Context, continuationID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx,
		`UPDATE continuations SET wake_sent_at = ?, wake_count = wake_count + 1 WHERE continuation_id = ? AND state IN ('pending','notified')`,
		now.Format(time.RFC3339Nano), continuationID,
	)
	if err != nil {
		return fmt.Errorf("update wake_sent_at: %w", err)
	}
	return nil
}

// CountRootCauseInChain counts how many continuations in the run chain ending
// at runID have the given rootCause. The chain is traced backward via the
// successor_run_id link: starting from runID, it finds the continuation whose
// successor_run_id is runID (the continuation that created runID), then moves
// to that continuation's run_id (the predecessor run), and repeats until no
// predecessor continuation is found.
//
// The count does NOT include a continuation for runID itself (which may not
// exist yet) — callers add 1 for the current occurrence when evaluating the
// circuit breaker.
func (s *Store) CountRootCauseInChain(ctx context.Context, runID, rootCause string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	current := runID
	// Bound the walk to prevent infinite loops on cyclic data (should not
	// happen, but defensive).
	for i := 0; i < 1000; i++ {
		var contID, predRunID, rc string
		err := s.db.QueryRowContext(ctx,
			`SELECT continuation_id, run_id, root_cause FROM continuations
			 WHERE successor_run_id = ? ORDER BY created_at ASC LIMIT 1`,
			current,
		).Scan(&contID, &predRunID, &rc)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("count root cause in chain: %w", err)
		}
		if rc == rootCause {
			count++
		}
		current = predRunID
	}
	return count, nil
}

// RecordAutoContinuation inserts a fulfilled continuation record linking
// prevRunID to successorRunID with the given rootCause. This is used by
// AutoContinue to durably record each auto-continuation so the root-cause
// circuit breaker can count occurrences across the run chain.
//
// Unlike RegisterContinuation (which starts in pending and requires the
// pending→notified→fulfilled transition sequence), this creates the
// continuation directly in the fulfilled state with successor_run_id set,
// because the successor run has already been created and started.
func (s *Store) RecordAutoContinuation(ctx context.Context, c *domain.Continuation, successorRunID, eventID string) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC()
		c.CreatedAt = now
		c.State = domain.ContinuationFulfilled
		c.SuccessorRunID = successorRunID
		fulfilledAt := now

		_, err := tx.Exec(
			`INSERT INTO continuations (continuation_id, run_id, project_id, owner, successor_objective, state, wake_count, created_at, fulfilled_at, successor_run_id, root_cause)
			 VALUES (?, ?, ?, ?, ?, 'fulfilled', 0, ?, ?, ?, ?)`,
			c.ContinuationID, c.RunID, c.ProjectID, c.Owner, c.SuccessorObjective,
			c.CreatedAt.Format(time.RFC3339Nano), fulfilledAt.Format(time.RFC3339Nano),
			successorRunID, c.RootCause,
		)
		if err != nil {
			return fmt.Errorf("insert auto-continuation: %w", err)
		}

		_, err = appendEvent(tx, &domain.Event{
			EventID:   eventID,
			RunID:     c.RunID,
			EventType: "continuation.auto_recorded",
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"continuation_id":  c.ContinuationID,
				"successor_run_id": successorRunID,
				"root_cause":       c.RootCause,
			}),
			Timestamp: now,
		})
		if err != nil {
			return fmt.Errorf("append continuation.auto_recorded event: %w", err)
		}
		return nil
	})
}
