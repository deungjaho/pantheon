package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// SupersedeRun links an old run to its successor (ADR-0018, C3). This is an
// explicit PM/operator action — a link, not a state transition. The old
// run's state is NOT changed by supersede (it may be terminalized separately).
//
// Validation (all in one transaction):
//   - oldRunID and successorRunID must both exist.
//   - oldRunID != successorRunID.
//   - No existing supersede for oldRunID (one successor per run).
//
// Appends a run.superseded event. Returns the SupersedeRecord.
func (s *Store) SupersedeRun(ctx context.Context, oldRunID, successorRunID, reason string) (*domain.SupersedeRecord, error) {
	if oldRunID == "" || successorRunID == "" {
		return nil, domain.ErrInvalidInput("old_run_id and successor_run_id are required")
	}
	if oldRunID == successorRunID {
		return nil, domain.ErrInvalidInput("old_run_id and successor_run_id must differ")
	}
	if reason == "" {
		return nil, domain.ErrInvalidInput("reason is required")
	}

	rec := &domain.SupersedeRecord{
		OldRunID:       oldRunID,
		SuccessorRunID: successorRunID,
		Reason:         reason,
		CreatedAt:      time.Now().UTC(),
	}
	err := s.runInTx(ctx, func(tx *sql.Tx) error {
		// Validate both runs exist.
		var tmp string
		if err := tx.QueryRow(`SELECT run_id FROM runs WHERE run_id = ?`, oldRunID).Scan(&tmp); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrNotFound("old run not found: " + oldRunID)
			}
			return fmt.Errorf("select old run: %w", err)
		}
		if err := tx.QueryRow(`SELECT run_id FROM runs WHERE run_id = ?`, successorRunID).Scan(&tmp); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrNotFound("successor run not found: " + successorRunID)
			}
			return fmt.Errorf("select successor run: %w", err)
		}
		// Validate no existing supersede for oldRunID (one successor per run).
		var existingID string
		err := tx.QueryRow(`SELECT supersede_id FROM supersedes WHERE old_run_id = ?`, oldRunID).Scan(&existingID)
		if err == nil {
			return domain.ErrConflict("run already superseded: " + oldRunID + " (supersede_id=" + existingID + ")")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check existing supersede: %w", err)
		}

		supersedeID, err := domain.NewID("sup_")
		if err != nil {
			return fmt.Errorf("new supersede id: %w", err)
		}
		rec.SupersedeID = supersedeID

		if _, err := tx.Exec(
			`INSERT INTO supersedes(supersede_id, old_run_id, successor_run_id, reason, created_at)
			 VALUES(?, ?, ?, ?, ?)`,
			rec.SupersedeID, rec.OldRunID, rec.SuccessorRunID, rec.Reason,
			rec.CreatedAt.Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert supersede: %w", err)
		}

		// Append a run.superseded event for auditability.
		evtID, err := domain.NewID("evt_")
		if err != nil {
			return fmt.Errorf("new event id: %w", err)
		}
		_, err = appendEvent(tx, &domain.Event{
			EventID:   evtID,
			RunID:     oldRunID,
			EventType: "run.superseded",
			Severity:  domain.SeverityInfo,
			Payload: mustMarshal(map[string]string{
				"supersede_id":     rec.SupersedeID,
				"old_run_id":       rec.OldRunID,
				"successor_run_id": rec.SuccessorRunID,
				"reason":           rec.Reason,
			}),
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			return fmt.Errorf("append run.superseded event: %w", err)
		}
		// Update last_event on the old run projection.
		if _, err := tx.Exec(`UPDATE runs SET last_event = ? WHERE run_id = ?`, evtID, oldRunID); err != nil {
			return fmt.Errorf("update old run last_event: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// GetSupersede returns the supersede record for an old run, or nil if none.
func (s *Store) GetSupersede(ctx context.Context, oldRunID string) (*domain.SupersedeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rec domain.SupersedeRecord
	var createdAt string
	err := s.db.QueryRowContext(ctx,
		`SELECT supersede_id, old_run_id, successor_run_id, reason, created_at
		 FROM supersedes WHERE old_run_id = ?`, oldRunID,
	).Scan(&rec.SupersedeID, &rec.OldRunID, &rec.SuccessorRunID, &rec.Reason, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get supersede: %w", err)
	}
	rec.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	return &rec, nil
}
