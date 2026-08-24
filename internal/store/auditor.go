package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// migrateFindings creates the findings table for the Global Auditor (Phase 4).
// The auditor periodically analyzes run history and produces structured
// findings (recommendations, memory candidates, policy proposals, risk
// findings). Findings are stored locally in SQLite — Mnemos integration
// (memory candidates) is deferred. The auditor does NOT auto-modify
// anything; all findings start pending and require human acceptance.
//
// Additive and idempotent (tolerates "already exists") so a partially-applied
// migration can be retried. Runs inside the migration transaction (runInTx),
// so a failure rolls back.
func migrateFindings(tx *sql.Tx) error {
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS findings (
		finding_id      TEXT PRIMARY KEY,
		type            TEXT NOT NULL,
		severity        TEXT NOT NULL,
		title           TEXT NOT NULL,
		description     TEXT NOT NULL,
		run_ids         TEXT NOT NULL DEFAULT '[]',
		evidence        TEXT NOT NULL DEFAULT '[]',
		proposed_action TEXT NOT NULL DEFAULT '',
		status          TEXT NOT NULL DEFAULT 'pending',
		created_at      TEXT NOT NULL,
		reviewed_at     TEXT,
		reviewed_by     TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		return fmt.Errorf("create findings: %w", err)
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_findings_status ON findings(status)`); err != nil {
		return fmt.Errorf("index findings status: %w", err)
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_findings_type ON findings(type)`); err != nil {
		return fmt.Errorf("index findings type: %w", err)
	}
	return nil
}

// CreateFinding inserts a new auditor finding. The FindingID, Type, Severity,
// Title, Description, and CreatedAt must be set by the caller; Status defaults
// to pending if empty. RunIDs and Evidence are stored as JSON arrays.
func (s *Store) CreateFinding(ctx context.Context, f *domain.Finding) error {
	if f.FindingID == "" {
		return domain.ErrInvalidInput("finding_id is required")
	}
	if f.Status == "" {
		f.Status = domain.FindingPending
	}
	runIDs := f.RunIDs
	if runIDs == nil {
		runIDs = []string{}
	}
	evidence := f.Evidence
	if evidence == nil {
		evidence = []string{}
	}
	runIDsJSON, err := json.Marshal(runIDs)
	if err != nil {
		return fmt.Errorf("marshal run_ids: %w", err)
	}
	evidenceJSON, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("marshal evidence: %w", err)
	}
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO findings(finding_id, type, severity, title, description, run_ids, evidence,
				proposed_action, status, created_at, reviewed_at, reviewed_by)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.FindingID, string(f.Type), string(f.Severity), f.Title, f.Description,
			string(runIDsJSON), string(evidenceJSON), f.ProposedAction, string(f.Status),
			f.CreatedAt.UTC().Format(time.RFC3339Nano),
			nullableTimeString(f.ReviewedAt), f.ReviewedBy,
		); err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return domain.ErrConflict("finding already exists: " + f.FindingID)
			}
			return fmt.Errorf("insert finding: %w", err)
		}
		return nil
	})
}

// nullableTimeString returns a sql.NullString for a *time.Time.
func nullableTimeString(t *time.Time) sql.NullString {
	if t == nil || t.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{String: t.UTC().Format(time.RFC3339Nano), Valid: true}
}

// ListFindings returns findings ordered by created_at descending. If status
// is non-empty, only findings with that status are returned. limit caps the
// result count; limit <= 0 defaults to 100.
func (s *Store) ListFindings(ctx context.Context, status string, limit int) ([]*domain.Finding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	var (
		query string
		args  []any
	)
	if status != "" {
		query = `SELECT finding_id, type, severity, title, description, run_ids, evidence,
			proposed_action, status, created_at, reviewed_at, reviewed_by
		 FROM findings WHERE status = ? ORDER BY created_at DESC LIMIT ?`
		args = []any{status, limit}
	} else {
		query = `SELECT finding_id, type, severity, title, description, run_ids, evidence,
			proposed_action, status, created_at, reviewed_at, reviewed_by
		 FROM findings ORDER BY created_at DESC LIMIT ?`
		args = []any{limit}
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list findings: %w", err)
	}
	defer rows.Close()
	var out []*domain.Finding
	for rows.Next() {
		f, err := scanFindingRow(rows)
		if err != nil {
			return nil, fmt.Errorf("list findings scan: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// GetFinding returns a finding by ID, or nil if not found.
func (s *Store) GetFinding(ctx context.Context, findingID string) (*domain.Finding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanFinding(s.db.QueryRowContext(ctx,
		`SELECT finding_id, type, severity, title, description, run_ids, evidence,
			proposed_action, status, created_at, reviewed_at, reviewed_by
		 FROM findings WHERE finding_id = ?`, findingID))
}

// ReviewFinding marks a finding as accepted or rejected by a human reviewer.
// The status must be "accepted" or "rejected"; reviewedBy identifies the
// reviewer. ReviewedAt is set to now.
func (s *Store) ReviewFinding(ctx context.Context, findingID, status, reviewedBy string) error {
	st := domain.FindingStatus(status)
	if st != domain.FindingAccepted && st != domain.FindingRejected {
		return domain.ErrInvalidInput("status must be accepted or rejected, got: " + status)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(
			`UPDATE findings SET status = ?, reviewed_at = ?, reviewed_by = ? WHERE finding_id = ?`,
			string(st), now, reviewedBy, findingID,
		)
		if err != nil {
			return fmt.Errorf("update finding review: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected: %w", err)
		}
		if n == 0 {
			return domain.ErrNotFound("finding not found: " + findingID)
		}
		return nil
	})
}

// findingScanner is the common scan interface for *sql.Row and *sql.Rows.
type findingScanner interface {
	Scan(dest ...any) error
}

// scanFindingColumns scans a finding row into a domain.Finding.
func scanFindingColumns(sc findingScanner) (*domain.Finding, error) {
	var f domain.Finding
	var (
		runIDsJSON, evidenceJSON string
		createdAt                string
		reviewedAt               sql.NullString
	)
	err := sc.Scan(
		&f.FindingID, &f.Type, &f.Severity, &f.Title, &f.Description,
		&runIDsJSON, &evidenceJSON, &f.ProposedAction, &f.Status,
		&createdAt, &reviewedAt, &f.ReviewedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan finding: %w", err)
	}
	if runIDsJSON != "" {
		_ = json.Unmarshal([]byte(runIDsJSON), &f.RunIDs)
	}
	if evidenceJSON != "" {
		_ = json.Unmarshal([]byte(evidenceJSON), &f.Evidence)
	}
	f.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	if reviewedAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, reviewedAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse reviewed_at: %w", err)
		}
		f.ReviewedAt = &t
	}
	return &f, nil
}

// scanFinding scans a finding from a *sql.Row.
func scanFinding(row *sql.Row) (*domain.Finding, error) {
	return scanFindingColumns(row)
}

// scanFindingRow scans a finding from a *sql.Rows cursor.
func scanFindingRow(rows *sql.Rows) (*domain.Finding, error) {
	return scanFindingColumns(rows)
}
