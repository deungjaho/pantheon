package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"

	_ "modernc.org/sqlite"
)

// Store is the durable state for Pantheon: an append-only event journal plus a
// projection of current state, written in the same SQLite transaction.
//
// Invariants:
//   - Single writer (the daemon). Readers use read transactions.
//   - Every state change appends an event AND updates the projection in one
//     transaction. The projection is rebuildable from events.
//   - event_id and request_id are unique; duplicates are no-ops.
//   - File mode 0600, WAL mode, busy_timeout set.
type Store struct {
	db *sql.DB
	mu sync.Mutex // serializes writers; SQLite single-writer

	path string
}

// Open opens or creates the store at path, runs migrations, and runs an
// initial reconcile pass. The parent directory is created with 0700.
func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	// Single writer connection; multiple readers.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	if err := s.setFileMode(path); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: chmod: %w", err)
	}
	return s, nil
}

func (s *Store) setFileMode(path string) error {
	return os.Chmod(path, 0o600)
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Path returns the on-disk database path.
func (s *Store) Path() string { return s.path }

// --- Transactions ---

// runInTx runs fn inside a read/write transaction. The writer mutex is held
// for the duration so only one write transaction is active at a time.
func (s *Store) runInTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// --- Migrations ---

type migration struct {
	version int
	name    string
	fn      func(tx *sql.Tx) error
}

var migrations = []migration{
	{1, "initial_schema", migrateV1},
	{2, "agent_tmux_session", migrateV2},
	{3, "run_contract_v2", migrateV3},
	{4, "agent_epoch", migrateV4},
	{5, "run_project_owner", migrateV5},
	{6, "message_envelope", migrateV6},
	{7, "message_ack_retry", migrateV7},
	{8, "continuations", migrateV8},
	{9, "terminal_state", migrateV9},
	{10, "task_spec_risk", migrateV10},
	{11, "continuation_root_cause", migrateContinuationRootCause},
	{12, "findings", migrateFindings},
}

func (s *Store) migrate(ctx context.Context) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`); err != nil {
			return fmt.Errorf("create meta: %w", err)
		}
		var current int
		row := tx.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`)
		var v string
		if err := row.Scan(&v); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("read schema_version: %w", err)
			}
			current = 0
		} else {
			if _, err := fmt.Sscanf(v, "%d", &current); err != nil {
				return fmt.Errorf("parse schema_version %q: %w", v, err)
			}
		}
		for _, m := range migrations {
			if m.version <= current {
				continue
			}
			if err := m.fn(tx); err != nil {
				return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
			}
			if _, err := tx.Exec(
				`INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?)`,
				fmt.Sprintf("%d", m.version),
			); err != nil {
				return fmt.Errorf("update schema_version: %w", err)
			}
		}
		return nil
	})
}

func migrateV1(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL UNIQUE,
			run_id TEXT,
			task_id TEXT,
			agent_id TEXT,
			event_type TEXT NOT NULL,
			severity TEXT NOT NULL DEFAULT 'info',
			payload BLOB,
			timestamp TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, seq)`,
		`CREATE TABLE IF NOT EXISTS projects (
			project_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			repo_path TEXT NOT NULL,
			base_ref TEXT NOT NULL,
			registered_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS workspaces (
			workspace_id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			name TEXT NOT NULL,
			objective TEXT NOT NULL,
			state TEXT NOT NULL,
			owner TEXT NOT NULL,
			host TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(project_id) REFERENCES projects(project_id)
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			run_id TEXT PRIMARY KEY,
			workspace_id TEXT NOT NULL,
			base_commit TEXT NOT NULL,
			budget_seconds INTEGER NOT NULL,
			state TEXT NOT NULL,
			result_state TEXT NOT NULL DEFAULT 'not_started',
			started_at TEXT,
			ended_at TEXT,
			exit_code INTEGER,
			FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
		)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			task_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			objective TEXT NOT NULL,
			scope_include TEXT,
			scope_exclude TEXT,
			worktree_path TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(run_id) REFERENCES runs(run_id)
		)`,
		`CREATE TABLE IF NOT EXISTS agents (
			agent_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			task_id TEXT,
			role TEXT NOT NULL,
			runtime TEXT NOT NULL,
			pid INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL,
			session_id TEXT,
			started_at TEXT NOT NULL,
			exited_at TEXT,
			exit_code INTEGER,
			FOREIGN KEY(run_id) REFERENCES runs(run_id)
		)`,
		`CREATE TABLE IF NOT EXISTS artifacts (
			artifact_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			task_id TEXT,
			kind TEXT NOT NULL,
			path TEXT NOT NULL,
			size INTEGER NOT NULL DEFAULT 0,
			sha256 TEXT NOT NULL,
			sensitive INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS candidates (
			candidate_id TEXT PRIMARY KEY,
			task_id TEXT NOT NULL,
			run_id TEXT NOT NULL,
			ref_name TEXT NOT NULL,
			commit_sha TEXT NOT NULL,
			summary TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS idempotency (
			request_id TEXT PRIMARY KEY,
			response BLOB NOT NULL,
			created_at TEXT NOT NULL
		)`,
	}
	for _, st := range stmts {
		if _, err := tx.Exec(st); err != nil {
			return fmt.Errorf("exec: %w (stmt: %s)", err, firstLine(st))
		}
	}
	return nil
}

// migrateV2 adds the tmux_session column to the agents table for the
// notification adapter (ADR-0016).
func migrateV2(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agents ADD COLUMN tmux_session TEXT`)
	if err != nil {
		// Column may already exist if the DB was created with it.
		// SQLite returns "duplicate column name" — treat as non-fatal.
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("alter agents: %w", err)
		}
	}
	return nil
}

// migrateV3 adds the control-plane §8.2 Run contract columns to the runs
// table: epoch, lease_holder, lease_renew_deadline, last_event, checkpoint,
// evidence. The event journal is append-only and unchanged; the runs table
// remains a projection. Each ALTER is idempotent (tolerates "duplicate
// column name") so a partially-applied migration can be retried. All ALTERs
// run inside the migration transaction (runInTx), so a failure rolls back.
func migrateV3(tx *sql.Tx) error {
	alterStmts := []string{
		`ALTER TABLE runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE runs ADD COLUMN lease_holder TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN lease_renew_deadline INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE runs ADD COLUMN last_event TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN checkpoint TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN evidence TEXT NOT NULL DEFAULT '[]'`,
	}
	for _, st := range alterStmts {
		if _, err := tx.Exec(st); err != nil {
			// Column may already exist if the DB was created with it or
			// a prior migration attempt partially applied. SQLite returns
			// "duplicate column name" — treat as non-fatal and continue.
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("alter runs: %w (stmt: %s)", err, firstLine(st))
			}
		}
	}
	return nil
}

func firstLine(s string) string {
	for i, c := range s {
		if c == '\n' {
			return s[:i]
		}
	}
	return s
}

// migrateV4 adds the epoch column to the agents table. The epoch records
// the run epoch at which the agent acquired the lease (control-plane §8.2),
// enabling run.verify to reject stale verifiers whose epoch no longer
// matches the run's current epoch. Idempotent (tolerates "duplicate column
// name") so a partially-applied migration can be retried.
func migrateV4(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agents ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0`)
	if err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("alter agents: %w", err)
		}
	}
	return nil
}

// migrateV5 adds the project_id and owner columns to the runs table.
// These denormalize the workspace's project_id and owner onto each run
// so that run queries can filter/group by project or owner without a
// join to the workspaces table. Idempotent (tolerates "duplicate column
// name") so a partially-applied migration can be retried.
func migrateV5(tx *sql.Tx) error {
	alterStmts := []string{
		`ALTER TABLE runs ADD COLUMN project_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN owner TEXT NOT NULL DEFAULT ''`,
	}
	for _, st := range alterStmts {
		if _, err := tx.Exec(st); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("alter runs: %w (stmt: %s)", err, firstLine(st))
			}
		}
	}
	// Backfill existing runs from their workspaces.
	_, err := tx.Exec(`UPDATE runs SET project_id = (
		SELECT w.project_id FROM workspaces w WHERE w.workspace_id = runs.workspace_id
	) WHERE project_id = ''`)
	if err != nil {
		return fmt.Errorf("backfill project_id: %w", err)
	}
	_, err = tx.Exec(`UPDATE runs SET owner = (
		SELECT w.owner FROM workspaces w WHERE w.workspace_id = runs.workspace_id
	) WHERE owner = ''`)
	if err != nil {
		return fmt.Errorf("backfill owner: %w", err)
	}
	return nil
}

// migrateV6 adds the v1.1 message envelope columns to the events table:
//   - message_id: the typed message ID (distinct from event_id)
//   - idempotency_key: client-supplied dedup key for at-least-once delivery
//   - message_seq: per-Run monotonic sequence number (distinct from global seq)
//
// These columns are nullable so that pre-v1.1 events (including legacy
// message events with {topic, body, from, to} payloads) remain valid.
// New indexes support idempotency lookup and per-Run message ordering.
// Idempotent (tolerates "duplicate column name") so a partially-applied
// migration can be retried.
func migrateV6(tx *sql.Tx) error {
	alterStmts := []string{
		`ALTER TABLE events ADD COLUMN message_id TEXT`,
		`ALTER TABLE events ADD COLUMN idempotency_key TEXT`,
		`ALTER TABLE events ADD COLUMN message_seq INTEGER`,
	}
	for _, st := range alterStmts {
		if _, err := tx.Exec(st); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("alter events: %w (stmt: %s)", err, firstLine(st))
			}
		}
	}
	idxStmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_events_message_id ON events(message_id) WHERE message_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_events_idempotency ON events(idempotency_key) WHERE idempotency_key IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_events_run_msgseq ON events(run_id, message_seq) WHERE message_seq IS NOT NULL`,
	}
	for _, st := range idxStmts {
		if _, err := tx.Exec(st); err != nil {
			return fmt.Errorf("create index: %w (stmt: %s)", err, firstLine(st))
		}
	}
	return nil
}

// migrateV7 adds the ack_state and retry_count columns to the events table
// for C-002 (message.ack/nack, TTL/deadline, retry/backoff).
func migrateV7(tx *sql.Tx) error {
	alterStmts := []string{
		`ALTER TABLE events ADD COLUMN ack_state TEXT`,
		`ALTER TABLE events ADD COLUMN retry_count INTEGER`,
	}
	for _, st := range alterStmts {
		if _, err := tx.Exec(st); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("alter events: %w (stmt: %s)", err, firstLine(st))
			}
		}
	}
	idxStmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_events_ack_state ON events(ack_state) WHERE ack_state IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_events_msgid_ack ON events(message_id) WHERE message_id IS NOT NULL AND ack_state IS NOT NULL`,
	}
	for _, st := range idxStmts {
		if _, err := tx.Exec(st); err != nil {
			return fmt.Errorf("create index: %w (stmt: %s)", err, firstLine(st))
		}
	}
	return nil
}

// migrateV9 adds the terminal-state consistency schema (ADR-0018):
//   - next_action column on runs: the PM's explicit decision at run
//     completion (none | continuation | blocked). Empty string means "not
//     decided" — the reconcile tick surfaces terminal runs with an empty
//     next_action as the "missing decision" case. Existing runs get "".
//   - supersedes table: explicit link from an old run to its successor
//     (one successor per old run). The old run's state is NOT changed by
//     supersede — it is a link, not a state transition.
//
// Both are additive and idempotent (tolerates "duplicate column name" and
// "already exists") so a partially-applied migration can be retried. All
// statements run inside the migration transaction (runInTx), so a failure
// rolls back.
func migrateV9(tx *sql.Tx) error {
	// next_action column on runs.
	if _, err := tx.Exec(`ALTER TABLE runs ADD COLUMN next_action TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("alter runs next_action: %w", err)
		}
	}
	// supersedes table.
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS supersedes (
		supersede_id TEXT PRIMARY KEY,
		old_run_id TEXT NOT NULL UNIQUE,
		successor_run_id TEXT NOT NULL,
		reason TEXT NOT NULL,
		created_at TEXT NOT NULL,
		FOREIGN KEY(old_run_id) REFERENCES runs(run_id),
		FOREIGN KEY(successor_run_id) REFERENCES runs(run_id)
	)`); err != nil {
		return fmt.Errorf("create supersedes: %w", err)
	}
	return nil
}

// migrateV10 adds the TaskSpec / risk-graded-verification columns to the
// tasks table (Phase 2 P3+):
//   - acceptance_criteria TEXT (JSON array of verifiable conditions)
//   - constraints         TEXT (JSON array of hard limits)
//   - deliverables        TEXT (JSON array of expected outputs)
//   - risk_level          TEXT (R0-R3, default 'R2')
//
// All are additive (ALTER TABLE ADD COLUMN) and idempotent (tolerates
// "duplicate column name") so a partially-applied migration can be retried.
// The default risk_level is 'R2' (medium) — a safe default requiring human
// approval. Existing tasks get 'R2' and empty JSON arrays. All statements
// run inside the migration transaction (runInTx), so a failure rolls back.
func migrateV10(tx *sql.Tx) error {
	alterStmts := []string{
		`ALTER TABLE tasks ADD COLUMN acceptance_criteria TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE tasks ADD COLUMN constraints TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE tasks ADD COLUMN deliverables TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE tasks ADD COLUMN risk_level TEXT NOT NULL DEFAULT 'R2'`,
	}
	for _, st := range alterStmts {
		if _, err := tx.Exec(st); err != nil {
			// Column may already exist if the DB was created with it or
			// a prior migration attempt partially applied. SQLite returns
			// "duplicate column name" — treat as non-fatal and continue.
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("alter tasks: %w (stmt: %s)", err, firstLine(st))
			}
		}
	}
	return nil
}

// --- Event append ---

// AppendEvent appends an event to the journal within the given transaction.
// It returns the assigned seq. Duplicate event_id is a no-op (returns the
// existing seq).
func appendEvent(tx *sql.Tx, e *domain.Event) (int64, error) {
	var existingSeq int64
	err := tx.QueryRow(`SELECT seq FROM events WHERE event_id = ?`, e.EventID).Scan(&existingSeq)
	if err == nil {
		return existingSeq, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("check event_id: %w", err)
	}
	ts := e.Timestamp.UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(
		`INSERT INTO events(event_id, run_id, task_id, agent_id, event_type, severity, payload, timestamp,
		   message_id, idempotency_key, message_seq)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.EventID, e.RunID, e.TaskID, e.AgentID, e.EventType, string(e.Severity), []byte(e.Payload), ts,
		nullableString(e.MessageID), nullableString(e.IdempotencyKey), nullableInt64(e.MessageSeq),
	)
	if err != nil {
		return 0, fmt.Errorf("insert event: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return seq, nil
}

// nullableString returns a sql.NullString for use with nullable TEXT columns.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// nullableInt64 returns a sql.NullInt64 for use with nullable INTEGER columns.
func nullableInt64(n int64) sql.NullInt64 {
	if n == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}

// --- Idempotency ---

// GetCachedResponse returns a cached response for request_id, or nil if none.
func (s *Store) GetCachedResponse(ctx context.Context, requestID string) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT response FROM idempotency WHERE request_id = ?`, requestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get cached response: %w", err)
	}
	return json.RawMessage(raw), true, nil
}

// CacheResponse stores a response for request_id.
func (s *Store) CacheResponse(ctx context.Context, requestID string, resp json.RawMessage) error {
	return s.runInTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT OR REPLACE INTO idempotency(request_id, response, created_at) VALUES(?, ?, ?)`,
			requestID, []byte(resp), time.Now().UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			return fmt.Errorf("cache response: %w", err)
		}
		return nil
	})
}
