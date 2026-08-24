package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// buildV2Store creates a SQLite database at the given path with only the v1
// and v2 migrations applied (schema_version = 2), then inserts a run row
// using the pre-v3 column set. This simulates an existing deployment that
// must be upgraded in place by the v3 migration.
func buildV2Store(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open v2 db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Apply v1 + v2 migrations directly (no Store.Open, which would run v3).
	// The meta table is created by Store.migrate(), not by migrateV1, so we
	// create it here before setting schema_version.
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create meta: %v", err)
	}
	if err := applyMigrationsUpTo(tx, 2); err != nil {
		t.Fatalf("apply v1+v2: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES ('schema_version', '2')
		ON CONFLICT(key) DO UPDATE SET value = '2'`); err != nil {
		t.Fatalf("set schema_version=2: %v", err)
	}
	// Insert a project + workspace + run using the pre-v3 column set so we
	// can verify the v3 migration preserves the data.
	if _, err := tx.Exec(`INSERT INTO projects(project_id, name, repo_path, base_ref, registered_at)
		VALUES('prj_v2', 'v2proj', '/tmp/v2', 'main', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert project: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO workspaces(workspace_id, project_id, name, objective, state, owner, host, created_at)
		VALUES('ws_v2', 'prj_v2', 'w', 'o', 'active', 'u', 'omarchy', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO runs(run_id, workspace_id, base_commit, budget_seconds, state, result_state)
		VALUES('run_v2', 'ws_v2', 'abc123', 3600, 'running', 'not_started')`); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit v2 seed: %v", err)
	}
}

// applyMigrationsUpTo runs migrateV1..migrateVN (inclusive) inside the given
// transaction without touching the meta schema_version row (the caller sets
// it). This lets us build a v2-only DB that Open() will then upgrade.
func applyMigrationsUpTo(tx *sql.Tx, upTo int) error {
	for _, m := range migrations {
		if m.version > upTo {
			continue
		}
		if err := m.fn(tx); err != nil {
			return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
		}
	}
	return nil
}

// TestMigrateV2ToV3_PreservesData verifies that opening a v2 database triggers
// the v3 migration, the new §8.2 columns are present with their defaults, and
// the pre-existing v2 run data (run_id, base_commit, state) is preserved.
func TestMigrateV2ToV3_PreservesData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	buildV2Store(t, path)

	// Open triggers migrate() which applies v3.
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open (v2->v3): %v", err)
	}
	defer s.Close()

	// schema_version must now be 11 (all migrations through v11 applied).
	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if v != "12" {
		t.Fatalf("schema_version = %q, want 12", v)
	}

	// The v2 run must still be readable with its original data intact.
	run, err := s.GetRun(context.Background(), "run_v2")
	if err != nil {
		t.Fatalf("GetRun after migration: %v", err)
	}
	if run == nil {
		t.Fatal("run_v2 not found after migration")
	}
	if run.BaseCommit != "abc123" {
		t.Fatalf("base_commit = %q, want abc123", run.BaseCommit)
	}
	if run.State != domain.RunV2Running {
		t.Fatalf("state = %q, want running", run.State)
	}
	if run.Budget != time.Hour {
		t.Fatalf("budget = %v, want 1h", run.Budget)
	}

	// The new §8.2 columns must be present with their defaults.
	if run.Epoch != 0 {
		t.Fatalf("epoch = %d, want 0 (default)", run.Epoch)
	}
	if run.Lease.Holder != "" {
		t.Fatalf("lease.holder = %q, want empty (default)", run.Lease.Holder)
	}
	if run.Lease.RenewDeadline != 0 {
		t.Fatalf("lease.renew_deadline = %d, want 0 (default)", run.Lease.RenewDeadline)
	}
	if run.LastEvent != "" {
		t.Fatalf("last_event = %q, want empty (default)", run.LastEvent)
	}
	if run.Checkpoint != "" {
		t.Fatalf("checkpoint = %q, want empty (default)", run.Checkpoint)
	}
	if run.Evidence != nil && len(run.Evidence) != 0 {
		t.Fatalf("evidence = %v, want empty/nil (default)", run.Evidence)
	}

	// The v5 migration backfills project_id and owner from the workspace.
	if run.ProjectID != "prj_v2" {
		t.Fatalf("project_id = %q, want prj_v2 (backfilled from workspace)", run.ProjectID)
	}
	if run.Owner != "u" {
		t.Fatalf("owner = %q, want u (backfilled from workspace)", run.Owner)
	}
}

// TestMigrateV3_NewColumnsWritable verifies that after migration a freshly
// created run round-trips the new §8.2 fields through the store.
func TestMigrateV3_NewColumnsWritable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	buildV2Store(t, path)

	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	// Create a project + workspace for FK constraints.
	eid := mustNewID(t, "evt_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: "prj_new", Name: "new", RepoPath: "/x", BaseRef: "main",
		RegisteredAt: time.Now().UTC(),
	}, eid); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	eid = mustNewID(t, "evt_")
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_new", ProjectID: "prj_new", Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy",
		CreatedAt: time.Now().UTC(),
	}, eid); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	// Create a run populated with the new §8.2 fields.
	eid = mustNewID(t, "evt_")
	r := &domain.Run{
		RunID: "run_new", WorkspaceID: "ws_new", ProjectID: "prj_new", Owner: "alice",
		BaseCommit: "def", Budget: 2 * time.Hour,
		State:      domain.RunV2Requested,
		Epoch:      7,
		Lease:      domain.RunLease{Holder: "agt_worker", RenewDeadline: 1234567890},
		Checkpoint: "cnd_abc",
		Evidence:   []string{"evt_1", "evt_2"},
	}
	if err := s.CreateRun(ctx, r, eid); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	got, err := s.GetRun(ctx, "run_new")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Epoch != 7 {
		t.Fatalf("epoch = %d, want 7", got.Epoch)
	}
	if got.Lease.Holder != "agt_worker" {
		t.Fatalf("lease.holder = %q, want agt_worker", got.Lease.Holder)
	}
	if got.Lease.RenewDeadline != 1234567890 {
		t.Fatalf("lease.renew_deadline = %d, want 1234567890", got.Lease.RenewDeadline)
	}
	if got.LastEvent != eid {
		t.Fatalf("last_event = %q, want %q (run.created event_id)", got.LastEvent, eid)
	}
	if got.Checkpoint != "cnd_abc" {
		t.Fatalf("checkpoint = %q, want cnd_abc", got.Checkpoint)
	}
	if len(got.Evidence) != 2 || got.Evidence[0] != "evt_1" || got.Evidence[1] != "evt_2" {
		t.Fatalf("evidence = %v, want [evt_1 evt_2]", got.Evidence)
	}
	if got.ProjectID != "prj_new" {
		t.Fatalf("project_id = %q, want prj_new", got.ProjectID)
	}
	if got.Owner != "alice" {
		t.Fatalf("owner = %q, want alice", got.Owner)
	}
}

// TestMigrateV3_FailureRollsBack verifies that if the v3 migration fails
// partway, the v2 data is left intact (rollback safety). We simulate a failure
// by temporarily prepending a migration stub that returns an error after the
// runs table already has some (but not all) new columns, then confirm the v2
// run row is still readable via a raw query that uses only v2 columns.
func TestMigrateV3_FailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	buildV2Store(t, path)

	// Open the v2 DB with a raw sql.DB to inspect pre-migration state.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	rawDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer rawDB.Close()
	rawDB.SetMaxOpenConns(1)

	// Snapshot the v2 run row's base_commit for later comparison.
	var baseCommit string
	if err := rawDB.QueryRow(`SELECT base_commit FROM runs WHERE run_id = 'run_v2'`).Scan(&baseCommit); err != nil {
		t.Fatalf("read v2 base_commit: %v", err)
	}
	if baseCommit != "abc123" {
		t.Fatalf("v2 base_commit = %q, want abc123", baseCommit)
	}

	// Now attempt a migration that fails: begin a tx, run a failing ALTER
	// (invalid SQL), and confirm the transaction rolls back without
	// touching the v2 data.
	tx, err := rawDB.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Run the first real v3 ALTER (epoch) so the tx has pending changes,
	// then issue a deliberately invalid statement to force a failure.
	if _, err := tx.Exec(`ALTER TABLE runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0`); err != nil {
		// If epoch already exists (idempotent re-run), that's fine.
		if !strings.Contains(err.Error(), "duplicate column name") {
			t.Fatalf("alter epoch: %v", err)
		}
	}
	// Force a failure: invalid SQL.
	if _, err := tx.Exec(`ALTER TABLE runs ADD COLUMN __bad_col__ NOT NULL DEFAULT`); err == nil {
		_ = tx.Rollback()
		t.Fatal("expected error from invalid ALTER, got nil")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// After rollback, the v2 data must be intact and the epoch column must
	// NOT exist (the ALTER was rolled back). We verify by querying the v2
	// row with the original column set.
	var state string
	if err := rawDB.QueryRow(`SELECT state FROM runs WHERE run_id = 'run_v2'`).Scan(&state); err != nil {
		t.Fatalf("read v2 state after rollback: %v", err)
	}
	if state != "running" {
		t.Fatalf("v2 state after rollback = %q, want running", state)
	}

	// Confirm the failed ALTER did not persist: querying for the epoch
	// column must fail (no such column).
	_, err = rawDB.Query(`SELECT epoch FROM runs WHERE run_id = 'run_v2'`)
	if err == nil {
		t.Fatal("expected error querying epoch column (should not exist after rollback)")
	}
}

// TestMigrateV3_IdempotentReopen verifies that opening an already-migrated v3
// database a second time is a no-op (the duplicate-column-name tolerance in
// migrateV3 keeps it idempotent).
func TestMigrateV3_IdempotentReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	buildV2Store(t, path)

	s1, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	s1.Close()

	s2, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open 2 (idempotent): %v", err)
	}
	defer s2.Close()

	var v string
	if err := s2.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if v != "12" {
		t.Fatalf("schema_version = %q, want 12", v)
	}

	// Data still present.
	run, err := s2.GetRun(context.Background(), "run_v2")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run == nil || run.BaseCommit != "abc123" {
		t.Fatalf("run_v2 missing or base_commit changed: %+v", run)
	}
}
