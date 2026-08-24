package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// seedRunForTerminal creates a project + workspace + run in the given state
// and returns the runID. It is a store-level helper for terminal-state tests.
func seedRunForTerminal(t *testing.T, s *Store, state domain.RunStateV2) string {
	t.Helper()
	ctx := context.Background()
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	rid := mustNewID(t, "run_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "argus", RepoPath: "/x", BaseRef: "main",
		RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy",
		CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: rid, WorkspaceID: wid, ProjectID: pid, Owner: "u",
		BaseCommit: "abc", Budget: time.Hour, State: state,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return rid
}

// seedTwoRuns creates two runs (old + successor) and returns their IDs.
func seedTwoRuns(t *testing.T, s *Store) (oldRunID, successorRunID string) {
	t.Helper()
	oldRunID = seedRunForTerminal(t, s, domain.RunV2Completed)
	successorRunID = seedRunForTerminal(t, s, domain.RunV2Requested)
	return oldRunID, successorRunID
}

func TestSupersedeRun_Success(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	oldRunID, successorRunID := seedTwoRuns(t, s)

	rec, err := s.SupersedeRun(ctx, oldRunID, successorRunID, "P2-B superseded by P2-UX")
	if err != nil {
		t.Fatalf("SupersedeRun: %v", err)
	}
	if rec.SupersedeID == "" {
		t.Fatal("empty supersede_id")
	}
	if rec.OldRunID != oldRunID {
		t.Fatalf("old_run_id = %q, want %q", rec.OldRunID, oldRunID)
	}
	if rec.SuccessorRunID != successorRunID {
		t.Fatalf("successor_run_id = %q, want %q", rec.SuccessorRunID, successorRunID)
	}
	if rec.Reason != "P2-B superseded by P2-UX" {
		t.Fatalf("reason = %q", rec.Reason)
	}
	if rec.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}

	// GetSupersede should return the same record.
	got, err := s.GetSupersede(ctx, oldRunID)
	if err != nil {
		t.Fatalf("GetSupersede: %v", err)
	}
	if got == nil {
		t.Fatal("GetSupersede returned nil")
	}
	if got.SupersedeID != rec.SupersedeID {
		t.Fatalf("supersede_id = %q, want %q", got.SupersedeID, rec.SupersedeID)
	}

	// A run.superseded event should be in the journal.
	events, err := s.EventsSince(ctx, oldRunID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var found bool
	for i := range events {
		if events[i].EventType == "run.superseded" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("run.superseded event not found in journal")
	}
}

func TestSupersedeRun_DuplicateRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	oldRunID, successorRunID := seedTwoRuns(t, s)

	if _, err := s.SupersedeRun(ctx, oldRunID, successorRunID, "first"); err != nil {
		t.Fatalf("SupersedeRun 1: %v", err)
	}
	// Second supersede for the same old run must fail (one successor per run).
	_, err := s.SupersedeRun(ctx, oldRunID, successorRunID, "second")
	if err == nil {
		t.Fatal("duplicate supersede should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("code = %q, want CONFLICT", de.Code)
	}
}

func TestSupersedeRun_SameRunRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rid := seedRunForTerminal(t, s, domain.RunV2Completed)

	_, err := s.SupersedeRun(ctx, rid, rid, "self")
	if err == nil {
		t.Fatal("supersede with same run should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", de.Code)
	}
}

func TestSupersedeRun_NonexistentRunRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, successorRunID := seedTwoRuns(t, s)

	_, err := s.SupersedeRun(ctx, "run_nonexistent", successorRunID, "x")
	if err == nil {
		t.Fatal("supersede with nonexistent old run should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", de.Code)
	}

	_, err = s.SupersedeRun(ctx, successorRunID, "run_nonexistent", "x")
	if err == nil {
		t.Fatal("supersede with nonexistent successor should fail")
	}
	de = domain.AsError(err)
	if de.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", de.Code)
	}
}

func TestSupersedeRun_OldRunStateUnchanged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Old run starts in 'running' (not terminal) — supersede must NOT change it.
	pid := mustNewID(t, "prj_")
	wid := mustNewID(t, "ws_")
	oldRunID := mustNewID(t, "run_")
	succRunID := mustNewID(t, "run_")
	if err := s.RegisterProject(ctx, &domain.Project{
		ProjectID: pid, Name: "a", RepoPath: "/x", BaseRef: "main",
		RegisteredAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("RegisterProject: %v", err)
	}
	if err := s.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid, ProjectID: pid, Name: "w", Objective: "o",
		State: domain.WorkspaceActive, Owner: "u", Host: "omarchy",
		CreatedAt: time.Now().UTC(),
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: oldRunID, WorkspaceID: wid, ProjectID: pid, Owner: "u",
		BaseCommit: "abc", Budget: time.Hour, State: domain.RunV2Running,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun old: %v", err)
	}
	if err := s.CreateRun(ctx, &domain.Run{
		RunID: succRunID, WorkspaceID: wid, ProjectID: pid, Owner: "u",
		BaseCommit: "abc", Budget: time.Hour, State: domain.RunV2Requested,
	}, mustNewID(t, "evt_")); err != nil {
		t.Fatalf("CreateRun succ: %v", err)
	}

	if _, err := s.SupersedeRun(ctx, oldRunID, succRunID, "link only"); err != nil {
		t.Fatalf("SupersedeRun: %v", err)
	}
	run, _ := s.GetRun(ctx, oldRunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("old run state = %q, want running (supersede must not change state)", run.State)
	}
}

func TestListSupersededRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	oldRunID, successorRunID := seedTwoRuns(t, s)

	if _, err := s.SupersedeRun(ctx, oldRunID, successorRunID, "test"); err != nil {
		t.Fatalf("SupersedeRun: %v", err)
	}
	list, err := s.ListSupersededRuns(ctx)
	if err != nil {
		t.Fatalf("ListSupersededRuns: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 superseded run, got %d", len(list))
	}
	if list[0].OldRunID != oldRunID {
		t.Fatalf("old_run_id = %q, want %q", list[0].OldRunID, oldRunID)
	}
	if list[0].SuccessorRunID != successorRunID {
		t.Fatalf("successor_run_id = %q, want %q", list[0].SuccessorRunID, successorRunID)
	}
	if list[0].OldRunState != string(domain.RunV2Completed) {
		t.Fatalf("old_run_state = %q, want completed", list[0].OldRunState)
	}
}

func TestMigrateV8ToV9_PreservesData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pantheon.db")
	buildV8Store(t, path)

	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open (v8->v9): %v", err)
	}
	defer s.Close()

	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if v != "12" {
		t.Fatalf("schema_version = %q, want 12", v)
	}

	// The v8 run must still be readable with next_action = "" (not decided).
	run, err := s.GetRun(context.Background(), "run_v8")
	if err != nil {
		t.Fatalf("GetRun after migration: %v", err)
	}
	if run == nil {
		t.Fatal("run_v8 not found after migration")
	}
	if run.NextAction != "" {
		t.Fatalf("next_action = %q, want empty (not decided)", run.NextAction)
	}
	// The supersedes table must exist (empty).
	rec, err := s.GetSupersede(context.Background(), "run_v8")
	if err != nil {
		t.Fatalf("GetSupersede after migration: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected no supersede for run_v8, got %+v", rec)
	}
}

// buildV8Store creates a SQLite database with only migrations v1..v8 applied
// (schema_version = 8), then inserts a run row. This simulates an existing
// v8 deployment that must be upgraded in place by the v9 migration.
func buildV8Store(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open v8 db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create meta: %v", err)
	}
	if err := applyMigrationsUpTo(tx, 8); err != nil {
		t.Fatalf("apply v1..v8: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO meta(key, value) VALUES ('schema_version', '8')
		ON CONFLICT(key) DO UPDATE SET value = '8'`); err != nil {
		t.Fatalf("set schema_version=8: %v", err)
	}
	// Insert a project + workspace + run using the v8 column set.
	if _, err := tx.Exec(`INSERT INTO projects(project_id, name, repo_path, base_ref, registered_at)
		VALUES('prj_v8', 'v8proj', '/tmp/v8', 'main', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert project: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO workspaces(workspace_id, project_id, name, objective, state, owner, host, created_at)
		VALUES('ws_v8', 'prj_v8', 'w', 'o', 'active', 'u', 'omarchy', ?)`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO runs(run_id, workspace_id, project_id, owner, base_commit, budget_seconds, state, result_state,
		epoch, lease_holder, lease_renew_deadline, last_event, checkpoint, evidence)
		VALUES('run_v8', 'ws_v8', 'prj_v8', 'u', 'abc123', 3600, 'completed', 'accepted',
		0, '', 0, '', '', '[]')`); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit v8 seed: %v", err)
	}
}
