package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/store"
)

func newTestService(t *testing.T) (*Service, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(context.Background(), dir+"/pantheon.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	svc := &Service{
		Store:         s,
		ServerName:    "pantheond-test",
		ServerVersion: "0.0.1-test",
	}
	return svc, s
}

func callRPC(t *testing.T, svc *Service, method string, params any) *Response {
	t.Helper()
	srv := NewServer(new(bytes.Buffer))
	svc.RegisterAll(srv)
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}
		raw = b
	}
	req := &Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Method:  method,
		Params:  raw,
	}
	h, ok := srv.handlers[method]
	if !ok {
		t.Fatalf("no handler for %s", method)
	}
	result, err := h(context.Background(), req.Params)
	if err != nil {
		return &Response{JSONRPC: "2.0", ID: req.ID, Error: domain.AsError(err)}
	}
	b, _ := json.Marshal(result)
	return &Response{JSONRPC: "2.0", ID: req.ID, Result: b}
}

// createAndStartRun is a test helper that performs the v2 run.create +
// run.start sequence (replacing the legacy run.submit facade) and returns
// the RunCreateResult. It fatals on any error.
func createAndStartRun(t *testing.T, svc *Service, params RunCreateParams) *RunCreateResult {
	t.Helper()
	createResp := callRPC(t, svc, "run.create", params)
	if createResp.Error != nil {
		t.Fatalf("run.create error: %v", createResp.Error)
	}
	var result RunCreateResult
	json.Unmarshal(createResp.Result, &result)
	startResp := callRPC(t, svc, "run.start", RunStartParams{RunID: result.RunID})
	if startResp.Error != nil {
		t.Fatalf("run.start error: %v", startResp.Error)
	}
	return &result
}

func TestInitialize(t *testing.T) {
	svc, _ := newTestService(t)
	resp := callRPC(t, svc, "initialize", InitializeParams{
		ClientName: "pantheon-cli", ClientVersion: "0.1.0",
	})
	if resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	var r InitializeResult
	if err := json.Unmarshal(resp.Result, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.ServerName != "pantheond-test" {
		t.Fatalf("server_name = %q", r.ServerName)
	}
	if r.Protocol != 1 {
		t.Fatalf("protocol = %d, want 1", r.Protocol)
	}
	if len(r.Capabilities) == 0 {
		t.Fatal("no capabilities")
	}
}

func TestProjectRegisterAndRunSubmit(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Register project.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/home/camt/Work/Argus", BaseRef: "main",
	})
	if resp.Error != nil {
		t.Fatalf("project.register error: %v", resp.Error)
	}
	var pr ProjectRegisterResult
	if err := json.Unmarshal(resp.Result, &pr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pr.ProjectID == "" {
		t.Fatal("empty project_id")
	}

	// Create + start run (no workspace manager or runtime; uses placeholder paths).
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID,
		Objective: "add connection epoch to recorder",
	})
	if rs.RunID == "" || rs.WorkspaceID == "" || rs.TaskID == "" {
		t.Fatalf("missing ids: %+v", rs)
	}

	// Verify run is in running state.
	run, err := svc.Store.GetRun(ctx, rs.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running", run.State)
	}
	if run.Budget != DefaultBudget {
		t.Fatalf("budget = %v, want %v", run.Budget, DefaultBudget)
	}
}

func TestRunStatusNotFound(t *testing.T) {
	svc, _ := newTestService(t)
	resp := callRPC(t, svc, "run.status", RunStatusParams{RunID: "run_nonexistent"})
	if resp.Error == nil {
		t.Fatal("expected error for nonexistent run")
	}
	if resp.Error.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", resp.Error.Code)
	}
}

func TestRunBlockUnblockTerminate(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Setup: register project + create + start run.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "test task",
	})

	// Block.
	resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("block error: %v", resp.Error)
	}
	var blockRes RunBlockResult
	json.Unmarshal(resp.Result, &blockRes)
	if blockRes.State != domain.RunV2Blocked {
		t.Fatalf("state = %q, want blocked", blockRes.State)
	}
	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Blocked {
		t.Fatalf("state = %q, want blocked", run.State)
	}

	// Unblock.
	resp = callRPC(t, svc, "run.unblock", RunUnblockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("unblock error: %v", resp.Error)
	}
	var unblockRes RunUnblockResult
	json.Unmarshal(resp.Result, &unblockRes)
	if unblockRes.State != domain.RunV2Running {
		t.Fatalf("state = %q, want running", unblockRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("state = %q, want running", run.State)
	}

	// Terminate.
	resp = callRPC(t, svc, "run.terminate", RunTerminateParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("terminate error: %v", resp.Error)
	}
	var termRes RunTerminateResult
	json.Unmarshal(resp.Result, &termRes)
	if termRes.State != domain.RunV2Canceled {
		t.Fatalf("state = %q, want canceled", termRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Canceled {
		t.Fatalf("state = %q, want canceled", run.State)
	}
}

func TestRunEvents(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "test",
	})

	// Query events.
	resp = callRPC(t, svc, "run.events", RunEventsParams{RunID: rs.RunID, Cursor: 0})
	if resp.Error != nil {
		t.Fatalf("run.events error: %v", resp.Error)
	}
	var er RunEventsResult
	if err := json.Unmarshal(resp.Result, &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(er.Events) == 0 {
		t.Fatal("no events returned")
	}
	for _, e := range er.Events {
		if e.RunID != rs.RunID {
			t.Fatalf("event run_id = %q, want %q", e.RunID, rs.RunID)
		}
	}
}

func TestMalformedRequest(t *testing.T) {
	out := new(bytes.Buffer)
	srv := NewServer(out)
	svc, _ := newTestService(t)
	svc.RegisterAll(srv)

	in := strings.NewReader("not json\n")
	if err := srv.Serve(context.Background(), in); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 response, got %d", len(lines))
	}
	var resp Response
	if err := json.Unmarshal([]byte(lines[0]), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("expected INVALID_INPUT error, got %+v", resp)
	}
}

func TestMethodNotFound(t *testing.T) {
	out := new(bytes.Buffer)
	srv := NewServer(out)
	svc, _ := newTestService(t)
	svc.RegisterAll(srv)

	req := `{"jsonrpc":"2.0","id":"1","method":"nonexistent.method","params":{}}`
	in := strings.NewReader(req + "\n")
	if err := srv.Serve(context.Background(), in); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var resp Response
	if err := json.Unmarshal([]byte(out.String()), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != domain.CodeNotFound {
		t.Fatalf("expected NOT_FOUND, got %+v", resp)
	}
}

func TestBudgetOverride(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	customBudget := 2 * time.Hour
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "test", Budget: customBudget,
	})

	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.Budget != customBudget {
		t.Fatalf("budget = %v, want %v", run.Budget, customBudget)
	}
}

// fakeRuntime is a test RuntimeAdapter that records calls.
type fakeRuntime struct {
	mu         sync.Mutex
	started    []RuntimeStartParams
	stopped    []RuntimeHandle
	inspected  []RuntimeHandle
	startCount int
	startErr   error
	stopErr    error
}

func (f *fakeRuntime) Start(ctx context.Context, p RuntimeStartParams) (RuntimeHandle, error) {
	if f.startErr != nil {
		return RuntimeHandle{}, f.startErr
	}
	f.mu.Lock()
	f.startCount++
	idx := f.startCount
	f.started = append(f.started, p)
	f.mu.Unlock()
	return RuntimeHandle{
		AgentID:   fmt.Sprintf("agt_fake_%d", idx),
		PID:       10000 + idx,
		SessionID: p.SessionID,
	}, nil
}

func (f *fakeRuntime) Stop(ctx context.Context, h RuntimeHandle, grace time.Duration) error {
	if f.stopErr != nil {
		return f.stopErr
	}
	f.mu.Lock()
	f.stopped = append(f.stopped, h)
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Inspect(ctx context.Context, h RuntimeHandle) (RuntimeStatus, error) {
	f.mu.Lock()
	f.inspected = append(f.inspected, h)
	f.mu.Unlock()
	return RuntimeStatus{State: domain.AgentRunning}, nil
}

func TestRunBlockWithRuntimeAndCheckpoint(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Wire a fake runtime and checkpoint manager.
	fr := &fakeRuntime{}
	svc.Runtime = fr
	checkpointCalled := false
	svc.Checkpoint = CheckpointManager{
		CreateCheckpoint: func(ctx context.Context, taskID, runID, worktreePath, summary string) (string, error) {
			checkpointCalled = true
			if worktreePath == "" {
				t.Error("worktreePath should not be empty")
			}
			return "cnd_test123", nil
		},
		GetCandidate: func(ctx context.Context, candidateID string) (*domain.Candidate, error) {
			return &domain.Candidate{CandidateID: candidateID}, nil
		},
	}

	// Setup: register project + create + start run (this starts the fake runtime).
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "test task",
	})

	if len(fr.started) != 1 {
		t.Fatalf("expected runtime.Start to be called once, got %d", len(fr.started))
	}

	// Block — should stop runtime + create checkpoint.
	resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("block error: %v", resp.Error)
	}

	var blockResult RunBlockResult
	json.Unmarshal(resp.Result, &blockResult)
	if blockResult.State != domain.RunV2Blocked {
		t.Fatalf("state = %q, want blocked", blockResult.State)
	}
	if blockResult.CandidateID != "cnd_test123" {
		t.Fatalf("candidate_id = %q, want cnd_test123", blockResult.CandidateID)
	}
	if len(fr.stopped) != 1 {
		t.Fatalf("expected runtime.Stop to be called once, got %d", len(fr.stopped))
	}
	if !checkpointCalled {
		t.Fatal("expected CheckpointManager.CreateCheckpoint to be called")
	}

	// Verify candidate was saved to store.
	cand, _ := svc.Store.GetCandidate(ctx, "cnd_test123")
	if cand == nil {
		t.Fatal("candidate not saved to store")
	}
	if cand.RunID != rs.RunID {
		t.Fatalf("candidate run_id = %q, want %q", cand.RunID, rs.RunID)
	}

	// Unblock — should start runtime with prior SessionID.
	resp = callRPC(t, svc, "run.unblock", RunUnblockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("unblock error: %v", resp.Error)
	}
	if len(fr.started) != 2 {
		t.Fatalf("expected runtime.Start to be called twice (submit + unblock), got %d", len(fr.started))
	}

	// Verify unblock reused the SessionID from the first agent.
	if fr.started[1].SessionID != fr.started[0].SessionID {
		t.Fatal("unblock should reuse SessionID from prior agent")
	}

	// Terminate — should stop runtime.
	resp = callRPC(t, svc, "run.terminate", RunTerminateParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("terminate error: %v", resp.Error)
	}
	if len(fr.stopped) != 2 {
		t.Fatalf("expected runtime.Stop to be called twice (block + terminate), got %d", len(fr.stopped))
	}
}

func TestRunTakeoverWithStoredCandidate(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()

	// Create project + workspace first (FK constraints).
	eid, _ := newEventID()
	_ = st.RegisterProject(ctx, &domain.Project{
		ProjectID:    "prj_orig",
		Name:         "test",
		RepoPath:     "/tmp/test",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}, eid)
	eid, _ = newEventID()
	if err := st.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_orig",
		ProjectID:   "prj_orig",
		Name:        "test",
		Objective:   "test",
		State:       domain.WorkspaceActive,
		Owner:       "test",
		Host:        "test",
		CreatedAt:   time.Now().UTC(),
	}, eid); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	// Manually insert a candidate into the store.
	eid, _ = newEventID()
	cand := &domain.Candidate{
		CandidateID: "cnd_test_takeover",
		TaskID:      "tsk_orig",
		RunID:       "ws_orig", // takeover uses cand.RunID as WorkspaceID
		RefName:     "refs/pantheon/cnd_test_takeover",
		CommitSHA:   "abc123def456",
		Summary:     "original work",
		CreatedAt:   time.Now().UTC(),
	}
	if err := st.SaveCandidate(ctx, cand, eid); err != nil {
		t.Fatalf("SaveCandidate: %v", err)
	}

	// Takeover from the candidate.
	resp := callRPC(t, svc, "run.takeover", RunTakeoverParams{
		CandidateID: "cnd_test_takeover",
		Objective:   "continue the work",
	})
	if resp.Error != nil {
		t.Fatalf("takeover error: %v", resp.Error)
	}
	var result RunTakeoverResult
	json.Unmarshal(resp.Result, &result)
	if result.RunID == "" {
		t.Fatal("run_id is empty")
	}
	if result.TaskID == "" {
		t.Fatal("task_id is empty")
	}

	// Verify the new run uses the candidate's commit SHA as base.
	run, _ := st.GetRun(ctx, result.RunID)
	if run == nil {
		t.Fatal("new run not found in store")
	}
	if run.BaseCommit != "abc123def456" {
		t.Fatalf("base_commit = %q, want abc123def456", run.BaseCommit)
	}
}

func TestRunTakeoverCandidateNotFound(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "run.takeover", RunTakeoverParams{
		CandidateID: "cnd_nonexistent",
		Objective:   "test",
	})
	if resp.Error == nil {
		t.Fatal("expected error for nonexistent candidate")
	}
}

func TestReconcile(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()

	// Create project + workspace first (FK constraints).
	eid, _ := newEventID()
	_ = st.RegisterProject(ctx, &domain.Project{
		ProjectID:    "prj_reconcile",
		Name:         "test",
		RepoPath:     "/tmp/test",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}, eid)
	eid, _ = newEventID()
	if err := st.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_reconcile",
		ProjectID:   "prj_reconcile",
		Name:        "test",
		Objective:   "test",
		State:       domain.WorkspaceActive,
		Owner:       "test",
		Host:        "test",
		CreatedAt:   time.Now().UTC(),
	}, eid); err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}

	// Insert a run in running state (simulating a crash).
	eid, _ = newEventID()
	_ = st.CreateRun(ctx, &domain.Run{
		RunID:       "run_crashed",
		WorkspaceID: "ws_reconcile",
		BaseCommit:  "abc",
		Budget:      time.Hour,
		State:       domain.RunV2Running,
	}, eid)

	// Reconcile should mark it as failed.
	resp := callRPC(t, svc, "reconcile.crash", nil)
	if resp.Error != nil {
		t.Fatalf("reconcile.crash error: %v", resp.Error)
	}
	var result ReconcileResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Reconciled) == 0 {
		t.Fatal("expected reconciled items")
	}

	// Verify the run is now failed.
	run, _ := st.GetRun(ctx, "run_crashed")
	if run.State != domain.RunV2Failed {
		t.Fatalf("run state = %q, want failed", run.State)
	}
}

func TestRunList(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()

	// Create project + workspace + 2 runs.
	eid, _ := newEventID()
	_ = st.RegisterProject(ctx, &domain.Project{
		ProjectID:    "prj_list",
		Name:         "test",
		RepoPath:     "/tmp/test",
		BaseRef:      "main",
		RegisteredAt: time.Now().UTC(),
	}, eid)
	eid, _ = newEventID()
	_ = st.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: "ws_list",
		ProjectID:   "prj_list",
		Name:        "test",
		Objective:   "test",
		State:       domain.WorkspaceActive,
		Owner:       "test",
		Host:        "test",
		CreatedAt:   time.Now().UTC(),
	}, eid)
	eid, _ = newEventID()
	_ = st.CreateRun(ctx, &domain.Run{
		RunID:       "run_list_1",
		WorkspaceID: "ws_list",
		BaseCommit:  "abc",
		Budget:      time.Hour,
		State:       domain.RunV2Running,
	}, eid)
	eid, _ = newEventID()
	_ = st.CreateRun(ctx, &domain.Run{
		RunID:       "run_list_2",
		WorkspaceID: "ws_list",
		BaseCommit:  "def",
		Budget:      time.Hour,
		State:       domain.RunV2Blocked,
	}, eid)

	// List all runs.
	resp := callRPC(t, svc, "run.list", nil)
	if resp.Error != nil {
		t.Fatalf("run.list error: %v", resp.Error)
	}
	var result RunListResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(result.Runs))
	}

	// List filtered by V2 state.
	resp = callRPC(t, svc, "run.list", RunListParams{State: domain.RunV2Running})
	json.Unmarshal(resp.Result, &result)
	if len(result.Runs) != 1 {
		t.Fatalf("expected 1 running run, got %d", len(result.Runs))
	}
	if result.Runs[0].RunID != "run_list_1" {
		t.Fatalf("run_id = %q, want run_list_1", result.Runs[0].RunID)
	}
	if result.Runs[0].State != domain.RunV2Running {
		t.Fatalf("state = %q, want running (V2)", result.Runs[0].State)
	}

	// List filtered by V2 blocked state.
	resp = callRPC(t, svc, "run.list", RunListParams{State: domain.RunV2Blocked})
	json.Unmarshal(resp.Result, &result)
	if len(result.Runs) != 1 {
		t.Fatalf("expected 1 blocked run, got %d", len(result.Runs))
	}
	if result.Runs[0].RunID != "run_list_2" {
		t.Fatalf("run_id = %q, want run_list_2", result.Runs[0].RunID)
	}
	if result.Runs[0].State != domain.RunV2Blocked {
		t.Fatalf("state = %q, want blocked (V2)", result.Runs[0].State)
	}
}

func TestRunListEmpty(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "run.list", nil)
	if resp.Error != nil {
		t.Fatalf("run.list error: %v", resp.Error)
	}
	var result RunListResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(result.Runs))
	}
}

// TestConcurrentRunSubmit verifies that 2 concurrent run.create + run.start
// sequences both succeed, both reach running state with independent agents,
// both produce independent checkpoints on pause, and run.list sees both.
func TestConcurrentRunSubmit(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()

	// Wire a fake runtime that supports concurrent starts.
	fr := &fakeRuntime{}
	svc.Runtime = fr
	var checkpointMu sync.Mutex
	checkpointCount := 0
	svc.Checkpoint = CheckpointManager{
		CreateCheckpoint: func(ctx context.Context, taskID, runID, worktreePath, summary string) (string, error) {
			checkpointMu.Lock()
			checkpointCount++
			cid := fmt.Sprintf("cnd_concurrent_%d", checkpointCount)
			checkpointMu.Unlock()
			return cid, nil
		},
		GetCandidate: func(ctx context.Context, candidateID string) (*domain.Candidate, error) {
			return &domain.Candidate{CandidateID: candidateID}, nil
		},
	}

	// Register a project.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "concurrent-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	// Create + start 2 runs concurrently.
	type submitResult struct {
		runID  string
		taskID string
		err    error
	}
	resultCh := make(chan submitResult, 2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			createResp := callRPC(t, svc, "run.create", RunCreateParams{
				ProjectID: pr.ProjectID,
				Objective: fmt.Sprintf("concurrent task %d", idx),
			})
			if createResp.Error != nil {
				resultCh <- submitResult{err: fmt.Errorf("run %d create error: %v", idx, createResp.Error)}
				return
			}
			var cr RunCreateResult
			json.Unmarshal(createResp.Result, &cr)
			startResp := callRPC(t, svc, "run.start", RunStartParams{RunID: cr.RunID})
			if startResp.Error != nil {
				resultCh <- submitResult{err: fmt.Errorf("run %d start error: %v", idx, startResp.Error)}
				return
			}
			resultCh <- submitResult{runID: cr.RunID, taskID: cr.TaskID}
		}(i)
	}

	// Collect results.
	var results [2]submitResult
	for i := 0; i < 2; i++ {
		results[i] = <-resultCh
	}

	// Verify both succeeded.
	var runIDs [2]string
	for i := 0; i < 2; i++ {
		if results[i].err != nil {
			t.Fatalf("%v", results[i].err)
		}
		runIDs[i] = results[i].runID
		if results[i].runID == "" {
			t.Fatalf("run %d: empty run_id", i)
		}
		if results[i].taskID == "" {
			t.Fatalf("run %d: empty task_id", i)
		}
	}

	// Verify run IDs are unique (no collision).
	if runIDs[0] == runIDs[1] {
		t.Fatal("both runs got the same run_id — concurrency bug")
	}

	// Verify both runs are in running state.
	for i := 0; i < 2; i++ {
		run, _ := st.GetRun(ctx, runIDs[i])
		if run == nil {
			t.Fatalf("run %d (%s) not found in store", i, runIDs[i])
		}
		if run.State != domain.RunV2Running {
			t.Fatalf("run %d state = %q, want running", i, run.State)
		}
	}

	// Verify 2 agents were registered with unique agent IDs.
	agent1, _ := st.GetAgentByRun(ctx, runIDs[0])
	agent2, _ := st.GetAgentByRun(ctx, runIDs[1])
	if agent1 == nil || agent2 == nil {
		t.Fatal("one or both agents not found")
	}
	if agent1.AgentID == agent2.AgentID {
		t.Fatal("both runs got the same agent_id — concurrency bug")
	}
	if agent1.PID == agent2.PID {
		t.Fatal("both agents got the same PID — concurrency bug")
	}

	// Verify runtime.Start was called twice.
	if len(fr.started) != 2 {
		t.Fatalf("expected runtime.Start called 2 times, got %d", len(fr.started))
	}

	// Verify run.list returns both runs.
	resp = callRPC(t, svc, "run.list", RunListParams{State: domain.RunV2Running})
	if resp.Error != nil {
		t.Fatalf("run.list error: %v", resp.Error)
	}
	var listResult RunListResult
	json.Unmarshal(resp.Result, &listResult)
	if len(listResult.Runs) != 2 {
		t.Fatalf("run.list returned %d runs, want 2", len(listResult.Runs))
	}

	// Block both runs — each should produce an independent checkpoint.
	for i := 0; i < 2; i++ {
		resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: runIDs[i]})
		if resp.Error != nil {
			t.Fatalf("run %d block error: %v", i, resp.Error)
		}
		var blockResult RunBlockResult
		json.Unmarshal(resp.Result, &blockResult)
		if blockResult.State != domain.RunV2Blocked {
			t.Fatalf("run %d state = %q, want blocked (V2)", i, blockResult.State)
		}
		if blockResult.CandidateID == "" {
			t.Fatalf("run %d: empty candidate_id after block", i)
		}
	}

	// Verify both candidates are unique and saved.
	cand1, _ := st.GetCandidate(ctx, "cnd_concurrent_1")
	cand2, _ := st.GetCandidate(ctx, "cnd_concurrent_2")
	if cand1 == nil || cand2 == nil {
		t.Fatal("one or both candidates not saved to store")
	}
	if cand1.RunID == cand2.RunID {
		t.Fatal("both candidates point to the same run — checkpoint isolation bug")
	}

	// Verify both runs are now blocked.
	for i := 0; i < 2; i++ {
		run, _ := st.GetRun(ctx, runIDs[i])
		if run.State != domain.RunV2Blocked {
			t.Fatalf("run %d state = %q, want blocked", i, run.State)
		}
	}

	// Verify runtime.Stop was called twice (once per block).
	if len(fr.stopped) != 2 {
		t.Fatalf("expected runtime.Stop called 2 times, got %d", len(fr.stopped))
	}

	// Verify run.list now shows 2 blocked runs.
	resp = callRPC(t, svc, "run.list", RunListParams{State: domain.RunV2Blocked})
	json.Unmarshal(resp.Result, &listResult)
	if len(listResult.Runs) != 2 {
		t.Fatalf("run.list(blocked) returned %d runs, want 2", len(listResult.Runs))
	}
}

// fakeNotifier is a test TmuxNotifier that records calls.
type fakeNotifier struct {
	calls []struct{ agentID, msg string }
	err   error
}

func (f *fakeNotifier) Notify(ctx context.Context, agentID, message string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct{ agentID, msg string }{agentID, message})
	return nil
}

func TestMessagePublish(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		MessageID:      "msg_test_1",
		RunID:          "run_test",
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "idem_test_1",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "start working on Phase 2"},
	})
	if resp.Error != nil {
		t.Fatalf("message.publish.envelope error: %v", resp.Error)
	}
	var result MessagePublishEnvelopeResult
	json.Unmarshal(resp.Result, &result)
	if result.Seq == 0 {
		t.Fatal("expected non-zero seq")
	}
	if result.MessageSeq == 0 {
		t.Fatal("expected non-zero message_seq")
	}
	if result.MessageID != "msg_test_1" {
		t.Fatalf("message_id = %q, want msg_test_1", result.MessageID)
	}
	if result.Deduped {
		t.Fatal("expected deduped=false on first publish")
	}
}

func TestMessagePublishValidation(t *testing.T) {
	svc, _ := newTestService(t)

	// Missing run_id (message_id is auto-generated, so test run_id instead).
	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "idem_val_1",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "hello"},
	})
	if resp.Error == nil {
		t.Fatal("expected error for missing run_id")
	}

	// Missing payload (empty PayloadRef.Kind).
	resp = callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		MessageID:      "msg_val_2",
		RunID:          "run_val",
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
		Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "idem_val_2",
	})
	if resp.Error == nil {
		t.Fatal("expected error for missing payload_ref.kind")
	}
}

func TestMessageSubscribe(t *testing.T) {
	svc, _ := newTestService(t)

	// Publish 3 messages to run_sub and 1 to run_other.
	pub := func(msgID, runID string, msgType domain.MessageType) {
		resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      msgID,
			RunID:          runID,
			Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           msgType,
			IdempotencyKey: "idem_" + msgID,
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "body_" + msgID},
		})
		if resp.Error != nil {
			t.Fatalf("publish %s: %v", msgID, resp.Error)
		}
	}
	pub("msg_sub_1", "run_sub", domain.MsgDirective)
	pub("msg_sub_2", "run_sub", domain.MsgReport)
	pub("msg_sub_3", "run_sub", domain.MsgDirective)
	pub("msg_other_1", "run_other", domain.MsgReport)

	// Query messages.by_run for run_sub — should get 3, ordered by message_seq.
	resp := callRPC(t, svc, "messages.by_run", MessagesByRunParams{RunID: "run_sub"})
	if resp.Error != nil {
		t.Fatalf("messages.by_run error: %v", resp.Error)
	}
	var result MessagesByRunResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(result.Messages))
	}
	for i, m := range result.Messages {
		if m.MessageSeq != int64(i+1) {
			t.Fatalf("event %d: message_seq = %d, want %d", i, m.MessageSeq, i+1)
		}
		if m.RunID != "run_sub" {
			t.Fatalf("event %d: run_id = %q, want run_sub", i, m.RunID)
		}
	}
	if result.NextCursor == 0 {
		t.Fatal("expected non-zero next_cursor")
	}

	// Query run_other — should get 1 message.
	resp = callRPC(t, svc, "messages.by_run", MessagesByRunParams{RunID: "run_other"})
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result.Messages))
	}

	// Re-query run_sub to capture its NextCursor (3).
	resp = callRPC(t, svc, "messages.by_run", MessagesByRunParams{RunID: "run_sub"})
	json.Unmarshal(resp.Result, &result)
	subCursor := result.NextCursor

	// Query run_sub with cursor — should get 0 new messages.
	resp = callRPC(t, svc, "messages.by_run", MessagesByRunParams{
		RunID:  "run_sub",
		Cursor: subCursor,
	})
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 0 {
		t.Fatalf("expected 0 messages after cursor, got %d", len(result.Messages))
	}
}

func TestMessageHistory(t *testing.T) {
	svc, _ := newTestService(t)

	pub := func(msgID string) {
		resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
			MessageID:      msgID,
			RunID:          "run_hist",
			Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
			Recipient:      domain.MessageEndpoint{Role: domain.RoleWorker},
			Type:           domain.MsgState,
			IdempotencyKey: "idem_" + msgID,
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "hist_" + msgID},
		})
		if resp.Error != nil {
			t.Fatalf("publish %s: %v", msgID, resp.Error)
		}
	}
	pub("msg_hist_1")
	pub("msg_hist_2")

	resp := callRPC(t, svc, "messages.by_run", MessagesByRunParams{RunID: "run_hist"})
	if resp.Error != nil {
		t.Fatalf("messages.by_run error: %v", resp.Error)
	}
	var result MessagesByRunResult
	json.Unmarshal(resp.Result, &result)
	if len(result.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(result.Messages))
	}
}

func TestMessagePublishWithNotifier(t *testing.T) {
	svc, _ := newTestService(t)
	fn := &fakeNotifier{}
	svc.Notifier = fn

	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		MessageID:      "msg_notify_1",
		RunID:          "run_notify",
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
		Recipient:      domain.MessageEndpoint{AgentID: "agt_123", Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "idem_notify_1",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "wake up"},
	})
	if resp.Error != nil {
		t.Fatalf("message.publish.envelope error: %v", resp.Error)
	}
	if len(fn.calls) != 1 {
		t.Fatalf("expected 1 notifier call, got %d", len(fn.calls))
	}
	if fn.calls[0].agentID != "agt_123" {
		t.Fatalf("agentID = %q, want agt_123", fn.calls[0].agentID)
	}
	if fn.calls[0].msg != "wake up" {
		t.Fatalf("msg = %q, want 'wake up'", fn.calls[0].msg)
	}
}

func TestMessagePublishNoNotifier(t *testing.T) {
	svc, _ := newTestService(t)
	// Notifier is nil — should still succeed (message persisted).

	resp := callRPC(t, svc, "message.publish.envelope", MessagePublishEnvelopeParams{
		MessageID:      "msg_nonotify_1",
		RunID:          "run_nonotify",
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis},
		Recipient:      domain.MessageEndpoint{AgentID: "agt_123", Role: domain.RoleWorker},
		Type:           domain.MsgDirective,
		IdempotencyKey: "idem_nonotify_1",
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: "hello"},
	})
	if resp.Error != nil {
		t.Fatalf("message.publish.envelope error: %v", resp.Error)
	}
}

// --- G3-VERIFY: run.verify completion integrity tests ---

// setupRunForVerify creates a project + run, transitions it to running
// state, registers a verifier agent (RoleVerifier) for the run, and
// returns (runID, verifierAgentID, evidenceRef). The evidenceRef is a
// real event_id from the run's event journal (the agent.registered event).
// Used by the G3-VERIFY fixture tests.
func setupRunForVerify(t *testing.T, svc *Service) (runID, verifierID, evidenceRef string) {
	t.Helper()
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "verify-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "verify test task",
		RiskLevel: "R1", // low risk: auto-accept on PASS (existing verify tests)
	})

	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("setup: run state = %q, want running", run.State)
	}

	// Register a verifier agent for the run.
	resp = callRPC(t, svc, "agent.register", AgentRegisterParams{
		RunID:   rs.RunID,
		Role:    domain.RoleVerifier,
		Runtime: "devin",
		PID:     0,
	})
	if resp.Error != nil {
		t.Fatalf("setup: agent.register verifier: %v", resp.Error)
	}
	var ar AgentRegisterResult
	json.Unmarshal(resp.Result, &ar)

	// Find a real event_id on the run to use as evidence_ref.
	events, err := svc.Store.EventsSince(ctx, rs.RunID, 0, 100)
	if err != nil {
		t.Fatalf("setup: EventsSince: %v", err)
	}
	var evID string
	for i := range events {
		if events[i].EventType == "agent.registered" {
			evID = events[i].EventID
			break
		}
	}
	if evID == "" && len(events) > 0 {
		evID = events[0].EventID
	}
	if evID == "" {
		t.Fatal("setup: no events found for evidence_ref")
	}
	return rs.RunID, ar.AgentID, evID
}

// TestRunVerify_NoVerdictRejected verifies that calling run.verify without a
// verdict (or without verifier_agent_id / evidence_ref) is rejected with
// ErrInvalidInput and the run is NOT transitioned to completed (G3-VERIFY.1,
// G3-VERIFY.4 — no fake success / stub).
func TestRunVerify_NoVerdictRejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	// Missing verdict entirely.
	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID,
	})
	if resp.Error == nil {
		t.Fatal("expected error for missing verdict")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}

	// Missing verifier_agent_id.
	resp = callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID, Verdict: VerdictPass, EvidenceRef: evidenceRef,
	})
	if resp.Error == nil {
		t.Fatal("expected error for missing verifier_agent_id")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}

	// Missing evidence_ref.
	resp = callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID, VerifierAgentID: verifierID, Verdict: VerdictPass,
	})
	if resp.Error == nil {
		t.Fatal("expected error for missing evidence_ref")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}

	// Invalid verdict value.
	resp = callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID, VerifierAgentID: verifierID, Verdict: "MAYBE", EvidenceRef: evidenceRef,
	})
	if resp.Error == nil {
		t.Fatal("expected error for invalid verdict")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}

	// Verify the run is still running (not completed).
	run, _ := svc.Store.GetRun(ctx, runID)
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running (no transition on rejected verdict)", run.State)
	}
}

// TestRunVerify_PASS_TransitionsToCompleted verifies that a PASS verdict
// transitions the run to stopped (≈ completed) and persists the verify.passed
// event with the verdict, verifier, and evidence in the event journal
// (G3-VERIFY.2, G3-VERIFY.3).
func TestRunVerify_PASS_TransitionsToCompleted(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS error: %v", resp.Error)
	}
	var result RunVerifyResult
	json.Unmarshal(resp.Result, &result)
	if result.State != domain.RunV2Completed {
		t.Fatalf("state = %q, want completed", result.State)
	}
	if result.Verdict != VerdictPass {
		t.Fatalf("verdict = %q, want PASS", result.Verdict)
	}

	// Verify the run is in stopped state.
	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("run state = %q, want stopped", run.State)
	}

	// Verify the verify.passed event was persisted with the verdict payload.
	events, err := st.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var verifyEvent *domain.Event
	for i := range events {
		if events[i].EventType == "verify.passed" {
			verifyEvent = &events[i]
			break
		}
	}
	if verifyEvent == nil {
		t.Fatal("verify.passed event not found in journal")
	}
	var payload map[string]string
	json.Unmarshal(verifyEvent.Payload, &payload)
	if payload["verifier_agent_id"] != verifierID {
		t.Fatalf("verifier_agent_id = %q, want %q", payload["verifier_agent_id"], verifierID)
	}
	if payload["verdict"] != "PASS" {
		t.Fatalf("verdict = %q, want PASS", payload["verdict"])
	}
	if payload["evidence_ref"] != evidenceRef {
		t.Fatalf("evidence_ref = %q, want %q", payload["evidence_ref"], evidenceRef)
	}

	// Verify the evidence reference was appended to the run's evidence slice.
	if len(run.Evidence) == 0 || run.Evidence[len(run.Evidence)-1] != evidenceRef {
		t.Fatalf("evidence = %v, want last entry %q", run.Evidence, evidenceRef)
	}
}

// TestRunVerify_FAIL_TransitionsToFailed verifies that a FAIL verdict
// transitions the run to failed and persists the verify.failed event
// (G3-VERIFY.2, G3-VERIFY.3).
func TestRunVerify_FAIL_TransitionsToFailed(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictFail,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify FAIL error: %v", resp.Error)
	}
	var result RunVerifyResult
	json.Unmarshal(resp.Result, &result)
	if result.State != domain.RunV2Failed {
		t.Fatalf("state = %q, want failed", result.State)
	}
	if result.Verdict != VerdictFail {
		t.Fatalf("verdict = %q, want FAIL", result.Verdict)
	}

	// Verify the run is in failed state.
	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Failed {
		t.Fatalf("run state = %q, want failed", run.State)
	}

	// Verify the verify.failed event was persisted.
	events, err := st.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var verifyEvent *domain.Event
	for i := range events {
		if events[i].EventType == "verify.failed" {
			verifyEvent = &events[i]
			break
		}
	}
	if verifyEvent == nil {
		t.Fatal("verify.failed event not found in journal")
	}
	var payload map[string]string
	json.Unmarshal(verifyEvent.Payload, &payload)
	if payload["verdict"] != "FAIL" {
		t.Fatalf("verdict = %q, want FAIL", payload["verdict"])
	}
	if payload["evidence_ref"] != evidenceRef {
		t.Fatalf("evidence_ref = %q, want %q", payload["evidence_ref"], evidenceRef)
	}
}

// TestRunVerify_RunNotFound verifies that verifying a nonexistent run
// returns NOT_FOUND without any state transition.
func TestRunVerify_RunNotFound(t *testing.T) {
	svc, _ := newTestService(t)
	_, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           "run_nonexistent",
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error == nil {
		t.Fatal("expected error for nonexistent run")
	}
	if resp.Error.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", resp.Error.Code)
	}
}

// TestRunVerify_UnregisteredVerifierRejected verifies D1 fix: an
// unregistered verifier_agent_id must NOT be able to forge run completion.
// The verifier must be a registered agent with RoleVerifier belonging to the
// same run. An arbitrary string like "NOT_A_REAL_AGENT" must be rejected
// without transitioning state (G3-VERIFY.1 — authorized verifier required).
func TestRunVerify_UnregisteredVerifierRejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	runID, _, evidenceRef := setupRunForVerify(t, svc)

	// Unregistered verifier attempts to forge completion.
	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: "NOT_A_REAL_AGENT",
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error == nil {
		t.Fatal("D1 DEFECT: unregistered verifier forged completion — expected error")
	}

	// Run must still be running (not completed/failed).
	run, _ := svc.Store.GetRun(ctx, runID)
	if run.State != domain.RunV2Running {
		t.Fatalf("D1 DEFECT: run state = %q, want running (unregistered verifier must not transition)", run.State)
	}
}

// TestRunVerify_WrongRoleRejected verifies that a registered agent with a
// non-verifier role (e.g. worker) cannot verify a run (G3-VERIFY.1).
func TestRunVerify_WrongRoleRejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	runID, _, evidenceRef := setupRunForVerify(t, svc)

	// Register a worker agent for the run (simulating the worker that
	// executed the run). A worker must NOT be able to verify.
	resp := callRPC(t, svc, "agent.register", AgentRegisterParams{
		RunID:   runID,
		Role:    domain.RoleWorker,
		Runtime: "devin",
		PID:     99999,
	})
	if resp.Error != nil {
		t.Fatalf("agent.register failed: %v", resp.Error)
	}
	var ar AgentRegisterResult
	json.Unmarshal(resp.Result, &ar)

	// Worker agent attempts to verify — must be rejected (wrong role).
	resp = callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: ar.AgentID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error == nil {
		t.Fatal("expected error for worker-role verifier")
	}

	run, _ := svc.Store.GetRun(ctx, runID)
	if run.State != domain.RunV2Running {
		t.Fatalf("run state = %q, want running (wrong-role verifier must not transition)", run.State)
	}
}

// TestRunTypedPath_StateMachine verifies that the typed run.create +
// run.start path drives the full §8.1 state machine:
// requested → planning → ready → running, and that each intermediate
// state is persisted in the DB (acceptance-contract G1.2 — §8.1 is the
// authoritative path, not a decoration).
func TestRunTypedPath_StateMachine(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Register project.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "typed-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	// run.create → state must be "requested".
	resp = callRPC(t, svc, "run.create", RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "typed path test",
	})
	if resp.Error != nil {
		t.Fatalf("run.create: %v", resp.Error)
	}
	var rc RunCreateResult
	json.Unmarshal(resp.Result, &rc)

	run, _ := svc.Store.GetRun(ctx, rc.RunID)
	if run.State != domain.RunV2Requested {
		t.Fatalf("after create: state = %q, want requested", run.State)
	}

	// run.start → drives requested → planning → ready → running.
	resp = callRPC(t, svc, "run.start", RunStartParams{RunID: rc.RunID})
	if resp.Error != nil {
		t.Fatalf("run.start: %v", resp.Error)
	}
	var rsr RunStartResult
	json.Unmarshal(resp.Result, &rsr)
	if rsr.State != domain.RunV2Running {
		t.Fatalf("run.start result state = %q, want running", rsr.State)
	}

	// Final DB state must be "running".
	run, _ = svc.Store.GetRun(ctx, rc.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("after start: state = %q, want running", run.State)
	}

	// The event journal must contain the full transition sequence:
	// run.state_changed events for planning, ready, running.
	events, err := svc.Store.EventsSince(ctx, rc.RunID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var transitions []string
	for i := range events {
		if events[i].EventType == "run.state_changed" {
			var p map[string]string
			json.Unmarshal(events[i].Payload, &p)
			transitions = append(transitions, p["to"])
		}
	}
	want := []string{"planning", "ready", "running"}
	if len(transitions) < len(want) {
		t.Fatalf("transitions = %v, want at least %v", transitions, want)
	}
	// Check the last 3 transitions match (there may be other events interleaved).
	gotTail := transitions[len(transitions)-len(want):]
	for i, w := range want {
		if gotTail[i] != w {
			t.Fatalf("transition[%d] = %q, want %q (full: %v)", i, gotTail[i], w, transitions)
		}
	}
}

// TestRunVerify_PASS_PersistsV2Completed verifies that a PASS verdict
// transitions the run to the V2 "completed" state (not the legacy
// "stopped"), proving §8.1 is the authoritative path (G1.2).
func TestRunVerify_PASS_PersistsV2Completed(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS: %v", resp.Error)
	}
	var result RunVerifyResult
	json.Unmarshal(resp.Result, &result)
	if result.State != domain.RunV2Completed {
		t.Fatalf("result state = %q, want completed (V2)", result.State)
	}

	run, _ := svc.Store.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("DB state = %q, want completed (V2)", run.State)
	}

	// The event journal must contain the verifying → completed transitions.
	events, err := svc.Store.EventsSince(ctx, runID, 0, 100)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var sawVerifying, sawCompleted bool
	for i := range events {
		if events[i].EventType == "run.state_changed" {
			var p map[string]string
			json.Unmarshal(events[i].Payload, &p)
			if p["to"] == "verifying" {
				sawVerifying = true
			}
			if p["to"] == "completed" {
				sawCompleted = true
			}
		}
	}
	if !sawVerifying {
		t.Fatal("missing run.state_changed → verifying event (§8.1 requires running → verifying → completed)")
	}
	if !sawCompleted {
		t.Fatal("missing run.state_changed → completed event")
	}
}

// TestRunBlock_PersistsV2Blocked verifies that run.block transitions
// the run to the V2 "blocked" state and returns V2 state directly,
// proving §8.1 is the authoritative path (G1.2).
func TestRunBlock_PersistsV2Blocked(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "block-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "block test",
	})

	resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.block: %v", resp.Error)
	}
	// V2 typed contract returns "blocked" directly.
	var result RunBlockResult
	json.Unmarshal(resp.Result, &result)
	if result.State != domain.RunV2Blocked {
		t.Fatalf("v2 state = %q, want blocked", result.State)
	}

	// DB stores V2 "blocked" as the authoritative state.
	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Blocked {
		t.Fatalf("DB state = %q, want blocked (V2)", run.State)
	}
}

// TestRunUnblock_FromV2Blocked verifies that run.unblock transitions
// from V2 "blocked" back to V2 "running" (§8.1: blocked → running).
func TestRunUnblock_FromV2Blocked(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "resume-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "resume test",
	})

	// Block → blocked.
	resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.block: %v", resp.Error)
	}

	// Unblock → running.
	resp = callRPC(t, svc, "run.unblock", RunUnblockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.unblock: %v", resp.Error)
	}
	// V2 typed contract returns "running" directly.
	var result RunUnblockResult
	json.Unmarshal(resp.Result, &result)
	if result.State != domain.RunV2Running {
		t.Fatalf("v2 state = %q, want running", result.State)
	}

	// DB stores V2 "running" as the authoritative state.
	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("DB state = %q, want running (V2)", run.State)
	}
}

// TestDualTrack_LegacyFacadeVsTypedV2 is the acceptance-contract G3-BC
// dual-track black-box test. It verifies that:
//
//  1. The DB stores V2 states as the authoritative representation.
//  2. run.status (legacy facade) returns legacy state strings via the
//     facade translation (G3-BC.4).
//  3. V2 RPCs (run.block/unblock/terminate, run.list) return V2 state
//     strings directly.
//
// This proves §8.1 is the authoritative path (G1.2) while legacy
// observability is preserved for run.status (G3-BC.4).
func TestDualTrack_LegacyFacadeVsTypedV2(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Register project.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "dual-track", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	// run.create + run.start → DB state must be V2 "running".
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "dual-track test",
	})

	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("[DB] after submit: state = %q, want running (V2)", run.State)
	}

	// run.status (v2) → must return V2 "running".
	resp = callRPC(t, svc, "run.status", RunStatusParams{RunID: rs.RunID})
	var status RunStatusResult
	json.Unmarshal(resp.Result, &status)
	if status.Run.State != domain.RunV2Running {
		t.Fatalf("[v2 run.status] state = %q, want running (V2)", status.Run.State)
	}

	// run.block (v2) → DB must be V2 "blocked", result must be "blocked".
	resp = callRPC(t, svc, "run.block", RunBlockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.block: %v", resp.Error)
	}
	var blockRes RunBlockResult
	json.Unmarshal(resp.Result, &blockRes)
	if blockRes.State != domain.RunV2Blocked {
		t.Fatalf("[v2 run.block] state = %q, want blocked (V2)", blockRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Blocked {
		t.Fatalf("[DB] after block: state = %q, want blocked (V2)", run.State)
	}

	// run.list (v2) with "blocked" filter → must find the run,
	// and its state must be V2 "blocked".
	resp = callRPC(t, svc, "run.list", RunListParams{State: domain.RunV2Blocked})
	var listRes RunListResult
	json.Unmarshal(resp.Result, &listRes)
	if len(listRes.Runs) != 1 {
		t.Fatalf("[v2 run.list(blocked)] got %d runs, want 1", len(listRes.Runs))
	}
	if listRes.Runs[0].State != domain.RunV2Blocked {
		t.Fatalf("[v2 run.list(blocked)] state = %q, want blocked (V2)", listRes.Runs[0].State)
	}

	// run.unblock (v2) → DB must be V2 "running", result must be "running".
	resp = callRPC(t, svc, "run.unblock", RunUnblockParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.unblock: %v", resp.Error)
	}
	var unblockRes RunUnblockResult
	json.Unmarshal(resp.Result, &unblockRes)
	if unblockRes.State != domain.RunV2Running {
		t.Fatalf("[v2 run.unblock] state = %q, want running (V2)", unblockRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("[DB] after unblock: state = %q, want running (V2)", run.State)
	}

	// run.terminate (v2) → DB must be V2 "canceled", result must be "canceled".
	resp = callRPC(t, svc, "run.terminate", RunTerminateParams{RunID: rs.RunID})
	if resp.Error != nil {
		t.Fatalf("run.terminate: %v", resp.Error)
	}
	var termRes RunTerminateResult
	json.Unmarshal(resp.Result, &termRes)
	if termRes.State != domain.RunV2Canceled {
		t.Fatalf("[v2 run.terminate] state = %q, want canceled (V2)", termRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Canceled {
		t.Fatalf("[DB] after terminate: state = %q, want canceled (V2)", run.State)
	}
}

// TestDualTrack_TypedV2_VerifyAndBlock verifies that typed RPCs
// (run.verify, agent.block) return V2 state strings, not legacy
// translations (G1.2 — §8.1 is the authoritative path for typed contracts).
func TestDualTrack_TypedV2_VerifyAndBlock(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// --- run.verify (typed) returns V2 "completed" ---
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)
	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify: %v", resp.Error)
	}
	var verifyRes RunVerifyResult
	json.Unmarshal(resp.Result, &verifyRes)
	if verifyRes.State != domain.RunV2Completed {
		t.Fatalf("[typed run.verify] state = %q, want completed (V2)", verifyRes.State)
	}
	run, _ := svc.Store.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("[DB] after verify: state = %q, want completed (V2)", run.State)
	}

	// --- agent.block (typed) returns V2 "blocked" ---
	// Create a fresh run + agent for the block test.
	resp = callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "block-typed", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "block typed test",
	})

	resp = callRPC(t, svc, "agent.register", AgentRegisterParams{
		RunID:   rs.RunID,
		Role:    domain.RoleWorker,
		Runtime: "devin",
		PID:     99999,
	})
	if resp.Error != nil {
		t.Fatalf("agent.register: %v", resp.Error)
	}
	var ar AgentRegisterResult
	json.Unmarshal(resp.Result, &ar)

	resp = callRPC(t, svc, "agent.block", AgentBlockParams{AgentID: ar.AgentID})
	if resp.Error != nil {
		t.Fatalf("agent.block: %v", resp.Error)
	}
	var blockRes AgentBlockResult
	json.Unmarshal(resp.Result, &blockRes)
	if blockRes.State != domain.RunV2Blocked {
		t.Fatalf("[typed agent.block] state = %q, want blocked (V2)", blockRes.State)
	}
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Blocked {
		t.Fatalf("[DB] after block: state = %q, want blocked (V2)", run.State)
	}
}

// TestRunProjectIDAndOwner_Persisted verifies that run.create and
// run.start populate the ProjectID and Owner fields on the run from
// the request params, and that these fields are persisted to the DB
// and surfaced in both typed and legacy facade responses.
func TestRunProjectIDAndOwner_Persisted(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	// Register project.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "proj-owner-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	// run.create with explicit owner.
	resp = callRPC(t, svc, "run.create", RunCreateParams{
		ProjectID: pr.ProjectID,
		Objective: "project/owner test",
		Owner:     "alice",
	})
	if resp.Error != nil {
		t.Fatalf("run.create: %v", resp.Error)
	}
	var rc RunCreateResult
	json.Unmarshal(resp.Result, &rc)

	// DB must have ProjectID and Owner populated.
	run, _ := svc.Store.GetRun(ctx, rc.RunID)
	if run.ProjectID != pr.ProjectID {
		t.Fatalf("[DB] project_id = %q, want %q", run.ProjectID, pr.ProjectID)
	}
	if run.Owner != "alice" {
		t.Fatalf("[DB] owner = %q, want alice", run.Owner)
	}

	// run.status (v2) must surface project_id and owner.
	resp = callRPC(t, svc, "run.status", RunStatusParams{RunID: rc.RunID})
	var status RunStatusResult
	json.Unmarshal(resp.Result, &status)
	if status.Run.ProjectID != pr.ProjectID {
		t.Fatalf("[v2 run.status] project_id = %q, want %q", status.Run.ProjectID, pr.ProjectID)
	}
	if status.Run.Owner != "alice" {
		t.Fatalf("[v2 run.status] owner = %q, want alice", status.Run.Owner)
	}

	// run.create + run.start with explicit owner.
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID,
		Objective: "submit owner test",
		Owner:     "bob",
	})

	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.ProjectID != pr.ProjectID {
		t.Fatalf("[DB] submit project_id = %q, want %q", run.ProjectID, pr.ProjectID)
	}
	if run.Owner != "bob" {
		t.Fatalf("[DB] submit owner = %q, want bob", run.Owner)
	}

	// run.create + run.start without owner → default "local-user".
	rs = createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID,
		Objective: "default owner test",
	})
	run, _ = svc.Store.GetRun(ctx, rs.RunID)
	if run.Owner != "local-user" {
		t.Fatalf("[DB] default owner = %q, want local-user", run.Owner)
	}
}

// --- Terminal-state consistency slice (ADR-0018) ---

// setupTwoRunsForSupersede creates two runs (via run.create + run.start) and
// returns their IDs for supersede testing.
func setupTwoRunsForSupersede(t *testing.T, svc *Service) (oldRunID, successorRunID string) {
	t.Helper()
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "argus", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs1 := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "old run",
	})

	rs2 := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "successor run",
	})
	return rs1.RunID, rs2.RunID
}

func TestRunSupersede_Success(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	oldRunID, successorRunID := setupTwoRunsForSupersede(t, svc)

	resp := callRPC(t, svc, "run.supersede", RunSupersedeParams{
		OldRunID:       oldRunID,
		SuccessorRunID: successorRunID,
		Reason:         "P2-B superseded by P2-UX",
	})
	if resp.Error != nil {
		t.Fatalf("run.supersede error: %v", resp.Error)
	}
	var r RunSupersedeResult
	json.Unmarshal(resp.Result, &r)
	if r.SupersedeID == "" {
		t.Fatal("empty supersede_id")
	}
	if r.OldRunID != oldRunID {
		t.Fatalf("old_run_id = %q, want %q", r.OldRunID, oldRunID)
	}
	if r.SuccessorRunID != successorRunID {
		t.Fatalf("successor_run_id = %q, want %q", r.SuccessorRunID, successorRunID)
	}
	// The supersede record should be in the store.
	rec, _ := st.GetSupersede(ctx, oldRunID)
	if rec == nil {
		t.Fatal("supersede record not found in store")
	}
}

func TestRunSupersede_DuplicateRejected(t *testing.T) {
	svc, _ := newTestService(t)
	oldRunID, successorRunID := setupTwoRunsForSupersede(t, svc)

	resp := callRPC(t, svc, "run.supersede", RunSupersedeParams{
		OldRunID: oldRunID, SuccessorRunID: successorRunID, Reason: "first",
	})
	if resp.Error != nil {
		t.Fatalf("run.supersede 1 error: %v", resp.Error)
	}
	resp = callRPC(t, svc, "run.supersede", RunSupersedeParams{
		OldRunID: oldRunID, SuccessorRunID: successorRunID, Reason: "second",
	})
	if resp.Error == nil {
		t.Fatal("duplicate supersede should fail")
	}
	if resp.Error.Code != domain.CodeConflict {
		t.Fatalf("code = %q, want CONFLICT", resp.Error.Code)
	}
}

func TestRunSupersede_SameRunRejected(t *testing.T) {
	svc, _ := newTestService(t)
	oldRunID, _ := setupTwoRunsForSupersede(t, svc)

	resp := callRPC(t, svc, "run.supersede", RunSupersedeParams{
		OldRunID: oldRunID, SuccessorRunID: oldRunID, Reason: "self",
	})
	if resp.Error == nil {
		t.Fatal("self-supersede should fail")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}
}

func TestRunVerify_NextActionParam(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
		NextAction:      domain.NextActionContinuation,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify error: %v", resp.Error)
	}
	var r RunVerifyResult
	json.Unmarshal(resp.Result, &r)
	if r.NextAction != domain.NextActionContinuation {
		t.Fatalf("next_action = %q, want continuation", r.NextAction)
	}
	if r.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted", r.ResultState)
	}
	run, _ := st.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionContinuation {
		t.Fatalf("DB next_action = %q, want continuation", run.NextAction)
	}
	if run.ResultState != domain.ResultAccepted {
		t.Fatalf("DB result_state = %q, want accepted", run.ResultState)
	}
}

func TestRunVerify_DefaultNextAction(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify error: %v", resp.Error)
	}
	var r RunVerifyResult
	json.Unmarshal(resp.Result, &r)
	if r.NextAction != domain.NextActionNone {
		t.Fatalf("default next_action = %q, want none (PASS default)", r.NextAction)
	}
	run, _ := st.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("DB next_action = %q, want none", run.NextAction)
	}
}

func TestRunVerify_TerminalizesAgents(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify error: %v", resp.Error)
	}
	// The verifier agent should be terminalized (exited).
	events, err := st.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var termCount int
	for i := range events {
		if events[i].EventType == "agent.terminalized" {
			termCount++
		}
	}
	if termCount == 0 {
		t.Fatal("C2: no agent.terminalized events after run.verify")
	}
}

func TestRunSetNextAction(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)
	// Verify first (sets default next_action=none).
	callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID, VerifierAgentID: verifierID, Verdict: VerdictPass, EvidenceRef: evidenceRef,
	})

	// Set next_action to continuation.
	resp := callRPC(t, svc, "run.set_next_action", RunSetNextActionParams{
		RunID: runID, NextAction: domain.NextActionContinuation,
	})
	if resp.Error != nil {
		t.Fatalf("run.set_next_action error: %v", resp.Error)
	}
	var r RunSetNextActionResult
	json.Unmarshal(resp.Result, &r)
	if r.NextAction != domain.NextActionContinuation {
		t.Fatalf("next_action = %q, want continuation", r.NextAction)
	}
	run, _ := st.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionContinuation {
		t.Fatalf("DB next_action = %q, want continuation", run.NextAction)
	}

	// Idempotent: set again to none.
	resp = callRPC(t, svc, "run.set_next_action", RunSetNextActionParams{
		RunID: runID, NextAction: domain.NextActionNone,
	})
	if resp.Error != nil {
		t.Fatalf("run.set_next_action 2 error: %v", resp.Error)
	}
	run, _ = st.GetRun(ctx, runID)
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("DB next_action = %q, want none (updated)", run.NextAction)
	}
}

func TestRunSetNextAction_InvalidRejected(t *testing.T) {
	svc, _ := newTestService(t)
	runID, _, _ := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.set_next_action", RunSetNextActionParams{
		RunID: runID, NextAction: "bogus",
	})
	if resp.Error == nil {
		t.Fatal("invalid next_action should be rejected")
	}
	if resp.Error.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", resp.Error.Code)
	}
}

func TestReconcileTerminalState_SurfacesMissingNextAction(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerify(t, svc)
	// Verify with PASS — sets next_action=none, so it should NOT be surfaced.
	callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID: runID, VerifierAgentID: verifierID, Verdict: VerdictPass, EvidenceRef: evidenceRef,
	})

	// Create a second run and mark it completed WITHOUT next_action by
	// directly using the store (simulating a legacy/migrated run).
	legacyRunID := "run_legacy_no_next_action"
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "legacy", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "legacy",
	})
	// Force it to completed with empty next_action via store.
	legacyRunID = rs.RunID
	for _, st2 := range []domain.RunStateV2{
		domain.RunV2Planning, domain.RunV2Ready, domain.RunV2Running,
		domain.RunV2Verifying, domain.RunV2Completed,
	} {
		eid, _ := domain.NewID("evt_")
		if err := st.UpdateRunState(ctx, legacyRunID, st2, eid); err != nil {
			t.Fatalf("-> %s: %v", st2, err)
		}
	}
	// Clear next_action (UpdateRunState doesn't touch it, so it's already "").

	resp = callRPC(t, svc, "reconcile.terminal_state", ReconcileTerminalStateParams{})
	if resp.Error != nil {
		t.Fatalf("reconcile.terminal_state error: %v", resp.Error)
	}
	var r ReconcileTerminalStateResult
	json.Unmarshal(resp.Result, &r)
	// The legacy run (empty next_action) should be surfaced.
	var found bool
	for _, m := range r.MissingNextAction {
		if m.RunID == legacyRunID {
			found = true
		}
	}
	if !found {
		t.Fatalf("C4: legacy run %s with empty next_action not surfaced; got %+v", legacyRunID, r.MissingNextAction)
	}
	// The verified run (next_action=none) should NOT be surfaced.
	for _, m := range r.MissingNextAction {
		if m.RunID == runID {
			t.Fatalf("C4: verified run %s with next_action=none should not be surfaced", runID)
		}
	}
}

func TestReconcileTerminalState_SurfacesSuperseded(t *testing.T) {
	svc, _ := newTestService(t)
	oldRunID, successorRunID := setupTwoRunsForSupersede(t, svc)

	callRPC(t, svc, "run.supersede", RunSupersedeParams{
		OldRunID: oldRunID, SuccessorRunID: successorRunID, Reason: "test",
	})

	resp := callRPC(t, svc, "reconcile.terminal_state", ReconcileTerminalStateParams{})
	if resp.Error != nil {
		t.Fatalf("reconcile.terminal_state error: %v", resp.Error)
	}
	var r ReconcileTerminalStateResult
	json.Unmarshal(resp.Result, &r)
	var found bool
	for _, s := range r.Superseded {
		if s.OldRunID == oldRunID {
			found = true
			if s.SuccessorRunID != successorRunID {
				t.Fatalf("successor_run_id = %q, want %q", s.SuccessorRunID, successorRunID)
			}
		}
	}
	if !found {
		t.Fatalf("C3: superseded run %s not surfaced; got %+v", oldRunID, r.Superseded)
	}
}

func TestRunTerminate_TerminalizesAgents(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, _, _ := setupRunForVerify(t, svc)

	resp := callRPC(t, svc, "run.terminate", RunTerminateParams{RunID: runID})
	if resp.Error != nil {
		t.Fatalf("run.terminate error: %v", resp.Error)
	}
	// The agents of the canceled run should be terminalized.
	events, err := st.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var termCount int
	for i := range events {
		if events[i].EventType == "agent.terminalized" {
			termCount++
		}
	}
	if termCount == 0 {
		t.Fatal("C2: no agent.terminalized events after run.terminate")
	}
}

// --- Risk-graded verification tests ---

// setupRunForVerifyWithRisk is like setupRunForVerify but lets the caller
// specify the risk level. It creates a project, run (driven to running),
// registers a verifier agent, and returns (runID, verifierAgentID,
// evidenceRef).
func setupRunForVerifyWithRisk(t *testing.T, svc *Service, risk string) (runID, verifierID, evidenceRef string) {
	t.Helper()
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "risk-verify-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "risk verify test",
		RiskLevel: risk,
	})

	run, _ := svc.Store.GetRun(ctx, rs.RunID)
	if run.State != domain.RunV2Running {
		t.Fatalf("setup: run state = %q, want running", run.State)
	}

	// Register a verifier agent for the run.
	resp = callRPC(t, svc, "agent.register", AgentRegisterParams{
		RunID:   rs.RunID,
		Role:    domain.RoleVerifier,
		Runtime: "devin",
		PID:     0,
	})
	if resp.Error != nil {
		t.Fatalf("setup: agent.register verifier: %v", resp.Error)
	}
	var ar AgentRegisterResult
	json.Unmarshal(resp.Result, &ar)

	events, err := svc.Store.EventsSince(ctx, rs.RunID, 0, 100)
	if err != nil {
		t.Fatalf("setup: EventsSince: %v", err)
	}
	var evID string
	for i := range events {
		if events[i].EventType == "agent.registered" {
			evID = events[i].EventID
			break
		}
	}
	if evID == "" && len(events) > 0 {
		evID = events[0].EventID
	}
	if evID == "" {
		t.Fatal("setup: no events found for evidence_ref")
	}
	return rs.RunID, ar.AgentID, evID
}

// TestRunCreateWithRiskLevel verifies that creating a run with a risk level
// stores it on the task and that run.status surfaces it.
func TestRunCreateWithRiskLevel(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "risk-create-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	resp = callRPC(t, svc, "run.create", RunCreateParams{
		ProjectID:          pr.ProjectID,
		Objective:          "risk level test",
		RiskLevel:          "R0",
		AcceptanceCriteria: []string{"docs updated"},
		Constraints:        []string{"no code changes"},
		Deliverables:       []string{"README.md"},
	})
	if resp.Error != nil {
		t.Fatalf("run.create: %v", resp.Error)
	}
	var rc RunCreateResult
	json.Unmarshal(resp.Result, &rc)

	task, err := svc.Store.GetTaskByRun(ctx, rc.RunID)
	if err != nil {
		t.Fatalf("GetTaskByRun: %v", err)
	}
	if task == nil {
		t.Fatal("task not found")
	}
	if task.RiskLevel != domain.RiskR0 {
		t.Fatalf("risk_level = %q, want R0", task.RiskLevel)
	}
	if len(task.AcceptanceCriteria) != 1 || task.AcceptanceCriteria[0] != "docs updated" {
		t.Fatalf("acceptance_criteria = %v, want [docs updated]", task.AcceptanceCriteria)
	}
	if len(task.Constraints) != 1 || task.Constraints[0] != "no code changes" {
		t.Fatalf("constraints = %v, want [no code changes]", task.Constraints)
	}
	if len(task.Deliverables) != 1 || task.Deliverables[0] != "README.md" {
		t.Fatalf("deliverables = %v, want [README.md]", task.Deliverables)
	}
}

// TestRiskLevelDefaults verifies that creating a run without a risk level
// defaults the task's risk level to R2 (medium).
func TestRiskLevelDefaults(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "risk-default-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	resp = callRPC(t, svc, "run.create", RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "default risk test",
		// RiskLevel intentionally omitted.
	})
	if resp.Error != nil {
		t.Fatalf("run.create: %v", resp.Error)
	}
	var rc RunCreateResult
	json.Unmarshal(resp.Result, &rc)

	task, err := svc.Store.GetTaskByRun(ctx, rc.RunID)
	if err != nil {
		t.Fatalf("GetTaskByRun: %v", err)
	}
	if task == nil {
		t.Fatal("task not found")
	}
	if task.RiskLevel != domain.RiskR2 {
		t.Fatalf("default risk_level = %q, want R2", task.RiskLevel)
	}

	// An invalid risk level must also default to R2.
	resp = callRPC(t, svc, "run.create", RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "invalid risk test",
		RiskLevel: "R9", // invalid
	})
	if resp.Error != nil {
		t.Fatalf("run.create (invalid risk): %v", resp.Error)
	}
	var rc2 RunCreateResult
	json.Unmarshal(resp.Result, &rc2)
	task2, _ := svc.Store.GetTaskByRun(ctx, rc2.RunID)
	if task2 == nil {
		t.Fatal("task2 not found")
	}
	if task2.RiskLevel != domain.RiskR2 {
		t.Fatalf("invalid risk_level = %q, want R2 (default)", task2.RiskLevel)
	}
}

// TestRiskGradedAutoAcceptR0 verifies that an R0 run auto-accepts on a
// verify PASS verdict (transitions to completed, result_state=accepted).
func TestRiskGradedAutoAcceptR0(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerifyWithRisk(t, svc, "R0")

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS: %v", resp.Error)
	}
	var r RunVerifyResult
	json.Unmarshal(resp.Result, &r)
	if r.State != domain.RunV2Completed {
		t.Fatalf("state = %q, want completed (R0 auto-accept)", r.State)
	}
	if r.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted", r.ResultState)
	}
	if r.NextAction != domain.NextActionNone {
		t.Fatalf("next_action = %q, want none", r.NextAction)
	}

	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("DB state = %q, want completed", run.State)
	}
}

// TestRiskGradedApprovalRequiredR2 verifies that an R2 run does NOT
// auto-accept on a verify PASS verdict. Instead it stays in verifying with
// next_action=approval_required, and a PM approval message is published.
func TestRiskGradedApprovalRequiredR2(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerifyWithRisk(t, svc, "R2")

	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS: %v", resp.Error)
	}
	var r RunVerifyResult
	json.Unmarshal(resp.Result, &r)
	if r.State != domain.RunV2Verifying {
		t.Fatalf("state = %q, want verifying (R2 requires approval)", r.State)
	}
	if r.NextAction != domain.NextActionApprovalRequired {
		t.Fatalf("next_action = %q, want approval_required", r.NextAction)
	}
	if r.ResultState != domain.ResultAccepted {
		t.Fatalf("result_state = %q, want accepted (verify passed)", r.ResultState)
	}

	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Verifying {
		t.Fatalf("DB state = %q, want verifying", run.State)
	}
	if run.NextAction != domain.NextActionApprovalRequired {
		t.Fatalf("DB next_action = %q, want approval_required", run.NextAction)
	}

	// A PM approval message must have been published (message envelope).
	// The message bus appends events with event_type="message"; the
	// payload is the marshaled envelope. We check for a "message" event
	// whose payload contains the approval-request inline text.
	events, err := st.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var foundPMMessage bool
	for i := range events {
		if events[i].EventType == "message" {
			var msg domain.Message
			if json.Unmarshal(events[i].Payload, &msg) == nil {
				if msg.Recipient.Role == domain.RolePM && strings.Contains(msg.PayloadRef.Inline, "requires human approval") {
					foundPMMessage = true
				}
			}
		}
	}
	if !foundPMMessage {
		t.Fatal("no PM approval-request message published after R2 verify PASS")
	}
}

// TestRunApprove verifies that run.approve transitions a verifying
// (approval-required) run to completed with result_state=approved.
func TestRunApprove(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerifyWithRisk(t, svc, "R2")

	// Drive to verifying with approval_required via run.verify PASS.
	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS (R2): %v", resp.Error)
	}
	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Verifying {
		t.Fatalf("pre-approve state = %q, want verifying", run.State)
	}

	// Now approve.
	resp = callRPC(t, svc, "run.approve", RunApproveParams{
		RunID:       runID,
		Approver:    "human-pm",
		EvidenceRef: evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.approve: %v", resp.Error)
	}
	var ar RunApproveResult
	json.Unmarshal(resp.Result, &ar)
	if ar.State != domain.RunV2Completed {
		t.Fatalf("state = %q, want completed", ar.State)
	}
	if ar.ResultState != domain.ResultApproved {
		t.Fatalf("result_state = %q, want approved", ar.ResultState)
	}

	run, _ = st.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("DB state = %q, want completed", run.State)
	}
	if run.ResultState != domain.ResultApproved {
		t.Fatalf("DB result_state = %q, want approved", run.ResultState)
	}
	if run.NextAction != domain.NextActionNone {
		t.Fatalf("DB next_action = %q, want none", run.NextAction)
	}

	// A run.approved event must be in the journal.
	events, err := st.EventsSince(ctx, runID, 0, 200)
	if err != nil {
		t.Fatalf("EventsSince: %v", err)
	}
	var foundApproved bool
	for i := range events {
		if events[i].EventType == "run.approved" {
			foundApproved = true
		}
	}
	if !foundApproved {
		t.Fatal("no run.approved event in journal")
	}
}

// TestRunApproveWrongState verifies that run.approve rejects a run that is
// not in the verifying state.
func TestRunApproveWrongState(t *testing.T) {
	svc, _ := newTestService(t)

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "approve-wrong-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)
	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "wrong state", RiskLevel: "R2",
	})
	// Run is in running state (not verifying).
	resp = callRPC(t, svc, "run.approve", RunApproveParams{
		RunID: rs.RunID, Approver: "human-pm",
	})
	if resp.Error == nil {
		t.Fatal("run.approve on a non-verifying run should fail")
	}
	if resp.Error.Code != domain.CodeConflict {
		t.Fatalf("code = %q, want CONFLICT", resp.Error.Code)
	}
}

// TestRunVerifyApprovedFlag verifies that calling run.verify with PASS and
// Approved=true on a verifying run transitions it to completed with
// result_state=approved (the alternative approval path).
func TestRunVerifyApprovedFlag(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()
	runID, verifierID, evidenceRef := setupRunForVerifyWithRisk(t, svc, "R2")

	// Drive to verifying with approval_required.
	resp := callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:           runID,
		VerifierAgentID: verifierID,
		Verdict:         VerdictPass,
		EvidenceRef:     evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify PASS (R2): %v", resp.Error)
	}
	run, _ := st.GetRun(ctx, runID)
	if run.State != domain.RunV2Verifying {
		t.Fatalf("pre-approve state = %q, want verifying", run.State)
	}

	// Approve via run.verify with Approved=true (no verifier agent needed).
	resp = callRPC(t, svc, "run.verify", RunVerifyParams{
		RunID:       runID,
		Verdict:     VerdictPass,
		Approved:    true,
		EvidenceRef: evidenceRef,
	})
	if resp.Error != nil {
		t.Fatalf("run.verify Approved: %v", resp.Error)
	}
	var r RunVerifyResult
	json.Unmarshal(resp.Result, &r)
	if r.State != domain.RunV2Completed {
		t.Fatalf("state = %q, want completed", r.State)
	}
	if r.ResultState != domain.ResultApproved {
		t.Fatalf("result_state = %q, want approved", r.ResultState)
	}

	run, _ = st.GetRun(ctx, runID)
	if run.State != domain.RunV2Completed {
		t.Fatalf("DB state = %q, want completed", run.State)
	}
}

// registerExitedAgent is a test helper that registers a worker agent for the
// given run and immediately marks it as exited with the supplied exit code.
// This simulates the scanner detecting an exited agent.
func registerExitedAgent(t *testing.T, svc *Service, runID string, exitCode int) {
	t.Helper()
	ctx := context.Background()
	aid, err := domain.NewID("agt_")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	eid, err := domain.NewID("evt_")
	if err != nil {
		t.Fatalf("event id: %v", err)
	}
	task, _ := svc.Store.GetTaskByRun(ctx, runID)
	taskID := ""
	if task != nil {
		taskID = task.TaskID
	}
	if err := svc.Store.RegisterAgent(ctx, &domain.Agent{
		AgentID:   aid,
		RunID:     runID,
		TaskID:    taskID,
		Role:      domain.RoleWorker,
		Runtime:   "devin",
		PID:       0,
		State:     domain.AgentRunning,
		StartedAt: time.Now().UTC(),
	}, eid); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	eid2, err := domain.NewID("evt_")
	if err != nil {
		t.Fatalf("event id 2: %v", err)
	}
	if err := svc.Store.UpdateAgentState(ctx, aid, domain.AgentExited, &exitCode, eid2); err != nil {
		t.Fatalf("UpdateAgentState: %v", err)
	}
}

// TestCircuitBreakerTripsAfter3SameRootCause verifies that AutoContinue
// blocks the run (instead of creating another successor) when the same root
// cause has occurred 3 times across the run chain. The first two
// auto-continuations succeed; the third trips the breaker.
func TestCircuitBreakerTripsAfter3SameRootCause(t *testing.T) {
	svc, st := newTestService(t)
	ctx := context.Background()

	// Register a project and create the first run.
	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "cb-test", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "circuit breaker test",
	})
	runID := rs.RunID

	// All agents exit with code 1 → root cause "exit_code_1" each time.
	// 1st auto-continue: 0 existing + 1 = 1 < 3 → succeeds.
	registerExitedAgent(t, svc, runID, 1)
	newRun1, err := svc.AutoContinue(ctx, runID)
	if err != nil {
		t.Fatalf("1st AutoContinue should succeed: %v", err)
	}

	// 2nd auto-continue: 1 existing + 1 = 2 < 3 → succeeds.
	registerExitedAgent(t, svc, newRun1, 1)
	newRun2, err := svc.AutoContinue(ctx, newRun1)
	if err != nil {
		t.Fatalf("2nd AutoContinue should succeed: %v", err)
	}

	// 3rd auto-continue: 2 existing + 1 = 3 >= 3 → breaker trips.
	registerExitedAgent(t, svc, newRun2, 1)
	_, err = svc.AutoContinue(ctx, newRun2)
	if err == nil {
		t.Fatal("3rd AutoContinue should trip the circuit breaker and return an error")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("3rd AutoContinue error code = %q, want CONFLICT", de.Code)
	}

	// The run that triggered the breaker should be transitioned to blocked.
	run, _ := st.GetRun(ctx, newRun2)
	if run == nil {
		t.Fatalf("run %s not found after breaker", newRun2)
	}
	if run.State != domain.RunV2Blocked {
		t.Fatalf("run state after breaker = %q, want blocked", run.State)
	}

	// A block message should have been published to the PM message queue.
	msgs, err := st.MessagesByRun(ctx, newRun2, 0, 50)
	if err != nil {
		t.Fatalf("MessagesByRun: %v", err)
	}
	var foundBlock bool
	for i := range msgs {
		if msgs[i].EventType == "message" {
			var m domain.Message
			if err := json.Unmarshal(msgs[i].Payload, &m); err == nil {
				if m.Type == domain.MsgBlock && m.Recipient.Role == domain.RolePM {
					foundBlock = true
				}
			}
		}
	}
	if !foundBlock {
		t.Fatal("no block message published to PM after circuit breaker trip")
	}
}

// TestCircuitBreaker_DifferentRootCausesDoNotTrip verifies that the breaker
// only counts the *same* root cause — three continuations with three
// different root causes must NOT trip the breaker.
func TestCircuitBreaker_DifferentRootCausesDoNotTrip(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	resp := callRPC(t, svc, "project.register", ProjectRegisterParams{
		Name: "cb-diff", RepoPath: "/x", BaseRef: "main",
	})
	var pr ProjectRegisterResult
	json.Unmarshal(resp.Result, &pr)

	rs := createAndStartRun(t, svc, RunCreateParams{
		ProjectID: pr.ProjectID, Objective: "different root causes",
	})
	runID := rs.RunID

	// Three different exit codes → three different root causes. Each count
	// is 0 existing + 1 = 1 < 3, so none should trip.
	registerExitedAgent(t, svc, runID, 1)
	r1, err := svc.AutoContinue(ctx, runID)
	if err != nil {
		t.Fatalf("AutoContinue (exit_code_1) should succeed: %v", err)
	}
	registerExitedAgent(t, svc, r1, 2)
	r2, err := svc.AutoContinue(ctx, r1)
	if err != nil {
		t.Fatalf("AutoContinue (exit_code_2) should succeed: %v", err)
	}
	registerExitedAgent(t, svc, r2, 0)
	_, err = svc.AutoContinue(ctx, r2)
	if err != nil {
		t.Fatalf("AutoContinue (exit_code_0_incomplete) should succeed, breaker must not trip on different root causes: %v", err)
	}
}
