package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSemanticCLI_EndToEnd exercises the full semantic CLI surface against a
// real pantheond running in Unix socket mode with a temp SQLite store (no SSH,
// no real devin runtime). It covers acceptance-contract G3.1:
//   - project register → list → status
//   - run create → start → status → message → stop → resume → verify
//   - agent register → heartbeat → complete → block
func TestSemanticCLI_EndToEnd(t *testing.T) {
	// Build both binaries.
	pantheondBin := buildPantheondForTest(t)
	pantheonBin := buildPantheonCLI(t)

	dir := t.TempDir()
	socketDir := shortSocketDir(t)
	socketPath := filepath.Join(socketDir, "pantheond.sock")
	dbPath := filepath.Join(dir, "test.db")
	wtDir := filepath.Join(dir, "wt")

	// Create a real git repo so the workspace manager can resolve base commits.
	repoPath := filepath.Join(dir, "test-repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("mkdir repo: %v", err)
	}
	if err := runGit(repoPath, "init"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("# test\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	if err := runGit(repoPath, "add", "-A"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(repoPath, "commit", "-m", "initial"); err != nil {
		t.Fatalf("git commit: %v", err)
	}
	// Determine the default branch name (git 2.28+ may use "main", older "master").
	branchOut, err := exec.Command("git", "-C", repoPath, "symbolic-ref", "--short", "HEAD").Output()
	if err != nil {
		t.Fatalf("git symbolic-ref: %v", err)
	}
	baseBranch := strings.TrimSpace(string(branchOut))

	// Start pantheond in socket mode with scanner disabled (no real runtime).
	daemon := exec.Command(pantheondBin, "-db", dbPath, "-worktrees", wtDir, "-socket", socketPath, "-no-scanner")
	daemon.Stderr = &strings.Builder{}
	if err := daemon.Start(); err != nil {
		t.Fatalf("start pantheond: %v", err)
	}
	defer func() {
		_ = daemon.Process.Signal(os.Interrupt)
		_, _ = daemon.Process.Wait()
	}()

	// Wait for socket to appear.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket not created: %v", err)
	}

	// Helper: run a semantic CLI command and return the parsed JSON-RPC response.
	cliRun := func(args ...string) map[string]any {
		t.Helper()
		fullArgs := append([]string{"-socket", socketPath}, args...)
		cmd := exec.Command(pantheonBin, fullArgs...)
		stdout := &strings.Builder{}
		stderr := &strings.Builder{}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("CLI %v: %v\nstderr: %s", args, err, stderr.String())
		}
		out := strings.TrimSpace(stdout.String())
		if out == "" {
			t.Fatalf("CLI %v: empty output\nstderr: %s", args, stderr.String())
		}
		var resp map[string]any
		if err := json.Unmarshal([]byte(out), &resp); err != nil {
			t.Fatalf("CLI %v: unmarshal: %v\noutput: %s", args, err, out)
		}
		if resp["error"] != nil {
			t.Fatalf("CLI %v: error: %v", args, resp["error"])
		}
		return resp
	}

	// --- project register → list → status ---

	resp := cliRun("project", "register", "--name", "test-proj", "--repo-path", repoPath, "--base-ref", baseBranch)
	result := resp["result"].(map[string]any)
	projectID := result["project_id"].(string)
	if !strings.HasPrefix(projectID, "prj_") {
		t.Fatalf("project_id = %q, want prj_*", projectID)
	}

	resp = cliRun("project", "list")
	result = resp["result"].(map[string]any)
	projects := result["projects"].([]any)
	if len(projects) < 1 {
		t.Fatalf("expected at least 1 project, got %d", len(projects))
	}

	resp = cliRun("project", "status", "--project-id", projectID)
	result = resp["result"].(map[string]any)
	proj := result["project"].(map[string]any)
	if proj["name"] != "test-proj" {
		t.Fatalf("project name = %q, want test-proj", proj["name"])
	}

	// --- run create → start → status ---

	resp = cliRun("run", "create", "--project-id", projectID, "--objective", "end-to-end test task", "--risk-level", "R1")
	result = resp["result"].(map[string]any)
	runID := result["run_id"].(string)
	if !strings.HasPrefix(runID, "run_") {
		t.Fatalf("run_id = %q, want run_*", runID)
	}
	workspaceID := result["workspace_id"].(string)
	if workspaceID == "" {
		t.Fatal("empty workspace_id")
	}
	taskID := result["task_id"].(string)
	if taskID == "" {
		t.Fatal("empty task_id")
	}

	resp = cliRun("run", "start", "--run-id", runID)
	result = resp["result"].(map[string]any)
	if result["state"] != "running" {
		t.Fatalf("run start state = %v, want running", result["state"])
	}

	resp = cliRun("run", "status", "--run-id", runID)
	result = resp["result"].(map[string]any)
	run := result["run"].(map[string]any)
	if run["state"] != "running" {
		t.Fatalf("run status state = %v, want running", run["state"])
	}

	// --- run message ---

	resp = cliRun("run", "message", "--run-id", runID, "--body", "hello from semantic CLI")
	result = resp["result"].(map[string]any)
	if result["seq"] == nil {
		t.Fatal("expected non-nil seq from message publish")
	}

	// --- run stop → resume ---

	resp = cliRun("run", "stop", "--run-id", runID)
	result = resp["result"].(map[string]any)
	if result["state"] != "blocked" {
		t.Fatalf("run stop state = %v, want blocked (V2)", result["state"])
	}

	resp = cliRun("run", "resume", "--run-id", runID)
	result = resp["result"].(map[string]any)
	if result["state"] != "running" {
		t.Fatalf("run resume state = %v, want running", result["state"])
	}

	// --- run verify (PASS verdict) ---
	// G3-VERIFY: verify requires --verifier --verdict --evidence. A PASS
	// verdict transitions the run to stopped (≈ completed) and persists
	// the verdict + evidence in the event journal.
	//
	// D1: the verifier must be a registered agent with RoleVerifier
	// belonging to the same run, in the current epoch, with a real
	// evidence_ref. Register a verifier and use a real event_id.

	resp = cliRun("agent", "register", "--run-id", runID, "--role", "verifier", "--runtime", "devin", "--pid", "0")
	result = resp["result"].(map[string]any)
	verifierID := result["agent_id"].(string)

	// Get a real event_id from the run's event journal via legacy run.events.
	resp = cliRun("run.events", `{"run_id":"`+runID+`"}`)
	result = resp["result"].(map[string]any)
	events := result["events"].([]any)
	if len(events) == 0 {
		t.Fatal("no events found for evidence_ref")
	}
	firstEvent := events[0].(map[string]any)
	evidenceRef := firstEvent["event_id"].(string)

	resp = cliRun("run", "verify", "--run-id", runID, "--verifier", verifierID, "--verdict", "PASS", "--evidence", evidenceRef)
	result = resp["result"].(map[string]any)
	if result["state"] != "completed" {
		t.Fatalf("run verify state = %v, want completed", result["state"])
	}
	if result["verdict"] != "PASS" {
		t.Fatalf("run verify verdict = %v, want PASS", result["verdict"])
	}

	// --- agent register → heartbeat → complete → block ---
	// Use a fresh run for agent tests (the previous run is now stopped/terminal).
	resp = cliRun("run", "create", "--project-id", projectID, "--objective", "agent test task")
	result = resp["result"].(map[string]any)
	agentRunID := result["run_id"].(string)

	resp = cliRun("run", "start", "--run-id", agentRunID)
	// Run is now running; register an agent against it.

	resp = cliRun("agent", "register", "--run-id", agentRunID, "--role", "worker", "--runtime", "devin", "--pid", "12345")
	result = resp["result"].(map[string]any)
	agentID := result["agent_id"].(string)
	if !strings.HasPrefix(agentID, "agt_") {
		t.Fatalf("agent_id = %q, want agt_*", agentID)
	}

	resp = cliRun("agent", "heartbeat", "--agent-id", agentID)
	result = resp["result"].(map[string]any)
	if result["renew_deadline"] == nil {
		t.Fatal("expected non-nil renew_deadline")
	}

	resp = cliRun("agent", "complete", "--agent-id", agentID, "--exit-code", "0")
	result = resp["result"].(map[string]any)
	if result["state"] != "exited" {
		t.Fatalf("agent complete state = %v, want exited", result["state"])
	}

	// Block: use another fresh run since the prior agent is exited.
	resp = cliRun("run", "create", "--project-id", projectID, "--objective", "block test task")
	result = resp["result"].(map[string]any)
	blockRunID := result["run_id"].(string)
	resp = cliRun("run", "start", "--run-id", blockRunID)
	resp = cliRun("agent", "register", "--run-id", blockRunID, "--role", "worker", "--runtime", "devin", "--pid", "67890")
	result = resp["result"].(map[string]any)
	blockAgentID := result["agent_id"].(string)

	resp = cliRun("agent", "block", "--agent-id", blockAgentID, "--reason", "test block")
	result = resp["result"].(map[string]any)
	if result["state"] != "blocked" {
		t.Fatalf("agent block state = %v, want blocked (V2 typed)", result["state"])
	}
}

// buildPantheondForTest builds the pantheond binary and returns its path.
func buildPantheondForTest(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pantheond-test")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/pantheond")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pantheond: %v\n%s", err, out)
	}
	return bin
}

// shortSocketDir returns a short-lived temp directory for Unix sockets.
// macOS limits Unix socket paths to ~104 bytes (SUN_LEN); t.TempDir()
// paths under /var/folders/.../T/<TestName><rand>/00X can exceed this.
// This helper uses /tmp with a short prefix to stay well under the limit.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pt-")
	if err != nil {
		t.Fatalf("create short socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestCLI_ExitsNonzeroOnRPCError verifies that the pantheon CLI exits
// with a nonzero code when the daemon returns a JSON-RPC error response
// (D6). This is critical for scripting: a caller that checks $? must
// detect RPC failures.
func TestCLI_ExitsNonzeroOnRPCError(t *testing.T) {
	pantheonBin := buildPantheonCLI(t)
	pantheondBin := buildPantheondForTest(t)

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pantheon.db")
	socketDir := shortSocketDir(t)
	socketPath := filepath.Join(socketDir, "pantheond.sock")
	wtDir := filepath.Join(dir, "wt")

	// Start pantheond in socket mode with scanner disabled (no real runtime).
	daemonCmd := exec.Command(pantheondBin, "-db", dbPath, "-worktrees", wtDir, "-socket", socketPath, "-no-scanner")
	if err := daemonCmd.Start(); err != nil {
		t.Fatalf("start pantheond: %v", err)
	}
	defer func() {
		_ = daemonCmd.Process.Kill()
		daemonCmd.Wait()
	}()

	// Wait for the socket to appear.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket not created: %v", err)
	}

	// Query a nonexistent run — the daemon should return a NOT_FOUND error.
	cmd := exec.Command(pantheonBin, "-socket", socketPath, "run.status", `{"run_id":"run_nonexistent"}`)
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("expected nonzero exit for RPC error, got nil\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected *exec.ExitError, got %T: %v", err, err)
	}
	if exitErr.ExitCode() == 0 {
		t.Fatalf("expected nonzero exit code, got 0\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
	}

	// The response should still be written to stdout and contain the error.
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		t.Fatal("expected non-empty stdout with error response")
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("unmarshal response: %v\noutput: %s", err, out)
	}
	if resp["error"] == nil {
		t.Fatalf("expected error field in response, got: %v", resp)
	}

	// Verify a successful RPC still exits 0.
	cmd = exec.Command(pantheonBin, "-socket", socketPath, "project.register", `{"name":"ok-test","repo_path":"/x","base_ref":"main"}`)
	stdout.Reset()
	stderr.Reset()
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("expected exit 0 for successful RPC, got: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	out = strings.TrimSpace(stdout.String())
	resp = nil // reset to avoid stale keys from the error response
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("unmarshal success response: %v\noutput: %s", err, out)
	}
	if resp["error"] != nil {
		t.Fatalf("expected no error field in success response, got: %v", resp["error"])
	}
}

// runGit runs a git command in the given repo directory.
func runGit(repoPath string, args ...string) error {
	fullArgs := append([]string{"-C", repoPath}, args...)
	cmd := exec.Command("git", fullArgs...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}
