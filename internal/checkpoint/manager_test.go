package checkpoint

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tangtszho/pantheon/internal/domain"
)

// fakeGitRunner is a test GitRunner that records calls and returns
// configurable results.
type fakeGitRunner struct {
	responses map[string]gitResponse // keyed by first arg after "git"
	calls     []gitCall
}

type gitResponse struct {
	stdout   string
	stderr   string
	exitCode int
	err      error
}

type gitCall struct {
	dir  string
	name string
	args []string
}

func (f *fakeGitRunner) Run(ctx context.Context, dir, name string, args ...string) (string, string, int, error) {
	f.calls = append(f.calls, gitCall{dir: dir, name: name, args: args})
	key := ""
	if len(args) > 0 {
		key = args[0]
	}
	resp, ok := f.responses[key]
	if !ok {
		return "", "", 0, nil
	}
	return resp.stdout, resp.stderr, resp.exitCode, resp.err
}

func TestManager_CreateCheckpoint_RequiresWorktreePath(t *testing.T) {
	m := NewManager()
	_, err := m.CreateCheckpoint(context.Background(), "tsk_1", "run_1", "", "summary")
	if err == nil {
		t.Fatal("expected error for empty worktree_path")
	}
}

func TestManager_CreateCheckpoint_RequiresTaskID(t *testing.T) {
	m := NewManager()
	_, err := m.CreateCheckpoint(context.Background(), "", "run_1", "/tmp/wt", "summary")
	if err == nil {
		t.Fatal("expected error for empty task_id")
	}
}

func TestManager_CreateCheckpoint_RevParseFails(t *testing.T) {
	fr := &fakeGitRunner{
		responses: map[string]gitResponse{
			"rev-parse": {stderr: "fatal: not a git repo", exitCode: 128},
		},
	}
	m := NewManager(WithGitRunner(fr))
	_, err := m.CreateCheckpoint(context.Background(), "tsk_1", "run_1", "/tmp/wt", "summary")
	if err == nil {
		t.Fatal("expected error from rev-parse failure")
	}
}

func TestManager_CreateCheckpoint_Success(t *testing.T) {
	fr := &fakeGitRunner{
		responses: map[string]gitResponse{
			"rev-parse":  {stdout: "abc123def456\n"},
			"update-ref": {stdout: ""},
		},
	}
	m := NewManager(WithGitRunner(fr), WithPusher(NoopPusher{}))
	candidateID, err := m.CreateCheckpoint(context.Background(), "tsk_1", "run_1", "/tmp/wt", "fix bug")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if !startsWith(candidateID, "cnd_") {
		t.Fatalf("candidate_id should start with cnd_, got %s", candidateID)
	}

	// Verify rev-parse was called.
	foundRevParse := false
	foundUpdateRef := false
	for _, call := range fr.calls {
		if len(call.args) > 0 && call.args[0] == "rev-parse" {
			foundRevParse = true
		}
		if len(call.args) > 0 && call.args[0] == "update-ref" {
			foundUpdateRef = true
			// Verify ref name format.
			if len(call.args) < 2 {
				t.Fatal("update-ref should have ref name argument")
			}
			refName := call.args[1]
			if !startsWith(refName, "refs/pantheon/") {
				t.Fatalf("ref name should be refs/pantheon/<id>, got %s", refName)
			}
		}
	}
	if !foundRevParse {
		t.Fatal("rev-parse was not called")
	}
	if !foundUpdateRef {
		t.Fatal("update-ref was not called")
	}
}

func TestManager_CreateCheckpoint_UpdateRefFails(t *testing.T) {
	fr := &fakeGitRunner{
		responses: map[string]gitResponse{
			"rev-parse":  {stdout: "abc123\n"},
			"update-ref": {stderr: "fatal: ref exists", exitCode: 1},
		},
	}
	m := NewManager(WithGitRunner(fr))
	_, err := m.CreateCheckpoint(context.Background(), "tsk_1", "run_1", "/tmp/wt", "summary")
	if err == nil {
		t.Fatal("expected error from update-ref failure")
	}
}

func TestManager_GetCandidate_RequiresID(t *testing.T) {
	m := NewManager()
	_, err := m.GetCandidate(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty candidate_id")
	}
}

func TestManager_GetCandidate_ReturnsMinimalCandidate(t *testing.T) {
	m := NewManager()
	c, err := m.GetCandidate(context.Background(), "cnd_test123")
	if err != nil {
		t.Fatalf("GetCandidate: %v", err)
	}
	if c.CandidateID != "cnd_test123" {
		t.Fatalf("candidate_id = %s, want cnd_test123", c.CandidateID)
	}
	if !startsWith(c.RefName, "refs/pantheon/") {
		t.Fatalf("ref_name should start with refs/pantheon/, got %s", c.RefName)
	}
}

func TestManager_AsRPCCheckpointManager(t *testing.T) {
	m := NewManager()
	rpcCM := m.AsRPCCheckpointManager()
	if rpcCM.CreateCheckpoint == nil {
		t.Fatal("CreateCheckpoint function should not be nil")
	}
	if rpcCM.GetCandidate == nil {
		t.Fatal("GetCandidate function should not be nil")
	}
}

func TestNoopPusher(t *testing.T) {
	p := NoopPusher{}
	err := p.Push(context.Background(), "/tmp/repo", "refs/pantheon/test")
	if err != nil {
		t.Fatalf("NoopPusher should not error: %v", err)
	}
}

func TestGitPusher_Success(t *testing.T) {
	fr := &fakeGitRunner{
		responses: map[string]gitResponse{
			"push": {stdout: ""},
		},
	}
	p := NewGitPusher("origin", fr)
	err := p.Push(context.Background(), "/tmp/repo", "refs/pantheon/test")
	if err != nil {
		t.Fatalf("GitPusher.Push: %v", err)
	}
}

func TestGitPusher_PushFails(t *testing.T) {
	fr := &fakeGitRunner{
		responses: map[string]gitResponse{
			"push": {stderr: "fatal: remote rejected", exitCode: 1},
		},
	}
	p := NewGitPusher("origin", fr)
	err := p.Push(context.Background(), "/tmp/repo", "refs/pantheon/test")
	if err == nil {
		t.Fatal("expected error from push failure")
	}
}

// --- integration test with real git ---

func TestManager_CreateCheckpoint_RealGit(t *testing.T) {
	// Skip if git is not available.
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	dir := t.TempDir()
	ctx := context.Background()

	// Init a git repo and make a commit.
	mustRun(t, ctx, dir, "git", "init")
	mustRun(t, ctx, dir, "git", "config", "user.email", "test@test.com")
	mustRun(t, ctx, dir, "git", "config", "user.name", "Test")
	mustRun(t, ctx, dir, "git", "config", "commit.gpgsign", "false")
	mustWriteFile(t, filepath.Join(dir, "README.md"), "# test")
	mustRun(t, ctx, dir, "git", "add", "README.md")
	mustRun(t, ctx, dir, "git", "commit", "-m", "initial")

	mgr := NewManager()
	candidateID, err := mgr.CreateCheckpoint(ctx, "tsk_1", "run_1", dir, "test checkpoint")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if !startsWith(candidateID, "cnd_") {
		t.Fatalf("candidate_id should start with cnd_, got %s", candidateID)
	}

	// Verify the ref was created.
	refName := "refs/pantheon/" + candidateID
	output := mustRun(t, ctx, dir, "git", "rev-parse", refName)
	output = strings.TrimSpace(output)
	if output == "" {
		t.Fatal("ref should point to a commit")
	}

	// Verify the ref is immutable (update-ref without -f should fail if we
	// try to move it — but update-ref by default overwrites. The immutability
	// is a convention, not enforced by git. We verify the ref exists.)
	refs := mustRun(t, ctx, dir, "git", "for-each-ref", "refs/pantheon/")
	if !strings.Contains(refs, refName) {
		t.Fatalf("ref %s not found in refs: %s", refName, refs)
	}
}

// TestGitPusher_RealBareRemote tests GitPusher with a real local bare remote.
// This verifies the full push path: create a ref in a worktree repo, push
// it to a bare remote, then verify the ref exists in the remote.
func TestGitPusher_RealBareRemote(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a bare remote repo.
	remoteDir := filepath.Join(tmpDir, "remote.git")
	mustRun(t, ctx, tmpDir, "git", "init", "--bare", remoteDir)

	// Create a work repo with a commit.
	workDir := filepath.Join(tmpDir, "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir workDir: %v", err)
	}
	mustRun(t, ctx, workDir, "git", "init")
	mustRun(t, ctx, workDir, "git", "config", "user.email", "test@test.com")
	mustRun(t, ctx, workDir, "git", "config", "user.name", "Test")
	mustRun(t, ctx, workDir, "git", "config", "commit.gpgsign", "false")
	mustWriteFile(t, filepath.Join(workDir, "README.md"), "# test")
	mustRun(t, ctx, workDir, "git", "add", "README.md")
	mustRun(t, ctx, workDir, "git", "commit", "-m", "initial")

	// Add the bare remote.
	mustRun(t, ctx, workDir, "git", "remote", "add", "origin", remoteDir)

	// Create a checkpoint ref in the work repo.
	mgr := NewManager()
	candidateID, err := mgr.CreateCheckpoint(ctx, "tsk_1", "run_1", workDir, "test push")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	refName := "refs/pantheon/" + candidateID

	// Push the ref to the bare remote using GitPusher.
	pusher := NewGitPusher("origin", DefaultGitRunner{})
	if err := pusher.Push(ctx, workDir, refName); err != nil {
		t.Fatalf("GitPusher.Push: %v", err)
	}

	// Verify the ref exists in the bare remote.
	refsInRemote := mustRun(t, ctx, remoteDir, "git", "for-each-ref", "refs/pantheon/")
	if !strings.Contains(refsInRemote, refName) {
		t.Fatalf("ref %s not found in remote refs: %s", refName, refsInRemote)
	}

	// Verify the ref in the remote points to the same commit as in the work repo.
	localSHA := strings.TrimSpace(mustRun(t, ctx, workDir, "git", "rev-parse", refName))
	remoteSHA := strings.TrimSpace(mustRun(t, ctx, remoteDir, "git", "rev-parse", refName))
	if localSHA != remoteSHA {
		t.Fatalf("local SHA %s != remote SHA %s", localSHA, remoteSHA)
	}
}

// TestManager_CreateCheckpoint_WithPush tests the full CreateCheckpoint
// flow with a real push to a bare remote.
func TestManager_CreateCheckpoint_WithPush(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a bare remote.
	remoteDir := filepath.Join(tmpDir, "remote.git")
	mustRun(t, ctx, tmpDir, "git", "init", "--bare", remoteDir)

	// Create a work repo.
	workDir := filepath.Join(tmpDir, "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir workDir: %v", err)
	}
	mustRun(t, ctx, workDir, "git", "init")
	mustRun(t, ctx, workDir, "git", "config", "user.email", "test@test.com")
	mustRun(t, ctx, workDir, "git", "config", "user.name", "Test")
	mustRun(t, ctx, workDir, "git", "config", "commit.gpgsign", "false")
	mustWriteFile(t, filepath.Join(workDir, "file.txt"), "content")
	mustRun(t, ctx, workDir, "git", "add", "file.txt")
	mustRun(t, ctx, workDir, "git", "commit", "-m", "initial")
	mustRun(t, ctx, workDir, "git", "remote", "add", "origin", remoteDir)

	// Create a Manager with GitPusher.
	mgr := NewManager(WithPusher(NewGitPusher("origin", DefaultGitRunner{})))
	candidateID, err := mgr.CreateCheckpoint(ctx, "tsk_1", "run_1", workDir, "test push")
	if err != nil {
		t.Fatalf("CreateCheckpoint: %v", err)
	}
	if !startsWith(candidateID, "cnd_") {
		t.Fatalf("candidate_id should start with cnd_, got %s", candidateID)
	}

	// Verify the ref was pushed to the remote.
	refName := "refs/pantheon/" + candidateID
	refsInRemote := mustRun(t, ctx, remoteDir, "git", "for-each-ref", "refs/pantheon/")
	if !strings.Contains(refsInRemote, refName) {
		t.Fatalf("ref %s not pushed to remote: %s", refName, refsInRemote)
	}
}

// --- helpers ---

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func mustRun(t *testing.T, ctx context.Context, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run %s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := writeFile(path, content); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeFile(path, content string) error {
	return exec.Command("sh", "-c", "echo '"+content+"' > '"+path+"'").Run()
}

// Ensure domain import is used.
var _ = domain.ErrInvalidInput
