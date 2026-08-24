package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// fakeCall records a single invocation of the runner.
type fakeCall struct {
	dir  string
	name string
	args []string
}

// fakeRunner is a configurable CommandRunner for testing CleanupWorktree.
// It records every call so tests can assert that no command ran on
// containment-rejected paths. On a successful worktree remove it actually
// deletes the directory to simulate git's behavior.
type fakeRunner struct {
	mu              sync.Mutex
	calls           []fakeCall
	commonDir       string // returned by git rev-parse --git-common-dir
	removeFail      bool   // worktree remove returns non-zero
	removeErrCode   int
	removeErrStderr string
	worktreeList    string // porcelain output for git worktree list
}

func (f *fakeRunner) record(dir, name string, args []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{
		dir:  dir,
		name: name,
		args: append([]string(nil), args...),
	})
}

func (f *fakeRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeRunner) Run(_ context.Context, dir, name string, args ...string) (string, string, int, error) {
	f.record(dir, name, args)

	switch {
	case name == "git" && len(args) == 2 && args[0] == "rev-parse" && args[1] == "--git-common-dir":
		return f.commonDir, "", 0, nil

	case name == "git" && len(args) >= 2 && args[0] == "worktree" && args[1] == "remove":
		if f.removeFail {
			return "", f.removeErrStderr, f.removeErrCode, nil
		}
		// Simulate git worktree remove: delete the directory.
		path := args[len(args)-1]
		if err := os.RemoveAll(path); err != nil {
			return "", err.Error(), 1, nil
		}
		return "", "", 0, nil

	case name == "git" && len(args) >= 2 && args[0] == "worktree" && args[1] == "list":
		return f.worktreeList, "", 0, nil

	default:
		return "", "unexpected command", 1, errors.New("unexpected command")
	}
}

// newTestManager builds a Manager rooted at a temp baseDir with the given runner.
func newTestManager(t *testing.T, r CommandRunner) (*Manager, string) {
	t.Helper()
	base := t.TempDir()
	m := NewManager(WithBaseDir(base), WithRunner(r))
	return m, base
}

// mkdirWorktree creates a directory under base to simulate an existing worktree.
func mkdirWorktree(t *testing.T, base, name string) string {
	t.Helper()
	p := filepath.Join(base, name)
	if err := os.MkdirAll(p, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", p, err)
	}
	return p
}

func assertCode(t *testing.T, err error, want domain.ErrorCode) {
	t.Helper()
	var e *domain.Error
	if !errors.As(err, &e) {
		t.Fatalf("err is not *domain.Error: %v", err)
	}
	if e.Code != want {
		t.Fatalf("err code = %s, want %s (msg: %s)", e.Code, want, e.Message)
	}
}

func assertDirExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Stat(path)
	switch {
	case err == nil && !want:
		t.Fatalf("dir %s still exists, want removed", path)
	case err != nil && want:
		t.Fatalf("dir %s missing, want exists: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// Containment / path validation (F1, F6)
// ---------------------------------------------------------------------------

func TestCleanupWorktree_Containment(t *testing.T) {
	base := t.TempDir()
	// Sentinel dir outside baseDir: must survive every rejected call.
	sentinel := t.TempDir()
	t.Cleanup(func() { _ = os.RemoveAll(sentinel) })

	r := &fakeRunner{commonDir: t.TempDir()}
	m := NewManager(WithBaseDir(base), WithRunner(r))

	tests := []struct {
		name string
		path string
	}{
		{"empty", ""},
		{"relative", "some/relative/path"},
		{"dot", "."},
		{"dotdot", ".."},
		{"root", "/"},
		{"traversal out of base", filepath.Join(base, "..", "escape")},
		{"outside baseDir", sentinel},
		{"home", os.Getenv("HOME")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := r.callCount()
			err := m.CleanupWorktree(context.Background(), tc.path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			assertCode(t, err, domain.CodeInvalidInput)
			if r.callCount() != before {
				t.Fatalf("runner was called %d times after a rejected path; no command may run before containment check",
					r.callCount()-before)
			}
			// Sentinel must be untouched.
			assertDirExists(t, sentinel, true)
		})
	}
}

// ---------------------------------------------------------------------------
// Successful cleanup (F3, F5)
// ---------------------------------------------------------------------------

func TestCleanupWorktree_Success(t *testing.T) {
	r := &fakeRunner{
		commonDir:    t.TempDir(),
		worktreeList: "", // empty list: worktree is gone
	}
	m, base := newTestManager(t, r)
	wt := mkdirWorktree(t, base, "task-abc")

	err := m.CleanupWorktree(context.Background(), wt)
	if err != nil {
		t.Fatalf("CleanupWorktree: %v", err)
	}
	assertDirExists(t, wt, false)

	// Verify the remove ran from the owning repo (commonDir), not the worktree.
	removeFound := false
	for _, c := range r.calls {
		if c.name == "git" && len(c.args) >= 2 && c.args[0] == "worktree" && c.args[1] == "remove" {
			removeFound = true
			if c.dir != r.commonDir {
				t.Fatalf("worktree remove ran from %q, want common dir %q", c.dir, r.commonDir)
			}
		}
	}
	if !removeFound {
		t.Fatal("git worktree remove was never called")
	}
}

// ---------------------------------------------------------------------------
// Failed cleanup: error surfaced, no fallback, no mutation (F1, F3)
// ---------------------------------------------------------------------------

func TestCleanupWorktree_RemoveFails(t *testing.T) {
	r := &fakeRunner{
		commonDir:       t.TempDir(),
		removeFail:      true,
		removeErrCode:   128,
		removeErrStderr: "fatal: worktree is in use",
	}
	m, base := newTestManager(t, r)
	wt := mkdirWorktree(t, base, "task-fail")

	err := m.CleanupWorktree(context.Background(), wt)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertCode(t, err, domain.CodeInternal)
	// Directory must still exist: no os.RemoveAll fallback ran.
	assertDirExists(t, wt, true)
	// The error must surface the real exit code.
	if !strings.Contains(err.Error(), "128") {
		t.Fatalf("error must contain exit code 128: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Orphaned metadata detection (F4): remove exits 0 but worktree list still
// contains the path → error surfaced, not silently accepted.
// ---------------------------------------------------------------------------

func TestCleanupWorktree_OrphanedMetadata(t *testing.T) {
	commonDir := t.TempDir()
	r := &fakeRunner{
		commonDir:    commonDir,
		worktreeList: "worktree " + filepath.Join(commonDir, "ghost") + "\n",
	}
	m, base := newTestManager(t, r)
	wt := mkdirWorktree(t, base, "task-orphan")

	// Patch the worktreeList so the removed path still appears.
	r.worktreeList = "worktree " + wt + "\n"

	err := m.CleanupWorktree(context.Background(), wt)
	if err == nil {
		t.Fatal("expected error for orphaned metadata, got nil")
	}
	assertCode(t, err, domain.CodeInternal)
	if !strings.Contains(err.Error(), "still registered") {
		t.Fatalf("error must mention orphaned registration: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Not a git worktree: rev-parse fails → ErrInvalidInput, no mutation (F5)
// ---------------------------------------------------------------------------

func TestCleanupWorktree_NotAWorktree(t *testing.T) {
	r := &notWorktreeRunner{}
	m, base := newTestManager(t, r)
	wt := mkdirWorktree(t, base, "task-notgit")

	err := m.CleanupWorktree(context.Background(), wt)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	assertCode(t, err, domain.CodeInvalidInput)
	assertDirExists(t, wt, true)
}

// notWorktreeRunner makes rev-parse --git-common-dir fail (non-zero exit).
type notWorktreeRunner struct{}

func (notWorktreeRunner) Run(_ context.Context, _, _ string, _ ...string) (string, string, int, error) {
	return "", "fatal: not a git repository", 128, nil
}

// ---------------------------------------------------------------------------
// CleanupExpired: collects and surfaces errors, does not silently skip (F9)
// ---------------------------------------------------------------------------

func TestCleanupExpired_CollectsErrors(t *testing.T) {
	base := t.TempDir()
	commonDir := t.TempDir()

	// Two expired worktrees: one will succeed, one will fail.
	okPath := mkdirWorktree(t, base, "task-ok")
	failPath := mkdirWorktree(t, base, "task-fail")

	// Make both old enough to be expired.
	oldTime := time.Now().Add(-48 * time.Hour)
	for _, p := range []string{okPath, failPath} {
		if err := os.Chtimes(p, oldTime, oldTime); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}

	r := &selectiveRunner{
		commonDir:    commonDir,
		failPaths:    map[string]bool{failPath: true},
		worktreeList: "", // empty after successful remove
	}
	m := NewManager(WithBaseDir(base), WithRunner(r), WithRetention(1*time.Hour))

	removed, err := m.CleanupExpired(context.Background())
	if err == nil {
		t.Fatal("expected aggregated error, got nil")
	}
	// The failed path must be mentioned in the error.
	if !strings.Contains(err.Error(), failPath) {
		t.Fatalf("error must mention failed path %s: %v", failPath, err)
	}
	// Only the successful path should be in removed.
	if len(removed) != 1 || removed[0] != okPath {
		t.Fatalf("removed = %v, want [%s]", removed, okPath)
	}
	assertDirExists(t, okPath, false)
	assertDirExists(t, failPath, true)
}

func TestCleanupExpired_AllSucceed(t *testing.T) {
	base := t.TempDir()
	commonDir := t.TempDir()

	wt1 := mkdirWorktree(t, base, "task-1")
	wt2 := mkdirWorktree(t, base, "task-2")

	oldTime := time.Now().Add(-48 * time.Hour)
	for _, p := range []string{wt1, wt2} {
		if err := os.Chtimes(p, oldTime, oldTime); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}

	r := &selectiveRunner{
		commonDir:    commonDir,
		failPaths:    nil,
		worktreeList: "",
	}
	m := NewManager(WithBaseDir(base), WithRunner(r), WithRetention(1*time.Hour))

	removed, err := m.CleanupExpired(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(removed) != 2 {
		t.Fatalf("removed count = %d, want 2", len(removed))
	}
	assertDirExists(t, wt1, false)
	assertDirExists(t, wt2, false)
}

func TestCleanupExpired_SkipsNonExpired(t *testing.T) {
	base := t.TempDir()
	commonDir := t.TempDir()

	wt := mkdirWorktree(t, base, "task-fresh")
	// Leave ModTime as now (not expired).

	r := &selectiveRunner{
		commonDir:    commonDir,
		worktreeList: "",
	}
	m := NewManager(WithBaseDir(base), WithRunner(r), WithRetention(1*time.Hour))

	removed, err := m.CleanupExpired(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want empty", removed)
	}
	assertDirExists(t, wt, true)
}

// selectiveRunner succeeds for all worktree remove calls except those whose
// path is in failPaths.
type selectiveRunner struct {
	commonDir    string
	failPaths    map[string]bool
	worktreeList string
	mu           sync.Mutex
	removed      map[string]bool
}

func (s *selectiveRunner) Run(_ context.Context, dir, name string, args ...string) (string, string, int, error) {
	switch {
	case name == "git" && len(args) == 2 && args[0] == "rev-parse" && args[1] == "--git-common-dir":
		return s.commonDir, "", 0, nil

	case name == "git" && len(args) >= 2 && args[0] == "worktree" && args[1] == "remove":
		path := args[len(args)-1]
		if s.failPaths[path] {
			return "", "fatal: refused", 128, nil
		}
		if err := os.RemoveAll(path); err != nil {
			return "", err.Error(), 1, nil
		}
		return "", "", 0, nil

	case name == "git" && len(args) >= 2 && args[0] == "worktree" && args[1] == "list":
		return s.worktreeList, "", 0, nil

	default:
		return "", "unexpected", 1, errors.New("unexpected command")
	}
}
