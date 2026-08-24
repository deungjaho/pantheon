package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// fakeVerifyStore records the last VerifyRun call.
type fakeVerifyStore struct {
	lastRunID      string
	lastVerdict    string
	lastVerifier   string
	lastEvidence   string
	lastNextAction domain.NextAction
	callCount      int
}

func (s *fakeVerifyStore) VerifyRun(ctx context.Context, runID string, verdict string, verifierAgentID, evidenceRef, eventID string, nextAction domain.NextAction) (domain.RunStateV2, error) {
	s.callCount++
	s.lastRunID = runID
	s.lastVerdict = verdict
	s.lastVerifier = verifierAgentID
	s.lastEvidence = evidenceRef
	s.lastNextAction = nextAction
	if verdict == "PASS" {
		return domain.RunV2Completed, nil
	}
	return domain.RunV2Failed, nil
}

func TestVerifier_PassOnCleanWorktree(t *testing.T) {
	// Create a minimal Go module that passes all checks.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeVerifyStore{}
	verifier := NewVerifier(store, VerifierConfig{Timeout: 60 * time.Second})

	result, err := verifier.Verify(context.Background(), "run_test", dir)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Verdict != "PASS" {
		t.Fatalf("verdict = %q, want PASS (output: %s)", result.Verdict, result.Output)
	}
	if store.lastVerdict != "PASS" {
		t.Fatalf("store verdict = %q, want PASS", store.lastVerdict)
	}
	if store.lastNextAction != domain.NextActionNone {
		t.Fatalf("next_action = %q, want none", store.lastNextAction)
	}
}

func TestVerifier_FailOnBrokenCode(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// main.go with a syntax error.
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeVerifyStore{}
	verifier := NewVerifier(store, VerifierConfig{Timeout: 60 * time.Second})

	result, err := verifier.Verify(context.Background(), "run_test", dir)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Verdict != "FAIL" {
		t.Fatalf("verdict = %q, want FAIL", result.Verdict)
	}
	if result.ExitCode == 0 {
		t.Fatal("exit code should be non-zero for broken code")
	}
	if store.lastVerdict != "FAIL" {
		t.Fatalf("store verdict = %q, want FAIL", store.lastVerdict)
	}
	if store.lastNextAction != domain.NextActionBlocked {
		t.Fatalf("next_action = %q, want blocked", store.lastNextAction)
	}
}

func TestVerifier_FailOnFailingTest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main_test.go"), []byte(`package main

import "testing"

func TestAlwaysFails(t *testing.T) {
	t.Fatal("intentional failure")
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeVerifyStore{}
	verifier := NewVerifier(store, VerifierConfig{Timeout: 60 * time.Second})

	result, err := verifier.Verify(context.Background(), "run_test", dir)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Verdict != "FAIL" {
		t.Fatalf("verdict = %q, want FAIL", result.Verdict)
	}
	if !strings.Contains(result.Output, "intentional failure") {
		t.Fatalf("output should contain test failure, got: %s", result.Output)
	}
}

func TestVerifier_RecordsRunID(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeVerifyStore{}
	verifier := NewVerifier(store, VerifierConfig{Timeout: 60 * time.Second})

	if _, err := verifier.Verify(context.Background(), "run_abc123", dir); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if store.lastRunID != "run_abc123" {
		t.Fatalf("runID = %q, want run_abc123", store.lastRunID)
	}
	if store.callCount != 1 {
		t.Fatalf("callCount = %d, want 1", store.callCount)
	}
}

// TestVerifier_GofmtFail ensures gofmt -l detects unformatted code.
func TestVerifier_GofmtFail(t *testing.T) {
	if _, err := exec.LookPath("gofmt"); err != nil {
		t.Skip("gofmt not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module test\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Misaligned struct fields — gofmt will flag this.
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\ntype T struct {\n\ta int\n\tbc int\n}\n\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeVerifyStore{}
	verifier := NewVerifier(store, VerifierConfig{Timeout: 60 * time.Second})

	result, err := verifier.Verify(context.Background(), "run_test", dir)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Verdict != "FAIL" {
		t.Fatalf("verdict = %q, want FAIL (gofmt should detect unformatted code)", result.Verdict)
	}
}
