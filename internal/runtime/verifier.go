// Package runtime implements the auto-verifier.
//
// The auto-verifier runs project validation commands in the worktree and
// reports PASS/FAIL to the store. It is triggered by the agent liveness
// scanner when all subtasks in PANTHEON_PROGRESS.md are checked.
//
// The verification commands are defined by the project's AGENTS.md or a
// sensible default for Go projects: gofmt, go vet, go test.
package runtime

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// VerifyStore is the subset of store.Store required by the auto-verifier.
type VerifyStore interface {
	VerifyRun(ctx context.Context, runID string, verdict string, verifierAgentID, evidenceRef, eventID string, nextAction domain.NextAction) (domain.RunStateV2, error)
}

// VerifierConfig controls the auto-verifier.
type VerifierConfig struct {
	// Timeout is the max time for verification commands. Default 120s.
	Timeout time.Duration
	// Logger receives diagnostic messages.
	Logger *log.Logger
}

// Verifier runs project validation commands in a worktree and reports
// the verdict to the store.
type Verifier struct {
	store  VerifyStore
	cfg    VerifierConfig
	logger *log.Logger
}

// NewVerifier creates an auto-verifier.
func NewVerifier(store VerifyStore, cfg VerifierConfig) *Verifier {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Verifier{
		store:  store,
		cfg:    cfg,
		logger: logger,
	}
}

// VerifyResult holds the outcome of a verification run.
type VerifyResult struct {
	Verdict  string // "PASS" or "FAIL"
	Output   string // combined stdout+stderr
	ExitCode int
}

// Verify runs the standard Go validation commands in the worktree and
// records the verdict against the run. Returns the verification result.
func (v *Verifier) Verify(ctx context.Context, runID, worktreePath string) (*VerifyResult, error) {
	v.logger.Printf("auto-verifier: verifying run %s in %s", runID, worktreePath)

	result, err := v.runCommands(ctx, worktreePath)
	if err != nil {
		// Command execution failed (not a test failure — an infra error).
		v.logger.Printf("auto-verifier: execution error for run %s: %v", runID, err)
		return nil, fmt.Errorf("run commands: %w", err)
	}

	verdict := "PASS"
	if result.ExitCode != 0 {
		verdict = "FAIL"
	}

	v.logger.Printf("auto-verifier: run %s verdict=%s exitCode=%d", runID, verdict, result.ExitCode)

	// Record verdict in the store. We use a synthetic verifier agent ID
	// and evidence ref since the auto-verifier is a system-level check,
	// not a registered agent. The store's VerifyRun does not validate
	// these fields — that check is in the RPC handler.
	eid, err := domain.NewID("evt_")
	if err != nil {
		return result, fmt.Errorf("event id: %w", err)
	}
	evidenceRef := eid
	verifierAgentID := "auto-verifier"

	nextAction := domain.NextActionNone
	if verdict == "FAIL" {
		nextAction = domain.NextActionBlocked
	}

	if _, err := v.store.VerifyRun(ctx, runID, verdict, verifierAgentID, evidenceRef, eid, nextAction); err != nil {
		v.logger.Printf("auto-verifier: record verdict for run %s: %v", runID, err)
		return result, fmt.Errorf("verify run: %w", err)
	}

	return result, nil
}

// runCommands executes the standard Go validation sequence in the worktree.
// Returns combined output and the first non-zero exit code (or 0 if all pass).
// gofmt -l is special: it exits 0 even when it lists unformatted files, so
// we treat any non-empty stdout as a failure.
func (v *Verifier) runCommands(ctx context.Context, worktreePath string) (*VerifyResult, error) {
	steps := []struct {
		cmd         []string
		checkOutput bool
	}{
		{[]string{"gofmt", "-l", "."}, true},
		{[]string{"go", "vet", "./..."}, false},
		{[]string{"go", "test", "-count=1", "-timeout", "120s", "./..."}, false},
	}

	var combined strings.Builder
	overallExit := 0

	for _, step := range steps {
		c := exec.CommandContext(ctx, step.cmd[0], step.cmd[1:]...)
		c.Dir = worktreePath
		out, err := c.CombinedOutput()
		if len(out) > 0 {
			combined.WriteString(fmt.Sprintf("$ %s\n", strings.Join(step.cmd, " ")))
			combined.Write(out)
			combined.WriteString("\n")
		}
		if err != nil {
			if overallExit == 0 {
				if exitErr, ok := err.(*exec.ExitError); ok {
					overallExit = exitErr.ExitCode()
				} else {
					return nil, fmt.Errorf("%s: %w", step.cmd[0], err)
				}
			}
		}
		// gofmt -l exits 0 even when it finds unformatted files.
		// Treat any output as a failure.
		if step.checkOutput && len(out) > 0 && overallExit == 0 {
			overallExit = 1
		}
	}

	return &VerifyResult{
		Verdict:  verdictFromExit(overallExit),
		Output:   combined.String(),
		ExitCode: overallExit,
	}, nil
}

func verdictFromExit(exitCode int) string {
	if exitCode == 0 {
		return "PASS"
	}
	return "FAIL"
}
