package domain

import (
	"encoding/json"
	"time"
)

type Project struct {
	ProjectID    string    `json:"project_id"`
	Name         string    `json:"name"`
	RepoPath     string    `json:"repo_path"`
	BaseRef      string    `json:"base_ref"`
	RegisteredAt time.Time `json:"registered_at"`
}

type Workspace struct {
	WorkspaceID string         `json:"workspace_id"`
	ProjectID   string         `json:"project_id"`
	Name        string         `json:"name"`
	Objective   string         `json:"objective"`
	State       WorkspaceState `json:"state"`
	Owner       string         `json:"owner"`
	Host        string         `json:"host"`
	CreatedAt   time.Time      `json:"created_at"`
}

type WorkspaceState string

const (
	WorkspaceCreated  WorkspaceState = "created"
	WorkspaceActive   WorkspaceState = "active"
	WorkspaceStopping WorkspaceState = "stopping"
	WorkspaceStopped  WorkspaceState = "stopped"
	WorkspaceFailed   WorkspaceState = "failed"
)

type Run struct {
	RunID       string        `json:"run_id"`
	WorkspaceID string        `json:"workspace_id"`
	ProjectID   string        `json:"project_id"`
	Owner       string        `json:"owner"`
	BaseCommit  string        `json:"base_commit"`
	Budget      time.Duration `json:"budget"`
	State       RunStateV2    `json:"state"`
	ResultState ResultState   `json:"result_state"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	EndedAt     *time.Time    `json:"ended_at,omitempty"`
	ExitCode    *int          `json:"exit_code,omitempty"`

	// Control-plane §8.2 Run contract fields (additive; populated by the
	// SQLite v3 migration). Legacy RPC methods continue to read/write the
	// fields above; the semantic CLI and typed contracts surface these.
	Epoch      int      `json:"epoch"`
	Lease      RunLease `json:"lease"`
	LastEvent  string   `json:"last_event"`
	Checkpoint string   `json:"checkpoint"`
	Evidence   []string `json:"evidence,omitempty"`

	// NextAction is the PM's explicit decision recorded at run completion
	// (ADR-0018, C4): none | continuation | blocked. Empty string means
	// "not decided" — the reconcile tick surfaces terminal runs with an
	// empty next_action as the "missing decision" case. Populated by the
	// SQLite v9 migration (additive; existing runs get "").
	NextAction NextAction `json:"next_action,omitempty"`
}

type RunState string

const (
	RunPending   RunState = "pending"
	RunPreparing RunState = "preparing"
	RunRunning   RunState = "running"
	RunPaused    RunState = "paused"
	RunResuming  RunState = "resuming"
	RunStopping  RunState = "stopping"
	RunStopped   RunState = "stopped"
	RunFailed    RunState = "failed"
	RunCanceled  RunState = "canceled"

	// RunVerifying and RunCompleted are the §8.1 states surfaced by
	// run.verify (additive — old code that doesn't use them won't break).
	// PASS transitions: running → RunVerifying → RunCompleted.
	// Legacy RunStopped maps to RunV2Completed via LegacyRunStateMap.
	RunVerifying RunState = "verifying"
	RunCompleted RunState = "completed"
)

type Task struct {
	TaskID       string    `json:"task_id"`
	RunID        string    `json:"run_id"`
	Objective    string    `json:"objective"`
	Scope        TaskScope `json:"scope"`
	WorktreePath string    `json:"worktree_path"`
	State        TaskState `json:"state"`
	CreatedAt    time.Time `json:"created_at"`

	// TaskSpec fields (Phase 2 P3+, risk-graded verification):
	// AcceptanceCriteria are the verifiable conditions a run must satisfy;
	// Constraints are hard limits (e.g. "no breaking changes", "tests must
	// pass"); Deliverables are the expected outputs; RiskLevel grades the
	// change (R0-R3) and drives the verification gate (auto-accept vs
	// human approval). All are additive — empty/zero values preserve the
	// legacy behavior. Populated by the SQLite v10 migration.
	AcceptanceCriteria []string  `json:"acceptance_criteria,omitempty"` // verifiable conditions
	Constraints        []string  `json:"constraints,omitempty"`         // e.g. "no breaking changes", "tests must pass"
	Deliverables       []string  `json:"deliverables,omitempty"`        // expected outputs
	RiskLevel          RiskLevel `json:"risk_level,omitempty"`          // R0-R3
}

// RiskLevel grades a change for risk-graded verification (docs/contracts/
// README.md §风险分级). R0/R1 auto-accept on verify PASS; R2/R3 require
// human approval even on verify PASS. The default is R2 (medium) — a safe
// default that requires human approval.
type RiskLevel string

const (
	// RiskR0 is trivial: formatting, docs, comments. Auto-accepts on a
	// verify PASS verdict (the verify still runs; R0 is not "no verify").
	RiskR0 RiskLevel = "R0"
	// RiskR1 is low: single-file logic, test-only changes. Auto-accepts
	// on a verify PASS verdict.
	RiskR1 RiskLevel = "R1"
	// RiskR2 is medium: multi-file, new features. Requires human approval
	// even on a verify PASS verdict — the run stays in verifying with
	// next_action=approval_required until run.approve is called.
	RiskR2 RiskLevel = "R2"
	// RiskR3 is high: security, auth, data migration. Requires human
	// approval plus additional review even on a verify PASS verdict.
	RiskR3 RiskLevel = "R3"
)

// AutoAccepts reports whether a verify PASS verdict auto-accepts the run
// (transitions to completed) without human approval. R0 and R1 auto-accept;
// R2 and R3 require an explicit run.approve (or run.verify with an approval
// flag) after the verify PASS.
func (r RiskLevel) AutoAccepts() bool {
	return r == RiskR0 || r == RiskR1
}

// ValidRiskLevel reports whether r is a recognized RiskLevel (R0-R3).
func ValidRiskLevel(r RiskLevel) bool {
	switch r {
	case RiskR0, RiskR1, RiskR2, RiskR3:
		return true
	}
	return false
}

type TaskScope struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
}

type TaskState string

const (
	TaskDraft          TaskState = "draft"
	TaskReady          TaskState = "ready"
	TaskRunning        TaskState = "running"
	TaskCandidateReady TaskState = "candidate_ready"
	TaskFailed         TaskState = "failed"
	TaskCanceled       TaskState = "canceled"
)

type Agent struct {
	AgentID     string     `json:"agent_id"`
	RunID       string     `json:"run_id"`
	TaskID      string     `json:"task_id,omitempty"`
	Role        AgentRole  `json:"role"`
	Runtime     string     `json:"runtime"`
	PID         int        `json:"pid"`
	State       AgentState `json:"state"`
	SessionID   string     `json:"session_id,omitempty"`
	TmuxSession string     `json:"tmux_session,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	ExitedAt    *time.Time `json:"exited_at,omitempty"`
	ExitCode    *int       `json:"exit_code,omitempty"`

	// Epoch is the run epoch at which this agent acquired the lease
	// (control-plane §8.2). Used by run.verify to reject stale verifiers
	// whose epoch no longer matches the run's current epoch.
	Epoch int `json:"epoch"`
}

type AgentRole string

const (
	RoleController AgentRole = "controller"
	RoleWorker     AgentRole = "worker"
	RoleVerifier   AgentRole = "verifier"
)

type AgentState string

const (
	AgentRegistered AgentState = "registered"
	AgentStarting   AgentState = "starting"
	AgentRunning    AgentState = "running"
	AgentExited     AgentState = "exited"
	AgentLost       AgentState = "lost"
)

type Event struct {
	EventID        string          `json:"event_id"`
	RunID          string          `json:"run_id,omitempty"`
	TaskID         string          `json:"task_id,omitempty"`
	AgentID        string          `json:"agent_id,omitempty"`
	EventType      string          `json:"event_type"`
	Severity       Severity        `json:"severity"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Timestamp      time.Time       `json:"timestamp"`
	Seq            int64           `json:"seq"`
	MessageID      string          `json:"message_id,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MessageSeq     int64           `json:"message_seq,omitempty"`
	AckState       string          `json:"ack_state,omitempty"`   // C-002: pending/acked/nacked/expired/dead
	RetryCount     int             `json:"retry_count,omitempty"` // C-002: number of retries
}

type Severity string

const (
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
)

type Artifact struct {
	ArtifactID string    `json:"artifact_id"`
	RunID      string    `json:"run_id"`
	TaskID     string    `json:"task_id,omitempty"`
	Kind       string    `json:"kind"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	SHA256     string    `json:"sha256"`
	Sensitive  bool      `json:"sensitive"`
	CreatedAt  time.Time `json:"created_at"`
}

type Candidate struct {
	CandidateID string    `json:"candidate_id"`
	TaskID      string    `json:"task_id"`
	RunID       string    `json:"run_id"`
	RefName     string    `json:"ref_name"`
	CommitSHA   string    `json:"commit_sha"`
	Summary     string    `json:"summary"`
	CreatedAt   time.Time `json:"created_at"`
}
