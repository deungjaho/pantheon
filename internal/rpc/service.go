package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/tangtszho/pantheon/internal/auditor"
	"github.com/tangtszho/pantheon/internal/beacon"
	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/hydra"
	"github.com/tangtszho/pantheon/internal/push"
	"github.com/tangtszho/pantheon/internal/store"
	"github.com/tangtszho/pantheon/internal/wake"
)

// hostname returns the local hostname, or "localhost" if it can't be determined.
// Used for workspace Host field — no hardcoded hostname (ADR-0001 revised).
var cachedHostname string
var hostnameOnce bool

func hostname() string {
	if hostnameOnce {
		return cachedHostname
	}
	hostnameOnce = true
	if h, err := os.Hostname(); err == nil && h != "" {
		cachedHostname = h
	} else {
		cachedHostname = "localhost"
	}
	return cachedHostname
}

// newEventID wraps domain.NewID for event IDs, returning a domain.Error on
// failure so callers can return it directly.
func newEventID() (string, error) {
	id, err := domain.NewID("evt_")
	if err != nil {
		return "", domain.ErrInternal("event id generation: " + err.Error())
	}
	return id, nil
}

// eventIDFromMessageID derives the event_id that Store.PublishMessageEnvelope
// assigns to a message event. The store builds event_id as "evt_" + the
// message_id with any "msg_" prefix stripped. This mirrors that derivation so
// the push layer can carry the same event_id subscribers would see in the
// pull-based event journal.
func eventIDFromMessageID(msgID string) string {
	if strings.HasPrefix(msgID, "msg_") {
		return "evt_" + msgID[4:]
	}
	return "evt_" + msgID
}

// DefaultBudget is the Phase 1 default for normal tasks.
const DefaultBudget = 8 * time.Hour

// StopGracePeriod is the time given to a worker to shut down gracefully
// before SIGKILL is sent.
const StopGracePeriod = 10 * time.Second

// MaxRootCauseBreaker is the same-root-cause circuit breaker threshold
// (ROADMAP "同根因 3 次熔断"). When the same root cause has occurred this
// many times across a run chain, AutoContinue blocks the run instead of
// creating another successor. This is an additional check on top of the
// scanner's progress gate — it catches repeated identical failure reasons
// even when the remaining-subtask count appears to decrease.
const MaxRootCauseBreaker = 3

// Service wires RPC methods to the store and Phase 1 services. It is the
// application layer that the daemon embeds.
type Service struct {
	Store *store.Store

	// WorkspaceMgr resolves base commits and creates worktrees. May be nil
	// in tests that only exercise the store-backed RPC paths.
	WorkspaceMgr WorkspaceManager

	// Runtime starts/stops agent processes. May be nil in tests.
	Runtime RuntimeAdapter

	// Checkpoint manages immutable refs and candidates. May be nil.
	Checkpoint CheckpointManager

	// Notifier sends tmux notifications on message.publish. May be nil.
	Notifier TmuxNotifier

	// InboxProjector projects published messages to inbox/outbox files
	// (C-004). May be nil. Projection is best-effort — failures do not
	// block publish. Messages with sensitivity=restricted are not projected.
	InboxProjector InboxProjector

	// Beacon discovers active agent sessions from tmux panes. May be nil
	// (degraded mode — agent.discover returns "beacon not configured").
	Beacon BeaconDiscoverer

	// Hydra is the LLM model-routing gateway. May be nil (degraded mode —
	// hydra.models/hydra.health return "hydra not configured").
	Hydra HydraGateway

	// Pusher triggers real-time message-published notifications to push
	// subscribers (Solution B). Defaults to NoopPusher (push disabled) so
	// the system falls back to pull-based cursor polling. May be nil —
	// nil is treated as NoopPusher. NotifyMessage is best-effort and must
	// not block the caller.
	Pusher push.Pusher

	// Auditor is the Global Auditor (Phase 4): a periodic analyzer of run
	// history that produces structured findings. May be nil (degraded mode —
	// auditor.* RPCs return "auditor not configured"). The auditor does NOT
	// auto-modify anything; findings require human acceptance.
	Auditor *auditor.Auditor

	// Mnemos is the memory service for auto-ingest of completed runs.
	// May be nil (disabled — run complete is not ingested). When set, a
	// best-effort, asynchronous ingest is fired on every run that
	// transitions to completed (R0/R1 auto-accept, run.approve, or the
	// run.verify approval path). Ingest failures are logged to stderr and
	// never block run completion.
	Mnemos MnemosIngester

	// Version info for initialize.
	ServerName    string
	ServerVersion string
}

// BeaconDiscoverer is the consumer-side interface for Beacon agent
// discovery. Implementations shell out to the `beacon` CLI.
type BeaconDiscoverer interface {
	DiscoverAgents(ctx context.Context) ([]beacon.AgentSession, error)
}

// HydraGateway is the consumer-side interface for the Hydra LLM gateway.
// Implementations call Hydra's HTTP API.
type HydraGateway interface {
	ListModels(ctx context.Context) ([]hydra.Model, error)
	Healthz(ctx context.Context) error
}

// MnemosIngester is the consumer-side interface for the Mnemos memory
// service. Implementations call Mnemos's HTTP API. Ingest is best-effort
// and asynchronous from the caller's perspective — the run is already
// completed in the store before ingest is attempted.
type MnemosIngester interface {
	Ingest(ctx context.Context, text string, metadata map[string]string) error
}

// InboxProjector projects messages to inbox/outbox files (C-004).
// Implementations must be best-effort: errors must not block publish.
type InboxProjector interface {
	Project(ctx context.Context, msg *domain.Message) error
}

// TmuxNotifier is the interface for sending notifications to agents.
type TmuxNotifier interface {
	Notify(ctx context.Context, agentID, message string) error
}

// WorkspaceManager is the consumer-side interface for workspace operations.
type WorkspaceManager interface {
	ResolveBaseCommit(ctx context.Context, repoPath, baseRef string) (string, error)
	PrepareWorktree(ctx context.Context, repoPath, baseCommit, taskID string) (string, error)
	CleanupWorktree(ctx context.Context, worktreePath string) error
}

// RuntimeAdapter is the consumer-side interface for runtime lifecycle.
type RuntimeAdapter interface {
	Start(ctx context.Context, p RuntimeStartParams) (RuntimeHandle, error)
	Stop(ctx context.Context, h RuntimeHandle, grace time.Duration) error
	Inspect(ctx context.Context, h RuntimeHandle) (RuntimeStatus, error)
}

type RuntimeStartParams struct {
	RunID, TaskID, WorktreePath string
	Objective                   string
	Scope                       domain.TaskScope
	Budget                      time.Duration
	SessionID                   string
}

type RuntimeHandle struct {
	AgentID   string
	PID       int
	SessionID string
}

type RuntimeStatus struct {
	State    domain.AgentState
	ExitCode *int
}

// CheckpointManager is the consumer-side interface for checkpoints.
type CheckpointManager struct {
	CreateCheckpoint func(ctx context.Context, taskID, runID, worktreePath, summary string) (string, error)
	GetCandidate     func(ctx context.Context, candidateID string) (*domain.Candidate, error)
}

// RegisterAll registers all RPC methods on the server.
//
// Methods are grouped:
//   - current: the typed contract surface used by the semantic CLI.
//   - legacy:  run.status is kept as a legacy facade for run status queries
//     (acceptance-contract G3-BC.4). Existing clients keep working; new
//     clients should use the semantic CLI.
func (svc *Service) RegisterAll(srv *Server) {
	// Current typed-contract methods.
	srv.Register("initialize", svc.handleInitialize)
	srv.Register("project.register", svc.handleProjectRegister)
	srv.Register("project.list", svc.handleProjectList)
	srv.Register("project.status", svc.handleProjectStatus)
	srv.Register("run.create", svc.handleRunCreate)
	srv.Register("run.start", svc.handleRunStart)
	srv.Register("run.status", svc.handleRunStatus)
	srv.Register("run.verify", svc.handleRunVerify)
	srv.Register("run.approve", svc.handleRunApprove)
	srv.Register("run.block", svc.handleRunBlock)
	srv.Register("run.unblock", svc.handleRunUnblock)
	srv.Register("run.terminate", svc.handleRunTerminate)
	srv.Register("run.events", svc.handleRunEvents)
	srv.Register("run.takeover", svc.handleRunTakeover)
	srv.Register("run.list", svc.handleRunList)
	srv.Register("agent.register", svc.handleAgentRegister)
	srv.Register("agent.heartbeat", svc.handleAgentHeartbeat)
	srv.Register("agent.complete", svc.handleAgentComplete)
	srv.Register("agent.block", svc.handleAgentBlock)
	srv.Register("message.publish.envelope", svc.handleMessagePublishEnvelope)
	srv.Register("messages.by_run", svc.handleMessagesByRun)
	srv.Register("message.ack", svc.handleMessageAck)
	srv.Register("message.nack", svc.handleMessageNack)
	srv.Register("messages.deadline_check", svc.handleMessagesDeadlineCheck)
	srv.Register("messages.status", svc.handleMessagesStatus)
	srv.Register("reconcile.crash", svc.handleReconcileCrash)

	// Continuation slice (ADR-0017): durable wake/continuation records.
	// These are explicit PM/operator actions — the system never auto-creates
	// successors. The reconcile tick detects and notifies; the PM acts.
	srv.Register("continuation.register", svc.handleContinuationRegister)
	srv.Register("continuation.list", svc.handleContinuationList)
	srv.Register("continuation.fulfill", svc.handleContinuationFulfill)
	srv.Register("continuation.cancel", svc.handleContinuationCancel)
	srv.Register("reconcile.continuations", svc.handleReconcileContinuations)

	// Terminal-state consistency slice (ADR-0018): atomic verify projection,
	// agent terminalization, explicit supersede link, next_action decision.
	srv.Register("run.supersede", svc.handleRunSupersede)
	srv.Register("run.set_next_action", svc.handleRunSetNextAction)
	srv.Register("reconcile.terminal_state", svc.handleReconcileTerminalState)

	// Beacon discovery + Hydra model routing (optional integrations).
	// When not configured, these return a clear "not configured" error
	// (degraded mode) rather than a generic internal error.
	srv.Register("agent.discover", svc.handleAgentDiscover)
	srv.Register("hydra.models", svc.handleHydraModels)
	srv.Register("hydra.health", svc.handleHydraHealth)

	// Global Auditor (Phase 4): periodic run-history analysis producing
	// structured findings. When not configured (svc.Auditor is nil), these
	// return "auditor not configured" (degraded mode). The auditor does NOT
	// auto-modify anything — findings require human acceptance.
	srv.Register("auditor.audit", svc.handleAuditorAudit)
	srv.Register("auditor.findings", svc.handleAuditorFindings)
	srv.Register("auditor.review", svc.handleAuditorReview)
}

func (svc *Service) handleInitialize(ctx context.Context, params json.RawMessage) (any, error) {
	var p InitializeParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid initialize params: " + err.Error())
		}
	}
	return &InitializeResult{
		ServerName:    orDefault(svc.ServerName, "pantheond"),
		ServerVersion: orDefault(svc.ServerVersion, "0.1.0"),
		Protocol:      1,
		// Current typed-contract methods are listed first; legacy facade
		// methods follow (deprecated, G3-BC.2 — kept callable, not
		// prioritized).
		Capabilities: []string{
			"initialize",
			"project.register", "project.list", "project.status",
			"run.create", "run.start", "run.status", "run.verify", "run.approve",
			"run.block", "run.unblock", "run.terminate",
			"run.events", "run.takeover", "run.list",
			"agent.register", "agent.heartbeat", "agent.complete", "agent.block",
			// v1.1 typed message envelope:
			"message.publish.envelope", "messages.by_run",
			"message.ack", "message.nack",
			"messages.deadline_check", "messages.status",
			"reconcile.crash",
			// continuation slice (ADR-0017): durable wake/continuation:
			"continuation.register", "continuation.list",
			"continuation.fulfill", "continuation.cancel",
			"reconcile.continuations",
			// terminal-state consistency slice (ADR-0018):
			"run.supersede", "run.set_next_action",
			"reconcile.terminal_state",
			// Beacon discovery + Hydra model routing (optional):
			"agent.discover", "hydra.models", "hydra.health",
			// Global Auditor (Phase 4, optional):
			"auditor.audit", "auditor.findings", "auditor.review",
		},
	}, nil
}

func (svc *Service) handleProjectRegister(ctx context.Context, params json.RawMessage) (any, error) {
	var p ProjectRegisterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.Name == "" || p.RepoPath == "" || p.BaseRef == "" {
		return nil, domain.ErrInvalidInput("name, repo_path, base_ref are required")
	}
	pid, err := domain.NewID("prj_")
	if err != nil {
		return nil, domain.ErrInternal("id generation: " + err.Error())
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	proj := &domain.Project{
		ProjectID:    pid,
		Name:         p.Name,
		RepoPath:     p.RepoPath,
		BaseRef:      p.BaseRef,
		RegisteredAt: time.Now().UTC(),
	}
	if err := svc.Store.RegisterProject(ctx, proj, eid); err != nil {
		return nil, err
	}
	return &ProjectRegisterResult{ProjectID: pid}, nil
}

// handleProjectList lists all registered projects.
func (svc *Service) handleProjectList(ctx context.Context, params json.RawMessage) (any, error) {
	projects, err := svc.Store.ListProjects(ctx)
	if err != nil {
		return nil, domain.ErrInternal("list projects: " + err.Error())
	}
	if projects == nil {
		projects = []*domain.Project{}
	}
	return &ProjectListResult{Projects: projects}, nil
}

// handleProjectStatus returns a single project by ID.
func (svc *Service) handleProjectStatus(ctx context.Context, params json.RawMessage) (any, error) {
	var p ProjectStatusParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.ProjectID == "" {
		return nil, domain.ErrInvalidInput("project_id is required")
	}
	proj, err := svc.Store.GetProject(ctx, p.ProjectID)
	if err != nil {
		return nil, domain.ErrInternal("get project: " + err.Error())
	}
	if proj == nil {
		return nil, domain.ErrNotFound("project not found: " + p.ProjectID)
	}
	return &ProjectStatusResult{Project: proj}, nil
}

// handleRunCreate creates a run without starting it (control-plane §8.2).
// The run is left in the pending state; run.start begins execution.
func (svc *Service) handleRunCreate(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunCreateParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.ProjectID == "" || p.Objective == "" {
		return nil, domain.ErrInvalidInput("project_id and objective are required")
	}
	proj, err := svc.Store.GetProject(ctx, p.ProjectID)
	if err != nil {
		return nil, domain.ErrInternal("get project: " + err.Error())
	}
	if proj == nil {
		return nil, domain.ErrNotFound("project not found: " + p.ProjectID)
	}
	baseRef := p.BaseRef
	if baseRef == "" {
		baseRef = proj.BaseRef
	}
	budget := p.Budget
	if budget == 0 {
		budget = DefaultBudget
	}

	var baseCommit string
	if svc.WorkspaceMgr != nil {
		bc, err := svc.WorkspaceMgr.ResolveBaseCommit(ctx, proj.RepoPath, baseRef)
		if err != nil {
			return nil, err
		}
		baseCommit = bc
	} else {
		baseCommit = baseRef
	}

	wid, err := domain.NewID("ws_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}
	rid, err := domain.NewID("run_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}
	tid, err := domain.NewID("tsk_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}

	owner := p.Owner
	if owner == "" {
		owner = "local-user"
	}
	name := p.Name
	if name == "" {
		name = fmt.Sprintf("run-%s", rid[:12])
	}

	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.CreateWorkspace(ctx, &domain.Workspace{
		WorkspaceID: wid,
		ProjectID:   p.ProjectID,
		Name:        name,
		Objective:   p.Objective,
		State:       domain.WorkspaceActive,
		Owner:       owner,
		Host:        hostname(),
		CreatedAt:   time.Now().UTC(),
	}, eid); err != nil {
		return nil, err
	}

	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.CreateRun(ctx, &domain.Run{
		RunID:       rid,
		WorkspaceID: wid,
		ProjectID:   p.ProjectID,
		Owner:       owner,
		BaseCommit:  baseCommit,
		Budget:      budget,
		State:       domain.RunV2Requested,
	}, eid); err != nil {
		return nil, err
	}

	worktreePath := ""
	if p.ContinueFrom != "" {
		// Continuation: reuse the previous run's worktree so the new
		// agent picks up code changes and PANTHEON_PROGRESS.md.
		prevTask, err := svc.Store.GetTaskByRun(ctx, p.ContinueFrom)
		if err != nil {
			eid, _ := newEventID()
			_ = svc.Store.UpdateRunState(ctx, rid, domain.RunV2Failed, eid)
			return nil, domain.ErrInternal("continuation: get previous task: " + err.Error())
		}
		if prevTask == nil || prevTask.WorktreePath == "" {
			eid, _ := newEventID()
			_ = svc.Store.UpdateRunState(ctx, rid, domain.RunV2Failed, eid)
			return nil, domain.ErrInvalidInput("continuation: previous run has no worktree")
		}
		worktreePath = prevTask.WorktreePath
	} else if svc.WorkspaceMgr != nil {
		wp, err := svc.WorkspaceMgr.PrepareWorktree(ctx, proj.RepoPath, baseCommit, tid)
		if err != nil {
			eid, _ := newEventID()
			_ = svc.Store.UpdateRunState(ctx, rid, domain.RunV2Failed, eid)
			return nil, err
		}
		worktreePath = wp
	} else {
		worktreePath = fmt.Sprintf("/tmp/pantheon-worktrees/%s", tid)
	}

	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	// Parse the risk level (default R2 — a safe default requiring human
	// approval). An empty or invalid value falls back to R2.
	risk := domain.RiskLevel(p.RiskLevel)
	if !domain.ValidRiskLevel(risk) {
		risk = domain.RiskR2
	}
	if err := svc.Store.CreateTask(ctx, &domain.Task{
		TaskID:             tid,
		RunID:              rid,
		Objective:          p.Objective,
		Scope:              p.Scope,
		WorktreePath:       worktreePath,
		State:              domain.TaskReady,
		CreatedAt:          time.Now().UTC(),
		AcceptanceCriteria: p.AcceptanceCriteria,
		Constraints:        p.Constraints,
		Deliverables:       p.Deliverables,
		RiskLevel:          risk,
	}, eid); err != nil {
		return nil, err
	}

	return &RunCreateResult{WorkspaceID: wid, RunID: rid, TaskID: tid}, nil
}

// handleRunStart begins execution of a previously created run. It drives
// the §8.1 state machine: requested → planning → ready → running and
// starts the runtime if available.
func (svc *Service) handleRunStart(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunStartParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}

	// §8.1: requested → planning → ready → running.
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Planning, eid); err != nil {
		return nil, err
	}
	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Ready, eid); err != nil {
		return nil, err
	}

	task, _ := svc.Store.GetTaskByRun(ctx, p.RunID)
	worktreePath := ""
	objective := ""
	if task != nil {
		worktreePath = task.WorktreePath
		objective = task.Objective
	}

	var agentID string
	if svc.Runtime != nil && worktreePath != "" {
		h, err := svc.Runtime.Start(ctx, RuntimeStartParams{
			RunID:        p.RunID,
			TaskID:       task.TaskID,
			WorktreePath: worktreePath,
			Objective:    objective,
			Budget:       run.Budget,
		})
		if err != nil {
			eid, _ := newEventID()
			_ = svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Failed, eid)
			return nil, err
		}
		agentID = h.AgentID
		eid, err = newEventID()
		if err != nil {
			return nil, err
		}
		if err := svc.Store.RegisterAgent(ctx, &domain.Agent{
			AgentID:   h.AgentID,
			RunID:     p.RunID,
			TaskID:    task.TaskID,
			Role:      domain.RoleWorker,
			Runtime:   "devin",
			PID:       h.PID,
			State:     domain.AgentRunning,
			SessionID: h.SessionID,
			StartedAt: time.Now().UTC(),
		}, eid); err != nil {
			return nil, err
		}
	}

	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Running, eid); err != nil {
		return nil, err
	}
	return &RunStartResult{RunID: p.RunID, State: domain.RunV2Running, AgentID: agentID}, nil
}

// handleRunVerify processes a typed verdict against a run (control-plane
// §3.3, §8.1, acceptance-contract G3-VERIFY). The verdict, verifier identity,
// and evidence reference are all required — missing any of them returns
// ErrInvalidInput without transitioning the run state.
//
// Risk-graded verification (docs/contracts/README.md §风险分级):
//   - R0/R1 (auto-accept): on PASS the run transitions through the §8.1
//     verifying state to completed (running → verifying → completed) and a
//     verify.passed event is appended. result_state='accepted'.
//   - R2/R3 (approval required): on PASS the run transitions only to
//     verifying (running → verifying), result_state='accepted',
//     next_action='approval_required', and a message is published to the PM
//     message queue requesting human approval. The run stays in verifying
//     until run.approve (or run.verify with Approved=true) is called.
//
// On FAIL (any risk level): the run transitions to failed and a
// verify.failed event is appended.
//
// The Approved flag is the human-approval path: when a run is already in
// the verifying state with next_action=approval_required, calling
// run.verify with PASS and Approved=true transitions the run to completed
// with result_state='approved'. The verifier-agent authorization checks
// are relaxed for this path since the caller is a human approver.
//
// completed is only set by an explicit PASS verdict (or approval) here —
// never manufactured by a worker's self-report or a stub (G3-VERIFY.4).
func (svc *Service) handleRunVerify(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunVerifyParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	if p.Verdict != VerdictPass && p.Verdict != VerdictFail {
		return nil, domain.ErrInvalidInput("verdict must be PASS or FAIL")
	}

	// 0. The run must exist (NOT_FOUND before any further checks).
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}

	// --- Approval path: run.verify with Approved=true on a verifying run ---
	// When a high-risk run is in verifying with next_action=approval_required,
	// a human approver may call run.verify with PASS + Approved=true to
	// transition it to completed. The verifier-agent authorization checks
	// are relaxed for this path (the caller is a human, not a registered
	// verifier agent). The evidence_ref is optional for approval.
	if p.Approved && p.Verdict == VerdictPass && run.State == domain.RunV2Verifying {
		eid, err := newEventID()
		if err != nil {
			return nil, err
		}
		approver := p.VerifierAgentID
		if approver == "" {
			approver = "unknown"
		}
		finalState, err := svc.Store.ApproveRun(ctx, p.RunID, approver, p.EvidenceRef, eid)
		if err != nil {
			return nil, err
		}
		// Auto-ingest to Mnemos (best-effort, async). The run is already
		// completed in the store; ingest failure does not block completion.
		task, _ := svc.Store.GetTaskByRun(ctx, p.RunID)
		svc.ingestCompletedRun(p.RunID, run.ProjectID, domain.ResultApproved, task)
		return &RunVerifyResult{
			RunID:       p.RunID,
			State:       finalState,
			Verdict:     p.Verdict,
			EvidenceRef: p.EvidenceRef,
			ResultState: domain.ResultApproved,
			NextAction:  domain.NextActionNone,
		}, nil
	}

	// G3-VERIFY.1: verdict without verifier identity or evidence is rejected.
	// Do NOT transition state.
	if p.VerifierAgentID == "" {
		return nil, domain.ErrInvalidInput("verifier_agent_id is required")
	}
	if p.EvidenceRef == "" {
		return nil, domain.ErrInvalidInput("evidence_ref is required")
	}

	// --- D1: Authorization checks BEFORE any state transition ---
	// The verifier must be a registered agent with RoleVerifier belonging
	// to the same run, in the current epoch, with a real evidence_ref.
	// On any failure, return the error WITHOUT transitioning state.

	// 1. The verifier_agent_id must refer to a registered agent.
	verifier, err := svc.Store.GetAgent(ctx, p.VerifierAgentID)
	if err != nil {
		return nil, domain.ErrInternal("get verifier agent: " + err.Error())
	}
	if verifier == nil {
		return nil, domain.ErrNotFound("verifier agent not found: " + p.VerifierAgentID)
	}

	// 2. The agent's role must be RoleVerifier.
	if verifier.Role != domain.RoleVerifier {
		return nil, domain.ErrUnauthorized("agent " + p.VerifierAgentID + " is not a verifier (role=" + string(verifier.Role) + ")")
	}

	// 3. The agent must belong to the same run.
	if verifier.RunID != p.RunID {
		return nil, domain.ErrUnauthorized("verifier agent " + p.VerifierAgentID + " does not belong to run " + p.RunID)
	}

	// 4. The agent must be in the current epoch of the run (reject stale
	// verifiers whose epoch no longer matches the run's current epoch).
	if verifier.Epoch != run.Epoch {
		return nil, domain.ErrConflict(fmt.Sprintf("stale verifier epoch %d != run epoch %d", verifier.Epoch, run.Epoch))
	}

	// 5. The evidence_ref must resolve to a real event in the journal.
	exists, err := svc.Store.EventExists(ctx, p.RunID, p.EvidenceRef)
	if err != nil {
		return nil, domain.ErrInternal("check evidence_ref: " + err.Error())
	}
	if !exists {
		return nil, domain.ErrInvalidInput("evidence_ref not found: " + p.EvidenceRef)
	}

	// --- Risk-graded verification gate ---
	// Look up the task's risk level. R0/R1 auto-accept on PASS; R2/R3
	// require human approval (stop at verifying). FAIL always goes to
	// failed regardless of risk level.
	task, err := svc.Store.GetTaskByRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get task for risk level: " + err.Error())
	}
	risk := domain.RiskR2 // safe default if no task
	if task != nil && task.RiskLevel != "" {
		risk = task.RiskLevel
	}

	if p.Verdict == VerdictPass && !risk.AutoAccepts() {
		// R2/R3 PASS: stop at verifying, request human approval.
		eid, err := newEventID()
		if err != nil {
			return nil, err
		}
		finalState, err := svc.Store.VerifyRunApprovalRequired(ctx, p.RunID, string(p.Verdict), p.VerifierAgentID, p.EvidenceRef, eid)
		if err != nil {
			return nil, err
		}
		// Publish a message to the PM message queue requesting human
		// approval. Best-effort: a publish failure does not undo the
		// verify (the run is already in verifying with
		// next_action=approval_required, which the reconcile tick
		// surfaces). The PM acts on the message or the projection.
		svc.publishApprovalRequest(ctx, p.RunID, task, risk, p.EvidenceRef)
		return &RunVerifyResult{
			RunID:       p.RunID,
			State:       finalState,
			Verdict:     p.Verdict,
			EvidenceRef: p.EvidenceRef,
			ResultState: domain.ResultAccepted,
			NextAction:  domain.NextActionApprovalRequired,
		}, nil
	}

	// R0/R1 PASS (auto-accept) or FAIL (any risk level): drive the full
	// §8.1 transition via VerifyRun.
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	finalState, err := svc.Store.VerifyRun(ctx, p.RunID, string(p.Verdict), p.VerifierAgentID, p.EvidenceRef, eid, domain.NextAction(p.NextAction))
	if err != nil {
		return nil, err
	}
	// Determine the effective next_action (default applied inside VerifyRun).
	effNext := domain.NextAction(p.NextAction)
	var effResult domain.ResultState
	if effNext == "" {
		if p.Verdict == VerdictPass {
			effNext = domain.NextActionNone
		} else {
			effNext = domain.NextActionBlocked
		}
	}
	if p.Verdict == VerdictPass {
		effResult = domain.ResultAccepted
	} else {
		effResult = domain.ResultFailed
	}
	// Auto-ingest to Mnemos on PASS (completed). FAIL goes to failed and
	// is not ingested. Best-effort, async — the run is already completed
	// in the store; ingest failure does not block completion.
	if p.Verdict == VerdictPass {
		svc.ingestCompletedRun(p.RunID, run.ProjectID, effResult, task)
	}
	return &RunVerifyResult{
		RunID:       p.RunID,
		State:       finalState,
		Verdict:     p.Verdict,
		EvidenceRef: p.EvidenceRef,
		ResultState: effResult,
		NextAction:  effNext,
	}, nil
}

// publishApprovalRequest publishes a message to the PM message queue
// requesting human approval for a high-risk (R2/R3) run that passed
// verification. Best-effort: a publish failure is logged but does not
// undo the verify — the run is already in verifying with
// next_action=approval_required, which the reconcile tick surfaces.
func (svc *Service) publishApprovalRequest(ctx context.Context, runID string, task *domain.Task, risk domain.RiskLevel, evidenceRef string) {
	msgID, err := domain.NewID("msg_")
	if err != nil {
		return
	}
	objective := ""
	if task != nil {
		objective = task.Objective
	}
	inline := fmt.Sprintf("run %s passed verification (risk=%s) and requires human approval before completing. objective=%q evidence_ref=%s",
		runID, risk, objective, evidenceRef)
	msg := domain.Message{
		MessageID:      msgID,
		RunID:          runID,
		TaskID:         "",
		Sender:         domain.MessageEndpoint{Role: domain.RoleController},
		Recipient:      domain.MessageEndpoint{Role: domain.RolePM},
		Type:           domain.MsgBlock,
		IdempotencyKey: msgID,
		PayloadRef:     domain.PayloadRef{Kind: "inline", Inline: inline},
		Sensitivity:    domain.SensNormal,
		CreatedAt:      time.Now().UTC(),
	}
	if _, msgSeq, msgID, err := svc.Store.PublishMessageEnvelope(ctx, &msg); err != nil {
		// Best-effort: the run is already in verifying with
		// next_action=approval_required; the reconcile tick surfaces it.
		_ = err
	} else if svc.Pusher != nil {
		// Solution B: notify push subscribers of the approval-required
		// message so the PM learns in real time.
		svc.Pusher.NotifyMessage(runID, msgSeq, eventIDFromMessageID(msgID))
	}
}

// ingestCompletedRun fires a best-effort, asynchronous Mnemos ingest for a
// run that just transitioned to completed. It does NOT block the caller —
// the run is already completed in the store. If svc.Mnemos is nil this is
// a no-op (Mnemos disabled). Ingest failures are logged to stderr.
//
// runID is the completed run; projectID is run.ProjectID; resultState is
// the result_state set by the transition (accepted/approved). task is the
// run's task (may be nil — objective and risk_level fall back to empty).
func (svc *Service) ingestCompletedRun(runID, projectID string, resultState domain.ResultState, task *domain.Task) {
	if svc.Mnemos == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	objective := ""
	risk := domain.RiskLevel("")
	taskID := ""
	if task != nil {
		objective = task.Objective
		risk = task.RiskLevel
		taskID = task.TaskID
	}

	// Enrich with project name and agent info for better memory extraction.
	projectName := ""
	if proj, err := svc.Store.GetProject(ctx, projectID); err == nil && proj != nil {
		projectName = proj.Name
	}

	agentRuntime := ""
	if agent, err := svc.Store.GetAgentByRun(ctx, runID); err == nil && agent != nil {
		agentRuntime = string(agent.Runtime)
	}

	// Build a richer text that Mnemos's LLM pipeline can extract facts from.
	// The pipeline expects conversational/content text, not telegraphic logs.
	text := fmt.Sprintf(
		"Pantheon orchestration run completed in project %q (repo: %s). "+
			"Run ID: %s. Agent runtime: %s. "+
			"Objective: %s. Risk level: %s. "+
			"Result: %s (verifier accepted). "+
			"The agent executed the task and the run transitioned to completed state.",
		projectName, projectName, runID, agentRuntime,
		objective, string(risk), string(resultState))

	metadata := map[string]string{
		"run_id":     runID,
		"project_id": projectID,
		"project":    projectName,
		"task_id":    taskID,
		"runtime":    agentRuntime,
		"source":     "pantheon",
	}
	go func() {
		ictx, icancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer icancel()
		if err := svc.Mnemos.Ingest(ictx, text, metadata); err != nil {
			// Best-effort: log and continue, don't block run completion.
			// The run is already completed in the store.
			fmt.Fprintf(os.Stderr, "pantheon: mnemos ingest failed for run %s: %v\n", runID, err)
		}
	}()
}

// handleRunApprove is the human-approval method for high-risk (R2/R3) runs
// that passed verification but require human sign-off before transitioning
// to completed (risk-graded verification). The run must be in the verifying
// state with next_action=approval_required.
//
// On success the run transitions verifying → completed with
// result_state='approved' (distinct from 'accepted' so the verdict trail
// records human sign-off), next_action='none', and all nonterminal agents
// are terminalized. A run.approved event is appended with the approver
// identity and optional evidence reference.
func (svc *Service) handleRunApprove(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunApproveParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	if p.Approver == "" {
		return nil, domain.ErrInvalidInput("approver is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	if run.State != domain.RunV2Verifying {
		return nil, domain.ErrConflict(fmt.Sprintf("run.approve requires verifying state, got %s", run.State))
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	finalState, err := svc.Store.ApproveRun(ctx, p.RunID, p.Approver, p.EvidenceRef, eid)
	if err != nil {
		return nil, err
	}
	// Auto-ingest to Mnemos (best-effort, async). The run is already
	// completed in the store; ingest failure does not block completion.
	task, _ := svc.Store.GetTaskByRun(ctx, p.RunID)
	svc.ingestCompletedRun(p.RunID, run.ProjectID, domain.ResultApproved, task)
	return &RunApproveResult{
		RunID:       p.RunID,
		State:       finalState,
		ResultState: domain.ResultApproved,
	}, nil
}

// handleAgentRegister registers an agent for a run and acquires the run lease
// (control-plane §8.2). The agent is recorded and the run's lease holder and
// epoch are updated in one transaction.
func (svc *Service) handleAgentRegister(ctx context.Context, params json.RawMessage) (any, error) {
	var p AgentRegisterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" || p.Role == "" || p.Runtime == "" {
		return nil, domain.ErrInvalidInput("run_id, role, runtime are required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	aid, err := domain.NewID("agt_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}
	taskID := ""
	if task, _ := svc.Store.GetTaskByRun(ctx, p.RunID); task != nil {
		taskID = task.TaskID
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.RegisterAgent(ctx, &domain.Agent{
		AgentID:   aid,
		RunID:     p.RunID,
		TaskID:    taskID,
		Role:      p.Role,
		Runtime:   p.Runtime,
		PID:       p.PID,
		State:     domain.AgentRegistered,
		SessionID: p.SessionID,
		StartedAt: time.Now().UTC(),
	}, eid); err != nil {
		return nil, err
	}
	// Acquire the run lease for this agent.
	renewDeadline := time.Now().Add(5 * time.Minute).Unix()
	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.AcquireRunLease(ctx, p.RunID, aid, renewDeadline, eid); err != nil {
		return nil, err
	}
	return &AgentRegisterResult{AgentID: aid}, nil
}

// handleAgentHeartbeat renews the run lease for an agent.
func (svc *Service) handleAgentHeartbeat(ctx context.Context, params json.RawMessage) (any, error) {
	var p AgentHeartbeatParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.AgentID == "" {
		return nil, domain.ErrInvalidInput("agent_id is required")
	}
	agent, err := svc.Store.GetAgent(ctx, p.AgentID)
	if err != nil {
		return nil, domain.ErrInternal("get agent: " + err.Error())
	}
	if agent == nil {
		return nil, domain.ErrNotFound("agent not found: " + p.AgentID)
	}
	renewDeadline := time.Now().Add(5 * time.Minute).Unix()
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.RenewRunLease(ctx, agent.RunID, renewDeadline, eid); err != nil {
		return nil, err
	}
	return &AgentHeartbeatResult{AgentID: p.AgentID, RenewDeadline: renewDeadline}, nil
}

// handleAgentComplete marks an agent as exited with the given exit code.
func (svc *Service) handleAgentComplete(ctx context.Context, params json.RawMessage) (any, error) {
	var p AgentCompleteParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.AgentID == "" {
		return nil, domain.ErrInvalidInput("agent_id is required")
	}
	agent, err := svc.Store.GetAgent(ctx, p.AgentID)
	if err != nil {
		return nil, domain.ErrInternal("get agent: " + err.Error())
	}
	if agent == nil {
		return nil, domain.ErrNotFound("agent not found: " + p.AgentID)
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateAgentState(ctx, p.AgentID, domain.AgentExited, p.ExitCode, eid); err != nil {
		return nil, err
	}
	return &AgentCompleteResult{AgentID: p.AgentID, State: domain.AgentExited}, nil
}

// handleAgentBlock blocks an agent: marks it lost and transitions the
// associated run to the paused state (legacy equivalent of §8.1 blocked).
func (svc *Service) handleAgentBlock(ctx context.Context, params json.RawMessage) (any, error) {
	var p AgentBlockParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.AgentID == "" {
		return nil, domain.ErrInvalidInput("agent_id is required")
	}
	agent, err := svc.Store.GetAgent(ctx, p.AgentID)
	if err != nil {
		return nil, domain.ErrInternal("get agent: " + err.Error())
	}
	if agent == nil {
		return nil, domain.ErrNotFound("agent not found: " + p.AgentID)
	}
	// Mark the agent as lost.
	eid, _ := newEventID()
	_ = svc.Store.UpdateAgentState(ctx, p.AgentID, domain.AgentLost, nil, eid)
	// Transition the run to blocked (§8.1).
	run, _ := svc.Store.GetRun(ctx, agent.RunID)
	if run != nil {
		if err := domain.CheckRunTransitionV2(run.State, domain.RunV2Blocked); err == nil {
			eid, err := newEventID()
			if err != nil {
				return nil, err
			}
			if err := svc.Store.UpdateRunState(ctx, agent.RunID, domain.RunV2Blocked, eid); err != nil {
				return nil, err
			}
		}
	}
	return &AgentBlockResult{AgentID: p.AgentID, RunID: agent.RunID, State: domain.RunV2Blocked}, nil
}

// handleRunStatus returns the current run state with V2 state types.
// The Run field carries V2 states (the authoritative DB representation).
func (svc *Service) handleRunStatus(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunStatusParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	res := &RunStatusResult{Run: run}
	// Task and agent lookup are best-effort; nil is acceptable.
	if task, _ := svc.Store.GetTaskByRun(ctx, p.RunID); task != nil {
		res.Task = task
	}
	if agent, _ := svc.Store.GetAgentByRun(ctx, p.RunID); agent != nil {
		res.Agent = agent
	}
	return res, nil
}

// handleRunEvents is the v2 typed-contract method for event queries. It
// returns a bounded event list (not a stream) for a given run, ordered
// by sequence number since the given cursor.
func (svc *Service) handleRunEvents(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunEventsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	limit := p.Limit
	if limit == 0 {
		limit = 100
	}
	events, err := svc.Store.EventsSince(ctx, p.RunID, p.Cursor, limit)
	if err != nil {
		return nil, domain.ErrInternal("events query: " + err.Error())
	}
	if events == nil {
		events = []domain.Event{}
	}
	return &RunEventsResult{Events: events}, nil
}

// handleRunBlock is the v2 typed-contract method for blocking (pausing)
// a run. It stops the runtime agent, creates a checkpoint if configured,
// and transitions the run to the V2 "blocked" state. Returns V2 state
// directly (domain.RunV2Blocked).
func (svc *Service) handleRunBlock(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunBlockParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	if err := domain.CheckRunTransitionV2(run.State, domain.RunV2Blocked); err != nil {
		return nil, domain.ErrConflict(err.Error())
	}

	// Stop the runtime agent if one is running.
	agent, _ := svc.Store.GetAgentByRun(ctx, p.RunID)
	if agent != nil && agent.State == domain.AgentRunning && svc.Runtime != nil {
		if err := svc.Runtime.Stop(ctx, RuntimeHandle{
			AgentID:   agent.AgentID,
			PID:       agent.PID,
			SessionID: agent.SessionID,
		}, StopGracePeriod); err != nil {
			// Non-fatal: continue with checkpoint even if stop fails.
			_ = err
		}
		eid, _ := newEventID()
		_ = svc.Store.UpdateAgentState(ctx, agent.AgentID, domain.AgentExited, nil, eid)
	}

	// Create checkpoint via CheckpointManager if configured.
	var candidateID string
	if svc.Checkpoint.CreateCheckpoint != nil && agent != nil {
		task, _ := svc.Store.GetTask(ctx, agent.TaskID)
		worktreePath := ""
		if task != nil {
			worktreePath = task.WorktreePath
		}
		if worktreePath != "" {
			cid, err := svc.Checkpoint.CreateCheckpoint(ctx, agent.TaskID, p.RunID, worktreePath, "blocked by user")
			if err != nil {
				// Non-fatal: checkpoint failure doesn't block.
				_ = err
			} else {
				candidateID = cid
				// Persist candidate record to store.
				eid, _ := newEventID()
				_ = svc.Store.SaveCandidate(ctx, &domain.Candidate{
					CandidateID: cid,
					TaskID:      agent.TaskID,
					RunID:       p.RunID,
					RefName:     "refs/pantheon/" + cid,
					CommitSHA:   "",
					Summary:     "blocked by user",
					CreatedAt:   time.Now().UTC(),
				}, eid)
			}
		}
	}

	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Blocked, eid); err != nil {
		return nil, err
	}
	return &RunBlockResult{
		RunID:       p.RunID,
		State:       domain.RunV2Blocked,
		CandidateID: candidateID,
	}, nil
}

// handleRunUnblock is the v2 typed-contract method for unblocking
// (resuming) a blocked run. It starts a new runtime session from the
// last checkpoint, reusing the prior agent's SessionID if available,
// and transitions the run to the V2 "running" state. Returns V2 state
// directly (domain.RunV2Running).
func (svc *Service) handleRunUnblock(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunUnblockParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	if err := domain.CheckRunTransitionV2(run.State, domain.RunV2Running); err != nil {
		return nil, domain.ErrConflict(err.Error())
	}
	// Start a new runtime session from the last checkpoint.
	// Reuse the SessionID from the prior agent if available.
	var agentID string
	agent, _ := svc.Store.GetAgentByRun(ctx, p.RunID)
	if svc.Runtime != nil && agent != nil {
		task, _ := svc.Store.GetTask(ctx, agent.TaskID)
		worktreePath := ""
		objective := ""
		if task != nil {
			worktreePath = task.WorktreePath
			objective = task.Objective
		}
		if worktreePath != "" {
			h, err := svc.Runtime.Start(ctx, RuntimeStartParams{
				RunID:        p.RunID,
				TaskID:       agent.TaskID,
				WorktreePath: worktreePath,
				Objective:    objective,
				SessionID:    agent.SessionID, // resume from prior session
				Budget:       run.Budget,
			})
			if err != nil {
				eid, _ := newEventID()
				_ = svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Failed, eid)
				return nil, domain.ErrInternal("unblock runtime: " + err.Error())
			}
			agentID = h.AgentID
			eid, _ := newEventID()
			_ = svc.Store.RegisterAgent(ctx, &domain.Agent{
				AgentID:   h.AgentID,
				RunID:     p.RunID,
				TaskID:    agent.TaskID,
				Role:      domain.RoleWorker,
				Runtime:   "devin",
				PID:       h.PID,
				State:     domain.AgentRunning,
				SessionID: h.SessionID,
				StartedAt: time.Now().UTC(),
			}, eid)
		}
	}

	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Running, eid); err != nil {
		return nil, err
	}
	return &RunUnblockResult{
		RunID:   p.RunID,
		State:   domain.RunV2Running,
		AgentID: agentID,
	}, nil
}

// handleRunTerminate is the v2 typed-contract method for terminating
// (canceling) a run. It stops the runtime agent, transitions the run to
// the V2 "canceled" state, and terminalizes all nonterminal agents.
// Returns V2 state directly (domain.RunV2Canceled).
func (svc *Service) handleRunTerminate(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunTerminateParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	run, err := svc.Store.GetRun(ctx, p.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get run: " + err.Error())
	}
	if run == nil {
		return nil, domain.ErrNotFound("run not found: " + p.RunID)
	}
	if err := domain.CheckRunTransitionV2(run.State, domain.RunV2Canceled); err != nil {
		return nil, domain.ErrConflict(err.Error())
	}
	// Stop the runtime agent if one is running.
	agent, _ := svc.Store.GetAgentByRun(ctx, p.RunID)
	if agent != nil && agent.State == domain.AgentRunning && svc.Runtime != nil {
		if err := svc.Runtime.Stop(ctx, RuntimeHandle{
			AgentID:   agent.AgentID,
			PID:       agent.PID,
			SessionID: agent.SessionID,
		}, StopGracePeriod); err != nil {
			_ = err
		}
		eid, _ := newEventID()
		_ = svc.Store.UpdateAgentState(ctx, agent.AgentID, domain.AgentExited, nil, eid)
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.UpdateRunState(ctx, p.RunID, domain.RunV2Canceled, eid); err != nil {
		return nil, err
	}
	// ADR-0018 C2: terminalize all nonterminal agents of the canceled run.
	// This is a follow-up transaction (UpdateRunState already committed);
	// best-effort — a failure to terminalize does not un-cancel the run.
	_ = svc.Store.TerminalizeAgents(ctx, p.RunID, "run_canceled", eid)
	return &RunTerminateResult{
		RunID: p.RunID,
		State: domain.RunV2Canceled,
	}, nil
}

// handleRunTakeover is the v2 typed-contract method for taking over a
// candidate. It creates a new run from a candidate's commit SHA, with
// the run in the V2 "requested" state.
func (svc *Service) handleRunTakeover(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunTakeoverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.CandidateID == "" {
		return nil, domain.ErrInvalidInput("candidate_id is required")
	}
	// Use store.GetCandidate for the full candidate record (with commit_sha).
	cand, err := svc.Store.GetCandidate(ctx, p.CandidateID)
	if err != nil {
		return nil, domain.ErrInternal("get candidate: " + err.Error())
	}
	if cand == nil {
		return nil, domain.ErrNotFound("candidate not found: " + p.CandidateID)
	}

	// Resolve the workspace → project → repo_path to prepare a worktree.
	// The candidate's RunID field is used as the WorkspaceID for the new run.
	ws, err := svc.Store.GetWorkspace(ctx, cand.RunID)
	if err != nil {
		return nil, domain.ErrInternal("get workspace: " + err.Error())
	}
	var repoPath string
	if ws != nil {
		proj, err := svc.Store.GetProject(ctx, ws.ProjectID)
		if err != nil {
			return nil, domain.ErrInternal("get project: " + err.Error())
		}
		if proj != nil {
			repoPath = proj.RepoPath
		}
	}

	rid, err := domain.NewID("run_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}
	tid, err := domain.NewID("tsk_")
	if err != nil {
		return nil, domain.ErrInternal("id: " + err.Error())
	}

	// Prepare a worktree from the candidate's commit SHA, if possible.
	worktreePath := ""
	if svc.WorkspaceMgr != nil && repoPath != "" && cand.CommitSHA != "" {
		wp, err := svc.WorkspaceMgr.PrepareWorktree(ctx, repoPath, cand.CommitSHA, tid)
		if err != nil {
			// Non-fatal: the run is created but without a worktree.
			// The user can manually prepare one or the run will fail on start.
			_ = err
		} else {
			worktreePath = wp
		}
	}

	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.CreateRun(ctx, &domain.Run{
		RunID:       rid,
		WorkspaceID: cand.RunID,
		ProjectID:   ws.ProjectID,
		Owner:       ws.Owner,
		BaseCommit:  cand.CommitSHA,
		Budget:      DefaultBudget,
		State:       domain.RunV2Requested,
	}, eid); err != nil {
		return nil, err
	}
	eid, err = newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.CreateTask(ctx, &domain.Task{
		TaskID:       tid,
		RunID:        rid,
		Objective:    p.Objective,
		WorktreePath: worktreePath,
		State:        domain.TaskDraft,
		CreatedAt:    time.Now().UTC(),
	}, eid); err != nil {
		return nil, err
	}
	return &RunTakeoverResult{RunID: rid, TaskID: tid}, nil
}

// handleRunList is the v2 typed-contract method for listing runs. It
// returns runs with V2 states directly (no legacy translation). An
// optional state filter accepts a V2 state string.
func (svc *Service) handleRunList(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunListParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
		}
	}
	runs, err := svc.Store.ListRuns(ctx, string(p.State))
	if err != nil {
		return nil, domain.ErrInternal("list runs: " + err.Error())
	}
	if runs == nil {
		runs = []*domain.Run{}
	}
	return &RunListResult{Runs: runs}, nil
}

// handleMessagePublishEnvelope handles the v1.1 typed message publish.
// It accepts a MessagePublishEnvelopeParams, validates the envelope,
// delegates to Store.PublishMessageEnvelope for idempotency dedup and
// per-Run seq assignment, and returns the assigned seqs.
func (svc *Service) handleMessagePublishEnvelope(ctx context.Context, params json.RawMessage) (any, error) {
	var p MessagePublishEnvelopeParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	msg := domain.Message{
		MessageID:      p.MessageID,
		RunID:          p.RunID,
		TaskID:         p.TaskID,
		Sender:         p.Sender,
		Recipient:      p.Recipient,
		Type:           p.Type,
		TTL:            p.TTL,
		IdempotencyKey: p.IdempotencyKey,
		PayloadRef:     p.PayloadRef,
		Sensitivity:    p.Sensitivity,
		CreatedAt:      time.Now().UTC(),
	}
	if msg.MessageID == "" {
		id, err := domain.NewID("msg_")
		if err != nil {
			return nil, domain.ErrInternal("generate message_id: " + err.Error())
		}
		msg.MessageID = id
	}
	if msg.IdempotencyKey == "" {
		msg.IdempotencyKey = msg.MessageID
	}

	seq, msgSeq, msgID, err := svc.Store.PublishMessageEnvelope(ctx, &msg)
	if err != nil {
		return nil, domain.ErrInternal("publish envelope: " + err.Error())
	}

	// Best-effort tmux notification if a target agent is specified.
	if p.Recipient.AgentID != "" && svc.Notifier != nil {
		_ = svc.Notifier.Notify(ctx, p.Recipient.AgentID, p.PayloadRef.Inline)
	}

	// C-004: best-effort inbox/outbox projection.
	// Restricted messages are not projected to files.
	if svc.InboxProjector != nil && msg.Sensitivity != domain.SensRestricted {
		_ = svc.InboxProjector.Project(ctx, &msg)
	}

	// Solution B: best-effort real-time push notification. The push layer
	// is on top of the durable SQLite journal — it notifies subscribers but
	// does not replace the pull-based cursor path. Deduped publishes still
	// notify so reconnecting subscribers learn the latest seq. A nil Pusher
	// (push disabled) is treated as NoopPusher.
	if svc.Pusher != nil {
		svc.Pusher.NotifyMessage(p.RunID, msgSeq, eventIDFromMessageID(msgID))
	}

	deduped := msgID != msg.MessageID
	return &MessagePublishEnvelopeResult{
		Seq:        seq,
		MessageSeq: msgSeq,
		MessageID:  msgID,
		Deduped:    deduped,
	}, nil
}

// handleMessagesByRun handles the v1.1 typed message query by run_id.
func (svc *Service) handleMessagesByRun(ctx context.Context, params json.RawMessage) (any, error) {
	var p MessagesByRunParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	events, err := svc.Store.MessagesByRun(ctx, p.RunID, p.Cursor, p.Limit)
	if err != nil {
		return nil, domain.ErrInternal("messages by run: " + err.Error())
	}
	if events == nil {
		events = []domain.Event{}
	}
	var nextCursor int64
	if len(events) > 0 {
		nextCursor = events[len(events)-1].MessageSeq
	}
	return &MessagesByRunResult{Messages: events, NextCursor: nextCursor}, nil
}

// handleMessageAck handles the v1.1 message.ack RPC (C-002).
// Marks a message as acked. Idempotent — acking an already-acked message
// is a no-op.
func (svc *Service) handleMessageAck(ctx context.Context, params json.RawMessage) (any, error) {
	var p MessageAckParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.MessageID == "" {
		return nil, domain.ErrInvalidInput("message_id is required")
	}
	if err := svc.Store.AckMessage(ctx, p.MessageID, p.AgentID); err != nil {
		return nil, err
	}
	return &MessageAckResult{Acked: true}, nil
}

// handleMessageNack handles the v1.1 message.nack RPC (C-002).
// Marks a message as nacked, increments retry_count. If retry_count
// reaches MaxRetries, the message is marked 'dead'.
func (svc *Service) handleMessageNack(ctx context.Context, params json.RawMessage) (any, error) {
	var p MessageNackParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.MessageID == "" {
		return nil, domain.ErrInvalidInput("message_id is required")
	}
	retryCount, finalState, err := svc.Store.NackMessage(ctx, p.MessageID, p.AgentID, p.Reason)
	if err != nil {
		return nil, err
	}
	return &MessageNackResult{RetryCount: retryCount, FinalState: finalState}, nil
}

// handleMessagesDeadlineCheck handles the v1.1 messages.deadline_check RPC (C-002).
// Finds all pending messages whose TTL has expired and marks them as 'expired'.
func (svc *Service) handleMessagesDeadlineCheck(ctx context.Context, params json.RawMessage) (any, error) {
	expired, err := svc.Store.ExpireMessages(ctx, time.Now().UTC())
	if err != nil {
		return nil, domain.ErrInternal("deadline check: " + err.Error())
	}
	if expired == nil {
		expired = []string{}
	}
	return &MessagesDeadlineCheckResult{ExpiredMessageIDs: expired}, nil
}

// handleMessagesStatus handles the v1.1 messages.status RPC (C-002).
// Returns the current delivery state of a message.
func (svc *Service) handleMessagesStatus(ctx context.Context, params json.RawMessage) (any, error) {
	var p MessagesStatusParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.MessageID == "" {
		return nil, domain.ErrInvalidInput("message_id is required")
	}
	st, err := svc.Store.GetMessageStatus(ctx, p.MessageID)
	if err != nil {
		return nil, err
	}
	return &MessagesStatusResult{
		MessageID:  st.MessageID,
		AckState:   st.AckState,
		RetryCount: st.RetryCount,
		IsDead:     st.IsDead,
		IsExpired:  st.IsExpired,
	}, nil
}

// handleReconcileCrash is the v2 typed-contract method for crash
// reconciliation. It marks lost agents and failed runs after a daemon
// restart (replaces the legacy "reconcile" method).
func (svc *Service) handleReconcileCrash(ctx context.Context, params json.RawMessage) (any, error) {
	reconciled, err := svc.Store.ReconcileAfterCrash(ctx)
	if err != nil {
		return nil, domain.ErrInternal("reconcile: " + err.Error())
	}
	return &ReconcileResult{Reconciled: reconciled}, nil
}

// --- Continuation slice (ADR-0017) ---

// handleContinuationRegister is the explicit PM/operator action to register a
// continuation need for a run that requires an explicit successor. Idempotent:
// the same run_id + successor_objective is a no-op if an active continuation
// already exists (the existing continuation_id is returned).
func (svc *Service) handleContinuationRegister(ctx context.Context, params json.RawMessage) (any, error) {
	var p ContinuationRegisterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" || p.SuccessorObjective == "" {
		return nil, domain.ErrInvalidInput("run_id and successor_objective are required")
	}
	// Look up the run to populate project_id/owner (best-effort; the run may
	// already be in a terminal state, which is exactly when a continuation
	// is registered).
	var projectID, owner string
	if run, _ := svc.Store.GetRun(ctx, p.RunID); run != nil {
		projectID = run.ProjectID
		owner = run.Owner
	}
	if p.Owner != "" {
		owner = p.Owner
	}
	cid, err := domain.NewID("cont_")
	if err != nil {
		return nil, domain.ErrInternal("id generation: " + err.Error())
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	c := &domain.Continuation{
		ContinuationID:     cid,
		RunID:              p.RunID,
		ProjectID:          projectID,
		Owner:              owner,
		SuccessorObjective: p.SuccessorObjective,
	}
	if err := svc.Store.RegisterContinuation(ctx, c, eid); err != nil {
		return nil, err
	}
	return &ContinuationRegisterResult{ContinuationID: c.ContinuationID}, nil
}

// handleContinuationList lists continuations, optionally filtered by state.
func (svc *Service) handleContinuationList(ctx context.Context, params json.RawMessage) (any, error) {
	var p ContinuationListParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
		}
	}
	list, err := svc.Store.ListContinuations(ctx, p.StateFilter)
	if err != nil {
		return nil, domain.ErrInternal("list continuations: " + err.Error())
	}
	if list == nil {
		list = []*domain.Continuation{}
	}
	return &ContinuationListResult{Continuations: list}, nil
}

// handleContinuationFulfill is the explicit PM action to link a successor run
// and mark the continuation fulfilled. NO implicit successor creation happens
// here — the successor_run_id must already exist (created by run.create or
// run.submit).
func (svc *Service) handleContinuationFulfill(ctx context.Context, params json.RawMessage) (any, error) {
	var p ContinuationFulfillParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.ContinuationID == "" || p.SuccessorRunID == "" {
		return nil, domain.ErrInvalidInput("continuation_id and successor_run_id are required")
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.FulfillContinuation(ctx, p.ContinuationID, p.SuccessorRunID, eid); err != nil {
		return nil, err
	}
	c, err := svc.Store.GetContinuation(ctx, p.ContinuationID)
	if err != nil {
		return nil, domain.ErrInternal("get continuation after fulfill: " + err.Error())
	}
	if c == nil {
		return nil, domain.ErrNotFound("continuation not found after fulfill: " + p.ContinuationID)
	}
	return &ContinuationFulfillResult{
		ContinuationID: c.ContinuationID,
		State:          c.State,
		SuccessorRunID: c.SuccessorRunID,
	}, nil
}

// handleContinuationCancel is the explicit PM action to cancel a continuation
// (the PM decided no successor is needed).
func (svc *Service) handleContinuationCancel(ctx context.Context, params json.RawMessage) (any, error) {
	var p ContinuationCancelParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.ContinuationID == "" {
		return nil, domain.ErrInvalidInput("continuation_id is required")
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.CancelContinuation(ctx, p.ContinuationID, eid); err != nil {
		return nil, err
	}
	c, err := svc.Store.GetContinuation(ctx, p.ContinuationID)
	if err != nil {
		return nil, domain.ErrInternal("get continuation after cancel: " + err.Error())
	}
	if c == nil {
		return nil, domain.ErrNotFound("continuation not found after cancel: " + p.ContinuationID)
	}
	return &ContinuationCancelResult{
		ContinuationID: c.ContinuationID,
		State:          c.State,
	}, nil
}

// handleReconcileContinuations triggers the continuation reconcile tick
// manually. It returns the reconcile result with checked/notified/orphaned
// counts. This is the same tick the daemon runs periodically; invoking it
// manually is useful for testing and for PM-initiated sweeps.
func (svc *Service) handleReconcileContinuations(ctx context.Context, params json.RawMessage) (any, error) {
	var p ReconcileContinuationsParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
		}
	}
	wakeGap := time.Hour
	if p.WakeGapSeconds > 0 {
		wakeGap = time.Duration(p.WakeGapSeconds) * time.Second
	}
	rec := wake.NewReconciler(svc.Store, svc.Store, nil, wakeGap)
	result, err := rec.Tick(ctx)
	if err != nil {
		return nil, domain.ErrInternal("reconcile tick: " + err.Error())
	}
	return &ReconcileContinuationsResult{
		Checked:       result.Checked,
		Notified:      result.Notified,
		ReNotified:    result.ReNotified,
		Skipped:       result.Skipped,
		Errors:        result.Errors,
		OrphanedRuns:  result.OrphanedRuns,
		Notifications: result.Notifications,
	}, nil
}

// --- helpers ---

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// --- Terminal-state consistency slice (ADR-0018) ---

// handleRunSupersede is the explicit PM/operator action to link an old run
// to its successor (ADR-0018, C3). The old run's state is NOT changed —
// supersede is a link, not a state transition. One successor per old run.
func (svc *Service) handleRunSupersede(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunSupersedeParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	rec, err := svc.Store.SupersedeRun(ctx, p.OldRunID, p.SuccessorRunID, p.Reason)
	if err != nil {
		return nil, err
	}
	return &RunSupersedeResult{
		SupersedeID:    rec.SupersedeID,
		OldRunID:       rec.OldRunID,
		SuccessorRunID: rec.SuccessorRunID,
		Reason:         rec.Reason,
	}, nil
}

// handleRunSetNextAction is the explicit PM action to set or change the
// next_action decision on a run (ADR-0018, C4). Idempotent: calling it
// twice updates the value.
func (svc *Service) handleRunSetNextAction(ctx context.Context, params json.RawMessage) (any, error) {
	var p RunSetNextActionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.RunID == "" {
		return nil, domain.ErrInvalidInput("run_id is required")
	}
	if p.NextAction == "" {
		return nil, domain.ErrInvalidInput("next_action is required")
	}
	eid, err := newEventID()
	if err != nil {
		return nil, err
	}
	if err := svc.Store.SetNextAction(ctx, p.RunID, p.NextAction, eid); err != nil {
		return nil, err
	}
	return &RunSetNextActionResult{RunID: p.RunID, NextAction: p.NextAction}, nil
}

// handleReconcileTerminalState is the manual trigger of the terminal-state
// reconcile sweep (ADR-0018). It surfaces:
//   - terminal runs with missing next_action decisions (the "missing
//     decision" case)
//   - terminal runs with stale (nonterminal) agents
//   - superseded runs (old run → successor link)
//
// It does NOT auto-terminalize or auto-set next_action — it only surfaces.
// The PM must act explicitly.
func (svc *Service) handleReconcileTerminalState(ctx context.Context, params json.RawMessage) (any, error) {
	missing, err := svc.Store.ListMissingNextAction(ctx)
	if err != nil {
		return nil, domain.ErrInternal("list missing next_action: " + err.Error())
	}
	stale, err := svc.Store.ListTerminalRunsWithStaleAgents(ctx)
	if err != nil {
		return nil, domain.ErrInternal("list stale agents: " + err.Error())
	}
	superseded, err := svc.Store.ListSupersededRuns(ctx)
	if err != nil {
		return nil, domain.ErrInternal("list superseded runs: " + err.Error())
	}
	return &ReconcileTerminalStateResult{
		MissingNextAction: missing,
		StaleAgents:       stale,
		Superseded:        superseded,
	}, nil
}

// AutoContinue creates and starts a continuation run for the given run ID.
// It reuses the previous run's worktree and objective. This is called by
// the agent liveness scanner when an agent exits with remaining subtasks.
//
// Before creating the successor, AutoContinue evaluates the same-root-cause
// circuit breaker (ROADMAP "同根因 3 次熔断"): it derives the root cause of
// the current continuation from the agent's exit code, counts how many times
// that root cause has already occurred in the run chain, and — if the total
// (including the current occurrence) would reach MaxRootCauseBreaker — blocks
// the run instead of continuing. This is an additional check on top of the
// scanner's progress gate; it does not change the progress gate logic.
func (svc *Service) AutoContinue(ctx context.Context, prevRunID string) (string, error) {
	prevRun, err := svc.Store.GetRun(ctx, prevRunID)
	if err != nil {
		return "", domain.ErrInternal("auto-continue: get run: " + err.Error())
	}
	if prevRun == nil {
		return "", domain.ErrNotFound("auto-continue: run not found: " + prevRunID)
	}

	prevTask, err := svc.Store.GetTaskByRun(ctx, prevRunID)
	if err != nil {
		return "", domain.ErrInternal("auto-continue: get task: " + err.Error())
	}
	if prevTask == nil {
		return "", domain.ErrInvalidInput("auto-continue: no task for run")
	}

	// --- Same-root-cause circuit breaker ---
	// Derive the root cause of this continuation from the agent's exit code.
	// The agent exited (the scanner only calls AutoContinue on exited agents
	// with remaining subtasks), so a non-zero exit is a failure and a zero
	// exit with remaining subtasks is "incomplete".
	rootCause := deriveRootCause(svc.lookupAgentExitCode(ctx, prevRunID))

	// Count how many times this root cause has already occurred in the run
	// chain (traced back via continuation successor_run_id links). The
	// current occurrence is the (count+1)-th; if that reaches the threshold,
	// trip the breaker.
	existing, err := svc.Store.CountRootCauseInChain(ctx, prevRunID, rootCause)
	if err != nil {
		return "", domain.ErrInternal("auto-continue: count root cause: " + err.Error())
	}
	if existing+1 >= MaxRootCauseBreaker {
		log.Printf("auto-continue: circuit breaker tripped: root cause %q occurred %d times in chain (run %s)",
			rootCause, existing+1, prevRunID)
		return "", svc.tripCircuitBreaker(ctx, prevRunID, rootCause, existing+1)
	}

	// Create continuation run via the same logic as run.create.
	createParams, err := json.Marshal(RunCreateParams{
		ProjectID:    prevRun.ProjectID,
		Objective:    prevTask.Objective,
		Budget:       prevRun.Budget,
		Owner:        prevRun.Owner,
		ContinueFrom: prevRunID,
	})
	if err != nil {
		return "", domain.ErrInternal("auto-continue: marshal params: " + err.Error())
	}

	createResult, err := svc.handleRunCreate(ctx, createParams)
	if err != nil {
		return "", err
	}
	cr := createResult.(*RunCreateResult)
	newRunID := cr.RunID

	// Start the continuation run.
	startParams, err := json.Marshal(RunStartParams{RunID: newRunID})
	if err != nil {
		return "", domain.ErrInternal("auto-continue: marshal start params: " + err.Error())
	}
	if _, err := svc.handleRunStart(ctx, startParams); err != nil {
		return "", err
	}

	// Durably record this auto-continuation with its root cause so the
	// breaker can count it on the next cycle.
	contID, err := domain.NewID("cont_")
	if err != nil {
		return "", domain.ErrInternal("auto-continue: continuation id: " + err.Error())
	}
	eid, err := newEventID()
	if err != nil {
		return "", err
	}
	if err := svc.Store.RecordAutoContinuation(ctx, &domain.Continuation{
		ContinuationID:     contID,
		RunID:              prevRunID,
		ProjectID:          prevRun.ProjectID,
		Owner:              prevRun.Owner,
		SuccessorObjective: prevTask.Objective,
		RootCause:          rootCause,
	}, newRunID, eid); err != nil {
		return "", domain.ErrInternal("auto-continue: record continuation: " + err.Error())
	}

	return newRunID, nil
}

// lookupAgentExitCode returns the exit code of the most recent agent for the
// run, or nil if no agent / no exit code is recorded.
func (svc *Service) lookupAgentExitCode(ctx context.Context, runID string) *int {
	agent, err := svc.Store.GetAgentByRun(ctx, runID)
	if err != nil || agent == nil {
		return nil
	}
	return agent.ExitCode
}

// deriveRootCause maps an agent exit code to a root-cause label for the
// circuit breaker. A nil exit code (agent not found or not yet recorded)
// yields "exit_code_unknown". Exit 0 with remaining subtasks yields
// "exit_code_0_incomplete". A non-zero exit N yields "exit_code_N".
func deriveRootCause(exitCode *int) string {
	if exitCode == nil {
		return "exit_code_unknown"
	}
	if *exitCode == 0 {
		return "exit_code_0_incomplete"
	}
	return fmt.Sprintf("exit_code_%d", *exitCode)
}

// tripCircuitBreaker transitions the run to blocked and publishes a block
// message to the PM message queue about the circuit breaker trip. It returns
// a domain.Error so AutoContinue's caller sees a structured failure.
func (svc *Service) tripCircuitBreaker(ctx context.Context, runID, rootCause string, count int) error {
	// Transition the run to blocked (allowed from running/ready/planning/
	// requested per the §8.1 state machine).
	eid, err := newEventID()
	if err != nil {
		return domain.ErrInternal("circuit breaker: event id: " + err.Error())
	}
	if err := svc.Store.UpdateRunState(ctx, runID, domain.RunV2Blocked, eid); err != nil {
		return domain.ErrInternal("circuit breaker: block run: " + err.Error())
	}

	// Publish a block message to the PM message queue so the PM is notified
	// that the run needs human attention. Best-effort — a publish failure is
	// logged but does not undo the block.
	msgID, err := domain.NewID("msg_")
	if err != nil {
		log.Printf("circuit breaker: generate message id: %v", err)
	} else {
		body := fmt.Sprintf("circuit breaker tripped: root cause %q occurred %d times in chain (run %s) — needs human decision",
			rootCause, count, runID)
		_, msgSeq, pubMsgID, perr := svc.Store.PublishMessageEnvelope(ctx, &domain.Message{
			MessageID:      msgID,
			RunID:          runID,
			Sender:         domain.MessageEndpoint{Role: domain.RoleController},
			Recipient:      domain.MessageEndpoint{Role: domain.RolePM},
			Type:           domain.MsgBlock,
			IdempotencyKey: msgID,
			PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: body},
			Sensitivity:    domain.SensNormal,
			CreatedAt:      time.Now().UTC(),
		})
		if perr != nil {
			log.Printf("circuit breaker: publish block message: %v", perr)
		} else if svc.Pusher != nil {
			// Solution B: notify push subscribers of the block message.
			svc.Pusher.NotifyMessage(runID, msgSeq, eventIDFromMessageID(pubMsgID))
		}
	}

	return domain.ErrConflict(fmt.Sprintf(
		"circuit breaker tripped: root cause %q occurred %d times in chain (run %s)",
		rootCause, count, runID))
}

// --- Beacon discovery + Hydra model routing (optional integrations) ---

// handleAgentDiscover queries Beacon for active agent sessions across tmux
// panes. When Beacon is not configured (svc.Beacon is nil), it returns a
// clear "beacon not configured" error so callers can surface degraded mode.
// An optional agent_type filter (devin/claude/codex) narrows the result.
func (svc *Service) handleAgentDiscover(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Beacon == nil {
		return nil, domain.ErrUnsupported("beacon not configured")
	}
	var p AgentDiscoverParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
		}
	}
	sessions, err := svc.Beacon.DiscoverAgents(ctx)
	if err != nil {
		return nil, domain.ErrInternal("beacon discover: " + err.Error())
	}
	sessions = beacon.FilterByAgentType(sessions, p.AgentType)
	if sessions == nil {
		sessions = []beacon.AgentSession{}
	}
	return &AgentDiscoverResult{
		Sessions: sessions,
		Count:    len(sessions),
	}, nil
}

// handleHydraModels lists available models from the Hydra LLM gateway. When
// Hydra is not configured (svc.Hydra is nil), it returns a clear "hydra not
// configured" error so callers can surface degraded mode.
func (svc *Service) handleHydraModels(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Hydra == nil {
		return nil, domain.ErrUnsupported("hydra not configured")
	}
	models, err := svc.Hydra.ListModels(ctx)
	if err != nil {
		return nil, domain.ErrInternal("hydra list models: " + err.Error())
	}
	if models == nil {
		models = []hydra.Model{}
	}
	return &HydraModelsResult{Models: models}, nil
}

// handleHydraHealth checks whether the Hydra gateway is reachable and
// healthy. When Hydra is not configured (svc.Hydra is nil), it returns a
// clear "hydra not configured" error so callers can surface degraded mode.
func (svc *Service) handleHydraHealth(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Hydra == nil {
		return nil, domain.ErrUnsupported("hydra not configured")
	}
	if err := svc.Hydra.Healthz(ctx); err != nil {
		return &HydraHealthResult{Healthy: false}, nil
	}
	return &HydraHealthResult{Healthy: true}, nil
}

// --- Global Auditor (Phase 4) ---

// handleAuditorAudit triggers one audit cycle. The auditor analyzes recent
// run history and produces structured findings (recommendations, memory
// candidates, policy proposals, risk findings). Findings are always pending
// until human review — the auditor does NOT auto-modify anything. When the
// auditor is not configured (svc.Auditor is nil), returns a clear "auditor
// not configured" error so callers can surface degraded mode.
func (svc *Service) handleAuditorAudit(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Auditor == nil {
		return nil, domain.ErrUnsupported("auditor not configured")
	}
	result, err := svc.Auditor.Audit(ctx)
	if err != nil {
		return nil, domain.ErrInternal("auditor audit: " + err.Error())
	}
	return &AuditorAuditResult{
		RunsAnalyzed:     result.RunsAnalyzed,
		FindingsProduced: result.FindingsProduced,
		Findings:         result.Findings,
	}, nil
}

// handleAuditorFindings lists auditor findings, optionally filtered by
// status (pending/accepted/rejected). When the auditor is not configured,
// returns "auditor not configured".
func (svc *Service) handleAuditorFindings(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Auditor == nil {
		return nil, domain.ErrUnsupported("auditor not configured")
	}
	var p AuditorFindingsParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
		}
	}
	findings, err := svc.Store.ListFindings(ctx, p.Status, p.Limit)
	if err != nil {
		return nil, domain.ErrInternal("list findings: " + err.Error())
	}
	if findings == nil {
		findings = []*domain.Finding{}
	}
	return &AuditorFindingsResult{Findings: findings}, nil
}

// handleAuditorReview records a human accept/reject decision on a finding.
// This is the only path to a non-pending status — the auditor never
// auto-accepts. When the auditor is not configured, returns "auditor not
// configured".
func (svc *Service) handleAuditorReview(ctx context.Context, params json.RawMessage) (any, error) {
	if svc.Auditor == nil {
		return nil, domain.ErrUnsupported("auditor not configured")
	}
	var p AuditorReviewParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, domain.ErrInvalidInput("invalid params: " + err.Error())
	}
	if p.FindingID == "" {
		return nil, domain.ErrInvalidInput("finding_id is required")
	}
	if p.Status != string(domain.FindingAccepted) && p.Status != string(domain.FindingRejected) {
		return nil, domain.ErrInvalidInput("status must be accepted or rejected")
	}
	if err := svc.Store.ReviewFinding(ctx, p.FindingID, p.Status, p.ReviewedBy); err != nil {
		return nil, err
	}
	return &AuditorReviewResult{FindingID: p.FindingID, Status: p.Status}, nil
}
