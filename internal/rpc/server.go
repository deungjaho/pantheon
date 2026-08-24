// Package rpc implements line-delimited JSON-RPC 2.0 over stdio for the
// pantheond daemon. Each line on stdin is one request (or notification);
// each line on stdout is one response or server-pushed notification.
package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tangtszho/pantheon/internal/beacon"
	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/hydra"
	"github.com/tangtszho/pantheon/internal/store"
)

// JSON-RPC 2.0 wire types.

type Request struct {
	JSONRPC   string          `json:"jsonrpc"`
	ID        json.RawMessage `json:"id,omitempty"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params,omitempty"`
	RequestID string          `json:"request_id,omitempty"` // Pantheon idempotency key
}

type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *domain.Error   `json:"error,omitempty"`
}

// Notification is a server-pushed message (no id).
type Notification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Handler is a function that processes one RPC method.
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// MaxSSHRequestSize is the maximum request line size for SSH stdio mode (C-004).
// Requests exceeding this are rejected to prevent abuse over the SSH boundary.
const MaxSSHRequestSize = 64 * 1024 // 64 KB

// IdempotencyStore is the interface for request_id-based response caching
// (C-004). This enables cross-SSH-boundary idempotency: a retried request
// with the same request_id returns the cached response without re-executing.
type IdempotencyStore interface {
	GetCachedResponse(ctx context.Context, requestID string) (json.RawMessage, bool, error)
	CacheResponse(ctx context.Context, requestID string, resp json.RawMessage) error
}

// Server reads JSON-RPC requests from r and writes responses/notifications
// to w. It is safe for concurrent use.
type Server struct {
	handlers    map[string]Handler
	mu          sync.RWMutex
	w           io.Writer
	writeMu     sync.Mutex
	idempotency IdempotencyStore // C-004: optional, nil = no caching
	maxLineSize int              // C-004: max request line size, 0 = default 1MiB
}

func NewServer(w io.Writer) *Server {
	return &Server{
		handlers:    make(map[string]Handler),
		w:           w,
		maxLineSize: 1 << 20, // 1 MiB default
	}
}

// SetIdempotencyStore enables request_id-based response caching (C-004).
// Must be called before Serve. Pass nil to disable.
func (s *Server) SetIdempotencyStore(store IdempotencyStore) {
	s.idempotency = store
}

// SetMaxLineSize sets the maximum request line size in bytes (C-004).
// For SSH stdio mode, use MaxSSHRequestSize (64KB).
// Must be called before Serve.
func (s *Server) SetMaxLineSize(size int) {
	s.maxLineSize = size
}

// Register registers a method handler.
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// writeLine writes one JSON object followed by a newline. Thread-safe.
func (s *Server) writeLine(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("rpc: marshal: %w", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := fmt.Fprintf(s.w, "%s\n", data); err != nil {
		return fmt.Errorf("rpc: write: %w", err)
	}
	return nil
}

// SendNotification pushes a server-to-client notification.
func (s *Server) SendNotification(method string, params any) error {
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("rpc: marshal notification: %w", err)
		}
		raw = b
	}
	return s.writeLine(&Notification{
		JSONRPC: "2.0",
		Method:  method,
		Params:  raw,
	})
}

// Serve reads requests from r until EOF or ctx is done. Each request is
// handled; responses are written to the server's writer (set via NewServer).
// This is the stdin/stdout mode for per-request SSH invocation.
func (s *Server) Serve(ctx context.Context, r io.Reader) error {
	return s.serveConn(ctx, r, s.w)
}

// ServeConn reads requests from r and writes responses to w. This is used
// for Unix socket connections where each connection has its own writer.
// The handler set is shared across all connections (thread-safe via mu).
func (s *Server) ServeConn(ctx context.Context, r io.Reader, w io.Writer) error {
	return s.serveConn(ctx, r, w)
}

// serveConn is the shared implementation for Serve and ServeConn.
func (s *Server) serveConn(ctx context.Context, r io.Reader, w io.Writer) error {
	cw := &connWriter{w: w, mu: &sync.Mutex{}}
	scanner := bufio.NewScanner(r)
	// Use configured max line size (C-004: 64KB for SSH, 1MiB default).
	maxLine := s.maxLineSize
	if maxLine <= 0 {
		maxLine = 1 << 20
	}
	scanner.Buffer(make([]byte, 0, 4096), maxLine)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// C-004: enforce request size limit.
		if len(line) > maxLine {
			cw.writeLine(&Response{
				JSONRPC: "2.0",
				Error:   domain.ErrInvalidInput(fmt.Sprintf("request too large: %d bytes (max %d)", len(line), maxLine)),
			})
			continue
		}
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			cw.writeLine(&Response{
				JSONRPC: "2.0",
				Error:   domain.ErrInvalidInput("malformed request: " + err.Error()),
			})
			continue
		}
		if req.JSONRPC != "2.0" {
			cw.writeLine(&Response{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   domain.ErrInvalidInput("jsonrpc must be \"2.0\""),
			})
			continue
		}
		// Notifications (no id) are not accepted from clients in Phase 1.
		if len(req.ID) == 0 {
			cw.writeLine(&Response{
				JSONRPC: "2.0",
				Error:   domain.ErrInvalidInput("client notifications not supported"),
			})
			continue
		}
		s.handleConn(ctx, &req, cw)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("rpc: scanner: %w", err)
	}
	return nil
}

// connWriter is a per-connection writer with its own mutex.
type connWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (cw *connWriter) writeLine(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("rpc: marshal: %w", err)
	}
	cw.mu.Lock()
	defer cw.mu.Unlock()
	if _, err := fmt.Fprintf(cw.w, "%s\n", data); err != nil {
		return fmt.Errorf("rpc: write: %w", err)
	}
	return nil
}

func (s *Server) handle(ctx context.Context, req *Request) {
	s.handleConn(ctx, req, &connWriter{w: s.w, mu: &s.writeMu})
}

// handleConn dispatches a request to the registered handler and writes the
// response via the provided connWriter. If an idempotency store is configured
// and the request has a request_id, it checks the cache first and caches the
// response after successful execution (C-004).
func (s *Server) handleConn(ctx context.Context, req *Request, cw *connWriter) {
	// C-004: check idempotency cache for retried requests.
	if s.idempotency != nil && req.RequestID != "" {
		cached, ok, err := s.idempotency.GetCachedResponse(ctx, req.RequestID)
		if err == nil && ok {
			// Return cached response without re-executing.
			var cachedResp Response
			if json.Unmarshal(cached, &cachedResp) == nil {
				cachedResp.ID = req.ID // preserve original ID
				cw.writeLine(&cachedResp)
				return
			}
		}
	}

	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()
	if !ok {
		cw.writeLine(&Response{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   domain.ErrNotFound("method not found: " + req.Method),
		})
		return
	}
	result, err := h(ctx, req.Params)
	if err != nil {
		cw.writeLine(&Response{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   domain.AsError(err),
		})
		return
	}
	var raw json.RawMessage
	if result != nil {
		b, mErr := json.Marshal(result)
		if mErr != nil {
			cw.writeLine(&Response{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   domain.ErrInternal("marshal result: " + mErr.Error()),
			})
			return
		}
		raw = b
	}
	resp := Response{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  raw,
	}

	// C-004: cache the response for retried requests.
	if s.idempotency != nil && req.RequestID != "" {
		if respBytes, mErr := json.Marshal(resp); mErr == nil {
			_ = s.idempotency.CacheResponse(ctx, req.RequestID, respBytes)
		}
	}

	cw.writeLine(&resp)
}

// --- Standard params/results ---

type InitializeParams struct {
	ClientName    string `json:"client_name"`
	ClientVersion string `json:"client_version"`
}

type InitializeResult struct {
	ServerName    string   `json:"server_name"`
	ServerVersion string   `json:"server_version"`
	Protocol      int      `json:"protocol"`
	Capabilities  []string `json:"capabilities"`
}

type ProjectRegisterParams struct {
	Name     string `json:"name"`
	RepoPath string `json:"repo_path"`
	BaseRef  string `json:"base_ref"`
}

type ProjectRegisterResult struct {
	ProjectID string `json:"project_id"`
}

// ProjectListResult is the typed result of project.list.
type ProjectListResult struct {
	Projects []*domain.Project `json:"projects"`
}

// ProjectStatusParams is the typed input for project.status.
type ProjectStatusParams struct {
	ProjectID string `json:"project_id"`
}

// ProjectStatusResult is the typed result of project.status.
type ProjectStatusResult struct {
	Project *domain.Project `json:"project"`
}

// RunCreateParams is the typed input for run.create (control-plane §8.2).
// Unlike the legacy run.submit, run.create only creates the run; run.start
// begins execution.
//
// ContinueFrom is optional: if set to a previous run_id, the new run reuses
// the previous run's worktree (preserving code changes and PANTHEON_PROGRESS.md)
// instead of creating a fresh one. This enables the checkpoint-continuation
// pattern for long tasks.
type RunCreateParams struct {
	ProjectID    string           `json:"project_id"`
	Objective    string           `json:"objective"`
	BaseRef      string           `json:"base_ref,omitempty"`
	Budget       time.Duration    `json:"budget,omitempty"`
	Name         string           `json:"name,omitempty"`
	Owner        string           `json:"owner,omitempty"`
	Scope        domain.TaskScope `json:"scope,omitempty"`
	ContinueFrom string           `json:"continue_from,omitempty"`

	// TaskSpec fields (Phase 2 P3+, risk-graded verification):
	// AcceptanceCriteria/Constraints/Deliverables are stored on the task
	// and surfaced to the verifier; RiskLevel (R0-R3) drives the
	// verification gate. RiskLevel defaults to R2 (medium) when empty or
	// invalid — a safe default requiring human approval.
	AcceptanceCriteria []string `json:"acceptance_criteria,omitempty"`
	Constraints        []string `json:"constraints,omitempty"`
	Deliverables       []string `json:"deliverables,omitempty"`
	RiskLevel          string   `json:"risk_level,omitempty"` // R0-R3, default R2
}

// RunCreateResult is the typed result of run.create.
type RunCreateResult struct {
	WorkspaceID string `json:"workspace_id"`
	RunID       string `json:"run_id"`
	TaskID      string `json:"task_id"`
}

// RunStartParams is the typed input for run.start.
type RunStartParams struct {
	RunID string `json:"run_id"`
}

// RunStartResult is the typed result of run.start.
type RunStartResult struct {
	RunID   string            `json:"run_id"`
	State   domain.RunStateV2 `json:"state"`
	AgentID string            `json:"agent_id,omitempty"`
}

// Verdict is the typed PASS/FAIL verdict for run.verify (control-plane §3.3,
// §8.1, acceptance-contract G3-VERIFY). completed is only set by an explicit
// PASS verdict; a FAIL verdict transitions to failed. Both persist the
// verdict and evidence reference in the event journal.
type Verdict string

const (
	VerdictPass Verdict = "PASS"
	VerdictFail Verdict = "FAIL"
)

// RunVerifyParams is the typed input for run.verify. All fields are required
// (acceptance-contract G3-VERIFY.1): a verdict without a verifier identity or
// evidence reference is rejected. The handler must not transition the run to
// completed without a valid PASS verdict.
//
// ADR-0018 (C4): NextAction is optional. If empty, it defaults to 'none' for
// PASS and 'blocked' for FAIL. If provided, it is set atomically with the
// verify result.
type RunVerifyParams struct {
	RunID           string            `json:"run_id"`
	VerifierAgentID string            `json:"verifier_agent_id"`
	Verdict         Verdict           `json:"verdict"`
	EvidenceRef     string            `json:"evidence_ref"`
	NextAction      domain.NextAction `json:"next_action,omitempty"`

	// Approved is the human-approval flag for risk-graded verification
	// (R2/R3). When a run is in the verifying state with
	// next_action=approval_required, calling run.verify with a PASS
	// verdict and Approved=true transitions the run to completed with
	// result_state=approved. This is an alternative to run.approve; the
	// verifier-agent authorization checks are relaxed for this path since
	// the caller is a human approver, not a registered verifier agent.
	Approved bool `json:"approved,omitempty"`
}

// RunVerifyResult is the typed result of run.verify. The actual verification
// logic is P2; P0 requires only that the verdict is persisted and the state
// transition is gated on it (G3-VERIFY.4 — no fake success / stub).
//
// ADR-0018 (C1/C4): ResultState and NextAction are projected atomically with
// the state transition. ResultState is 'accepted' for PASS and 'failed' for
// FAIL. NextAction is the effective decision (defaulted if not provided).
type RunVerifyResult struct {
	RunID       string             `json:"run_id"`
	State       domain.RunStateV2  `json:"state"`
	Verdict     Verdict            `json:"verdict"`
	EvidenceRef string             `json:"evidence_ref"`
	ResultState domain.ResultState `json:"result_state"`
	NextAction  domain.NextAction  `json:"next_action"`
}

// RunApproveParams is the typed input for run.approve — the human-approval
// method for high-risk (R2/R3) runs that passed verification but require
// human sign-off before transitioning to completed (risk-graded
// verification). The run must be in the verifying state with
// next_action=approval_required.
type RunApproveParams struct {
	RunID       string `json:"run_id"`
	Approver    string `json:"approver"`               // human identifier
	EvidenceRef string `json:"evidence_ref,omitempty"` // optional evidence
}

// RunApproveResult is the typed result of run.approve. State is the
// post-approval V2 state (completed); ResultState is 'approved' (distinct
// from 'accepted' so the verdict trail records human sign-off).
type RunApproveResult struct {
	RunID       string             `json:"run_id"`
	State       domain.RunStateV2  `json:"state"`
	ResultState domain.ResultState `json:"result_state"`
}

// AgentRegisterParams is the typed input for agent.register.
type AgentRegisterParams struct {
	RunID     string           `json:"run_id"`
	Role      domain.AgentRole `json:"role"`
	Runtime   string           `json:"runtime"`
	PID       int              `json:"pid"`
	SessionID string           `json:"session_id,omitempty"`
}

// AgentRegisterResult is the typed result of agent.register.
type AgentRegisterResult struct {
	AgentID string `json:"agent_id"`
}

// AgentHeartbeatParams is the typed input for agent.heartbeat.
type AgentHeartbeatParams struct {
	AgentID string `json:"agent_id"`
}

// AgentHeartbeatResult is the typed result of agent.heartbeat.
type AgentHeartbeatResult struct {
	AgentID       string `json:"agent_id"`
	RenewDeadline int64  `json:"renew_deadline"`
}

// AgentCompleteParams is the typed input for agent.complete.
type AgentCompleteParams struct {
	AgentID  string `json:"agent_id"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

// AgentCompleteResult is the typed result of agent.complete.
type AgentCompleteResult struct {
	AgentID string            `json:"agent_id"`
	State   domain.AgentState `json:"state"`
}

// AgentBlockParams is the typed input for agent.block.
type AgentBlockParams struct {
	AgentID string `json:"agent_id"`
	Reason  string `json:"reason,omitempty"`
}

// AgentBlockResult is the typed result of agent.block.
type AgentBlockResult struct {
	AgentID string            `json:"agent_id"`
	RunID   string            `json:"run_id"`
	State   domain.RunStateV2 `json:"state"`
}

type RunStatusParams struct {
	RunID string `json:"run_id"`
}

// RunStatusResult is the typed result of run.status. The Run field
// carries V2 states (the authoritative DB representation).
type RunStatusResult struct {
	Run   *domain.Run   `json:"run"`
	Task  *domain.Task  `json:"task,omitempty"`
	Agent *domain.Agent `json:"agent,omitempty"`
}

type RunEventsParams struct {
	RunID  string `json:"run_id"`
	Cursor int64  `json:"cursor"`
	Limit  int    `json:"limit,omitempty"`
}

type RunEventsResult struct {
	Events []domain.Event `json:"events"`
}

// RunBlockParams is the typed input for run.block (v2 replacement for
// run.pause). It transitions a running run to the blocked state.
type RunBlockParams struct {
	RunID string `json:"run_id"`
}

// RunBlockResult is the typed result of run.block. State is a V2 state
// string (domain.RunV2Blocked). CandidateID is set when a checkpoint
// was created during the block.
type RunBlockResult struct {
	RunID       string            `json:"run_id"`
	State       domain.RunStateV2 `json:"state"`
	CandidateID string            `json:"candidate_id,omitempty"`
}

// RunUnblockParams is the typed input for run.unblock (v2 replacement
// for run.resume). It transitions a blocked run back to running.
type RunUnblockParams struct {
	RunID string `json:"run_id"`
}

// RunUnblockResult is the typed result of run.unblock. State is a V2
// state string (domain.RunV2Running). AgentID is set when a new runtime
// session was started.
type RunUnblockResult struct {
	RunID   string            `json:"run_id"`
	State   domain.RunStateV2 `json:"state"`
	AgentID string            `json:"agent_id,omitempty"`
}

// RunTerminateParams is the typed input for run.terminate (v2
// replacement for run.cancel). It transitions a run to the canceled
// state.
type RunTerminateParams struct {
	RunID string `json:"run_id"`
}

// RunTerminateResult is the typed result of run.terminate. State is a
// V2 state string (domain.RunV2Canceled).
type RunTerminateResult struct {
	RunID string            `json:"run_id"`
	State domain.RunStateV2 `json:"state"`
}

type RunTakeoverParams struct {
	CandidateID string `json:"candidate_id"`
	Objective   string `json:"objective"`
}

type RunTakeoverResult struct {
	RunID  string `json:"run_id"`
	TaskID string `json:"task_id"`
}

type ReconcileResult struct {
	Reconciled []string `json:"reconciled"`
}

type RunListParams struct {
	State domain.RunStateV2 `json:"state,omitempty"`
}

type RunListResult struct {
	Runs []*domain.Run `json:"runs"`
}

// --- Message bus types ---

// MessagePublishEnvelopeParams is the v1.1 typed message publish request.
type MessagePublishEnvelopeParams struct {
	MessageID      string                 `json:"message_id"`
	RunID          string                 `json:"run_id"`
	TaskID         string                 `json:"task_id,omitempty"`
	Sender         domain.MessageEndpoint `json:"sender"`
	Recipient      domain.MessageEndpoint `json:"recipient"`
	Type           domain.MessageType     `json:"type"`
	TTL            int                    `json:"ttl,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key"`
	PayloadRef     domain.PayloadRef      `json:"payload_ref"`
	Sensitivity    domain.Sensitivity     `json:"sensitivity,omitempty"`
}

// MessagePublishEnvelopeResult is the v1.1 typed message publish response.
type MessagePublishEnvelopeResult struct {
	Seq        int64  `json:"seq"`
	MessageSeq int64  `json:"message_seq"`
	MessageID  string `json:"message_id"`
	Deduped    bool   `json:"deduped"`
}

// MessagesByRunParams queries v1.1 envelope messages for a given run.
type MessagesByRunParams struct {
	RunID  string `json:"run_id"`
	Cursor int64  `json:"cursor"`
	Limit  int    `json:"limit,omitempty"`
}

// MessagesByRunResult is the response for messages.by_run.
type MessagesByRunResult struct {
	Messages   []domain.Event `json:"messages"`
	NextCursor int64          `json:"next_cursor"`
}

// MessageAckParams is the v1.1 message.ack request (C-002).
type MessageAckParams struct {
	MessageID string `json:"message_id"`
	AgentID   string `json:"agent_id,omitempty"`
}

// MessageAckResult is the v1.1 message.ack response.
type MessageAckResult struct {
	Acked bool `json:"acked"`
}

// MessageNackParams is the v1.1 message.nack request (C-002).
type MessageNackParams struct {
	MessageID string `json:"message_id"`
	AgentID   string `json:"agent_id,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// MessageNackResult is the v1.1 message.nack response.
type MessageNackResult struct {
	RetryCount int             `json:"retry_count"`
	FinalState domain.AckState `json:"final_state"`
}

// MessagesDeadlineCheckParams is the v1.1 messages.deadline_check request (C-002).
type MessagesDeadlineCheckParams struct {
	// No params — checks all pending messages against current time.
}

// MessagesDeadlineCheckResult is the v1.1 messages.deadline_check response.
type MessagesDeadlineCheckResult struct {
	ExpiredMessageIDs []string `json:"expired_message_ids"`
}

// MessagesStatusParams is the v1.1 messages.status request (C-002).
type MessagesStatusParams struct {
	MessageID string `json:"message_id"`
}

// MessagesStatusResult is the v1.1 messages.status response.
type MessagesStatusResult struct {
	MessageID  string          `json:"message_id"`
	AckState   domain.AckState `json:"ack_state"`
	RetryCount int             `json:"retry_count"`
	IsDead     bool            `json:"is_dead"`
	IsExpired  bool            `json:"is_expired"`
}

// --- Continuation types (ADR-0017) ---

// ContinuationRegisterParams is the typed input for continuation.register —
// an explicit PM/operator action to register a continuation need for a
// completed/blocked run that requires an explicit successor. Idempotent:
// the same run_id + successor_objective is a no-op if an active continuation
// already exists.
type ContinuationRegisterParams struct {
	RunID              string `json:"run_id"`
	SuccessorObjective string `json:"successor_objective"`
	Owner              string `json:"owner,omitempty"`
}

// ContinuationRegisterResult is the typed result of continuation.register.
type ContinuationRegisterResult struct {
	ContinuationID string `json:"continuation_id"`
}

// ContinuationListParams is the typed input for continuation.list. The
// state_filter is optional: "pending" (pending+notified), "all" or "" (all
// states), or a specific state ("pending","notified","fulfilled","cancelled").
type ContinuationListParams struct {
	StateFilter string `json:"state_filter,omitempty"`
}

// ContinuationListResult is the typed result of continuation.list.
type ContinuationListResult struct {
	Continuations []*domain.Continuation `json:"continuations"`
}

// ContinuationFulfillParams is the typed input for continuation.fulfill — an
// explicit PM action to link a successor run and mark the continuation
// fulfilled. NO implicit successor creation happens here.
type ContinuationFulfillParams struct {
	ContinuationID string `json:"continuation_id"`
	SuccessorRunID string `json:"successor_run_id"`
}

// ContinuationFulfillResult is the typed result of continuation.fulfill.
type ContinuationFulfillResult struct {
	ContinuationID string                   `json:"continuation_id"`
	State          domain.ContinuationState `json:"state"`
	SuccessorRunID string                   `json:"successor_run_id"`
}

// ContinuationCancelParams is the typed input for continuation.cancel — an
// explicit PM action to cancel a continuation (no successor needed).
type ContinuationCancelParams struct {
	ContinuationID string `json:"continuation_id"`
}

// ContinuationCancelResult is the typed result of continuation.cancel.
type ContinuationCancelResult struct {
	ContinuationID string                   `json:"continuation_id"`
	State          domain.ContinuationState `json:"state"`
}

// ReconcileContinuationsParams is the typed input for reconcile.continuations —
// a manual trigger of the continuation reconcile tick.
type ReconcileContinuationsParams struct {
	WakeGapSeconds int `json:"wake_gap_seconds,omitempty"`
}

// ReconcileContinuationsResult is the typed result of reconcile.continuations.
// It mirrors wake.ReconcileResult but as a serializable DTO.
type ReconcileContinuationsResult struct {
	Checked       int                  `json:"checked"`
	Notified      int                  `json:"notified"`
	ReNotified    int                  `json:"re_notified"`
	Skipped       int                  `json:"skipped"`
	Errors        int                  `json:"errors"`
	OrphanedRuns  []domain.OrphanedRun `json:"orphaned_runs,omitempty"`
	Notifications []string             `json:"notifications,omitempty"`
}

// ErrNoHandler is returned when a method has no registered handler.
var ErrNoHandler = errors.New("rpc: no handler")

// --- Terminal-state consistency types (ADR-0018) ---

// RunSupersedeParams is the typed input for run.supersede — an explicit
// PM/operator action to link an old run to its successor. The old run's
// state is NOT changed by supersede (it is a link, not a state transition).
// One successor per old run (UNIQUE on old_run_id).
type RunSupersedeParams struct {
	OldRunID       string `json:"old_run_id"`
	SuccessorRunID string `json:"successor_run_id"`
	Reason         string `json:"reason"`
}

// RunSupersedeResult is the typed result of run.supersede.
type RunSupersedeResult struct {
	SupersedeID    string `json:"supersede_id"`
	OldRunID       string `json:"old_run_id"`
	SuccessorRunID string `json:"successor_run_id"`
	Reason         string `json:"reason"`
}

// RunSetNextActionParams is the typed input for run.set_next_action — an
// explicit PM action to set or change the next_action decision on a
// terminal run (ADR-0018, C4). Idempotent: calling it twice updates the
// value.
type RunSetNextActionParams struct {
	RunID      string            `json:"run_id"`
	NextAction domain.NextAction `json:"next_action"`
}

// RunSetNextActionResult is the typed result of run.set_next_action.
type RunSetNextActionResult struct {
	RunID      string            `json:"run_id"`
	NextAction domain.NextAction `json:"next_action"`
}

// ReconcileTerminalStateParams is the typed input for
// reconcile.terminal_state — a manual trigger of the terminal-state
// reconcile sweep (ADR-0018). It surfaces terminal runs with missing
// next_action decisions, terminal runs with stale agents, and superseded
// runs.
type ReconcileTerminalStateParams struct{}

// ReconcileTerminalStateResult is the typed result of
// reconcile.terminal_state.
type ReconcileTerminalStateResult struct {
	MissingNextAction []store.MissingNextActionRun `json:"missing_next_action,omitempty"`
	StaleAgents       []store.StaleAgentRun        `json:"stale_agents,omitempty"`
	Superseded        []store.SupersededRun        `json:"superseded,omitempty"`
}

// --- Beacon discovery types ---

// AgentDiscoverParams is the typed input for agent.discover — a query for
// active agent sessions discovered by Beacon across tmux panes. AgentType is
// an optional filter (devin, claude, codex); empty returns all agent types.
type AgentDiscoverParams struct {
	AgentType string `json:"agent_type,omitempty"` // filter: devin, claude, codex, or empty for all
}

// AgentDiscoverResult is the typed result of agent.discover. Sessions is the
// list of discovered agent sessions; Count is len(Sessions).
type AgentDiscoverResult struct {
	Sessions []beacon.AgentSession `json:"sessions"`
	Count    int                   `json:"count"`
}

// --- Hydra model routing types ---

// HydraModelsResult is the typed result of hydra.models — the list of models
// available through the Hydra LLM gateway.
type HydraModelsResult struct {
	Models []hydra.Model `json:"models"`
}

// HydraHealthResult is the typed result of hydra.health — whether the Hydra
// gateway is reachable and healthy.
type HydraHealthResult struct {
	Healthy bool `json:"healthy"`
}

// --- Global Auditor types (Phase 4) ---

// AuditorAuditResult is the typed result of auditor.audit — one audit cycle.
// It reports how many runs were analyzed and how many findings were produced.
type AuditorAuditResult struct {
	RunsAnalyzed     int               `json:"runs_analyzed"`
	FindingsProduced int               `json:"findings_produced"`
	Findings         []*domain.Finding `json:"findings"`
}

// AuditorFindingsParams is the typed input for auditor.findings — a listing
// of auditor findings, optionally filtered by status (pending/accepted/
// rejected).
type AuditorFindingsParams struct {
	Status string `json:"status,omitempty"` // pending, accepted, rejected
	Limit  int    `json:"limit,omitempty"`
}

// AuditorFindingsResult is the typed result of auditor.findings.
type AuditorFindingsResult struct {
	Findings []*domain.Finding `json:"findings"`
}

// AuditorReviewParams is the typed input for auditor.review — a human
// accept/reject decision on a finding. The auditor never auto-accepts; this
// is the only path to a non-pending status.
type AuditorReviewParams struct {
	FindingID  string `json:"finding_id"`
	Status     string `json:"status"` // accepted or rejected
	ReviewedBy string `json:"reviewed_by"`
}

// AuditorReviewResult is the typed result of auditor.review.
type AuditorReviewResult struct {
	FindingID string `json:"finding_id"`
	Status    string `json:"status"`
}
