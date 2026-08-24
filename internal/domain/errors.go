package domain

import (
	"errors"
	"fmt"
)

type ResultState string

const (
	ResultNotStarted     ResultState = "not_started"
	ResultFailed         ResultState = "failed"
	ResultPartial        ResultState = "partial"
	ResultAccepted       ResultState = "accepted"
	ResultUnknown        ResultState = "result_unknown"
	ResultCompleted      ResultState = "completed"
	ResultBudgetExceeded ResultState = "budget_exceeded"
	// ResultApproved is set when a high-risk (R2/R3) run is approved by a
	// human via run.approve after a verify PASS. Distinct from accepted
	// (auto-accepted on R0/R1 verify PASS) so the verdict trail records
	// that a human signed off.
	ResultApproved ResultState = "approved"
)

type ErrorCode string

const (
	CodeOK                 ErrorCode = "OK"
	CodeInvalidInput       ErrorCode = "INVALID_INPUT"
	CodeNotFound           ErrorCode = "NOT_FOUND"
	CodeAlreadyExists      ErrorCode = "ALREADY_EXISTS"
	CodeSnapshotRequired   ErrorCode = "SNAPSHOT_REQUIRED"
	CodeBaseRefMissing     ErrorCode = "BASE_REF_MISSING"
	CodeWorktreeConflict   ErrorCode = "WORKTREE_CONFLICT"
	CodeRuntimeUnavailable ErrorCode = "RUNTIME_UNAVAILABLE"
	CodeBudgetExceeded     ErrorCode = "BUDGET_EXCEEDED"
	CodeConflict           ErrorCode = "CONFLICT"
	CodeUnauthorized       ErrorCode = "UNAUTHORIZED"
	CodeTransportError     ErrorCode = "TRANSPORT_ERROR"
	CodeUnsupported        ErrorCode = "UNSUPPORTED"
	CodeInternal           ErrorCode = "INTERNAL"
)

type Error struct {
	Code        ErrorCode   `json:"code"`
	Message     string      `json:"message"`
	Retryable   bool        `json:"retryable"`
	ResultState ResultState `json:"result_state"`
	Details     any         `json:"details,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("pantheon: %s: %s", e.Code, e.Message)
}

func NewError(code ErrorCode, msg string, retryable bool, state ResultState) *Error {
	return &Error{Code: code, Message: msg, Retryable: retryable, ResultState: state}
}

func ErrInvalidInput(msg string) *Error {
	return NewError(CodeInvalidInput, msg, false, ResultNotStarted)
}

func ErrNotFound(msg string) *Error {
	return NewError(CodeNotFound, msg, false, ResultNotStarted)
}

func ErrConflict(msg string) *Error {
	return NewError(CodeConflict, msg, false, ResultNotStarted)
}

func ErrUnauthorized(msg string) *Error {
	return NewError(CodeUnauthorized, msg, false, ResultNotStarted)
}

func ErrSnapshotRequired(msg string) *Error {
	return NewError(CodeSnapshotRequired, msg, false, ResultNotStarted)
}

func ErrBaseRefMissing(msg string) *Error {
	return NewError(CodeBaseRefMissing, msg, false, ResultNotStarted)
}

func ErrWorktreeConflict(msg string) *Error {
	return NewError(CodeWorktreeConflict, msg, false, ResultNotStarted)
}

func ErrRuntimeUnavailable(msg string) *Error {
	return NewError(CodeRuntimeUnavailable, msg, true, ResultNotStarted)
}

func ErrBudgetExceeded(msg string) *Error {
	return NewError(CodeBudgetExceeded, msg, false, ResultFailed)
}

func ErrTransport(msg string) *Error {
	return NewError(CodeTransportError, msg, true, ResultUnknown)
}

// ErrUnsupported is returned when an optional integration (e.g. Beacon,
// Hydra) is not configured. The caller can surface a clear "degraded
// mode" message rather than a generic internal error.
func ErrUnsupported(msg string) *Error {
	return NewError(CodeUnsupported, msg, false, ResultNotStarted)
}

func ErrInternal(msg string) *Error {
	return NewError(CodeInternal, msg, false, ResultUnknown)
}

func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return ErrInternal(err.Error())
}
