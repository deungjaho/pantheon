package domain

import (
	"fmt"
	"time"
)

// ContinuationState is the state of a continuation record (ADR-0017).
//
// State graph:
//
//	pending → notified → fulfilled
//	                    ↘ cancelled
//	pending → cancelled
type ContinuationState string

const (
	ContinuationPending   ContinuationState = "pending"
	ContinuationNotified  ContinuationState = "notified"
	ContinuationFulfilled ContinuationState = "fulfilled"
	ContinuationCancelled ContinuationState = "cancelled"
)

// ValidContinuationTransition reports whether a transition is allowed.
func ValidContinuationTransition(from, to ContinuationState) bool {
	switch from {
	case ContinuationPending:
		return to == ContinuationNotified || to == ContinuationCancelled
	case ContinuationNotified:
		return to == ContinuationFulfilled || to == ContinuationCancelled || to == ContinuationNotified
	case ContinuationFulfilled, ContinuationCancelled:
		return false
	}
	return false
}

// CheckContinuationTransition returns an error if the transition is invalid.
func CheckContinuationTransition(from, to ContinuationState) error {
	if !ValidContinuationTransition(from, to) {
		return fmt.Errorf("invalid continuation transition %s -> %s", from, to)
	}
	return nil
}

// Continuation is the durable representation of a completed/blocked run that
// requires an explicit successor (ADR-0017).
//
// A continuation is created by an explicit PM/operator action
// (RegisterContinuation) when a run completes or blocks and a successor is
// needed. The daemon tick (ReconcileContinuations) detects pending continuations
// and sends deduplicated wake notifications to the PM message queue. The PM
// then explicitly creates the successor run (CreateSuccessorRun), which
// fulfills the continuation.
//
// The system never auto-creates successors. It only detects and notifies.
type Continuation struct {
	ContinuationID     string            `json:"continuation_id"`
	RunID              string            `json:"run_id"`
	ProjectID          string            `json:"project_id"`
	Owner              string            `json:"owner"`
	SuccessorObjective string            `json:"successor_objective"`
	State              ContinuationState `json:"state"`
	WakeSentAt         *time.Time        `json:"wake_sent_at,omitempty"`
	WakeCount          int               `json:"wake_count"`
	SuccessorRunID     string            `json:"successor_run_id,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	FulfilledAt        *time.Time        `json:"fulfilled_at,omitempty"`

	// RootCause is the derived reason a continuation was needed, used by the
	// same-root-cause circuit breaker (ROADMAP "同根因 3 次熔断"). Examples:
	// "exit_code_0_incomplete" (agent exited 0 but subtasks remain),
	// "exit_code_N" (agent exited with non-zero code N), "no_progress"
	// (progress gate detected no progress). Empty for continuations created
	// before the root-cause tracking migration.
	RootCause string `json:"root_cause,omitempty"`
}
