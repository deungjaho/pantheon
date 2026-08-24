package domain

import "time"

// NextAction is the PM's explicit decision recorded at run completion
// (ADR-0018, C4). It records what should happen next for a terminal run.
//
// Empty string ("") means "not decided" — the reconcile tick surfaces
// terminal runs with an empty next_action as the "missing decision" case.
type NextAction string

const (
	// NextActionNone means no successor is needed; the run is fully done.
	NextActionNone NextAction = "none"
	// NextActionContinuation means a continuation/successor is expected.
	// The PM will register a continuation record (ADR-0017) and create the
	// successor run explicitly.
	NextActionContinuation NextAction = "continuation"
	// NextActionBlocked means the run is blocked and needs PM attention
	// before a successor can be decided.
	NextActionBlocked NextAction = "blocked"
	// NextActionApprovalRequired means a verify PASS has been recorded but
	// the run's risk level (R2/R3) requires human approval before it can
	// transition to completed. The run stays in the verifying state until
	// run.approve (or run.verify with an approval flag) is called.
	NextActionApprovalRequired NextAction = "approval_required"
)

// ValidNextAction reports whether s is a recognized NextAction value.
func ValidNextAction(s NextAction) bool {
	switch s {
	case NextActionNone, NextActionContinuation, NextActionBlocked, NextActionApprovalRequired:
		return true
	}
	return false
}

// SupersedeRecord links an old run to its successor (ADR-0018, C3).
// This is an explicit PM/operator action. The old run's state is NOT
// changed — supersede is a link, not a state transition. The old run
// may be terminalized separately if desired.
//
// One successor per old run (UNIQUE on old_run_id). The link is durable
// and append-only; it never rewrites the old run's history.
type SupersedeRecord struct {
	SupersedeID    string    `json:"supersede_id"`
	OldRunID       string    `json:"old_run_id"`
	SuccessorRunID string    `json:"successor_run_id"`
	Reason         string    `json:"reason"`
	CreatedAt      time.Time `json:"created_at"`
}
