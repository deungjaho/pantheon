package domain

import "fmt"

// RunStateV2 is the canonical Run state per control-plane §8.1.
//
// This is the P0 Runtime Closure state machine. Legacy RunState
// (pending/preparing/running/paused/resuming/stopping/stopped/failed/
// canceled) is preserved as a compatibility facade; legacy RPC methods
// delegate to this contract via the state mapping (see acceptance-contract
// G3-BC.4). New semantic CLI and typed contracts use RunStateV2.
//
// State graph (control-plane §8.1):
//
//	requested → planning → ready → running → verifying → completed
//	            ↑          │        │           │
//	            └──────────┴────────┴───────────┘
//	                                               ↓
//	                                          blocked / failed / canceled
type RunStateV2 string

const (
	RunV2Requested RunStateV2 = "requested"
	RunV2Planning  RunStateV2 = "planning"
	RunV2Ready     RunStateV2 = "ready"
	RunV2Running   RunStateV2 = "running"
	RunV2Verifying RunStateV2 = "verifying"
	RunV2Completed RunStateV2 = "completed"
	RunV2Blocked   RunStateV2 = "blocked"
	RunV2Failed    RunStateV2 = "failed"
	RunV2Canceled  RunStateV2 = "canceled"
)

// ValidRunTransitionV2 reports whether a Run state transition is allowed
// under control-plane §8.1.
func ValidRunTransitionV2(from, to RunStateV2) bool {
	switch from {
	case RunV2Requested:
		return to == RunV2Planning || to == RunV2Blocked ||
			to == RunV2Canceled || to == RunV2Failed
	case RunV2Planning:
		return to == RunV2Ready || to == RunV2Blocked ||
			to == RunV2Canceled || to == RunV2Failed
	case RunV2Ready:
		return to == RunV2Running || to == RunV2Planning ||
			to == RunV2Blocked || to == RunV2Canceled || to == RunV2Failed
	case RunV2Running:
		return to == RunV2Verifying || to == RunV2Planning ||
			to == RunV2Blocked || to == RunV2Canceled || to == RunV2Failed
	case RunV2Verifying:
		return to == RunV2Completed || to == RunV2Running ||
			to == RunV2Planning || to == RunV2Failed
	case RunV2Blocked:
		return to == RunV2Running || to == RunV2Canceled || to == RunV2Failed
	case RunV2Completed, RunV2Failed, RunV2Canceled:
		return false
	}
	return false
}

// CheckRunTransitionV2 returns an error if the transition is invalid.
func CheckRunTransitionV2(from, to RunStateV2) error {
	if !ValidRunTransitionV2(from, to) {
		return fmt.Errorf("invalid run transition %s -> %s", from, to)
	}
	return nil
}

// RunLease is the lease holder and renew deadline for a Run (control-plane
// §8.2). RenewDeadline is a Unix timestamp in seconds; 0 means no lease.
type RunLease struct {
	Holder        string `json:"holder"`
	RenewDeadline int64  `json:"renew_deadline"`
}

// RunContractV2 is the canonical Run contract per control-plane §8.2.
// This is the typed contract surfaced by the semantic CLI; DTOs do not
// leak DB row shapes (control-plane §七).
type RunContractV2 struct {
	RunID      string     `json:"run_id"`
	ProjectID  string     `json:"project_id"`
	Owner      string     `json:"owner"`
	Workspace  string     `json:"workspace"`
	State      RunStateV2 `json:"state"`
	Epoch      int        `json:"epoch"`
	Lease      RunLease   `json:"lease"`
	LastEvent  string     `json:"last_event"`
	Checkpoint string     `json:"checkpoint"`
	Evidence   []string   `json:"evidence"`
}

// LegacyRunStateMap maps legacy RunState values to RunStateV2 for the
// compatibility facade (acceptance-contract G3-BC.4). The §8.1 state machine
// (ValidRunTransitionV2) is the authoritative path; legacy RPC methods
// translate legacy state strings to V2 at the boundary, drive multi-step
// V2 transitions internally, and translate back for display.
var LegacyRunStateMap = map[RunState]RunStateV2{
	RunPending:   RunV2Requested,
	RunPreparing: RunV2Planning,
	RunRunning:   RunV2Running,
	RunPaused:    RunV2Blocked,
	RunResuming:  RunV2Running,
	RunStopping:  RunV2Blocked,
	RunStopped:   RunV2Completed,
	RunFailed:    RunV2Failed,
	RunCanceled:  RunV2Canceled,
	RunVerifying: RunV2Verifying,
	RunCompleted: RunV2Completed,
}

// V2ToLegacyRunStateMap maps V2 states back to legacy state strings for
// legacy facade display (acceptance-contract G3-BC.4). Where a V2 state
// has no exact legacy equivalent, the closest legacy string is chosen
// (e.g. RunV2Blocked → RunPaused). This is the inverse of
// LegacyRunStateMap for the states that legacy callers observe.
var V2ToLegacyRunStateMap = map[RunStateV2]RunState{
	RunV2Requested: RunPending,
	RunV2Planning:  RunPreparing,
	RunV2Ready:     RunPreparing, // no legacy equivalent; closest is preparing
	RunV2Running:   RunRunning,
	RunV2Verifying: RunVerifying,
	RunV2Completed: RunStopped,
	RunV2Blocked:   RunPaused,
	RunV2Failed:    RunFailed,
	RunV2Canceled:  RunCanceled,
}

// LegacyRunStateFromV2 translates a V2 state to the closest legacy state
// string for legacy facade display. Returns the V2 string unchanged if no
// mapping exists (should not happen for valid states).
func LegacyRunStateFromV2(s RunStateV2) RunState {
	if legacy, ok := V2ToLegacyRunStateMap[s]; ok {
		return legacy
	}
	return RunState(s)
}

// IsTerminalRunState reports whether s is a terminal run state
// (completed/failed/canceled) per control-plane §8.1 (ADR-0018, C2).
// Terminal runs have no further state transitions; their nonterminal agents
// must be terminalized.
func IsTerminalRunState(s RunStateV2) bool {
	return s == RunV2Completed || s == RunV2Failed || s == RunV2Canceled
}
