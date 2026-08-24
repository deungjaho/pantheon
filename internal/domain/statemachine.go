package domain

import "fmt"

// ValidWorkspaceTransition reports whether a workspace state transition is
// allowed under the Phase 1 state machine.
func ValidWorkspaceTransition(from, to WorkspaceState) bool {
	switch from {
	case WorkspaceCreated:
		return to == WorkspaceActive || to == WorkspaceFailed
	case WorkspaceActive:
		return to == WorkspaceStopping || to == WorkspaceFailed
	case WorkspaceStopping:
		return to == WorkspaceStopped || to == WorkspaceFailed
	case WorkspaceStopped, WorkspaceFailed:
		return false
	}
	return false
}

// ValidRunTransition reports whether a run state transition is allowed.
func ValidRunTransition(from, to RunState) bool {
	switch from {
	case RunPending:
		return to == RunPreparing || to == RunCanceled || to == RunFailed
	case RunPreparing:
		return to == RunRunning || to == RunFailed || to == RunCanceled
	case RunRunning:
		return to == RunPaused || to == RunStopping || to == RunFailed ||
			to == RunStopped || to == RunCanceled
	case RunPaused:
		return to == RunResuming || to == RunStopping || to == RunCanceled ||
			to == RunFailed
	case RunResuming:
		return to == RunRunning || to == RunFailed || to == RunCanceled
	case RunStopping:
		return to == RunStopped || to == RunFailed
	case RunStopped, RunFailed, RunCanceled:
		return false
	}
	return false
}

// ValidTaskTransition reports whether a task state transition is allowed.
func ValidTaskTransition(from, to TaskState) bool {
	switch from {
	case TaskDraft:
		return to == TaskReady || to == TaskCanceled
	case TaskReady:
		return to == TaskRunning || to == TaskCanceled
	case TaskRunning:
		return to == TaskCandidateReady || to == TaskFailed || to == TaskCanceled
	case TaskCandidateReady, TaskFailed, TaskCanceled:
		return false
	}
	return false
}

// ValidAgentTransition reports whether an agent state transition is allowed.
func ValidAgentTransition(from, to AgentState) bool {
	switch from {
	case AgentRegistered:
		return to == AgentStarting || to == AgentExited
	case AgentStarting:
		return to == AgentRunning || to == AgentExited
	case AgentRunning:
		return to == AgentExited || to == AgentLost
	case AgentLost:
		return to == AgentExited
	case AgentExited:
		return false
	}
	return false
}

// CheckRunTransition returns an error if the transition is invalid.
func CheckRunTransition(from, to RunState) error {
	if !ValidRunTransition(from, to) {
		return fmt.Errorf("invalid run transition %s -> %s", from, to)
	}
	return nil
}
