package domain

import "testing"

func TestValidWorkspaceTransition(t *testing.T) {
	cases := []struct {
		from, to WorkspaceState
		want     bool
	}{
		{WorkspaceCreated, WorkspaceActive, true},
		{WorkspaceCreated, WorkspaceFailed, true},
		{WorkspaceCreated, WorkspaceStopped, false},
		{WorkspaceActive, WorkspaceStopping, true},
		{WorkspaceActive, WorkspaceFailed, true},
		{WorkspaceActive, WorkspaceCreated, false},
		{WorkspaceStopping, WorkspaceStopped, true},
		{WorkspaceStopped, WorkspaceActive, false},
		{WorkspaceFailed, WorkspaceActive, false},
	}
	for _, c := range cases {
		got := ValidWorkspaceTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("Workspace %s -> %s = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestValidRunTransition(t *testing.T) {
	cases := []struct {
		from, to RunState
		want     bool
	}{
		{RunPending, RunPreparing, true},
		{RunPending, RunCanceled, true},
		{RunPending, RunRunning, false},
		{RunPreparing, RunRunning, true},
		{RunPreparing, RunFailed, true},
		{RunRunning, RunPaused, true},
		{RunRunning, RunStopping, true},
		{RunRunning, RunFailed, true},
		{RunRunning, RunStopped, true},
		{RunPaused, RunResuming, true},
		{RunPaused, RunStopping, true},
		{RunResuming, RunRunning, true},
		{RunResuming, RunFailed, true},
		{RunStopping, RunStopped, true},
		{RunStopped, RunRunning, false},
		{RunFailed, RunRunning, false},
		{RunCanceled, RunRunning, false},
	}
	for _, c := range cases {
		got := ValidRunTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("Run %s -> %s = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestValidTaskTransition(t *testing.T) {
	cases := []struct {
		from, to TaskState
		want     bool
	}{
		{TaskDraft, TaskReady, true},
		{TaskDraft, TaskCanceled, true},
		{TaskDraft, TaskRunning, false},
		{TaskReady, TaskRunning, true},
		{TaskReady, TaskCanceled, true},
		{TaskRunning, TaskCandidateReady, true},
		{TaskRunning, TaskFailed, true},
		{TaskRunning, TaskCanceled, true},
		{TaskCandidateReady, TaskRunning, false},
		{TaskFailed, TaskRunning, false},
	}
	for _, c := range cases {
		got := ValidTaskTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("Task %s -> %s = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestValidAgentTransition(t *testing.T) {
	cases := []struct {
		from, to AgentState
		want     bool
	}{
		{AgentRegistered, AgentStarting, true},
		{AgentRegistered, AgentExited, true},
		{AgentRegistered, AgentRunning, false},
		{AgentStarting, AgentRunning, true},
		{AgentStarting, AgentExited, true},
		{AgentRunning, AgentExited, true},
		{AgentRunning, AgentLost, true},
		{AgentLost, AgentExited, true},
		{AgentExited, AgentRunning, false},
	}
	for _, c := range cases {
		got := ValidAgentTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("Agent %s -> %s = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestCheckRunTransitionError(t *testing.T) {
	if err := CheckRunTransition(RunStopped, RunRunning); err == nil {
		t.Fatal("expected error for stopped -> running")
	}
	if err := CheckRunTransition(RunPending, RunPreparing); err != nil {
		t.Fatalf("unexpected error for pending -> preparing: %v", err)
	}
}
