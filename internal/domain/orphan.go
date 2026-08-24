package domain

import "time"

// OrphanedRun is a run in running/planning/ready/verifying state whose agents
// are all dead (no live PID). The reconcile tick surfaces these as
// attention-required without auto-creating a successor.
//
// This is the durable representation of the Argus P2-B failure mode: a run
// left in a transient state with a dead agent PID and no tmux/devin session.
// The system detects and notifies; it never auto-creates a successor.
type OrphanedRun struct {
	RunID     string     `json:"run_id"`
	State     RunStateV2 `json:"state"`
	Owner     string     `json:"owner"`
	ProjectID string     `json:"project_id"`
	AgentID   string     `json:"agent_id,omitempty"`
	AgentPID  int        `json:"agent_pid,omitempty"`
	StartedAt time.Time  `json:"started_at,omitempty"`
}
