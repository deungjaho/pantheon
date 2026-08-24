package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// ListOrphanedRuns returns runs in a transient state (running/planning/ready/
// verifying) whose agents are all dead — no agent with state='running' has a
// live PID. The PID liveness check is performed in Go (os.FindProcess + signal
// 0), NOT in SQL: SQL returns the PID, Go decides whether it is alive.
//
// A run is orphaned when it has at least one agent in the 'running' state but
// none of those agents have a live PID. A run with no agents at all in a
// transient state is also considered orphaned (the agent record may have been
// lost or never created). A run whose only running agent has a live PID is NOT
// orphaned.
func (s *Store) ListOrphanedRuns(ctx context.Context) ([]domain.OrphanedRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Select transient runs joined with their running agents. We fetch the
	// agent pid/state so Go can check liveness. A run with no running agent
	// rows still appears (LEFT JOIN) with NULL agent columns — those are
	// orphaned too (no live agent to drive them).
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.run_id, r.state, r.owner, r.project_id,
		       a.agent_id, a.pid, a.started_at
		FROM runs r
		LEFT JOIN agents a ON a.run_id = r.run_id AND a.state = 'running'
		WHERE r.state IN ('running','planning','ready','verifying')
		ORDER BY r.run_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list orphaned runs: query: %w", err)
	}
	defer rows.Close()

	// Group agent rows by run_id. A run may have multiple running agents
	// (Phase 1 has one, but the query is general).
	type agentInfo struct {
		agentID   string
		pid       int
		startedAt time.Time
		hasRow    bool
	}
	runAgents := make(map[string][]agentInfo)
	runMeta := make(map[string]domain.OrphanedRun)

	for rows.Next() {
		var (
			runID, state, owner, projectID string
			agentID                        sql.NullString
			pid                            sql.NullInt64
			startedAt                      sql.NullString
		)
		if err := rows.Scan(&runID, &state, &owner, &projectID, &agentID, &pid, &startedAt); err != nil {
			return nil, fmt.Errorf("list orphaned runs: scan: %w", err)
		}
		runMeta[runID] = domain.OrphanedRun{
			RunID:     runID,
			State:     domain.RunStateV2(state),
			Owner:     owner,
			ProjectID: projectID,
		}
		if agentID.Valid {
			info := agentInfo{agentID: agentID.String, hasRow: true}
			if pid.Valid {
				info.pid = int(pid.Int64)
			}
			if startedAt.Valid && startedAt.String != "" {
				if t, err := time.Parse(time.RFC3339Nano, startedAt.String); err == nil {
					info.startedAt = t
				}
			}
			runAgents[runID] = append(runAgents[runID], info)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list orphaned runs: rows: %w", err)
	}

	var orphaned []domain.OrphanedRun
	for runID, agents := range runAgents {
		meta := runMeta[runID]
		// No running agents at all → orphaned (nothing is driving the run).
		if len(agents) == 0 {
			orphaned = append(orphaned, meta)
			continue
		}
		// Check if ANY running agent has a live PID. If none do, the run
		// is orphaned. Record the first dead agent's info for surfacing.
		anyLive := false
		for _, a := range agents {
			if pidAlive(a.pid) {
				anyLive = true
				break
			}
		}
		if !anyLive {
			dead := agents[0]
			meta.AgentID = dead.agentID
			meta.AgentPID = dead.pid
			meta.StartedAt = dead.startedAt
			orphaned = append(orphaned, meta)
		}
	}
	return orphaned, nil
}

// pidAlive reports whether a process with the given PID is currently running.
// PID 0 is never alive. The check uses os.FindProcess + signal 0, which is the
// portable Go way to test liveness without sending a real signal. On Unix,
// signal 0 returns nil if the process exists and the caller has permission to
// signal it, or an error otherwise (ESRCH = no such process, EPERM = alive but
// not ours — treated as not alive here since we cannot manage it).
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := p.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}
