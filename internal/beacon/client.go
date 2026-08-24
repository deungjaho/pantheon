// Package beacon provides a client for the Beacon CLI tool, which discovers
// active agent sessions (Devin/Claude/Codex/AGy) from tmux panes.
//
// The client shells out to the `beacon` binary via exec.Command — it does NOT
// import Beacon's code. This keeps Pantheon decoupled from Beacon's internals;
// the only contract is the JSON shape of `beacon agents --json`.
package beacon

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

// AgentSession is a single discovered agent session bound to a tmux pane.
// The JSON tags mirror the output of `beacon agents --json`.
type AgentSession struct {
	Pane      string `json:"pane"`
	Session   string `json:"session"`    // tmux session name
	Window    string `json:"window"`     // tmux window
	Agent     string `json:"agent"`      // devin, claude, codex, agy
	SessionID string `json:"session_id"` // agent's internal session ID
	Title     string `json:"title"`      // session title/description
	Cwd       string `json:"cwd"`        // working directory
	PID       int    `json:"pid"`        // agent main process PID
}

// DefaultTimeout is the default upper bound for a single beacon invocation.
const DefaultTimeout = 10 * time.Second

// Client calls the Beacon CLI to discover active agent sessions.
type Client struct {
	binaryPath string        // path to beacon binary, default "beacon"
	timeout    time.Duration // per-invocation timeout
}

// Option configures a Client.
type Option func(*Client)

// WithBinaryPath sets the path to the beacon binary.
func WithBinaryPath(p string) Option {
	return func(c *Client) { c.binaryPath = p }
}

// WithTimeout sets the per-invocation timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// NewClient creates a Beacon client. The binary path defaults to "beacon"
// (resolved via PATH) and the timeout defaults to DefaultTimeout.
func NewClient(opts ...Option) *Client {
	c := &Client{
		binaryPath: "beacon",
		timeout:    DefaultTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// DiscoverAgents runs `beacon agents --json` and parses the result into typed
// AgentSession structs. The context bounds the invocation; if no deadline is
// set on the context, the client's configured timeout is applied.
func (c *Client) DiscoverAgents(ctx context.Context) ([]AgentSession, error) {
	bin := c.binaryPath
	if bin == "" {
		bin = "beacon"
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	// If the caller's context has no deadline, apply our own so a hung
	// beacon process cannot block forever.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, bin, "agents", "--json")
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("beacon: invoke %q: %w (context: %v)", bin, err, ctx.Err())
		}
		// Surface stderr from a non-zero exit for diagnostics.
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("beacon: %q exited: %w: %s", bin, err, ee.Stderr)
		}
		return nil, fmt.Errorf("beacon: invoke %q: %w", bin, err)
	}

	var sessions []AgentSession
	if err := json.Unmarshal(out, &sessions); err != nil {
		return nil, fmt.Errorf("beacon: parse output: %w", err)
	}
	if sessions == nil {
		sessions = []AgentSession{}
	}
	return sessions, nil
}

// FilterByAgentType returns the subset of sessions whose Agent field matches
// the given agent type (e.g. "devin", "claude", "codex"). An empty agentType
// returns all sessions unchanged.
func FilterByAgentType(sessions []AgentSession, agentType string) []AgentSession {
	if agentType == "" {
		return sessions
	}
	filtered := make([]AgentSession, 0, len(sessions))
	for _, s := range sessions {
		if s.Agent == agentType {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
