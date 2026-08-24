package beacon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// writeMockBeacon writes a small executable to a temp directory that emits
// the given stdout (and optionally exits non-zero). It returns the path to
// the mock binary. The test uses this as the beacon binary path so the real
// exec.Command path and JSON parsing are exercised without depending on a
// real beacon installation.
func writeMockBeacon(t *testing.T, stdout string, exitCode int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mockbeacon")
	var script string
	if exitCode == 0 {
		script = "#!/bin/sh\ncat <<'EOF'\n" + stdout + "\nEOF\n"
	} else {
		script = "#!/bin/sh\nprintf '%s' " + quoteSh(stdout) + " 1>&2\nexit " + itoa(exitCode) + "\n"
	}
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write mock beacon: %v", err)
	}
	return path
}

// quoteSh wraps s in single quotes for safe embedding in a shell script.
func quoteSh(s string) string {
	return "'" + s + "'"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestDiscoverAgents_ParsesJSON(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mock beacon uses a shell script; skip on windows")
	}
	out := `[
	  {"pane":"%3","session":"main","window":"0","agent":"devin","session_id":"almond-chef","title":"cooking","cwd":"/tmp/repo","pid":12345},
	  {"pane":"%5","session":"main","window":"1","agent":"claude","session_id":"abc-123","title":"","cwd":"/tmp/other","pid":23456}
	]`
	bin := writeMockBeacon(t, out, 0)
	c := NewClient(WithBinaryPath(bin), WithTimeout(5*time.Second))

	sessions, err := c.DiscoverAgents(context.Background())
	if err != nil {
		t.Fatalf("DiscoverAgents: unexpected error: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
	if sessions[0].Agent != "devin" {
		t.Errorf("sessions[0].Agent = %q, want %q", sessions[0].Agent, "devin")
	}
	if sessions[0].SessionID != "almond-chef" {
		t.Errorf("sessions[0].SessionID = %q, want %q", sessions[0].SessionID, "almond-chef")
	}
	if sessions[0].PID != 12345 {
		t.Errorf("sessions[0].PID = %d, want 12345", sessions[0].PID)
	}
	if sessions[1].Agent != "claude" {
		t.Errorf("sessions[1].Agent = %q, want %q", sessions[1].Agent, "claude")
	}
	if sessions[1].Cwd != "/tmp/other" {
		t.Errorf("sessions[1].Cwd = %q, want %q", sessions[1].Cwd, "/tmp/other")
	}
}

func TestDiscoverAgents_EmptyArray(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mock beacon uses a shell script; skip on windows")
	}
	bin := writeMockBeacon(t, "[]", 0)
	c := NewClient(WithBinaryPath(bin))

	sessions, err := c.DiscoverAgents(context.Background())
	if err != nil {
		t.Fatalf("DiscoverAgents: unexpected error: %v", err)
	}
	if sessions == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(sessions) != 0 {
		t.Fatalf("expected 0 sessions, got %d", len(sessions))
	}
}

func TestDiscoverAgents_NonZeroExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mock beacon uses a shell script; skip on windows")
	}
	bin := writeMockBeacon(t, "tmux not found", 1)
	c := NewClient(WithBinaryPath(bin))

	_, err := c.DiscoverAgents(context.Background())
	if err == nil {
		t.Fatal("expected error for non-zero exit, got nil")
	}
}

func TestDiscoverAgents_BinaryNotFound(t *testing.T) {
	c := NewClient(WithBinaryPath(filepath.Join(t.TempDir(), "does-not-exist")))

	_, err := c.DiscoverAgents(context.Background())
	if err == nil {
		t.Fatal("expected error for missing binary, got nil")
	}
}

func TestDiscoverAgents_MalformedJSON(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mock beacon uses a shell script; skip on windows")
	}
	bin := writeMockBeacon(t, "{not valid json", 0)
	c := NewClient(WithBinaryPath(bin))

	_, err := c.DiscoverAgents(context.Background())
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestDiscoverAgents_ContextTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mock beacon uses a shell script; skip on windows")
	}
	// Write a mock beacon that sleeps longer than the context deadline.
	dir := t.TempDir()
	path := filepath.Join(dir, "mockbeacon")
	script := "#!/bin/sh\nsleep 5\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write mock beacon: %v", err)
	}
	c := NewClient(WithBinaryPath(path))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.DiscoverAgents(ctx)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestFilterByAgentType(t *testing.T) {
	sessions := []AgentSession{
		{Agent: "devin", SessionID: "s1"},
		{Agent: "claude", SessionID: "s2"},
		{Agent: "devin", SessionID: "s3"},
		{Agent: "codex", SessionID: "s4"},
	}
	tests := []struct {
		name      string
		agentType string
		want      int
		wantAgent string
	}{
		{"empty returns all", "", 4, ""},
		{"devin only", "devin", 2, "devin"},
		{"claude only", "claude", 1, "claude"},
		{"codex only", "codex", 1, "codex"},
		{"none match", "agy", 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FilterByAgentType(sessions, tc.agentType)
			if len(got) != tc.want {
				t.Fatalf("FilterByAgentType(%q) = %d sessions, want %d", tc.agentType, len(got), tc.want)
			}
			if tc.wantAgent != "" && len(got) > 0 {
				for _, s := range got {
					if s.Agent != tc.wantAgent {
						t.Errorf("got agent %q, want %q", s.Agent, tc.wantAgent)
					}
				}
			}
		})
	}
}

func TestNewClient_Defaults(t *testing.T) {
	c := NewClient()
	if c.binaryPath != "beacon" {
		t.Errorf("default binaryPath = %q, want %q", c.binaryPath, "beacon")
	}
	if c.timeout != DefaultTimeout {
		t.Errorf("default timeout = %v, want %v", c.timeout, DefaultTimeout)
	}
}

func TestNewClient_Options(t *testing.T) {
	c := NewClient(WithBinaryPath("/custom/beacon"), WithTimeout(30*time.Second))
	if c.binaryPath != "/custom/beacon" {
		t.Errorf("binaryPath = %q, want %q", c.binaryPath, "/custom/beacon")
	}
	if c.timeout != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", c.timeout)
	}
}

// Ensure exec.CommandContext is available (sanity — guards against the
// import being accidentally removed).
var _ = exec.CommandContext
