package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPantheond_Initialize spawns the real pantheond binary, sends an
// initialize request, and verifies the response.
func TestPantheond_Initialize(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()

	resp := sendRPC(t, bin, dir, map[string]any{
		"jsonrpc": "2.0",
		"id":      "req_test_init",
		"method":  "initialize",
		"params":  map[string]any{},
	})

	if resp["jsonrpc"] != "2.0" {
		t.Fatalf("jsonrpc = %v, want 2.0", resp["jsonrpc"])
	}
	if resp["id"] != "req_test_init" {
		t.Fatalf("id = %v, want req_test_init", resp["id"])
	}
	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result is not an object: %v", resp["result"])
	}
	if result["server_name"] != "pantheond" {
		t.Fatalf("server_name = %v, want pantheond", result["server_name"])
	}
	caps, ok := result["capabilities"].([]any)
	if !ok {
		t.Fatal("capabilities is not an array")
	}
	if len(caps) < 14 {
		t.Fatalf("expected at least 14 capabilities, got %d", len(caps))
	}
}

// TestPantheond_ProjectRegister verifies the full project.register flow.
func TestPantheond_ProjectRegister(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()

	// First register a project.
	resp := sendRPC(t, bin, dir, map[string]any{
		"jsonrpc": "2.0",
		"id":      "req_test_pr",
		"method":  "project.register",
		"params": map[string]any{
			"name":      "test-project",
			"repo_path": "/tmp/test-repo",
			"base_ref":  "main",
		},
	})

	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result is not an object: %v", resp["result"])
	}
	projectID, ok := result["project_id"].(string)
	if !ok || !strings.HasPrefix(projectID, "prj_") {
		t.Fatalf("project_id = %v, want prj_*", result["project_id"])
	}
}

// TestPantheond_UnknownMethod verifies error handling for unknown methods.
func TestPantheond_UnknownMethod(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()

	resp := sendRPC(t, bin, dir, map[string]any{
		"jsonrpc": "2.0",
		"id":      "req_test_unknown",
		"method":  "nonexistent.method",
		"params":  map[string]any{},
	})

	if resp["error"] == nil {
		t.Fatal("expected error for unknown method")
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error is not an object: %v", resp["error"])
	}
	if errObj["code"] == nil {
		t.Fatal("error.code is nil")
	}
}

// TestPantheond_InvalidJSON verifies error handling for malformed input.
// The daemon should return a JSON-RPC error response, not crash.
func TestPantheond_InvalidJSON(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()

	cmd := exec.Command(bin, "-db", filepath.Join(dir, "test.db"), "-worktrees", filepath.Join(dir, "wt"))
	cmd.Stdin = strings.NewReader("not valid json\n")
	stdout := &strings.Builder{}
	cmd.Stdout = stdout
	cmd.Stderr = &strings.Builder{}

	// The daemon handles malformed input gracefully by returning a
	// JSON-RPC error response. It should not crash (non-zero exit).
	_ = cmd.Run()

	output := strings.TrimSpace(stdout.String())
	if output == "" {
		t.Fatal("expected error response, got empty output")
	}

	var resp map[string]any
	if err := json.Unmarshal([]byte(output), &resp); err != nil {
		t.Fatalf("response is not valid JSON: %v\noutput: %s", err, output)
	}
	if resp["error"] == nil {
		t.Fatal("expected error field in response for invalid JSON")
	}
}

// TestPantheond_MultipleRequests verifies the daemon handles multiple
// requests in a single session.
func TestPantheond_MultipleRequests(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()

	cmd := exec.Command(bin, "-db", filepath.Join(dir, "test.db"), "-worktrees", filepath.Join(dir, "wt"))
	stdin := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":"req_multi_1","method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":"req_multi_2","method":"initialize","params":{}}`,
	}, "\n") + "\n")
	cmd.Stdin = stdin
	stdout := &strings.Builder{}
	cmd.Stdout = stdout
	cmd.Stderr = &strings.Builder{}
	if err := cmd.Run(); err != nil {
		t.Fatalf("pantheond run: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 response lines, got %d", len(lines))
	}

	for i, line := range lines {
		var resp map[string]any
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("line %d: unmarshal: %v", i, err)
		}
		if resp["jsonrpc"] != "2.0" {
			t.Fatalf("line %d: jsonrpc = %v", i, resp["jsonrpc"])
		}
	}
}

// TestPantheond_SocketMode verifies the Unix socket long-lived daemon mode.
// It starts pantheond with -socket, connects via Unix socket, sends
// initialize + run.list, and verifies responses from multiple connections.
func TestPantheond_SocketMode(t *testing.T) {
	bin := buildPantheond(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "pantheond.sock")
	dbPath := filepath.Join(dir, "test.db")
	wtDir := filepath.Join(dir, "wt")

	// Start pantheond in socket mode.
	cmd := exec.Command(bin, "-db", dbPath, "-worktrees", wtDir, "-socket", socketPath)
	cmd.Stderr = &strings.Builder{}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start pantheond: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_, _ = cmd.Process.Wait()
	}()

	// Wait for socket to appear.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("socket not created: %v", err)
	}

	// Connection 1: initialize.
	conn1, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn1.Close()

	resp := sendSocketRPC(t, conn1, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sock_init",
		"method":  "initialize",
		"params":  map[string]any{},
	})
	if resp["id"] != "sock_init" {
		t.Fatalf("id = %v, want sock_init", resp["id"])
	}
	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result not object: %v", resp["result"])
	}
	if result["server_name"] != "pantheond" {
		t.Fatalf("server_name = %v", result["server_name"])
	}

	// Connection 2: run.list (independent connection, same daemon).
	conn2, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial socket conn2: %v", err)
	}
	defer conn2.Close()

	resp = sendSocketRPC(t, conn2, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sock_list",
		"method":  "run.list",
		"params":  map[string]any{},
	})
	if resp["id"] != "sock_list" {
		t.Fatalf("id = %v, want sock_list", resp["id"])
	}
	listResult, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result not object: %v", resp["result"])
	}
	runs, ok := listResult["runs"].([]any)
	if !ok {
		t.Fatalf("runs not array: %v", listResult["runs"])
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(runs))
	}

	// Connection 3: project.register (verify write works via socket).
	conn3, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial socket conn3: %v", err)
	}
	defer conn3.Close()

	resp = sendSocketRPC(t, conn3, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sock_reg",
		"method":  "project.register",
		"params": map[string]any{
			"name":      "socket-test",
			"repo_path": "/tmp/test",
			"base_ref":  "main",
		},
	})
	if resp["error"] != nil {
		t.Fatalf("project.register error: %v", resp["error"])
	}
	regResult, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result not object: %v", resp["result"])
	}
	projectID, ok := regResult["project_id"].(string)
	if !ok || !strings.HasPrefix(projectID, "prj_") {
		t.Fatalf("project_id = %v, want prj_*", regResult["project_id"])
	}

	// Connection 4: message.publish.envelope + messages.by_run (message bus via socket).
	conn4, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial socket conn4: %v", err)
	}
	defer conn4.Close()

	resp = sendSocketRPC(t, conn4, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sock_pub",
		"method":  "message.publish.envelope",
		"params": map[string]any{
			"message_id":      "msg_sock_1",
			"run_id":          "run_sock",
			"sender":          map[string]any{"role": "metis"},
			"recipient":       map[string]any{"role": "worker"},
			"type":            "directive",
			"idempotency_key": "idem_sock_1",
			"payload_ref": map[string]any{
				"kind":   "inline",
				"inline": "hello from socket test",
			},
		},
	})
	if resp["error"] != nil {
		t.Fatalf("message.publish.envelope error: %v", resp["error"])
	}
	pubResult, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result not object: %v", resp["result"])
	}
	if pubResult["seq"] == nil {
		t.Fatal("expected non-nil seq")
	}

	// Connection 5: messages.by_run.
	conn5, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial socket conn5: %v", err)
	}
	defer conn5.Close()

	resp = sendSocketRPC(t, conn5, map[string]any{
		"jsonrpc": "2.0",
		"id":      "sock_sub",
		"method":  "messages.by_run",
		"params": map[string]any{
			"run_id": "run_sock",
		},
	})
	if resp["error"] != nil {
		t.Fatalf("messages.by_run error: %v", resp["error"])
	}
	subResult, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result not object: %v", resp["result"])
	}
	messages, ok := subResult["messages"].([]any)
	if !ok {
		t.Fatalf("messages not array: %v", subResult["messages"])
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
}

// sendSocketRPC sends a JSON-RPC request over a Unix socket connection
// and returns the parsed response.
func sendSocketRPC(t *testing.T, conn net.Conn, req map[string]any) map[string]any {
	t.Helper()
	reqBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	reqBytes = append(reqBytes, '\n')

	if _, err := conn.Write(reqBytes); err != nil {
		t.Fatalf("write: %v", err)
	}

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	if !scanner.Scan() {
		t.Fatalf("no response: %v", scanner.Err())
	}
	var resp map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v\nline: %s", err, scanner.Text())
	}
	return resp
}

// --- helpers ---

// buildPantheond builds the pantheond binary and returns its path.
func buildPantheond(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pantheond-test")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = "."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pantheond: %v\n%s", err, out)
	}
	return bin
}

// sendRPC spawns pantheond, sends one JSON-RPC request, and returns the
// parsed response.
func sendRPC(t *testing.T, bin, dir string, req map[string]any) map[string]any {
	t.Helper()
	reqBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	reqBytes = append(reqBytes, '\n')

	cmd := exec.Command(bin, "-db", filepath.Join(dir, "test.db"), "-worktrees", filepath.Join(dir, "wt"))
	cmd.Stdin = strings.NewReader(string(reqBytes))
	stdout := &strings.Builder{}
	cmd.Stdout = stdout
	stderr := &strings.Builder{}
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("pantheond run: %v\nstderr: %s", err, stderr.String())
	}

	output := strings.TrimSpace(stdout.String())
	if output == "" {
		t.Fatalf("no output from pantheond\nstderr: %s", stderr.String())
	}

	// Read the first line (the response).
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	if !scanner.Scan() {
		t.Fatalf("no response line: %s", output)
	}
	line := scanner.Text()

	var resp map[string]any
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("unmarshal response: %v\nline: %s", err, line)
	}
	return resp
}

// Ensure io import is used.
var _ = io.EOF

// Ensure os import is used.
var _ = os.Args

// Ensure time import is used (for potential future use).
var _ = time.Second
