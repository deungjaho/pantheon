// Command pantheon is the CLI for Pantheon.
//
// Transport strategy (ADR-0019): local socket first, SSH fallback.
//
//   - Local socket (default): connects to a long-lived pantheond daemon
//     via Unix socket. The default socket path is
//     ~/.local/share/pantheon/pantheond.sock (or $PANTHEON_SOCKET).
//     If the socket exists, the CLI uses it directly.
//
//   - SSH fallback (-host HOST): when no local socket is found, the CLI
//     spawns `ssh <host> pantheond` per request. -host must be explicitly
//     specified (no default hostname).
//
// Usage (semantic subcommands — the user-facing surface):
//
//	pantheon project register --name N --repo-path P --base-ref R
//	pantheon project list
//	pantheon project status --project-id ID
//	pantheon run create --project-id ID --objective OBJ [--base-ref R] [--budget D] [--owner O]
//	pantheon run start --run-id ID
//	pantheon run status --run-id ID
//	pantheon run message --run-id ID --body MSG [--from F] [--to T]
//	pantheon run stop --run-id ID
//	pantheon run resume --run-id ID
//	pantheon run verify --run-id ID
//	pantheon agent register --run-id ID --role ROLE --runtime RT --pid PID [--session-id S]
//	pantheon agent heartbeat --agent-id ID
//	pantheon agent complete --agent-id ID [--exit-code N]
//	pantheon agent block --agent-id ID [--reason R]
//
// Usage (legacy direct mode — debug backdoor, G2.5):
//
//	pantheon <method> [JSON-params]
//	pantheon initialize
//	pantheon -host omarchy run.list
//	pantheon run.submit '{"project_id":"prj_...","objective":"fix bug"}'
//
// The SSH host has no default — specify -host or PANTEON_SSH_HOST when
// no local daemon is running. The daemon path defaults to "pantheond"
// (expected on PATH on the remote host). Override with -daemon or
// PANTEON_DAEMON. The socket path defaults to
// $PANTHEON_SOCKET or ~/.local/share/pantheon/pantheond.sock.
//
// This CLI does not maintain state between invocations. request_id is
// generated per call; idempotency caching is the daemon's responsibility.
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"

	_ "modernc.org/sqlite" // pure-Go SQLite driver for doctor DB probe
)

// transport holds the connection configuration shared by all RPC calls.
type transport struct {
	socketPath string // default = local socket; probed at runtime, SSH fallback if not found
	sshHost    string
	daemonCmd  string
	dbPath     string
	pushSocket string // push server Unix socket (for message subscribe)
}

func main() {
	var (
		sshHost        string
		daemonCmd      string
		dbPath         string
		socketPath     string
		pushSocketPath string
	)
	flag.StringVar(&sshHost, "host", envOr("PANTHEON_SSH_HOST", ""), "SSH host alias (used when no local socket)")
	flag.StringVar(&daemonCmd, "daemon", envOr("PANTHEON_DAEMON", "pantheond"), "daemon command on remote")
	flag.StringVar(&dbPath, "db", "", "remote SQLite path (passed as -db to pantheond; empty = daemon default)")
	flag.StringVar(&socketPath, "socket", envOr("PANTHEON_SOCKET", defaultSocketPath()), "Unix socket path for local daemon mode")
	flag.StringVar(&pushSocketPath, "push-socket", envOr("PANTHEON_PUSH_SOCKET", defaultPushSocketPath()), "Unix socket path for the message bus push server")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <group> <sub> [flags]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s [flags] <method> [json-params]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSemantic groups: project, run, agent, message\n")
		fmt.Fprintf(os.Stderr, "Legacy methods: initialize, project.register, run.submit, run.status, ...\n")
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(2)
	}

	t := &transport{
		socketPath: socketPath,
		sshHost:    sshHost,
		daemonCmd:  daemonCmd,
		dbPath:     dbPath,
		pushSocket: pushSocketPath,
	}

	// Semantic subcommand dispatch: project/run/agent/message/doctor/wake-poll.
	switch args[0] {
	case "project":
		os.Exit(runProjectSub(t, args[1:]))
	case "run":
		os.Exit(runRunSub(t, args[1:]))
	case "agent":
		os.Exit(runAgentSub(t, args[1:]))
	case "message":
		os.Exit(runMessageSub(t, args[1:]))
	case "doctor":
		os.Exit(runDoctor(t, args[1:]))
	case "wake-poll":
		os.Exit(runWakePoll(t))
	}

	// Legacy direct mode (debug backdoor, G2.5): <method> [JSON-params].
	os.Exit(runLegacyDirect(t, args))
}

// runLegacyDirect sends a raw JSON-RPC method with optional JSON params.
func runLegacyDirect(t *transport, args []string) int {
	method := args[0]
	var params json.RawMessage
	if len(args) > 1 {
		params = json.RawMessage(args[1])
		if !json.Valid(params) {
			fmt.Fprintf(os.Stderr, "error: invalid JSON params: %s\n", args[1])
			return 2
		}
	}
	return sendRPC(t, method, params)
}

// sendRPC builds a JSON-RPC 2.0 request, sends it via the configured
// transport, and prints the response to stdout. Returns the process exit
// code (0 on success, 1 on error).
func sendRPC(t *transport, method string, params any) int {
	respBytes, code := rpcRaw(t, method, params)
	if respBytes != nil {
		os.Stdout.Write(respBytes)
		os.Stdout.Write([]byte("\n"))
	}
	return code
}

// rpcRaw builds a JSON-RPC 2.0 request, sends it via the configured
// transport, and returns the raw response bytes plus an exit code (0 on
// success, 1 on error). It does not write the response to stdout — callers
// decide how to present the result. On a transport-level error the returned
// bytes are nil.
func rpcRaw(t *transport, method string, params any) ([]byte, int) {
	var rawParams json.RawMessage
	if params != nil {
		switch v := params.(type) {
		case json.RawMessage:
			rawParams = v
		default:
			b, err := json.Marshal(v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: marshal params: %v\n", err)
				return nil, 1
			}
			rawParams = b
		}
	}

	reqID, err := domain.NewID("req")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: generate request_id: %v\n", err)
		return nil, 1
	}

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      reqID,
		"method":  method,
	}
	if rawParams != nil {
		req["params"] = rawParams
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: marshal request: %v\n", err)
		return nil, 1
	}
	reqBytes = append(reqBytes, '\n')

	var respBytes []byte
	useSocket := false
	if t.socketPath != "" {
		if _, err := os.Stat(t.socketPath); err == nil {
			useSocket = true
		}
	}

	if useSocket {
		resp, err := callSocket(t.socketPath, reqBytes, method)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: socket: %v\n", err)
			return nil, 1
		}
		respBytes = resp
	} else if t.sshHost != "" {
		// SSH fallback: spawn pantheond per request.
		remoteArgs := []string{t.sshHost, t.daemonCmd}
		if t.dbPath != "" {
			remoteArgs = append(remoteArgs, "-db", t.dbPath)
		}

		cmd := exec.Command("ssh", remoteArgs...)
		cmd.Stdin = bytes.NewReader(reqBytes)
		cmd.Stderr = os.Stderr

		var err error
		respBytes, err = cmd.Output()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return nil, exitErr.ExitCode()
			}
			fmt.Fprintf(os.Stderr, "error: ssh: %v\n", err)
			return nil, 1
		}
	} else {
		fmt.Fprintf(os.Stderr, "error: no local daemon socket at %s and no -host specified\n", t.socketPath)
		fmt.Fprintf(os.Stderr, "       start a local daemon or specify -host <ssh-alias>\n")
		return nil, 1
	}

	// D6: check for JSON-RPC error field and exit nonzero if present.
	var resp struct {
		Error *domain.Error `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		// Not valid JSON — can't check for error. Return the bytes with
		// code 0 to preserve existing behavior for non-JSON responses.
		return respBytes, 0
	}
	if resp.Error != nil {
		return respBytes, 1
	}
	return respBytes, 0
}

// --- project subcommands ---

func runProjectSub(t *transport, args []string) int {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pantheon project <register|list|status> [flags]\n")
		return 2
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "register":
		fs := flag.NewFlagSet("project register", flag.ExitOnError)
		name := fs.String("name", "", "project name")
		repoPath := fs.String("repo-path", "", "repository path")
		baseRef := fs.String("base-ref", "", "base ref (branch/tag/commit)")
		fs.Parse(rest)
		if *name == "" || *repoPath == "" || *baseRef == "" {
			fmt.Fprintf(os.Stderr, "error: --name, --repo-path, --base-ref are required\n")
			return 2
		}
		return sendRPC(t, "project.register", map[string]string{
			"name": *name, "repo_path": *repoPath, "base_ref": *baseRef,
		})
	case "list":
		return sendRPC(t, "project.list", nil)
	case "status":
		fs := flag.NewFlagSet("project status", flag.ExitOnError)
		projectID := fs.String("project-id", "", "project ID")
		fs.Parse(rest)
		if *projectID == "" {
			fmt.Fprintf(os.Stderr, "error: --project-id is required\n")
			return 2
		}
		return sendRPC(t, "project.status", map[string]string{
			"project_id": *projectID,
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown project subcommand: %s\n", sub)
		return 2
	}
}

// --- run subcommands ---

func runRunSub(t *transport, args []string) int {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pantheon run <create|start|status|message|stop|resume|verify> [flags]\n")
		return 2
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "create":
		fs := flag.NewFlagSet("run create", flag.ExitOnError)
		projectID := fs.String("project-id", "", "project ID")
		objective := fs.String("objective", "", "run objective")
		baseRef := fs.String("base-ref", "", "base ref override (default: project base)")
		budget := fs.Duration("budget", 0, "budget duration (default: 8h)")
		owner := fs.String("owner", "", "run owner (default: local-user)")
		continueFrom := fs.String("continue-from", "", "previous run ID to continue from (reuses worktree)")
		riskLevel := fs.String("risk-level", "", "risk level R0-R3 (default: R2, requires human approval)")
		fs.Parse(rest)
		if *projectID == "" || *objective == "" {
			fmt.Fprintf(os.Stderr, "error: --project-id and --objective are required\n")
			return 2
		}
		params := map[string]any{
			"project_id": *projectID,
			"objective":  *objective,
		}
		if *baseRef != "" {
			params["base_ref"] = *baseRef
		}
		if *budget != 0 {
			params["budget"] = *budget
		}
		if *owner != "" {
			params["owner"] = *owner
		}
		if *continueFrom != "" {
			params["continue_from"] = *continueFrom
		}
		if *riskLevel != "" {
			params["risk_level"] = *riskLevel
		}
		return sendRPC(t, "run.create", params)
	case "start":
		fs := flag.NewFlagSet("run start", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		return sendRPC(t, "run.start", map[string]string{"run_id": *runID})
	case "status":
		fs := flag.NewFlagSet("run status", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		return sendRPC(t, "run.status", map[string]string{"run_id": *runID})
	case "message":
		fs := flag.NewFlagSet("run message", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		body := fs.String("body", "", "message body")
		from := fs.String("from", "", "sender role (default: metis)")
		to := fs.String("to", "", "recipient role (default: pm)")
		fs.Parse(rest)
		if *runID == "" || *body == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id and --body are required\n")
			return 2
		}
		senderRole := domain.RoleMetis
		if *from != "" {
			senderRole = domain.AgentRole(*from)
		}
		recipientRole := domain.RolePM
		if *to != "" {
			recipientRole = domain.AgentRole(*to)
		}
		params := map[string]any{
			"run_id":          *runID,
			"sender":          map[string]any{"role": string(senderRole)},
			"recipient":       map[string]any{"role": string(recipientRole)},
			"type":            string(domain.MsgDirective),
			"idempotency_key": "cli_" + *runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
			"payload_ref":     map[string]any{"kind": "inline", "inline": *body},
		}
		return sendRPC(t, "message.publish.envelope", params)
	case "stop":
		fs := flag.NewFlagSet("run stop", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		// §8.1 stop semantics: running → blocked (resumable). The v2
		// run.block method transitions the run to V2 "blocked" state
		// directly, preserving the stop→resume sequence
		// (acceptance-contract G3.1).
		return sendRPC(t, "run.block", map[string]string{"run_id": *runID})
	case "resume":
		fs := flag.NewFlagSet("run resume", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		return sendRPC(t, "run.unblock", map[string]string{"run_id": *runID})
	case "verify":
		fs := flag.NewFlagSet("run verify", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		verifier := fs.String("verifier", "", "verifier agent ID (required)")
		verdict := fs.String("verdict", "", "verdict: PASS or FAIL (required)")
		evidence := fs.String("evidence", "", "evidence reference (event_id or artifact ref, required)")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		if *verifier == "" {
			fmt.Fprintf(os.Stderr, "error: --verifier is required (verifier agent ID)\n")
			return 2
		}
		if *verdict != "PASS" && *verdict != "FAIL" {
			fmt.Fprintf(os.Stderr, "error: --verdict must be PASS or FAIL\n")
			return 2
		}
		if *evidence == "" {
			fmt.Fprintf(os.Stderr, "error: --evidence is required (event_id or artifact ref)\n")
			return 2
		}
		return sendRPC(t, "run.verify", map[string]string{
			"run_id":            *runID,
			"verifier_agent_id": *verifier,
			"verdict":           *verdict,
			"evidence_ref":      *evidence,
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown run subcommand: %s\n", sub)
		return 2
	}
}

// --- agent subcommands ---

func runAgentSub(t *transport, args []string) int {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pantheon agent <register|heartbeat|complete|block> [flags]\n")
		return 2
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "register":
		fs := flag.NewFlagSet("agent register", flag.ExitOnError)
		runID := fs.String("run-id", "", "run ID")
		role := fs.String("role", "worker", "agent role (controller/worker)")
		runtime := fs.String("runtime", "devin", "runtime name")
		pid := fs.Int("pid", 0, "agent PID")
		sessionID := fs.String("session-id", "", "session ID (optional)")
		fs.Parse(rest)
		if *runID == "" {
			fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
			return 2
		}
		params := map[string]any{
			"run_id":  *runID,
			"role":    *role,
			"runtime": *runtime,
			"pid":     *pid,
		}
		if *sessionID != "" {
			params["session_id"] = *sessionID
		}
		return sendRPC(t, "agent.register", params)
	case "heartbeat":
		fs := flag.NewFlagSet("agent heartbeat", flag.ExitOnError)
		agentID := fs.String("agent-id", "", "agent ID")
		fs.Parse(rest)
		if *agentID == "" {
			fmt.Fprintf(os.Stderr, "error: --agent-id is required\n")
			return 2
		}
		return sendRPC(t, "agent.heartbeat", map[string]string{"agent_id": *agentID})
	case "complete":
		fs := flag.NewFlagSet("agent complete", flag.ExitOnError)
		agentID := fs.String("agent-id", "", "agent ID")
		exitCode := fs.Int("exit-code", 0, "exit code")
		fs.Parse(rest)
		if *agentID == "" {
			fmt.Fprintf(os.Stderr, "error: --agent-id is required\n")
			return 2
		}
		code := *exitCode
		return sendRPC(t, "agent.complete", map[string]any{
			"agent_id":  *agentID,
			"exit_code": code,
		})
	case "block":
		fs := flag.NewFlagSet("agent block", flag.ExitOnError)
		agentID := fs.String("agent-id", "", "agent ID")
		reason := fs.String("reason", "", "block reason")
		fs.Parse(rest)
		if *agentID == "" {
			fmt.Fprintf(os.Stderr, "error: --agent-id is required\n")
			return 2
		}
		params := map[string]any{"agent_id": *agentID}
		if *reason != "" {
			params["reason"] = *reason
		}
		return sendRPC(t, "agent.block", params)
	default:
		fmt.Fprintf(os.Stderr, "unknown agent subcommand: %s\n", sub)
		return 2
	}
}

// --- message subcommands ---

// runMessageSub dispatches the message subcommand group:
//
//	pantheon message subscribe --run-id ID [--run-id ID2 ...]
//	pantheon message receive  --run-id ID [--cursor N] [--limit L]
func runMessageSub(t *transport, args []string) int {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: pantheon message <subscribe|receive> [flags]\n")
		return 2
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "subscribe":
		return runMessageSubscribe(t, rest)
	case "receive":
		return runMessageReceive(t, rest)
	default:
		fmt.Fprintf(os.Stderr, "unknown message subcommand: %s\n", sub)
		return 2
	}
}

// runMessageSubscribe connects to the push server Unix socket, sends a
// subscription request, and prints real-time message-published
// notifications as they arrive (one JSON object per line). It tracks the
// last message_seq seen per run_id; on disconnect it prints a cursor
// fallback hint so the user can recover missed messages via
// `pantheon message receive --run-id ID --cursor N`.
func runMessageSubscribe(t *transport, args []string) int {
	fs := flag.NewFlagSet("message subscribe", flag.ExitOnError)
	// runIDs is collected via repeated --run-id flags.
	runIDs := repeatedStringFlag(fs, "run-id", "run ID to watch (repeatable; empty = all runs)")
	fs.Parse(args)

	if t.pushSocket == "" {
		fmt.Fprintf(os.Stderr, "error: no push socket configured (use -push-socket or PANTEON_PUSH_SOCKET)\n")
		return 1
	}
	if _, err := os.Stat(t.pushSocket); err != nil {
		fmt.Fprintf(os.Stderr, "error: push socket not found at %s (is the daemon started with -push-socket?)\n", t.pushSocket)
		return 1
	}

	conn, err := net.Dial("unix", t.pushSocket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: dial push socket %s: %v\n", t.pushSocket, err)
		return 1
	}
	defer conn.Close()

	// Send the subscription request (one JSON line).
	req := map[string]any{"run_ids": runIDs}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: marshal subscription: %v\n", err)
		return 1
	}
	if _, err := conn.Write(append(reqBytes, '\n')); err != nil {
		fmt.Fprintf(os.Stderr, "error: write subscription: %v\n", err)
		return 1
	}

	// lastSeq tracks the highest message_seq seen per run_id, for the
	// cursor-fallback hint printed on disconnect.
	lastSeq := make(map[string]int64)

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Print the raw notification JSON line to stdout.
		os.Stdout.Write(line)
		os.Stdout.Write([]byte("\n"))

		// Track the cursor for the fallback hint.
		var n struct {
			RunID      string `json:"run_id"`
			MessageSeq int64  `json:"message_seq"`
		}
		if json.Unmarshal(line, &n) == nil && n.RunID != "" {
			if prev, ok := lastSeq[n.RunID]; !ok || n.MessageSeq > prev {
				lastSeq[n.RunID] = n.MessageSeq
			}
		}
	}

	// The connection ended (server shutdown, network drop, or EOF).
	// Print a cursor-fallback hint to stderr so the user can recover any
	// missed messages via the pull-based `message receive` command.
	fmt.Fprintf(os.Stderr, "push: disconnected from %s\n", t.pushSocket)
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "push: read error: %v\n", err)
	}
	if len(lastSeq) == 0 {
		fmt.Fprintf(os.Stderr, "push: no notifications received — no cursor to recover\n")
	} else {
		fmt.Fprintf(os.Stderr, "push: to recover missed messages, run:\n")
		for runID, seq := range lastSeq {
			fmt.Fprintf(os.Stderr, "  pantheon message receive --run-id %s --cursor %d\n", runID, seq)
		}
	}
	return 0
}

// runMessageReceive calls the messages.by_run RPC method and prints each
// message as a JSON line, followed by the next cursor for pagination.
func runMessageReceive(t *transport, args []string) int {
	fs := flag.NewFlagSet("message receive", flag.ExitOnError)
	runID := fs.String("run-id", "", "run ID (required)")
	cursor := fs.Int64("cursor", 0, "message_seq cursor (returns messages with seq > cursor)")
	limit := fs.Int("limit", 100, "max messages to return")
	fs.Parse(args)
	if *runID == "" {
		fmt.Fprintf(os.Stderr, "error: --run-id is required\n")
		return 2
	}

	params := map[string]any{
		"run_id": *runID,
		"cursor": *cursor,
		"limit":  *limit,
	}
	respBytes, code := rpcRaw(t, "messages.by_run", params)
	if code != 0 {
		// rpcRaw already printed the error; write any partial response.
		if respBytes != nil {
			os.Stdout.Write(respBytes)
			os.Stdout.Write([]byte("\n"))
		}
		return code
	}

	// Parse the JSON-RPC response result.
	var resp struct {
		Result struct {
			Messages   []json.RawMessage `json:"messages"`
			NextCursor int64             `json:"next_cursor"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		// Fall back to printing the raw response.
		os.Stdout.Write(respBytes)
		os.Stdout.Write([]byte("\n"))
		fmt.Fprintf(os.Stderr, "error: parse messages.by_run response: %v\n", err)
		return 1
	}

	// Print each message as a compact JSON line.
	for _, m := range resp.Result.Messages {
		os.Stdout.Write(m)
		os.Stdout.Write([]byte("\n"))
	}
	// Print the next cursor to stderr so it doesn't interleave with the
	// JSON message stream on stdout.
	fmt.Fprintf(os.Stderr, "next_cursor: %d\n", resp.Result.NextCursor)
	if len(resp.Result.Messages) < *limit {
		fmt.Fprintf(os.Stderr, "no more messages\n")
	} else {
		fmt.Fprintf(os.Stderr, "more messages may be available — run again with --cursor %d\n", resp.Result.NextCursor)
	}
	return 0
}

// repeatedStringFlag registers a repeatable string flag on fs and returns a
// pointer to a slice that accumulates all values. Each occurrence of the
// flag appends to the slice.
func repeatedStringFlag(fs *flag.FlagSet, name, usage string) *[]string {
	var vals []string
	fs.Func(name, usage, func(v string) error {
		vals = append(vals, v)
		return nil
	})
	return &vals
}

// callSocket connects to a Unix socket, sends the request, and copies
// the response to stdout. The run.events RPC method returns a bounded
// event list (not a stream), so it is treated like any other method:
// read one JSON-RPC response line.
func callSocket(socketPath string, reqBytes []byte, method string) ([]byte, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", socketPath, err)
	}
	defer conn.Close()

	if _, err := conn.Write(reqBytes); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	// Read one response line.
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	if scanner.Scan() {
		return scanner.Bytes(), nil
	}
	return nil, scanner.Err()
}

// envOr returns the env var value or fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// defaultSocketPath returns the default Unix socket path for the local
// pantheond daemon: $XDG_DATA_HOME/pantheon/pantheond.sock, or
// ~/.local/share/pantheon/pantheond.sock if XDG_DATA_HOME is unset.
func defaultSocketPath() string {
	xdg := os.Getenv("XDG_DATA_HOME")
	if xdg != "" {
		return xdg + "/pantheon/pantheond.sock"
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/pantheond.sock"
	}
	return home + "/.local/share/pantheon/pantheond.sock"
}

// defaultPushSocketPath returns the default Unix socket path for the
// message bus push server: the same directory as the RPC socket but with
// the filename pantheond-push.sock.
func defaultPushSocketPath() string {
	xdg := os.Getenv("XDG_DATA_HOME")
	if xdg != "" {
		return xdg + "/pantheon/pantheond-push.sock"
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/pantheond-push.sock"
	}
	return home + "/.local/share/pantheon/pantheond-push.sock"
}

// doctorStatus is the status of a single doctor check.
type doctorStatus string

const (
	doctorOK     doctorStatus = "ok"
	doctorFailed doctorStatus = "failed"
	doctorInfo   doctorStatus = "info"
)

// doctorCheck is a single diagnostic check result.
type doctorCheck struct {
	Name   string       `json:"name"`
	Status doctorStatus `json:"status"`
	Detail string       `json:"detail,omitempty"`
}

// doctorReport is the full doctor report (used for --json output).
type doctorReport struct {
	Checks []doctorCheck `json:"checks"`
	OK     bool          `json:"ok"`
}

// runDoctor runs diagnostic checks and exits. With --json it emits
// machine-readable output; otherwise it prints the legacy text format
// ("ok|failed|info  <name>  [detail]"), one check per line.
func runDoctor(t *transport, args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON output")
	fs.Parse(args)

	var checks []doctorCheck

	// 1. Binary self-check.
	checks = append(checks, doctorCheck{Name: "pantheon", Status: doctorOK, Detail: "cli binary reachable"})

	// 2. Local socket probe.
	socketExists := false
	if t.socketPath != "" {
		if _, err := os.Stat(t.socketPath); err == nil {
			socketExists = true
		}
	}

	daemonReachable := false
	if socketExists {
		checks = append(checks, doctorCheck{Name: "socket", Status: doctorOK, Detail: t.socketPath})
		// 3. Daemon connectivity via initialize RPC.
		// Use rpcRaw (not sendRPC) so the response is not written to stdout.
		_, code := rpcRaw(t, "initialize", nil)
		if code == 0 {
			checks = append(checks, doctorCheck{Name: "daemon", Status: doctorOK, Detail: "initialize succeeded"})
			daemonReachable = true
		} else {
			checks = append(checks, doctorCheck{Name: "daemon", Status: doctorFailed, Detail: "socket exists but initialize failed"})
		}
	} else if t.sshHost != "" {
		checks = append(checks, doctorCheck{Name: "socket", Status: doctorInfo, Detail: fmt.Sprintf("no local socket; will use SSH -host %s", t.sshHost)})
		_, code := rpcRaw(t, "initialize", nil)
		if code == 0 {
			checks = append(checks, doctorCheck{Name: "daemon", Status: doctorOK, Detail: fmt.Sprintf("initialize succeeded via SSH %s", t.sshHost)})
			daemonReachable = true
		} else {
			checks = append(checks, doctorCheck{Name: "daemon", Status: doctorFailed, Detail: fmt.Sprintf("SSH %s unreachable", t.sshHost)})
		}
	} else {
		checks = append(checks, doctorCheck{Name: "socket", Status: doctorInfo, Detail: fmt.Sprintf("%s not found, no -host specified", t.socketPath)})
		checks = append(checks, doctorCheck{Name: "daemon", Status: doctorInfo, Detail: "no local daemon and no SSH host"})
	}

	// 4. Push server connectivity (if configured).
	if t.pushSocket != "" {
		if _, err := os.Stat(t.pushSocket); err == nil {
			checks = append(checks, doctorCheck{Name: "push-server", Status: doctorOK, Detail: t.pushSocket})
		} else {
			checks = append(checks, doctorCheck{Name: "push-server", Status: doctorInfo, Detail: fmt.Sprintf("not found at %s (start daemon with -push-socket to enable)", t.pushSocket)})
		}
	} else {
		checks = append(checks, doctorCheck{Name: "push-server", Status: doctorInfo, Detail: "not configured (pull-based polling only)"})
	}

	// 5. Database health (local mode only: open read-only, read schema_version).
	// In SSH mode the CLI cannot access the remote DB file, so this check is
	// skipped. The daemon's initialize success already implies the DB is
	// openable on the daemon side.
	dbPath := deriveDBPath(t)
	if dbPath != "" && socketExists {
		checks = append(checks, probeDatabase(dbPath))
	} else if daemonReachable {
		checks = append(checks, doctorCheck{Name: "database", Status: doctorInfo, Detail: "db file not accessible locally (remote/SSH mode); daemon initialize implies open"})
	} else {
		checks = append(checks, doctorCheck{Name: "database", Status: doctorInfo, Detail: "skipped (daemon unreachable)"})
	}

	// 6. Beacon connectivity (if configured) — via agent.discover RPC.
	if daemonReachable {
		checks = append(checks, probeIntegration(t, "beacon", "agent.discover", "beacon not configured"))
		// 7. Hydra connectivity (if configured) — via hydra.health RPC.
		checks = append(checks, probeIntegration(t, "hydra", "hydra.health", "hydra not configured"))
		// 8. Auditor availability (if configured) — via auditor.findings RPC.
		checks = append(checks, probeIntegration(t, "auditor", "auditor.findings", "auditor not configured"))
	} else {
		for _, name := range []string{"beacon", "hydra", "auditor"} {
			checks = append(checks, doctorCheck{Name: name, Status: doctorInfo, Detail: "skipped (daemon unreachable)"})
		}
	}

	// 9. Systemd service status (if running under systemd).
	checks = append(checks, probeSystemd())

	// 10. Disk space for worktrees.
	checks = append(checks, probeWorktreeDisk())

	// Aggregate.
	ok := true
	for _, c := range checks {
		if c.Status == doctorFailed {
			ok = false
			break
		}
	}

	if *jsonOut {
		report := doctorReport{Checks: checks, OK: ok}
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: marshal doctor report: %v\n", err)
			return 1
		}
		os.Stdout.Write(b)
		os.Stdout.Write([]byte("\n"))
	} else {
		for _, c := range checks {
			fmt.Printf("%-8s %s", c.Status, c.Name)
			if c.Detail != "" {
				fmt.Printf(" (%s)", c.Detail)
			}
			fmt.Println()
		}
	}

	if !ok {
		return 1
	}
	return 0
}

// probeDatabase opens the SQLite file read-only and reads the schema_version
// from the meta table. This is a best-effort, read-only diagnostic: it never
// runs migrations or writes. If the file is missing or unreadable, it returns
// an "info" check (the daemon's initialize is the authoritative DB check).
func probeDatabase(dbPath string) doctorCheck {
	if _, err := os.Stat(dbPath); err != nil {
		return doctorCheck{Name: "database", Status: doctorInfo, Detail: fmt.Sprintf("file not found at %s", dbPath)}
	}
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(2000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return doctorCheck{Name: "database", Status: doctorInfo, Detail: fmt.Sprintf("open: %v", err)}
	}
	defer db.Close()
	var version string
	err = db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&version)
	if err != nil {
		return doctorCheck{Name: "database", Status: doctorFailed, Detail: fmt.Sprintf("read schema_version: %v", err)}
	}
	return doctorCheck{Name: "database", Status: doctorOK, Detail: fmt.Sprintf("schema_version=%s", version)}
}

// probeIntegration calls an optional-integration RPC method and classifies the
// result. If the daemon returns the "not configured" error, the check is
// "info" (the integration is optional). A successful RPC is "ok". Any other
// error is "failed".
func probeIntegration(t *transport, name, method, notConfiguredMsg string) doctorCheck {
	respBytes, code := rpcRaw(t, method, nil)
	if code == 0 {
		return doctorCheck{Name: name, Status: doctorOK, Detail: "reachable"}
	}
	// Inspect the error message to distinguish "not configured" from a real failure.
	if respBytes != nil {
		var resp struct {
			Error *domain.Error `json:"error"`
		}
		if json.Unmarshal(respBytes, &resp) == nil && resp.Error != nil {
			msg := resp.Error.Message
			if strings.Contains(msg, notConfiguredMsg) {
				return doctorCheck{Name: name, Status: doctorInfo, Detail: "not configured (optional)"}
			}
			return doctorCheck{Name: name, Status: doctorFailed, Detail: msg}
		}
	}
	return doctorCheck{Name: name, Status: doctorFailed, Detail: "RPC error"}
}

// probeSystemd checks whether pantheond is running as a systemd user service.
// If systemctl is unavailable or the service is not managed by systemd, it
// reports "info" (not a failure — the daemon may be running manually).
func probeSystemd() doctorCheck {
	cmd := exec.Command("systemctl", "--user", "is-active", "pantheond.service")
	out, err := cmd.Output()
	if err != nil {
		// systemctl missing or service not loaded — not a failure.
		return doctorCheck{Name: "systemd", Status: doctorInfo, Detail: "not running as systemd user service (or systemctl unavailable)"}
	}
	state := strings.TrimSpace(string(out))
	if state == "active" {
		return doctorCheck{Name: "systemd", Status: doctorOK, Detail: "pantheond.service active"}
	}
	return doctorCheck{Name: "systemd", Status: doctorInfo, Detail: fmt.Sprintf("pantheond.service %s", state)}
}

// probeWorktreeDisk checks that the worktree directory exists and reports free
// disk space. The worktree directory defaults to $PANTHEON_HOME/worktrees or
// ~/.local/share/pantheon/worktrees. If the directory does not exist yet (no
// runs have been created), it reports "info" rather than "failed".
func probeWorktreeDisk() doctorCheck {
	dir := defaultWorktreesDir()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return doctorCheck{Name: "worktrees", Status: doctorInfo, Detail: fmt.Sprintf("dir %s not found (created on first run)", dir)}
	}
	freeBytes := uint64(st.Bavail) * uint64(st.Bsize)
	freeMB := freeBytes / (1024 * 1024)
	return doctorCheck{Name: "worktrees", Status: doctorOK, Detail: fmt.Sprintf("%s (%d MB free)", dir, freeMB)}
}

// deriveDBPath returns the SQLite database path the daemon is expected to use.
// In local socket mode it uses -db if provided, otherwise the default
// ($PANTHEON_HOME/pantheon.db or ~/.local/share/pantheon/pantheon.db). In SSH
// mode it returns the -db value if set, or empty (the remote DB path is
// unknown to the CLI).
func deriveDBPath(t *transport) string {
	if t.dbPath != "" {
		return t.dbPath
	}
	if t.sshHost != "" {
		// Remote mode: we don't know the remote DB path.
		return ""
	}
	// Local mode: use the same default as the daemon.
	if v := os.Getenv("PANTHEON_HOME"); v != "" {
		return filepath.Join(v, "pantheon.db")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".local", "share", "pantheon", "pantheon.db")
}

// defaultWorktreesDir returns the default worktree directory: $PANTHEON_HOME/worktrees
// or ~/.local/share/pantheon/worktrees.
func defaultWorktreesDir() string {
	if v := os.Getenv("PANTHEON_HOME"); v != "" {
		return filepath.Join(v, "worktrees")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/pantheon-worktrees"
	}
	return filepath.Join(home, ".local", "share", "pantheon", "worktrees")
}

// runWakePoll triggers a single continuation reconcile tick via the
// reconcile.continuations RPC method. This is the timer-driven fallback
// path: systemd timer → pantheon wake-poll → daemon RPC → reconciler.Tick.
// It connects to the daemon's Unix socket (or SSH fallback) and prints the
// reconcile result counts.
func runWakePoll(t *transport) int {
	return sendRPC(t, "reconcile.continuations", nil)
}

// Ensure time import is used (for --budget flag duration parsing).
var _ = time.Second
