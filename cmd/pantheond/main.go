// Command pantheond is the omarchy-side daemon for Pantheon.
//
// It supports two modes:
//
//   - stdin/stdout (default): reads line-delimited JSON-RPC 2.0 from stdin,
//     writes responses to stdout. Spawned per SSH invocation by the Mac CLI.
//
//   - Unix socket (-socket PATH): listens on a Unix socket, accepts multiple
//     connections concurrently, each handled in its own goroutine. Used for
//     long-lived daemon mode (systemd service).
//
// Usage:
//
//	pantheond [-db PATH] [-worktrees DIR] [-name NAME] [-version VER] [-socket PATH] [-runtime devin|claude|codex]
//
// The daemon opens the SQLite store at -db (default
// $PANTHEON_HOME/pantheon.db), creates a WorkspaceManager rooted at
// -worktrees (default $PANTHEON_HOME/worktrees), and registers all
// RPC methods on a JSON-RPC server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tangtszho/pantheon/internal/auditor"
	"github.com/tangtszho/pantheon/internal/beacon"
	"github.com/tangtszho/pantheon/internal/checkpoint"
	"github.com/tangtszho/pantheon/internal/domain"
	"github.com/tangtszho/pantheon/internal/hydra"
	"github.com/tangtszho/pantheon/internal/mnemos"
	"github.com/tangtszho/pantheon/internal/notify"
	"github.com/tangtszho/pantheon/internal/push"
	"github.com/tangtszho/pantheon/internal/rpc"
	"github.com/tangtszho/pantheon/internal/runtime"
	"github.com/tangtszho/pantheon/internal/store"
	"github.com/tangtszho/pantheon/internal/wake"
	"github.com/tangtszho/pantheon/internal/workspace"
)

func main() {
	var (
		dbPath       string
		worktrees    string
		serverName   string
		serverVer    string
		retention    string
		socketPath   string
		wakeEnabled  bool
		wakeInterval time.Duration
		wakeBatch    int
		noScanner    bool
		runtimeName  string
		beaconBin    string
		hydraURL     string
		hydraKey     string
		pushSocket   string
		auditorOn    bool
		mnemosURL    string
	)
	flag.StringVar(&dbPath, "db", defaultDBPath(), "SQLite store path")
	flag.StringVar(&worktrees, "worktrees", defaultWorktreesDir(), "worktree base directory")
	flag.StringVar(&serverName, "name", "pantheond", "server name reported by initialize")
	flag.StringVar(&serverVer, "version", "0.1.0-alpha", "server version reported by initialize")
	flag.StringVar(&retention, "retention", "168h", "worktree retention window (e.g. 168h for 7 days)")
	flag.StringVar(&socketPath, "socket", "", "Unix socket path for long-lived daemon mode (empty = stdin/stdout)")
	flag.BoolVar(&wakeEnabled, "wake", false, "enable event-driven wake loop (socket mode only)")
	flag.DurationVar(&wakeInterval, "wake-interval", 5*time.Second, "wake loop poll interval")
	flag.IntVar(&wakeBatch, "wake-batch", 100, "wake loop batch size")
	flag.BoolVar(&noScanner, "no-scanner", false, "disable agent liveness scanner (socket mode only)")
	flag.StringVar(&runtimeName, "runtime", "devin", "runtime adapter: devin, claude, or codex")
	flag.StringVar(&beaconBin, "beacon", "", "path to beacon binary for agent discovery (empty = disabled)")
	flag.StringVar(&hydraURL, "hydra-url", "", "Hydra LLM gateway base URL (empty = disabled)")
	flag.StringVar(&hydraKey, "hydra-key", "", "Hydra API key (optional, sent as Bearer token)")
	flag.StringVar(&pushSocket, "push-socket", "", "Unix socket path for the message bus push server (empty = disabled, pull-based polling only)")
	flag.BoolVar(&auditorOn, "auditor", false, "enable the Global Auditor (Phase 4) for periodic run-history analysis")
	flag.StringVar(&mnemosURL, "mnemos-url", "", "Mnemos memory service base URL (empty = disabled)")
	flag.Parse()

	// Pantheon never writes logs to stdout (that channel is JSON-RPC).
	// Route diagnostics to stderr.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Open the SQLite store.
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		log.Fatalf("pantheond: open store: %v", err)
	}
	defer st.Close()

	// Note: ReconcileAfterCrash is NOT called automatically on startup.
	// In per-request mode, auto-reconcile would incorrectly mark workers
	// from prior requests as lost. In socket mode, the daemon is long-lived
	// so reconcile should be triggered explicitly via the reconcile RPC.

	// Create the workspace manager.
	retDur, err := parseDuration(retention)
	if err != nil {
		log.Fatalf("pantheond: invalid retention %q: %v", retention, err)
	}
	wm := workspace.NewManager(
		workspace.WithBaseDir(worktrees),
		workspace.WithRetention(retDur),
	)

	// Build the RPC service with the selected RuntimeAdapter and CheckpointManager.
	adapter, err := newRuntimeAdapter(runtimeName)
	if err != nil {
		log.Fatalf("pantheond: %v", err)
	}
	checkpointMgr := checkpoint.NewManager()
	tmuxNotifier := notify.NewTmuxNotifier(st)

	// C-004: inbox/outbox projection (best-effort, restricted not projected).
	inboxProjector := notify.NewFileInboxProjector(
		filepath.Join(filepath.Dir(dbPath), "inbox"),
		filepath.Join(filepath.Dir(dbPath), "outbox"),
	)

	svc := &rpc.Service{
		Store:          st,
		WorkspaceMgr:   wm,
		Runtime:        adapter,
		Checkpoint:     checkpointMgr.AsRPCCheckpointManager(),
		Notifier:       tmuxNotifier,
		InboxProjector: inboxProjector,
		ServerName:     serverName,
		ServerVersion:  serverVer,
	}

	// Optional Beacon integration: agent discovery from tmux panes.
	// When -beacon is empty, the integration is disabled and the
	// agent.discover RPC returns "beacon not configured" (degraded mode).
	if beaconBin != "" {
		svc.Beacon = beacon.NewClient(beacon.WithBinaryPath(beaconBin))
		log.Printf("pantheond: beacon agent discovery enabled (binary=%s)", beaconBin)
	}

	// Optional Hydra integration: LLM model routing gateway.
	// When -hydra-url is empty, the integration is disabled and the
	// hydra.models/hydra.health RPCs return "hydra not configured"
	// (degraded mode).
	if hydraURL != "" {
		svc.Hydra = hydra.NewClient(hydraURL, hydraKey)
		log.Printf("pantheond: hydra model routing enabled (url=%s)", hydraURL)
	}

	// Optional message bus push layer (Solution B). When -push-socket is
	// set, start a push server on a separate Unix socket that streams
	// real-time message-published notifications to subscribers. The push
	// layer is on top of the durable SQLite journal — it is a
	// notification, not a replacement. When disabled (empty), the service
	// uses NoopPusher and the system falls back to pull-based cursor
	// polling exactly as before.
	if pushSocket != "" {
		pushSrv := push.NewServer(pushSocket, log.New(os.Stderr, "pantheond: ", log.LstdFlags|log.Lmicroseconds))
		if err := pushSrv.Start(ctx); err != nil {
			log.Fatalf("pantheond: push server start: %v", err)
		}
		defer pushSrv.Close()
		svc.Pusher = pushSrv
		log.Printf("pantheond: message bus push server enabled (socket=%s)", pushSocket)
	}

	// Optional Global Auditor (Phase 4): periodic run-history analysis that
	// produces structured findings (recommendations, memory candidates,
	// policy proposals, risk findings). When -auditor is off (default), the
	// auditor.* RPCs return "auditor not configured" (degraded mode). The
	// auditor does NOT auto-modify anything — findings require human
	// acceptance. Mnemos integration (memory candidates) is deferred;
	// findings are stored locally in SQLite.
	if auditorOn {
		aud := auditor.NewAuditor(auditor.StoreAdapter{
			ListRunsFn:      st.ListRuns,
			ListFindingsFn:  st.ListFindings,
			CreateFindingFn: st.CreateFinding,
			EventsSinceFn:   st.EventsSince,
		}, log.Default())
		svc.Auditor = aud
		log.Printf("pantheond: global auditor enabled")
	}

	// Optional Mnemos integration: semantic memory auto-ingest. When
	// -mnemos-url is set, completed runs (R0/R1 auto-accept, run.approve,
	// or the run.verify approval path) are asynchronously ingested to
	// Mnemos as best-effort memory entries. When empty, the integration is
	// disabled and run completion does not trigger ingest.
	if mnemosURL != "" {
		svc.Mnemos = mnemos.NewClient(mnemosURL)
		log.Printf("pantheond: mnemos auto-ingest enabled (url=%s)", mnemosURL)
	}

	srv := rpc.NewServer(os.Stdout)
	svc.RegisterAll(srv)

	// C-004: enable request_id idempotency for cross-SSH-boundary retries.
	srv.SetIdempotencyStore(st)
	// C-004: enforce 64KB request size limit for SSH stdio mode.
	srv.SetMaxLineSize(rpc.MaxSSHRequestSize)

	// Start the agent liveness scanner (on by default in socket mode).
	// The scanner detects exited agents and triggers continuations.
	// Use -no-scanner to disable (e.g. in tests without a real runtime).
	if socketPath != "" && !noScanner {
		verifier := runtime.NewVerifier(st, runtime.VerifierConfig{
			Timeout: 120 * time.Second,
			Logger:  log.Default(),
		})
		scanner := runtime.NewScanner(st, adapter, runtime.ScannerConfig{
			PollInterval: 10 * time.Second,
			Logger:       log.Default(),
			OnContinuationNeeded: func(ctx context.Context, runID, worktreePath string, remaining int) {
				log.Printf("pantheond: auto-continuing run %s (%d remaining subtasks)", runID, remaining)
				newRunID, err := svc.AutoContinue(ctx, runID)
				if err != nil {
					log.Printf("pantheond: auto-continue failed: %v", err)
					return
				}
				log.Printf("pantheond: auto-continuation started: %s -> %s", runID, newRunID)
			},
			OnAllSubtasksComplete: func(ctx context.Context, runID, worktreePath string) {
				log.Printf("pantheond: auto-verifying run %s", runID)
				result, err := verifier.Verify(ctx, runID, worktreePath)
				if err != nil {
					log.Printf("pantheond: auto-verify failed: %v", err)
					return
				}
				log.Printf("pantheond: auto-verify run %s verdict=%s exitCode=%d",
					runID, result.Verdict, result.ExitCode)
			},
			OnBlocked: func(ctx context.Context, runID, worktreePath string, remaining int) {
				log.Printf("pantheond: run %s BLOCKED — %d subtasks stuck for too many continuations, needs human decision",
					runID, remaining)
			},
		})
		scanner.Start(ctx)
		log.Printf("pantheond: agent scanner enabled (poll=10s)")
	}

	// Start the event-driven wake loop if requested (C-003).
	// Only valid in socket mode — the wake loop needs a long-lived daemon.
	// The wake handler dispatches to the reconciler: the wake loop signals
	// "new events happened", and the reconciler queries the store for the
	// current state (pending continuations, orphaned runs) and acts on it.
	if wakeEnabled && socketPath == "" {
		log.Fatalf("pantheond: -wake requires -socket (long-lived mode)")
	}
	var wakeLoop *wake.Loop
	if wakeEnabled {
		reconciler := wake.NewReconciler(st, st, log.Default(), time.Hour)
		handler := func(ctx context.Context, events []domain.Event) error {
			if len(events) > 0 {
				log.Printf("wake: received %d new events", len(events))
			}
			result, err := reconciler.Tick(ctx)
			if err != nil {
				log.Printf("wake: reconcile tick error: %v", err)
				return err
			}
			log.Printf("wake: reconcile tick checked=%d notified=%d re-notified=%d skipped=%d errors=%d orphaned=%d",
				result.Checked, result.Notified, result.ReNotified, result.Skipped, result.Errors, len(result.OrphanedRuns))
			return nil
		}
		wakeLoop = wake.New(st, handler, wake.Config{
			PollInterval: wakeInterval,
			BatchSize:    wakeBatch,
			Logger:       log.Default(),
		})
		if err := wakeLoop.Start(ctx); err != nil {
			log.Fatalf("pantheond: wake loop start: %v", err)
		}
		log.Printf("pantheond: wake loop enabled (interval=%s batch=%d)", wakeInterval, wakeBatch)
	}

	if socketPath != "" {
		// Long-lived Unix socket mode.
		serveSocket(ctx, srv, socketPath, dbPath, worktrees)
	} else {
		// Per-request stdin/stdout mode.
		log.Printf("pantheond: serving on stdin/stdout (db=%s worktrees=%s)", dbPath, worktrees)
		if err := srv.Serve(ctx, os.Stdin); err != nil {
			log.Fatalf("pantheond: serve: %v", err)
		}
	}
}

// serveSocket listens on a Unix socket and handles connections concurrently.
// The daemon stays alive until the context is cancelled (SIGINT/SIGTERM).
func serveSocket(ctx context.Context, srv *rpc.Server, socketPath, dbPath, worktrees string) {
	// Remove stale socket file if it exists.
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("pantheond: remove stale socket: %v", err)
	}

	// Ensure parent directory exists.
	if dir := filepath.Dir(socketPath); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Fatalf("pantheond: create socket dir: %v", err)
		}
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("pantheond: listen %s: %v", socketPath, err)
	}
	defer ln.Close()

	// Clean up socket file on exit.
	defer os.Remove(socketPath)

	log.Printf("pantheond: serving on Unix socket %s (db=%s worktrees=%s)", socketPath, dbPath, worktrees)

	// Accept connections in a goroutine, close listener on ctx cancel.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("pantheond: accept: %v", err)
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			if err := srv.ServeConn(ctx, c, c); err != nil {
				log.Printf("pantheond: conn ended: %v", err)
			}
		}(conn)
	}
}

// defaultDBPath returns $PANTHEON_HOME/pantheon.db or ~/.local/share/pantheon/pantheon.db.
func defaultDBPath() string {
	if v := os.Getenv("PANTHEON_HOME"); v != "" {
		return filepath.Join(v, "pantheon.db")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "pantheon.db"
	}
	return filepath.Join(home, ".local", "share", "pantheon", "pantheon.db")
}

// defaultWorktreesDir returns $PANTHEON_HOME/worktrees or ~/.local/share/pantheon/worktrees.
func defaultWorktreesDir() string {
	if v := os.Getenv("PANTHEON_HOME"); v != "" {
		return filepath.Join(v, "worktrees")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "worktrees"
	}
	return filepath.Join(home, ".local", "share", "pantheon", "worktrees")
}

// parseDuration wraps time.ParseDuration with a friendlier error.
func parseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", s, err)
	}
	return d, nil
}

// newRuntimeAdapter selects and constructs the RuntimeAdapter named by the
// -runtime flag. Supported values: devin (default), claude, codex.
func newRuntimeAdapter(name string) (rpc.RuntimeAdapter, error) {
	switch name {
	case "devin", "":
		return runtime.NewDevinAdapter(
			runtime.WithModel("glm-5-2"),
			runtime.WithPermissionMode("dangerous"),
		), nil
	case "claude":
		return runtime.NewClaudeAdapter(
			runtime.WithClaudeModel("claude-sonnet-4"),
			runtime.WithClaudePermissionMode("dangerous"),
		), nil
	case "codex":
		return runtime.NewCodexAdapter(
			runtime.WithCodexModel("o4-mini"),
			runtime.WithCodexPermissionMode("dangerous"),
		), nil
	default:
		return nil, fmt.Errorf("unknown runtime %q (want devin, claude, or codex)", name)
	}
}
