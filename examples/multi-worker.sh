#!/bin/sh
# examples/multi-worker.sh — create two concurrent runs on the same project.
#
# Prerequisites:
#   - pantheond is running (systemctl --user start pantheond, or pantheond -socket ...)
#   - A project is registered (run examples/quick-start.sh first, or
#     pantheon project register manually).
#
# Usage:
#   ./examples/multi-worker.sh <project-id>
#
# This demonstrates the multi-Worker capability (Phase 2 Priority 1): two
# independent runs on independent tasks against the same project, each with
# its own worktree. Both runs are started concurrently.
set -eu

PROJECT_ID=${1:-}
if [ -z "$PROJECT_ID" ]; then
    echo "Usage: $0 <project-id>" >&2
    echo "Get a project-id from: pantheon project list" >&2
    exit 2
fi

echo "==> Creating run #1 (task A)"
RESP=$(pantheon run create --project-id "$PROJECT_ID" --objective "multi-worker task A" --risk-level R1)
echo "$RESP"
RUN_A=$(echo "$RESP" | sed -n 's/.*"run_id":"\(run_[^"]*\)".*/\1/p')
if [ -z "$RUN_A" ]; then
    echo "error: could not parse run_id from run #1 response" >&2
    exit 1
fi
echo "run_a = $RUN_A"

echo
echo "==> Creating run #2 (task B)"
RESP=$(pantheon run create --project-id "$PROJECT_ID" --objective "multi-worker task B" --risk-level R1)
echo "$RESP"
RUN_B=$(echo "$RESP" | sed -n 's/.*"run_id":"\(run_[^"]*\)".*/\1/p')
if [ -z "$RUN_B" ]; then
    echo "error: could not parse run_id from run #2 response" >&2
    exit 1
fi
echo "run_b = $RUN_B"

echo
echo "==> Starting both runs concurrently"
pantheon run start --run-id "$RUN_A" &
PID_A=$!
pantheon run start --run-id "$RUN_B" &
PID_B=$!
wait "$PID_A"
wait "$PID_B"

echo
echo "==> Status of both runs"
echo "--- run A ($RUN_A) ---"
pantheon run status --run-id "$RUN_A"
echo "--- run B ($RUN_B) ---"
pantheon run status --run-id "$RUN_B"

echo
echo "Both runs are now in the running state with independent worktrees."
echo "run_a=$RUN_A  run_b=$RUN_B"
