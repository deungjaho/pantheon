#!/bin/sh
# examples/quick-start.sh — register a project, create a run, start it, check status.
#
# Prerequisites:
#   - pantheond is running (systemctl --user start pantheond, or pantheond -socket ...)
#   - pantheon and pantheond are on PATH (or run deploy/install.sh first)
#   - A git repo with at least one commit
#
# Usage:
#   ./examples/quick-start.sh /path/to/repo [base-ref]
#
# This script uses the real pantheon CLI. It prints each command before
# running it so you can follow along. Set PANTEON_SOCKET to point at a
# non-default socket if needed.
set -eu

REPO_PATH=${1:-}
BASE_REF=${2:-main}

if [ -z "$REPO_PATH" ]; then
    echo "Usage: $0 /path/to/repo [base-ref]" >&2
    exit 2
fi

# Resolve to an absolute path so the daemon sees it correctly.
REPO_PATH=$(cd "$REPO_PATH" && pwd)

echo "==> Registering project from $REPO_PATH (base-ref: $BASE_REF)"
RESP=$(pantheon project register --name demo --repo-path "$REPO_PATH" --base-ref "$BASE_REF")
echo "$RESP"
PROJECT_ID=$(echo "$RESP" | sed -n 's/.*"project_id":"\(prj_[^"]*\)".*/\1/p')
if [ -z "$PROJECT_ID" ]; then
    echo "error: could not parse project_id from response" >&2
    exit 1
fi
echo "project_id = $PROJECT_ID"

echo
echo "==> Listing projects"
pantheon project list

echo
echo "==> Creating a run"
RESP=$(pantheon run create --project-id "$PROJECT_ID" --objective "quick-start demo run" --risk-level R1)
echo "$RESP"
RUN_ID=$(echo "$RESP" | sed -n 's/.*"run_id":"\(run_[^"]*\)".*/\1/p')
if [ -z "$RUN_ID" ]; then
    echo "error: could not parse run_id from response" >&2
    exit 1
fi
echo "run_id = $RUN_ID"

echo
echo "==> Starting the run"
pantheon run start --run-id "$RUN_ID"

echo
echo "==> Checking run status"
pantheon run status --run-id "$RUN_ID"

echo
echo "Done. run_id=$RUN_ID"
echo "Next: pantheon run stop --run-id $RUN_ID   # then resume with: pantheon run resume --run-id $RUN_ID"
