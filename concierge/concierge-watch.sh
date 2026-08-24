#!/usr/bin/env bash
# concierge-watch.sh — Watch the Concierge inbox and launch Concierge when items arrive.
#
# This is the persistent watcher. It runs as a systemd service and polls the
# inbox directory every 5 seconds. When new files appear, it launches the
# Concierge Devin agent (via concierge-launch.sh) if it's not already running.
#
# The watcher is intentionally dumb: it does not parse inbox content or make
# decisions. It just detects files and starts the agent.
set -euo pipefail

CONCIERGE_DIR="${HOME}/.local/share/pantheon/concierge"
INBOX_DIR="${CONCIERGE_DIR}/inbox"
LAUNCH_SCRIPT="${HOME}/.local/bin/concierge-launch"
POLL_INTERVAL="${CONCIERGE_POLL_INTERVAL:-5}"
SESSION="concierge"

echo "concierge-watch: started (poll=${POLL_INTERVAL}s, inbox=${INBOX_DIR})"

while true; do
    # Check for pending inbox files
    PENDING=$(find "$INBOX_DIR" -type f -name "*.md" 2>/dev/null | wc -l)

    if [ "$PENDING" -gt 0 ]; then
        # Check if concierge session is already running
        if ! tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "concierge-watch: $PENDING pending item(s), launching concierge..."
            "$LAUNCH_SCRIPT" 2>&1 || true
        fi
    fi

    sleep "$POLL_INTERVAL"
done
