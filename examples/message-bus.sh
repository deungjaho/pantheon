#!/bin/sh
# examples/message-bus.sh — publish a message, then subscribe to messages.
#
# Prerequisites:
#   - pantheond is running WITH a push socket:
#       pantheond -socket ~/.local/share/pantheon/pantheond.sock \
#                 -push-socket ~/.local/share/pantheon/pantheond-push.sock -wake
#   - A run already exists (run pantheon run create + run start first, or
#     run examples/quick-start.sh to get one).
#
# Usage:
#   ./examples/message-bus.sh <run-id>
#
# This script publishes one message, then subscribes to the push socket for a
# few seconds so you can see the real-time notification. In a real workflow
# you would run `pantheon message subscribe` in a separate terminal while
# publishing from another.
set -eu

RUN_ID=${1:-}
if [ -z "$RUN_ID" ]; then
    echo "Usage: $0 <run-id>" >&2
    echo "Get a run-id from: pantheon run create --project-id prj_... --objective ..." >&2
    exit 2
fi

echo "==> Publishing a message to run $RUN_ID"
RESP=$(pantheon run message --run-id "$RUN_ID" --body "hello from message-bus example")
echo "$RESP"

echo
echo "==> Pulling recent messages for run $RUN_ID"
pantheon message receive --run-id "$RUN_ID"

echo
echo "==> Subscribing to real-time notifications for 5 seconds..."
echo "(In production, leave this running. Press Ctrl-C to stop early.)"
# Subscribe in the background; kill it after 5 seconds.
pantheon message subscribe --run-id "$RUN_ID" &
SUB_PID=$!
sleep 5
kill "$SUB_PID" 2>/dev/null || true
wait "$SUB_PID" 2>/dev/null || true

echo
echo "Done. To watch continuously, run in a separate terminal:"
echo "  pantheon message subscribe --run-id $RUN_ID"
