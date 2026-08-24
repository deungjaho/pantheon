#!/usr/bin/env bash
# concierge-inbox.sh — Drop a message into the Concierge inbox.
#
# Usage:
#   concierge-inbox "修复 lacoeur 项目的登录bug"
#   echo "status report" | concierge-inbox
#
# This is the user-facing command to send instructions to Concierge.
# It writes a timestamped file to the inbox directory. The watcher will
# detect it and launch the Concierge agent.
set -euo pipefail

CONCIERGE_DIR="${HOME}/.local/share/pantheon/concierge"
INBOX_DIR="${CONCIERGE_DIR}/inbox"

mkdir -p "$INBOX_DIR"

# Read message from argument or stdin
if [ $# -gt 0 ]; then
    MESSAGE="$*"
else
    MESSAGE=$(cat)
fi

if [ -z "$MESSAGE" ]; then
    echo "concierge-inbox: no message provided" >&2
    exit 1
fi

# Generate timestamped filename
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RAND=$(printf '%03d' $((RANDOM % 1000)))
FILENAME="${TIMESTAMP}-${RAND}.md"
FILEPATH="${INBOX_DIR}/${FILENAME}"

# Write message
echo "$MESSAGE" > "$FILEPATH"

echo "concierge-inbox: queued as ${FILENAME}"
echo "concierge-inbox: ${FILEPATH}"
