#!/usr/bin/env bash
# concierge-launch.sh — Start the Concierge Devin agent in a tmux session.
#
# Concierge reads pending inbox files, translates them to pantheon CLI
# operations, executes them, and exits when done.
#
# This script is called by concierge-watch.sh when new inbox items arrive.
# It can also be run manually for testing.
set -euo pipefail

CONCIERGE_DIR="${HOME}/.local/share/pantheon/concierge"
INBOX_DIR="${CONCIERGE_DIR}/inbox"
PROCESSED_DIR="${CONCIERGE_DIR}/processed"
LOG_DIR="${CONCIERGE_DIR}/logs"
PROMPT_FILE="${CONCIERGE_DIR}/system-prompt.md"
SESSION="concierge"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="${LOG_DIR}/concierge-${TIMESTAMP}.log"

# Ensure dirs exist
mkdir -p "$INBOX_DIR" "$PROCESSED_DIR" "$LOG_DIR"

# Check if there are pending inbox items
PENDING=$(find "$INBOX_DIR" -type f -name "*.md" 2>/dev/null | wc -l)
if [ "$PENDING" -eq 0 ]; then
    echo "concierge: no pending inbox items, not starting" >&2
    exit 0
fi

# Check if concierge session is already running
if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "concierge: session already running, skipping" >&2
    exit 0
fi

# Build the prompt: system prompt + current inbox listing
PROMPT=$(cat <<EOF
$(cat "$PROMPT_FILE")

---

## Current inbox

The following files are pending in your inbox. Read each one, process it, and exit when done.

$(for f in $(find "$INBOX_DIR" -type f -name "*.md" | sort); do
    echo "### $(basename "$f")"
    echo '```'
    cat "$f"
    echo '```'
    echo
done)

Process each inbox file now. After processing all files, exit.
EOF
)

# Write the combined prompt to a temp file
COMBINED_PROMPT="${CONCIERGE_DIR}/.combined-prompt-${TIMESTAMP}.md"
echo "$PROMPT" > "$COMBINED_PROMPT"

# Start Devin in a tmux session
# -p: print mode (non-interactive, process and exit)
# --prompt-file: load prompt from file
# --permission-mode dangerous: auto-approve (concierge only runs pantheon CLI)
# --model: use the configured model
DEVIN_MODEL="${DEVIN_MODEL:-claude-sonnet-4}"

tmux new-session -d -s "$SESSION" \
    "devin -p --prompt-file '$COMBINED_PROMPT' --permission-mode dangerous --model '$DEVIN_MODEL' 2>&1 | tee '$LOG_FILE'; echo '---CONCIERGE EXIT CODE: '$? >> '$LOG_FILE'; rm -f '$COMBINED_PROMPT'"

echo "concierge: started session '$SESSION', log: $LOG_FILE"
