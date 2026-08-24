#!/bin/sh
# Pantheon uninstall script (POSIX sh).
#
# Disables and stops the systemd user units, removes the unit files, reloads
# the user daemon, and deletes the installed binaries. The data directory
# ($HOME/.local/share/pantheon) is left intact so the SQLite store, event
# journal, and worktrees are preserved in case the user wants to reinstall.
set -eu

SYSTEMD_DIR=${HOME}/.config/systemd/user
BIN_DIR=${HOME}/.local/bin

echo "==> Disabling and stopping systemd units"
systemctl --user disable pantheond.service pantheon-wake.timer 2>/dev/null || true
systemctl --user stop pantheond.service pantheon-wake.timer 2>/dev/null || true

echo "==> Removing systemd unit files"
rm -f "$SYSTEMD_DIR/pantheond.service"
rm -f "$SYSTEMD_DIR/pantheon-wake.service"
rm -f "$SYSTEMD_DIR/pantheon-wake.timer"
systemctl --user daemon-reload

echo "==> Removing binaries"
rm -f "$BIN_DIR/pantheond"
rm -f "$BIN_DIR/pantheon"

echo
echo "Pantheon binaries and systemd units removed."
echo "Data directory ${HOME}/.local/share/pantheon/ left intact."
