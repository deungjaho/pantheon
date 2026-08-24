#!/bin/sh
# Pantheon install script (POSIX sh).
#
# Builds pantheond and pantheon, installs them to ~/.local/bin, creates the
# data directory, installs systemd user units, reloads the user daemon, and
# enables pantheond. Run from the repository root.
#
# This script is intentionally POSIX sh (not bash) so it runs on any
# base system without extra dependencies.
set -eu

# Resolve the repository root from the script location so the script can be
# run from anywhere. deploy/install.sh -> parent is the repo root.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

BIN_DIR=${HOME}/.local/bin
DATA_DIR=${HOME}/.local/share/pantheon
SYSTEMD_DIR=${HOME}/.config/systemd/user

echo "==> Building binaries"
mkdir -p "$BIN_DIR"
cd "$REPO_ROOT"
go build -o "$BIN_DIR/pantheond" ./cmd/pantheond
go build -o "$BIN_DIR/pantheon" ./cmd/pantheon

echo "==> Creating data directory $DATA_DIR"
mkdir -p "$DATA_DIR"

echo "==> Installing systemd user units to $SYSTEMD_DIR"
mkdir -p "$SYSTEMD_DIR"
cp "$REPO_ROOT/deploy/systemd/pantheond.service" "$SYSTEMD_DIR/pantheond.service"
cp "$REPO_ROOT/deploy/systemd/pantheon-wake.service" "$SYSTEMD_DIR/pantheon-wake.service"
cp "$REPO_ROOT/deploy/systemd/pantheon-wake.timer" "$SYSTEMD_DIR/pantheon-wake.timer"

echo "==> Reloading systemd user daemon"
systemctl --user daemon-reload

echo "==> Enabling pantheond.service"
systemctl --user enable pantheond.service

echo
echo "Pantheon installed."
echo "  Binaries:   $BIN_DIR/pantheond, $BIN_DIR/pantheon"
echo "  Data dir:   $DATA_DIR"
echo "  Systemd:    $SYSTEMD_DIR/pantheond.service (enabled, not started)"
echo
echo "Next steps:"
echo "  systemctl --user start pantheond"
echo "  pantheon doctor"
