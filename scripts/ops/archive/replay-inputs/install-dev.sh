#!/usr/bin/env bash
# install-dev.sh - put the DEV replay-inputs archive on the dev Mac (MacBook-Pro-2 / .102) and prove it runs.
# A HOST action run deliberately by a person; no Jenkins job calls it. Same pattern as
# install-dev-mac-watchdog.sh. Touches ONLY ~/Library/Application Support/OptionsEdge/dev-replay-inputs and
# one LaunchAgent; nothing of dev-nas-archive, es4 or production.
#
#   bash scripts/ops/archive/replay-inputs/install-dev.sh [--dry-run]
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${REPLAY_INPUTS_DEST:-$HOME/Library/Application Support/OptionsEdge/dev-replay-inputs}"
AGENTS="${LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
LABEL=com.optionsedge.dev-replay-inputs-archive
PY="$HOME/oe-ops/venv/bin/python"
DRY=false; [ "${1:-}" = "--dry-run" ] && DRY=true

[ "$(/bin/hostname)" = "MacBook-Pro-2.local" ] || { echo "REFUSING: this is the dev Mac installer (MacBook-Pro-2.local)" >&2; exit 1; }
"$PY" -c 'import confluent_kafka, fastavro' || { echo "REFUSING: $PY lacks confluent_kafka/fastavro" >&2; exit 1; }
for f in oe-replay-inputs-archive.py replay-inputs-topics.conf "$LABEL.plist"; do
  [ -f "$SRC/$f" ] || { echo "missing $SRC/$f" >&2; exit 1; }
done
run() { if $DRY; then echo "DRY: $*"; else "$@"; fi; }
run mkdir -p "$DEST"
run install -m 0644 "$SRC/oe-replay-inputs-archive.py" "$SRC/replay-inputs-topics.conf" "$DEST/"
run install -m 0644 "$SRC/$LABEL.plist" "$AGENTS/$LABEL.plist"
if ! $DRY; then
  # Prove it starts BEFORE the agent is trusted: one real capture, then a verify of today's ET date.
  PYTHONPATH="$HOME/oe-ops/venv/lib/python3.14/site-packages" /opt/homebrew/bin/python3 "$DEST/oe-replay-inputs-archive.py" capture --root /Volumes/database/optionsedge/replay-inputs/dev \
        --topics "$DEST/replay-inputs-topics.conf" --nas-guard --verbose
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$AGENTS/$LABEL.plist"
  n=$(launchctl list | grep -c "$LABEL" || true)
  [ "$n" = 1 ] || { echo "FAIL: expected exactly one agent $LABEL, found $n" >&2; exit 1; }
  echo "installed; verify a day with: $PY $DEST/oe-replay-inputs-archive.py verify --root /Volumes/database/optionsedge/replay-inputs/dev --dt YYYY-MM-DD"
fi
