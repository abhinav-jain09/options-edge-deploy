#!/usr/bin/env bash
#
# Install the Mac-local nightly Kafka cleanup launchd job.
#
# Idempotent: re-running unloads the old plist, re-templates it, and
# reloads. Safe to call after editing the cleanup script.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC_SCRIPT="$REPO_DIR/scripts/local/cleanup-kafka-changelogs.sh"
SRC_PLIST="$REPO_DIR/scripts/local/com.optionsedge.kafka-cleanup.plist"

DEST_SCRIPT="$HOME/bin/oe-kafka-cleanup.sh"
DEST_PLIST="$HOME/Library/LaunchAgents/com.optionsedge.kafka-cleanup.plist"
LABEL="com.optionsedge.kafka-cleanup"

if [[ ! -f "$SRC_SCRIPT" ]]; then
  echo "FATAL: missing $SRC_SCRIPT" >&2; exit 1
fi
if [[ ! -f "$SRC_PLIST" ]]; then
  echo "FATAL: missing $SRC_PLIST" >&2; exit 1
fi

mkdir -p "$HOME/bin" "$HOME/Library/LaunchAgents"

echo "[install] copying $SRC_SCRIPT -> $DEST_SCRIPT"
cp "$SRC_SCRIPT" "$DEST_SCRIPT"
chmod +x "$DEST_SCRIPT"

echo "[install] rendering plist -> $DEST_PLIST"
# Use Python's plistlib to set the ProgramArguments value. This is
# XML-safe (handles `&`, `<`, `>` in paths by emitting `&amp;` etc.),
# whereas string-template approaches (sed, awk, bash parameter
# expansion) all produce invalid plist XML if the path contains those
# characters. plistlib also re-emits a normalized, plutil-cleanable
# document.
SRC_PLIST="$SRC_PLIST" DEST_PLIST="$DEST_PLIST" DEST_SCRIPT="$DEST_SCRIPT" \
python3 - <<'PYEOF'
import os, plistlib
with open(os.environ['SRC_PLIST'], 'rb') as f:
    p = plistlib.load(f)
# Replace the entire ProgramArguments array: invoking bash with the
# script path as a separate argv element, NOT via shell parsing.
p['ProgramArguments'] = ['/bin/bash', os.environ['DEST_SCRIPT']]
with open(os.environ['DEST_PLIST'], 'wb') as f:
    plistlib.dump(p, f)
PYEOF

# Validate the rendered plist before launchctl ever sees it.
plutil -lint "$DEST_PLIST" >/dev/null

# Unload first if already present (safe even if not loaded).
if launchctl list | grep -q "$LABEL"; then
  echo "[install] unloading existing job"
  launchctl unload -w "$DEST_PLIST" 2>/dev/null || true
fi

echo "[install] loading job"
launchctl load -w "$DEST_PLIST"

echo "[install] verifying"
launchctl list | grep "$LABEL" || { echo "FATAL: job not registered" >&2; exit 1; }

echo
echo "[install] done. Job will fire daily at 00:00 local time."
echo "[install] ad-hoc run :  launchctl start $LABEL"
echo "[install] logs       :  ~/.local/var/log/oe-kafka-cleanup/"
echo "[install] launchd logs:  /tmp/oe-kafka-cleanup.stdout.log /tmp/oe-kafka-cleanup.stderr.log"
echo "[install] uninstall  :  launchctl unload -w $DEST_PLIST && rm $DEST_PLIST $DEST_SCRIPT"
