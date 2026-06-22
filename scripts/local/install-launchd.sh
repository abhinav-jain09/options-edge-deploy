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
# Use bash parameter expansion (${var//pattern/replacement}) rather
# than sed or awk gsub — both of those interpret `&` and `\` in the
# replacement string as special, which would corrupt a script path
# that happens to contain them. Parameter expansion treats the
# replacement as a literal string.
PLIST_CONTENT=$(<"$SRC_PLIST")
printf '%s\n' "${PLIST_CONTENT//__SCRIPT__/$DEST_SCRIPT}" > "$DEST_PLIST"

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
