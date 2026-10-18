#!/usr/bin/env bash
# oe-breakout-audit-capture.sh — incremental capture of the breakout coverage-audit dataset (DAT 09) on the DEV Mac.
#
# The dev clean deletes every topic at 17:20 ET and recreates them with new topic ids, and the capture refuses a checkpoint
# beyond a recreated topic. One dataset directory per New York trading date keeps every day's chain whole. The capture is
# incremental (files are the checkpoint), so the 30 minute launchd cadence costs nothing and bounds the loss at a wipe.
# Dev only. Reads Kafka and the Schema Registry, writes only under $AUDIT_ROOT.
set -uo pipefail
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
HOME_DIR="${HOME:?}"
JAR="${AUDIT_JAR:-$HOME_DIR/oe-ops/breakout-audit/options-edge-breakout-audit.jar}"
AUDIT_ROOT="${AUDIT_ROOT:-$HOME_DIR/oe-ops/breakout-audit/data}"
BOOTSTRAP="${AUDIT_BOOTSTRAP:-localhost:19092}"
REGISTRY="${AUDIT_REGISTRY:-http://localhost:8082}"
TOPICS="${AUDIT_TOPICS:-breakout.sequenced options.databento.gex.strike underlying.spx.price breakout.episode breakout.emission breakout.features}"
DAY="$(TZ=America/New_York date +%Y-%m-%d)"
DOW="$(TZ=America/New_York date +%u)"
log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }

[ "$DOW" -le 5 ] || { log "weekend in New York; nothing to capture"; exit 0; }
[ -f "$JAR" ] || { log "FATAL: $JAR missing"; exit 1; }
LOCK="$AUDIT_ROOT/.run.lock"
mkdir -p "$AUDIT_ROOT" || exit 1
mkdir "$LOCK" 2>/dev/null || { log "another capture holds $LOCK; skipping"; exit 0; }
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT

fail=0
for t in $TOPICS; do
  out="$AUDIT_ROOT/$DAY"
  if java -jar "$JAR" capture --bootstrap "$BOOTSTRAP" --registry "$REGISTRY" --out "$out" --topics "$t"; then
    log "captured $t -> $out"
  else
    log "FAILED $t (exit $?)"; fail=1
  fi
done
exit "$fail"
