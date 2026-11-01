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
# ORDER IS LOAD-BEARING: a later capture is a superset of an earlier one, so every record must be captured AFTER the records it refers to are
# already durable: terminals (emission) name episodes, episodes name sequenced offsets and strike rows. Outputs first, inputs last; the reverse
# order captured a terminal whose episode arrived after the episode topic was filed and the enumerator rightly refused the dataset.
TOPICS="${AUDIT_TOPICS:-breakout.emission breakout.features breakout.episode breakout.sequenced options.databento.gex.strike underlying.spx.price}"
# ES-side topics are captured as one TIME SLICE per day (capture-slice): the regular session with a margin, 08:30-16:30 New York, once that window
# has closed and while the topic still exists. The aligned book republishes a ~16 KB record several times a second, so it is thinned to one record
# per BOOK_SAMPLE_MS of broker time (found by seeking, not by downloading it whole); the others are taken whole. A slice is research input, not a
# completeness-proven audit dataset: its .json names the window, the sampling and the scan range. A slice that exists is never rewritten.
SLICE_BOOK="${AUDIT_SLICE_BOOK:-options.es-gex-spx-aligned}"
SLICE_WHOLE="${AUDIT_SLICE_WHOLE:-options.es-strike-intel-spx-aligned es.futures.cvd.bars es.futures.footprint.strike}"
BOOK_SAMPLE_MS="${AUDIT_BOOK_SAMPLE_MS:-2000}"
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

# stamp() prints the epoch milliseconds of a New York wall time on $DAY
stamp() { echo $(( $(TZ=America/New_York date -j -f '%Y-%m-%d %H:%M:%S' "$DAY $1" +%s) * 1000 )); }
FROM_TS="$(stamp 08:30:00)"; TO_TS="$(stamp 16:30:00)"
if [ "$(( $(date +%s) * 1000 ))" -lt "$TO_TS" ]; then
  log "slice window of $DAY still open; ES slices wait until 16:30 New York"
else
  # outputs-first as well: the whole-topic slices first, the thinned book last
  for t in $SLICE_WHOLE $SLICE_BOOK; do
    sample=0; [ "$t" = "$SLICE_BOOK" ] && sample="$BOOK_SAMPLE_MS"
    if java -jar "$JAR" capture-slice --bootstrap "$BOOTSTRAP" --registry "$REGISTRY" --out "$AUDIT_ROOT/$DAY" --topics "$t" --from-ts "$FROM_TS" --to-ts "$TO_TS" --sample-ms "$sample"; then
      log "sliced $t -> $AUDIT_ROOT/$DAY"
    else
      log "FAILED slice $t (exit $?)"; fail=1
    fi
  done
fi
exit "$fail"
