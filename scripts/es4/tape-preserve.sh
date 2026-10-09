#!/usr/bin/env bash
# tape-preserve.sh — SOURCED by cleanup-es4.sh. Carries the ES trade tape across the Kafka wipe so
# es-amt-service starts READY instead of NOT_READY until the next weekend.
#
# WHY. es-amt-service builds today's auction ledger from the PRIOR RTH session's prints. At startup it
# seeks every partition of es.underlying.es.trades to the replay horizon (EsAmtRuntime.seek); if the
# earliest retained record is after the prior session's 09:30 ET open it sets retentionShort and
# /health/ready answers 503 for the life of the pod. The clean-reset deletes the whole Kafka data
# directory, so every clean left AMT in exactly that state (2026-09-13, 2026-10-08).
#
# WHAT. Before the wipe: copy [AMT replay horizon - margin, end) of the tape to a file outside the Kafka
# volume. After the topics are recreated and BEFORE any app is restored: produce it back into the empty
# topic with the original partition, CreateTime timestamp, key, value and headers (offsets restart at 0).
# TapePreserve.java does the copying (no stock CLI producer can set a timestamp or partition) and ends
# with AMT's own retention test as a cross-check.
#
# WHY IT IS SAFE TO RESTORE: the tape values are plain JSON, not Schema-Registry-framed Avro, so no record
# carries a schema id that the wiped registry could resolve differently (see cleanup-es4.sh header).
#
# FAILURE POLICY — NON-FATAL, fail toward the old behaviour. Both functions return non-zero on failure
# and the caller only warns: an emergency clean (disk full) must still be able to wipe, and AMT without
# a preserved tape is merely not-ready, exactly as before. What this file refuses to do is leave a
# WRONG tape behind:
#   * an import that fails part-way truncates the topic again (TapePreserve), never leaving half a tape;
#   * a preserved file older than ES4_TAPE_PRESERVE_MAX_AGE_HOURS (24) is never imported — a stale
#     artifact must not be restored into a later, unrelated wipe;
#   * a consumed artifact is renamed away, so one export restores into one wipe;
#   * the target must be empty (log start == log end).
#
# CONSUMER GROUPS — the part that is easy to get wrong. After a wipe every group that read the tape is
# brand new, so each would re-read the restored history: the off-box es-trades-bridge would republish
# it into PROD's trade tape (duplicate trades), and on-box services would re-derive a session of
# outputs that the mirrors then re-append to dev/prod. So:
#   * export records the groups that held offsets on the topic (visible in its log line);
#   * import REFUSES (nothing is produced) while any of them has live members — pause the
#     es-trades-bridge launchd job first (es4-cleanup.sh does) — and tells you which;
#   * after a successful import every recorded group is pinned to the restored end, so each starts
#     exactly where it would have after a plain wipe. A group that cannot be pinned is a WARNING that
#     names it. es-amt-service is excluded (it assigns and seeks by timestamp; ES4_TAPE_UNPINNED_GROUPS).
#
# Knobs (all optional):
#   ES4_TAPE_PRESERVE=off             disable both steps
#   ES4_TAPE_PRESERVE_TOPICS="a b"    topics to carry (default es.underlying.es.trades)
#   ES4_TAPE_PRESERVE_DIR             artifact dir (default $ES4_HOME/.es4-tape-preserve)
#   ES4_TAPE_PRESERVE_MAX_AGE_HOURS   refuse older artifacts (default 24)
#   ES4_TAPE_LOOKBACK_HOURS           must equal ES_AMT_REPLAY_LOOKBACK_HOURS on es-amt-service (default 34)
#   ES4_TAPE_MARGIN_HOURS             extra history before the horizon (default 2)
#   ES4_TAPE_UNPINNED_GROUPS          regex of groups NOT pinned (default ^es-amt-service)
#   ES4_TAPE_BOOTSTRAP / ES4_KAFKA_LIBS / ES4_TAPE_JAVA / ES4_TAPE_TIMEOUT_S
#   ES4_TAPE_CAL_DIR / ES4_TAPE_NOW_EPOCH   (tests: calendar module dir, frozen clock)

_TP_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

tp_log() { printf '  [tape-preserve] %s\n' "$*"; }

tp_enabled() { [ "${ES4_TAPE_PRESERVE:-on}" != off ]; }

tp_dir() { printf '%s' "${ES4_TAPE_PRESERVE_DIR:-${ES4_HOME:-/home/es4}/.es4-tape-preserve}"; }

# A java that can launch a single .java source (needs jdk.compiler, which the es4 JREs carry).
tp_java() {
  local j
  if [ -n "${ES4_TAPE_JAVA:-}" ]; then printf '%s' "$ES4_TAPE_JAVA"; return 0; fi
  for j in /usr/lib/jvm/jre-17-openjdk/bin/java /usr/lib/jvm/jre-21-openjdk/bin/java; do
    [ -x "$j" ] && { printf '%s' "$j"; return 0; }
  done
  command -v java
}

# TapePreserve <args...>; stdout/stderr pass through, exit status is the tool's.
tp_tool() {
  local java; java="$(tp_java)" || { echo "no java found" >&2; return 127; }
  timeout "${ES4_TAPE_TIMEOUT_S:-900}" "$java" -cp "${ES4_KAFKA_LIBS:-/opt/kafka/current/libs}/*" \
    "$_TP_SELF_DIR/tape-preserve/TapePreserve.java" "$@"
}

# Prints "<exportFromMs> <requiredFromMs>", mirroring the definitions in EsAmtSession:
#   tradeDate    = today, or tomorrow from 18:00 ET, rolled forward to a trading day
#   required     = 09:30 ET of the trading day BEFORE tradeDate (what the tape must reach)
#   horizon      = min(now - lookback, required)           (where AMT seeks)
#   exportFrom   = horizon - margin
# Prints nothing and returns non-zero if the calendar cannot be loaded.
tp_window() {
  ES4_TAPE_CAL_DIR="${ES4_TAPE_CAL_DIR:-$_TP_SELF_DIR/../jenkins}" \
  LOOKBACK_H="${ES4_TAPE_LOOKBACK_HOURS:-34}" MARGIN_H="${ES4_TAPE_MARGIN_HOURS:-2}" \
  python3 - <<'PY' 2>/dev/null
import os, sys, datetime as dt
from zoneinfo import ZoneInfo
sys.path.insert(0, os.environ["ES4_TAPE_CAL_DIR"])
from market_calendar import MarketCalendar
et = ZoneInfo("America/New_York")
cal = MarketCalendar()
import time
now = dt.datetime.fromtimestamp(float(os.environ.get("ES4_TAPE_NOW_EPOCH") or time.time()), et)   # NOW_EPOCH: tests only
d = now.date() + (dt.timedelta(days=1) if now.time() >= dt.time(18, 0) else dt.timedelta(0))
for _ in range(15):
    if cal.is_trading_day(d):
        break
    d += dt.timedelta(days=1)
p = d - dt.timedelta(days=1)
for _ in range(15):
    if cal.is_trading_day(p):
        break
    p -= dt.timedelta(days=1)
required = int(dt.datetime.combine(p, dt.time(9, 30), et).timestamp() * 1000)
lookback = int((now - dt.timedelta(hours=float(os.environ["LOOKBACK_H"]))).timestamp() * 1000)
horizon = min(lookback, required)
print(horizon - int(float(os.environ["MARGIN_H"]) * 3600 * 1000), required)
PY
}

# --------------------------------------------------------------------------------------------- export
# Call AFTER every producer is proven down and BEFORE the Kafka data directory is wiped.
tape_preserve_export() {
  tp_enabled || { tp_log "disabled (ES4_TAPE_PRESERVE=off)"; return 0; }
  local dir topics bs win from required t tmp recs out rc=0
  dir="$(tp_dir)"; topics="${ES4_TAPE_PRESERVE_TOPICS:-es.underlying.es.trades}"; bs="${ES4_TAPE_BOOTSTRAP:-localhost:9092}"
  if win="$(tp_window)" && [ -n "$win" ]; then
    from="${win% *}"; required="${win#* }"
  else
    from=$(( ($(date +%s) - 96 * 3600) * 1000 )); required=""
    tp_log "WARNING: calendar unavailable — exporting a fixed 96h window and skipping the coverage check"
  fi
  if [ "${DRY:-false}" = true ]; then
    tp_log "DRY: would export $topics from ${from} (ms) to $dir"
    return 0
  fi
  mkdir -p "$dir" || { tp_log "WARNING: cannot create $dir"; return 1; }
  for t in $topics; do
    tmp="$dir/.tmp.$t.$$"
    rm -f "$tmp" "$tmp.manifest"
    if out="$(tp_tool export "$bs" "$t" "$from" "$tmp" 2>&1)"; then
      recs="$(sed -n 's/^records=//p' "$tmp.manifest" 2>/dev/null)"
      if [ "${recs:-0}" -gt 0 ] 2>/dev/null; then
        # tape first, manifest last: the manifest is the completion marker, so drop the old one before moving.
        rm -f "$dir/$t.tape.manifest"
        mv -f "$tmp" "$dir/$t.tape" && mv -f "$tmp.manifest" "$dir/$t.tape.manifest" \
          || { tp_log "WARNING: could not publish the export of $t"; rc=1; continue; }
        printf '%s\n' "$required" > "$dir/$t.required"
        tp_log "preserved $t: $recs records -> $dir/$t.tape (readers to pin on restore: $(printf '%s' "$out" | sed -n 's/.*groups=//p'))"
      else
        rm -f "$tmp" "$tmp.manifest"
        tp_log "WARNING: $t held no records in the window (already wiped?) — any earlier artifact is kept for a resumed reset"
      fi
    else
      rm -f "$tmp" "$tmp.manifest"
      tp_log "WARNING: export of $t failed — es-amt-service will be NOT_READY after this clean: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
      rc=1
    fi
  done
  return $rc
}

# --------------------------------------------------------------------------------------------- import
# Call AFTER the topics are recreated (empty) and BEFORE any app is restored.
tape_preserve_import() {
  tp_enabled || { tp_log "disabled (ES4_TAPE_PRESERVE=off)"; return 0; }
  local dir topics bs t tape mf age_s max_s required out rc=0 irc
  dir="$(tp_dir)"; topics="${ES4_TAPE_PRESERVE_TOPICS:-es.underlying.es.trades}"; bs="${ES4_TAPE_BOOTSTRAP:-localhost:9092}"
  max_s=$(( ${ES4_TAPE_PRESERVE_MAX_AGE_HOURS:-24} * 3600 ))
  for t in $topics; do
    tape="$dir/$t.tape"; mf="$tape.manifest"
    if [ ! -f "$tape" ] || [ ! -f "$mf" ]; then tp_log "no preserved $t to restore"; continue; fi
    age_s=$(( $(date +%s) - $(stat -c %Y "$mf" 2>/dev/null || stat -f %m "$mf") ))
    if [ "$age_s" -gt "$max_s" ]; then
      tp_log "WARNING: preserved $t is $((age_s / 3600))h old (> $((max_s / 3600))h) — refusing to restore a stale tape into this wipe"
      rc=1; continue
    fi
    if [ "${DRY:-false}" = true ]; then tp_log "DRY: would restore $t from $tape"; continue; fi
    out="$(tp_tool import "$bs" "$tape" "${ES4_TAPE_UNPINNED_GROUPS:-^es-amt-service}" 2>&1)"; irc=$?
    case "$irc" in
      0)
        tp_log "restored $t: $(printf '%s' "$out" | grep -E 'TAPE_IMPORTED|TAPE_IMPORT_SKIPPED' | tail -1)"
        printf '%s\n' "$out" | grep '^TAPE_PINNED' | while read -r l; do tp_log "pinned to the restored end: ${l#TAPE_PINNED }"; done
        if printf '%s\n' "$out" | grep -q '^TAPE_PIN_FAILED'; then
          tp_log "WARNING: could not pin: $(printf '%s' "$out" | sed -n 's/^TAPE_PIN_FAILED group=\([^ ]*\).*/\1/p' | tr '\n' ' ')— they will RE-READ the restored history"
          rc=1
        fi
        required="$(cat "$dir/$t.required" 2>/dev/null)"
        if [ -n "$required" ]; then
          if tp_tool coverage "$bs" "$t" "$required" >/dev/null 2>&1; then
            tp_log "coverage check: $t reaches the prior RTH open — es-amt-service will start READY"
          else
            tp_log "WARNING: coverage check: $t does NOT reach the prior RTH open (the source tape was already short) — es-amt-service will stay NOT_READY until a session roll"
            rc=1
          fi
        fi
        # one export restores into one wipe: retire the artifact, keep only the latest retired copy.
        mv -f "$tape" "$dir/consumed.$t.tape" 2>/dev/null; mv -f "$mf" "$dir/consumed.$t.tape.manifest" 2>/dev/null
        rm -f "$dir/$t.required"
        ;;
      4) tp_log "WARNING: $t already holds records — not importing over them (artifact kept)"; rc=1 ;;
      5) tp_log "WARNING: NOT restoring $t — consumer group(s) that read it are ACTIVE: $(printf '%s' "$out" | sed -n 's/^TAPE_IMPORT_GROUP_ACTIVE group=\([^ ]*\).*/\1/p' | tr '\n' ' ')— restored history would stream to them (the es4->prod bridge would republish it into prod). Pause them (launchctl bootout gui/\$(id -u)/com.optionsedge.es-trades-bridge-192-168-100-252-9092) and rerun, or accept es-amt-service NOT_READY"; rc=1 ;;
      *) tp_log "WARNING: restore of $t failed (rc=$irc; the topic was truncated back to empty): $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; rc=1 ;;
    esac
  done
  return $rc
}
