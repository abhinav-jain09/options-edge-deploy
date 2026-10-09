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
# PINNED CONSUMERS START COLD, ON PURPOSE. Pinning a group to the restored end makes it start exactly where a
# plain wipe would have left it: a Kafka Streams app (indicator-service-es4) with wiped state stores does NOT
# rebuild from the restored history, and nothing here changes that. Only es-amt-service reads the history.
#
# THE RESTORE-IN-PROGRESS MARKER (<topic>.importing) records the topic's id. It blocks export while that
# incarnation exists (a stopped reset is resumed and would otherwise snapshot a possibly partial tape over the
# good artifact) and is obsolete the moment the topic is recreated, so it can never disable later resets.
#
# THE FENCE IS BEST-EFFORT DETECTION, NOT A LOCK — what it does and what it does not:
#   * before the first record: a recorded group with live members - or any other group whose live member may be
#     reading the topic, recorded or not - refuses the import (nothing produced). Consumer/classic groups are
#     judged by their assignment; SHARE and STREAMS groups (which the consumer-group tools cannot see, and whose
#     assignment materialises lazily) by having ANY live member. A group of a protocol this tool cannot inspect,
#     or one it cannot read, fails the restore closed rather than being guessed at;
#   * after the last record, before pinning: the topic is scanned again for any group that holds offsets on it
#     or may be reading it (nothing was pinned yet, so on a topic that was empty a moment ago that is an
#     intruder); if one is found the topic is emptied again (rc 7, TAPE_IMPORT_FENCE_BREACH, named and logged
#     as an ERROR); then ALL-OR-NOTHING pinning - a group that cannot be pinned also empties the topic;
#   * NOT prevented: a consumer started by someone else during the few seconds of the produce reads part of the
#     tape before the rollback. Kafka without ACLs has nothing a client can use to keep it out, so a hard
#     guarantee needs ACLs or network isolation of the topic during the restore, which this does not set up. The
#     off-box es-trades-bridge is held down by es4-cleanup.sh for the whole reset, so only a manual start in that
#     window can open the gap; the scan then reports it (best effort) so it is not silent.
#
# ANY OUTCOME THE TOOL DOES NOT REPORT CLEANLY IS SETTLED FROM THE TOPIC'S REAL STATE, not trusted: a killed
# process cannot roll itself back, and an exit code is only a claim. tp_settle reads the topic (EMPTY /
# COMPLETE / PARTIAL), finishes a complete-but-unpinned restore or empties the rest, and PROVES it. If a
# partial tape cannot be emptied the function returns 2 and cleanup-es4.sh stops before any app starts.
#
# FAILURE POLICY — NON-FATAL, fail toward the old behaviour. Both functions return non-zero on failure
# and the caller only warns: an emergency clean (disk full) must still be able to wipe, and AMT without
# a preserved tape is merely not-ready, exactly as before. What this file refuses to do is leave a
# WRONG tape behind:
#   * an import that fails part-way truncates the topic again (TapePreserve), never leaving half a tape;
#   * a preserved file older than ES4_TAPE_PRESERVE_MAX_AGE_HOURS (24) is never imported — a stale
#     artifact must not be restored into a later, unrelated wipe;
#   * a consumed artifact is retired (renamed, else removed, else an ERROR), so one export restores into one wipe;
#   * the artifact must run THROUGH the close of the prior session as of import time, or it is refused (an
#     abandoned earlier reset's mid-session export would otherwise pass AMT's timestamp test while partial);
#   * the target topic must be CreateTime - a LogAppendTime topic would overwrite every restored timestamp;
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
#   ES4_TAPE_BOOTSTRAP / ES4_KAFKA_LIBS / ES4_TAPE_JAVA / ES4_TAPE_TIMEOUT_S / ES4_TAPE_IMPORT_TIMEOUT_S
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
  local tmo="${ES4_TAPE_TIMEOUT_S:-900}"
  [ "${1:-}" = import ] && tmo="${ES4_TAPE_IMPORT_TIMEOUT_S:-$tmo}"
  timeout --kill-after=30 "$tmo" "$java" -cp "${ES4_KAFKA_LIBS:-/opt/kafka/current/libs}/*" \
    "$_TP_SELF_DIR/tape-preserve/TapePreserve.java" "$@"
}

# Prints "<exportFromMs> <requiredFromMs> <requiredCloseMs>", mirroring the definitions in EsAmtSession:
#   tradeDate    = today, or tomorrow from 18:00 ET, rolled forward to a trading day
#   required     = 09:30 ET of the trading day BEFORE tradeDate (what the tape must reach)
#   requiredClose= that same day's close (16:00 ET, 13:00 on an early close): the tape must run THROUGH it
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
required_close = int(dt.datetime.combine(p, cal.close_time(p), et).timestamp() * 1000)
lookback = int((now - dt.timedelta(hours=float(os.environ["LOOKBACK_H"]))).timestamp() * 1000)
horizon = min(lookback, required)
print(horizon - int(float(os.environ["MARGIN_H"]) * 3600 * 1000), required, required_close)
PY
}

# --------------------------------------------------------------------------------------------- export
# Call AFTER every producer is proven down and BEFORE the Kafka data directory is wiped.
tape_preserve_export() {
  tp_enabled || { tp_log "disabled (ES4_TAPE_PRESERVE=off)"; return 0; }
  local dir topics bs win from t tmp recs out rc=0 mid cur obsolete
  dir="$(tp_dir)"; topics="${ES4_TAPE_PRESERVE_TOPICS:-es.underlying.es.trades}"; bs="${ES4_TAPE_BOOTSTRAP:-localhost:9092}"
  if win="$(tp_window)" && [ -n "$win" ]; then
    from="${win%% *}"
  else
    from=$(( ($(date +%s) - 96 * 3600) * 1000 ))
    tp_log "WARNING: calendar unavailable — exporting a fixed 96h window (the import will refuse without the calendar)"
  fi
  if [ "${DRY:-false}" = true ]; then
    tp_log "DRY: would export $topics from ${from} (ms) to $dir"
    return 0
  fi
  mkdir -p "$dir" || { tp_log "WARNING: cannot create $dir"; return 1; }
  for t in $topics; do
    tp_recover "$dir" "$t"
    # A restore that did not finish cleanly (UNSAFE, or the wrapper itself was killed) leaves this marker. The
    # topic may then hold a PARTIAL restored tape, and exporting it would overwrite the good artifact with a
    # partial copy that a later restore would present as complete. Keep the artifact; the wipe that follows
    # (resumed reset) empties the topic and the good artifact is restored.
    # The marker describes ONE incarnation of the topic (its id). Once the topic has been recreated - the wipe that
    # follows a stopped reset - it no longer applies; left in place it would silently disable preservation for
    # every later reset. If the current id cannot be read, assume it still applies (the export would fail anyway).
    if [ -e "$dir/$t.importing" ]; then
      mid="$(awk '{print $2}' "$dir/$t.importing" 2>/dev/null)"; cur="$(tp_topic_id "$t")"
      # Obsolete only on PROOF: the topic is gone, or it is a different incarnation than the marker names, or (a
      # marker without a usable id - written by an earlier version) the topic is proven EMPTY. Age proves nothing:
      # a killed restore leaves a partial tape in the SAME incarnation however long ago it happened.
      obsolete=0
      if [ "$cur" = ABSENT ]; then obsolete=1
      elif tp_valid_id "$cur" && tp_valid_id "$mid"; then [ "$cur" != "$mid" ] && obsolete=1
      elif tp_valid_id "$cur" && [ "$(tp_state "$dir/$t.tape")" = EMPTY ]; then obsolete=1
      fi
      if [ "$obsolete" = 1 ]; then
        tp_log "the restore marker for $t is obsolete (marker id '${mid:-none}', topic now '${cur}') - clearing it"
        rm -f "$dir/$t.importing"
      else
        tp_log "NOT exporting $t: an earlier restore did not finish cleanly (marker $dir/$t.importing) - the topic may hold a partial tape; keeping the existing artifact"
        continue
      fi
    fi
    tmp="$dir/.tmp.$t.$$"
    rm -f "$tmp" "$tmp.manifest"
    if out="$(tp_tool export "$bs" "$t" "$from" "$tmp" 2>&1)"; then
      recs="$(sed -n 's/^records=//p' "$tmp.manifest" 2>/dev/null)"
      if [ "${recs:-0}" -gt 0 ] 2>/dev/null; then
        tp_publish "$dir" "$t" "$tmp" || { tp_log "WARNING: could not publish the export of $t - the previous artifact (if any) was put back"; rc=1; continue; }
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

# Publishes the exported pair (<tmp>, <tmp>.manifest) as the active artifact. Tape first, manifest last: the
# manifest is the completion marker, so an artifact without one is inactive. The PREVIOUS pair is parked until the
# new one is in place, so a failure between the two renames puts it back instead of leaving the good artifact
# destroyed by a half-published new one.
tp_publish() { # <dir> <topic> <tmp>
  local dir="$1" t="$2" tmp="$3" parked=0
  if [ -f "$dir/$t.tape" ] && [ -f "$dir/$t.tape.manifest" ]; then
    rm -f "$dir/prev.$t.tape" "$dir/prev.$t.tape.manifest"
    mv -f "$dir/$t.tape.manifest" "$dir/prev.$t.tape.manifest" || return 1
    mv -f "$dir/$t.tape" "$dir/prev.$t.tape" || { mv -f "$dir/prev.$t.tape.manifest" "$dir/$t.tape.manifest"; return 1; }
    parked=1
  fi
  if mv -f "$tmp" "$dir/$t.tape" && mv -f "$tmp.manifest" "$dir/$t.tape.manifest"; then
    rm -f "$dir/prev.$t.tape" "$dir/prev.$t.tape.manifest"
    return 0
  fi
  rm -f "$dir/$t.tape" "$dir/$t.tape.manifest" "$tmp" "$tmp.manifest"
  [ "$parked" = 1 ] && tp_recover "$dir" "$t"
  return 1
}

# Settles an interrupted publish: a manifest means the active pair is complete (it is renamed last) and any parked
# pair is stale; no manifest but a parked pair means the publish died half-way, and the parked pair is restored.
tp_recover() { # <dir> <topic>
  local dir="$1" t="$2"
  if [ -f "$dir/$t.tape.manifest" ]; then rm -f "$dir/prev.$t.tape" "$dir/prev.$t.tape.manifest"; return 0; fi
  if [ -f "$dir/prev.$t.tape" ] && [ -f "$dir/prev.$t.tape.manifest" ]; then
    rm -f "$dir/$t.tape"
    if mv -f "$dir/prev.$t.tape" "$dir/$t.tape" && mv -f "$dir/prev.$t.tape.manifest" "$dir/$t.tape.manifest"; then
      tp_log "recovered the previous artifact for $t after an interrupted publish"; return 0
    fi
    tp_log "ERROR: could not restore the previous artifact for $t from $dir/prev.$t.tape"; return 1
  fi
  return 0
}

# --------------------------------------------------------------------------------------------- import
# Brings the topic to a state that is SAFE to start apps on after an import that did not report a clean
# outcome - including one whose process was killed and so could not roll itself back. Prints the state it
# settled in on the LAST line (EMPTY | COMPLETE) and returns 0, or returns 2 when the topic holds a partial
# tape that could not be emptied: the one state nothing may start on.
# The broker's id for the topic's CURRENT incarnation (a recreated topic gets a new one); "ABSENT" if it does not exist.
tp_valid_id() { printf '%s' "$1" | grep -Eq '^[A-Za-z0-9_-]{22}$'; }   # a Kafka topic id: 16 bytes, base64url
tp_topic_id() { tp_tool topicid "${ES4_TAPE_BOOTSTRAP:-localhost:9092}" "$1" 2>&1 | sed -n 's/^TAPE_TOPIC_ID //p' | tail -1; }
tp_state() { tp_tool state "${ES4_TAPE_BOOTSTRAP:-localhost:9092}" "$1" 2>&1 | sed -n 's/^TAPE_STATE \([A-Z]*\).*/\1/p' | tail -1; }
tp_settle() { # <topic> <tape>
  local t="$1" tape="$2" bs="${ES4_TAPE_BOOTSTRAP:-localhost:9092}" st
  st="$(tp_state "$tape")"
  case "$st" in
    EMPTY) echo EMPTY; return 0 ;;
    COMPLETE)
      # The records landed but the process died before (or during) pinning: finish the job; if it cannot be
      # finished the tape comes back out rather than stay un-pinned.
      if tp_tool pin "$bs" "$tape" "${ES4_TAPE_UNPINNED_GROUPS:-^es-amt-service}" >/dev/null 2>&1; then echo COMPLETE; return 0; fi ;;
  esac
  # PARTIAL, unreadable, or complete-but-unpinnable: empty it and PROVE it is empty.
  tp_tool truncate "$bs" "$t" >/dev/null 2>&1
  st="$(tp_state "$tape")"
  if [ "$st" = EMPTY ]; then echo EMPTY; return 0; fi
  echo "${st:-UNKNOWN}"; return 2
}

# Retire the artifact so one export restores into one wipe. A rename that fails must not leave it active
# (it could be imported into a later incarnation): fall back to removing it, and say so if even that fails.
tp_retire() { # <dir> <topic>
  local dir="$1" t="$2"
  mv -f "$dir/$t.tape" "$dir/consumed.$t.tape" 2>/dev/null && mv -f "$dir/$t.tape.manifest" "$dir/consumed.$t.tape.manifest" 2>/dev/null && return 0
  rm -f "$dir/$t.tape" "$dir/$t.tape.manifest"
  if [ -e "$dir/$t.tape" ] || [ -e "$dir/$t.tape.manifest" ]; then
    tp_log "ERROR: could not retire the artifact for $t (still at $dir/$t.tape) - remove it by hand or it may be restored into a later wipe"
    return 1
  fi
  tp_log "WARNING: could not rename the artifact for $t; it was removed instead"
}

# Call AFTER the topics are recreated (empty) and BEFORE any app is restored.
# Returns 0 restored (or nothing to do); 1 not restored / needs attention (a warning); 2 UNSAFE - the topic
# holds a partial tape that could not be emptied, and the caller must NOT start anything on it.
tape_preserve_import() {
  tp_enabled || { tp_log "disabled (ES4_TAPE_PRESERVE=off)"; return 0; }
  local dir topics bs t tape mf age_s max_s required reqclose maxts win out rc=0 irc settled sc skip tid
  dir="$(tp_dir)"; topics="${ES4_TAPE_PRESERVE_TOPICS:-es.underlying.es.trades}"; bs="${ES4_TAPE_BOOTSTRAP:-localhost:9092}"
  skip="${ES4_TAPE_UNPINNED_GROUPS:-^es-amt-service}"
  max_s=$(( ${ES4_TAPE_PRESERVE_MAX_AGE_HOURS:-24} * 3600 ))
  for t in $topics; do
    tp_recover "$dir" "$t"
    tape="$dir/$t.tape"; mf="$tape.manifest"
    if [ ! -f "$tape" ] || [ ! -f "$mf" ]; then tp_log "no preserved $t to restore"; continue; fi
    age_s=$(( $(date +%s) - $(stat -c %Y "$mf" 2>/dev/null || stat -f %m "$mf") ))
    if [ "$age_s" -gt "$max_s" ]; then
      tp_log "WARNING: preserved $t is $((age_s / 3600))h old (> $((max_s / 3600))h) - refusing to restore a stale tape into this wipe"
      rc=1; continue
    fi
    # The artifact must run THROUGH the close of the prior session as of NOW. Timestamps alone would pass AMT's
    # coverage test for an artifact exported mid-session by an abandoned earlier reset - and AMT would then
    # build today's references from a partial prior session. An export taken after that close is complete.
    if ! win="$(tp_window)" || [ -z "$win" ]; then
      tp_log "WARNING: calendar unavailable - cannot prove the preserved $t reaches the prior session's close; not restoring"
      rc=1; continue
    fi
    required="$(printf '%s' "$win" | cut -d' ' -f2)"; reqclose="$(printf '%s' "$win" | cut -d' ' -f3)"
    maxts="$(sed -n 's/^maxTs=//p' "$mf" 2>/dev/null)"
    if [ "${maxts:-0}" -lt "$reqclose" ] 2>/dev/null; then
      tp_log "WARNING: preserved $t ends at ${maxts:-0} ms, BEFORE the prior session's close ($reqclose ms) - restoring it would present a partial prior session as complete; not restoring"
      rc=1; continue
    fi
    if [ "${DRY:-false}" = true ]; then tp_log "DRY: would restore $t from $tape"; continue; fi
    # The marker must name the incarnation it protects, or a later run could not tell a partial tape in THIS
    # topic from a harmless leftover. No readable id, no restore: nothing has been produced yet.
    tid="$(tp_topic_id "$t")"
    if ! tp_valid_id "$tid"; then
      tp_log "WARNING: cannot read the id of topic $t (got '${tid:-nothing}') - not restoring without being able to scope the restore-in-progress marker to it (artifact kept)"
      rc=1; continue
    fi
    printf '%s %s\n' "$(date +%s)" "$tid" > "$dir/$t.importing"
    out="$(tp_tool import "$bs" "$tape" "$skip" 2>&1)"; irc=$?
    # An exit code is a claim, not evidence: look at the topic before believing "restored". 98 = the tool said
    # success but the topic is not COMPLETE, which is handled exactly like any other unknown outcome.
    if [ "$irc" = 0 ] && [ "$(tp_state "$tape")" != COMPLETE ]; then irc=98; fi
    settled=""
    case "$irc" in
      0) settled=COMPLETE ;;
      4) rm -f "$dir/$t.importing"; tp_log "WARNING: $t already holds records - not importing over them (artifact kept)"; rc=1; continue ;;
      5) rm -f "$dir/$t.importing"; tp_log "WARNING: NOT restoring $t - consumer group(s) that read it are ACTIVE: $(printf '%s' "$out" | sed -n 's/^TAPE_IMPORT_GROUP_ACTIVE group=\([^ ]*\).*/\1/p' | tr '\n' ' ')- restored history would stream to them (the es4->prod bridge would republish it into prod). Pause them (launchctl bootout gui/\$(id -u)/com.optionsedge.es-trades-bridge-192-168-100-252-9092) and rerun, or accept es-amt-service NOT_READY"; rc=1; continue ;;
      6) rm -f "$dir/$t.importing"; tp_log "WARNING: NOT restoring $t - $(printf '%s' "$out" | sed -n 's/^TAPE_IMPORT_TIMESTAMP_TYPE //p' | head -1). A LogAppendTime topic stamps every restored record with the import time, which would defeat the restore. Nothing was produced"; rc=1; continue ;;
      *)
        # 7 = the tool rolled itself back; anything else (error, timeout kill, signal) = outcome unknown.
        # Never trust either: look at the topic and make it safe.
        tp_log "WARNING: restore of $t did not complete cleanly (rc=$irc): $(printf '%s' "$out" | grep -E 'ROLLED_BACK|PIN_FAILED|PRESERVE_ERROR|TRUNCATE|FENCE_BREACH' | tail -2 | tr '\n' ' ')"
        if printf '%s' "$out" | grep -q 'TAPE_IMPORT_FENCE_BREACH'; then
          tp_log "ERROR: a consumer started reading $t WHILE the history was being restored: $(printf '%s' "$out" | sed -n 's/^TAPE_IMPORT_FENCE_BREACH .*groups=\([^ ]*\).*/\1/p'). The restore was rolled back, but whatever it consumed may already have been republished downstream - if that is the es-trades-bridge, check prod's underlying.es.trades for repeated keys in the restored window"
        fi
        settled="$(tp_settle "$t" "$tape")"; sc=$?
        if [ "$sc" = 2 ]; then
          tp_log "UNSAFE: $t holds a PARTIAL tape that could not be emptied (state=$settled). Nothing may start on it."
          return 2
        fi
        if [ "$settled" = EMPTY ]; then
          rm -f "$dir/$t.importing"
          tp_log "rolled back: $t is empty (artifact kept); es-amt-service will be NOT_READY until a session roll"
          rc=1; continue
        fi
        ;;
    esac
    # Restored: complete, and every recorded group fenced and pinned.
    rm -f "$dir/$t.importing"
    tp_log "restored $t: $(printf '%s' "$out" | grep -E 'TAPE_IMPORTED|TAPE_IMPORT_SKIPPED' | tail -1)"
    printf '%s\n' "$out" | grep '^TAPE_PINNED' | while read -r l; do tp_log "pinned to the restored end: ${l#TAPE_PINNED }"; done
    if tp_tool coverage "$bs" "$t" "$required" >/dev/null 2>&1; then
      tp_log "coverage check: $t reaches the prior RTH open - es-amt-service will start READY"
    else
      tp_log "WARNING: coverage check: $t does NOT reach the prior RTH open (the source tape was already short) - es-amt-service will stay NOT_READY until a session roll"
      rc=1
    fi
    tp_retire "$dir" "$t" || rc=1
  done
  return $rc
}
