#!/usr/bin/env bash
# oe-archive-dev.sh — frequent, incremental capture of DEV dealer-ledger evidence to the NAS.
#
# WHY THIS EXISTS SEPARATELY FROM THE NIGHTLY RUN
# Dev's `dealer-ledger-signal-fired` carries a per-topic retention.ms=86400000 (24h) override, and
# the service RE-STAMPS that value from its configmap on every startup — so the live sweep that set
# the cluster default to -1 does not stick for dev. Dev evidence therefore deletes itself daily.
# A once-per-day archive is one missed run away from permanently losing a session; at 2h it takes
# twelve consecutive failures to lose anything. Duplicates are recoverable, gaps are not.
#
# The archiver is incremental and offset-checkpointed, so running it often is cheap: each run reads
# only what arrived since the last checkpoint, and the manifest still proves continuity
# (archived-to == next archived-from).
#
# Scope is the ledger EVIDENCE only. Dev's market data is a synthetic/cascaded feed — archiving it
# would add volume, not truth. The corpus that both environments replay is shared and versioned at
# <NAS>/optionsedge/corpus/v<N>, deliberately NOT duplicated per environment: if the two copies
# drifted, a dev-vs-prod difference would become unattributable (engine or input?).
#
# NOTE ON THE DEV BROKER: it advertises itself as host.docker.internal:19092 (Docker Desktop), so
# reaching it from this box needs `192.168.100.102 host.docker.internal` in /etc/hosts. A plain TCP
# check to the port passes WITHOUT that entry and tells you nothing — the Kafka handshake is what
# fails, and it fails as "UnknownHostException", not as a connection error.
#
# USAGE: cron every 2h. Safe to run at any time, including while the nightly run is in flight
# (different lock, different manifest topic set).
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
NAS_DIR="${NAS_DIR:-/mnt/nas/optionsedge}"
ARCHIVER="${ARCHIVER:-/home/abhinav/oe-ops/oe-archive-kafka.sh}"
DEV_BOOTSTRAP="${DEV_BOOTSTRAP:-192.168.100.102:19092}"
LOG="${LOG:-/home/abhinav/oe-ops/archive-dev.log}"
LOCK="${LOCK:-/tmp/oe-archive-dev.lock}"
OE_TOPICS_ENV="${OE_TOPICS_ENV:-$(dirname "$0")/oe-topics.env}"
# The canonical set must come FROM THE FILE. An inherited DEALER_LEDGER_EVIDENCE would otherwise
# satisfy the check below and let a caller archive a different set behind the canonical file's
# back — equality by luck, not by construction. Require the file, then discard anything inherited.
[ -r "$OE_TOPICS_ENV" ] || { echo "FATAL: '$OE_TOPICS_ENV' missing or unreadable — refusing to archive an unknown evidence set" >&2; exit 1; }
unset DEALER_LEDGER_EVIDENCE
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"
: "${DEALER_LEDGER_EVIDENCE:?oe-topics.env did not define DEALER_LEDGER_EVIDENCE}"
TOPICS="${TOPICS:-$DEALER_LEDGER_EVIDENCE}"
export JAVA_HOME="${JAVA_HOME:-$(ls -d /usr/lib/jvm/*jre-17* 2>/dev/null | head -1)}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# Real delivery, not just a log line. The alert() contract — every clause of which is a bug that
# actually happened — now lives in ONE place, oe-alert.sh, because a second copy would be correct
# on the day it was pasted and free to rot afterwards. test-alert-delivery.sh still asserts it
# end-to-end through THIS script against a controlled local endpoint. 2026-08-12.
# shellcheck source=/dev/null
. "$OE_DIR/oe-alert.sh"

# The staleness guard's own sources, needed by the contention branch below as well — ONE definition, so
# the two cannot disagree about when the last run succeeded.
RUNS="$NAS_DIR/kafka/dev/_manifest/runs.log"
BOOTSTRAP_MARK="$NAS_DIR/kafka/dev/_manifest/.first_seen"
STALE_H="${STALE_H:-8}"

exec 9>"$LOCK"
if ! flock -n 9; then
  # Contention used to exit 0 immediately, BEFORE the staleness check below — so a hung archiver could
  # hold this lock through the whole 24h retention window while every later cron run reported success
  # and nothing ever noticed the evidence expiring (r12 #6). A busy lock is only benign if the last
  # run actually succeeded recently; that is exactly what the staleness check knows, so it runs first.
  # Contention used to exit 0 immediately, BEFORE the staleness guard below — so a hung archiver could
  # hold this lock through the whole 24h retention window while every later cron run reported success
  # and nothing noticed the evidence expiring (r12 #6). A busy lock is only benign if a run actually
  # SUCCEEDED recently, and the runs log is what knows that; it is the same source the guard uses.
  log "another dev-archive run holds $LOCK"
  _ok=$(awk '/failed=0/{ts=$1} END{if(ts!="") print ts}' "$RUNS" 2>/dev/null)
  _ok_epoch=$(date -u -d "$(printf '%s' "${_ok:-}" | sed 's/T/ /; s/Z//')" +%s 2>/dev/null)
  case "${_ok_epoch:-}" in (*[!0-9]*|"") _ok_epoch=0 ;; esac
  _age_h=$(( ( $(date -u +%s) - _ok_epoch ) / 3600 ))
  if [ "$_ok_epoch" -eq 0 ] || [ "$_age_h" -ge "${STALE_H:-8}" ]; then
    alert "dev archive: the lock has been held while NO run has succeeded for ${_age_h}h — a run is wedged and dev evidence expires at 24h"
    exit 1
  fi
  log "  last success was ${_age_h}h ago — the holder will cover this range, exiting clean"
  exit 0
fi

# Fail loud, never silently local: dev evidence staged on this box is evidence that is still one
# disk failure from gone, and a fallback that looks like success is how months of nothing happen.
if [ ! -d "$NAS_DIR" ] || ! touch "$NAS_DIR/.oe_probe_dev.$$" 2>/dev/null; then
  log "FATAL: NAS '$NAS_DIR' not mounted or not writable — dev evidence NOT archived."
  log "       Dev's signal-fired topic expires in 24h; fix the mount before the window closes."
  exit 1
fi
rm -f "$NAS_DIR/.oe_probe_dev.$$"

# No trading-day gate on purpose: dev is also exercised outside RTH (replays, overnight work), and
# a gate here would drop that evidence on exactly the days someone was testing something unusual.
log "=== dev evidence archive start (ET $(TZ=America/New_York date '+%a %Y-%m-%d %H:%M')) ==="
rc=0
env ARCHIVE_DIR="$NAS_DIR" ENV=dev BOOTSTRAP="$DEV_BOOTSTRAP" TOPICS="$TOPICS" \
    ARCHIVE_JOB=dev bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1

# --- staleness guard: shout BEFORE the 24h window closes, not after ---------------------------
# Measures the only thing that matters — how long since a dev run last SUCCEEDED — and escalates
# while there is still time to act. At a 2h cadence, 8h means four consecutive failures, so this
# fires with two thirds of the 24h retention window still left. Deliberately NOT based on record
# counts: dev is legitimately idle at weekends, and an idle-day alarm is an alarm nobody reads.
#
# FAILS CLOSED. "No successful run has EVER been recorded" is the most dangerous state, not the
# safest: a freshly deployed or freshly broken collector whose every run has failed would sail past
# the retention boundary in silence if absence were treated as "fine". Absent runs.log, absent
# success line, and unparseable timestamps all alert. (Codex, 2026-08-08: the earlier version
# failed open on exactly these three.)
# (RUNS/BOOTSTRAP_MARK are defined above the lock: the contention branch reads the same runs log the
# staleness guard does, so the two cannot drift about what "recently succeeded" means.)
[ -f "$BOOTSTRAP_MARK" ] || date -u +%s > "$BOOTSTRAP_MARK" 2>/dev/null

stale_reason=""
last_ok_line=$(awk '/failed=0/{ts=$1} END{if(ts!="") print ts}' "$RUNS" 2>/dev/null)
if [ -z "$last_ok_line" ]; then
  # Never succeeded. Allow one grace window from first-seen so a genuinely new install does not
  # page on its very first minute, then alert — and keep alerting.
  first_seen=$(cat "$BOOTSTRAP_MARK" 2>/dev/null)
  case "${first_seen:-}" in (*[!0-9]*|"") first_seen=0 ;; esac
  grace_h=$(( ( $(date -u +%s) - first_seen ) / 3600 ))
  [ "$first_seen" -eq 0 ] && stale_reason="no successful run recorded and the bootstrap marker is unreadable"
  [ "$first_seen" -ne 0 ] && [ "$grace_h" -ge 4 ] && stale_reason="NO successful dev archive has EVER been recorded (${grace_h}h since this collector was first seen)"
else
  last_ok_epoch=$(date -u -d "$(printf '%s' "$last_ok_line" | sed 's/T/ /; s/Z//')" +%s 2>/dev/null)
  case "${last_ok_epoch:-}" in
    (*[!0-9]*|"") stale_reason="last-success timestamp '$last_ok_line' could not be parsed — treating as stale" ;;
    (*)
      now=$(date -u +%s)
      if [ "$last_ok_epoch" -gt "$now" ]; then
        stale_reason="last-success timestamp '$last_ok_line' is in the FUTURE — clock skew, treating as stale"
      else
        age_h=$(( (now - last_ok_epoch) / 3600 ))
        [ "$age_h" -ge "$STALE_H" ] && stale_reason="last SUCCESSFUL dev archive was ${age_h}h ago"
      fi
      ;;
  esac
fi

if [ -n "$stale_reason" ]; then
  log "ALERT: $stale_reason"
  log "ALERT: dev dealer-ledger-signal-fired retention is 24h — evidence expires unarchived. Act now."
  alert "🚨 dev calibration evidence at risk — $stale_reason. Retention is 24h; unarchived fires are lost permanently. Host $(hostname), log $LOG"
  rc=1
fi

log "=== dev evidence archive done rc=$rc ==="
exit "$rc"
