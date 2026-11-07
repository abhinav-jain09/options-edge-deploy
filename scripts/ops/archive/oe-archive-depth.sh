#!/usr/bin/env bash
# oe-archive-depth.sh — the DEV depth archive: es.rithmic.depth, the linearized ES tape, the liquidity-wall
# closures and the Rithmic MBO order events (es.rithmic.mbo), captured to the NAS every six hours from the dev Mac (launchd/oe-archive-depth.plist: 02:00, 08:00,
# 14:00 and 20:00 ET).
#
# WHY A SEPARATE JOB, ON A SEPARATE SCHEDULE. es.rithmic.depth carries a 24-HOUR retention on dev (raised from 12 on 2026-11-07, owner order) (ES Level
# Absorption Requirements section 9, document A; ES Book Liquidity Walls Requirements rev 11 section 8,
# document D). The nightly 17:10 ET pass and the 17:01 es4 pass both run once a day, so either would lose half of
# every session; four captures a day against twenty-four hours of retention means one missed run costs nothing and
# two consecutive misses cost a known, reported range. Document A section 9 item 6 names this schedule and keeps
# it apart from the nightly run; item 1 names the invocation: ENV=dev TOPICS="$OE_DEPTH_TOPICS_dev" against the
# dev broker and the dev schema registry.
#
# ONE DEFINITION OF THE SET. The four topics (es.rithmic.mbo joined 2026-11-05) are OE_DEPTH_TOPICS_dev in oe-topics.env and nowhere else: this
# wrapper sources that file, refuses to run without the variable, and passes it as the archiver's TOPICS
# override. The override is load-bearing — ENV=dev has no depth default in oe-archive-kafka.sh, so a wrapper that
# forgot it would archive the prod default set against the dev broker and report success. The archiver itself
# requires the variable too (unset, sourced, non-empty), exactly as it requires OE_ES4_TOPICS.
#
# WHAT THE ARCHIVER DOES WITH THEM (oe-archive-kafka.sh): es.rithmic.depth, es.rithmic.mbo and futures.liquidity-wall.closures
# are OE_CANONICAL_ARCHIVE_TOPICS, read committed through CanonicalArchiveDecoder.java under their released
# schemas (RithmicDepthFrame id 3301, RithmicMboEvent id 3302, LiquidityWallClosure id 3401; a frame under any other id fails the capture
# with the checkpoint untouched); underlying.es.trades.linearized is a committed-read, STRICT topic (an absent
# tape is a FAILED capture, never a quiet SKIP). The canonical path needs the JDK 21+ helper jar and the
# contracts jar beside the archiver — install-dev-mac-depth-archive.sh refuses to install without them.
#
# ON_LOCK_BUSY=fail, deliberately. The archiver's rule: a busy lock is only "exiting clean" for a job that
# will retry inside its own data's retention window. The wrapper stays loud anyway (a run that cannot take its lock is a
# failure, not a deferral); the 24-hour window only means a later run can still recover the range.
#
# THE ARCHIVE ROOT IS RESOLVED, NOT PINNED (BZ 360, the watchdog's lesson): the NAS is not mounted at the same
# path on every host — on the dev Mac //192.168.100.100/database is on $HOME/nas-db, on .252 it is
# /mnt/nas/optionsedge — and a pinned wrong path is a job that fails every six hours until someone stops
# reading its alerts. ARCHIVE_DIR, when set, wins; otherwise the first existing candidate is used and the log
# says which. Whatever root is used MUST hold the archiver's NAS sentinel (.oe_nas_sentinel): a CIFS mount that
# dropped leaves an ordinary empty directory at the same path, and writing dev depth to local disk while the log
# says NAS is the false-mount success the sentinel exists to refuse. The sentinel is required here for EVERY
# root, which matters because the archiver itself only demands it for /mnt/nas and /Volumes/nas paths and needs
# ALLOW_NON_NAS=true for any other — that flag is passed only after this wrapper has read the sentinel.
#
# USAGE: run by launchd (see the plist). By hand, for a one-off capture:
#   ARCHIVE_DIR=/path/to/nas/optionsedge bash oe-archive-depth.sh
set -uo pipefail
export PATH="${PATH:-/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}"

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
ARCHIVER="${ARCHIVER:-$OE_DIR/oe-archive-kafka.sh}"
DEV_BOOTSTRAP="${DEV_BOOTSTRAP:-192.168.100.102:19092}"
LOG="${LOG:-$OE_DIR/archive-depth.log}"
ARCHIVE_ROOT_CANDIDATES="${ARCHIVE_ROOT_CANDIDATES:-$HOME/nas-db/optionsedge /Volumes/database/optionsedge /mnt/nas/optionsedge}"
OE_TOPICS_ENV="${OE_TOPICS_ENV:-$OE_DIR/oe-topics.env}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# Real delivery, not just a log line: the ONE alert implementation (oe-alert.sh). A missing copy must not stop
# the capture — a wrapper that cannot alert is still a wrapper that can archive — but it must say so.
# shellcheck source=/dev/null
[ -r "$OE_DIR/oe-alert.sh" ] && . "$OE_DIR/oe-alert.sh"
if ! declare -F alert >/dev/null 2>&1; then
  log "WARN oe-alert.sh missing beside this script — alerts will be LOGGED ONLY, not delivered"
  alert() { log "ALERT (undelivered, no oe-alert.sh): $*"; }
fi

# The set must come FROM THE FILE. An inherited OE_DEPTH_TOPICS_dev would satisfy the check below and let a
# caller archive a different set behind the canonical file's back — equality by luck, not by construction.
[ -r "$OE_TOPICS_ENV" ] || { log "FATAL: '$OE_TOPICS_ENV' missing or unreadable — refusing to archive an unknown depth set"; exit 1; }
unset OE_DEPTH_TOPICS_dev
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"
if [ -z "${OE_DEPTH_TOPICS_dev:-}" ]; then
  log "FATAL: oe-topics.env did not define OE_DEPTH_TOPICS_dev — the dev depth set is defined nowhere, nothing archived"
  exit 1
fi

# --- the archive root: resolved, then proven to be the NAS ----------------------------------------------
ROOT_SOURCE="ARCHIVE_DIR"
if [ -z "${ARCHIVE_DIR:-}" ]; then
  for _c in $ARCHIVE_ROOT_CANDIDATES; do
    [ -d "$_c" ] || continue
    ARCHIVE_DIR="$_c"; ROOT_SOURCE="resolved from ARCHIVE_ROOT_CANDIDATES"; break
  done
fi
if [ -z "${ARCHIVE_DIR:-}" ]; then
  log "FATAL: no archive root on this host (tried: $ARCHIVE_ROOT_CANDIDATES) — this is a MOUNT problem here; dev depth NOT archived"
  alert "dev depth archive cannot run on $(hostname): no archive root (tried: $ARCHIVE_ROOT_CANDIDATES). es.rithmic.depth expires at 24h."
  exit 1
fi
SENTINEL="${SENTINEL:-$ARCHIVE_DIR/.oe_nas_sentinel}"
if [ ! -s "$SENTINEL" ]; then
  log "FATAL: '$ARCHIVE_DIR' ($ROOT_SOURCE) has no NAS sentinel '$SENTINEL' — the share is NOT mounted there; refusing to write what would really be local disk"
  alert "dev depth archive refused on $(hostname): $ARCHIVE_DIR has no NAS sentinel (share not mounted). es.rithmic.depth expires at 24h."
  exit 1
fi
NON_NAS=""
case "$ARCHIVE_DIR" in
  /mnt/nas/*|/Volumes/nas/*) : ;;
  *) NON_NAS="ALLOW_NON_NAS=true" ;;   # the sentinel above is what earns this flag
esac

# JDK and Kafka CLI: the archiver finds its own JDK 21+ for the canonical path (CANONICAL_ARCHIVE_JAVA, then
# JAVA_HOME, then PATH); on a Mac the JDK 17 fallback it also knows does not exist, so JAVA_HOME is resolved from
# java_home when the caller set none. KAFKA_BIN defaults to the Homebrew kafka when present; the archiver refuses to
# run without kafka-get-offsets.sh there, so a wrong path is loud, not silent.
if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
fi
if [ -z "${KAFKA_BIN:-}" ] && [ -d /opt/homebrew/opt/kafka/bin ]; then KAFKA_BIN=/opt/homebrew/opt/kafka/bin; fi

log "=== dev depth archive start (ET $(TZ=America/New_York date '+%a %Y-%m-%d %H:%M')) root=$ARCHIVE_DIR ($ROOT_SOURCE) topics=[$OE_DEPTH_TOPICS_dev] ==="
rc=0
env ARCHIVE_DIR="$ARCHIVE_DIR" ENV=dev BOOTSTRAP="$DEV_BOOTSTRAP" TOPICS="$OE_DEPTH_TOPICS_dev" \
    ARCHIVE_JOB=depth ON_LOCK_BUSY=fail ${NON_NAS:+"$NON_NAS"} \
    ${JAVA_HOME:+JAVA_HOME="$JAVA_HOME"} ${KAFKA_BIN:+KAFKA_BIN="$KAFKA_BIN"} \
    bash "$ARCHIVER" 2>&1 | tee -a "$LOG"
[ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
if [ "$rc" -ne 0 ]; then
  log "ALERT: the dev depth archive run FAILED — es.rithmic.depth retention is 24h; at most three more scheduled runs can still capture this range"
  alert "dev depth archive FAILED on $(hostname) (ET $(TZ=America/New_York date '+%H:%M')). es.rithmic.depth retention is 24h: three more runs can still capture this range. Log $LOG"
fi
log "=== dev depth archive done rc=$rc ==="
exit "$rc"
