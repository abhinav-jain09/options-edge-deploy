#!/usr/bin/env bash
# oe-archive-gamma-zone.sh — hourly capture of the GAMMA ZONE CORPUS to a file on the DEV Mac.
#
# WHY THIS EXISTS. options.spx.gamma-zone.log IS the corpus: gamma-zone-service publishes one
# measurement per folded bar and scoring the service means reading months of them back. Kafka is not
# an archive, and two things take the record away:
#   1. The dev wipes. The topic is excluded from all four deletion paths BY NAME -- and that is
#      exactly the protection that has failed before: a cleanup run destroyed ten keep-list topics
#      because the script printed the list it was keeping and then did not read it.
#   2. Its own cap. The topic is delete-only at retention.ms=-1 with retention.bytes=1 GiB, about 87
#      sessions, after which the OLDEST emissions drop off the back. That bound is deliberate (this
#      project has run a disk to zero before), so the file on disk is what carries the corpus past
#      three months.
#
# WHY ON THE DEV MAC, AND NOT ON .252. The .252 crontab's archive entries are disabled, by the owner,
# with the reason written beside them: "archive moves to dev". This wrapper is that move for this one
# topic. Running here also removes a dependency: from the 201 Mac the dev broker is only reachable
# through an SSH tunnel on 127.0.0.1:19092 whose launchd job has exited 255 before, and the broker
# advertises host.docker.internal, which resolves to 127.0.0.1 on any Mac running Docker Desktop. On
# THIS host both localhost:19092 and host.docker.internal:19092 reach the real broker directly, with
# nothing in between to fail quietly.
#
# LOCAL DISK ON PURPOSE (owner, 2026-11-11: "dont worry about nas save the file on dev"). This is the
# one archive wrapper that does NOT look for the NAS sentinel, because its destination is meant to be
# local. The NAS is at 100% (906G of 906G) and no archive of any kind has run since 2026-10-26, so a
# NAS destination here would be a job that fails every hour. ALLOW_NON_NAS is therefore passed
# deliberately and unconditionally, and the destination is printed on every run so "where did it go"
# is never a question.
#
# WHAT IT WRITES — oe-archive-kafka.sh's own layout, nothing bespoke:
#   $ARCHIVE_DIR/kafka/dev/options.spx.gamma-zone.log/dt=<date>/*.jsonl.gz   gzipped JSON Lines
#   $ARCHIVE_DIR/kafka/dev/options.spx.gamma-zone.log/dt=<date>/_manifest.jsonl   sha256 per file
#   $ARCHIVE_DIR/kafka/dev/_manifest/options.spx.gamma-zone.log.offsets      the checkpoint
# Incremental and offset-checkpointed, so an hourly run is cheap and a no-op run costs nothing.
# Verified against the live topic on 2026-11-11 before this file was written: 349 records into one
# file, then +1 on the next run -- no duplication, and each record keeps its key, partition and
# CreateTime.
#
# MOVING IT TO THE NAS LATER is ARCHIVE_DIR plus moving the EXISTING tree with `mv`: the offsets
# checkpoint lives under ARCHIVE_DIR, so a fresh directory would re-archive from offset 0 and
# duplicate everything already captured.
#
# Usage:   bash oe-archive-gamma-zone.sh            (launchd runs exactly this, hourly)
#          ARCHIVE_DIR=/somewhere/else bash oe-archive-gamma-zone.sh
set -uo pipefail
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

OE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVER="${ARCHIVER:-$OE_DIR/oe-archive-kafka.sh}"
OE_TOPICS_ENV="${OE_TOPICS_ENV:-$OE_DIR/oe-topics.env}"
BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY:-http://localhost:8082}"
DEFAULT_ARCHIVE_DIR="$HOME/oe-ops/gamma-zone-corpus"
ARCHIVE_DIR="${ARCHIVE_DIR:-$DEFAULT_ARCHIVE_DIR}"
LOG="${LOG:-$OE_DIR/archive-gamma-zone.log}"
MIN_FREE_MB="${MIN_FREE_MB:-5000}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# Delivery, not just a log line: one definition, shared with every other archive job.
if [ -r "$OE_DIR/oe-alert.sh" ]; then . "$OE_DIR/oe-alert.sh"; else alert() { log "ALERT(undelivered): $*"; }; fi

# THE TOPIC SET MUST COME FROM THE FILE. An inherited GAMMA_ZONE_CORPUS would let a caller archive a
# different set behind the canonical file's back -- equality by luck, not by construction. This is
# the same refusal oe-archive-dev.sh makes, for the same reason.
[ -r "$OE_TOPICS_ENV" ] || { log "FATAL: '$OE_TOPICS_ENV' missing or unreadable — refusing to archive an unknown set"; exit 1; }
# BOTH names are cleared. Clearing only GAMMA_ZONE_CORPUS and then writing
# TOPICS="${TOPICS:-$GAMMA_ZONE_CORPUS}" left the back door wide open: a caller supplying TOPICS
# archived whatever it liked, while this file and its tests both claimed "the set comes from the
# file" (Codex round 1). The assignment is now unconditional, so the claim is true.
unset GAMMA_ZONE_CORPUS TOPICS
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"
: "${GAMMA_ZONE_CORPUS:?oe-topics.env did not define GAMMA_ZONE_CORPUS}"
TOPICS="$GAMMA_ZONE_CORPUS"

[ -x "$ARCHIVER" ] || { log "FATAL: archiver '$ARCHIVER' is missing or not executable"; exit 1; }

# JAVA_HOME and KAFKA_BIN the way the depth wrapper resolves them on this host: the archiver refuses
# without a Kafka CLI, and launchd carries almost no environment.
[ -n "${JAVA_HOME:-}" ] || JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null)"
if [ -z "${KAFKA_BIN:-}" ]; then
  for _k in /opt/homebrew/opt/kafka/bin "$HOME"/kafka-*/bin /opt/kafka/current/bin; do
    [ -x "$_k/kafka-get-offsets.sh" ] && { KAFKA_BIN="$_k"; break; }
  done
fi
[ -n "${KAFKA_BIN:-}" ] || { log "FATAL: no Kafka CLI found (looked for kafka-get-offsets.sh)"; exit 1; }

# AN OVERRIDDEN DESTINATION NEEDS AN EXPLICIT OPT-IN. This wrapper bypasses the archiver's NAS
# guard on purpose, so nothing downstream would question a typo: it would create the directory,
# archive into it, exit 0, and the corpus would accumulate somewhere nobody ever looks (Codex round
# 1). "Local disk is allowed" does not mean "every local path is intended".
#
# RESOLVED PATHS, NOT A PREFIX TEST. The first version matched "$HOME"/oe-ops/* lexically, which
# accepts $HOME/oe-ops/../../anywhere; refusing a literal '..' was still not containment, because
# any symlinked component walks straight out (Codex round 2). Both the approved root and the
# candidate are canonicalised below -- `pwd -P` resolves every symlink -- and compared on a whole
# directory boundary, so /x/oe-opsEVIL cannot pass as /x/oe-ops either. The policy this states is
# "under whatever physical directory $HOME/oe-ops resolves to": if that root is itself a symlink,
# both sides resolve through it and agree, which is the intended reading and not an escape.
canonical_dir() {          # prints the physical path of $1; its FINAL component need not exist
  local d="$1" base parent
  if [ -d "$d" ]; then ( cd "$d" 2>/dev/null && pwd -P ); return; fi
  base=${d##*/}
  parent=${d%/*}
  [ "$parent" = "$d" ] && parent="."
  [ -z "$parent" ] && parent="/"
  [ -d "$parent" ] || return 1
  ( cd "$parent" 2>/dev/null && printf '%s/%s\n' "$(pwd -P)" "$base" )
}

approved_root="$(canonical_dir "$HOME/oe-ops")" || {
  log "FATAL: the approved root '$HOME/oe-ops' does not exist — refusing"; exit 1; }
candidate="$(canonical_dir "$ARCHIVE_DIR")" || {
  log "FATAL: ARCHIVE_DIR '$ARCHIVE_DIR' has no existing parent — refusing to guess where it belongs"
  alert "gamma-zone archive refused on $(hostname): ARCHIVE_DIR '$ARCHIVE_DIR' has no existing parent."
  exit 1; }

case "$candidate/" in
  "$approved_root"/*) : ;;
  *) [ "${ALLOW_ARCHIVE_DIR_OVERRIDE:-false}" = "true" ] || {
       log "FATAL: ARCHIVE_DIR '$ARCHIVE_DIR' resolves to '$candidate', outside '$approved_root' — set ALLOW_ARCHIVE_DIR_OVERRIDE=true to mean it"
       alert "gamma-zone archive refused on $(hostname): ARCHIVE_DIR resolves to '$candidate', outside the approved root."
       exit 1; } ;;
esac

mkdir -p "$ARCHIVE_DIR" 2>/dev/null || true
[ -d "$ARCHIVE_DIR" ] || { log "FATAL: ARCHIVE_DIR '$ARCHIVE_DIR' does not exist and could not be created"; exit 1; }

# Free space BEFORE writing. This destination is the dev Mac's own disk, which also holds the dev
# cluster: filling it would take the environment down, not just the archive.
avail_mb="$(df -m "$ARCHIVE_DIR" | awk 'NR==2 {print $4}')"
if [ -n "$avail_mb" ] && [ "$avail_mb" -lt "$MIN_FREE_MB" ]; then
  log "FATAL: only ${avail_mb}MB free at '$ARCHIVE_DIR', below MIN_FREE_MB=${MIN_FREE_MB} — refusing"
  alert "gamma-zone archive refused on $(hostname): only ${avail_mb}MB free at $ARCHIVE_DIR. The corpus is not being captured."
  exit 1
fi

# A REAL KAFKA HANDSHAKE, not a TCP check. The dev broker advertises host.docker.internal, so a
# client that connects to the port and then cannot resolve the advertised listener fails with
# UnknownHostException -- while a plain port check passes and tells you nothing. oe-archive-dev.sh
# documents exactly this trap. Asking for the topic's offsets exercises the whole path.
# ONE PROBE PER TOPIC. Passing the whole whitespace-joined set as a single --topic asked Kafka for
# one name containing a space: it would reject a healthy broker the moment the set grew past one
# topic, and the failure would read as "the broker is down" (Codex round 1).
# AN EXIT CODE IS NOT AN ANSWER. Discarding the output and trusting exit 0 let a CLI that printed a
# diagnostic, or answered about a topic with no partitions, pass as a healthy handshake -- and the
# archiver downstream treats an absent non-strict topic as "nothing to do" and finishes failed=0.
# The two together are a green run that captured nothing (Codex round 3). The probe now has to come
# back with a line for THE TOPIC IT ASKED ABOUT: kafka-get-offsets prints <topic>:<partition>:<offset>.
for _t in $TOPICS; do
  # BOTH have to hold: the CLI must succeed AND answer about this topic. Checking only the output
  # was my own regression while fixing the exit-code one -- a failing CLI that still printed a
  # matching line would have passed. The test for the unreachable broker caught it.
  if ! _probe="$(env ${JAVA_HOME:+JAVA_HOME="$JAVA_HOME"} "$KAFKA_BIN/kafka-get-offsets.sh" \
                  --bootstrap-server "$BOOTSTRAP" --topic "$_t" 2>/dev/null)"; then
    log "FATAL: no Kafka handshake with '$BOOTSTRAP' for topic '$_t'"
    alert "gamma-zone archive on $(hostname): cannot reach the dev broker at $BOOTSTRAP for '$_t'. The corpus is not being captured."
    exit 1
  fi
  # PER LINE, not "the whole output starts with". A CLI that prints a warning first and the offsets
  # after is a HEALTHY broker, and matching the whole blob refused it -- fail-closed, but refusing a
  # working broker is the same class of mistake as the single --topic one (Codex round 4).
  _ok=false
  while IFS= read -r _line; do
    case "$_line" in "$_t":*) _ok=true; break ;; esac
  done <<PROBE_OUTPUT
$_probe
PROBE_OUTPUT
  if [ "$_ok" != "true" ]; then
    log "FATAL: no usable offsets for '$_t' from '$BOOTSTRAP' (probe said: ${_probe:-<nothing>})"
    alert "gamma-zone archive on $(hostname): the dev broker at $BOOTSTRAP returned no offsets for '$_t'. The corpus is not being captured."
    exit 1
  fi
done

log "archiving $TOPICS -> $ARCHIVE_DIR (local disk by design; ${avail_mb}MB free)"
# ALLOW_NON_NAS is passed deliberately: see LOCAL DISK ON PURPOSE above. ARCHIVE_JOB is its own name
# because it is half the archiver's lock key -- two jobs once shared one, and the daily run exited 0
# having archived nothing, every weekday for two weeks.
#
# `env`, not a bare assignment prefix. ${JAVA_HOME:+JAVA_HOME="$JAVA_HOME"} expands to ONE WORD
# containing '='; bash decides what is an assignment prefix BEFORE expansion, so that word became
# the command name and the run died with exit 127 -- "JAVA_HOME=/opt/...: No such file or
# directory". Found by running this wrapper rather than reading it. env treats every leading
# NAME=VALUE word as an assignment, expanded or not, and a conditional one that expands to nothing
# simply disappears.
# OE_STRICT_TOPICS="$TOPICS": EVERY topic this job archives is evidence. For an ordinary topic the
# archiver treats "absent or unreadable" as a quiet day -- SKIP, absent++, exit 0 -- which for THIS
# topic would be an hourly green run that captured none of the corpus, the exact failure the whole
# wrapper exists to prevent. The preflight probe above narrows the window but cannot close it: the
# topic can go between the probe and the read (Codex round 5).
#
# Passed HERE rather than added to the archiver's own default list, deliberately. That list is
# global, and this topic exists on dev only -- a prod or es4 run would then FAIL on a topic that is
# absent by design. Using "$TOPICS" also keeps it true if the corpus set ever grows.
#
# ON_LOCK_BUSY=fail, like the depth sibling. The archiver defaults to "skip", which exits 0 when
# another run holds the job lock -- so a scheduled run that archived NOTHING would be recorded as a
# success and launchd would see a healthy job. Silence reading as health is how the 17:10 run went
# two weeks archiving nothing (Codex round 1).
env TOPICS="$TOPICS" \
    ARCHIVE_DIR="$ARCHIVE_DIR" \
    ALLOW_NON_NAS=true \
    ON_LOCK_BUSY=fail \
    OE_STRICT_TOPICS="$TOPICS" \
    ENV=dev \
    ARCHIVE_JOB=gamma-zone \
    BOOTSTRAP="$BOOTSTRAP" \
    SCHEMA_REGISTRY="$SCHEMA_REGISTRY" \
    ${JAVA_HOME:+JAVA_HOME="$JAVA_HOME"} KAFKA_BIN="$KAFKA_BIN" \
    "$ARCHIVER" >>"$LOG" 2>&1
rc=$?

if [ "$rc" -ne 0 ]; then
  log "FAILURE: archiver exited $rc"
  alert "gamma-zone archive FAILED on $(hostname) (exit $rc). The corpus is not being captured — see $LOG."
fi
exit "$rc"
