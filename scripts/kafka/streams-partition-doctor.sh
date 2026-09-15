#!/usr/bin/env bash
# streams-partition-doctor.sh — find Kafka Streams apps that refuse to run because an INTERNAL topic
# (…-repartition / …-changelog / FK-join subscription topic) has the wrong partition count, and repair them.
#
# WHY: Streams sizes each internal topic from its sub-topology's sources when it first creates it, and
# afterwards only VALIDATES it. When a source was once the wrong size (auto-created at the broker default,
# created by a mirror before the declaration ran, grown later), the app logs
#   Existing internal topic <T> has invalid partitions: expected: <E>; actual: <A>
# and never processes anything. It happened on dev/es4/prod again and again (2026-09-10 strike-flow
# classifier, 2026-09-14 es-spx-align …) and was repaired by hand each time. The count Streams wants is
# computed by Streams itself (InternalTopicManager throws on the FIRST mismatch per start, repartition
# topics before changelogs), so the doctor never guesses a number: it reads it from the log, confirms the
# live topic still has the rejected count, deletes only that topic with the app scaled to 0, lets Streams
# recreate it, and repeats until the app runs.
#
# What a repair costs: a recreated repartition topic starts empty and the downstream task resumes by the
# app's own auto.offset.reset (records in flight can be skipped for `latest` apps). A deleted changelog's
# store is wiped by Streams on restart (checkpoint beyond the new log => TaskCorruptedException => restore)
# and rebuilt from the new, empty changelog — the price of the app running at all.
# Co-partitioning errors (two SOURCES with different counts) are NOT repairable here: they are reported
# and the exit is non-zero, because only the source topic declarations can fix them.
#
# Environment (the caller sets them for its host):
#   KUBECTL        kubectl prefix incl. namespace       e.g. "kubectl --context docker-desktop -n options-edge"
#   KUBECTL_SCALE  prefix used for `scale` (admission policy needs the deployer SA); default $KUBECTL
#   KAFKA_TOPICS   kafka-topics prefix incl. bootstrap  e.g. "/opt/kafka/current/bin/kafka-topics.sh --bootstrap-server localhost:9092"
#   DOCTOR_MAX_ROUNDS            repair rounds per app (default 8; one rejected topic is reported per start)
#   DOCTOR_READY_TIMEOUT         seconds to wait for READY after a repair (default 420)
#   DOCTOR_READY_GRACE_SECONDS   READY must hold this long with no new rejection (default 30)
#   DOCTOR_POLL_SECONDS          poll interval (default 5)
#
# Usage: streams-partition-doctor.sh [--repair] [deployment ...]     (default: every deployment with replicas > 0)
# Exit:  0 all healthy or repaired
#        3 not repairable here (co-partitioning, or a rejected topic that is not the app's own internal topic)
#        4 a repair did not bring the app READY, or a mismatch exists and --repair was not given
#        5 unknown: the cluster or a pod's log could not be read
set -uo pipefail

REPAIR=false
DEPLOYS=""
for a in "$@"; do
  case "$a" in
    --repair) REPAIR=true ;;
    -*) echo "unknown option: $a" >&2; exit 2 ;;
    *) DEPLOYS="$DEPLOYS $a" ;;
  esac
done
: "${KUBECTL:?KUBECTL is required}"
: "${KAFKA_TOPICS:?KAFKA_TOPICS is required}"
KUBECTL_SCALE="${KUBECTL_SCALE:-$KUBECTL}"
MAX_ROUNDS="${DOCTOR_MAX_ROUNDS:-8}"
READY_TIMEOUT="${DOCTOR_READY_TIMEOUT:-420}"
GRACE="${DOCTOR_READY_GRACE_SECONDS:-30}"
SLEEP="${DOCTOR_POLL_SECONDS:-5}"
STEP=$SLEEP; [ "$STEP" -ge 1 ] 2>/dev/null || STEP=1          # waits count attempts, so SLEEP=0 cannot spin forever
READY_POLLS=$(( READY_TIMEOUT / STEP )); [ "$READY_POLLS" -ge 1 ] || READY_POLLS=1

MISMATCH_RE='Existing internal topic [^ ]+ has invalid partitions: expected: [0-9]+; actual: [0-9]+'
COPARTITION_RE='TopologyException: Invalid topology: .*(co-partition|number of partitions)'
APPID_RE='stream-client \[[^]]+\]'

log() { echo "[doctor $(date '+%H:%M:%S')] $*"; }

rc=0
# Keep the most serious outcome: 3 (needs a declaration fix) > 4 (repair failed) > 5 (unknown).
set_rc() {
  case "$rc:$1" in
    0:*) rc=$1 ;;
    5:3|5:4|4:3) rc=$1 ;;
  esac
}

# A deployment scaled to 0 by the doctor is ALWAYS scaled back, whatever ends the run.
SCALED_DOWN="" SCALED_REPLICAS=""
restore_scale() {
  if [ -n "$SCALED_DOWN" ]; then
    log "restoring $SCALED_DOWN to $SCALED_REPLICAS replica(s)"
    $KUBECTL_SCALE scale deploy/"$SCALED_DOWN" --replicas="$SCALED_REPLICAS" >/dev/null 2>&1
    SCALED_DOWN=""
  fi
}
trap 'restore_scale' EXIT
trap 'restore_scale; exit 130' INT TERM

if [ -z "$DEPLOYS" ]; then
  if ! DEPLOYS=$($KUBECTL get deploy -o jsonpath='{range .items[?(@.spec.replicas>0)]}{.metadata.name}{"\n"}{end}' 2>/dev/null); then
    log "UNKNOWN: cannot list deployments"
    exit 5
  fi
fi

selector() {
  local sel
  sel=$($KUBECTL get deploy "$1" -o go-template='{{range $k,$v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>/dev/null)
  printf '%s' "${sel%,}"
}

# Names of the deployment's pods that are not in a terminal phase (Evicted/Failed/Succeeded pods never go
# away on a scale-down and have no readable log, so they must not block a repair or count as unknown).
app_pods() {
  local sel; sel=$(selector "$1")
  [ -n "$sel" ] || return 0
  $KUBECTL get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' 2>/dev/null \
    | awk '$1 != "" && $2 != "Failed" && $2 != "Succeeded" {print $1}'
}

# Logs of the deployment's live pods: current container from its start (byte-bounded keeps the HEAD,
# where Streams logs the rejection) plus the previous, crashed container. An unreadable current log
# prints UNREADABLE_LOG so it counts as unknown, never healthy; --previous fails when nothing crashed.
app_logs() {
  local p
  for p in $(app_pods "$1"); do
    $KUBECTL logs "$p" --all-containers --limit-bytes=16000000 --request-timeout=60s 2>/dev/null || echo "UNREADABLE_LOG $p"
    $KUBECTL logs "$p" --all-containers --previous --limit-bytes=16000000 --request-timeout=60s 2>/dev/null
  done
}

# kafka-topics --topic is a regular expression: escape the dots so "a.b-x" never also matches "a-b-x".
topic_re() { printf '%s' "$1" | sed 's/\./\\./g'; }

partitions_of() {   # live partition count, empty when absent/unreadable
  $KAFKA_TOPICS --describe --topic "$(topic_re "$1")" 2>/dev/null \
    | awk -F'\t' -v t="$1" '$1 == "Topic: " t {for (i = 1; i <= NF; i++) if ($i ~ /^PartitionCount: /) {sub(/^PartitionCount: /, "", $i); print $i; exit}}'
}

topic_exists() { $KAFKA_TOPICS --list 2>/dev/null | grep -qxF "$1"; }

# Streams application ids the app printed ("stream-client [<appId>-<uuid>]").
app_ids() {
  printf '%s\n' "$1" | grep -oE "$APPID_RE" \
    | sed -E 's/^stream-client \[//; s/\]$//; s/-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$//' | sort -u
}

# From log text $1 print "topic expected actual" for rejections that are STILL true on the broker and
# whose topic is an internal topic of one of the app's own application ids; "FOREIGN topic e a" otherwise.
# A stale line (topic since recreated or fixed) prints nothing, so it can never cause a delete.
live_rejections() {
  local text="$1" ids m t e a cur own id
  ids=$(app_ids "$text")
  printf '%s\n' "$text" | grep -oE "$MISMATCH_RE" | sort -u | while read -r m; do
    [ -n "$m" ] || continue
    t=$(printf '%s' "$m" | awk '{print $4}')
    e=$(printf '%s' "$m" | sed -E 's/.*expected: ([0-9]+);.*/\1/')
    a=$(printf '%s' "$m" | sed -E 's/.*actual: ([0-9]+).*/\1/')
    cur=$(partitions_of "$t")
    if [ -z "$cur" ] || [ "$cur" = "$e" ] || [ "$cur" != "$a" ]; then
      continue
    fi
    own=""
    for id in $ids; do case "$t" in "$id"-*) own=yes ;; esac; done
    case "$t" in
      *-repartition|*-changelog|*-SUBSCRIPTION-REGISTRATION-*-topic|*-SUBSCRIPTION-RESPONSE-*-topic) ;;
      *) own="" ;;
    esac
    if [ -n "$own" ]; then echo "$t $e $a"; else echo "FOREIGN $t $e $a"; fi
  done
}

ready() {
  local d="$1" want have
  want=$($KUBECTL get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null)
  have=$($KUBECTL get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  [ -n "$want" ] && [ "${have:-0}" -ge "$want" ] 2>/dev/null
}

for d in $DEPLOYS; do
  logs=$(app_logs "$d")
  unread=$(printf '%s\n' "$logs" | awk '$1 == "UNREADABLE_LOG" {printf "%s ", $2}')
  cop=$(printf '%s\n' "$logs" | grep -E "$COPARTITION_RE" | head -1)
  if [ -n "$cop" ]; then
    log "UNREPAIRABLE $d: co-partitioning error (fix the SOURCE topic declarations): $(printf '%s' "$cop" | cut -c1-220)"
    set_rc 3
    continue
  fi
  rej=$(live_rejections "$logs")
  foreign=$(printf '%s\n' "$rej" | awk '$1 == "FOREIGN" {printf "%s ", $2}')
  todo=$(printf '%s\n' "$rej" | awk 'NF == 3')
  if [ -n "$foreign" ]; then
    log "REFUSED $d: rejected topic(s) not an internal topic of this app's application.id: $foreign"
    set_rc 3
  fi
  if [ -z "$todo" ]; then
    if [ -n "$unread" ]; then log "INCONCLUSIVE $d: could not read logs of: $unread"; set_rc 5
    elif [ -z "$foreign" ]; then log "OK $d"; fi
    continue
  fi
  if [ "$REPAIR" != true ]; then
    printf '%s\n' "$todo" | while read -r t e a; do log "MISMATCH $d: $t has $a partition(s), Streams expects $e"; done
    set_rc 4
    continue
  fi

  replicas=$($KUBECTL get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null)
  [ "${replicas:-0}" -gt 0 ] 2>/dev/null || replicas=1
  round=0 healed=false repaired="" aborted=false
  while [ -n "$todo" ] && [ "$round" -lt "$MAX_ROUNDS" ]; do
    round=$((round + 1))
    log "REPAIR $d round $round: $(printf '%s\n' "$todo" | awk '{printf "%s (%s->%s) ", $1, $3, $2}')"
    SCALED_DOWN="$d" SCALED_REPLICAS="$replicas"
    $KUBECTL_SCALE scale deploy/"$d" --replicas=0 >/dev/null 2>&1
    # Every live pod gone before a topic is touched: a running instance would re-create the internal
    # topic from its stale metadata the moment it is deleted.
    gone=false
    for i in $(seq 1 120); do
      [ -z "$(app_pods "$d")" ] && { gone=true; break; }
      sleep "$SLEEP"
    done
    if [ "$gone" != true ]; then
      log "FAILED $d: pods still present after scale to 0; nothing deleted"
      aborted=true
      break
    fi
    for t in $(printf '%s\n' "$todo" | awk '{print $1}'); do
      $KAFKA_TOPICS --delete --topic "$(topic_re "$t")" >/dev/null 2>&1
      for i in $(seq 1 60); do topic_exists "$t" || break; sleep "$SLEEP"; done
      topic_exists "$t" && log "WARN $d: $t still listed after delete"
      case " $repaired " in *" $t "*) ;; *) repaired="$repaired $t" ;; esac
    done
    $KUBECTL_SCALE scale deploy/"$d" --replicas="$replicas" >/dev/null 2>&1
    SCALED_DOWN=""
    todo=""
    for i in $(seq 1 "$READY_POLLS"); do
      todo=$(live_rejections "$(app_logs "$d")" | awk 'NF == 3')
      [ -n "$todo" ] && break
      if ready "$d"; then
        # A handler that replaces dead stream threads can answer READY while Streams still refuses a
        # topic: READY counts only after a grace period with no live rejection.
        sleep "$GRACE"
        todo=$(live_rejections "$(app_logs "$d")" | awk 'NF == 3')
        [ -z "$todo" ] && healed=true
        break
      fi
      sleep "$SLEEP"
    done
    [ "$healed" = true ] && break
  done
  restore_scale
  if [ "$healed" = true ]; then
    log "REPAIRED $d (recreated by Streams:$repaired)"
  else
    [ "$aborted" = true ] || log "FAILED $d: not READY after $round repair round(s)${todo:+; still rejected: $(printf '%s\n' "$todo" | awk '{printf "%s ", $1}')}"
    set_rc 4
  fi
done
exit "$rc"
