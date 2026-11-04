#!/usr/bin/env bash
# ensure-es-liquidity-wall-topics.sh — es-liquidity-wall-service's seven topics must be right BEFORE the service is ever raised (the same first-deploy barrier as
# ensure-line-acceptance-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]) and this barrier runs from service-deploy SERVICE=es-liquidity-wall; it refuses
# to run unless ENVIRONMENT is exactly "dev" (Jenkinsfile.service-deploy exports the job parameter).
#
#   NO BROKER-WIDE CHANGE. This barrier never reads or alters a broker config. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   futures.liquidity-wall.closures   1 partition, delete,  retention.ms EXACTLY 604800000 (7 days)
#   futures.liquidity-wall.buckets    1 partition, delete,  retention.ms EXACTLY 86400000 (1 day)
#   futures.liquidity-wall.dashboard  1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   futures.liquidity-wall.events     1 partition, delete,  retention.ms EXACTLY -1, retention.bytes EXACTLY 268435456 (256 MiB cap; NOT compacted: the event log)
#   futures.liquidity-wall.state      1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   futures.liquidity-wall.snapshot   1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   futures.liquidity-wall.voice      1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
# The events log and the four pure-compact topics are created WITH their declared retention.bytes (the way ensure-level-absorption-topics.sh creates its events topic: the shared routine
# creates no retention.bytes) and that value is verified exactly. An existing topic is never recreated, altered or shrunk: any difference fails closed. The input topics (es.rithmic.depth, underlying.es.trades.linearized) belong to other services and are not
# created here. No tombstones, no versioned names. dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env (partitions, policy, retention,
# retention.bytes), and this barrier is the check that a deploy runs before the service is raised.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-es-liquidity-wall-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); es-liquidity-wall has no production topics — refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${ES_LIQUIDITY_WALL_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${ES_LIQUIDITY_WALL_MIN_INSYNC_REPLICAS:-1}"
SEVEN_DAYS_MS=604800000
ONE_DAY_MS=86400000
EVENTS_BYTES=268435456
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-es-liquidity-wall-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

CLOSURES="${KAFKA_ES_LIQUIDITY_WALL_CLOSURES_TOPIC:-futures.liquidity-wall.closures}"
BUCKETS="${KAFKA_ES_LIQUIDITY_WALL_BUCKETS_TOPIC:-futures.liquidity-wall.buckets}"
DASHBOARD="${KAFKA_ES_LIQUIDITY_WALL_DASHBOARD_TOPIC:-futures.liquidity-wall.dashboard}"
EVENTS="${KAFKA_ES_LIQUIDITY_WALL_EVENTS_TOPIC:-futures.liquidity-wall.events}"
STATE="${KAFKA_ES_LIQUIDITY_WALL_STATE_TOPIC:-futures.liquidity-wall.state}"
SNAPSHOT="${KAFKA_ES_LIQUIDITY_WALL_SNAPSHOT_TOPIC:-futures.liquidity-wall.snapshot}"
VOICE="${KAFKA_ES_LIQUIDITY_WALL_VOICE_TOPIC:-futures.liquidity-wall.voice}"

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what the service's own describeConfigs sees) and compare exactly. Never altered here.
check_retention_bytes() { # <topic> <exact retention.bytes>
  local topic="$1" want="$2" cfg bytes
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  bytes=$(extract 'retention\.bytes' "$cfg")
  case "${bytes:-}" in
    ''|*[!0-9-]*|-|?*-*|-[!1]*|-1?*) echo "FAIL: '$topic' reports retention.bytes='${bytes:-<unreadable>}', expected exactly $want."; return 1 ;;
  esac
  if [ "$bytes" != "$want" ]; then
    echo "FAIL: '$topic' has retention.bytes=$bytes, expected exactly $want (the declared shape in scripts/kafka/topics.env; never altered here)."
    return 1
  fi
  echo "ok: '$topic' — retention.bytes=$bytes"
}

# The events log and the four pure-compact topics carry a DECLARED retention.bytes (the cap, or -1: retention.ms=-1 alone does not stop byte-based deletion), which the shared routine does
# not create. They are created here WITH it, the way ensure-level-absorption-topics.sh creates its events topic, then verified by the shared routine with create-if-absent=no (so it can never
# create one at the default shape) and their retention.bytes compared exactly.
ensure_with_bytes() { # <topic> <cleanup.policy> <retention.ms> <retention.bytes>
  local topic="$1" policy="$2" ms="$3" bytes="$4" out
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$policy, retention.ms=$ms, retention.bytes=$bytes)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$policy" --config retention.ms="$ms" --config retention.bytes="$bytes" \
                  --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
    LIST="$LIST"$'\n'"$topic"
  fi
  ensure_topic "$topic" "$policy" "exact:$ms" 1048576 no || return 1
  check_retention_bytes "$topic" "$bytes"
}

rc=0
ensure_topic "$CLOSURES"  delete  "exact:$SEVEN_DAYS_MS" 1048576 || rc=1
ensure_topic "$BUCKETS"   delete  "exact:$ONE_DAY_MS"    1048576 || rc=1
for t in "$DASHBOARD" "$STATE" "$SNAPSHOT" "$VOICE"; do
  ensure_with_bytes "$t" compact -1 -1 || rc=1
done
ensure_with_bytes "$EVENTS" delete -1 "$EVENTS_BYTES" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-es-liquidity-wall-topics FAILED — the service must not be raised ==="
  exit 1
fi
echo "=== ensure-es-liquidity-wall-topics OK ==="
