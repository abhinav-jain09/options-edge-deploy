#!/usr/bin/env bash
# ensure-es-liquidity-engine-topics.sh - es-liquidity-engine-service's two MBO output topics must be right BEFORE the MBO engine publishes (the first-deploy barrier, as
# ensure-es-liquidity-wall-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   es.mbo.level-flow   1 partition, delete, retention.ms EXACTLY 86400000 (1 day), retention.bytes EXACTLY 268435456 (256 MiB cap: ~22 rows a second)
#   es.mbo.liquidity-events 1 partition, delete, retention.ms EXACTLY -1, retention.bytes EXACTLY 134217728 (128 MiB cap; NOT compacted: the event log)
#   es.mbo.snapshot     1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   es.mbo.health       1 partition, delete, retention.ms EXACTLY 86400000 (1 day)
#   es.mbo.level-flow-final 1 partition, delete, retention.ms EXACTLY 86400000 (1 day), retention.bytes EXACTLY 268435456
# An existing topic is never recreated, altered or shrunk: any difference fails closed. The input topic es.rithmic.mbo and the drop engine's es.drop.* topics belong to other
# owners and are not created here (the drop runtime creates its own outputs at start). dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes
# declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-es-liquidity-engine-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); es-liquidity-engine has no production topics - refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${ES_LIQUIDITY_ENGINE_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${ES_LIQUIDITY_ENGINE_MIN_INSYNC_REPLICAS:-1}"
ONE_DAY_MS=86400000
LEVEL_FLOW_BYTES=268435456
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-es-liquidity-engine-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

LEVEL_FLOW="${KAFKA_ES_MBO_LEVEL_FLOW_TOPIC:-es.mbo.level-flow}"
HEALTH="${KAFKA_ES_MBO_HEALTH_TOPIC:-es.mbo.health}"
FINAL="${KAFKA_ES_MBO_FINAL_TOPIC:-es.mbo.level-flow-final}"
SNAPSHOT="${KAFKA_ES_MBO_SNAPSHOT_TOPIC:-es.mbo.snapshot}"
EVENTS="${KAFKA_ES_MBO_EVENT_TOPIC:-es.mbo.liquidity-events}"
EVENTS_BYTES=134217728

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

# The level-flow topic carries a DECLARED retention.bytes (the cap), which the shared routine does not create. It is created here WITH it, the way ensure-level-absorption-topics.sh creates
# its events topic, then verified by the shared routine with create-if-absent=no (so it can never create one at the default shape) and its retention.bytes compared exactly.
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
ensure_with_bytes "$LEVEL_FLOW" delete "$ONE_DAY_MS" "$LEVEL_FLOW_BYTES" || rc=1
ensure_with_bytes "$SNAPSHOT" compact -1 -1 || rc=1
ensure_with_bytes "$EVENTS" delete -1 "$EVENTS_BYTES" || rc=1
ensure_with_bytes "$FINAL" delete "$ONE_DAY_MS" "$LEVEL_FLOW_BYTES" || rc=1
ensure_topic "$HEALTH" delete "exact:$ONE_DAY_MS" 1048576 || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-es-liquidity-engine-topics FAILED - the MBO engine must not publish ==="
  exit 1
fi
echo "=== ensure-es-liquidity-engine-topics OK ==="
