#!/usr/bin/env bash
# ensure-level-absorption-outcome-topics.sh - the two topics of the in-service outcomes of level-absorption-service must be right BEFORE the service rolls
# (the first-deploy barrier, as ensure-break-line-topics.sh). DEV ONLY: refuses to run unless ENVIRONMENT is exactly "dev". Called by ensure-level-absorption-topics.sh on dev.
# options-edge-documents design/es-liquidity/liq-V-in-service-outcomes-design.md sections 6 and 11. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   futures.level-absorption.outcomes    1 partition, delete,  retention.ms EXACTLY 2592000000 (30 days), retention.bytes EXACTLY 268435456 (256 MiB)
#   futures.level-absorption.scoreboard  1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact: the growing table, RESET-PRESERVED)
# NO BROKER-WIDE CHANGE. An existing topic is never recreated, altered or shrunk: any difference fails closed.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-level-absorption-outcome-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); the outcome topics have no production declaration - refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${LEVEL_ABSORPTION_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${LEVEL_ABSORPTION_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-level-absorption-outcome-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics - refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

OUTCOMES="${KAFKA_LEVEL_ABSORPTION_OUTCOMES_TOPIC:-futures.level-absorption.outcomes}"
SCOREBOARD="${KAFKA_LEVEL_ABSORPTION_SCOREBOARD_TOPIC:-futures.level-absorption.scoreboard}"
THIRTY_DAYS=2592000000
OUTCOMES_BYTES=268435456

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all) and compare exactly. Never altered here.
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
  echo "ok: '$topic' - retention.bytes=$bytes"
}

create_with_bytes() { # <topic> <cleanup.policy> <retention.ms> <retention.bytes>
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
}

rc=0
create_with_bytes "$OUTCOMES" delete "$THIRTY_DAYS" "$OUTCOMES_BYTES" || rc=1
create_with_bytes "$SCOREBOARD" compact -1 -1 || rc=1
if [ "$rc" -eq 0 ]; then
  ensure_topic "$OUTCOMES" delete "exact:$THIRTY_DAYS" 1048576 no || rc=1
  check_retention_bytes "$OUTCOMES" "$OUTCOMES_BYTES" || rc=1
  ensure_topic "$SCOREBOARD" compact "exact:-1" 1048576 no || rc=1
  check_retention_bytes "$SCOREBOARD" -1 || rc=1
fi
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-level-absorption-outcome-topics FAILED - level-absorption-service must not roll with the in-service outcomes ==="
  exit 1
fi
echo "=== ensure-level-absorption-outcome-topics OK ==="
