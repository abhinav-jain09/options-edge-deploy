#!/usr/bin/env bash
# ensure-strike-hold-topics.sh - strike-hold-service's two output topics must be right BEFORE the service publishes (the first-deploy barrier, as
# ensure-liquidity-validator-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (four partitions each, exact):
#   options.spx.strike-hold.current  4 partitions, compact,delete, retention.ms EXACTLY -1                                  the newest state of one candidate strike
#   options.spx.strike-hold.scoring  4 partitions, delete,         retention.ms EXACTLY -1, retention.bytes EXACTLY 268435456  ONE evidence row per candidate per session
#
# WHY THIS BARRIER EXISTS AT ALL, given the service ensures its own topics at start-up. The service can set the cleanup policy; it cannot set the RETENTION the
# declaration carries. If the pod reached an empty broker first it would create `scoring` at the cluster default retention, and the evidence record would then be
# aged out from under the measurement — silently, because a topic that exists and accepts writes looks healthy. The barrier makes the shape a precondition of the
# deploy rather than something a race decides.
#
# The difference between the two topics is the point and is enforced here, not assumed: `current` is COMPACTED because the newest record per candidate is the whole
# answer, and `scoring` must NOT be, because every record is a distinct (candidate, session) observation and compaction would discard exactly what the topic
# accumulates. An existing topic is never recreated, altered or shrunk: any difference fails closed. The four INPUT topics belong to other owners and are not created here.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-strike-hold-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); strike-hold has no production topics - refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_ISR="${STRIKE_HOLD_MIN_INSYNC_REPLICAS:-1}"
PARTITIONS=4
SCORING_BYTES=268435456
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-strike-hold-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

CURRENT="${KAFKA_STRIKE_HOLD_OUTPUT_TOPIC:-options.spx.strike-hold.current}"
SCORING="${KAFKA_STRIKE_HOLD_SCORING_TOPIC:-options.spx.strike-hold.scoring}"

# The declared partition width is EXACT. Kafka can add partitions but never remove them, so a topic already wider than the declaration is a fact to fail on, not to
# reconcile: widening is irreversible and the service's single re-key key would leave the extra logs empty for ever.
check_partitions() { # <topic>
  local topic="$1" out n
  if ! out=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$out"; return 1
  fi
  n=$(sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' <<<"$out" | head -1)
  case "${n:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports PartitionCount='${n:-<unreadable>}'."; return 1 ;;
  esac
  if [ "$n" != "$PARTITIONS" ]; then
    echo "FAIL: '$topic' has $n partitions, expected exactly $PARTITIONS (the declared shape in scripts/kafka/topics.env; never altered here)."
    return 1
  fi
  echo "ok: '$topic' — partitions=$n"
}

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what the service's own describeConfigs sees) and compare exactly.
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

create_if_absent() { # <topic> <cleanup.policy> <retention.ms> [<retention.bytes>]
  local topic="$1" policy="$2" ms="$3" bytes="${4:-}" out
  if grep -qxF "$topic" <<<"$LIST"; then
    return 0
  fi
  echo "creating '$topic' ($PARTITIONS partitions, RF=$RF, cleanup.policy=$policy, retention.ms=$ms${bytes:+, retention.bytes=$bytes})"
  local args=(--create --if-not-exists --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$RF"
              --config cleanup.policy="$policy" --config retention.ms="$ms"
              --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR")
  if [ -n "$bytes" ]; then
    args+=(--config retention.bytes="$bytes")
  fi
  if ! out=$(kt "${args[@]}" 2>&1); then
    echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
  fi
  LIST="$LIST"$'\n'"$topic"
}

rc=0
# `current`: compact,delete. create-if-absent=no on the shared routine, so it can never create one at the default shape.
create_if_absent "$CURRENT" compact,delete -1 || rc=1
ensure_topic "$CURRENT" compact,delete "exact:-1" 1048576 no || rc=1
check_partitions "$CURRENT" || rc=1

# `scoring`: plain delete with an unbounded retention and a byte cap. NOT compacted — asserted, because a compacted evidence log would still accept every write
# and still look healthy while deleting the measurement.
create_if_absent "$SCORING" delete -1 "$SCORING_BYTES" || rc=1
ensure_topic "$SCORING" delete "exact:-1" 1048576 no || rc=1
check_partitions "$SCORING" || rc=1
check_retention_bytes "$SCORING" "$SCORING_BYTES" || rc=1

if [ "$rc" -ne 0 ]; then
  echo "=== ensure-strike-hold-topics FAILED - strike-hold must not publish ==="
  exit 1
fi
echo "=== ensure-strike-hold-topics OK ==="
