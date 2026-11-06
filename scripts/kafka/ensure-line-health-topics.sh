#!/usr/bin/env bash
# ensure-line-health-topics.sh — the seven futures.line-health.* topics of the Line health feature (requirement LH-O-1, design 3.3) must EXIST with their declared shape BEFORE the
# line-health loop of level-absorption-service is ever raised. DEV ONLY (requirement LH-E-5: production keeps the level absorption service at zero replicas and is not touched);
# it refuses to run unless ENVIRONMENT is exactly "dev". It is called by scripts/kafka/ensure-level-absorption-topics.sh (the existing level-absorption barrier, run by the
# "Verify level-absorption output topics" stage of Jenkinsfile.service-deploy for SERVICE=level-absorption on dev), and can be run alone.
#
#   NO BROKER-WIDE CHANGE. This barrier never reads or alters a broker config. The shapes are the ones scripts/kafka/topics.env declares (ONE partition each, exact: the streams are
#   totally ordered, requirement LH-O-1):
#   futures.line-health.state       1 partition, compact,delete, retention.ms EXACTLY 604800000 (7 days), retention.bytes EXACTLY -1
#   futures.line-health.events      1 partition, delete,         retention.ms EXACTLY 604800000,         retention.bytes EXACTLY -1
#   futures.line-health.voice       1 partition, delete,         retention.ms EXACTLY 604800000,         retention.bytes EXACTLY -1
#   futures.line-health.user-lines  1 partition, compact,delete, retention.ms EXACTLY 604800000,         retention.bytes EXACTLY -1
#   futures.line-health.status      1 partition, compact,        retention.ms EXACTLY -1,                retention.bytes EXACTLY -1 (pure compact)
#   futures.line-health.closures    1 partition, delete,         retention.ms EXACTLY 604800000,         retention.bytes EXACTLY -1
#   futures.line-health.snapshot    1 partition, compact,        retention.ms EXACTLY -1,                retention.bytes EXACTLY -1 (pure compact), max.message.bytes >= 9437184
# The snapshot record carries the whole canonical state of the loop (design 3.2, "no compression and no framing ... the topic's max.message.bytes is raised"), so it is created at 9 MiB
# (9437184, the value options.spx.line-acceptance.checkpoint uses; a Claude decision, the owner may overrule). No byte cap is declared on any topic (none was asked for): every
# retention.bytes is -1 and is verified exactly, so a cap added by hand is a difference. An existing topic is never recreated, altered or shrunk: any difference fails closed.
# The input topics belong to other services and are not created here. No tombstones, no versioned names.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-line-health-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); line health has no production topics — refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${LINE_HEALTH_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${LINE_HEALTH_MIN_INSYNC_REPLICAS:-1}"
SEVEN_DAYS_MS=604800000
SNAPSHOT_MAX_BYTES=9437184
DEFAULT_MAX_BYTES=1048576
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-line-health-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

STATE="${KAFKA_LINE_HEALTH_STATE_TOPIC:-futures.line-health.state}"
EVENTS="${KAFKA_LINE_HEALTH_EVENTS_TOPIC:-futures.line-health.events}"
VOICE="${KAFKA_LINE_HEALTH_VOICE_TOPIC:-futures.line-health.voice}"
USER_LINES="${KAFKA_LINE_HEALTH_USER_LINES_TOPIC:-futures.line-health.user-lines}"
STATUS="${KAFKA_LINE_HEALTH_STATUS_TOPIC:-futures.line-health.status}"
CLOSURES="${KAFKA_LINE_HEALTH_CLOSURES_TOPIC:-futures.line-health.closures}"
SNAPSHOT="${KAFKA_LINE_HEALTH_SNAPSHOT_TOPIC:-futures.line-health.snapshot}"

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

# Created WITH an explicit retention.bytes=-1 (the shared routine creates none), then verified by the shared routine with create-if-absent=no (so it can never create one at the default
# shape) and its retention.bytes compared exactly.
ensure_lh() { # <topic> <cleanup.policy> <retention.ms> <max.message.bytes> [extra --config value, e.g. min.compaction.lag.ms=604800000]
  local topic="$1" policy="$2" ms="$3" maxb="$4" extra="${5:-}" out extra_args=()
  [ -n "$extra" ] && extra_args=(--config "$extra")
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$policy, retention.ms=$ms, retention.bytes=-1, max.message.bytes=$maxb)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$policy" --config retention.ms="$ms" --config retention.bytes=-1 \
                  --config max.message.bytes="$maxb" --config min.insync.replicas="$MIN_ISR" ${extra_args[@]+"${extra_args[@]}"} 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
    LIST="$LIST"$'\n'"$topic"
  fi
  ensure_topic "$topic" "$policy" "exact:$ms" "$maxb" no || return 1
  check_retention_bytes "$topic" -1
}

rc=0
ensure_lh "$STATE"      compact,delete "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$EVENTS"     delete         "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$VOICE"      delete         "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$USER_LINES" compact,delete "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES" "min.compaction.lag.ms=$SEVEN_DAYS_MS" || rc=1   # design 3.3: nothing inside the recovery horizon is compacted away
ensure_lh "$STATUS"     compact        -1               "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$CLOSURES"   delete         "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$SNAPSHOT"   compact        -1               "$SNAPSHOT_MAX_BYTES" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-line-health-topics FAILED — the line-health loop must not be raised ==="
  exit 1
fi

# --- the compacted INPUT topics the loop replays -------------------------------------------------------------------------------------------------
# Recovery re-reads every input from its recorded position (design 4.5). A compacted topic can only be replayed exactly if compaction cannot remove a record inside the recovery
# horizon, i.e. its min.compaction.lag.ms is at least the horizon (7 days). The loop checks this at start and, when a compacted input does not have it, takes the empty-start path
# (RECOVERY_UNAVAILABLE:INPUT_NOT_REPLAYABLE) instead of recovering: found on dev for underlying.spx.price (compact,delete, lag 0) and the compacted futures.pf.* / futures.volume-nodes
# topics. This step sets min.compaction.lag.ms to 7 days on every compacted topic of the list that EXISTS (a delete-only topic is left alone; a topic with a larger lag is left alone),
# then reads the effective value back and fails when it is missing, unreadable or lower: a read that cannot be parsed is a failure, never a pass. It changes nothing else about those
# topics (they belong to other services) and only ever adds this one setting. Override the list with LINE_HEALTH_REPLAYED_INPUT_TOPICS (space separated).
REPLAYED_INPUTS="${LINE_HEALTH_REPLAYED_INPUT_TOPICS:-underlying.es.trades.linearized underlying.spx.price es.futures.auction futures.auction options.spx.market-structure.ui futures.level-absorption.manual-line options.indicators.studies.current options.spx.market-structure.patterns options.spx.market-structure.tools options.spx.market-structure.geometry options.spx.market-structure.equal-levels futures.pf.columns futures.pf.events futures.pf.chart futures.volume-nodes futures.flow.events options.spx.wave-cycle.waves options.spx.wave-cycle.cycles futures.liquidity-wall.closures futures.liquidity-wall.buckets es.mbo.level-flow es.mbo.level-flow-final $USER_LINES}"
effective() { # <topic> <key> -> the effective value, or nothing when unreadable
  local cfg
  cfg=$(kc --entity-type topics --entity-name "$1" --describe --all 2>&1) || return 1
  extract "$(printf '%s' "$2" | sed 's/\./\\./g')" "$cfg"
}
lag_rc=0
checked=0
for t in $REPLAYED_INPUTS; do
  grep -qxF "$t" <<<"$LIST" || { echo "input '$t' does not exist here: skipped"; continue; }
  policy=$(effective "$t" cleanup.policy) || policy=""
  if [ -z "$policy" ]; then echo "FAIL: could not read cleanup.policy of '$t'"; lag_rc=1; continue; fi
  case "$policy" in
    *compact*) ;;
    *) echo "ok: '$t' is $policy (not compacted): replayable as it is"; continue ;;
  esac
  checked=$((checked + 1))
  have=$(effective "$t" min.compaction.lag.ms) || have=""
  case "$have" in ''|*[!0-9]*) echo "FAIL: '$t' reports min.compaction.lag.ms='${have:-<unreadable>}'"; lag_rc=1; continue ;; esac
  if [ "$have" -lt "$SEVEN_DAYS_MS" ]; then
    echo "setting min.compaction.lag.ms=$SEVEN_DAYS_MS on '$t' (was $have): the loop replays it"
    if ! out=$(kc --entity-type topics --entity-name "$t" --alter --add-config "min.compaction.lag.ms=$SEVEN_DAYS_MS" 2>&1); then
      echo "FAIL: could not alter '$t':"; printf '%s\n' "$out"; lag_rc=1; continue
    fi
    have=$(effective "$t" min.compaction.lag.ms) || have=""
    case "$have" in ''|*[!0-9]*) echo "FAIL: '$t' read back min.compaction.lag.ms='${have:-<unreadable>}' after the alter"; lag_rc=1; continue ;; esac
  fi
  if [ "$have" -lt "$SEVEN_DAYS_MS" ]; then echo "FAIL: '$t' has min.compaction.lag.ms=$have, expected at least $SEVEN_DAYS_MS"; lag_rc=1; continue; fi
  echo "ok: '$t' ($policy) — min.compaction.lag.ms=$have (read back)"
done
echo "compacted replayed inputs checked: $checked"
if [ "$lag_rc" -ne 0 ]; then
  echo "=== ensure-line-health-topics FAILED — a compacted input cannot be replayed ==="
  exit 1
fi
echo "=== ensure-line-health-topics OK ==="
