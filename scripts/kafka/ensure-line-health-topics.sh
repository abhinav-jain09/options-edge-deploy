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
ensure_lh() { # <topic> <cleanup.policy> <retention.ms> <max.message.bytes>
  local topic="$1" policy="$2" ms="$3" maxb="$4" out
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$policy, retention.ms=$ms, retention.bytes=-1, max.message.bytes=$maxb)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$policy" --config retention.ms="$ms" --config retention.bytes=-1 \
                  --config max.message.bytes="$maxb" --config min.insync.replicas="$MIN_ISR" 2>&1); then
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
ensure_lh "$USER_LINES" compact,delete "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$STATUS"     compact        -1               "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$CLOSURES"   delete         "$SEVEN_DAYS_MS" "$DEFAULT_MAX_BYTES"  || rc=1
ensure_lh "$SNAPSHOT"   compact        -1               "$SNAPSHOT_MAX_BYTES" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-line-health-topics FAILED — the line-health loop must not be raised ==="
  exit 1
fi
echo "=== ensure-line-health-topics OK ==="
