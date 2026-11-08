#!/usr/bin/env bash
# ensure-break-line-topics.sh - break-line-service's five topics must be right BEFORE the service or the web tier publishes (the first-deploy barrier, as
# ensure-liquidity-validator-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
# Break Score rev 13: options-edge-documents design/es-liquidity/liq-S-break-score-rev13-design.md section 15.2.
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   futures.break-line.lines     1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)   the user's line commands: USER DATA
#   futures.break-line.state     1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   futures.break-line.events    1 partition, delete,  retention.ms EXACTLY 2592000000 (30 days)
#   futures.break-line.status    1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)
#   futures.break-line.snapshot  1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1, max.message.bytes >= 9437184 (the restart snapshot)
# An existing topic is never recreated, altered or shrunk: any difference fails closed. The INPUT topics belong to other owners and are not created here.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-break-line-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); break-line has no production topics - refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${BREAK_LINE_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${BREAK_LINE_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-break-line-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

LINES="${KAFKA_BREAK_LINE_LINES_TOPIC:-futures.break-line.lines}"
STATE="${KAFKA_BREAK_LINE_STATE_TOPIC:-futures.break-line.state}"
EVENTS="${KAFKA_BREAK_LINE_EVENTS_TOPIC:-futures.break-line.events}"
STATUS="${KAFKA_BREAK_LINE_STATUS_TOPIC:-futures.break-line.status}"
SNAPSHOT="${KAFKA_BREAK_LINE_SNAPSHOT_TOPIC:-futures.break-line.snapshot}"
SNAPSHOT_MAX=9437184
THIRTY_DAYS=2592000000

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what a service's own describeConfigs sees) and compare exactly. Never altered here.
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

# A pure-compact topic: created here WITH retention.bytes -1 (and the message bound it needs), then verified by the shared routine with create-if-absent=no (so it can never create one at the default shape)
# and its retention.bytes compared exactly.
ensure_pure_compact() { # <topic> <create max.message.bytes> <min max.message.bytes>
  local topic="$1" create_bytes="$2" min_bytes="$3" out
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=compact, retention.ms=-1, retention.bytes=-1, max.message.bytes=$create_bytes)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy=compact --config retention.ms=-1 --config retention.bytes=-1 \
                  --config max.message.bytes="$create_bytes" --config min.insync.replicas="$MIN_ISR" 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
    LIST="$LIST"$'\n'"$topic"
  fi
  ensure_topic "$topic" compact "exact:-1" "$min_bytes" no || return 1
  check_retention_bytes "$topic" -1
}

rc=0
ensure_pure_compact "$LINES" 1048576 1048576 || rc=1
ensure_pure_compact "$STATE" 1048576 1048576 || rc=1
ensure_topic "$EVENTS" delete "exact:$THIRTY_DAYS" 1048576 yes || rc=1
ensure_pure_compact "$STATUS" 1048576 1048576 || rc=1
ensure_pure_compact "$SNAPSHOT" "$SNAPSHOT_MAX" "$SNAPSHOT_MAX" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-break-line-topics FAILED - the break line service and the break score commands must not publish ==="
  exit 1
fi
echo "=== ensure-break-line-topics OK ==="
