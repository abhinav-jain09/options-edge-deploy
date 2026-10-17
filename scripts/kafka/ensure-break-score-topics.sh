#!/usr/bin/env bash
# ensure-break-score-topics.sh — break-score-service's events topic must EXIST with its declared shape BEFORE the service
# rolls out (the same barrier as ensure-early-auction-verdict-topics).
#
# ONE topic (options-edge-processing EventTopicContract, docs/break-score/as-built/runtime-gaps.md section 3):
#   futures.break-score.events   1 partition (one partition is the event stream's total order), cleanup.policy=delete,
#                                retention.ms=-1, retention.bytes = a FINITE positive cap (decided 2026-10-16: 16777216),
#                                message.timestamp.type=CreateTime (the producer stamps each record with the event's exchange
#                                time, and the restart seeks by that timestamp)
#
# The SERVICE only verifies this contract at start (it never creates or alters the topic) and refuses to start on a wrong partition
# count, a cleanup policy without delete, retention.ms other than -1, or an absent / non-positive / unlimited retention.bytes.
# This barrier asserts the same, with two differences: cleanup.policy must be EXACTLY delete (a compacted or compact,delete topic
# would drop the earlier states of a break), and message.timestamp.type must be CreateTime.
#
# A topic that is ABSENT is created with retention.bytes = $BREAK_SCORE_EVENTS_RETENTION_BYTES (default 16777216, the decided
# value; the same variable BreakScoreTopicApplyMain reads). A topic that EXISTS is never altered: any finite positive
# retention.bytes passes, because the cap is an owner-adjustable number (raising it is a deliberate topic change), but an absent or
# unlimited one fails. CREATE if absent, then VERIFY — including a topic just created — and fail closed. ensure() runs in an `if`
# context where bash ignores `set -e`; nothing here relies on errexit.
set -euo pipefail

EVENTS_TOPIC="${KAFKA_BREAK_SCORE_EVENTS_TOPIC:-futures.break-score.events}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${BREAK_SCORE_MIN_REPLICATION_FACTOR:-1}"
RETENTION_BYTES="${BREAK_SCORE_EVENTS_RETENTION_BYTES:-16777216}"
case "$RETENTION_BYTES" in
  ''|*[!0-9]*|0) echo "FAIL: BREAK_SCORE_EVENTS_RETENTION_BYTES='$RETENTION_BYTES' is not a positive whole number."; exit 1 ;;
esac

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-break-score-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic>
  local topic="$1" out desc parts rf cfg policy retention bytes tstype
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=-1, retention.bytes=$RETENTION_BYTES, message.timestamp.type=CreateTime)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes="$RETENTION_BYTES" \
                  --config message.timestamp.type=CreateTime 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (one partition is what gives the event stream its total order)."
    return 1
  fi
  rf=$(awk '{for (i = 1; i < NF; i++) if ($i == "ReplicationFactor:") { print $(i + 1); exit }}' <<<"$desc")
  case "${rf:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports ReplicationFactor='${rf:-unknown}', which is not a number."; return 1 ;;
  esac
  if [ "$rf" -lt "$MIN_RF" ]; then
    echo "FAIL: '$topic' has ReplicationFactor=$rf, expected >= $MIN_RF."
    return 1
  fi
  # --all: the EFFECTIVE values, which are what the service's own AdminClient describeConfigs sees.
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  bytes=$(extract 'retention\.bytes' "$cfg")
  tstype=$(extract 'message\.timestamp\.type' "$cfg")
  if [ "${policy:-}" != "delete" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly 'delete' (compaction would drop the earlier states of a break)."
    return 1
  fi
  if [ "${retention:-}" != "-1" ]; then
    echo "FAIL: '$topic' has retention.ms='${retention:-<unreadable>}', expected exactly -1 (keep by size, not by age)."
    return 1
  fi
  case "${bytes:-}" in
    ''|*[!0-9]*|0)
      echo "FAIL: '$topic' has retention.bytes='${bytes:-<unreadable>}', expected a finite positive cap (the service refuses to start on an absent or unlimited one)."
      return 1 ;;
  esac
  if [ "${tstype:-}" != "CreateTime" ]; then
    echo "FAIL: '$topic' has message.timestamp.type='${tstype:-<unreadable>}', expected CreateTime (the producer stamps the event's exchange time)."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 rf=$rf cleanup.policy=delete retention.ms=-1 retention.bytes=$bytes message.timestamp.type=CreateTime"
}

rc=0
ensure "$EVENTS_TOPIC" || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-break-score-topics FAILED — the rollout must not proceed ==="
  exit 1
fi
echo "=== ensure-break-score-topics OK ==="
