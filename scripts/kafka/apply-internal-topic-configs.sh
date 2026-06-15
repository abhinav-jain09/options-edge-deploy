#!/usr/bin/env bash
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"

RETENTION_MS="${KAFKA_CHANGELOG_RETENTION_MS:-${KAFKA_STREAMS_INTERNAL_RETENTION_MS:-${KAFKA_TOPIC_RETENTION_MS:-86400000}}}"
SEGMENT_MS="${KAFKA_STREAMS_INTERNAL_SEGMENT_MS:-3600000}"
DELETE_RETENTION_MS="${KAFKA_CHANGELOG_DELETE_RETENTION_MS:-3600000}"
MIN_CLEANABLE_DIRTY_RATIO="${KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO:-0.01}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
APPLY_SMOKE_INTERNAL_TOPICS="${KAFKA_APPLY_SMOKE_INTERNAL_TOPICS:-false}"
APPLY_REPLAY_INTERNAL_TOPICS="${KAFKA_APPLY_REPLAY_INTERNAL_TOPICS:-false}"

apply_changelog_config() {
  local topic="$1"
  echo "Applying compact+delete one-day changelog policy: $topic"
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "cleanup.policy=[compact,delete],retention.ms=$RETENTION_MS,segment.ms=$SEGMENT_MS,delete.retention.ms=$DELETE_RETENTION_MS,min.cleanable.dirty.ratio=$MIN_CLEANABLE_DIRTY_RATIO,min.insync.replicas=$MIN_ISR"
}

apply_repartition_config() {
  local topic="$1"
  echo "Applying delete one-day repartition policy: $topic"
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "cleanup.policy=delete,retention.ms=$RETENTION_MS,segment.ms=$SEGMENT_MS,min.insync.replicas=$MIN_ISR"
}

found=false
while read -r topic; do
  [[ -n "$topic" ]] || continue
  if [[ "$APPLY_SMOKE_INTERNAL_TOPICS" != "true" && "$topic" == *-smoke-* ]]; then
    echo "Skipping smoke internal topic: $topic"
    continue
  fi
  if [[ "$APPLY_REPLAY_INTERNAL_TOPICS" != "true" && "$topic" == *-replay-* ]]; then
    echo "Skipping replay internal topic: $topic"
    continue
  fi
  case "$topic" in
    *-changelog)
      found=true
      apply_changelog_config "$topic"
      ;;
    *-repartition)
      found=true
      apply_repartition_config "$topic"
      ;;
  esac
done < <(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | sort)

if [[ "$found" != "true" ]]; then
  echo "No Kafka Streams internal topics found yet."
fi
