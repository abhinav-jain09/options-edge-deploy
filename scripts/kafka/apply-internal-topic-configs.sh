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

# A topic listed by `kafka-topics --list` above can be deleted before we reach its
# --alter here (the nightly changelog/repartition cleanup, or an orphaned internal topic
# of a now-removed service being purged). Under `set -e` that would abort the ENTIRE
# deploy for a topic that no longer needs configuring — so treat "topic vanished"
# (UnknownTopicOrPartition) as a skip, and only fail on genuine errors.
alter_topic_config() {
  local topic="$1" configs="$2" err
  if err="$(kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --entity-type topics --entity-name "$topic" --alter --add-config "$configs" 2>&1)"; then
    return 0
  fi
  if printf '%s' "$err" | grep -q "UnknownTopicOrPartition"; then
    echo "  topic no longer exists (race with cleanup / orphaned internal topic); skipping: $topic"
    return 0
  fi
  printf '%s\n' "$err" >&2
  return 1
}

apply_changelog_config() {
  local topic="$1"
  echo "Applying compact+delete one-day changelog policy: $topic"
  alter_topic_config "$topic" \
    "cleanup.policy=[compact,delete],retention.ms=$RETENTION_MS,segment.ms=$SEGMENT_MS,delete.retention.ms=$DELETE_RETENTION_MS,min.cleanable.dirty.ratio=$MIN_CLEANABLE_DIRTY_RATIO,min.insync.replicas=$MIN_ISR"
}

apply_repartition_config() {
  local topic="$1"
  echo "Applying delete one-day repartition policy: $topic"
  alter_topic_config "$topic" \
    "cleanup.policy=delete,retention.ms=$RETENTION_MS,segment.ms=$SEGMENT_MS,min.insync.replicas=$MIN_ISR"
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
    # Streams names a repartition topic after the operator that created it, so the
    # `-repartition` suffix is only the DEFAULT. Anything built with an explicit
    # Repartitioned.as("chain-rekey") / .as("repart-flow") name lands on `-rekey` or
    # `-repart-<x>` instead and used to fall through this case with NO config at all —
    # on dev that was 11 topics (5 `-rekey`, 6 `-repart-*`), including the
    # dealer-ledger and strike-liquidity-heatmap chain rekeys, which are among the
    # largest partitions on the broker. (2026-07-30)
    *-repartition|*-rekey|*-repart-*)
      found=true
      apply_repartition_config "$topic"
      ;;
  esac
done < <(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | sort)

if [[ "$found" != "true" ]]; then
  echo "No Kafka Streams internal topics found yet."
fi
