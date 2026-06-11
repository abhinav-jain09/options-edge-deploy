#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
RECREATE_MISMATCHED="${KAFKA_RECREATE_MISMATCHED_TOPICS:-false}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"

describe_topic() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null | sed -n '/^Topic:/p' || true
}

wait_for_topic_absent() {
  local topic="$1"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    if [[ -z "$(describe_topic "$topic")" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  describe_topic "$topic"
  return 1
}

create_topic() {
  local topic="$1"
  local partitions="$2"

  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$RETENTION_MS" \
    --config "cleanup.policy=$CLEANUP_POLICY" \
    --config "min.insync.replicas=$MIN_ISR"
}

for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  partitions="${entry##*:}"
  description="$(describe_topic "$topic")"
  if [[ -n "$description" ]]; then
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"

    if [[ "$current_partitions" != "$partitions" || "$current_replication_factor" != "$REPLICATION_FACTOR" ]]; then
      if [[ "$RECREATE_MISMATCHED" != "true" ]]; then
        echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected partitions=$partitions replicationFactor=$REPLICATION_FACTOR" >&2
        echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
        exit 1
      fi

      echo "Recreating mismatched topic $topic: partitions=$current_partitions replicationFactor=$current_replication_factor -> partitions=$partitions replicationFactor=$REPLICATION_FACTOR"
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
      wait_for_topic_absent "$topic"
      create_topic "$topic" "$partitions"
    else
      echo "Topic $topic already exists with expected partitions=$partitions replicationFactor=$REPLICATION_FACTOR"
    fi
  else
    create_topic "$topic" "$partitions"
  fi

  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics --entity-name "$topic" --alter \
    --add-config "retention.ms=$RETENTION_MS,cleanup.policy=$CLEANUP_POLICY,min.insync.replicas=$MIN_ISR"
done
