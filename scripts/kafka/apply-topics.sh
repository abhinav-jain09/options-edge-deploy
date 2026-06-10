#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"
for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  partitions="${entry##*:}"
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$RETENTION_MS" \
    --config "cleanup.policy=$CLEANUP_POLICY" \
    --config "min.insync.replicas=$MIN_ISR"
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics --entity-name "$topic" --alter \
    --add-config "retention.ms=$RETENTION_MS,cleanup.policy=$CLEANUP_POLICY,min.insync.replicas=$MIN_ISR"
done
