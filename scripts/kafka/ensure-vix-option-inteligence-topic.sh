#!/usr/bin/env bash
# Reconcile the live vix-option-inteligence-service current-state topic without touching any other Kafka topic.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?load scripts/kafka/load-kafka-settings.sh first}"
TOPIC="${TOPIC_PREFIX:-}options.spx.vix-option-inteligence-service.current"
PARTITIONS=32
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:--1}"

if ! kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$TOPIC" >/dev/null 2>&1; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --create --topic "$TOPIC" \
    --partitions "$PARTITIONS" --replication-factor "$RF"
fi

current="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$TOPIC" \
  | awk -F'PartitionCount: ' 'NR==1 {split($2,a," "); print a[1]}')"
case "$current" in
  ''|*[!0-9]*) echo "FATAL: could not read partition count for $TOPIC" >&2; exit 1 ;;
esac
if [ "$current" -lt "$PARTITIONS" ]; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --alter --topic "$TOPIC" --partitions "$PARTITIONS"
elif [ "$current" -gt "$PARTITIONS" ]; then
  echo "FATAL: $TOPIC has $current partitions; policy requires $PARTITIONS and Kafka cannot shrink in place" >&2
  exit 1
fi

kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --alter \
  --entity-type topics --entity-name "$TOPIC" \
  --add-config "cleanup.policy=compact,retention.ms=${RETENTION_MS}"

verified="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$TOPIC" \
  | awk -F'PartitionCount: ' 'NR==1 {split($2,a," "); print a[1]}')"
config="$(kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe \
  --entity-type topics --entity-name "$TOPIC")"
[ "$verified" = "$PARTITIONS" ] || { echo "FATAL: $TOPIC partitions=$verified expected=$PARTITIONS" >&2; exit 1; }
printf '%s\n' "$config" | grep -q 'cleanup.policy=compact' \
  || { echo "FATAL: $TOPIC is not compacted" >&2; exit 1; }
echo "$TOPIC reconciled partitions=$verified cleanup=compact retention.ms=$RETENTION_MS"
