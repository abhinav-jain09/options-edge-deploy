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

# ---- zero-orphan prune of the retired identity (One Service One Identity Rule) ----
# The service formerly published ${TOPIC_PREFIX:-}options.spx.0dte.intelligence.current under
# consumer group zero-dte-intelligence-service-v1[-profile]. Both were retired by the
# service-aligned rename. Prune them wherever they still exist — exact topic name and exact
# group prefix only; nothing else is ever a candidate. Idempotent: absent means done.
LEGACY_TOPIC="${TOPIC_PREFIX:-}options.spx.0dte.intelligence.current"
if kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$LEGACY_TOPIC" >/dev/null 2>&1; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$LEGACY_TOPIC"
  echo "$LEGACY_TOPIC deleted (retired identity)"
else
  echo "$LEGACY_TOPIC absent (nothing to prune)"
fi

LEGACY_GROUP_PREFIX="zero-dte-intelligence-service-v1"
LEGACY_GROUPS="$(kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null \
  | grep -E "^${LEGACY_GROUP_PREFIX}(-|\$)" || true)"
if [ -z "$LEGACY_GROUPS" ]; then
  echo "no ${LEGACY_GROUP_PREFIX}* consumer groups remain (nothing to prune)"
fi
for g in $LEGACY_GROUPS; do
  # --delete refuses a group with live members; that would mean the retired identity is
  # still running somewhere, which must fail the deploy loudly, never be skipped.
  if kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --group "$g"; then
    echo "consumer group $g deleted (retired identity)"
  else
    echo "FATAL: failed to delete retired consumer group $g (still active?)" >&2
    exit 1
  fi
done
