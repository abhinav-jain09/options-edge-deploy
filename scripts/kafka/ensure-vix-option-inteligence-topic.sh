#!/usr/bin/env bash
# Reconcile the live vix-option-inteligence-service current-state topic without touching any other Kafka topic.
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?load scripts/kafka/load-kafka-settings.sh first}"

# RECONCILE_PHASE splits the two halves so the caller can run them at the right MOMENT, not just
# in the right order. The topic half is safe while the service is running; the prune half is not
# (an identity held by a live consumer can never be retired), so it must run while the service is
# paused. Keeping both halves in one invocation forced the pause to cover the topic work too,
# lengthening the outage and adding failure points that can strand the service at zero replicas.
# Unset = both, so any other caller keeps its current behaviour.
PHASE="${RECONCILE_PHASE:-all}"
case "$PHASE" in
  all|topic|prune) ;;
  *) echo "FATAL: RECONCILE_PHASE must be all, topic or prune (got '$PHASE')" >&2; exit 1 ;;
esac
TOPIC="${TOPIC_PREFIX:-}options.spx.vix-option-inteligence-service.current"
PARTITIONS=32
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:--1}"

if [ "$PHASE" != "prune" ]; then
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
fi


# ---- zero-orphan prune of the retired identity (One Service One Identity Rule) ----
# Single implementation lives in prune-retired-zero-dte-identity.lib.sh (shared with the
# es4 mirror in scripts/es4/create-es-topics.sh, which supplies docker-exec wrappers).
if [ "$PHASE" = "topic" ]; then
  exit 0
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/prune-retired-zero-dte-identity.lib.sh"
prune_kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
prune_kg() { kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
prune_retired_zero_dte_identity "${TOPIC_PREFIX:-}options.spx.0dte.intelligence.current"
