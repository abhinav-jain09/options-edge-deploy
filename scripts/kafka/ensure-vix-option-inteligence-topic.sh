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
# group prefix only. The zero-dte-intelligence-service-v1 hyphen namespace is contractually
# retired: version-suffixed identities are banned by the One Service One Identity Rule, so
# nothing legitimate can ever be created under this prefix again.
# FAIL-CLOSED: a broker/listing failure aborts the deploy; it must never read as "no orphans".
LEGACY_TOPIC="${TOPIC_PREFIX:-}options.spx.0dte.intelligence.current"
DELETE_WAIT="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

list_topics() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list \
    || { echo "FATAL: could not list topics — refusing to prune (fail-closed)" >&2; exit 1; }
}
list_groups() {
  kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list \
    || { echo "FATAL: could not list consumer groups — refusing to prune (fail-closed)" >&2; exit 1; }
}

# Capture-first, never pipe the listing into a test: a failed listing inside a negated
# pipeline would read as "absent" — the exact fail-open this section forbids. A failed
# capture aborts via set -e after the function prints its FATAL. Matching uses grep
# WITHOUT -q into a captured variable: grep -q exits at the first match, which can
# SIGPIPE the producer under pipefail and falsely report absence on large listings.
TOPICS_NOW="$(list_topics)"
present="$(printf '%s\n' "$TOPICS_NOW" | grep -Fx "$LEGACY_TOPIC" || true)"
if [ -n "$present" ]; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$LEGACY_TOPIC" \
    || { echo "FATAL: failed to delete retired topic $LEGACY_TOPIC" >&2; exit 1; }
  # Topic deletion is asynchronous — verify terminal absence within a bounded wait.
  deleted=false
  for _ in $(seq 1 "$DELETE_WAIT"); do
    TOPICS_NOW="$(list_topics)"
    still="$(printf '%s\n' "$TOPICS_NOW" | grep -Fx "$LEGACY_TOPIC" || true)"
    if [ -z "$still" ]; then deleted=true; break; fi
    sleep 1
  done
  [ "$deleted" = "true" ] || { echo "FATAL: $LEGACY_TOPIC still present ${DELETE_WAIT}s after delete" >&2; exit 1; }
  echo "$LEGACY_TOPIC deleted and verified absent (retired identity)"
else
  echo "$LEGACY_TOPIC absent (nothing to prune)"
fi

LEGACY_GROUP_PREFIX="zero-dte-intelligence-service-v1"
GROUPS_NOW="$(list_groups)"
LEGACY_GROUPS="$(printf '%s\n' "$GROUPS_NOW" | grep -E "^${LEGACY_GROUP_PREFIX}(-|\$)" || true)"
if [ -z "$LEGACY_GROUPS" ]; then
  echo "no ${LEGACY_GROUP_PREFIX}* consumer groups remain (nothing to prune)"
else
  for g in $LEGACY_GROUPS; do
    # --delete refuses a group with live members; that would mean the retired identity is
    # still running somewhere, which must fail the deploy loudly, never be skipped.
    kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --group "$g" \
      || { echo "FATAL: failed to delete retired consumer group $g (still active?)" >&2; exit 1; }
    echo "consumer group $g deleted (retired identity)"
  done
  # Terminal zero-orphan proof: re-list and require that no retired group remains.
  GROUPS_NOW="$(list_groups)"
  REMAINING="$(printf '%s\n' "$GROUPS_NOW" | grep -E "^${LEGACY_GROUP_PREFIX}(-|\$)" || true)"
  [ -z "$REMAINING" ] || { echo "FATAL: retired groups still present after delete: $REMAINING" >&2; exit 1; }
  echo "zero-orphan verified: no ${LEGACY_GROUP_PREFIX}* consumer groups remain"
fi
