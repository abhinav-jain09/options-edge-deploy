#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
EXPECTED_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
EXPECTED_MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"
# RF/minISR are derived per-environment from the configmap (load-kafka-settings.sh):
# dev RF=1, prod RF=2. Accept any positive integers (minISR <= RF).
if ! [[ "$EXPECTED_REPLICATION_FACTOR" =~ ^[1-9][0-9]*$ ]]; then
  echo "Invalid KAFKA_TOPIC_REPLICATION_FACTOR=$EXPECTED_REPLICATION_FACTOR (must be a positive integer)" >&2
  exit 1
fi
if ! [[ "$EXPECTED_MIN_ISR" =~ ^[1-9][0-9]*$ ]] || (( EXPECTED_MIN_ISR > EXPECTED_REPLICATION_FACTOR )); then
  echo "Invalid KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=$EXPECTED_MIN_ISR (must be a positive integer <= replication factor $EXPECTED_REPLICATION_FACTOR)" >&2
  exit 1
fi

TOPICS=(
  'options.opra.tcbbo|32|delete|86400000'
  'options.opra.trades|32|delete|86400000'
  'options.opra.quotes|32|delete|86400000'
  'underlying.es.trades|32|delete|86400000'
  'underlying.spx.price|1|compact,delete|86400000'
  'options.hpsf.underlying-state|1|compact,delete|604800000'
  'options.hpsf.market-flow|32|compact,delete|172800000'
  'options.hpsf.strike-flow|32|compact,delete|172800000'
  'options.hpsf.strike-score|32|compact,delete|172800000'
  'options.hpsf.latest-signal|32|compact,delete|2592000000'
  'options.hpsf.signal|32|delete|2592000000'
  'options.hpsf.audit|32|delete|604800000'
  'options.hpsf.dlq|32|delete|2592000000'
  'options.hpsf.writer-dlq|32|delete|2592000000'
  'options.hpsf.exit-signal|32|delete|2592000000'
  'options.databento.strike-flow|32|compact,delete|172800000'
  'options.ibkr.strike-flow|32|compact,delete|172800000'
)

topic_name() {
  printf '%s%s' "$TOPIC_PREFIX" "$1"
}

# Mirror create-hpsf-topics.sh: when the env-derived cap KAFKA_MAX_RETENTION_MS is
# set (dev = 10h; unset in prod), the topics were CREATED with retention capped at
# that value, so the verifier must expect the capped retention too — otherwise dev
# verification would fail against its own capped topics. Unset => no cap (prod).
cap_retention() {
  local v="$1"
  if [[ -n "${KAFKA_MAX_RETENTION_MS:-}" && "$v" =~ ^[0-9]+$ && "$KAFKA_MAX_RETENTION_MS" =~ ^[0-9]+$ && "$v" -gt "$KAFKA_MAX_RETENTION_MS" ]]; then
    printf '%s' "$KAFKA_MAX_RETENTION_MS"
  else
    printf '%s' "$v"
  fi
}

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r topic expected_partitions expected_cleanup expected_retention <<<"$spec"
  expected_retention="$(cap_retention "$expected_retention")"
  topic="$(topic_name "$topic")"
  echo "Verifying $topic"
  description="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic")"
  echo "$description"
  partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
  if (( partitions < expected_partitions )); then
    echo "Topic $topic partitions=$partitions expectedAtLeast=$expected_partitions" >&2
    exit 1
  fi
  if (( partitions > expected_partitions )); then
    echo "Topic $topic partitions=$partitions expectedMinimum=$expected_partitions; accepting existing larger partition count."
  fi
  if (( replication_factor < EXPECTED_REPLICATION_FACTOR )); then
    echo "Topic $topic replicationFactor=$replication_factor expected at least=$EXPECTED_REPLICATION_FACTOR" >&2
    exit 1
  fi
  configs="$(kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe)"
  echo "$configs"
  for required in "retention.ms=$expected_retention" "min.insync.replicas=$EXPECTED_MIN_ISR" 'compression.type=lz4'; do
    if ! echo "$configs" | grep -Fq "$required"; then
      echo "Topic $topic missing required config $required" >&2
      exit 1
    fi
  done
  if ! echo "$configs" | grep -F "cleanup.policy" | grep -Fq "$expected_cleanup"; then
    echo "Topic $topic cleanup.policy does not include expected value $expected_cleanup" >&2
    exit 1
  fi
done
