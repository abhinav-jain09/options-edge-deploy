#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
EXPECTED_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
EXPECTED_MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
if [[ "$EXPECTED_REPLICATION_FACTOR" != "1" || "$EXPECTED_MIN_ISR" != "1" ]]; then
  echo "HPSF verification only supports RF=1/min.insync.replicas=1 for the current cluster" >&2
  exit 1
fi

TOPICS=(
  'options.opra.tcbbo|6|delete|86400000'
  'options.opra.trades|6|delete|86400000'
  'options.opra.quotes|6|delete|86400000'
  'underlying.es.trades|2|delete|86400000'
  'underlying.spx.price|1|compact,delete|86400000'
  'options.hpsf.underlying-state|1|compact,delete|604800000'
  'options.hpsf.market-flow|1|compact,delete|172800000'
  'options.hpsf.strike-flow|6|compact,delete|172800000'
  'options.hpsf.strike-score|6|compact,delete|172800000'
  'options.hpsf.latest-signal|1|compact,delete|2592000000'
  'options.hpsf.signal|1|delete|2592000000'
  'options.hpsf.audit|2|delete|604800000'
  'options.hpsf.dlq|1|delete|2592000000'
  'options.hpsf.writer-dlq|1|delete|2592000000'
  'options.hpsf.exit-signal|1|delete|2592000000'
  'options.databento.strike-flow|32|compact,delete|172800000'
)

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r topic expected_partitions expected_cleanup expected_retention <<<"$spec"
  echo "Verifying $topic"
  description="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic")"
  echo "$description"
  partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
  if [[ "$partitions" != "$expected_partitions" ]]; then
    echo "Topic $topic partitions=$partitions expected=$expected_partitions" >&2
    exit 1
  fi
  if [[ "$replication_factor" != "$EXPECTED_REPLICATION_FACTOR" ]]; then
    echo "Topic $topic replicationFactor=$replication_factor expected=$EXPECTED_REPLICATION_FACTOR" >&2
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
