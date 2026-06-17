#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
EXPECTED_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"
for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  expected_partitions="${entry##*:}"
  echo "Verifying $topic"
  description="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic")"
  echo "$description"
  partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
  if (( partitions < expected_partitions )); then
    echo "Topic $topic has partition count $partitions, expected at least $expected_partitions" >&2
    exit 1
  fi
  if (( partitions > expected_partitions )); then
    echo "Topic $topic has partition count $partitions, expected minimum $expected_partitions; accepting existing larger partition count."
  fi
  if [[ "$replication_factor" != "$EXPECTED_REPLICATION_FACTOR" ]]; then
    echo "Topic $topic has replication factor $replication_factor, expected $EXPECTED_REPLICATION_FACTOR" >&2
    exit 1
  fi
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe
done
