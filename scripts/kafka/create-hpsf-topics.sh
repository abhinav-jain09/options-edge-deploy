#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"

if [[ "$REPLICATION_FACTOR" != "1" ]]; then
  echo "HPSF RF=1 cluster rule violated: KAFKA_TOPIC_REPLICATION_FACTOR=$REPLICATION_FACTOR" >&2
  exit 1
fi
if [[ "$MIN_ISR" != "1" ]]; then
  echo "HPSF RF=1 cluster rule violated: KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=$MIN_ISR" >&2
  exit 1
fi
if [[ "$DRY_RUN" != "true" && -z "$BOOTSTRAP_SERVERS" ]]; then
  echo "KAFKA_BOOTSTRAP_SERVERS is required unless --dry-run is used" >&2
  exit 1
fi

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-DRY_RUN_BOOTSTRAP}"
DELETE_RETENTION_MS=3600000
MIN_CLEANABLE_DIRTY_RATIO=0.10
COMPACTED_SEGMENT_MS=1800000
COMPACTED_SEGMENT_BYTES=134217728
MAX_COMPACTION_LAG_MS=3600000

# topic|partitions|cleanup.policy|retention.ms|segment.ms|segment.bytes|extra-config-csv
HPSF_TOPIC_SPECS=(
  'options.opra.tcbbo|6|delete|86400000|900000|536870912|'
  'options.opra.trades|6|delete|86400000|900000|536870912|'
  'options.opra.quotes|6|delete|86400000|900000|536870912|'
  'underlying.es.trades|2|delete|86400000|1800000|268435456|'
  'underlying.spx.price|1|compact,delete|86400000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10'
  'options.hpsf.underlying-state|1|compact,delete|604800000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10'
  'options.hpsf.market-flow|1|compact,delete|172800000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10'
  'options.hpsf.strike-flow|6|compact,delete|172800000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10,max.compaction.lag.ms=3600000'
  'options.hpsf.strike-score|6|compact,delete|172800000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10,max.compaction.lag.ms=3600000'
  'options.hpsf.latest-signal|1|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10'
  'options.hpsf.signal|1|delete|2592000000|1800000|134217728|'
  'options.hpsf.audit|2|delete|604800000|1800000|134217728|'
  'options.hpsf.dlq|1|delete|2592000000|1800000|134217728|'
  'options.hpsf.writer-dlq|1|delete|2592000000|1800000|134217728|'
  'options.hpsf.exit-signal|1|delete|2592000000|1800000|134217728|'
  'options.databento.strike-flow|32|compact,delete|172800000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10,max.compaction.lag.ms=3600000'
)

rf1_warning() {
  cat <<'MSG'
WARNING: Creating HPSF topics with replication.factor=1 and min.insync.replicas=1 for Abhinav's current 2-broker Kafka limit.
This has no broker-failure durability. Do not claim high availability until the cluster supports RF>=3.
MSG
}

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

cleanup_for_config_add() {
  local cleanup_policy="$1"
  if [[ "$cleanup_policy" == *,* ]]; then
    echo "[$cleanup_policy]"
  else
    echo "$cleanup_policy"
  fi
}

base_configs() {
  local cleanup_policy="$1"
  local retention_ms="$2"
  local segment_ms="$3"
  local segment_bytes="$4"
  local extra="$5"
  local configs="retention.ms=$retention_ms,cleanup.policy=$(cleanup_for_config_add "$cleanup_policy"),min.insync.replicas=$MIN_ISR,compression.type=lz4,segment.ms=$segment_ms,segment.bytes=$segment_bytes"
  if [[ -n "$extra" ]]; then
    configs="$configs,$extra"
  fi
  echo "$configs"
}

describe_topic_header() {
  local topic="$1"
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" 2>/dev/null | sed -n '/^Topic:/p' || true
}

wait_for_topic_shape() {
  local topic="$1"
  local expected_partitions="$2"
  local attempts="${KAFKA_TOPIC_REPAIR_WAIT_SECONDS:-60}"
  for ((i = 1; i <= attempts; i++)); do
    local description partitions replication_factor
    description="$(describe_topic_header "$topic")"
    partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
    if [[ "$partitions" == "$expected_partitions" && "$replication_factor" == "$REPLICATION_FACTOR" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for topic $topic to become partitions=$expected_partitions replicationFactor=$REPLICATION_FACTOR" >&2
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" || true
  return 1
}

create_topic_if_needed() {
  local topic="$1"
  local partitions="$2"
  local cleanup_policy="$3"
  local retention_ms="$4"
  local segment_ms="$5"
  local segment_bytes="$6"
  local extra="$7"
  local description current_partitions current_replication_factor configs

  configs="$(base_configs "$cleanup_policy" "$retention_ms" "$segment_ms" "$segment_bytes" "$extra")"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN topic=$topic partitions=$partitions replication.factor=$REPLICATION_FACTOR cleanup.policy=$cleanup_policy retention.ms=$retention_ms min.insync.replicas=$MIN_ISR compression.type=lz4"
    run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" --config "cleanup.policy=$cleanup_policy" --config "retention.ms=$retention_ms" --config "min.insync.replicas=$MIN_ISR" --config 'compression.type=lz4' --config "segment.ms=$segment_ms" --config "segment.bytes=$segment_bytes"
    run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config "$configs"
    run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic"
    run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe
    return 0
  fi

  description="$(describe_topic_header "$topic")"
  if [[ -n "$description" ]]; then
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
    if [[ "$current_partitions" != "$partitions" || "$current_replication_factor" != "$REPLICATION_FACTOR" ]]; then
      echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected partitions=$partitions replicationFactor=$REPLICATION_FACTOR" >&2
      echo "Refusing destructive topic repair in HPSF topic script. Fix manually if this is intentional." >&2
      exit 1
    fi
    echo "Topic $topic already exists with expected partitions=$partitions replicationFactor=$REPLICATION_FACTOR"
  else
    run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor "$REPLICATION_FACTOR" \
      --config "cleanup.policy=$cleanup_policy" \
      --config "retention.ms=$retention_ms" \
      --config "min.insync.replicas=$MIN_ISR" \
      --config 'compression.type=lz4' \
      --config "segment.ms=$segment_ms" \
      --config "segment.bytes=$segment_bytes"
    wait_for_topic_shape "$topic" "$partitions"
  fi

  run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config "$configs"
  run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic"
  run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe
}

rf1_warning
for spec in "${HPSF_TOPIC_SPECS[@]}"; do
  IFS='|' read -r topic partitions cleanup_policy retention_ms segment_ms segment_bytes extra <<<"$spec"
  create_topic_if_needed "$topic" "$partitions" "$cleanup_policy" "$retention_ms" "$segment_ms" "$segment_bytes" "$extra"
done
