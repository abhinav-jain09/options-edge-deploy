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
RESET_TOPICS="${HPSF_REPLAY_RESET_TOPICS:-false}"
REPLAY_DATE="${REPLAY_DATE:-2026-06-12}"
COMPACT_DATE="${REPLAY_DATE//-/}"
TOPIC_PREFIX="${TOPIC_PREFIX:-options.replay.$COMPACT_DATE}"
UNDERLYING_TOPIC_PREFIX="${UNDERLYING_TOPIC_PREFIX:-underlying.replay.$COMPACT_DATE}"
TOPIC_METADATA_RETRIES="${HPSF_REPLAY_TOPIC_METADATA_RETRIES:-60}"
TOPIC_METADATA_RETRY_SLEEP_SECONDS="${HPSF_REPLAY_TOPIC_METADATA_RETRY_SLEEP_SECONDS:-2}"

if [[ "$REPLICATION_FACTOR" != "1" ]]; then
  echo "HPSF replay RF=1 cluster rule violated: KAFKA_TOPIC_REPLICATION_FACTOR=$REPLICATION_FACTOR" >&2
  exit 1
fi
if [[ "$MIN_ISR" != "1" ]]; then
  echo "HPSF replay RF=1 cluster rule violated: KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=$MIN_ISR" >&2
  exit 1
fi
if [[ "$DRY_RUN" != "true" && -z "$BOOTSTRAP_SERVERS" ]]; then
  echo "KAFKA_BOOTSTRAP_SERVERS is required unless --dry-run is used" >&2
  exit 1
fi

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-DRY_RUN_BOOTSTRAP}"

# topic|partitions|cleanup.policy|retention.ms|segment.ms|segment.bytes|extra-config-csv
REPLAY_TOPIC_SPECS=(
  "$TOPIC_PREFIX.opra.tcbbo|6|delete|2592000000|900000|536870912|"
  "$UNDERLYING_TOPIC_PREFIX.es.trades|2|delete|2592000000|1800000|268435456|"
  "$UNDERLYING_TOPIC_PREFIX.spx.price|1|delete|2592000000|1800000|134217728|"
  "$TOPIC_PREFIX.hpsf.underlying-state|1|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10"
  "$TOPIC_PREFIX.hpsf.strike-flow|6|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10,max.compaction.lag.ms=3600000"
  "$TOPIC_PREFIX.hpsf.market-flow|1|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10"
  "$TOPIC_PREFIX.hpsf.strike-score|6|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10,max.compaction.lag.ms=3600000"
  "$TOPIC_PREFIX.hpsf.signal|1|delete|2592000000|1800000|134217728|"
  "$TOPIC_PREFIX.hpsf.latest-signal|1|compact,delete|2592000000|1800000|134217728|delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.10"
  "$TOPIC_PREFIX.hpsf.audit|2|delete|2592000000|1800000|134217728|"
  "$TOPIC_PREFIX.hpsf.dlq|1|delete|2592000000|1800000|134217728|"
)

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

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

describe_topic_header() {
  local topic="$1"
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" 2>/dev/null | sed -n '/^Topic:/p' || true
}

wait_for_topic() {
  local topic="$1"
  local attempt
  for attempt in $(seq 1 "$TOPIC_METADATA_RETRIES"); do
    if [[ -n "$(describe_topic_header "$topic")" ]]; then
      return 0
    fi
    sleep "$TOPIC_METADATA_RETRY_SLEEP_SECONDS"
  done
  echo "Timed out waiting for Kafka topic metadata: $topic" >&2
  return 1
}

wait_for_topic_deleted() {
  local topic="$1"
  local attempt
  for attempt in $(seq 1 60); do
    if [[ -z "$(describe_topic_header "$topic")" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for Kafka topic deletion: $topic" >&2
  return 1
}

reset_replay_topics() {
  local spec topic
  if [[ "$RESET_TOPICS" != "true" ]]; then
    return 0
  fi
  echo "Resetting HPSF replay topics before replay run"
  for spec in "${REPLAY_TOPIC_SPECS[@]}"; do
    IFS='|' read -r topic _ <<<"$spec"
    if [[ "$DRY_RUN" == "true" || -n "$(describe_topic_header "$topic")" ]]; then
      run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "$topic"
    fi
  done
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  for spec in "${REPLAY_TOPIC_SPECS[@]}"; do
    IFS='|' read -r topic _ <<<"$spec"
    wait_for_topic_deleted "$topic"
  done
}

alter_topic_configs() {
  local topic="$1"
  local configs="$2"
  local attempt
  for attempt in $(seq 1 10); do
    if run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config "$configs"; then
      return 0
    fi
    echo "Retrying Kafka topic config update for $topic after metadata propagation delay ($attempt/10)" >&2
    sleep 2
  done
  echo "Failed to update Kafka topic configs for $topic" >&2
  return 1
}

verify_topic_ready() {
  local topic="$1"
  local attempt
  local describe_out
  local describe_err
  local configs_out
  local configs_err

  describe_out="$(mktemp)"
  describe_err="$(mktemp)"
  configs_out="$(mktemp)"
  configs_err="$(mktemp)"

  for attempt in $(seq 1 "$TOPIC_METADATA_RETRIES"); do
    if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$topic" >"$describe_out" 2>"$describe_err"; then
      if kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --describe >"$configs_out" 2>"$configs_err"; then
        cat "$describe_out"
        cat "$configs_out"
        rm -f "$describe_out" "$describe_err" "$configs_out" "$configs_err"
        return 0
      fi
      echo "Waiting for Kafka topic config metadata for $topic after create/config alter ($attempt/$TOPIC_METADATA_RETRIES)" >&2
    else
      echo "Waiting for Kafka topic metadata for $topic after create/config alter ($attempt/$TOPIC_METADATA_RETRIES)" >&2
    fi
    sleep "$TOPIC_METADATA_RETRY_SLEEP_SECONDS"
  done

  echo "Timed out verifying Kafka topic after create/config alter: $topic" >&2
  echo "Last kafka-topics --describe error:" >&2
  cat "$describe_err" >&2 || true
  echo "Last kafka-configs --describe error:" >&2
  cat "$configs_err" >&2 || true
  rm -f "$describe_out" "$describe_err" "$configs_out" "$configs_err"
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
  local configs
  configs="$(base_configs "$cleanup_policy" "$retention_ms" "$segment_ms" "$segment_bytes" "$extra")"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN topic=$topic partitions=$partitions replication.factor=$REPLICATION_FACTOR cleanup.policy=$cleanup_policy retention.ms=$retention_ms min.insync.replicas=$MIN_ISR compression.type=lz4"
    run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" --config "cleanup.policy=$cleanup_policy" --config "retention.ms=$retention_ms" --config "min.insync.replicas=$MIN_ISR" --config 'compression.type=lz4' --config "segment.ms=$segment_ms" --config "segment.bytes=$segment_bytes"
    run_cmd kafka-configs --bootstrap-server "$BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config "$configs"
    return 0
  fi

  if [[ -n "$(describe_topic_header "$topic")" ]]; then
    echo "Topic $topic already exists; applying replay configs"
  else
    run_cmd kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --create --topic "$topic" --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" --config "cleanup.policy=$cleanup_policy" --config "retention.ms=$retention_ms" --config "min.insync.replicas=$MIN_ISR" --config 'compression.type=lz4' --config "segment.ms=$segment_ms" --config "segment.bytes=$segment_bytes"
  fi
  wait_for_topic "$topic"
  alter_topic_configs "$topic" "$configs"
  verify_topic_ready "$topic"
}

echo "Creating HPSF replay topics for $REPLAY_DATE with replication.factor=1, min.insync.replicas=1, compression.type=lz4"
reset_replay_topics
for spec in "${REPLAY_TOPIC_SPECS[@]}"; do
  IFS='|' read -r topic partitions cleanup_policy retention_ms segment_ms segment_bytes extra <<<"$spec"
  create_topic_if_needed "$topic" "$partitions" "$cleanup_policy" "$retention_ms" "$segment_ms" "$segment_bytes" "$extra"
done
