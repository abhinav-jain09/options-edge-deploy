#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
# RF must be provided explicitly (Jenkins sources scripts/kafka/load-kafka-settings.sh,
# which derives it from the rendered per-environment configmap). No silent default.
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:?KAFKA_TOPIC_REPLICATION_FACTOR must be set (source scripts/kafka/load-kafka-settings.sh)}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
RECREATE_MISMATCHED="${KAFKA_RECREATE_MISMATCHED_TOPICS:-false}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"

# TOPIC_SET selects WHICH declared set to apply. Default (empty) = the SPX dev/prod set, i.e.
# byte-for-byte the previous behaviour. TOPIC_SET=es4 applies the es4 (192.168.100.4) set instead.
# es4 must NOT get the SPX set: that would create ~50 unrelated options.* topics on its broker.
TOPIC_SET="${TOPIC_SET:-}"
case "$TOPIC_SET" in
  "")
    ;;
  es4)
    : "${OPTIONS_EDGE_ES4_TOPICS:?OPTIONS_EDGE_ES4_TOPICS missing from topics.env}"
    OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_ES4_TOPICS"
    OPTIONS_EDGE_COMPACTED_TOPICS="${OPTIONS_EDGE_ES4_COMPACTED_TOPICS:-}"
    OPTIONS_EDGE_PURE_COMPACT_TOPICS="${OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS:-}"
    OPTIONS_EDGE_EXACT_PARTITION_TOPICS="${OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS:-}"
    OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="${OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES:-}"
    ;;
  *)
    echo "Unknown TOPIC_SET '$TOPIC_SET' (expected empty for dev/prod, or 'es4')" >&2
    exit 1
    ;;
esac

describe_topic() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null | sed -n '/^Topic:/p' || true
}

broker_ids() {
  kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" 2>/dev/null \
    | sed -n 's/.*(id: \([0-9][0-9]*\).*/\1/p' \
    | sort -n \
    | uniq
}

wait_for_topic_absent() {
  local topic="$1"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    if [[ -z "$(describe_topic "$topic")" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  describe_topic "$topic"
  return 1
}

wait_for_topic_shape() {
  local topic="$1"
  local expected_partitions="$2"
  local expected_replication_factor="$3"
  local attempts="${KAFKA_TOPIC_REPAIR_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    description="$(describe_topic "$topic")"
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
    if [[ "$current_partitions" == "$expected_partitions" && "$current_replication_factor" == "$expected_replication_factor" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for topic $topic to become partitions=$expected_partitions replicationFactor=$expected_replication_factor" >&2
  describe_topic "$topic"
  return 1
}

reassign_topic_replication_factor() {
  local topic="$1"
  local partitions="$2"
  local tmp
  mapfile -t brokers < <(broker_ids)

  if (( ${#brokers[@]} < REPLICATION_FACTOR )); then
    echo "Cannot assign replication factor $REPLICATION_FACTOR with only ${#brokers[@]} brokers" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  {
    printf '{"version":1,"partitions":['
    for ((partition = 0; partition < partitions; partition++)); do
      if (( partition > 0 )); then
        printf ','
      fi
      printf '{"topic":"%s","partition":%d,"replicas":[' "$topic" "$partition"
      for ((replica = 0; replica < REPLICATION_FACTOR; replica++)); do
        if (( replica > 0 )); then
          printf ','
        fi
        printf '%s' "${brokers[$(((partition + replica) % ${#brokers[@]}))]}"
      done
      printf ']}'
    done
    printf ']}\n'
  } > "$tmp"

  kafka-reassign-partitions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --reassignment-json-file "$tmp" \
    --execute
  rm -f "$tmp"
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  local cleanup_policy
  cleanup_policy="$(topic_cleanup_policy "$topic")"

  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$(topic_retention_ms "$topic")" \
    --config "cleanup.policy=$cleanup_policy" \
    --config "min.insync.replicas=$MIN_ISR"
}

topic_cleanup_policy() {
  local topic="$1"
  # PURE-compact topics keep the latest value per key FOREVER. They must not get the
  # "compact,delete" policy, because the delete half would drop the record once the
  # global retention elapses (spx.basis.state holds the ES-SPX anchor: losing it puts
  # the basis engine back to UNAVAILABLE and fail-closes the ES-on-SPX overlay).
  for pure_topic in ${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-}; do
    if [[ "$topic" == "$pure_topic" ]]; then
      echo "compact"
      return
    fi
  done
  for compacted_topic in ${OPTIONS_EDGE_COMPACTED_TOPICS:-}; do
    if [[ "$topic" == "$compacted_topic" ]]; then
      echo "${KAFKA_COMPACTED_TOPIC_CLEANUP_POLICY:-compact,delete}"
      return
    fi
  done
  echo "$CLEANUP_POLICY"
}

# Topics whose partition count must be EXACT, not "at least". The default policy treats extra
# partitions as harmless (more parallelism), but a topic read via an explicit
# assign(TopicPartition(topic, 0)) is only correct at exactly 1 partition — any record that hashes
# to another partition is invisible. Kafka cannot shrink a topic, so repairing these is DESTRUCTIVE
# (delete + recreate) and therefore still gated behind KAFKA_RECREATE_MISMATCHED_TOPICS=true.
topic_requires_exact_partitions() {
  local topic="$1" t
  for t in ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-}; do
    [[ "$topic" == "$t" ]] && return 0
  done
  return 1
}

# Per-topic retention override (ms), e.g. "spx.basis.state=-1". Unlisted topics use $RETENTION_MS.
topic_retention_ms() {
  local topic="$1" entry
  for entry in ${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-}; do
    if [[ "${entry%%=*}" == "$topic" ]]; then
      echo "${entry#*=}"
      return
    fi
  done
  echo "$RETENTION_MS"
}

kafka_config_value() {
  local value="$1"
  if [[ "$value" == \[*\] ]]; then
    echo "$value"
  elif [[ "$value" == *,* ]]; then
    echo "[$value]"
  else
    echo "$value"
  fi
}

alter_topic_config() {
  local topic="$1"
  local cleanup_policy="$2"
  local attempts="${KAFKA_TOPIC_CONFIG_RETRY_ATTEMPTS:-10}"

  for ((i = 1; i <= attempts; i++)); do
    if kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --entity-type topics --entity-name "$topic" --alter \
      --add-config "retention.ms=$(topic_retention_ms "$topic"),cleanup.policy=$(kafka_config_value "$cleanup_policy"),min.insync.replicas=$MIN_ISR"; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out updating config for topic $topic" >&2
  return 1
}

for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  partitions="${entry##*:}"
  cleanup_policy="$(topic_cleanup_policy "$topic")"
  description="$(describe_topic "$topic")"
  if [[ -n "$description" ]]; then
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"

    if [[ -z "$current_partitions" || -z "$current_replication_factor" ]]; then
      echo "Cannot parse current topic shape for $topic" >&2
      echo "$description" >&2
      exit 1
    fi

    exact_partition_mismatch=false
    if topic_requires_exact_partitions "$topic" && (( current_partitions != partitions )); then
      exact_partition_mismatch=true
    fi

    if [[ "$exact_partition_mismatch" == "true" ]]; then
      if [[ "$RECREATE_MISMATCHED" != "true" ]]; then
        echo "Topic $topic exists with partitions=$current_partitions but requires EXACTLY $partitions (single-partition read contract)." >&2
        echo "Kafka cannot shrink partitions: this needs a destructive delete+recreate." >&2
        echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
        echo "NOTE: recreating $topic DISCARDS its records — any bootstrap state it held must be re-seeded afterwards." >&2
        exit 1
      fi
      echo "Repairing EXACT-partition topic $topic: partitions=$current_partitions -> $partitions (destructive delete+recreate; records discarded)"
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic"
      wait_for_topic_absent "$topic"
      create_topic "$topic" "$partitions"
      wait_for_topic_shape "$topic" "$partitions" "$REPLICATION_FACTOR"
    elif (( current_partitions < partitions )) || (( current_replication_factor < REPLICATION_FACTOR )); then
      if [[ "$RECREATE_MISMATCHED" != "true" ]]; then
        if (( current_partitions < partitions )); then
          echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected at least partitions=$partitions replicationFactor=$REPLICATION_FACTOR" >&2
          echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
          exit 1
        fi
        echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected at least replicationFactor=$REPLICATION_FACTOR" >&2
        echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
        exit 1
      fi

      echo "Repairing mismatched topic $topic: partitions=$current_partitions replicationFactor=$current_replication_factor -> partitions=$partitions replicationFactor=$REPLICATION_FACTOR"
      desired_partitions="$(printf '%s\n' "$current_partitions" "$partitions" | sort -n | tail -1)"
      if (( current_partitions < partitions )); then
        kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
          --alter \
          --topic "$topic" \
          --partitions "$partitions"
      fi
      if [[ "$current_replication_factor" != "$REPLICATION_FACTOR" ]]; then
        reassign_topic_replication_factor "$topic" "$desired_partitions"
      fi
      wait_for_topic_shape "$topic" "$desired_partitions" "$REPLICATION_FACTOR"
    else
      echo "Topic $topic already exists with compatible partitions=$current_partitions expectedMinimum=$partitions replicationFactor=$REPLICATION_FACTOR"
    fi
  else
    create_topic "$topic" "$partitions"
    wait_for_topic_shape "$topic" "$partitions" "$REPLICATION_FACTOR"
  fi

  alter_topic_config "$topic" "$cleanup_policy"
done
