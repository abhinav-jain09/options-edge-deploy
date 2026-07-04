#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${NAMESPACE:-options-edge}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:19092}"
KUBECTL_AS="${KUBECTL_AS:---as=system:serviceaccount:options-edge:jenkins-deployer}"
TIMESTAMP_AFTER_MAX_MS="${TIMESTAMP_AFTER_MAX_MS:-604800000}"
REPLICA_FILE="${REPLICA_FILE:-/tmp/options-edge-dev-replicas-before-replay-cleanup.txt}"

if [[ "$ENVIRONMENT" != "dev" ]]; then
  echo "Refusing to run timewarp replay cleanup outside dev (ENVIRONMENT=$ENVIRONMENT)" >&2
  exit 1
fi

# shellcheck disable=SC2206
KUBECTL_AUTH_ARGS=($KUBECTL_AS)

kube() {
  kubectl "${KUBECTL_AUTH_ARGS[@]}" -n "$NAMESPACE" "$@"
}

wait_deployments_zero() {
  local attempts="${DEPLOYMENT_ZERO_WAIT_SECONDS:-180}"
  for ((i = 1; i <= attempts; i++)); do
    if ! kube get deploy -l app.kubernetes.io/part-of=options-edge \
      -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.readyReplicas}{"\n"}{end}' \
      | awk '$2 != "" && $2 != "0" { found=1 } END { exit found ? 1 : 0 }'; then
      sleep 1
      continue
    fi
    return 0
  done
  echo "Timed out waiting for options-edge deployments to scale to zero" >&2
  kube get deploy -l app.kubernetes.io/part-of=options-edge
  exit 1
}

wait_topics_deleted() {
  local attempts="${TOPIC_DELETE_WAIT_SECONDS:-180}"
  for ((i = 1; i <= attempts; i++)); do
    remaining="$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list \
      | grep -Ev '^(__consumer_offsets|__transaction_state)$' || true)"
    if [[ -z "$remaining" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for Kafka topics to delete:" >&2
  printf '%s\n' "$remaining" >&2
  exit 1
}

wait_snapshot_seeded() {
  local attempts="${SNAPSHOT_SEED_WAIT_SECONDS:-240}"
  for ((i = 1; i <= attempts; i++)); do
    if kube logs deploy/databento-timewarp-snapshot-replay --since=10m --tail=300 2>/dev/null \
      | grep -q '"event": "seeded"'; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for databento-timewarp-snapshot-replay to finish seeding" >&2
  kube logs deploy/databento-timewarp-snapshot-replay --tail=80 || true
  exit 1
}

topic_offset_total() {
  kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$1" \
    --time -1 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}

wait_topic_records() {
  local topic="$1"
  local minimum="$2"
  local attempts="${3:-120}"
  local total
  for ((i = 1; i <= attempts; i++)); do
    total="$(topic_offset_total "$topic")"
    if (( total >= minimum )); then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $topic to reach $minimum records" >&2
  kafka-run-class kafka.tools.GetOffsetShell --broker-list "$KAFKA_BOOTSTRAP_SERVERS" --topic "$topic" --time -1 || true
  exit 1
}

topic_partitions() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null \
    | sed -n '1s/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' \
    || true
}

kafka_config_value() {
  local value="$1"
  if [[ "$value" == *,* ]]; then
    printf '[%s]' "$value"
  else
    printf '%s' "$value"
  fi
}

ensure_topic() {
  local topic="$1"
  local partitions="$2"
  local cleanup="$3"
  local retention_ms="${4:-${KAFKA_TOPIC_RETENTION_MS:-36000000}}"
  local current

  current="$(topic_partitions "$topic")"
  if [[ -z "$current" ]]; then
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-1}" \
      --config "retention.ms=$retention_ms" \
      --config "cleanup.policy=$cleanup" \
      --config "min.insync.replicas=${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
  elif (( current < partitions )); then
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --alter \
      --topic "$topic" \
      --partitions "$partitions"
  fi

  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "retention.ms=$retention_ms,cleanup.policy=$(kafka_config_value "$cleanup"),min.insync.replicas=${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}" >/dev/null
}

allow_future_timestamps() {
  local topic="$1"
  kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "message.timestamp.after.max.ms=$TIMESTAMP_AFTER_MAX_MS" >/dev/null
}

restore_deployments() {
  while read -r name replicas; do
    [[ -z "${name:-}" ]] && continue
    case "$name" in
      options-edge-web|feed-gateway-service|options-edge-redis|raw-to-display-databento-service|databento-gex-service|databento-gex-history-service|gex-delta-redis-writer|option-price-behavior-service|strike-liquidity-heatmap-service|databento-mission-pace-service|databento-mission-pressure-service|databento-mission-sandwich-service|spx-mission-control-service|strike-flow-classifier-databento|strike-flow-avro-adapter|delta-flow-service|directional-pressure-databento-service|unified-sr-service)
        replicas=1
        ;;
      *hpsf*|unusual-whales-*|strike-flow-classifier-ibkr|volume-pace-service|volume-sandwich-service)
        replicas=0
        ;;
    esac
    kube scale "deploy/$name" --replicas="$replicas" >/dev/null
  done < "$REPLICA_FILE"
}

echo "[replay] stopping replay producers"
kube delete job databento-timewarp-replay --ignore-not-found=true
kube scale deploy/databento-timewarp-snapshot-replay --replicas=0

echo "[replay] saving current dev replicas to $REPLICA_FILE"
kube get deploy -l app.kubernetes.io/part-of=options-edge \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.replicas}{"\n"}{end}' > "$REPLICA_FILE"

echo "[replay] scaling options-edge dev services to zero"
kube scale deploy -l app.kubernetes.io/part-of=options-edge --replicas=0
wait_deployments_zero

echo "[replay] deleting all non-system Kafka topics from dev broker $KAFKA_BOOTSTRAP_SERVERS"
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list \
  | grep -Ev '^(__consumer_offsets|__transaction_state)$' \
  | while read -r topic; do
      [[ -z "$topic" ]] && continue
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
    done
wait_topics_deleted

echo "[replay] truncating dev Session Flow tables when DB credentials are available"
if [[ -n "${SESSION_FLOW_DB_PASSWORD:-${PGPASSWORD:-}}" ]]; then
  PGPASSWORD="${SESSION_FLOW_DB_PASSWORD:-${PGPASSWORD:-}}" psql \
    "host=${SESSION_FLOW_DB_HOST:-127.0.0.1} port=${SESSION_FLOW_DB_PORT:-5432} dbname=${SESSION_FLOW_DB_NAME:-options_flow_dev} user=${SESSION_FLOW_DB_USER:-options_flow}" \
    -c "truncate table public.pin_strike_minute, public.pin_strike_fine restart identity"
else
  echo "[replay] SESSION_FLOW_DB_PASSWORD/PGPASSWORD not set; skipping Session Flow table truncate"
fi

echo "[replay] applying base dev Kafka topics"
export ENVIRONMENT KAFKA_BOOTSTRAP_SERVERS
# shellcheck source=/dev/null
source "$ROOT/scripts/kafka/load-kafka-settings.sh"
export KAFKA_RECREATE_MISMATCHED_TOPICS=true
"$ROOT/scripts/kafka/apply-topics.sh"

echo "[replay] creating replay/runtime topics before services start"
ensure_topic options.opra.tcbbo 32 delete
ensure_topic underlying.spx.price 1 compact,delete
ensure_topic options.databento.strike-flow 32 compact,delete 172800000
ensure_topic options.databento.strike-flow.strike.avro 32 delete
ensure_topic options.databento.gex.strike 32 compact,delete
ensure_topic options.databento.gex.strike.history 32 delete
ensure_topic options.databento.gex.flow.by-strike 32 delete
ensure_topic options.databento.gex.flow.10s 32 delete
ensure_topic options.databento.gex.flow.1m 32 delete
ensure_topic options.databento.gex.flow.5m 32 delete
ensure_topic options.databento.gex.flow.15m 32 delete
ensure_topic options.databento.gex.flow.session 32 delete
ensure_topic options.databento.gex.flow.dashboard 32 delete
ensure_topic option-price-behavior-by-option 32 compact,delete
ensure_topic option-price-behavior-10s 32 delete
ensure_topic option-price-behavior-1m 32 delete
ensure_topic option-price-behavior-5m 32 delete
ensure_topic option-price-behavior-15m 32 delete
ensure_topic option-price-behavior-session 32 delete
ensure_topic option-price-behavior-by-strike 32 delete
ensure_topic option-price-behavior-by-moneyness 32 delete
ensure_topic option-price-behavior-dashboard 32 compact,delete
ensure_topic delta-flow-by-trade 32 delete
ensure_topic delta-flow-10s 32 delete
ensure_topic delta-flow-1m 32 delete
ensure_topic delta-flow-5m 32 delete
ensure_topic delta-flow-15m 32 delete
ensure_topic delta-flow-session 32 delete
ensure_topic delta-flow-by-strike 32 delete
ensure_topic delta-flow-dashboard 32 compact,delete
ensure_topic delta-flow-diagnostics 32 delete
ensure_topic options-edge-strike-liquidity-heatmap-v1-dev-chain-rekey 32 delete

echo "[replay] allowing Monday-session replay timestamps on derived output topics"
for topic in \
  options.databento.strike-flow \
  options.databento.strike-flow.strike.avro \
  options.databento.gex.strike \
  options.databento.gex.strike.history \
  options.databento.gex.flow.by-strike \
  options.databento.gex.flow.10s \
  options.databento.gex.flow.1m \
  options.databento.gex.flow.5m \
  options.databento.gex.flow.15m \
  options.databento.gex.flow.session \
  options.databento.gex.flow.dashboard \
  strike-liquidity-heatmap-bucket \
  strike-liquidity-heatmap-dashboard \
  option-price-behavior-by-option \
  option-price-behavior-10s \
  option-price-behavior-1m \
  option-price-behavior-5m \
  option-price-behavior-15m \
  option-price-behavior-session \
  option-price-behavior-by-strike \
  option-price-behavior-by-moneyness \
  option-price-behavior-dashboard \
  delta-flow-by-trade \
  delta-flow-10s \
  delta-flow-1m \
  delta-flow-5m \
  delta-flow-15m \
  delta-flow-session \
  delta-flow-by-strike \
  delta-flow-dashboard \
  delta-flow-diagnostics \
  options-edge-strike-liquidity-heatmap-v1-dev-chain-rekey; do
  allow_future_timestamps "$topic"
done

echo "[replay] restoring dev services, leaving out-of-scope feeds scaled to zero"
restore_deployments

echo "[replay] waiting for replay consumers before starting producers"
for deploy in \
  databento-gex-service \
  delta-flow-service \
  option-price-behavior-service \
  strike-liquidity-heatmap-service \
  databento-mission-pace-service \
  spx-mission-control-service \
  strike-flow-classifier-databento \
  strike-flow-avro-adapter; do
  kube rollout status "deploy/$deploy" --timeout=180s || true
done

echo "[replay] refreshing timewarp manifests"
kube delete job databento-timewarp-replay --ignore-not-found=true
kubectl "${KUBECTL_AUTH_ARGS[@]}" apply -f "$ROOT/k8s/overlays/dev/databento-timewarp-snapshot-replay-deployment.yaml"
kubectl "${KUBECTL_AUTH_ARGS[@]}" apply -f "$ROOT/k8s/overlays/dev/databento-timewarp-replay-job.yaml"

echo "[replay] starting six-and-a-half-hour replay"
kube scale deploy/databento-timewarp-snapshot-replay --replicas=1
kube rollout status deploy/databento-timewarp-snapshot-replay --timeout=180s

echo "[replay] waiting for snapshot replay seed before starting TCBBO replay"
wait_snapshot_seeded
echo "[replay] waiting for derived GEX to be available before starting TCBBO replay"
wait_topic_records options.databento.gex.strike 1 "${GEX_WARMUP_WAIT_SECONDS:-120}"
kube patch job databento-timewarp-replay --type=merge -p '{"spec":{"suspend":false}}'

echo "[replay] allowing Monday-session replay timestamps on delta-flow internal changelogs"
for topic in $(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | grep '^options-edge-delta-flow-service-v1-dev-' || true); do
  allow_future_timestamps "$topic"
done

echo "[replay] waiting for core replay services"
for deploy in \
  options-edge-web \
  databento-gex-service \
  delta-flow-service \
  option-price-behavior-service \
  strike-liquidity-heatmap-service \
  databento-mission-pace-service \
  spx-mission-control-service \
  strike-flow-classifier-databento \
  strike-flow-avro-adapter; do
  kube rollout status "deploy/$deploy" --timeout=180s || true
done

echo "[replay] started. Check /option-chain, /mission-pace, /session-flow?date=2026-07-06, and /option-price-behavior."
