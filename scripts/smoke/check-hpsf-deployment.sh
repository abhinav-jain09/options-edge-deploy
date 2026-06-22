#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECONFIG="${KUBECONFIG:-}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.4:9092}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"
REQUIRE_LATEST_SIGNAL="${REQUIRE_LATEST_SIGNAL:-false}"
SKIP_K8S="${SKIP_K8S:-false}"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "$KUBECONFIG" ]]; then
  KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")
fi

log() {
  printf '[hpsf-smoke] %s\n' "$*"
}

topic_name() {
  printf '%s%s' "$TOPIC_PREFIX" "$1"
}

run_or_echo() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '+ %q' "$1"
    shift
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
  else
    "$@"
  fi
}

require_file_contains() {
  local file="$1"
  local pattern="$2"
  if ! grep -Fq "$pattern" "$file"; then
    echo "Missing required pattern '$pattern' in $file" >&2
    exit 1
  fi
}

validate_static_config() {
  log "validating HPSF deployment static config"
  require_file_contains "$ROOT_DIR/k8s/base/kustomization.yaml" "hpsf-stage-a-deployment.yaml"
  require_file_contains "$ROOT_DIR/k8s/base/kustomization.yaml" "hpsf-stage-b-deployment.yaml"
  require_file_contains "$ROOT_DIR/k8s/base/kustomization.yaml" "hpsf-postgres-writer-deployment.yaml"
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_TOPIC_LATEST_SIGNAL: options.hpsf.latest-signal"
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_STREAMS_REPLICATION_FACTOR: \"1\""
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_STREAMS_NUM_STANDBY_REPLICAS: \"0\""
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_ORDER_PLACEMENT_ENABLED: \"false\""
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-a-deployment.yaml" "APP_MARKET_DATA_SOURCE"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-a-deployment.yaml" "DATABENTO"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-a-deployment.yaml" "HPSF_STAGE_A_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-a-deployment.yaml" "HPSF_STAGE_B_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_STAGE_A_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_STAGE_B_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_TOPOLOGY_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "value: \"true\""
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-postgres-writer-deployment.yaml" "/health/ready"
}

check_k8s() {
  if [[ "$SKIP_K8S" == "true" ]]; then
    log "skipping Kubernetes checks because SKIP_K8S=true"
    return 0
  fi
  if ! command -v kubectl >/dev/null 2>&1; then
    log "kubectl not found; skipping Kubernetes checks"
    return 0
  fi
  for deployment in hpsf-stage-a-service hpsf-stage-b-service hpsf-postgres-writer-service feed-gateway-service; do
    log "checking rollout for $deployment"
    if ! run_or_echo "${KUBECTL[@]}" rollout status deployment/"$deployment" --timeout=180s; then
      print_k8s_diagnostics "$deployment"
      return 1
    fi
  done
  log "checking HPSF ConfigMap signal-only flags"
  if [[ "$DRY_RUN" == "false" ]]; then
    config="$(${KUBECTL[@]} get configmap options-edge-config -o yaml)"
    grep -F 'HPSF_ORDER_PLACEMENT_ENABLED: "false"' <<<"$config" >/dev/null
    grep -F 'ORDER_PLACEMENT_ENABLED: "false"' <<<"$config" >/dev/null
    if grep -E 'IBKR_ORDER_PLACEMENT_ENABLED: "?(true|1|on)"?' <<<"$config" >/dev/null; then
      echo "IBKR order placement flag is enabled in configmap; HPSF signal-only gate failed" >&2
      exit 1
    fi
  fi
}

print_k8s_diagnostics() {
  local deployment="$1"
  if [[ "$DRY_RUN" == "true" || "$SKIP_K8S" == "true" ]]; then
    return 0
  fi
  log "diagnostics for $deployment"
  "${KUBECTL[@]}" get deployment "$deployment" -o wide || true
  "${KUBECTL[@]}" describe deployment "$deployment" || true
  "${KUBECTL[@]}" get pods -l "app.kubernetes.io/name=$deployment" -o wide || true
  "${KUBECTL[@]}" describe pods -l "app.kubernetes.io/name=$deployment" || true
  "${KUBECTL[@]}" logs -l "app.kubernetes.io/name=$deployment" --tail=120 --all-containers=true || true
  "${KUBECTL[@]}" get events --sort-by=.lastTimestamp | tail -80 || true
}

check_topics() {
  if [[ -x "$ROOT_DIR/scripts/kafka/verify-hpsf-topics.sh" ]]; then
    log "verifying HPSF Kafka topics and RF=1 topic configs"
    if [[ "$DRY_RUN" == "true" ]]; then
      run_or_echo "$ROOT_DIR/scripts/kafka/verify-hpsf-topics.sh"
    else
      TOPIC_PREFIX="$TOPIC_PREFIX" KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" "$ROOT_DIR/scripts/kafka/verify-hpsf-topics.sh"
    fi
  else
    echo "Missing scripts/kafka/verify-hpsf-topics.sh" >&2
    exit 1
  fi
}

check_latest_signal() {
  if ! command -v kafka-console-consumer >/dev/null 2>&1; then
    log "kafka-console-consumer not found; skipping latest-signal read"
    return 0
  fi
  local output
  local status=0
  local latest_signal_topic
  latest_signal_topic="$(topic_name options.hpsf.latest-signal)"
  log "checking $latest_signal_topic readability"
  set +e
  output="$(kafka-console-consumer --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic "$latest_signal_topic" --from-beginning --max-messages 1 --timeout-ms 10000 2>&1)"
  status=$?
  set -e
  if [[ $status -ne 0 && "$REQUIRE_LATEST_SIGNAL" == "true" ]]; then
    echo "$output" >&2
    exit $status
  fi
  if grep -F '"enabled":true' <<<"$output" >/dev/null; then
    echo "HPSF latest signal contains orderInstruction.enabled=true" >&2
    exit 1
  fi
  if [[ $status -ne 0 ]]; then
    log "latest-signal had no message during smoke window; continuing because REQUIRE_LATEST_SIGNAL=false"
  fi
}

validate_static_config
check_k8s
check_topics
if [[ "$DRY_RUN" == "false" ]]; then
  check_latest_signal
fi
log "HPSF deployment smoke completed"
