#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?KUBECONFIG is required}"
NAMESPACE="${NAMESPACE:-options-edge}"
REMOTE_APP_HOME="${REMOTE_APP_HOME:-/home/options-edge}"
TMP_DIR="$REMOTE_APP_HOME/tmp"
WEB_BASE_URL="${WEB_BASE_URL:-http://192.168.100.252:8090}"
DATA_SEED_WAIT_SECONDS="${DATA_SEED_WAIT_SECONDS:-8}"
mkdir -p "$TMP_DIR"

check_deployment() {
  local deployment="$1"
  local local_port="$2"
  echo "Checking rollout for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s

  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >"$TMP_DIR/$deployment-port-forward.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_processing_service_ready'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_feed_gateway() {
  local deployment="feed-gateway-service"
  local local_port="19091"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8091" >"$TMP_DIR/$deployment-port-forward.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_feed_gateway_running'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_ibkr_feed() {
  local deployment="ibkr-feed-service"
  local local_port="18087"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >"$TMP_DIR/$deployment-port-forward.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_ibkr_feed_ready'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_integration_test() {
  local deployment="options-edge-integration-test"
  local local_port="18082"
  echo "Checking live health for $deployment"
  local pod
  pod="$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name="$deployment" -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NAMESPACE" port-forward "pod/$pod" "${local_port}:8080" >"$TMP_DIR/$deployment-port-forward.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  sleep 3
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  echo "Running final UI/data-path synthetic check"
  curl -fsS "http://127.0.0.1:${local_port}/api/check/once"
  echo
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

request_selected_contract() {
  local source_mode="${APP_MARKET_DATA_SOURCE:-${MARKET_DATA_SOURCE:-DATABENTO}}"
  if [[ "$source_mode" == "IBKR" ]]; then
    echo "Skipping Databento /api/connect seed because MARKET_DATA_SOURCE=IBKR; waiting for IBKR feed records."
    sleep "$DATA_SEED_WAIT_SECONDS"
    return
  fi
  local config_file="$TMP_DIR/options-edge-web-config.json"
  local form_file="$TMP_DIR/options-edge-web-connect-form.txt"
  echo "Requesting selected contract seed through $WEB_BASE_URL/api/connect"
  curl -fsS "$WEB_BASE_URL/api/config" >"$config_file"
  python3 - "$config_file" >"$form_file" <<'PY'
import json
import sys
from urllib.parse import urlencode

with open(sys.argv[1], encoding="utf-8") as fh:
    config = json.load(fh)

print(urlencode({
    "provider": config.get("provider", "IB"),
    "symbol": config.get("symbol", "SPX"),
    "expiry": config.get("expiry", ""),
    "port": str(config.get("port", 4001)),
    "clientId": str(config.get("clientId", 112)),
    "maxStrikes": str(config.get("maxStrikes", 43)),
    "delayed": str(config.get("delayed", True)).lower(),
}))
PY
  curl -fsS -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-binary "@$form_file" \
    "$WEB_BASE_URL/api/connect"
  echo
  sleep "$DATA_SEED_WAIT_SECONDS"
}

check_deployment raw-to-display-service 18080
check_deployment volume-pace-service 18081
check_deployment directional-pressure-service 18084
check_deployment volume-sandwich-service 18083
check_deployment unusual-whales-gex-service 18088
check_deployment raw-postgres-writer 18085
check_deployment pressure-postgres-writer 18086
check_ibkr_feed
check_feed_gateway
request_selected_contract
check_integration_test
