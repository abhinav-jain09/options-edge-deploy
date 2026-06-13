#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?KUBECONFIG is required}"
NAMESPACE="${NAMESPACE:-options-edge}"
REMOTE_APP_HOME="${REMOTE_APP_HOME:-/home/options-edge}"
TMP_DIR="$REMOTE_APP_HOME/tmp"
WEB_BASE_URL="${WEB_BASE_URL:-http://192.168.100.252:8090}"
DATA_SEED_WAIT_SECONDS="${DATA_SEED_WAIT_SECONDS:-8}"
SYNTHETIC_CHECK_ATTEMPTS="${SYNTHETIC_CHECK_ATTEMPTS:-12}"
SYNTHETIC_CHECK_SLEEP_SECONDS="${SYNTHETIC_CHECK_SLEEP_SECONDS:-10}"
LIVE_UI_MARKET_HOURS_ZONE="${LIVE_UI_MARKET_HOURS_ZONE:-America/New_York}"
LIVE_UI_MARKET_OPEN="${LIVE_UI_MARKET_OPEN:-09:30}"
LIVE_UI_MARKET_CLOSE="${LIVE_UI_MARKET_CLOSE:-16:00}"
mkdir -p "$TMP_DIR"

live_ui_market_open() {
  python3 - "$LIVE_UI_MARKET_HOURS_ZONE" "$LIVE_UI_MARKET_OPEN" "$LIVE_UI_MARKET_CLOSE" <<'PY'
from datetime import datetime, time
from zoneinfo import ZoneInfo
import sys

zone_name, open_text, close_text = sys.argv[1:4]
now = datetime.now(ZoneInfo(zone_name))
opened = time.fromisoformat(open_text)
closed = time.fromisoformat(close_text)
is_regular_weekday = now.weekday() < 5
is_regular_hours = opened <= now.time() <= closed
print("OPEN" if is_regular_weekday and is_regular_hours else "CLOSED")
PY
}

skip_live_ui_check_outside_market_hours() {
  local state
  state="$(live_ui_market_open)"
  if [ "$state" = "OPEN" ]; then
    return 1
  fi
  echo "SKIPPED - outside market hours."
  echo "Market hours for live UI/card/stream synthetic checks: Monday-Friday ${LIVE_UI_MARKET_OPEN}-${LIVE_UI_MARKET_CLOSE} ${LIVE_UI_MARKET_HOURS_ZONE}, excluding full market holidays and official early closes."
  echo "A skipped live-market check is not proof that live streams work."
  return 0
}

wait_for_port_forward() {
  local name="$1"
  local pid="$2"
  local local_port="$3"
  local path="$4"
  local log_file="$5"
  local attempts="${PORT_FORWARD_WAIT_ATTEMPTS:-20}"

  for attempt in $(seq 1 "$attempts"); do
    if curl -fsS --max-time 2 "http://127.0.0.1:${local_port}${path}" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Port-forward for $name exited before ${path} became reachable." >&2
      cat "$log_file" >&2 || true
      return 1
    fi
    sleep 1
  done

  echo "Timed out waiting for $name port-forward on 127.0.0.1:${local_port}${path}" >&2
  cat "$log_file" >&2 || true
  return 1
}

check_deployment() {
  local deployment="$1"
  local local_port="$2"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking rollout for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s

  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_processing_service_ready'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_feed_gateway() {
  local deployment="feed-gateway-service"
  local local_port="19091"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8091" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_feed_gateway_running'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_ibkr_feed() {
  local deployment="ibkr-feed-service"
  local local_port="18087"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  curl -fsS "http://127.0.0.1:${local_port}/metrics" | grep -q 'options_edge_ibkr_feed_live'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

check_integration_test() {
  local deployment="options-edge-integration-test"
  local local_port="18082"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  local pod
  pod="$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name="$deployment" -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NAMESPACE" port-forward "pod/$pod" "${local_port}:8080" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  echo "Running final UI/data-path synthetic check"
  for attempt in $(seq 1 "$SYNTHETIC_CHECK_ATTEMPTS"); do
    if curl -fsS "http://127.0.0.1:${local_port}/api/check/once"; then
      echo
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return
    fi
    echo
    if [ "$attempt" -lt "$SYNTHETIC_CHECK_ATTEMPTS" ]; then
      echo "Synthetic check failed on attempt $attempt/$SYNTHETIC_CHECK_ATTEMPTS; retrying in ${SYNTHETIC_CHECK_SLEEP_SECONDS}s"
      sleep "$SYNTHETIC_CHECK_SLEEP_SECONDS"
    fi
  done
  echo "Synthetic check failed after $SYNTHETIC_CHECK_ATTEMPTS attempts" >&2
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

request_selected_contract() {
  local source_mode="${APP_MARKET_DATA_SOURCE:-${MARKET_DATA_SOURCE:-DATABENTO}}"
  local config_file="$TMP_DIR/options-edge-web-config.json"
  local form_file="$TMP_DIR/options-edge-web-connect-form.txt"
  echo "Requesting selected $source_mode contract through $WEB_BASE_URL/api/connect"
  curl -fsS "$WEB_BASE_URL/api/config" >"$config_file"
  python3 - "$config_file" "$source_mode" >"$form_file" <<'PY'
import json
import sys
from urllib.parse import urlencode

with open(sys.argv[1], encoding="utf-8") as fh:
    config = json.load(fh)

source = (sys.argv[2] or config.get("marketDataSource") or "DATABENTO").upper()
if source == "IB":
    source = "IBKR"

print(urlencode({
    "provider": config.get("provider", "IB"),
    "marketDataSource": source,
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
check_deployment raw-to-display-databento-service 18090
check_deployment databento-volume-aggregator 18094
check_deployment volume-pace-service 18081
check_deployment volume-pace-databento-service 18091
check_deployment directional-pressure-service 18084
check_deployment directional-pressure-databento-service 18092
check_deployment volume-sandwich-service 18083
check_deployment volume-sandwich-databento-service 18093
check_deployment unusual-whales-gex-service 18088
check_deployment unusual-whales-gex-history-service 18089
check_deployment raw-postgres-writer 18085
check_deployment pressure-postgres-writer 18086
check_ibkr_feed
check_feed_gateway
if skip_live_ui_check_outside_market_hours; then
  exit 0
fi
request_selected_contract
check_integration_test
