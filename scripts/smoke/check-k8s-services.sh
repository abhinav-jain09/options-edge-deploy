#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?KUBECONFIG is required}"
NAMESPACE="${NAMESPACE:-options-edge}"
REMOTE_APP_HOME="${REMOTE_APP_HOME:-/home/options-edge}"
TMP_DIR="${JENKINS_WORK_DIR:-$REMOTE_APP_HOME/tmp}"
if [[ -z "${WEB_BASE_URL:-}" ]]; then
  WEB_BASE_URL="http://192.168.100.252:8090"
fi
DATA_SEED_WAIT_SECONDS="${DATA_SEED_WAIT_SECONDS:-8}"
SYNTHETIC_CHECK_ATTEMPTS="${SYNTHETIC_CHECK_ATTEMPTS:-12}"
SYNTHETIC_CHECK_SLEEP_SECONDS="${SYNTHETIC_CHECK_SLEEP_SECONDS:-10}"
LIVE_UI_MARKET_HOURS_ZONE="${LIVE_UI_MARKET_HOURS_ZONE:-America/New_York}"
LIVE_UI_MARKET_OPEN="${LIVE_UI_MARKET_OPEN:-09:30}"
LIVE_UI_MARKET_CLOSE="${LIVE_UI_MARKET_CLOSE:-16:00}"
mkdir -p "$TMP_DIR"

# Smoke an endpoint that may be protected by the Keycloak/OIDC login gate.
#   - HTTP 200            -> the endpoint served; its body MUST match $pattern.
#   - HTTP 401 + "Www-Authenticate: Bearer" -> the service is up AND correctly
#                           enforcing login (the gate we deliberately enabled).
#                           That is a healthy state, so it passes.
#   - anything else (000 connection-refused, 5xx, 401 without Bearer) -> fail.
# This keeps the smoke meaningful with auth ON or OFF without embedding tokens.
smoke_authgated_endpoint() {
  local url="$1" label="$2" pattern="$3"
  local hdr="$TMP_DIR/smoke-authgated.hdr" body="$TMP_DIR/smoke-authgated.body" code
  code="$(curl -s --connect-timeout 5 --max-time 20 -D "$hdr" -o "$body" -w '%{http_code}' "$url")" || code="000"
  if [ "$code" = "200" ]; then
    if grep -Eq "$pattern" "$body"; then
      echo "  $label -> 200, content OK"
    else
      echo "ERROR: $label returned 200 but body did not match /$pattern/" >&2
      return 1
    fi
  elif [ "$code" = "401" ] && grep -qi '^www-authenticate:[[:space:]]*bearer' "$hdr"; then
    echo "  $label -> 401 Bearer (service up + login enforced) OK"
  else
    echo "ERROR: $label returned unexpected HTTP $code (expected 200 with content, or 401 Bearer)" >&2
    return 1
  fi
}

live_ui_market_open() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "CLOSED"
    return 0
  fi
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

choose_local_port() {
  local requested_port="$1"
  local port="$requested_port"

  while ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN; do
    port=$((port + 100))
  done

  if [ "$port" != "$requested_port" ]; then
    echo "Local port $requested_port is already in use; using $port for port-forward." >&2
  fi
  echo "$port"
}

check_deployment() {
  local deployment="$1"
  local local_port
  local_port="$(choose_local_port "$2")"
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
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_feed_gateway() {
  local deployment="feed-gateway-service"
  local local_port
  local_port="$(choose_local_port 19091)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  local health_path="/health"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:18091" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "$health_path" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}${health_path}" | grep -q '"running":true'
  echo
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_ibkr_feed() {
  local deployment="ibkr-feed-service"
  local local_port
  local_port="$(choose_local_port 18087)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8080" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_ibkr_feed_live' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_strike_flow_classifier() {
  check_strike_flow_classifier_deployment "strike-flow-classifier-databento"
  check_strike_flow_classifier_deployment "strike-flow-classifier-ibkr"
}

check_strike_flow_classifier_deployment() {
  local deployment="$1"
  local local_port
  local_port="$(choose_local_port 18108)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8097" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready{service="strike-flow-classifier-service"}' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_spx_mission_control() {
  local deployment="spx-mission-control-service"
  local local_port
  local_port="$(choose_local_port 18096)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=180s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8096" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready{service="spx-mission-control-service"}' <<<"$metrics"
  smoke_authgated_endpoint "http://127.0.0.1:${local_port}/mission-control" "/mission-control" 'SPX Mission Control'
  smoke_authgated_endpoint "http://127.0.0.1:${local_port}/api/mission-control/latest" "/api/mission-control/latest" 'spx-mission-control|NO_DATA'
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_databento_mission_pressure() {
  local deployment="databento-mission-pressure-service"
  local local_port
  local_port="$(choose_local_port 18098)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8098" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready{service="databento-mission-pressure-service"}' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_databento_mission_pace() {
  local deployment="databento-mission-pace-service"
  local local_port
  local_port="$(choose_local_port 18097)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8097" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready{service="databento-mission-pace-service"}' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_databento_mission_sandwich() {
  local deployment="databento-mission-sandwich-service"
  local local_port
  local_port="$(choose_local_port 18099)"
  local log_file="$TMP_DIR/$deployment-port-forward.log"
  echo "Checking live health for $deployment"
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
  kubectl -n "$NAMESPACE" port-forward "deployment/$deployment" "${local_port}:8099" >"$log_file" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' RETURN
  wait_for_port_forward "$deployment" "$pid" "$local_port" "/health/live" "$log_file"
  curl -fsS "http://127.0.0.1:${local_port}/health/live"
  echo
  local metrics
  metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"
  grep -q 'options_edge_processing_service_ready{service="databento-mission-sandwich-service"}' <<<"$metrics"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - RETURN
}

check_integration_test() {
  local deployment="options-edge-integration-test"
  local local_port
  local_port="$(choose_local_port 18082)"
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
      trap - RETURN
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
  trap - RETURN
  return 1
}

request_selected_contract() {
  local source_mode="${APP_MARKET_DATA_SOURCE:-${MARKET_DATA_SOURCE:-DATABENTO}}"
  local config_file="$TMP_DIR/options-edge-web-config.json"
  local form_file="$TMP_DIR/options-edge-web-connect-form.txt"
  echo "Requesting selected $source_mode contract through $WEB_BASE_URL/api/connect"
  # /api/config and /api/connect are gated behind Keycloak login (401 without a bearer JWT). When auth is
  # enabled the unauthenticated connect-seed can't run — and it isn't needed: the live Databento feed pod
  # seeds market data into Kafka independently of the web app's /api/connect. Skip rather than false-fail.
  local config_code
  config_code="$(curl -s -o "$config_file" -w '%{http_code}' --connect-timeout 5 --max-time 15 "$WEB_BASE_URL/api/config")" || config_code="000"
  if [ "$config_code" = "401" ]; then
    echo "Web /api/config requires auth (login enabled); skipping unauthenticated connect-seed (feed pod seeds data independently)."
    return 0
  fi
  if [ "$config_code" != "200" ]; then
    echo "Unexpected status $config_code from $WEB_BASE_URL/api/config" >&2
    return 1
  fi
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
check_databento_mission_pace
check_databento_mission_pressure
check_databento_mission_sandwich
check_deployment volume-pace-service 18081
check_deployment volume-pace-databento-service 18091
check_deployment directional-pressure-service 18084
check_deployment directional-pressure-databento-service 18092
check_deployment volume-sandwich-service 18083
check_deployment volume-sandwich-databento-service 18093
check_deployment unusual-whales-gex-service 18088
check_deployment unusual-whales-gex-history-service 18089
check_deployment databento-gex-history-service 18095
check_deployment raw-postgres-writer 18085
check_deployment pressure-postgres-writer 18086
check_spx_mission_control
check_strike_flow_classifier
check_ibkr_feed
check_feed_gateway
if skip_live_ui_check_outside_market_hours; then
  exit 0
fi
request_selected_contract
check_integration_test
