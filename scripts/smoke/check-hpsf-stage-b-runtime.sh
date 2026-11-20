#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECONFIG="${KUBECONFIG:-}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"
DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "$KUBECONFIG" ]]; then
  KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")
fi

log() { printf '[hpsf-stage-b-smoke] %s\n' "$*"; }

# DISABLED until further notice (2026-07-02): hpsf-stage-a/b replicas are pinned
# to 0 in k8s/base (temporarily not deployed). With zero pods this runtime check
# can never pass, so skip cleanly. Remove this guard when re-enabling.
if [[ "$DRY_RUN" == "false" ]] && command -v kubectl >/dev/null 2>&1; then
  desired="$("${KUBECTL[@]}" get deployment hpsf-stage-b-service -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")"
  if [[ "$desired" == "0" ]]; then
    log "hpsf-stage-b-service is DISABLED until further notice (replicas=0); skipping Stage B runtime smoke."
    exit 0
  fi
fi

topic_name() {
  printf '%s%s' "$TOPIC_PREFIX" "$1"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

produce_keyed_json() {
  local topic="$1"
  local key="$2"
  local json="$3"
  printf '%s~%s\n' "$key" "$json" | kafka-console-producer \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator='~' >/dev/null
}

print_stage_b_logs() {
  echo "Recent hpsf-stage-b-service logs:" >&2
  "${KUBECTL[@]}" logs deployment/hpsf-stage-b-service --tail=500 --all-containers=true >&2 || true
}

wait_for_stage_b_running() {
  local timeout_seconds="${HPSF_STAGE_B_RUNNING_TIMEOUT_SECONDS:-240}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    local logs
    logs="$("${KUBECTL[@]}" logs deployment/hpsf-stage-b-service --tail=500 --all-containers=true || true)"
    local latest_transition
    latest_transition="$(grep -F 'HPSF stage-b stream state transition' <<<"$logs" | tail -n 1 || true)"
    if grep -F -- '-> RUNNING' <<<"$latest_transition" >/dev/null; then
      log "Stage B Kafka Streams state is RUNNING"
      return 0
    fi
    if grep -E 'stream uncaught exception|ERROR|Exception' <<<"$logs" >/dev/null; then
      echo "Stage B logs contain an exception before RUNNING:" >&2
      echo "$logs" >&2
      exit 1
    fi
    sleep 5
  done
  echo "Stage B did not reach Kafka Streams RUNNING within ${timeout_seconds}s" >&2
  print_stage_b_logs
  exit 1
}

wait_for_stage_b_log_contains() {
  local expected="$1"
  local description="$2"
  local timeout_seconds="${HPSF_STAGE_B_LOG_TIMEOUT_SECONDS:-120}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    local logs
    logs="$("${KUBECTL[@]}" logs deployment/hpsf-stage-b-service --tail=500 --all-containers=true || true)"
    if grep -F "$expected" <<<"$logs" >/dev/null; then
      log "observed Stage B log for $description"
      return 0
    fi
    sleep 5
  done
  echo "Stage B did not log $description within ${timeout_seconds}s" >&2
  print_stage_b_logs
  exit 1
}

wait_for_stage_b_startup_logs() {
  local timeout_seconds="${HPSF_STAGE_B_STARTUP_LOG_TIMEOUT_SECONDS:-120}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    local logs
    logs="$("${KUBECTL[@]}" logs deployment/hpsf-stage-b-service --tail=500 --all-containers=true || true)"
    if grep -F "HPSF Stage B topology enabled" <<<"$logs" >/dev/null; then
      log "observed legacy Stage B topology startup log"
      return 0
    fi
    if grep -F "HPSF processing topology started; mode=LIVE_SIGNAL_ONLY" <<<"$logs" >/dev/null \
      && grep -F "HPSF Stage B active-chain store=" <<<"$logs" >/dev/null; then
      log "observed current Stage B topology startup logs"
      return 0
    fi
    sleep 5
  done
  echo "Missing Stage B startup logs for legacy or current topology startup." >&2
  echo "Expected either legacy marker 'HPSF Stage B topology enabled' or current markers:" >&2
  echo "  HPSF processing topology started; mode=LIVE_SIGNAL_ONLY" >&2
  echo "  HPSF Stage B active-chain store=" >&2
  print_stage_b_logs
  exit 1
}

start_topic_capture() {
  local topic="$1"
  local output_file="$2"
  local error_file="$3"
  local scan_seconds="${HPSF_STAGE_B_OUTPUT_SCAN_SECONDS:-120}"
  local scan_ms=$((scan_seconds * 1000))
  : >"$output_file"
  : >"$error_file"
  timeout "${scan_seconds}s" kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --timeout-ms "$scan_ms" >"$output_file" 2>"$error_file" &
  echo "$!"
}

read_expected_record() {
  local topic="$1"
  local expected="$2"
  local output_file="$3"
  local error_file="$4"
  local deadline=$((SECONDS + ${HPSF_STAGE_B_OUTPUT_SCAN_SECONDS:-120}))
  while (( SECONDS < deadline )); do
    local matches
    matches="$(grep -F "$expected" "$output_file" || true)"
    if [[ -n "$matches" ]]; then
      tail -n 1 <<<"$matches"
      return 0
    fi
    sleep 5
  done
  echo "Stage B did not emit $topic containing $expected after fixture" >&2
  if [[ -s "$error_file" ]]; then
    echo "Consumer diagnostics for $topic:" >&2
    cat "$error_file" >&2
  fi
  print_stage_b_logs
  exit 1
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry-run: would verify Stage B logs, produce fixture records, and consume new signal/latest-signal records"
  exit 0
fi

require_cmd kubectl
require_cmd kafka-console-producer
require_cmd kafka-console-consumer

log "checking Stage B rollout"
"${KUBECTL[@]}" rollout status deployment/hpsf-stage-b-service --timeout=180s

log "checking Stage B startup logs"
wait_for_stage_b_startup_logs
wait_for_stage_b_running

trade_date="$(TZ=America/New_York date +%F)"
expiry="$trade_date"
underlying_event_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
underlying_state_json="{\"schemaVersion\":2,\"eventTime\":\"$underlying_event_time\",\"tradeDate\":\"$trade_date\",\"underlying\":\"SPX\",\"spot\":6005.0,\"spotTime\":\"$underlying_event_time\",\"esSymbol\":\"ESM6\",\"esLast\":6030.0,\"esLastTime\":\"$underlying_event_time\",\"esVwap\":6030.0,\"esVwapTime\":\"$underlying_event_time\",\"rollingBasis\":25.0,\"spxEquivalentVwap\":6005.0,\"distanceToVwap\":0.0,\"vwapSource\":\"ES_FUTURES_BASIS_ADJUSTED\",\"referenceSymbol\":\"ESM6\",\"basisTime\":\"$underlying_event_time\",\"expectedMove15m\":15.0,\"momentum1m\":3.0,\"momentum5m\":5.0}"

underlying_state_topic="$(topic_name options.hpsf.underlying-state)"
strike_flow_topic="$(topic_name options.hpsf.strike-flow)"
signal_topic="$(topic_name options.hpsf.signal)"
latest_signal_topic="$(topic_name options.hpsf.latest-signal)"

log "priming Stage B with underlying-state key $trade_date|SPX on $underlying_state_topic"
produce_keyed_json "$underlying_state_topic" "$trade_date|SPX" "$underlying_state_json"
sleep "${HPSF_STAGE_B_UNDERLYING_PRIME_SECONDS:-10}"
wait_for_stage_b_running

event_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fixture_id="hpsf65-${event_time//[:TZ-]/}"
expected_eval_id="SPX-$(date -u -d "$event_time" +%Y%m%d-%H%M%S)000"
expected_signal_marker="\"tradeDate\":\"$trade_date\""
flow_json="{\"schemaVersion\":2,\"algorithmVersion\":\"HPSF_V2.1\",\"configVersion\":\"hpsf65-smoke\",\"codeGitSha\":\"$fixture_id\",\"eventTime\":\"$event_time\",\"tradeDate\":\"$trade_date\",\"underlying\":\"SPX\",\"expiry\":\"$expiry\",\"strike\":6005.0,\"optionType\":\"CALL\",\"spot\":6005.0,\"bid\":8.90,\"ask\":9.40,\"mid\":9.15,\"spread\":0.50,\"spreadPct\":0.0546,\"totalVolume1m\":100,\"askVolume1m\":80,\"bidVolume1m\":10,\"midVolume1m\":10,\"askRatio1m\":0.80,\"askPremium1m\":1000000.0,\"bidPremium1m\":0.0,\"netBuyPremium1m\":1000000.0,\"totalVolume5m\":300,\"askVolume5m\":250,\"bidVolume5m\":25,\"midVolume5m\":25,\"askRatio5m\":0.83,\"askPremium5m\":2000000.0,\"bidPremium5m\":0.0,\"netBuyPremium5m\":2000000.0,\"volumeSpeed\":4.0,\"tradeCount1m\":20,\"liquidityOk\":true,\"candidateDistanceOk\":true}"

tmp_dir="$(mktemp -d)"
signal_output_file="$tmp_dir/signal.out"
signal_error_file="$tmp_dir/signal.err"
latest_output_file="$tmp_dir/latest.out"
latest_error_file="$tmp_dir/latest.err"
signal_capture_pid=""
latest_capture_pid=""

cleanup() {
  if [[ -n "$signal_capture_pid" || -n "$latest_capture_pid" ]]; then
    kill "$signal_capture_pid" "$latest_capture_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

log "producing deterministic Stage B fixture $fixture_id expecting live signal marker $expected_signal_marker; fixture event id would be $expected_eval_id"
signal_capture_pid="$(start_topic_capture "$signal_topic" "$signal_output_file" "$signal_error_file")"
latest_capture_pid="$(start_topic_capture "$latest_signal_topic" "$latest_output_file" "$latest_error_file")"
sleep "${HPSF_STAGE_B_CONSUMER_READY_SECONDS:-3}"
produce_keyed_json "$strike_flow_topic" "$trade_date|$expiry|6005|CALL" "$flow_json"

signal_output="$(read_expected_record "$signal_topic" "$expected_signal_marker" "$signal_output_file" "$signal_error_file")"
latest_output="$(read_expected_record "$latest_signal_topic" "$expected_signal_marker" "$latest_output_file" "$latest_error_file")"

if [[ -z "$signal_output" ]]; then
  echo "Stage B did not emit $signal_topic after fixture" >&2
  exit 1
fi
if [[ -z "$latest_output" ]]; then
  echo "Stage B did not emit $latest_signal_topic after fixture" >&2
  exit 1
fi
if grep -F '"enabled":true' <<<"$signal_output$latest_output" >/dev/null; then
  echo "Forbidden orderInstruction enabled true found in Stage B smoke output" >&2
  exit 1
fi

grep -F '"evaluationId"' <<<"$signal_output" >/dev/null
grep -F "\"expiry\":\"$expiry\"" <<<"$signal_output" >/dev/null
grep -F "\"expiry\":\"$expiry\"" <<<"$latest_output" >/dev/null
grep -F '"orderInstruction"' <<<"$signal_output" >/dev/null
log "Stage B signal output: $signal_output"
log "Stage B latest-signal output: $latest_output"
log "Stage B runtime smoke passed"
