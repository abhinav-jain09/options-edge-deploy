#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECONFIG="${KUBECONFIG:-}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "$KUBECONFIG" ]]; then
  KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")
fi

log() { printf '[hpsf-stage-b-smoke] %s\n' "$*"; }

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

read_expected_record() {
  local topic="$1"
  local expected="$2"
  local output_file="$4"
  local error_file="$5"
  local deadline=$((SECONDS + ${HPSF_STAGE_B_OUTPUT_SCAN_SECONDS:-120}))
  while (( SECONDS < deadline )); do
    : >"$output_file"
    : >"$error_file"
    kafka-console-consumer \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$topic" \
      --from-beginning \
      --max-messages "${HPSF_STAGE_B_OUTPUT_SCAN_MAX_MESSAGES:-200}" \
      --timeout-ms 10000 >"$output_file" 2>"$error_file" || true
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
stage_b_logs="$(${KUBECTL[@]} logs deployment/hpsf-stage-b-service --tail=400 --all-containers=true || true)"
for pattern in \
  "HPSF Stage B topology enabled" \
  "HPSF Stage B consuming options.hpsf.strike-flow" \
  "HPSF Stage B producing options.hpsf.signal" \
  "HPSF Stage B producing options.hpsf.latest-signal" \
  "HPSF Stage B producing options.hpsf.audit"; do
  if ! grep -F "$pattern" <<<"$stage_b_logs" >/dev/null; then
    echo "Missing Stage B startup log: $pattern" >&2
    echo "Recent hpsf-stage-b-service logs:" >&2
    echo "$stage_b_logs" >&2
    exit 1
  fi
done
wait_for_stage_b_running

trade_date="$(TZ=America/New_York date +%F)"
expiry="$trade_date"
underlying_event_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

spx_json="{\"schemaVersion\":1,\"symbol\":\"SPX\",\"eventTime\":\"$underlying_event_time\",\"price\":6005.0,\"size\":null,\"source\":\"SYNTHETIC_OPTION_SPOT\",\"quality\":\"LIVE\"}"
es_json="{\"schemaVersion\":1,\"source\":\"DATABENTO\",\"dataset\":\"GLBX.MDP3\",\"sourceSchema\":\"trades\",\"symbol\":\"ES.v.0\",\"eventTime\":\"$underlying_event_time\",\"receiveTime\":\"$underlying_event_time\",\"sessionDate\":\"$trade_date\",\"instrumentId\":\"42140864\",\"price\":6030.0,\"size\":10,\"side\":\"B\",\"flags\":0,\"sequence\":1}"

log "priming Stage B underlying state with SPX spot and ES trade"
produce_keyed_json underlying.spx.price SPX "$spx_json"
produce_keyed_json underlying.es.trades "ES.v.0|$trade_date|1" "$es_json"
sleep "${HPSF_STAGE_B_UNDERLYING_PRIME_SECONDS:-10}"
wait_for_stage_b_log_contains "HPSF Stage B received ES trade chainKey=$trade_date|$expiry eventTime=$underlying_event_time" "ES underlying state $underlying_event_time"
wait_for_stage_b_running

event_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fixture_id="hpsf65-${event_time//[:TZ-]/}"
expected_eval_id="SPX-$(date -u -d "$event_time" +%Y%m%d-%H%M%S)000"
flow_json="{\"schemaVersion\":2,\"algorithmVersion\":\"HPSF_V2.1\",\"configVersion\":\"hpsf65-smoke\",\"codeGitSha\":\"$fixture_id\",\"eventTime\":\"$event_time\",\"tradeDate\":\"$trade_date\",\"underlying\":\"SPX\",\"expiry\":\"$expiry\",\"strike\":6005.0,\"optionType\":\"CALL\",\"spot\":6005.0,\"bid\":8.90,\"ask\":9.40,\"mid\":9.15,\"spread\":0.50,\"spreadPct\":0.0546,\"totalVolume1m\":100,\"askVolume1m\":80,\"bidVolume1m\":10,\"midVolume1m\":10,\"askRatio1m\":0.80,\"askPremium1m\":1000000.0,\"bidPremium1m\":0.0,\"netBuyPremium1m\":1000000.0,\"totalVolume5m\":300,\"askVolume5m\":250,\"bidVolume5m\":25,\"midVolume5m\":25,\"askRatio5m\":0.83,\"askPremium5m\":2000000.0,\"bidPremium5m\":0.0,\"netBuyPremium5m\":2000000.0,\"volumeSpeed\":4.0,\"tradeCount1m\":20,\"liquidityOk\":true,\"candidateDistanceOk\":true}"

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

signal_output_file="$tmp_dir/signal.out"
signal_error_file="$tmp_dir/signal.err"
latest_output_file="$tmp_dir/latest.out"
latest_error_file="$tmp_dir/latest.err"

log "producing deterministic Stage B fixture $fixture_id expecting $expected_eval_id"
produce_keyed_json options.hpsf.strike-flow "$trade_date|$expiry|6005|CALL" "$flow_json"
wait_for_stage_b_log_contains "HPSF Stage B emitting options.hpsf.signal evaluationId=$expected_eval_id" "signal emission $expected_eval_id"

signal_output="$(read_expected_record options.hpsf.signal "$expected_eval_id" "$signal_output_file" "$signal_error_file")"
latest_output="$(read_expected_record options.hpsf.latest-signal "$expected_eval_id" "$latest_output_file" "$latest_error_file")"

if [[ -z "$signal_output" ]]; then
  echo "Stage B did not emit options.hpsf.signal after fixture" >&2
  exit 1
fi
if [[ -z "$latest_output" ]]; then
  echo "Stage B did not emit options.hpsf.latest-signal after fixture" >&2
  exit 1
fi
if grep -F '"enabled":true' <<<"$signal_output$latest_output" >/dev/null; then
  echo "Forbidden orderInstruction enabled true found in Stage B smoke output" >&2
  exit 1
fi

grep -F '"evaluationId"' <<<"$signal_output" >/dev/null
grep -F '"orderInstruction"' <<<"$signal_output" >/dev/null
log "Stage B signal output: $signal_output"
log "Stage B latest-signal output: $latest_output"
log "Stage B runtime smoke passed"
