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

latest_offset() {
  local topic="$1"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list "$KAFKA_BOOTSTRAP_SERVERS" --topic "$topic" --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
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

consume_from_offset() {
  local topic="$1"
  local offset="$2"
  kafka-console-consumer \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --partition 0 \
    --offset "$offset" \
    --max-messages 1 \
    --timeout-ms 30000 2>/dev/null
}

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry-run: would verify Stage B logs, produce fixture records, and consume new signal/latest-signal records"
  exit 0
fi

require_cmd kubectl
require_cmd kafka-console-producer
require_cmd kafka-console-consumer
require_cmd kafka-run-class

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

signal_offset_before="$(latest_offset options.hpsf.signal)"
latest_offset_before="$(latest_offset options.hpsf.latest-signal)"
log "signal offset before fixture: $signal_offset_before"
log "latest-signal offset before fixture: $latest_offset_before"

trade_date="$(TZ=America/New_York date +%F)"
event_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fixture_id="hpsf65-${event_time//[:TZ-]/}"
expiry="$trade_date"

spx_json="{\"schemaVersion\":1,\"symbol\":\"SPX\",\"eventTime\":\"$event_time\",\"price\":6005.0,\"size\":null,\"source\":\"SYNTHETIC_OPTION_SPOT\",\"quality\":\"LIVE\"}"
es_json="{\"schemaVersion\":1,\"source\":\"DATABENTO\",\"dataset\":\"GLBX.MDP3\",\"sourceSchema\":\"trades\",\"symbol\":\"ES.v.0\",\"eventTime\":\"$event_time\",\"receiveTime\":\"$event_time\",\"sessionDate\":\"$trade_date\",\"instrumentId\":\"42140864\",\"price\":6030.0,\"size\":10,\"side\":\"B\",\"flags\":0,\"sequence\":1}"
flow_json="{\"schemaVersion\":2,\"algorithmVersion\":\"HPSF_V2.1\",\"configVersion\":\"hpsf65-smoke\",\"codeGitSha\":\"$fixture_id\",\"eventTime\":\"$event_time\",\"tradeDate\":\"$trade_date\",\"underlying\":\"SPX\",\"expiry\":\"$expiry\",\"strike\":6005.0,\"optionType\":\"CALL\",\"spot\":6005.0,\"bid\":8.90,\"ask\":9.40,\"mid\":9.15,\"spread\":0.50,\"spreadPct\":0.0546,\"totalVolume1m\":100,\"askVolume1m\":80,\"bidVolume1m\":10,\"midVolume1m\":10,\"askRatio1m\":0.80,\"askPremium1m\":1000000.0,\"bidPremium1m\":0.0,\"netBuyPremium1m\":1000000.0,\"totalVolume5m\":300,\"askVolume5m\":250,\"bidVolume5m\":25,\"midVolume5m\":25,\"askRatio5m\":0.83,\"askPremium5m\":2000000.0,\"bidPremium5m\":0.0,\"netBuyPremium5m\":2000000.0,\"volumeSpeed\":4.0,\"tradeCount1m\":20,\"liquidityOk\":true,\"candidateDistanceOk\":true}"

log "producing deterministic Stage B fixture $fixture_id"
produce_keyed_json underlying.spx.price SPX "$spx_json"
produce_keyed_json underlying.es.trades "ES.v.0|$trade_date|1" "$es_json"
produce_keyed_json options.hpsf.strike-flow "$trade_date|$expiry|6005|CALL" "$flow_json"

sleep 10

signal_output="$(consume_from_offset options.hpsf.signal "$signal_offset_before")"
latest_output="$(consume_from_offset options.hpsf.latest-signal "$latest_offset_before")"

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
