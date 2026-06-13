#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOURCE_ROOT="${HPSF_SOURCE_ROOT:-$ROOT_DIR/../options-edge}"
NAMESPACE="${NAMESPACE:-options-edge}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
SKIP_REMOTE="${SKIP_REMOTE:-false}"
DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

KUBECTL=(kubectl -n "$NAMESPACE")
if [[ -n "${KUBECONFIG:-}" ]]; then
  KUBECTL=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")
fi

log() { printf '[hpsf-release-gate] %s\n' "$*"; }
require_file_contains() {
  local file="$1"
  local pattern="$2"
  if ! grep -Fq "$pattern" "$file"; then
    echo "Missing required release-gate pattern '$pattern' in $file" >&2
    exit 1
  fi
}
require_not_contains() {
  local file="$1"
  local pattern="$2"
  if grep -Fq "$pattern" "$file"; then
    echo "Forbidden release-gate pattern '$pattern' found in $file" >&2
    exit 1
  fi
}

static_gate() {
  log "checking release documentation and source gates"
  for file in \
    "$SOURCE_ROOT/hpsf.md" \
    "$SOURCE_ROOT/optionedge_hpsf_v2_expected_output_contract.md" \
    "$SOURCE_ROOT/codex_feedback_databento_to_hpsf_field_mapping.md" \
    "$SOURCE_ROOT/optionedge_codex_hpsf_v2_vwap_reclaim_prompt.md" \
    "$ROOT_DIR/docs/hpsf-v2-release-notes.md" \
    "$ROOT_DIR/docs/hpsf-v2-runbook.md"; do
    if [[ ! -f "$file" ]]; then
      echo "Required HPSF release source file is missing: $file" >&2
      exit 1
    fi
  done
  require_file_contains "$ROOT_DIR/docs/hpsf-v2-release-notes.md" "RF=1 has no broker-failure durability"
  require_file_contains "$ROOT_DIR/docs/hpsf-v2-release-notes.md" "HPSF V2.1 is signal-only"
  require_file_contains "$ROOT_DIR/docs/hpsf-v2-release-notes.md" "SHADOW mode"
  require_file_contains "$ROOT_DIR/docs/hpsf-v2-release-notes.md" "Databento"
  require_file_contains "$ROOT_DIR/docs/hpsf-v2-runbook.md" "HPSF must never place orders"
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_TOPIC_LATEST_SIGNAL: options.hpsf.latest-signal"
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_ORDER_PLACEMENT_ENABLED: \"false\""
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_STREAMS_REPLICATION_FACTOR: \"1\""
  require_file_contains "$ROOT_DIR/k8s/base/configmap.yaml" "HPSF_STREAMS_NUM_STANDBY_REPLICAS: \"0\""
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_STAGE_A_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_STAGE_B_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "HPSF_TOPOLOGY_ENABLED"
  require_file_contains "$ROOT_DIR/k8s/base/hpsf-stage-b-deployment.yaml" "value: \"true\""
  require_file_contains "$ROOT_DIR/scripts/monitoring/hpsf-alert-rules.yaml" "HpsfOpraLagTooHigh"
  require_file_contains "$ROOT_DIR/scripts/smoke/check-hpsf-deployment.sh" "options.hpsf.latest-signal"

  for file in \
    "$ROOT_DIR/k8s/base/configmap.yaml" \
    "$ROOT_DIR/config/hpsf.env.example" \
    "$ROOT_DIR/docs/hpsf-v2-release-notes.md" \
    "$ROOT_DIR/docs/hpsf-v2-runbook.md"; do
    require_not_contains "$file" '"enabled":true'
    require_not_contains "$file" 'orderInstruction.enabled=true'
  done
}

remote_gate() {
  if [[ "$DRY_RUN" == "true" || "$SKIP_REMOTE" == "true" ]]; then
    log "skipping remote checks"
    return 0
  fi
  log "verifying remote HPSF deployment smoke"
  KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" "$ROOT_DIR/scripts/smoke/check-hpsf-deployment.sh"

  if command -v kubectl >/dev/null 2>&1; then
    log "verifying HPSF pods are present"
    "${KUBECTL[@]}" get deployment hpsf-stage-a-service hpsf-stage-b-service hpsf-postgres-writer-service
  fi
}

static_gate
remote_gate
log "HPSF release gate passed"
