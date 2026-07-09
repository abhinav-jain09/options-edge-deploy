#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECONFIG="${KUBECONFIG:-/home/options-edge/config/kubeconfig}"
REMOTE_APP_HOME="${REMOTE_APP_HOME:-/home/options-edge}"
PROMETHEUS_CONFIG_FILE="${PROMETHEUS_CONFIG_FILE:-/etc/prometheus/prometheus.yml}"
PROMETHEUS_SERVICE="${PROMETHEUS_SERVICE:-prometheus}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9090}"
BEGIN_MARKER="# BEGIN OPTIONS_EDGE_MANAGED_SCRAPES"
END_MARKER="# END OPTIONS_EDGE_MANAGED_SCRAPES"

if [[ "$REMOTE_APP_HOME" != "/home/options-edge" ]]; then
  echo "REMOTE_APP_HOME must be /home/options-edge" >&2
  exit 1
fi

# Prometheus scrape wiring is optional observability config and is applied where
# Prometheus actually runs. When this stage runs on an agent without a local
# Prometheus config (e.g. the Mac deploy agent), skip gracefully instead of
# failing the whole deploy — the workloads are already rolled out by this point.
if [[ ! -f "$PROMETHEUS_CONFIG_FILE" ]]; then
  echo "Prometheus config $PROMETHEUS_CONFIG_FILE not present on this agent; skipping scrape configuration (non-fatal)."
  exit 0
fi

tmp_root="${JENKINS_WORK_DIR:-$REMOTE_APP_HOME/tmp}"
mkdir -p "$tmp_root"
work_dir="$(mktemp -d "$tmp_root/prometheus-scrapes.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

base_config="$work_dir/prometheus.base.yml"
managed_block="$work_dir/options-edge-scrapes.yml"
next_config="$work_dir/prometheus.next.yml"

kubectl_cmd=(kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE")

cat >"$managed_block" <<EOF
$BEGIN_MARKER
EOF

add_service_scrape() {
  local service_name="$1"
  local service_port="$2"
  local job_name
  local cluster_ip
  if [[ "$service_name" == options-edge-* ]]; then
    job_name="$service_name"
  else
    job_name="options-edge-${service_name}"
  fi
  # --ignore-not-found makes a genuinely-absent service return empty output with
  # exit 0, while a real lookup failure (API down, bad kubeconfig, RBAC) still
  # exits non-zero. We must distinguish the two: skipping an absent service is
  # fine, but treating a transient kubectl failure as "no services" would render
  # an empty managed block and wipe the live scrape config on install. Abort
  # instead so the existing prometheus.yml is left untouched.
  if ! cluster_ip="$("${kubectl_cmd[@]}" get service "$service_name" --ignore-not-found -o jsonpath='{.spec.clusterIP}')"; then
    echo "kubectl lookup failed for $service_name — aborting without touching Prometheus config" >&2
    exit 1
  fi

  # Services toggle on/off across environments (some are pinned to 0 replicas or
  # removed entirely — see the disabled-services list). A genuinely absent or
  # headless service must not fail the whole observability stage: skip it.
  if [[ -z "$cluster_ip" || "$cluster_ip" == "None" ]]; then
    echo "Skipping scrape for $service_name: no ClusterIP (service absent or headless)." >&2
    return 0
  fi

  cat >>"$managed_block" <<EOF
  - job_name: $job_name
    scrape_interval: 5s
    scrape_timeout: 5s
    metrics_path: /metrics
    # Some services' /metrics handlers omit a Content-Type header; Prometheus 3.x
    # rejects those unless a fallback wire format is declared. Compliant targets
    # (which send a proper Content-Type) ignore this.
    fallback_scrape_protocol: PrometheusText0.0.4
    static_configs:
      - targets:
          - ${cluster_ip}:${service_port}
        labels:
          namespace: $NAMESPACE
          service: $service_name

EOF
}

# Every deployed service that exposes an /metrics HTTP endpoint (app.kafka.ProcessingMetrics).
# add_service_scrape skips gracefully if a service is absent, so entries for services that are
# currently disabled/removed (mission-pressure, volume-pace, volume-sandwich, unusual-whales-gex)
# are harmless — they light up automatically if the service comes back.
add_service_scrape raw-to-display-service 8080
add_service_scrape options-edge-databento-feed 8010
add_service_scrape databento-volume-aggregator 8080
add_service_scrape option-price-behavior-service 8080
add_service_scrape databento-mission-pressure-service 8098
add_service_scrape databento-mission-sandwich-service 8099
add_service_scrape volume-pace-service 8080
add_service_scrape directional-pressure-service 8080
add_service_scrape volume-sandwich-service 8080
add_service_scrape unusual-whales-gex-service 8080
add_service_scrape raw-postgres-writer 8080
add_service_scrape pressure-postgres-writer 8080
add_service_scrape hpsf-postgres-writer-service 8080
add_service_scrape spx-mission-control-service 8096
add_service_scrape delta-flow-service 8110
add_service_scrape dealer-ledger-service 8113
add_service_scrape strike-liquidity-heatmap-service 8111
add_service_scrape ibkr-feed-service 8080
add_service_scrape feed-gateway-service 8091
add_service_scrape options-edge-integration-test 8080
# Added 2026-07-09 — remaining services that expose /metrics (verified serving options_edge_* metrics).
add_service_scrape databento-gex-service 8100
add_service_scrape databento-gex-history-service 8080
add_service_scrape gex-delta-redis-writer 8101
add_service_scrape strike-flow-avro-adapter 8109
add_service_scrape strike-flow-classifier-databento 8097
add_service_scrape strike-intelligence-service 8114
add_service_scrape unified-sr-service 8108

cat >>"$managed_block" <<EOF
$END_MARKER
EOF

if ! grep -q '^scrape_configs:' "$PROMETHEUS_CONFIG_FILE"; then
  echo "$PROMETHEUS_CONFIG_FILE does not contain scrape_configs" >&2
  exit 1
fi

# Guard: never install an empty managed block. Even though a failed kubectl
# lookup already aborts above, refuse to write a config that would drop every
# options-edge scrape — a real deploy always produces at least one job.
generated_jobs="$(grep -c '^  - job_name: ' "$managed_block" || true)"
if [[ "$generated_jobs" -eq 0 ]]; then
  echo "Managed scrape block is empty — refusing to overwrite $PROMETHEUS_CONFIG_FILE" >&2
  exit 1
fi

awk -v begin="$BEGIN_MARKER" -v end="$END_MARKER" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  skip != 1 { print }
' "$PROMETHEUS_CONFIG_FILE" >"$base_config"

cat "$base_config" "$managed_block" >"$next_config"
promtool check config "$next_config"

install_config() {
  cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  systemctl restart "$PROMETHEUS_SERVICE"
}

if [[ -w "$PROMETHEUS_CONFIG_FILE" ]]; then
  install_config
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  sudo cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  sudo systemctl restart "$PROMETHEUS_SERVICE"
elif [[ -n "${BECOME_PASSWORD:-}" ]]; then
  install_script="$work_dir/install-prometheus-config.sh"
  cat >"$install_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
systemctl restart "$PROMETHEUS_SERVICE"
EOF
  chmod 700 "$install_script"
  printf '%s\n' "$BECOME_PASSWORD" | su -c "$install_script" root
else
  echo "Cannot write $PROMETHEUS_CONFIG_FILE; provide sudo or BECOME_PASSWORD" >&2
  exit 1
fi

prometheus_query_is_one() {
  local query="$1"
  local body
  if ! body="$(curl -fsS --get "$PROMETHEUS_URL/api/v1/query" --data-urlencode "query=$query" 2>/dev/null)"; then
    return 1
  fi
  python3 -c 'import json, sys; data = json.load(sys.stdin); sys.exit(0 if any(str(row.get("value", ["", "0"])[1]) == "1" for row in data.get("data", {}).get("result", [])) else 1)' <<<"$body" 2>/dev/null
}

# Verify only the jobs we actually emitted (services that resolved to a ClusterIP).
# Individual services can legitimately be down (starting, pinned to 0, market-gated
# like the integration test), so a down scrape is a warning — the stage fails only
# if the managed block produced nothing or Prometheus reloaded none of it.
mapfile -t managed_jobs < <(awk '/^  - job_name: /{print $3}' "$managed_block")
up_count=0
for job_name in "${managed_jobs[@]}"; do
  query="up{job=\"${job_name}\"}"
  ok=false
  for _ in $(seq 1 20); do
    if prometheus_query_is_one "$query"; then ok=true; break; fi
    sleep 3
  done
  if [[ "$ok" == true ]]; then
    up_count=$((up_count + 1))
    echo "Prometheus scrape is up for $job_name"
  else
    echo "WARNING: Prometheus scrape not up for $job_name (service down or not yet ready)." >&2
  fi
done

if [[ "${#managed_jobs[@]}" -eq 0 ]]; then
  echo "No managed scrape jobs were generated" >&2
  exit 1
fi
if [[ "$up_count" -eq 0 ]]; then
  echo "None of the ${#managed_jobs[@]} managed scrapes became up — Prometheus reload likely failed" >&2
  exit 1
fi
echo "Prometheus scrapes up: ${up_count}/${#managed_jobs[@]}"
