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

mkdir -p "$REMOTE_APP_HOME/tmp"
work_dir="$(mktemp -d "$REMOTE_APP_HOME/tmp/prometheus-scrapes.XXXXXX")"
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
  cluster_ip="$("${kubectl_cmd[@]}" get service "$service_name" -o jsonpath='{.spec.clusterIP}')"

  if [[ -z "$cluster_ip" || "$cluster_ip" == "None" ]]; then
    echo "Service $service_name does not have a ClusterIP" >&2
    exit 1
  fi

  cat >>"$managed_block" <<EOF
  - job_name: $job_name
    scrape_interval: 5s
    scrape_timeout: 5s
    metrics_path: /metrics
    static_configs:
      - targets:
          - ${cluster_ip}:${service_port}
        labels:
          namespace: $NAMESPACE
          service: $service_name

EOF
}

add_service_scrape raw-to-display-service 8080
add_service_scrape databento-volume-aggregator 8080
add_service_scrape volume-pace-service 8080
add_service_scrape directional-pressure-service 8080
add_service_scrape volume-sandwich-service 8080
add_service_scrape unusual-whales-gex-service 8080
add_service_scrape raw-postgres-writer 8080
add_service_scrape pressure-postgres-writer 8080
add_service_scrape hpsf-postgres-writer-service 8080
add_service_scrape spx-mission-control-service 8096
add_service_scrape ibkr-feed-service 8080
add_service_scrape feed-gateway-service 8091
add_service_scrape options-edge-integration-test 8080

cat >>"$managed_block" <<EOF
$END_MARKER
EOF

if ! grep -q '^scrape_configs:' "$PROMETHEUS_CONFIG_FILE"; then
  echo "$PROMETHEUS_CONFIG_FILE does not contain scrape_configs" >&2
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

for service_name in raw-to-display-service databento-volume-aggregator volume-pace-service directional-pressure-service volume-sandwich-service unusual-whales-gex-service raw-postgres-writer pressure-postgres-writer hpsf-postgres-writer-service spx-mission-control-service ibkr-feed-service feed-gateway-service options-edge-integration-test; do
  if [[ "$service_name" == options-edge-* ]]; then
    job_name="$service_name"
  else
    job_name="options-edge-${service_name}"
  fi
  query="up{job=\"${job_name}\"}"
  for _ in $(seq 1 20); do
    if prometheus_query_is_one "$query"; then
      echo "Prometheus scrape is up for $service_name"
      break
    fi
    sleep 3
  done

  if ! prometheus_query_is_one "$query"; then
    echo "Prometheus scrape did not become up for $service_name" >&2
    exit 1
  fi
done
