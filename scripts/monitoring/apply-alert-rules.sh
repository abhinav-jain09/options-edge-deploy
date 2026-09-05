#!/usr/bin/env bash
set -euo pipefail

# Installs the Prometheus alerting rules (scripts/monitoring/hpsf-alert-rules.yaml) onto the host
# where Prometheus runs, registers them in prometheus.yml `rule_files`, validates with promtool, and
# reloads. Companion to apply-prometheus-scrapes.sh and follows the same conventions (env, become,
# graceful-skip-when-no-local-Prometheus). Idempotent: re-running replaces the managed block only.

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECONFIG="${KUBECONFIG:-/home/options-edge/config/kubeconfig}"
REMOTE_APP_HOME="${REMOTE_APP_HOME:-/home/options-edge}"
PROMETHEUS_CONFIG_FILE="${PROMETHEUS_CONFIG_FILE:-/etc/prometheus/prometheus.yml}"
PROMETHEUS_SERVICE="${PROMETHEUS_SERVICE:-prometheus}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9090}"
# Destination the rule file is installed to (referenced from prometheus.yml rule_files).
PROMETHEUS_RULES_FILE="${PROMETHEUS_RULES_FILE:-/etc/prometheus/hpsf-alert-rules.yaml}"
# U16: the CVD SPX levels paging rules install alongside, into the same managed rule_files block.
PROMETHEUS_CVD_LEVELS_RULES_FILE="${PROMETHEUS_CVD_LEVELS_RULES_FILE:-/etc/prometheus/cvd-spx-levels-alerts.yaml}"
BEGIN_MARKER="# BEGIN OPTIONS_EDGE_MANAGED_RULE_FILES"
END_MARKER="# END OPTIONS_EDGE_MANAGED_RULE_FILES"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_RULES_FILE="${SOURCE_RULES_FILE:-$SCRIPT_DIR/hpsf-alert-rules.yaml}"
SOURCE_CVD_LEVELS_RULES_FILE="${SOURCE_CVD_LEVELS_RULES_FILE:-$SCRIPT_DIR/cvd-spx-levels-alerts.yaml}"

if [[ "$REMOTE_APP_HOME" != "/home/options-edge" ]]; then
  echo "REMOTE_APP_HOME must be /home/options-edge" >&2
  exit 1
fi

if [[ ! -f "$SOURCE_RULES_FILE" ]]; then
  echo "Source rules file $SOURCE_RULES_FILE not found" >&2
  exit 1
fi
if [[ ! -f "$SOURCE_CVD_LEVELS_RULES_FILE" ]]; then
  echo "Source rules file $SOURCE_CVD_LEVELS_RULES_FILE not found" >&2
  exit 1
fi

# Alerting rules are optional observability config, applied where Prometheus actually runs. On an agent
# without a local Prometheus config (e.g. the Mac deploy agent) we skip — but skipping means NO alert
# rules are installed anywhere, so a release that claims monitoring coverage must be able to REQUIRE the
# apply. ALLOW_ALERT_RULE_SKIP defaults true (dev/CI agents without local Prometheus); set it false in a
# production/release context to turn a missing Prometheus config into a hard failure instead of a silent
# no-op.
ALLOW_ALERT_RULE_SKIP="${ALLOW_ALERT_RULE_SKIP:-true}"
if [[ ! -f "$PROMETHEUS_CONFIG_FILE" ]]; then
  if [[ "$ALLOW_ALERT_RULE_SKIP" == "true" ]]; then
    echo "Prometheus config $PROMETHEUS_CONFIG_FILE not present on this agent; skipping alert-rule configuration (ALLOW_ALERT_RULE_SKIP=true, non-fatal)."
    exit 0
  fi
  echo "Prometheus config $PROMETHEUS_CONFIG_FILE not present and ALLOW_ALERT_RULE_SKIP=false — refusing to report success without installing alert rules. Point this at the Prometheus host, or set ALLOW_ALERT_RULE_SKIP=true to skip explicitly." >&2
  exit 1
fi

if ! command -v promtool >/dev/null 2>&1; then
  echo "promtool not found on PATH; cannot validate rules" >&2
  exit 1
fi

# Validate the rules BEFORE touching anything on the host.
promtool check rules "$SOURCE_RULES_FILE"
promtool check rules "$SOURCE_CVD_LEVELS_RULES_FILE"

tmp_root="${JENKINS_WORK_DIR:-$REMOTE_APP_HOME/tmp}"
mkdir -p "$tmp_root"
work_dir="$(mktemp -d "$tmp_root/prometheus-rules.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

base_config="$work_dir/prometheus.base.yml"
next_config="$work_dir/prometheus.next.yml"

# Strip any previous managed rule_files block, then append a fresh one that references the rule file.
awk -v begin="$BEGIN_MARKER" -v end="$END_MARKER" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  skip != 1 { print }
' "$PROMETHEUS_CONFIG_FILE" >"$base_config"

if grep -qE '^[[:space:]]*rule_files:[[:space:]]*\[' "$base_config"; then
  # INLINE flow-style rule_files (e.g. `rule_files: ["a.yml"]`). Our marker-block insertion only
  # supports the block/standalone form; blindly appending a second top-level `rule_files:` would
  # produce a duplicate key that Prometheus rejects (or silently drop our managed block). Fail clearly
  # rather than emit invalid/partial config — convert prometheus.yml to the block form, or install yq.
  echo "prometheus.yml uses inline flow-style 'rule_files: [...]' which this script cannot safely merge." >&2
  echo "Convert it to block form:" >&2
  echo "  rule_files:" >&2
  echo "    - existing-rule.yml" >&2
  echo "then re-run. (Or extend this script with a YAML-aware tool such as yq.)" >&2
  exit 1
elif grep -qE '^[[:space:]]*rule_files:[[:space:]]*$' "$base_config"; then
  # Block/standalone `rule_files:` header — append our managed entry right after it.
  awk -v begin="$BEGIN_MARKER" -v end="$END_MARKER" -v path="$PROMETHEUS_RULES_FILE" -v cvdpath="$PROMETHEUS_CVD_LEVELS_RULES_FILE" '
    { print }
    $0 ~ /^[[:space:]]*rule_files:[[:space:]]*$/ && !done {
      print begin
      print "  - " path
      print "  - " cvdpath
      print end
      done = 1
    }
  ' "$base_config" >"$next_config"
else
  # No rule_files key yet — add the whole section (managed) at the end of the file.
  cp "$base_config" "$next_config"
  {
    echo "rule_files:"
    echo "$BEGIN_MARKER"
    echo "  - $PROMETHEUS_RULES_FILE"
    echo "  - $PROMETHEUS_CVD_LEVELS_RULES_FILE"
    echo "$END_MARKER"
  } >>"$next_config"
fi

# Guard against silently emitting a config that does NOT reference our rule file (e.g. an unexpected
# rule_files shape slipped past the checks above). The managed block MUST be present now.
if ! grep -qF "$BEGIN_MARKER" "$next_config" || ! grep -qF "$PROMETHEUS_RULES_FILE" "$next_config" \
   || ! grep -qF "$PROMETHEUS_CVD_LEVELS_RULES_FILE" "$next_config"; then
  echo "Refusing to install: the managed rule_files block was not inserted into the rendered prometheus.yml (unsupported existing rule_files shape)." >&2
  exit 1
fi

install_rules_and_config() {
  cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
  cp "$SOURCE_CVD_LEVELS_RULES_FILE" "$PROMETHEUS_CVD_LEVELS_RULES_FILE"
  cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  promtool check config "$PROMETHEUS_CONFIG_FILE"
  systemctl reload "$PROMETHEUS_SERVICE" 2>/dev/null || systemctl restart "$PROMETHEUS_SERVICE"
}

if [[ -w "$PROMETHEUS_CONFIG_FILE" && -w "$(dirname "$PROMETHEUS_RULES_FILE")" ]]; then
  install_rules_and_config
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  sudo cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
  sudo cp "$SOURCE_CVD_LEVELS_RULES_FILE" "$PROMETHEUS_CVD_LEVELS_RULES_FILE"
  sudo cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  sudo promtool check config "$PROMETHEUS_CONFIG_FILE"
  sudo systemctl reload "$PROMETHEUS_SERVICE" 2>/dev/null || sudo systemctl restart "$PROMETHEUS_SERVICE"
elif [[ -n "${BECOME_PASSWORD:-}" ]]; then
  install_script="$work_dir/install-prometheus-rules.sh"
  cat >"$install_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
cp "$SOURCE_CVD_LEVELS_RULES_FILE" "$PROMETHEUS_CVD_LEVELS_RULES_FILE"
cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
promtool check config "$PROMETHEUS_CONFIG_FILE"
systemctl reload "$PROMETHEUS_SERVICE" 2>/dev/null || systemctl restart "$PROMETHEUS_SERVICE"
EOF
  chmod 700 "$install_script"
  printf '%s\n' "$BECOME_PASSWORD" | su -c "$install_script" root
else
  echo "Cannot write $PROMETHEUS_CONFIG_FILE / $PROMETHEUS_RULES_FILE; provide sudo or BECOME_PASSWORD" >&2
  exit 1
fi

# Verify Prometheus actually loaded the delta-flow rule groups.
loaded=false
for _ in $(seq 1 20); do
  if body="$(curl -fsS "$PROMETHEUS_URL/api/v1/rules" 2>/dev/null)" \
     && printf '%s' "$body" | grep -q 'delta-flow.health' \
     && printf '%s' "$body" | grep -q '"cvd-spx-levels"'; then
    echo "Prometheus loaded delta-flow and cvd-spx-levels alert rules"
    loaded=true
    break
  fi
  sleep 3
done
if [[ "$loaded" != "true" ]]; then
  echo "Prometheus did not report both the delta-flow.health and cvd-spx-levels rule groups after reload" >&2
  exit 1
fi

# Scrape smoke check: the rules reference metrics under job="options-edge-delta-flow-service". Confirm
# Prometheus is actually SCRAPING that job and the metric family exists — otherwise the rules are dead
# (they reference series that never appear). This is a WARNING, not a failure: off-hours the service may
# be scaled to 0, so absence here is not necessarily a config error, but it must be visible in the log.
scrape_series_count() {
  local q="$1" body
  body="$(curl -fsS --get "$PROMETHEUS_URL/api/v1/query" --data-urlencode "query=$q" 2>/dev/null)" || return 1
  # count elements in data.result[] without needing jq
  printf '%s' "$body" | grep -o '"metric"' | wc -l | tr -d ' '
}
up_n="$(scrape_series_count 'up{job="options-edge-delta-flow-service"}' || echo 0)"
metric_n="$(scrape_series_count 'delta_flow_kafka_streams_state{job="options-edge-delta-flow-service"}' || echo 0)"
if [[ "${up_n:-0}" -ge 1 && "${metric_n:-0}" -ge 1 ]]; then
  echo "Scrape smoke OK: delta-flow target up ($up_n) and delta_flow_* metrics present ($metric_n)."
else
  echo "WARNING: delta-flow rules loaded, but Prometheus scrape for job=options-edge-delta-flow-service looks absent (up=$up_n, delta_flow_kafka_streams_state=$metric_n). If the service is running, check the scrape config (apply-prometheus-scrapes.sh) and the job label — the alerts are inert until the target is scraped." >&2
fi
exit 0
