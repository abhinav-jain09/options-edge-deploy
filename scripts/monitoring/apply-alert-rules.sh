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
BEGIN_MARKER="# BEGIN OPTIONS_EDGE_MANAGED_RULE_FILES"
END_MARKER="# END OPTIONS_EDGE_MANAGED_RULE_FILES"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_RULES_FILE="${SOURCE_RULES_FILE:-$SCRIPT_DIR/hpsf-alert-rules.yaml}"

if [[ "$REMOTE_APP_HOME" != "/home/options-edge" ]]; then
  echo "REMOTE_APP_HOME must be /home/options-edge" >&2
  exit 1
fi

if [[ ! -f "$SOURCE_RULES_FILE" ]]; then
  echo "Source rules file $SOURCE_RULES_FILE not found" >&2
  exit 1
fi

# Alerting rules are optional observability config, applied where Prometheus actually runs. On an agent
# without a local Prometheus config (e.g. the Mac deploy agent) skip gracefully — the workloads are
# already rolled out by this point and this must not fail the deploy.
if [[ ! -f "$PROMETHEUS_CONFIG_FILE" ]]; then
  echo "Prometheus config $PROMETHEUS_CONFIG_FILE not present on this agent; skipping alert-rule configuration (non-fatal)."
  exit 0
fi

if ! command -v promtool >/dev/null 2>&1; then
  echo "promtool not found on PATH; cannot validate rules" >&2
  exit 1
fi

# Validate the rules BEFORE touching anything on the host.
promtool check rules "$SOURCE_RULES_FILE"

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

if grep -qE '^[[:space:]]*rule_files:' "$base_config"; then
  # There is already a rule_files: key — append our managed entry right after it.
  awk -v begin="$BEGIN_MARKER" -v end="$END_MARKER" -v path="$PROMETHEUS_RULES_FILE" '
    { print }
    $0 ~ /^[[:space:]]*rule_files:[[:space:]]*$/ && !done {
      print begin
      print "  - " path
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
    echo "$END_MARKER"
  } >>"$next_config"
fi

install_rules_and_config() {
  cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
  cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  promtool check config "$PROMETHEUS_CONFIG_FILE"
  systemctl reload "$PROMETHEUS_SERVICE" 2>/dev/null || systemctl restart "$PROMETHEUS_SERVICE"
}

if [[ -w "$PROMETHEUS_CONFIG_FILE" && -w "$(dirname "$PROMETHEUS_RULES_FILE")" ]]; then
  install_rules_and_config
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  sudo cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
  sudo cp "$next_config" "$PROMETHEUS_CONFIG_FILE"
  sudo promtool check config "$PROMETHEUS_CONFIG_FILE"
  sudo systemctl reload "$PROMETHEUS_SERVICE" 2>/dev/null || sudo systemctl restart "$PROMETHEUS_SERVICE"
elif [[ -n "${BECOME_PASSWORD:-}" ]]; then
  install_script="$work_dir/install-prometheus-rules.sh"
  cat >"$install_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cp "$SOURCE_RULES_FILE" "$PROMETHEUS_RULES_FILE"
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
for _ in $(seq 1 20); do
  if body="$(curl -fsS "$PROMETHEUS_URL/api/v1/rules" 2>/dev/null)" \
     && printf '%s' "$body" | grep -q 'delta-flow.health'; then
    echo "Prometheus loaded delta-flow alert rules"
    exit 0
  fi
  sleep 3
done

echo "Prometheus did not report the delta-flow.health rule group after reload" >&2
exit 1
