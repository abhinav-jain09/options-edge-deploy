#!/usr/bin/env bash
# enforce-local-dev-defaults.sh — Validate stage local guard for options-edge-deploy.
#
# Asserts the new SINGLE-SOURCE-OF-TRUTH contract (UNIFORM-DEPLOY-POLICY.md):
#   - The Jenkinsfile imports @Library('oe') and resolves env-specific values via
#     oeProfile (no hardcoded registry / kafka / kubeconfig / image literals).
#   - The expected stage order (incl. the Resolve profile stage) is preserved.
#   - The bootstrap deploy guard still defaults to the local-mac ~/.kube paths.
#   - Manual-deploy-only invariants (no SCM polling, no dev manual-approval input).
#
# The detailed cross-job policy guard (hardcoded literals, allowlist, fail-closed
# switch) lives in the oe shared library: scripts/enforce-deploy-policy.sh.
set -euo pipefail

jenkinsfile="${1:-Jenkinsfile}"
bootstrap_script="${2:-scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh}"

require_text() {
  local file="$1" text="$2" message="$3"
  if ! grep -Fq "$text" "$file"; then
    echo "Local dev deploy guard failed: $message" >&2
    echo "Missing required text in $file:" >&2
    echo "  $text" >&2
    exit 1
  fi
}

reject_text() {
  local file="$1" text="$2" message="$3"
  if grep -Fq "$text" "$file"; then
    echo "Local dev deploy guard failed: $message" >&2
    echo "Forbidden text found in $file:" >&2
    echo "  $text" >&2
    exit 1
  fi
}

# --- Jenkinsfile: single-source-of-truth contract ---------------------------------
require_text "$jenkinsfile" "@Library('oe')" \
  "Jenkinsfile must import @Library('oe') to use the canonical deploy profile."
require_text "$jenkinsfile" "oeProfile(params.ENVIRONMENT)" \
  "Jenkinsfile must call oeProfile(params.ENVIRONMENT) to derive env-specific values from the single source of truth."
require_text "$jenkinsfile" "stage('Resolve profile')" \
  "Jenkinsfile must declare a 'Resolve profile' stage that surfaces oeProfile-derived values."

# --- Jenkinsfile: forbid hardcoded environment literals --------------------------
# (the policy guard scans the same patterns; these are spot-checks for THIS job)
reject_text "$jenkinsfile" "defaultValue: '/Users/abhinav/.kube/jenkins-deployer-dev.kubeconfig'" \
  "kubeconfig param defaults must be empty (derive from oeProfile.kubeconfigDeployer)."
reject_text "$jenkinsfile" "defaultValue: '/Users/abhinav/.kube/dd-admin.yaml'" \
  "kubeconfig param defaults must be empty (derive from oeProfile.kubeconfigAdmin)."
reject_text "$jenkinsfile" "REMOTE_APP_HOME = '/home/options-edge'" \
  "REMOTE_APP_HOME must derive from oeProfile.remoteAppHome (not a hardcoded literal)."
reject_text "$jenkinsfile" "192.168.100.252:5000/options-edge-" \
  "X_IMAGE defaults must derive from oeProfile.image() (not hardcoded prod-registry literals)."
reject_text "$jenkinsfile" "/var/jenkins_home/bin" \
  "Dead CentOS-controller PATH prefix removed; do not re-introduce."
reject_text "$jenkinsfile" "pollSCM(" \
  "SCM polling is disabled per UNIFORM-DEPLOY-POLICY.md (manual-trigger only)."
reject_text "$jenkinsfile" "input message: 'Deploy OptionsEdge to DEV?'" \
  "dev deploy must run automatically; only production deployment may require manual approval."

# --- Bootstrap helper still uses the local-mac ~/.kube paths ---------------------
# (the bootstrap script runs OUTSIDE the Jenkins pipeline, so it cannot use oeProfile
# directly; until it's migrated to read deploy-profiles.yaml, keep the explicit defaults.)
require_text "$bootstrap_script" 'admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/Users/abhinav/.kube/dd-admin.yaml}"' \
  "bootstrap guard must default admin kubeconfig to ~/.kube (persists across Jenkins reinstalls)."
require_text "$bootstrap_script" 'jenkins_kubeconfig="${KUBECONFIG_FILE:-/Users/abhinav/.kube/jenkins-deployer-dev.kubeconfig}"' \
  "bootstrap guard must default deployer kubeconfig to ~/.kube (persists across Jenkins reinstalls)."
reject_text "$bootstrap_script" 'admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/home/options-edge/config/kubeconfig}"' \
  "do not point the local Jenkins bootstrap guard at the remote server kubeconfig path."
reject_text "$bootstrap_script" 'jenkins_kubeconfig="${KUBECONFIG_FILE:-/home/options-edge/config/jenkins-deployer.kubeconfig}"' \
  "do not point the local Jenkins bootstrap guard at the remote server kubeconfig path."

# --- Stage order (must include 'Resolve profile' as the first stage) -------------
expected_stages=$(cat <<'EOF'
Resolve profile
Validate
Resolve Databento Expiry
Bootstrap Jenkins Kubernetes Guard
Render
Unusual Whales Secret
Resolve Images
Image Preflight
Pause Runtime For Kafka Cleanup
Kafka Cleanup
Kafka Topics
Reset HPSF Stage B Internal Topics
Kafka Internal Topics
Deploy
Resume Remote Apps
Prometheus Scrapes
Verify OptionsEdge Web App
Smoke
HPSF Smoke
Promote To Production
EOF
)

actual_stages="$(
  sed -n "s/^    stage('\([^']*\)').*/\1/p" "$jenkinsfile"
)"

if [[ "$actual_stages" != "$expected_stages" ]]; then
  echo "Local dev deploy guard failed: Jenkins stages are not in the approved logical order." >&2
  echo "Expected:" >&2
  printf '  %s\n' "$expected_stages" >&2
  echo "Actual:" >&2
  printf '  %s\n' "$actual_stages" >&2
  exit 1
fi

echo "Local dev deploy defaults guard passed (oeProfile single source of truth + stage order + bootstrap defaults + manual-deploy invariants)."
