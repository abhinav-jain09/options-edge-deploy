#!/usr/bin/env bash
set -euo pipefail

jenkinsfile="${1:-Jenkinsfile}"
bootstrap_script="${2:-scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh}"

require_text() {
  local file="$1"
  local text="$2"
  local message="$3"

  if ! grep -Fq "$text" "$file"; then
    echo "Local dev deploy guard failed: $message" >&2
    echo "Missing required text in $file:" >&2
    echo "$text" >&2
    exit 1
  fi
}

reject_text() {
  local file="$1"
  local text="$2"
  local message="$3"

  if grep -Fq "$text" "$file"; then
    echo "Local dev deploy guard failed: $message" >&2
    echo "Forbidden text found in $file:" >&2
    echo "$text" >&2
    exit 1
  fi
}

require_text "$jenkinsfile" "defaultValue: '/var/jenkins_home/config/jenkins-deployer.kubeconfig'" \
  "local Jenkins deployer kubeconfig must default to /var/jenkins_home/config."
require_text "$jenkinsfile" "defaultValue: '/var/jenkins_home/config/kubeconfig'" \
  "local Jenkins admin kubeconfig must default to /var/jenkins_home/config."
require_text "$jenkinsfile" "host.docker.internal:5001" \
  "dev image registry must default to the Mac local registry."
require_text "$jenkinsfile" "host.docker.internal:9092" \
  "dev Kafka must default to the Mac local Kafka broker."
require_text "$jenkinsfile" "http://host.docker.internal:8090" \
  "dev web smoke must default to the Mac local OptionsEdge app."

require_text "$bootstrap_script" 'admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/var/jenkins_home/config/kubeconfig}"' \
  "bootstrap guard must default admin kubeconfig to /var/jenkins_home/config."
require_text "$bootstrap_script" 'jenkins_kubeconfig="${KUBECONFIG_FILE:-/var/jenkins_home/config/jenkins-deployer.kubeconfig}"' \
  "bootstrap guard must default deployer kubeconfig to /var/jenkins_home/config."

reject_text "$bootstrap_script" 'admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/home/options-edge/config/kubeconfig}"' \
  "do not point the local Jenkins bootstrap guard at the remote server kubeconfig path."
reject_text "$bootstrap_script" 'jenkins_kubeconfig="${KUBECONFIG_FILE:-/home/options-edge/config/jenkins-deployer.kubeconfig}"' \
  "do not point the local Jenkins bootstrap guard at the remote server kubeconfig path."
reject_text "$jenkinsfile" "input message: 'Deploy OptionsEdge to DEV?'" \
  "dev deploy must run automatically; only production deployment may require manual approval."

expected_stages=$(cat <<'EOF'
Validate
One-Time Local Dev Databento Kafka Cleanup
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
  printf '%s\n' "$expected_stages" >&2
  echo "Actual:" >&2
  printf '%s\n' "$actual_stages" >&2
  exit 1
fi

echo "Local dev deploy defaults guard passed."
