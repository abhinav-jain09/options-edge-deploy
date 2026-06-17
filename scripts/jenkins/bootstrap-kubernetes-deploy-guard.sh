#!/usr/bin/env bash
set -euo pipefail

admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/var/jenkins_home/config/kubeconfig}"
jenkins_kubeconfig="${KUBECONFIG_FILE:-/var/jenkins_home/config/jenkins-deployer.kubeconfig}"
namespace="${K8S_NAMESPACE:-options-edge}"
service_account="${JENKINS_K8S_SERVICE_ACCOUNT:-jenkins-deployer}"
token_secret="${JENKINS_K8S_TOKEN_SECRET:-jenkins-deployer-token}"
break_glass_recreate_kubeconfig="${BREAK_GLASS_RECREATE_JENKINS_DEPLOYER_KUBECONFIG:-false}"

if [[ "$break_glass_recreate_kubeconfig" != "true" && ( -z "${JENKINS_URL:-}" || -z "${BUILD_NUMBER:-}" || -z "${JOB_NAME:-}" ) ]]; then
  echo "Kubernetes deploy guard bootstrap blocked: this must run from Jenkins." >&2
  exit 1
fi

if [[ ! -f "$admin_kubeconfig" ]]; then
  echo "Missing admin kubeconfig for deploy guard bootstrap: $admin_kubeconfig" >&2
  exit 1
fi

apply_jenkins_deployer_rbac() {
  kubectl --kubeconfig "$admin_kubeconfig" apply -f k8s/security/jenkins-deployer-rbac.yaml
}

verify_jenkins_deployer_access() {
  kubectl --kubeconfig "$jenkins_kubeconfig" -n "$namespace" auth can-i patch deployment >/dev/null
  kubectl --kubeconfig "$jenkins_kubeconfig" -n "$namespace" auth can-i create pods --subresource=portforward >/dev/null
}

write_jenkins_deployer_kubeconfig() {
  mkdir -p "$(dirname "$jenkins_kubeconfig")"

  for attempt in $(seq 1 120); do
    token="$(kubectl --kubeconfig "$admin_kubeconfig" -n "$namespace" get secret "$token_secret" -o jsonpath='{.data.token}' 2>/dev/null || true)"
    ca_data="$(kubectl --kubeconfig "$admin_kubeconfig" -n "$namespace" get secret "$token_secret" -o go-template='{{ index .data "ca.crt" }}' 2>/dev/null || true)"
    if [[ -n "$token" && -n "$ca_data" ]]; then
      break
    fi
    if (( attempt % 10 == 0 )); then
      echo "Waiting for Jenkins deployer service-account token secret data; attempt=$attempt/120"
    fi
    sleep 1
  done

  if [[ -z "${token:-}" || -z "${ca_data:-}" ]]; then
    echo "Timed out waiting for Jenkins deployer service-account token secret." >&2
    exit 1
  fi

  server="$(kubectl --kubeconfig "$admin_kubeconfig" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  decoded_token="$(printf '%s' "$token" | base64 -d)"

  cat >"$jenkins_kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: options-edge
  cluster:
    certificate-authority-data: $ca_data
    server: $server
contexts:
- name: jenkins-deployer
  context:
    cluster: options-edge
    namespace: $namespace
    user: $service_account
current-context: jenkins-deployer
users:
- name: $service_account
  user:
    token: $decoded_token
EOF
  chmod 0600 "$jenkins_kubeconfig"
}

if [[ "$break_glass_recreate_kubeconfig" == "true" ]]; then
  apply_jenkins_deployer_rbac
  write_jenkins_deployer_kubeconfig
  verify_jenkins_deployer_access
  echo "Break-glass repair complete: recreated Jenkins deployer kubeconfig at $jenkins_kubeconfig."
  exit 0
fi

apply_jenkins_deployer_rbac

if kubectl --kubeconfig "$admin_kubeconfig" get validatingadmissionpolicy options-edge-jenkins-only-workloads >/dev/null 2>&1; then
  if [[ -f "$jenkins_kubeconfig" ]] && verify_jenkins_deployer_access >/dev/null 2>&1; then
    echo "Kubernetes deploy guard is already active and Jenkins deployer kubeconfig is usable."
    exit 0
  fi
  echo "Kubernetes deploy guard is active, but Jenkins deployer kubeconfig is missing or unusable: $jenkins_kubeconfig" >&2
  echo "Repair requires temporary cluster-admin intervention because direct non-Jenkins workload changes are blocked." >&2
  exit 1
fi

mkdir -p "$(dirname "$jenkins_kubeconfig")"

kubectl --kubeconfig "$admin_kubeconfig" -n "$namespace" label namespace "$namespace" options-edge/deploy-guard=jenkins-only --overwrite

write_jenkins_deployer_kubeconfig
verify_jenkins_deployer_access
kubectl --kubeconfig "$admin_kubeconfig" apply -f k8s/security/jenkins-only-workload-admission.yaml

echo "Kubernetes deploy guard is active: workload mutations in $namespace require $namespace/$service_account."
