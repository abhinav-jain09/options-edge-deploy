#!/usr/bin/env bash
set -euo pipefail

admin_kubeconfig="${KUBECONFIG_ADMIN_FILE:-/Users/abhinav/.kube/dd-admin.yaml}"
jenkins_kubeconfig="${KUBECONFIG_FILE:-/Users/abhinav/.kube/jenkins-deployer-dev.kubeconfig}"
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

# Reconcile the admission policy on EVERY run (apply is idempotent). Previously
# this was applied only on first bootstrap, so subsequent edits to the policy
# YAML (e.g. adding new gated resources like persistentvolumeclaims) silently
# never landed on already-bootstrapped clusters. Apply it before the early-exit
# so the cluster's policy always matches the YAML in git.
kubectl --kubeconfig "$admin_kubeconfig" apply -f k8s/security/jenkins-only-workload-admission.yaml

# Already fully bootstrapped iff the Jenkins deployer kubeconfig exists AND works. The admission policy is
# now (idempotently) applied on EVERY run just above, so its mere existence no longer distinguishes a fresh
# cluster from a bootstrapped one -- gate the early-exit on the kubeconfig, not the policy. On a fresh OR a
# broken-kubeconfig cluster we fall through and (re)bootstrap below using the admin kubeconfig (namespace
# label + deployer SA/kubeconfig creation are admin-cred operations, not gated workload mutations), which
# self-heals instead of dead-ending at exit 1.
if [[ -f "$jenkins_kubeconfig" ]] && verify_jenkins_deployer_access >/dev/null 2>&1; then
  echo "Kubernetes deploy guard is already active and Jenkins deployer kubeconfig is usable."
  exit 0
fi

mkdir -p "$(dirname "$jenkins_kubeconfig")"

kubectl --kubeconfig "$admin_kubeconfig" -n "$namespace" label namespace "$namespace" options-edge/deploy-guard=jenkins-only --overwrite

write_jenkins_deployer_kubeconfig
verify_jenkins_deployer_access
# (admission policy was already applied above, before the early-exit; no
# need to re-apply here on the first-bootstrap path)

echo "Kubernetes deploy guard is active: workload mutations in $namespace require $namespace/$service_account."
