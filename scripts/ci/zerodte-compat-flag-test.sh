#!/usr/bin/env bash
# THE COMPATIBILITY RENDER (increment 9c, Codex 9c r3): the research migration's quiescence proof accepts a RUNNING vix-option-inteligence pod
# only when ZERODTE_RESEARCH_ENABLED is a LITERAL "false" in its own spec, and a Deployment template only when it resolves off — so the
# sanctioned deploy of every environment's overlay must RENDER exactly that. This renders the dev overlay patch's monolith, every generated
# service slice and the base, and asserts the literal on the service container. A regeneration drift is caught by the slice drift check.
set -euo pipefail
cd "$(dirname "$0")/../.."
pass=0; fail=0
flag() { yq -r 'select(.kind == "Deployment" and .metadata.name == "vix-option-inteligence-service") | .spec.template.spec.containers[] | select(.name == "vix-option-inteligence") | .env[] | select(.name == "ZERODTE_RESEARCH_ENABLED") | (.value // ("FROM:" + (.valueFrom | keys | join(","))))' "$1" | grep -v '^---$' | grep -v '^$' || true; }
check() { # check <what> <render file>
  local got; got="$(flag "$2")"
  if [ "$got" = '"false"' ] || [ "$got" = "false" ]; then pass=$((pass+1)); echo "  ok   $1: ZERODTE_RESEARCH_ENABLED is the literal \"false\""; else fail=$((fail+1)); echo "  FAIL $1: ZERODTE_RESEARCH_ENABLED is '${got:-<absent>}', not the literal \"false\""; fi
}
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
for env in dev production experiment; do
  kubectl kustomize "k8s/services/vix-option-inteligence/overlays/$env" > "$T/slice-$env.yaml" 2>/dev/null || { fail=$((fail+1)); echo "  FAIL the $env slice does not render"; continue; }
  check "the generated $env slice (what service-deploy.sh applies)" "$T/slice-$env.yaml"
  kubectl kustomize "k8s/overlays/$env" > "$T/mono-$env.yaml" 2>/dev/null || { fail=$((fail+1)); echo "  FAIL the $env monolith does not render"; continue; }
  check "the $env monolith render (the slice's source)" "$T/mono-$env.yaml"
done
check "the base Deployment" k8s/base/vix-option-inteligence-deployment.yaml
# the flag is never sourced from a ConfigMap or a Secret anywhere in the tree (a live pod's flag must be a literal)
if grep -rn "ZERODTE_RESEARCH_ENABLED" k8s --include=*.yaml | grep -v "value:" | grep -q "valueFrom\|configMapKeyRef"; then fail=$((fail+1)); echo "  FAIL a render source takes ZERODTE_RESEARCH_ENABLED from a reference"; else pass=$((pass+1)); echo "  ok   no render source takes ZERODTE_RESEARCH_ENABLED from a reference"; fi
echo "zerodte compatibility flag: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-compat-flag-test: OK ==="; exit 0; }
echo "=== zerodte-compat-flag-test: FAILED ==="; exit 1
