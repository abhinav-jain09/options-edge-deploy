#!/usr/bin/env bash
# THE COMPATIBILITY RENDER (increment 9c, Codex 9c r3): the research migration's quiescence proof accepts a RUNNING vix-option-inteligence pod
# only when ZERODTE_RESEARCH_ENABLED is a LITERAL "false" in its own spec, and a Deployment template only when it resolves off — so the
# sanctioned deploy of every environment's overlay must RENDER exactly that. This renders the dev overlay patch's monolith, every generated
# service slice and the base, and asserts the literal on the service container. A regeneration drift is caught by the slice drift check.
set -euo pipefail
cd "$(dirname "$0")/../.."
pass=0; fail=0
flag() { # the rendered SERVICE CONTAINER's env entries named ZERODTE_RESEARCH_ENABLED: "<count>|<value or absent>|<has valueFrom>" — the judgment is on the target container, nowhere else
  # two steps on purpose: the ONE Deployment document is extracted first (yq's array constructor yields [] for every non-matching document of a
  # multi-document render, which would read as "0 entries"), then judged alone
  local doc; doc="$(mktemp "$T/doc.XXXXXX")"
  yq ea 'select(.kind == "Deployment" and .metadata.name == "vix-option-inteligence-service")' "$1" > "$doc"
  [ "$(grep -c '^kind: Deployment$' "$doc")" = 1 ] || { echo "deployments=$(grep -c '^kind: Deployment$' "$doc")"; return; }
  # the value is reported WITH its YAML tag (`//` would swallow a YAML false as "absent"): a Kubernetes EnvVar.value is a string, so an
  # unquoted `false` (!!bool) is not the literal "false"; no value at all reads () — an empty value and an empty tag
  yq -r '.spec.template.spec.containers[] | select(.name == "vix-option-inteligence") | [.env[] | select(.name == "ZERODTE_RESEARCH_ENABLED")] | (length | tostring) + "|" + (.[0].value | tostring) + "(" + (.[0].value | tag) + ")|" + (([.[] | select(has("valueFrom"))] | length > 0) | tostring)' "$doc"
}
check() { # check <what> <render file>: exactly ONE entry, the literal STRING "false" (tag !!str), no valueFrom (Codex 9c r5 / r6: the judgment on the rendered target container)
  local got; got="$(flag "$2")"
  if [ "$got" = "1|false(!!str)|false" ]; then pass=$((pass+1)); echo "  ok   $1: ZERODTE_RESEARCH_ENABLED is exactly one entry, the literal STRING \"false\", no valueFrom"; else fail=$((fail+1)); echo "  FAIL $1: ZERODTE_RESEARCH_ENABLED entries '${got:-<no container>}' (want 1|false(!!str)|false)"; fi
}
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
for env in dev production experiment; do
  kubectl kustomize "k8s/services/vix-option-inteligence/overlays/$env" > "$T/slice-$env.yaml" 2>/dev/null || { fail=$((fail+1)); echo "  FAIL the $env slice does not render"; continue; }
  check "the generated $env slice (what service-deploy.sh applies)" "$T/slice-$env.yaml"
  kubectl kustomize "k8s/overlays/$env" > "$T/mono-$env.yaml" 2>/dev/null || { fail=$((fail+1)); echo "  FAIL the $env monolith does not render"; continue; }
  check "the $env monolith render (the slice's source)" "$T/mono-$env.yaml"
done
check "the base Deployment" k8s/base/vix-option-inteligence-deployment.yaml
# A SEPARATE policy, stated EXACTLY: nowhere under k8s/ may a map carrying both a `name` equal to the flag and a `valueFrom` key exist —
# a SOURCE-HYGIENE rule on the repository's YAML (it flags such a map wherever it sits, a CRD or a comment-like object included, and it is
# NOT a kubelet-equivalence proof: an envFrom importing a ConfigMap that carries the key is the quiescence helper's business at runtime,
# scripts/ops/zerodte-quiescence.py, which resolves the effective environment). A live pod's flag must be a literal, so no render SOURCE may
# take it from a reference. Judged STRUCTURALLY with yq over EVERY *.yaml / *.yml under k8s/ (enumerated with find, never a lexical grep
# shortlist: a YAML escape such as "\u005aERODTE_RESEARCH_ENABLED" parses to the kubelet-honoured name and a grep would never see it —
# Codex 9c r4 / r5 / r6).
judge() { yq ea '[.. | select(tag == "!!map" and has("name") and has("valueFrom") and .name == "ZERODTE_RESEARCH_ENABLED")] | length' "$1"; }
refs=0; scanned=0
while IFS= read -r -d '' f; do
  scanned=$((scanned+1))
  n="$(judge "$f")" || { fail=$((fail+1)); echo "  FAIL $f could not be judged by yq"; continue; }
  [ "$n" = 0 ] || { refs=$((refs+1)); echo "  FAIL $f takes ZERODTE_RESEARCH_ENABLED from a reference ($n env entries with valueFrom)"; }
done < <(find k8s -type f \( -name '*.yaml' -o -name '*.yml' \) -print0)
if [ "$refs" = 0 ] && [ "$scanned" -gt 0 ]; then pass=$((pass+1)); echo "  ok   source hygiene: no map under k8s/ names ZERODTE_RESEARCH_ENABLED with a valueFrom ($scanned files judged structurally; the exact name+valueFrom map policy)"; else fail=$((fail+refs+1)); fi
# positive controls: the judge SEES a configMapKeyRef on the flag in the ordinary spelling AND in an escaped spelling a grep cannot see
printf 'kind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n        - name: x\n          env:\n            - name: ZERODTE_RESEARCH_ENABLED\n              valueFrom:\n                configMapKeyRef:\n                  name: c\n                  key: k\n' > "$T/ref-probe.yaml"
[ "$(judge "$T/ref-probe.yaml")" = 1 ] && { pass=$((pass+1)); echo "  ok   the structural judge SEES a configMapKeyRef on the flag (positive control)"; } || { fail=$((fail+1)); echo "  FAIL the structural judge does not see a configMapKeyRef on the flag"; }
printf 'kind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n        - name: x\n          env:\n            - name: "\\u005aERODTE_RESEARCH_ENABLED"\n              valueFrom:\n                secretKeyRef:\n                  name: s\n                  key: k\n' > "$T/escaped-probe.yaml"
if ! grep -q "ZERODTE_RESEARCH_ENABLED" "$T/escaped-probe.yaml" && [ "$(judge "$T/escaped-probe.yaml")" = 1 ]; then pass=$((pass+1)); echo "  ok   the structural judge SEES an ESCAPED flag name a grep cannot (positive control)"; else fail=$((fail+1)); echo "  FAIL the escaped-name control: grep sees it or the judge does not (judge=$(judge "$T/escaped-probe.yaml"))"; fi
# and the container judgment itself rejects a reference and a duplicate (negative controls on the rendered target container)
printf 'kind: Deployment\nmetadata:\n  name: vix-option-inteligence-service\nspec:\n  template:\n    spec:\n      containers:\n        - name: vix-option-inteligence\n          env:\n            - name: ZERODTE_RESEARCH_ENABLED\n              valueFrom:\n                configMapKeyRef:\n                  name: c\n                  key: k\n' > "$T/container-ref.yaml"
[ "$(flag "$T/container-ref.yaml")" = "1|()|true" ] && { pass=$((pass+1)); echo "  ok   the container judgment reports a valueFrom (negative control)"; } || { fail=$((fail+1)); echo "  FAIL the container judgment on a valueFrom: '$(flag "$T/container-ref.yaml")'"; }
printf 'kind: Deployment\nmetadata:\n  name: vix-option-inteligence-service\nspec:\n  template:\n    spec:\n      containers:\n        - name: vix-option-inteligence\n          env:\n            - name: ZERODTE_RESEARCH_ENABLED\n              value: "false"\n            - name: ZERODTE_RESEARCH_ENABLED\n              value: "true"\n' > "$T/container-dup.yaml"
[ "$(flag "$T/container-dup.yaml")" = "2|false(!!str)|false" ] && { pass=$((pass+1)); echo "  ok   the container judgment reports a duplicate entry (negative control)"; } || { fail=$((fail+1)); echo "  FAIL the container judgment on a duplicate: '$(flag "$T/container-dup.yaml")'"; }
printf 'kind: Deployment\nmetadata:\n  name: vix-option-inteligence-service\nspec:\n  template:\n    spec:\n      containers:\n        - name: vix-option-inteligence\n          env:\n            - name: ZERODTE_RESEARCH_ENABLED\n              value: false\n' > "$T/container-bool.yaml"
[ "$(flag "$T/container-bool.yaml")" = "1|false(!!bool)|false" ] && { pass=$((pass+1)); echo "  ok   the container judgment reports an UNQUOTED boolean, not the string (negative control)"; } || { fail=$((fail+1)); echo "  FAIL the container judgment on an unquoted boolean: '$(flag "$T/container-bool.yaml")'"; }
echo "zerodte compatibility flag: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== zerodte-compat-flag-test: OK ==="; exit 0; }
echo "=== zerodte-compat-flag-test: FAILED ==="; exit 1
