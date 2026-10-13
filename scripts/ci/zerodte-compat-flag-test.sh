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
# the DEDICATED v7 writer (increment 9e) runs the same image: its pod must carry NO envFrom (an envFrom ConfigMap could carry the flag, unknowable at
# the container's start) and NO ZERODTE_RESEARCH_ENABLED entry at all (absent = off, as the service reads it), in every render and in the base
writer() { # writer <what> <render file>
  local envfrom entries cmd
  envfrom="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[] | (.envFrom // []) | length' "$2" | grep -v '^---$' | grep -v '^$' | tr '\n' ',')"
  entries="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[] | [.env[] | select(.name == "ZERODTE_RESEARCH_ENABLED")] | length' "$2" | grep -v '^---$' | grep -v '^$' | tr '\n' ',')"
  cmd="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[0].command | join(" ")' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  # the identity (era, generation) is read BY KEY from the ConfigMap zerodte-writer-identity that only the activation renders — never a literal in the render
  era="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[0].env[] | select(.name == "ZERO_DTE_ERA_ID") | .valueFrom.configMapKeyRef | .name + "/" + .key' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  gen="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[0].env[] | select(.name == "ZERO_DTE_PROVISIONING_GENERATION") | .valueFrom.configMapKeyRef | .name + "/" + .key' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  if [ "$era" = "zerodte-writer-identity/eraId" ] && [ "$gen" = "zerodte-writer-identity/provisioningGeneration" ]; then pass=$((pass+1)); echo "  ok   $1: the identity is read by key from zerodte-writer-identity"; else fail=$((fail+1)); echo "  FAIL $1: identity sources era '$era' generation '$gen'"; fi
  # the WHOLE §16 environment, every variable from its one source (literal / configMapKeyRef / secretKeyRef), nothing extra, nothing missing
  local matrix want
  matrix="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[] | select(.name == "zerodte-research-writer") | .env[] | .name + "=lit:" + (.value // "") + "|cm:" + (.valueFrom.configMapKeyRef.name // "") + "/" + (.valueFrom.configMapKeyRef.key // "") + "|secret:" + (.valueFrom.secretKeyRef.name // "") + "/" + (.valueFrom.secretKeyRef.key // "")' "$2" | grep -v '^---$' | grep -v '^$' | sort | tr '\n' ' ')"
  want="HEALTH_PORT=lit:8080|cm:/|secret:/ JAVA_TOOL_OPTIONS=lit:-Xms64m -Xmx384m|cm:/|secret:/ KAFKA_BOOTSTRAP_SERVERS=lit:|cm:options-edge-config/KAFKA_BOOTSTRAP_SERVERS|secret:/ POSTGRES_JDBC_URL=lit:|cm:options-edge-config/POSTGRES_JDBC_URL|secret:/ POSTGRES_PASSWORD=lit:|cm:/|secret:options-edge-runtime-secrets/POSTGRES_PASSWORD POSTGRES_USER=lit:|cm:options-edge-config/POSTGRES_USER|secret:/ ZERO_DTE_ERA_ID=lit:|cm:zerodte-writer-identity/eraId|secret:/ ZERO_DTE_EXPECTED_SESSIONS_AHEAD=lit:60|cm:/|secret:/ ZERO_DTE_FRAMES_TOPIC=lit:vix-option-inteligence.spx.frames|cm:/|secret:/ ZERO_DTE_PROVISIONING_GENERATION=lit:|cm:zerodte-writer-identity/provisioningGeneration|secret:/ ZERO_DTE_RESEARCH_GROUP=lit:zerodte-research-writer-spx|cm:/|secret:/ ZERO_DTE_RESEARCH_JDBC_PASSWORD_ENV=lit:POSTGRES_PASSWORD|cm:/|secret:/ ZERO_DTE_RESEARCH_LAG_ALERT_MS=lit:120000|cm:/|secret:/ ZERO_DTE_RESEARCH_RETRY_BACKOFF_MS=lit:5000|cm:/|secret:/ ZERO_DTE_SYMBOL=lit:SPX|cm:/|secret:/ "
  if [ "$matrix" = "$want" ]; then pass=$((pass+1)); echo "  ok   $1: the §16 environment is exactly the matrix (16 variables, each from its one source)"; else fail=$((fail+1)); echo "  FAIL $1: environment matrix differs:"; echo "       got  $matrix"; echo "       want $want"; fi
  # the probes, the service-account token, the one container
  local probes token ncont
  probes="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers[] | select(.name == "zerodte-research-writer") | .livenessProbe.httpGet.path + " " + .readinessProbe.httpGet.path + " " + (.livenessProbe.httpGet.port | tostring) + " " + (.readinessProbe.httpGet.port | tostring)' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  token="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.automountServiceAccountToken' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  ncont="$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.template.spec.containers | length' "$2" | grep -v '^---$' | grep -v '^$' | head -1)"
  if [ "$probes" = "/health/live /health/ready health health" ] && [ "$token" = false ] && [ "$ncont" = 1 ]; then pass=$((pass+1)); echo "  ok   $1: liveness /health/live, readiness /health/ready, no service-account token, one container"; else fail=$((fail+1)); echo "  FAIL $1: probes '$probes' token '$token' containers '$ncont'"; fi
  if [ "$envfrom" = "0," ] && [ "$entries" = "0," ] && [ "$cmd" = "java -cp /app/app.jar com.optionsedge.processing.zerodte.research.ZeroDteResearchWriterMain" ]; then pass=$((pass+1)); echo "  ok   $1: the writer has no envFrom, no ZERODTE_RESEARCH_ENABLED entry, and runs the writer main"; else fail=$((fail+1)); echo "  FAIL $1: writer envFrom counts '$envfrom', flag entries '$entries', command '$cmd'"; fi
}
for env in dev production experiment; do
  kubectl kustomize "k8s/services/zerodte-research-writer/overlays/$env" > "$T/writer-$env.yaml" 2>/dev/null || { fail=$((fail+1)); echo "  FAIL the $env writer slice does not render"; continue; }
  writer "the generated $env writer slice" "$T/writer-$env.yaml"
  [ "$(yq -r 'select(.kind == "Deployment" and .metadata.name == "zerodte-research-writer") | .spec.replicas' "$T/writer-$env.yaml" | grep -v '^---$' | head -1)" = 0 ] && { pass=$((pass+1)); echo "  ok   the $env writer slice ships at replicas 0"; } || { fail=$((fail+1)); echo "  FAIL the $env writer slice does not ship at replicas 0"; }
done
writer "the base writer Deployment" k8s/base/zerodte-research-writer-deployment.yaml
# the CHECK Job template (k8s/jobs/zerodte-writer-check-job.yaml): --check on the writer main, no retry, a 300 s deadline, no service-account token,
# no envFrom, the password by name, the identity as placeholders the wrapper substitutes
J=k8s/jobs/zerodte-writer-check-job.yaml
jcmd="$(yq -r '.spec.template.spec.containers[0].command | join(" ")' "$J")"; jback="$(yq -r '.spec.backoffLimit' "$J")"; jdead="$(yq -r '.spec.activeDeadlineSeconds' "$J")"; jtok="$(yq -r '.spec.template.spec.automountServiceAccountToken' "$J")"
jenvfrom="$(yq -r '.spec.template.spec.containers[0].envFrom // [] | length' "$J")"; jname="$(yq -r '.spec.template.spec.containers[0].name' "$J")"; jrestart="$(yq -r '.spec.template.spec.restartPolicy' "$J")"
jpw="$(yq -r '.spec.template.spec.containers[0].env[] | select(.name == "POSTGRES_PASSWORD") | .valueFrom.secretKeyRef.name + "/" + .valueFrom.secretKeyRef.key' "$J")"
jids="$(yq -r '[.spec.template.spec.containers[0].env[] | select(.name == "ZERO_DTE_ERA_ID" or .name == "ZERO_DTE_PROVISIONING_GENERATION" or .name == "ZERO_DTE_SYMBOL" or .name == "ZERO_DTE_FRAMES_TOPIC") | .value] | join(" ")' "$J")"
if [ "$jcmd" = "java -cp /app/app.jar com.optionsedge.processing.zerodte.research.ZeroDteResearchWriterMain --check" ] && [ "$jback" = 0 ] && [ "$jdead" = 300 ] && [ "$jtok" = false ] && [ "$jenvfrom" = 0 ] && [ "$jname" = writer-check ] && [ "$jrestart" = Never ] && [ "$jpw" = "options-edge-runtime-secrets/POSTGRES_PASSWORD" ] && [ "$jids" = "__SYMBOL__ __FRAMES_TOPIC__ __ERA_ID__ __GENERATION__" ]; then
  pass=$((pass+1)); echo "  ok   the CHECK Job template: --check on the writer main, backoffLimit 0, 300 s deadline, no SA token, no envFrom, the password by name, the identity as placeholders"
else fail=$((fail+1)); echo "  FAIL the CHECK Job template: cmd '$jcmd' backoff '$jback' deadline '$jdead' token '$jtok' envFrom '$jenvfrom' name '$jname' restart '$jrestart' password '$jpw' ids '$jids'"; fi
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
