#!/usr/bin/env bash
# FULLFUNDING TENANT DEPLOY.
#
# Applies k8s/tenants/$TENANT — the second application's space on the prod node: namespace,
# budget, priority, network boundary, admission rules, deploy identity, and its own Postgres.
#
# This job is the SINGLE owner of those resources. common-infra-deploy owns options-edge's
# shared layer and must never touch this namespace; equally, this job must never touch
# options-edge — which is asserted, not assumed (see the blast-radius gate below).
#
# Safety gates:
#   * DRY-RUN BY DEFAULT. A real apply must be opted into per run, and production additionally
#     requires the manual approval gate in the Jenkinsfile.
#   * BLAST-RADIUS gate: every namespaced document must target the tenant namespace, and every
#     cluster-scoped document must be on a closed allowlist. A stray `namespace: options-edge`
#     fails the job BEFORE anything is applied.
#   * GUARDRAIL gate: the quota, LimitRange, PSA labels, negative PriorityClass, default-deny
#     NetworkPolicy and admission policy are verified to exist AFTER the apply. A boundary that
#     is not re-asserted is assumed absent.
#   * StatefulSet volumeClaimTemplates are IMMUTABLE after creation, so a changed template is
#     failed loudly up front instead of being rejected mid-apply.
#
# Env:
#   ENVIRONMENT      production (required — selects the deployer kubeconfig via oeProfile)
#   TENANT           tenant directory under k8s/tenants (default: fullfunding)
#   DEPLOY_DRY_RUN   true|false (default true)
#   KUBECONFIG       deployer kubeconfig, set by the Jenkins job
#   WORK_DIR         scratch dir (default: mktemp)
set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT must be set (production)}"
TENANT="${TENANT:-fullfunding}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-true}"
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK_DIR"

DIR="k8s/tenants/${TENANT}"
NAMESPACE="${TENANT}"
RENDER="$WORK_DIR/tenant-${TENANT}.yaml"

[ -d "$DIR" ] || { echo "FATAL: no tenant directory $DIR" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

echo "=== render $DIR ==="
kubectl kustomize "$DIR" >"$RENDER"
total_docs="$(grep -c '^kind:' "$RENDER" || true)"
echo "rendered: $total_docs docs"
[ "$total_docs" -gt 0 ] || { echo "FATAL: render produced nothing" >&2; exit 1; }

# --- Blast-radius gate ---------------------------------------------------------------
# The whole point of a tenant namespace is that a mistake here cannot reach options-edge.
# Cluster-scoped kinds are allowed only by exact name; anything namespaced must name the
# tenant namespace explicitly (kustomize does not default it for us).
echo "=== blast-radius gate ==="
CLUSTER_SCOPED_RE='^(Namespace|PriorityClass|StorageClass|ValidatingAdmissionPolicy|ValidatingAdmissionPolicyBinding|ClusterRole|ClusterRoleBinding)$'
violations=0
while IFS=$'\t' read -r kind name ns; do
  [ -n "$kind" ] || continue
  if [[ "$kind" =~ $CLUSTER_SCOPED_RE ]]; then
    case "$kind/$name" in
      Namespace/"$TENANT"|PriorityClass/"$TENANT"-low|\
      ValidatingAdmissionPolicy/"$TENANT"-tenant-rules|ValidatingAdmissionPolicyBinding/"$TENANT"-tenant-rules|\
      StorageClass/"$TENANT"-*|ClusterRole/"$TENANT"-*|ClusterRoleBinding/"$TENANT"-*) ;;
      *) echo "  VIOLATION cluster-scoped $kind/$name is not on the tenant allowlist" >&2
         violations=$((violations + 1)) ;;
    esac
  elif [ "$ns" != "$NAMESPACE" ]; then
    echo "  VIOLATION $kind/$name targets namespace '${ns:-<unset>}', not '$NAMESPACE'" >&2
    violations=$((violations + 1))
  fi
done < <(yq -r '[.kind, .metadata.name, (.metadata.namespace // "")] | @tsv' "$RENDER")
if [ "$violations" -gt 0 ]; then
  echo "FATAL: $violations document(s) would act outside the tenant boundary — refusing to apply." >&2
  exit 1
fi
echo "  clean: every document is inside the tenant boundary"

# --- StatefulSet volumeClaimTemplate immutability ------------------------------------
# Changing a volumeClaimTemplate on a live StatefulSet is rejected by the API server. Catch it
# here, with a readable message, instead of failing halfway through an apply.
echo "=== StatefulSet storage gate ==="
sts_conflict=0
while IFS=$'\t' read -r sts_name; do
  [ -n "$sts_name" ] || continue
  live="$(kubectl -n "$NAMESPACE" get statefulset "$sts_name" \
            -o jsonpath='{range .spec.volumeClaimTemplates[*]}{.metadata.name}={.spec.storageClassName}:{.spec.resources.requests.storage} {end}' 2>/dev/null || true)"
  if [ -z "$live" ]; then
    echo "  $sts_name: not present yet (will be created)"
    continue
  fi
  want="$(yq -r "select(.kind == \"StatefulSet\" and .metadata.name == \"$sts_name\") | [.spec.volumeClaimTemplates[] | .metadata.name + \"=\" + .spec.storageClassName + \":\" + .spec.resources.requests.storage] | join(\" \")" "$RENDER") "
  if [ "$(echo "$live" | xargs)" != "$(echo "$want" | xargs)" ]; then
    echo "  CONFLICT $sts_name volumeClaimTemplates changed (immutable)" >&2
    echo "    live: $(echo "$live" | xargs)" >&2
    echo "    want: $(echo "$want" | xargs)" >&2
    sts_conflict=$((sts_conflict + 1))
  else
    echo "  $sts_name: storage unchanged"
  fi
done < <(yq -r 'select(.kind == "StatefulSet") | .metadata.name' "$RENDER" | grep -vE '^(---)?$' || true)
[ "$sts_conflict" -eq 0 ] || { echo "FATAL: $sts_conflict StatefulSet storage conflict(s)." >&2; exit 1; }

# --- Diff so the log shows exactly what changes --------------------------------------
echo "=== diff ==="
set +e
kubectl diff -f "$RENDER" >"$WORK_DIR/tenant-diff.txt" 2>&1
diff_rc=$?
set -e
if [ "$diff_rc" -eq 0 ]; then echo "no changes."
elif [ "$diff_rc" -eq 1 ]; then cat "$WORK_DIR/tenant-diff.txt"
else echo "WARN: kubectl diff failed (rc=$diff_rc) — continuing to server-side validation:" >&2
     tail -20 "$WORK_DIR/tenant-diff.txt" >&2
fi

# Split the Namespace out. It has to be applied first, and on a first run it is also the reason
# the namespaced documents cannot be server-validated yet — a server dry-run never creates it, so
# validating everything in one pass would fail every first run with a misleading NotFound.
NS_DOC="$WORK_DIR/tenant-${TENANT}-ns.yaml"
REST="$WORK_DIR/tenant-${TENANT}-rest.yaml"
yq 'select(.kind == "Namespace")' "$RENDER" >"$NS_DOC"
yq 'select(.kind != "Namespace")' "$RENDER" >"$REST"

ns_exists=false
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 && ns_exists=true

if [ "$DEPLOY_DRY_RUN" = "true" ]; then
  echo "=== DEPLOY_DRY_RUN=true: server-side validation only, nothing changes ==="
  kubectl apply --dry-run=server -f "$NS_DOC"
  if [ "$ns_exists" = true ]; then
    kubectl apply --dry-run=server -f "$REST"
    echo "dry-run OK — would apply $total_docs docs to namespace $NAMESPACE."
  else
    # Validate what genuinely can be validated rather than reporting a pass we did not earn.
    kubectl apply --dry-run=server -f - <<EOF_CS
$(yq 'select(.kind == "PriorityClass" or .kind == "StorageClass" or .kind == "ValidatingAdmissionPolicy" or .kind == "ValidatingAdmissionPolicyBinding" or .kind == "ClusterRole" or .kind == "ClusterRoleBinding")' "$REST")
EOF_CS
    echo
    echo "NOTE: namespace '$NAMESPACE' does not exist yet, so the namespaced documents cannot be"
    echo "      server-validated in a dry run (a dry run never creates the namespace). They were"
    echo "      rendered, passed the blast-radius gate and were diffed above. The cluster-scoped"
    echo "      documents and the Namespace itself validated clean."
    echo "dry-run OK — first run: would create the namespace and apply $total_docs docs."
  fi
  exit 0
fi

# Namespace first, and confirm it is really there before anything is placed inside it.
echo "=== apply namespace ==="
kubectl apply -f "$NS_DOC"
for _ in $(seq 1 30); do
  kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 && break
  sleep 1
done
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || { echo "FATAL: namespace $NAMESPACE did not appear after apply" >&2; exit 1; }

echo "=== apply the rest ==="
kubectl apply -f "$REST"

# --- Guardrail gate ------------------------------------------------------------------
# Re-assert the boundary after the apply. A guardrail that is not verified is assumed absent.
echo "=== guardrail gate ==="
fail=0
check() { # description, condition-output, expectation
  if [ -n "$2" ]; then echo "  ok    $1 ($2)"; else echo "  FAIL  $1" >&2; fail=$((fail + 1)); fi
}
check "ResourceQuota present" \
  "$(kubectl -n "$NAMESPACE" get resourcequota -o name 2>/dev/null | head -1)"
check "quota caps memory" \
  "$(kubectl -n "$NAMESPACE" get resourcequota -o jsonpath='{.items[0].spec.hard.limits\.memory}' 2>/dev/null)"
check "LimitRange present" \
  "$(kubectl -n "$NAMESPACE" get limitrange -o name 2>/dev/null | head -1)"
check "PSA enforce label" \
  "$(kubectl get ns "$NAMESPACE" -o jsonpath='{.metadata.labels.pod-security\.kubernetes\.io/enforce}' 2>/dev/null)"
check "PriorityClass is negative" \
  "$(kubectl get priorityclass "${TENANT}-low" -o jsonpath='{.value}' 2>/dev/null | grep '^-' || true)"
check "default-deny NetworkPolicy" \
  "$(kubectl -n "$NAMESPACE" get networkpolicy default-deny-all -o name 2>/dev/null)"
check "admission policy bound with Deny" \
  "$(kubectl get validatingadmissionpolicybinding "${TENANT}-tenant-rules" -o jsonpath='{.spec.validationActions[0]}' 2>/dev/null | grep -x Deny || true)"

# Absence checks: these must find NOTHING.
lb="$(kubectl -n "$NAMESPACE" get svc -o jsonpath='{range .items[?(@.spec.type!="ClusterIP")]}{.metadata.name} {end}' 2>/dev/null)"
if [ -z "$lb" ]; then echo "  ok    no LoadBalancer/NodePort Service"; else
  echo "  FAIL  non-ClusterIP Service present: $lb" >&2; fail=$((fail + 1)); fi

# An ingress rule with no `from` admits traffic from ANY source (ports only narrow the
# destination, not the origin); likewise an egress rule with no `to`. Both are what
# "default-deny plus a narrow allowlist" must never contain, so both are checked.
np_json="$(kubectl -n "$NAMESPACE" get networkpolicy -o json 2>/dev/null || echo '{"items":[]}')"
allow_all_in="$(printf '%s' "$np_json" | yq -r '.items[] | select((.spec.ingress // []) | any_c(has("from") | not)) | .metadata.name' 2>/dev/null || true)"
allow_all_out="$(printf '%s' "$np_json" | yq -r '.items[] | select((.spec.egress // []) | any_c(has("to") | not)) | .metadata.name' 2>/dev/null || true)"
if [ -z "$allow_all_in" ]; then echo "  ok    no unrestricted-source ingress rule"; else
  echo "  FAIL  NetworkPolicy admits traffic from any source: $allow_all_in" >&2; fail=$((fail + 1)); fi
if [ -z "$allow_all_out" ]; then echo "  ok    no unrestricted-destination egress rule"; else
  echo "  FAIL  NetworkPolicy permits egress anywhere: $allow_all_out" >&2; fail=$((fail + 1)); fi

[ "$fail" -eq 0 ] || { echo "FATAL: $fail guardrail check(s) failed — the tenant boundary is not intact." >&2; exit 1; }

echo "=== fullfunding tenant deploy complete (tenant=$TENANT env=$ENVIRONMENT) ==="
