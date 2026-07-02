#!/usr/bin/env bash
# COMMON-INFRA DEPLOY (standalone-service-deployment design §3.1 / §13.8).
#
# Applies k8s/infra/overlays/$ENVIRONMENT — the shared layer every service depends on
# (namespace, shared ConfigMaps, ALL Kafka-Streams state PVCs, ops CronJobs, prod
# Keycloak). Per-service deploys NEVER touch these resources (§13.5); this script is
# their single owner.
#
# Safety gates (§13.8 — infra is rare but dangerous):
#   * PVCs are CREATE-ONLY: an existing PVC is never re-applied or patched. If the
#     rendered storageClassName disagrees with the live immutable class, we FAIL LOUDLY
#     instead of letting the API reject a doomed patch mid-apply.
#   * Everything else is diffed first (kubectl diff) so the log shows exactly what will
#     change, then applied.
#   * DEPLOY_DRY_RUN=true renders + diffs + server-side validates and changes NOTHING.
#   * No Kafka-topic mutation here: topic create/reconcile stays with the existing
#     pipeline stages until the topic-reconcile phase of the design lands.
#
# Env:
#   ENVIRONMENT      dev | production | experiment   (required)
#   DEPLOY_DRY_RUN   true|false (default true — infra changes must be opted into)
#   KUBECONFIG       deployer kubeconfig (set by the Jenkins job from oeProfile)
#   WORK_DIR         scratch dir (default: mktemp)
set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT must be set (dev|production|experiment)}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-true}"
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK_DIR"
OVERLAY="k8s/infra/overlays/${ENVIRONMENT}"
NAMESPACE="options-edge"

[ -d "$OVERLAY" ] || { echo "FATAL: no infra overlay for ENVIRONMENT=$ENVIRONMENT ($OVERLAY)" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

RENDER="$WORK_DIR/infra-${ENVIRONMENT}.yaml"
PVCS="$WORK_DIR/infra-${ENVIRONMENT}-pvcs.yaml"
REST="$WORK_DIR/infra-${ENVIRONMENT}-rest.yaml"

echo "=== common-infra: render $OVERLAY ==="
kubectl kustomize "$OVERLAY" >"$RENDER"
yq 'select(.kind == "PersistentVolumeClaim")' "$RENDER" >"$PVCS"
yq 'select(.kind != "PersistentVolumeClaim")' "$RENDER" >"$REST"

total_docs="$(grep -c '^kind:' "$RENDER" || true)"
pvc_docs="$(grep -c '^kind:' "$PVCS" || true)"
echo "rendered: $total_docs docs ($pvc_docs PVCs, $((total_docs - pvc_docs)) other)"

# --- Blast-radius sanity: infra must not carry app workloads -------------------------
# (Keycloak's Deployment/StatefulSet are the sanctioned exception — it IS infra.)
unexpected="$(yq -r 'select(.kind == "Deployment" or .kind == "StatefulSet") | .metadata.name' "$RENDER" \
  | { grep -vE '^(oe-keycloak|keycloak)' || true; })"
if [ -n "$unexpected" ]; then
  echo "FATAL: infra overlay renders app workloads (belongs in a service slice or k8s/base):" >&2
  printf '%s\n' "$unexpected" >&2
  exit 1
fi

# --- PVC gate: create-only, immutable-class verification -----------------------------
echo "=== PVC gate (create-only; never patch an existing claim) ==="
pvc_create=() ; pvc_skip=0 ; pvc_conflict=0
while IFS=$'\t' read -r name manifest_sc; do
  [ -n "$name" ] || continue
  # Every env-rendered infra PVC MUST pin an explicit storageClassName: an empty class
  # would let the cluster's default provisioner pick one silently — the exact ambiguity
  # (manifest omits vs live defaulted class) that produced the immutable-field failures.
  if [ -z "$manifest_sc" ]; then
    echo "  CONFLICT $name: rendered PVC has NO storageClassName — the env overlay must pin the live class explicitly" >&2
    pvc_conflict=$((pvc_conflict + 1))
    continue
  fi
  if live_sc="$(kubectl -n "$NAMESPACE" get pvc "$name" -o jsonpath='{.spec.storageClassName}' 2>/dev/null)"; then
    if [ "$manifest_sc" != "$live_sc" ]; then
      echo "  CONFLICT $name: manifest storageClassName='$manifest_sc' but live immutable class='${live_sc:-<empty>}'" >&2
      pvc_conflict=$((pvc_conflict + 1))
    else
      pvc_skip=$((pvc_skip + 1))
    fi
  else
    pvc_create+=("$name")
  fi
done < <(yq -r '[.metadata.name, (.spec.storageClassName // "")] | @tsv' "$PVCS")
echo "  existing (skipped): $pvc_skip ; to create: ${#pvc_create[@]} ; class conflicts: $pvc_conflict"
if [ "$pvc_conflict" -gt 0 ]; then
  echo "FATAL: $pvc_conflict PVC(s) disagree with the live immutable storageClassName — fix the overlay (or the env) before deploying infra." >&2
  exit 1
fi

# --- Diff the mutable remainder so the log shows exactly what changes ----------------
echo "=== diff (non-PVC infra) ==="
set +e
kubectl diff -f "$REST" >"$WORK_DIR/infra-diff.txt" 2>&1
diff_rc=$?
set -e
if [ "$diff_rc" -eq 0 ]; then
  echo "no changes."
elif [ "$diff_rc" -eq 1 ]; then
  cat "$WORK_DIR/infra-diff.txt"
else
  echo "WARN: kubectl diff failed (rc=$diff_rc) — continuing to server-side validation:" >&2
  tail -20 "$WORK_DIR/infra-diff.txt" >&2
fi

if [ "$DEPLOY_DRY_RUN" = "true" ]; then
  echo "=== DEPLOY_DRY_RUN=true: server-side validation only, nothing changes ==="
  kubectl apply --dry-run=server -f "$REST"
  for name in ${pvc_create[@]+"${pvc_create[@]}"}; do
    yq "select(.metadata.name == \"$name\")" "$PVCS" | kubectl create --dry-run=server -f -
  done
  echo "dry-run OK — would create ${#pvc_create[@]} PVC(s) and apply $((total_docs - pvc_docs)) infra docs."
  exit 0
fi

echo "=== apply (non-PVC infra) ==="
kubectl apply -f "$REST"

if [ "${#pvc_create[@]}" -gt 0 ]; then
  echo "=== create missing PVCs (${#pvc_create[@]}) ==="
  for name in "${pvc_create[@]}"; do
    yq "select(.metadata.name == \"$name\")" "$PVCS" | kubectl create -f -
    echo "  created pvc/$name"
  done
else
  echo "=== no missing PVCs ==="
fi

echo "=== common-infra deploy complete (env=$ENVIRONMENT) ==="
