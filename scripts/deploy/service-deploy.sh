#!/usr/bin/env bash
# STANDALONE SERVICE DEPLOY (standalone-service-deployment design §3.2 / §13).
#
# Deploys ONE service as a single unit — the fast path that ships a service change
# without waiting for a full build of everything. Renders
# k8s/services/$SERVICE/overlays/$ENVIRONMENT, digest-pins the service image, applies
# ONLY that service's workload, waits for rollout, then health-gates the result.
#
# Production-grade requirements implemented here:
#   §13.2 rollback  — records the previously-running image ref BEFORE the rollout and
#                     prints the exact re-point + `rollout undo` commands (rollback never
#                     rebuilds an image: it re-points to the recorded immutable digest).
#   §13.3 health    — after `rollout status`: running imageID must contain the pinned
#                     digest, restartCount must be 0 on the new pods, and an optional
#                     HEALTH_URL must answer 200.
#   §13.5 blast     — the rendered docs may contain ONLY kinds owned by a service
#                     (Deployment/Service/HPA/ServiceMonitor/Ingress). PVCs, ConfigMaps,
#                     Secrets, Namespaces, CronJobs → FATAL (they belong to common-infra).
#   §13.7 immutable — the applied Deployment is digest-pinned (@sha256), never a bare tag.
#
# Env:
#   SERVICE          service name = dir under k8s/services (required, e.g. web)
#   ENVIRONMENT      dev | production | experiment (required)
#   DEPLOY_DRY_RUN   true|false (default false)
#   HEALTH_URL       optional http(s) URL that must answer 200 after rollout
#   KUBECONFIG       deployer kubeconfig (set by the Jenkins job from oeProfile)
#   DEPLOY_PLATFORM  image platform for digest resolution (default: env-derived)
#   REGISTRY_SCHEME  http for the insecure local registries (default http)
#   WORK_DIR         scratch dir (default: mktemp)
set -euo pipefail

SERVICE="${SERVICE:?SERVICE must be set (a dir under k8s/services)}"
ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT must be set (dev|production|experiment)}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}"
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK_DIR"
NAMESPACE="options-edge"
OVERLAY="k8s/services/${SERVICE}/overlays/${ENVIRONMENT}"

[ -d "$OVERLAY" ] || { echo "FATAL: no overlay $OVERLAY for SERVICE=$SERVICE ENVIRONMENT=$ENVIRONMENT" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

if [ -z "${DEPLOY_PLATFORM:-}" ]; then
  if [ "$ENVIRONMENT" = "dev" ]; then DEPLOY_PLATFORM="linux/arm64"; else DEPLOY_PLATFORM="linux/amd64"; fi
fi
export DEPLOY_PLATFORM
export REGISTRY_SCHEME="${REGISTRY_SCHEME:-http}"

RENDER="$WORK_DIR/${SERVICE}-${ENVIRONMENT}.yaml"

echo "=== service-deploy: render $OVERLAY (platform=$DEPLOY_PLATFORM) ==="
kubectl kustomize "$OVERLAY" >"$RENDER"

# --- §13.5 blast-radius guard: only service-owned kinds may be applied ----------------
bad_kinds="$(yq -r '.kind' "$RENDER" | grep -v '^---$' | sort -u \
  | { grep -vE '^(Deployment|Service|HorizontalPodAutoscaler|ServiceMonitor|Ingress)$' || true; })"
if [ -n "$bad_kinds" ]; then
  echo "FATAL: $OVERLAY renders kinds a per-service deploy must NOT own (they belong to common-infra):" >&2
  printf '  %s\n' $bad_kinds >&2
  exit 1
fi

# --- name-level blast radius (Codex): every rendered resource must be REGISTERED to
# this service in services.yaml — kind-level alone would let another service's
# Deployment slip through a mis-generated overlay.
ALLOWED="$(yq -r ".services[] | select(.name == \"$SERVICE\") | .deployments[]" services.yaml)"
[ -n "$ALLOWED" ] || { echo "FATAL: $SERVICE has no deployments registered in services.yaml" >&2; exit 1; }
for n in $(yq -r '.metadata.name' "$RENDER" | grep -v '^---$' | sort -u); do
  printf '%s\n' "$ALLOWED" | grep -qx "$n" || {
    echo "FATAL: overlay renders resource '$n' NOT registered to service '$SERVICE' in services.yaml (blast radius)" >&2
    exit 1
  }
done

# A service may own MULTIPLE Deployments (raw-to-display x2, classifiers x2, ...);
# they share one image, and every one is pinned/rolled/gated below.
DEPLOYMENTS="$(yq -r 'select(.kind == "Deployment") | .metadata.name' "$RENDER" | grep -v '^---$')"
DEPLOYMENT="$(printf '%s\n' "$DEPLOYMENTS" | head -1)"
CONTAINER="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].name' "$RENDER" | grep -v '^---$' | head -1)"
MUTABLE_IMAGE="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].image' "$RENDER" | grep -v '^---$' | head -1)"
[ -n "$DEPLOYMENT" ] && [ -n "$MUTABLE_IMAGE" ] || { echo "FATAL: could not extract Deployment/image from the render" >&2; exit 1; }
if [ "$(printf '%s\n' "$DEPLOYMENTS" | sort)" != "$(printf '%s\n' "$ALLOWED" | sort)" ]; then
  echo "FATAL: rendered Deployments do not exactly match services.yaml for '$SERVICE'" >&2
  echo "  rendered:   $(printf '%s ' $DEPLOYMENTS)" >&2
  echo "  registered: $(printf '%s ' $ALLOWED)" >&2
  exit 1
fi
echo "service=$SERVICE deployments=[$(printf '%s' "$DEPLOYMENTS" | tr '\n' ' ')] container=$CONTAINER image=$MUTABLE_IMAGE"

# --- §13.7 digest-pin the image (fail-closed via the shared resolver) -----------------
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" || {
  echo "FATAL: cannot resolve registry digest for $MUTABLE_IMAGE (is the image built + pushed for $ENVIRONMENT?)" >&2
  exit 1
}
echo "pinned $MUTABLE_IMAGE -> $PINNED_IMAGE"
PINNED_IMAGE="$PINNED_IMAGE" yq -i \
  "(. | select(.kind == \"Deployment\") | .spec.template.spec.containers[0].image) |= strenv(PINNED_IMAGE)" \
  "$RENDER"
unpinned="$(yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[].image' "$RENDER" \
  | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
[ -z "$unpinned" ] || { echo "FATAL: rendered image not digest-pinned: $unpinned" >&2; exit 1; }

# --- §13.2 record the CURRENT image for rollback BEFORE mutating -----------------------
PREV_FILE="$WORK_DIR/${SERVICE}-${ENVIRONMENT}-previous.txt"; : >"$PREV_FILE"
for dep in $DEPLOYMENTS; do
  prev="$(kubectl -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")"
  printf '%s\t%s\n' "$dep" "$prev" >>"$PREV_FILE"
  if [ -n "$prev" ]; then
    echo "previous image (rollback target) $dep: $prev"
  else
    echo "no live deployment yet for $dep (first deploy) — rollback target n/a"
  fi
done

if [ "$DEPLOY_DRY_RUN" = "true" ]; then
  echo "=== DEPLOY_DRY_RUN=true: server-side validation only ==="
  kubectl apply --dry-run=server -f "$RENDER"
  echo "dry-run OK — would apply $(grep -c '^kind:' "$RENDER") docs and roll deployment/$DEPLOYMENT to $PINNED_IMAGE"
  exit 0
fi

echo "=== apply (service-scoped) ==="
kubectl apply -f "$RENDER"
echo "=== rollout (all deployments of this service) ==="
for dep in $DEPLOYMENTS; do
  kubectl -n "$NAMESPACE" rollout status "deployment/$dep" --timeout=600s
done

# --- §13.3 post-rollout health gate ---------------------------------------------------
echo "=== health gate ==="
PINNED_DIGEST="${PINNED_IMAGE#*@}"
DESIRED_TOTAL=0
for dep in $DEPLOYMENTS; do
  r="$(kubectl -n "$NAMESPACE" get deployment "$dep" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  DESIRED_TOTAL=$((DESIRED_TOTAL + ${r:-0}))
done
if [ "$DESIRED_TOTAL" -eq 0 ]; then
  echo "  service is scaled to 0 (temporarily disabled) — spec applied, no pods to gate."
  echo "=== $SERVICE deployed to $ENVIRONMENT (replicas 0): $PINNED_IMAGE ==="
  exit 0
fi
gate_fail=0
for dep in $DEPLOYMENTS; do
  desired="$(kubectl -n "$NAMESPACE" get deployment "$dep" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  if [ "${desired:-0}" -eq 0 ]; then
    echo "  $dep: scaled to 0 — gates skipped"
    continue
  fi
  # -o json (not jsonpath: jsonpath renders maps as `map[k:v]`, which yq can't parse)
  selector="$(kubectl -n "$NAMESPACE" get deployment "$dep" -o json \
    | yq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")')"

  running_ids="$(kubectl -n "$NAMESPACE" get pods -l "$selector" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].imageID}{"\n"}{end}')"
  if printf '%s\n' "$running_ids" | grep -q "$PINNED_DIGEST"; then
    echo "  $dep image: running imageID matches pinned digest ✓"
  else
    echo "  $dep image: FAIL — no running pod reports the pinned digest $PINNED_DIGEST" >&2
    printf '%s\n' "$running_ids" | sed 's/^/    /' >&2
    gate_fail=1
  fi

  restarts="$(kubectl -n "$NAMESPACE" get pods -l "$selector" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | sort -rn | head -1)"
  if [ "${restarts:-0}" -eq 0 ]; then
    echo "  $dep restarts: 0 ✓"
  else
    echo "  $dep restarts: FAIL — pod restartCount=$restarts" >&2
    gate_fail=1
  fi
done

if [ -n "${HEALTH_URL:-}" ]; then
  code="$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 20 "$HEALTH_URL" || echo "000")"
  if [ "$code" = "200" ]; then
    echo "  endpoint: $HEALTH_URL -> 200 ✓"
  else
    echo "  endpoint: FAIL — $HEALTH_URL -> $code" >&2
    gate_fail=1
  fi
fi

# --- §13.2 rollback instructions (always printed) --------------------------------------
echo "=== rollback (no rebuild needed — re-points to the recorded digests) ==="
while IFS=$'\t' read -r dep prev; do
  [ -n "$dep" ] || continue
  if [ -n "$prev" ]; then
    echo "  kubectl -n $NAMESPACE set image deployment/$dep $CONTAINER=$prev"
  fi
  echo "  # or: kubectl -n $NAMESPACE rollout undo deployment/$dep"
done <"$PREV_FILE"

if [ "$gate_fail" -ne 0 ]; then
  echo "FATAL: health gate failed after rollout — use the rollback command above." >&2
  exit 1
fi
echo "=== $SERVICE deployed to $ENVIRONMENT: $PINNED_IMAGE ==="
