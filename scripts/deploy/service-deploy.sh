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
#   ROLLOUT_TIMEOUT  kubectl rollout timeout override (default 600s; production
#                    market-carry defaults to 7260s for its 4.3M-record restore)
#   WORK_DIR         scratch dir (default: mktemp)
set -euo pipefail

SERVICE="${SERVICE:?SERVICE must be set (a dir under k8s/services)}"
ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT must be set (dev|production|experiment)}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}"
if [ -z "${ROLLOUT_TIMEOUT:-}" ]; then
  if [ "$SERVICE" = "market-carry" ] && [ "$ENVIRONMENT" = "production" ]; then
    ROLLOUT_TIMEOUT="7260s"
  else
    ROLLOUT_TIMEOUT="600s"
  fi
fi
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
mkdir -p "$WORK_DIR"
OVERLAY="k8s/services/${SERVICE}/overlays/${ENVIRONMENT}"

[ -d "$OVERLAY" ] || { echo "FATAL: no overlay $OVERLAY for SERVICE=$SERVICE ENVIRONMENT=$ENVIRONMENT" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

# --- WHERE does this service deploy? -------------------------------------------------
# The namespace comes from the registry, not from a constant here. Until 2026-08-08 this
# was hard-coded to options-edge, which was fine while that was the only namespace — now
# `fullfunding` exists and a build has to know which application it belongs to.
#
# Unset means options-edge, so all 51 pre-existing services keep behaving exactly as before
# and nothing has to be back-filled.
NAMESPACE="$(yq -r ".services[] | select(.name == \"$SERVICE\") | .namespace // \"options-edge\"" services.yaml | head -1)"
[ -n "$NAMESPACE" ] || { echo "FATAL: $SERVICE is not registered in services.yaml — register it before deploying" >&2; exit 1; }
echo "=== service-deploy: $SERVICE -> namespace $NAMESPACE (env=$ENVIRONMENT) ==="

# The namespace must already exist. Creating one is a platform action with its own job and
# its own credential (see k8s/tenants/<tenant>/bootstrap), never a side effect of a service
# deploy — otherwise a typo in the registry silently creates a namespace and deploys into it.
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || { echo "FATAL: namespace '$NAMESPACE' does not exist. A service deploy never creates one — run the owning tenant/infra job first." >&2; exit 1; }

if [ -z "${DEPLOY_PLATFORM:-}" ]; then
  if [ "$ENVIRONMENT" = "dev" ]; then DEPLOY_PLATFORM="linux/arm64"; else DEPLOY_PLATFORM="linux/amd64"; fi
fi
export DEPLOY_PLATFORM
export REGISTRY_SCHEME="${REGISTRY_SCHEME:-http}"

# --- PGL-072: the public Gamma Lab may not scale up without its evidence ---------------
# This runs HERE, on the workload's ACTUAL apply path, not in common-infra. The gate first lived in
# Jenkinsfile.common-infra — and when the Deployment correctly moved out of the infra component into
# this service slice, the gate stayed behind and stopped guarding anything. A control on a path the
# thing does not travel is not a control.
RENDER="$WORK_DIR/${SERVICE}-${ENVIRONMENT}.yaml"

echo "=== service-deploy: render $OVERLAY (platform=$DEPLOY_PLATFORM) ==="
kubectl kustomize "$OVERLAY" >"$RENDER"

# --- §13.5 blast-radius guard: only service-owned kinds may be applied ----------------
bad_kinds="$(yq -r '.kind' "$RENDER" | grep -v '^---$' | sort -u \
  | { grep -vE '^(Deployment|Service|HorizontalPodAutoscaler|ServiceMonitor|Ingress)$' || true; })"
# --- the render must target the registered namespace, and nothing else ---------------
# A service whose manifests say options-edge but whose registry entry says fullfunding (or the
# reverse) would deploy into the wrong application. Catch it here, before any apply: the whole
# point of separate namespaces is that a mistake in one cannot reach the other.
wrong_ns="$(yq -r "select(.metadata.namespace != null and .metadata.namespace != \"$NAMESPACE\") | .kind + \"/\" + .metadata.name + \" -> \" + .metadata.namespace" "$RENDER" | { grep -vE '^(---)?$' || true; })"
if [ -n "$wrong_ns" ]; then
  echo "FATAL: $SERVICE is registered for namespace '$NAMESPACE' but its render targets another:" >&2
  printf '%s\n' "$wrong_ns" >&2
  echo "       Fix the overlay or the services.yaml entry — they must agree." >&2
  exit 1
fi

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
_ctrs="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].name' "$RENDER")"
CONTAINER="$(printf '%s\n' "$_ctrs" | grep -v '^---$' | head -1)"
_imgs="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].image' "$RENDER")"
MUTABLE_IMAGE="$(printf '%s\n' "$_imgs" | grep -v '^---$' | head -1)"
# The exact image string as it appears in the render (before any production remap below).
# A Deployment may run MORE THAN ONE container on this shared image (e.g. short-premium-agent's
# agent-a + agent-b); the pin step below rewrites EVERY container carrying this ref, not just [0].
RENDER_IMAGE0="$MUTABLE_IMAGE"
[ -n "$DEPLOYMENT" ] && [ -n "$MUTABLE_IMAGE" ] || { echo "FATAL: could not extract Deployment/image from the render" >&2; exit 1; }
if [ "$(printf '%s\n' "$DEPLOYMENTS" | sort)" != "$(printf '%s\n' "$ALLOWED" | sort)" ]; then
  echo "FATAL: rendered Deployments do not exactly match services.yaml for '$SERVICE'" >&2
  echo "  rendered:   $(printf '%s ' $DEPLOYMENTS)" >&2
  echo "  registered: $(printf '%s ' $ALLOWED)" >&2
  exit 1
fi
# --- env image remap (Codex round 3): generated slices mirror the MONOLITH render, which
# carries the base (dev-registry) image ref pre-pin. The authoritative per-env mutable ref
# lives in image-tags/${ENVIRONMENT}.yaml — remap BEFORE pinning so a production fast-path
# deploy can never pin (and roll out) the dev registry's image. Matched by the service's
# registered image basename; fail-closed if the env has no entry for it.
IMAGE_BASENAME="$(yq -r ".services[] | select(.name == \"$SERVICE\") | .image" services.yaml)"
[ -n "$IMAGE_BASENAME" ] && [ "$IMAGE_BASENAME" != "null" ] || { echo "FATAL: $SERVICE has no image registered in services.yaml" >&2; exit 1; }
ENV_IMAGE=""
if [ -f "image-tags/${ENVIRONMENT}.yaml" ]; then
  ENV_IMAGE="$(yq -r ".images | to_entries[] | select(.value | test(\"/${IMAGE_BASENAME}:\")) | .value" "image-tags/${ENVIRONMENT}.yaml" | awk 'NR==1')"
fi
if [ -n "$ENV_IMAGE" ] && [ "$ENV_IMAGE" != "null" ]; then
  if [ "$ENVIRONMENT" = "production" ] && [ "$ENV_IMAGE" != "$MUTABLE_IMAGE" ]; then
    echo "remapping render image -> env image: $MUTABLE_IMAGE -> $ENV_IMAGE"
    MUTABLE_IMAGE="$ENV_IMAGE"
  elif [ "$ENV_IMAGE" != "$MUTABLE_IMAGE" ]; then
    # Non-production renders are ALREADY env-correct by overlay construction (Codex round 7:
    # the dev overlay remaps every image to the local dev registry, while legacy
    # image-tags/dev.yaml entries still carry the remote registry). Remapping here would
    # swap a correct local ref for the remote one — so outside production the mapping is
    # informational only and the render ref wins.
    echo "INFO: image-tags/${ENVIRONMENT}.yaml maps '$IMAGE_BASENAME' to $ENV_IMAGE; keeping the env-correct render ref $MUTABLE_IMAGE (mapping is authoritative only for production)."
  fi
elif [ "$ENVIRONMENT" = "production" ]; then
  # The render ref is the BASE (dev-registry) ref by design; deploying it to production is
  # exactly the dev-image bug. No mapping -> no production fast path: fail closed.
  echo "FATAL: image-tags/production.yaml has no entry for image '$IMAGE_BASENAME' (service $SERVICE)." >&2
  echo "       Add the production mapping before using the per-service fast path for this service." >&2
  exit 1
else
  echo "WARN: image-tags/${ENVIRONMENT}.yaml has no entry for '$IMAGE_BASENAME'; using the render ref (dev-parity registries only)."
fi
echo "service=$SERVICE deployments=[$(printf '%s' "$DEPLOYMENTS" | tr '\n' ' ')] container=$CONTAINER image=$MUTABLE_IMAGE"

# --- PVC preflight (review P0): slices may NOT carry PVCs (blast radius — common-infra
# owns them), so any claim the workload references must ALREADY exist or the rollout dies
# on unbound claims. Fail early with a pointer to the common-infra job.
for _claim in $(yq -r 'select(.kind == "Deployment") | .spec.template.spec.volumes[]?.persistentVolumeClaim.claimName' "$RENDER" | grep -v '^---$' | grep -v '^null$' | sort -u); do
  if ! kubectl -n "$NAMESPACE" get pvc "$_claim" >/dev/null 2>&1; then
    echo "FATAL: PVC '$_claim' referenced by $SERVICE does not exist in $NAMESPACE." >&2
    echo "       Run the common-infra deploy (Jenkinsfile.common-infra) for $ENVIRONMENT first." >&2
    exit 1
  fi
done

# --- §13.7 digest-pin the image (fail-closed via the shared resolver) -----------------
. scripts/deploy/pin-image.sh
PINNED_IMAGE="$(pin_ref "$MUTABLE_IMAGE")" || {
  echo "FATAL: cannot resolve registry digest for $MUTABLE_IMAGE (is the image built + pushed for $ENVIRONMENT?)" >&2
  exit 1
}
echo "pinned $MUTABLE_IMAGE -> $PINNED_IMAGE"
# Pin EVERY container that carries the shared service image (RENDER_IMAGE0), not just
# containers[0] — a single-container Deployment is unchanged, while a multi-container pod
# (agent-a + agent-b) gets its sidecar pinned too instead of failing the digest-pin gate below.
PINNED_IMAGE="$PINNED_IMAGE" RENDER_IMAGE0="$RENDER_IMAGE0" yq -i \
  "(. | select(.kind == \"Deployment\") | .spec.template.spec.containers[] | select(.image == strenv(RENDER_IMAGE0)) | .image) |= strenv(PINNED_IMAGE)" \
  "$RENDER"
unpinned="$(yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[].image' "$RENDER" \
  | { grep '/options-edge-' || true; } | { grep -v '@sha256:' || true; })"
[ -z "$unpinned" ] || { echo "FATAL: rendered image not digest-pinned: $unpinned" >&2; exit 1; }

# --- PGL-072: the public Gamma Lab may not scale up without evidence for THIS digest ---
# Placed HERE, on the workload's real apply path and AFTER the image has been remapped for the
# environment and pinned — not against the overlay. The overlay carries the dev-registry ref, which
# production remaps and re-resolves, so an overlay-based check could accept evidence for one digest
# while a different one was applied moments later. This reads the exact manifest about to be applied.
if [ "$SERVICE" = "bleedingoptions-gamma-lab" ]; then
  echo "=== service-deploy: public gate (PGL-072) ==="
  bash "$(dirname "$0")/verify-public-gate.sh" --rendered "$RENDER" \
    || { echo "FATAL: the public Gamma Lab gate refused this deploy" >&2; exit 1; }
fi

# --- §13.2 record the CURRENT image for rollback BEFORE mutating -----------------------
PREV_FILE="$WORK_DIR/${SERVICE}-${ENVIRONMENT}-previous.txt"; : >"$PREV_FILE"
for dep in $DEPLOYMENTS; do
  # container name PER deployment (they can differ: hpsf-stage-a vs hpsf-stage-b)
  _c="$(yq -r "select(.kind == \"Deployment\" and .metadata.name == \"$dep\") | .spec.template.spec.containers[0].name" "$RENDER")"
  ctr="$(printf '%s\n' "$_c" | grep -v '^---$' | head -1)"
  prev="$(kubectl -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")"
  printf '%s\t%s\t%s\n' "$dep" "$ctr" "$prev" >>"$PREV_FILE"
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
  kubectl -n "$NAMESPACE" rollout status "deployment/$dep" --timeout="$ROLLOUT_TIMEOUT"
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
  if [ "$SERVICE" = "vix-option-inteligence" ]; then
    echo "Removing legacy zero-dte-intelligence Kubernetes identity after replacement spec applied."
    kubectl -n "$NAMESPACE" delete deployment zero-dte-intelligence-service --ignore-not-found
    kubectl -n "$NAMESPACE" delete service zero-dte-intelligence-service --ignore-not-found
  fi
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
  # `rollout status` returns the moment the Deployment is Available, but a web
  # container can accept the TCP connection a few seconds before it serves a full
  # HTTP response (curl 52 / empty reply). A single-shot check false-FAILs on that
  # startup race and prints a rollback for a deploy that is actually healthy. Retry
  # with a bounded grace window: still must reach 200, just tolerates the race.
  code=""
  for attempt in $(seq 1 "${HEALTH_RETRIES:-10}"); do
    code="$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 20 "$HEALTH_URL" || echo "000")"
    [ "$code" = "200" ] && break
    echo "  endpoint: $HEALTH_URL -> $code (attempt $attempt, retrying) …"
    sleep "${HEALTH_RETRY_DELAY:-3}"
  done
  if [ "$code" = "200" ]; then
    echo "  endpoint: $HEALTH_URL -> 200 ✓"
  else
    echo "  endpoint: FAIL — $HEALTH_URL -> $code (after ${HEALTH_RETRIES:-10} attempts)" >&2
    gate_fail=1
  fi
fi

# --- §13.2 rollback instructions (always printed) --------------------------------------
echo "=== rollback (no rebuild needed — re-points to the recorded digests) ==="
while IFS=$'\t' read -r dep ctr prev; do
  [ -n "$dep" ] || continue
  if [ -n "$prev" ]; then
    echo "  kubectl -n $NAMESPACE set image deployment/$dep $ctr=$prev"
  fi
  echo "  # or: kubectl -n $NAMESPACE rollout undo deployment/$dep"
done <"$PREV_FILE"

if [ "$gate_fail" -ne 0 ]; then
  echo "FATAL: health gate failed after rollout — use the rollback command above." >&2
  exit 1
fi
if [ "$SERVICE" = "vix-option-inteligence" ]; then
  # Rename migration: delete the old identity only after the replacement passed its digest,
  # restart and health gates. This prevents duplicate consumers while preserving rollback
  # safety during the new rollout itself.
  echo "Removing legacy zero-dte-intelligence Kubernetes identity after successful replacement."
  kubectl -n "$NAMESPACE" delete deployment zero-dte-intelligence-service --ignore-not-found
  kubectl -n "$NAMESPACE" delete service zero-dte-intelligence-service --ignore-not-found
fi
echo "=== $SERVICE deployed to $ENVIRONMENT: $PINNED_IMAGE ==="
