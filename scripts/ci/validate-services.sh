#!/usr/bin/env bash
# SERVICE-REGISTRY VALIDATION (standalone-service-deployment design §13.9).
#
# Fails when the repo drifts from services.yaml — the single source of truth:
#   1. COMPLETENESS: every Deployment rendered by the monolithic env overlays must be
#      registered in services.yaml (a new service that skips registration fails here —
#      the gap that let delta-flow/maxpain/integration-test reach a deploy unbuilt).
#   2. STANDALONE SLICES: every managedBy=standalone service has k8s/services/<name>
#      with an overlay per declared env, rendering ONLY service-owned kinds (§13.5).
#   3. MIRROR RULE: the standalone render of each service must be EQUIVALENT to the
#      monolithic overlay's render of the same workload — env patches replicated in the
#      service overlay cannot drift from the top-level ones.
#   4. IMAGE NAME: the image basename in the standalone render matches services.yaml.
#
# Requires: kubectl (kustomize), yq. Read-only; no cluster access needed.
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
fail=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ENVS=(dev production experiment)

echo "=== render monolithic overlays ==="
for e in "${ENVS[@]}"; do
  kubectl kustomize "k8s/overlays/$e" >"$TMP/mono-$e.yaml" || { echo "FATAL: k8s/overlays/$e does not render" >&2; exit 1; }
done

echo "=== 1) completeness: every rendered Deployment is registered ==="
yq -r '.services[].deployments[]' services.yaml | sort -u >"$TMP/registered.txt"
for e in "${ENVS[@]}"; do
  yq -r 'select(.kind=="Deployment") | .metadata.name' "$TMP/mono-$e.yaml"
done | grep -v '^---$' | sort -u >"$TMP/rendered.txt"
# Keycloak & friends are infra-owned, not service-registry entries.
unregistered="$(comm -23 "$TMP/rendered.txt" "$TMP/registered.txt" | { grep -vE '^(oe-keycloak|keycloak)' || true; })"
if [ -n "$unregistered" ]; then
  echo "FAIL: Deployments rendered by the overlays but NOT registered in services.yaml:" >&2
  printf '  %s\n' $unregistered >&2
  fail=1
else
  echo "ok: all $(wc -l <"$TMP/rendered.txt" | tr -d ' ') rendered Deployments are registered"
fi

echo "=== 2+3+4) standalone slices: structure, blast radius, mirror rule, image name ==="
while IFS= read -r name; do
  img="$(yq -r ".services[] | select(.name == \"$name\") | .image" services.yaml)"
  for e in $(yq -r ".services[] | select(.name == \"$name\") | .envs[]" services.yaml); do
    overlay="k8s/services/$name/overlays/$e"
    if [ ! -d "$overlay" ]; then
      echo "FAIL: $name declares env '$e' but $overlay does not exist" >&2
      fail=1; continue
    fi
    if ! kubectl kustomize "$overlay" >"$TMP/svc-$name-$e.yaml" 2>"$TMP/svc-$name-$e.err"; then
      echo "FAIL: $overlay does not render:" >&2; head -3 "$TMP/svc-$name-$e.err" >&2
      fail=1; continue
    fi
    # blast radius (§13.5)
    bad="$(yq -r '.kind' "$TMP/svc-$name-$e.yaml" | grep -v '^---$' | sort -u \
      | { grep -vE '^(Deployment|Service|HorizontalPodAutoscaler|ServiceMonitor|Ingress)$' || true; })"
    if [ -n "$bad" ]; then
      echo "FAIL: $overlay renders non-service-owned kinds: $bad" >&2
      fail=1
    fi
    # image basename (§13.9)
    rendered_img="$(yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[0].image' "$TMP/svc-$name-$e.yaml" | head -1)"
    base="${rendered_img##*/}"; base="${base%%[:@]*}"
    if [ "$base" != "$img" ]; then
      echo "FAIL: $overlay image basename '$base' != services.yaml image '$img'" >&2
      fail=1
    fi
    # mirror rule: standalone render == the same workload extracted from the monolith
    for dep in $(yq -r ".services[] | select(.name == \"$name\") | .deployments[]" services.yaml); do
      yq "select(.metadata.name == \"$dep\")" "$TMP/mono-$e.yaml" \
        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-mono.yaml"
      yq "select(.metadata.name == \"$dep\")" "$TMP/svc-$name-$e.yaml" \
        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-svc.yaml"
      if ! diff -u "$TMP/mirror-mono.yaml" "$TMP/mirror-svc.yaml" >"$TMP/mirror-diff.txt"; then
        echo "FAIL: MIRROR DRIFT for $name/$e ($dep): the standalone overlay renders differently from the monolithic overlay." >&2
        echo "      Replicate the top-level env patch in $overlay (or vice versa). Diff:" >&2
        head -25 "$TMP/mirror-diff.txt" | sed 's/^/      /' >&2
        fail=1
      fi
    done
    echo "ok: $name/$e (blast radius, image, mirror)"
  done
done < <(yq -r '.services[] | select(.managedBy == "standalone") | .name' services.yaml)

if [ "$fail" -ne 0 ]; then
  echo "=== validate-services: FAILED ===" >&2
  exit 1
fi
echo "=== validate-services: OK ==="
