#!/usr/bin/env bash
# pin-and-apply.sh <manifest> <registry> [dry-run-flag]
#
# Digest-pin every image (any tag, e.g. :prod) in the manifest against the registry, write the pinned
# render to .jenkins-tmp/, and kubectl apply it. A floating tag is NEVER applied
# (Codex D4): if the digest cannot be resolved, this fails closed.

set -euo pipefail
MANIFEST=$1
REGISTRY=$2
DRY=${3:-}

mkdir -p .jenkins-tmp
OUT=".jenkins-tmp/$(basename "$MANIFEST" .yaml)-pinned.yaml"
cp "$MANIFEST" "$OUT"

# every image ref of the form <registry>/<name>:<tag>
# NOTE: no pipe-into-while (a subshell would swallow the fail-closed exit); read from
# process substitution so a resolution failure aborts THIS script before any apply.
while read -r line; do
  ref=${line#image: }
  name=${ref#${REGISTRY}/}; name=${name%%:*}
  tag=${ref##*:}
  digest=$(curl -sfI \
    -H 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json' \
    "http://${REGISTRY}/v2/${name}/manifests/${tag}" \
    | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
  if [ -z "$digest" ]; then
    echo "FAIL-CLOSED: no digest for ${name}:${tag} in ${REGISTRY}" >&2
    exit 1
  fi
  sed -i '' "s|image: ${REGISTRY}/${name}:${tag}|image: ${REGISTRY}/${name}@${digest}|g" "$OUT" 2>/dev/null \
    || sed -i "s|image: ${REGISTRY}/${name}:${tag}|image: ${REGISTRY}/${name}@${digest}|g" "$OUT"
  echo "pinned ${name}:${tag} -> ${digest}"
done < <(grep -oE "image: ${REGISTRY}/[a-z0-9._-]+:[a-zA-Z0-9._-]+" "$MANIFEST" | sort -u)

# belt-and-braces: EVERY image line in the pinned render must carry @sha256 —
# regardless of registry or ref shape (Codex #11: the narrow regex above must
# never be the only gate).
if grep -E "^[[:space:]]*(- )?image:" "$OUT" | grep -vE "@sha256:[0-9a-f]{64}([\"']?)$" | grep -q .; then
  echo "FAIL-CLOSED: image ref without a full anchored sha256 digest remains in $OUT:" >&2
  grep -nE "^[[:space:]]*(- )?image:" "$OUT" | grep -vE "@sha256:[0-9a-f]{64}([\"']?)$" >&2
  exit 1
fi

# jq parses the applied render and the pod listing in the verification below. Check it BEFORE
# the apply, so a missing tool can never leave the cluster mutated with the proof step unrunnable.
if [ -z "$DRY" ] && ! command -v jq >/dev/null 2>&1; then
  echo "FAIL-CLOSED: jq is required to prove the running pods carry the pinned digest, and is not on PATH" >&2
  exit 1
fi

kubectl apply ${DRY} -f "$OUT"

# wait for each Deployment, then PROVE the running pods carry the pinned digest
# (the silent-stale-build trap: a green rollout is not proof — assert imageID==digest,
# same guarantee as the org's build->deploy §13.3 gate; fail-closed on any mismatch).
if [ -z "$DRY" ]; then
  # Capture the applied objects FIRST, in their own command substitution, so a kubectl failure
  # aborts here under set -e (a pipe would hand us the FILTER's exit status, and process
  # substitution swallows the producer's altogether; Codex #1). Only the captured text is then
  # filtered — jq/grep finding nothing in it is a fact about the render, not a hidden tool error.
  # A manifest with services must yield >=1 Deployment.
  # -o json, not -o name: the SAME render is also the only honest source of WHAT each pod must be
  # carrying — per Deployment, the name of every app container and the digest that container was
  # pinned to. The old code derived one `expected` for the whole file with
  # `grep -oE "image: …" | head -1`, so every Deployment and every container was judged against the
  # FIRST image line in the file; a manifest with two Deployments on different images, or with an
  # init container declared above the app container (es-feed-gateway.yaml), would have compared a
  # correct pod against another container's digest.
  applied_json=$(kubectl -n options-edge apply --dry-run=client -f "$OUT" -o json)
  # one "<deployment> <container> <digest>" line per app container of each Deployment in the render
  pinned=$(printf '%s' "$applied_json" | jq -r '
    (if .kind == "List" then (.items // []) else [.] end)
    | .[] | select(.kind == "Deployment")
    | .metadata.name as $d
    | (.spec.template.spec.containers // [])[]
    | ((.image | split("@")) as $p
       | "\($d) \(.name) \(if ($p | length) == 2 then $p[1] else "UNPINNED" end)")')
  deploys=$(printf '%s\n' "$pinned" | awk 'NF {print $1}' | sort -u)
  if [ -z "$deploys" ]; then
    echo "IMAGE VERIFY FAILED: no Deployment found in $OUT (manifest has none)" >&2
    exit 1
  fi
  if printf '%s\n' "$pinned" | grep -q ' UNPINNED$'; then
    echo "IMAGE VERIFY FAILED: a container in $OUT carries no @sha256 digest after pinning:" >&2
    printf '%s\n' "$pinned" | grep ' UNPINNED$' >&2
    exit 1
  fi
  while read -r name; do
    [ -n "$name" ] || continue
    # Read the DESIRED replica count once, fail-closed. An INTENTIONALLY zero-replica Deployment
    # has no pods, so there is neither a rollout to wait for nor a running imageID to prove.
    # Without this branch the pod listing below yields nothing and the check fail-closes AFTER
    # `kubectl apply` has already mutated the cluster — which would (a) make every zero-replica
    # deploy exit red, and (b) scale an already-running Deployment to zero and then abort, leaving
    # a wanted workload down. The digest guarantee is NOT weakened: the manifest was already proven
    # fully digest-pinned above, and nothing is running that could be stale.
    want=$(kubectl -n options-edge get "deployment/$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
    case "$want" in
      0) echo "image verify: $name is at replicas 0 by design — manifest is digest-pinned, no rollout or running pod to assert"
         continue ;;
      ''|*[!0-9]*) echo "IMAGE VERIFY FAILED: cannot read desired replicas for $name (got '$want')" >&2; exit 1 ;;
    esac
    # 600s mirrors the standalone path's default (service-deploy.sh ROLLOUT_TIMEOUT).
    # The old 240s was shorter than documented startup allowances (e.g. context-tape's
    # session backfill), so a valid slow start failed the build AFTER the apply had
    # already mutated the cluster.
    kubectl -n options-edge rollout status "deployment/$name" --timeout=600s || {
      echo "ROLLOUT FAILED: deployment/$name (manifest $MANIFEST)" >&2; exit 1; }
    pods_json=$(kubectl -n options-edge get pods -l "app.kubernetes.io/name=${name}" -o json) || {
      echo "IMAGE VERIFY FAILED: cannot list the pods of $name after the rollout" >&2; exit 1; }
    # WHICH pods this judges. The selector follows scripts/ops/zerodte-writer-activate.sh:371 — the
    # jq form `select(.metadata.deletionTimestamp == null)`, `select(.status.phase == "Running")`, a
    # Ready condition, and `select(.name == $c)` so only the NAMED container's imageID counts —
    # rather than service-deploy.sh's pod-template-hash scoping, because that one needs a second
    # ReplicaSet listing and yq while this script already has the render and jq, and the fact it
    # must exclude is simply "this pod is on its way out".
    #   `kubectl rollout status` returns as soon as the NEW ReplicaSet is complete, but the
    #   OUTGOING pod — already carrying a deletionTimestamp — still matches the bare
    #   app.kubernetes.io/name selector for several more seconds. Comparing ITS digest against the
    #   new pin failed two green rollouts of tape-zones-service on 2026-10-13 (es4-deploy #768 and
    #   #769) with a false "STALE IMAGE"; #769 pinned ae5cf68d, the live pod carried ae5cf68d on
    #   both spec.image and status.imageID, and the build reported 827b7a98 — the digest of the
    #   terminating pod of a ReplicaSet already scaled to zero.
    # The assertion is NOT weakened and there is no retry-until-it-goes-away: EVERY live (not
    # deleting) Running and Ready pod must carry the pin on the named container, and at least one
    # must exist. A genuinely stale pod is live, Running and Ready, so it still fails the build.
    live_filter='.items[]
      | select(.metadata.deletionTimestamp == null)
      | select(.status.phase == "Running")
      | select(([(.status.conditions // [])[] | select(.type == "Ready" and .status == "True")] | length) >= 1)'
    live=$(printf '%s' "$pods_json" | jq -r "[$live_filter] | length")
    if [ "${live:-0}" -lt 1 ]; then
      echo "IMAGE VERIFY FAILED: $name has no live (not deleting) Running and Ready pod to prove the pinned digest on" >&2
      exit 1
    fi
    while read -r pdeploy pcontainer pdigest; do
      [ "$pdeploy" = "$name" ] || continue
      bad=$(printf '%s' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest" "[$live_filter"'
        | {pod: .metadata.name,
           id: (([(.status.containerStatuses // [])[] | select(.name == $c) | (.imageID // "")] | first) // "<no containerStatus>")}
        | select((((.id | endswith("@" + $dg)) or (.id == $dg))) | not)
        | "\(.pod) runs \(if .id == "" then "<no imageID yet>" else .id end)"] | .[]')
      if [ -n "$bad" ]; then
        echo "IMAGE VERIFY FAILED: $name container $pcontainer pinned $pdigest (STALE IMAGE)" >&2
        printf '%s\n' "$bad" | sed 's/^/  /' >&2
        exit 1
      fi
      echo "verified $name container $pcontainer runs pinned digest $pdigest on all $live live Running+Ready pod(s)"
    done <<< "$pinned"
  done <<< "$deploys"
fi
