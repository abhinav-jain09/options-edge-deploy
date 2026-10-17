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
  # TOCTOU-close (Codex H2): if the 'Assert prod images fresh' gate already PROVED this image's
  # :prod digest against a build receipt, pin that EXACT digest — never a fresh re-resolution a
  # concurrent build could have moved between the gate and here. Steady-state images the gate
  # skipped (not in the file) fall through to a normal resolve.
  # When this pipeline set VERIFIED_DIGESTS_FILE, the freshness gate MUST have produced it. A missing
  # file means the gate did not run (or failed to write) — fail closed rather than silently fall back
  # to re-resolving :prod (which is the very TOCTOU this handoff exists to close). A present file that
  # simply lacks this image = an image the gate steady-state-skipped -> resolve it normally below.
  digest=""
  if [ -n "${VERIFIED_DIGESTS_FILE:-}" ]; then
    [ -f "${VERIFIED_DIGESTS_FILE}" ] \
      || { echo "FAIL-CLOSED: VERIFIED_DIGESTS_FILE=${VERIFIED_DIGESTS_FILE} is set but missing — the freshness gate did not run." >&2; exit 1; }
    digest=$(awk -v n="$name" '$1==n{print $2; exit}' "${VERIFIED_DIGESTS_FILE}")
    [ -n "$digest" ] && echo "using gate-verified digest for ${name}: ${digest}"
  fi
  if [ -z "$digest" ]; then
    digest=$(curl -sfI \
      -H 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json' \
      "http://${REGISTRY}/v2/${name}/manifests/${tag}" \
      | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
  fi
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
  # one "<deployment> <container> <digest>" line per container of each Deployment — init
  # containers included: es-feed-gateway.yaml runs one, its image is pinned by the gate above,
  # and a pod whose init container ran ANOTHER build is as stale as one whose app container did
  # (Codex r2 MINOR). Kubernetes forbids a name collision between the two lists, so the union of
  # containerStatuses and initContainerStatuses below resolves each name unambiguously.
  pinned=$(printf '%s' "$applied_json" | jq -r '
    (if .kind == "List" then (.items // []) else [.] end)
    | .[] | select(.kind == "Deployment")
    | .metadata.name as $d
    | ((.spec.template.spec.initContainers // []) + (.spec.template.spec.containers // []))[]
    | ((.image | split("@")) as $p
       | "\($d) \(.name) \(if ($p | length) == 2 then $p[1] else "UNPINNED" end)")')
  # one "<deployment> <replicas>" line per Deployment. The replica count comes from the render that
  # was just applied, never from the live object (Codex r1 BLOCKER 2): `kubectl apply` has already
  # written .spec.replicas from THIS manifest, so reading the live value back only opens a hole —
  # an HPA, a concurrent operator or a half-reverted reconciliation presenting 0 would skip the
  # rollout and the whole image proof for a Deployment this manifest asked to run.
  wanted=$(printf '%s' "$applied_json" | jq -r '
    (if .kind == "List" then (.items // []) else [.] end)
    | .[] | select(.kind == "Deployment")
    | "\(.metadata.name) \(.spec.replicas // 1)"')
  if [ -z "$(printf '%s\n' "$wanted" | awk 'NF')" ]; then
    echo "IMAGE VERIFY FAILED: no Deployment found in $OUT (manifest has none)" >&2
    exit 1
  fi
  if printf '%s\n' "$pinned" | grep -q ' UNPINNED$'; then
    echo "IMAGE VERIFY FAILED: a container in $OUT carries no @sha256 digest after pinning:" >&2
    printf '%s\n' "$pinned" | grep ' UNPINNED$' >&2
    exit 1
  fi
  while read -r name want; do
    [ -n "$name" ] || continue
    # An INTENTIONALLY zero-replica Deployment — one THIS MANIFEST declares at 0 (es-feed, the held
    # spread-skew stack) — has no pods, so there is neither a rollout to wait for nor a running
    # imageID to prove. Without this branch the pod listing below yields nothing and the check
    # fail-closes AFTER `kubectl apply` has already mutated the cluster, so every zero-replica
    # deploy would exit red. The digest guarantee is NOT weakened: the manifest was already proven
    # fully digest-pinned above, and nothing is running that could be stale.
    case "$want" in
      0) echo "image verify: $name is declared at replicas 0 by $MANIFEST — digest-pinned, no rollout or running pod to assert"
         continue ;;
      ''|*[!0-9]*) echo "IMAGE VERIFY FAILED: $OUT declares a non-numeric replica count for $name (got '$want')" >&2; exit 1 ;;
    esac
    # 600s mirrors the standalone path's default (service-deploy.sh ROLLOUT_TIMEOUT).
    # The old 240s was shorter than documented startup allowances (e.g. context-tape's
    # session backfill), so a valid slow start failed the build AFTER the apply had
    # already mutated the cluster.
    kubectl -n options-edge rollout status "deployment/$name" --timeout=600s || {
      echo "ROLLOUT FAILED: deployment/$name (manifest $MANIFEST)" >&2; exit 1; }

    # WHICH PODS THIS JUDGES — by OWNERSHIP, not by labels (Codex r2 MAJOR). A label selector is
    # not a statement of ownership: a pod of another controller, or a hand-made one, that happens
    # to carry app.kubernetes.io/name would be judged as this Deployment's and could both
    # false-fail the refusal and satisfy the proof, and rendering only .spec.selector.matchLabels
    # would silently drop any matchExpressions and widen the set further. So: the Deployment's own
    # uid, the ReplicaSets whose ownerReferences name it, and the pods whose ownerReferences name
    # those. (Reading the live Deployment for its UID is not the hazard BLOCKER 2 named: a uid is
    # identity, fixed for the object this script just applied, not a mutable desired state that
    # another actor could set to a value which skips the proof.)
    dep_uid=$(kubectl -n options-edge get "deployment/$name" -o jsonpath='{.metadata.uid}') || dep_uid=""
    if [ -z "$dep_uid" ]; then
      echo "IMAGE VERIFY FAILED: cannot read the uid of deployment/$name — its pods cannot be identified by ownership" >&2; exit 1
    fi
    rs_json=$(kubectl -n options-edge get rs -o json) || {
      echo "IMAGE VERIFY FAILED: cannot list the ReplicaSets of $name after the rollout" >&2; exit 1; }
    rs_uids=$(printf '%s' "$rs_json" | jq -c --arg u "$dep_uid" \
      '[.items[] | select(any((.metadata.ownerReferences // [])[]; .uid == $u)) | .metadata.uid]')
    if [ "$rs_uids" = "[]" ]; then
      echo "IMAGE VERIFY FAILED: deployment/$name owns no ReplicaSet after a completed rollout — nothing to prove the pinned digest on" >&2; exit 1
    fi
    all_pods=$(kubectl -n options-edge get pods -o json) || {
      echo "IMAGE VERIFY FAILED: cannot list the pods of $name after the rollout" >&2; exit 1; }
    pods_json=$(printf '%s' "$all_pods" | jq -c --argjson rs "$rs_uids" \
      '{items: [.items[] | select(any((.metadata.ownerReferences // [])[]; .uid as $x | ($rs | index($x)) != null))]}') || {
      echo "IMAGE VERIFY FAILED: cannot determine which pods belong to $name" >&2; exit 1; }

    #   `kubectl rollout status` returns as soon as the NEW ReplicaSet is complete, but the
    #   OUTGOING pod — already carrying a deletionTimestamp — is still owned by this Deployment's
    #   previous ReplicaSet and still Running for several more seconds. Comparing ITS digest
    #   against the new pin failed two green rollouts of tape-zones-service on 2026-10-13
    #   (es4-deploy #768 and #769) with a false "STALE IMAGE"; #769 pinned ae5cf68d, the live pod
    #   carried ae5cf68d on both spec.image and status.imageID, and the build reported 827b7a98 —
    #   the digest of the terminating pod of a ReplicaSet already scaled to zero. The jq clauses
    #   below are the form scripts/ops/zerodte-writer-activate.sh:371 already uses.
    # The two rules are deliberately asymmetric (Codex r1 BLOCKER 1). READINESS IS NOT PART OF THE
    # NEGATIVE RULE: a Running pod that is not Ready is still running its image, so it is still
    # evidence of a stale build. Readiness is demanded only of the pod that PROVES the pin, because
    # an unready pod proves nothing. Nothing retries until the old pod disappears.
    running_filter='.items[]
      | select(.metadata.deletionTimestamp == null)
      | select(.status.phase == "Running")'
    ready_filter="$running_filter"'
      | select(([(.status.conditions // [])[] | select(.type == "Ready" and .status == "True")] | length) >= 1)'
    # every status and every spec entry of a pod, init and app alike, keyed by container name
    statuses='((.status.initContainerStatuses // []) + (.status.containerStatuses // []))'
    specs='((.spec.initContainers // []) + (.spec.containers // []))'
    while read -r pdeploy pcontainer pdigest; do
      [ "$pdeploy" = "$name" ] || continue
      # (a) NEGATIVE — no live, Running pod may be on ANY other digest for this container. When the
      # kubelet has not reported an imageID yet, the pod's own SPEC image for that container is
      # judged instead, so an old-template pod cannot hide behind an unreported status (Codex r2
      # MAJOR). Only a container with neither an imageID nor a spec image is passed over, and rule
      # (b) then refuses to count it as proof.
      bad=$(printf '%s' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest" "[$running_filter"'
        | {pod: .metadata.name,
           id:   (([ '"$statuses"'[] | select(.name == $c) | (.imageID // "")] | first) // ""),
           spec: (([ '"$specs"'[]    | select(.name == $c) | (.image   // "")] | first) // "")}
        | (if .id != "" then {pod: .pod, what: "imageID", ref: .id} else {pod: .pod, what: "spec image", ref: .spec} end)
        | select(.ref != "")
        | select((((.ref | endswith("@" + $dg)) or (.ref == $dg))) | not)
        | "\(.pod) runs \(.what) \(.ref)"] | .[]')
      if [ -n "$bad" ]; then
        echo "IMAGE VERIFY FAILED: $name container $pcontainer pinned $pdigest (STALE IMAGE)" >&2
        printf '%s\n' "$bad" | sed 's/^/  /' >&2
        exit 1
      fi
      # (b) POSITIVE — at least one live, Running AND Ready pod must actually report the pin.
      proof=$(printf '%s' "$pods_json" | jq -r --arg c "$pcontainer" --arg dg "$pdigest" "[$ready_filter"'
        | select(([ '"$statuses"'[] | select(.name == $c)
                    | select((((.imageID // "") | endswith("@" + $dg)) or ((.imageID // "") == $dg)))] | length) >= 1)] | length')
      if [ "${proof:-0}" -lt 1 ]; then
        echo "IMAGE VERIFY FAILED: $name has no live (not deleting) Running and Ready pod carrying the pinned digest $pdigest on container $pcontainer" >&2
        printf '%s' "$pods_json" | jq -r --arg c "$pcontainer" '.items[]
          | "  \(.metadata.name) phase=\(.status.phase) deleting=\(if .metadata.deletionTimestamp then "yes" else "no" end) ready=\([(.status.conditions // [])[] | select(.type == "Ready")] | (first.status // "?")) \($c)=\((([ '"$statuses"'[] | select(.name == $c) | (.imageID // "")] | first) // "<no containerStatus>"))"' >&2
        exit 1
      fi
      echo "verified $name container $pcontainer runs pinned digest $pdigest ($proof Running+Ready pod(s) carry it; no live pod reports another digest)"
    done <<< "$pinned"
  done <<< "$wanted"
fi
