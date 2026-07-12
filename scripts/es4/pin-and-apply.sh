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

kubectl apply ${DRY} -f "$OUT"

# wait for each Deployment in this manifest (skip on dry-run)
if [ -z "$DRY" ]; then
  kubectl -n options-edge apply --dry-run=client -f "$OUT" -o name 2>/dev/null \
    | grep '^deployment' \
    | while read -r d; do
        kubectl -n options-edge rollout status "$d" --timeout=240s || {
          echo "ROLLOUT FAILED: $d (manifest $MANIFEST)" >&2; exit 1; }
      done
fi
