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

# wait for each Deployment, then PROVE the running pods carry the pinned digest
# (the silent-stale-build trap: a green rollout is not proof — assert imageID==digest,
# same guarantee as the org's build->deploy §13.3 gate; fail-closed on any mismatch).
if [ -z "$DRY" ]; then
  # Capture the Deployment list FIRST so a kubectl failure aborts (process substitution
  # swallows the producer's exit status; Codex #1). A manifest with services must yield >=1.
  # kubectl and grep must be separated: a piped `... | grep ... || true` would mask a
  # kubectl failure (pipe exit = grep's; || true swallows it). Run kubectl first — a
  # failure aborts here under set -e — then filter its captured output (grep no-match is fine).
  applied_names=$(kubectl -n options-edge apply --dry-run=client -f "$OUT" -o name)
  deploys=$(printf '%s\n' "$applied_names" | grep '^deployment' || true)
  if [ -z "$deploys" ]; then
    echo "IMAGE VERIFY FAILED: no Deployment found in $OUT (manifest has none)" >&2
    exit 1
  fi
  while read -r d; do
    kubectl -n options-edge rollout status "$d" --timeout=240s || {
      echo "ROLLOUT FAILED: $d (manifest $MANIFEST)" >&2; exit 1; }
    name=${d#deployment.apps/}; name=${name#deployment/}
    expected=$(grep -oE "image: [^ ]+@sha256:[0-9a-f]{64}" "$OUT" | head -1 | grep -oE "sha256:[0-9a-f]{64}")
    running=$(kubectl -n options-edge get pods -l "app.kubernetes.io/name=${name}" \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[*].imageID}{"\n"}{end}' 2>/dev/null | grep -oE "sha256:[0-9a-f]{64}" | sort -u)
    if [ -z "$running" ]; then
      echo "IMAGE VERIFY FAILED: no running imageID found for $name" >&2; exit 1
    fi
    for r in $running; do
      if [ "$r" != "$expected" ]; then
        echo "IMAGE VERIFY FAILED: $name runs $r but pinned $expected (STALE IMAGE)" >&2; exit 1
      fi
    done
    echo "verified $name runs pinned digest $expected"
  done <<< "$deploys"
fi
