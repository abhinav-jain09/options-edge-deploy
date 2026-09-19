#!/usr/bin/env bash
# resolve-pushed-digest.sh <registry[:port]> <repository> <tag>
#
# Artifact identity: an image job that just pushed a tag records WHICH image that tag names right now
# — the registry's Docker-Content-Digest for it — into its image lock, so a deploy job can bind itself
# to that exact digest instead of whatever the mutable tag resolves to later. Plain-HTTP first (the LAN
# registries), HTTPS second; bounded and retried, because a push propagates in a moment; never a guess:
# no digest, no output, non-zero. Prints `sha256:<64 hex>`.
set -euo pipefail
registry="${1:?registry}"; repo="${2:?repository}"; tag="${3:?tag}"
accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
for attempt in 1 2 3 4 5; do
  for scheme in http https; do
    digest="$({ curl -fsSI --connect-timeout 5 --max-time 20 -H "Accept: $accept" \
      "$scheme://$registry/v2/$repo/manifests/$tag" 2>/dev/null || true; } \
      | awk 'tolower($1)=="docker-content-digest:"{print $2; exit}' | tr -d '[:space:]')"
    if printf '%s' "$digest" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
      printf '%s\n' "$digest"
      exit 0
    fi
  done
  sleep 3
done
echo "resolve-pushed-digest: no Docker-Content-Digest for $registry/$repo:$tag after 5 attempts" >&2
exit 1
