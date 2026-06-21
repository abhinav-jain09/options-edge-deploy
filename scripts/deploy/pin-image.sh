#!/usr/bin/env bash
# Sourced helper: turn a mutable image ref (registry/repo:tag) into an immutable digest-pinned ref
# (registry/repo:tag@sha256:<digest>) so a stale or hand-clobbered tag can never change a running
# pod, and the deployment is traceable to an exact digest. Validates that the tag has exactly one
# manifest for $DEPLOY_PLATFORM (fails closed). Pins the TOP index/image digest -- the digest k8s
# reports as the pod imageID on docker-desktop -- so a post-rollout imageID check can match it.
#
# Env: DEPLOY_PLATFORM (e.g. linux/arm64), REGISTRY_SCHEME (http for the local insecure registry).
# Requires: curl, jq.

resolve_repo_digest() {
  local registry="$1" repo="$2" tag="$3"
  local os="${DEPLOY_PLATFORM%/*}" arch="${DEPLOY_PLATFORM#*/}"
  local accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
  local hdr man digest media n cfg cfgblob
  hdr="$(mktemp)"; man="$(mktemp)"
  curl -fsSL -D "$hdr" -H "Accept: $accept" -o "$man" \
    "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/manifests/$tag"
  digest="$(awk 'tolower($1)=="docker-content-digest:"{gsub("\r","",$2);print $2}' "$hdr" | tail -1)"
  [[ -n "$digest" ]] || { echo "no Docker-Content-Digest for $repo:$tag" >&2; return 1; }
  media="$(jq -r '.mediaType // ""' "$man")"
  case "$media" in
    *image.index*|*manifest.list*)
      n="$(jq --arg os "$os" --arg arch "$arch" '[.manifests[]? | select(.platform.os==$os and .platform.architecture==$arch)] | length' "$man")"
      [[ "$n" == "1" ]] || { echo "$repo:$tag has $n manifests for $DEPLOY_PLATFORM (need exactly 1)" >&2; return 1; }
      ;;
    *image.manifest*|*manifest.v2*)
      cfg="$(jq -r '.config.digest // ""' "$man")"
      [[ -n "$cfg" ]] || { echo "$repo:$tag manifest has no config digest" >&2; return 1; }
      cfgblob="$(mktemp)"
      curl -fsSL -o "$cfgblob" "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/blobs/$cfg"
      jq -e --arg os "$os" --arg arch "$arch" '.os==$os and .architecture==$arch' "$cfgblob" >/dev/null \
        || { echo "$repo:$tag is not $DEPLOY_PLATFORM" >&2; return 1; }
      ;;
    *) echo "unsupported manifest mediaType for $repo:$tag: $media" >&2; return 1 ;;
  esac
  printf '%s\n' "$digest"
}

# pin_ref <registry/repo:tag> -> <registry/repo:tag@sha256:digest>. Idempotent: an already-pinned
# ref is returned unchanged. Fails closed if the digest cannot be resolved/validated.
pin_ref() {
  local ref="$1"
  [[ "$ref" == *"@sha256:"* ]] && { printf '%s\n' "$ref"; return 0; }
  local no_tag="${ref%:*}" tag="${ref##*:}" digest
  digest="$(resolve_repo_digest "${no_tag%%/*}" "${no_tag#*/}" "$tag")" || return 1
  printf '%s\n' "$no_tag:$tag@$digest"
}
