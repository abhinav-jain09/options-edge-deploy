#!/usr/bin/env bash
# Sourced helper: turn a mutable image ref (registry/repo:tag) into an immutable digest-pinned ref
# (registry/repo:tag@sha256:<digest>) so a stale or hand-clobbered tag can never change a running
# pod, and the deployment is traceable to an exact digest. Validates that the tag has exactly one
# manifest for $DEPLOY_PLATFORM (fails closed). Pins the TOP index/image digest -- the digest k8s
# reports as the pod imageID on docker-desktop -- so a post-rollout imageID check can match it.
#
# Env: DEPLOY_PLATFORM (e.g. linux/arm64), REGISTRY_SCHEME (http for the local insecure registry).
#      REGISTRY_RESOLVE_HOST (optional) — the host:port the AGENT curls to read manifests/blobs,
#      used in place of the registry component of the image ref WITHOUT changing the ref itself. The
#      dev registry is named host.docker.internal:5001 in the manifests (resolved by kubelet ON the
#      dev host), but a REMOTE Jenkins agent cannot reach it by that name (host.docker.internal is its
#      OWN loopback); set REGISTRY_RESOLVE_HOST=<dev-LAN-ip>:5001 so the agent resolves the digest over
#      the LAN while the pinned ref still carries host.docker.internal:5001 for the pods. Same registry,
#      same digest. Unset = resolve via the ref's own registry (agent IS the dev host).
# Requires: curl, jq.

# --- LAYER INTEGRITY -------------------------------------------------------------------------
# A manifest that resolves is NOT an image that pulls. On 2026-11-12 the dev registry served
# options-edge-web's manifest with HTTP 200 while one of its nine layers -- 132 MB -- was a 404, and
# the deploy shipped it: the pod then died with
#     ErrImagePull: short read: expected 132767435 bytes but got 0: unexpected EOF
# hours after a build and a deploy that both reported success. The registry's blob store is GC'd by
# the dev clean, and a mark phase that misses a blob leaves exactly this shape -- manifest present,
# content gone. resolve_repo_digest already refuses an image of the wrong platform; it has to refuse
# an image the registry cannot actually serve too, because "the tag resolves" is the check that
# passed every time this broke.
#
# HEAD, not GET: the registry answers with Content-Length without moving the bytes, so verifying a
# nine-layer image costs nine round trips, not 250 MB. There is deliberately NO opt-out. An escape
# hatch here is an escape hatch straight back to a broken rollout, and the guard is only worth
# having if it cannot be stepped around in a hurry.
_blob_head() {   # prints "<http-code> <content-length>"
  local url="$1" hdr code len
  hdr="$(mktemp)"
  curl -sS -o /dev/null -D "$hdr" -I --max-time 30 "$url" >/dev/null 2>&1
  code="$(awk 'toupper($1) ~ /^HTTP/ {c=$2} END {print c+0}' "$hdr")"
  len="$(awk 'tolower($1)=="content-length:" {gsub("\r","",$2); l=$2} END {print l+0}' "$hdr")"
  rm -f "$hdr"
  printf '%s %s\n' "$code" "$len"
}

# verify_manifest_blobs <resolve-host> <repo> <manifest-file> <what>
# Every blob the manifest names -- its config and each layer -- must be served, and served at the
# length the manifest declares. A short blob is the same outage as a missing one and reads as a
# healthy 200, so the length is checked, not just the status.
verify_manifest_blobs() {
  local resolve="$1" repo="$2" man="$3" what="$4"
  local scheme="${REGISTRY_SCHEME:-http}" bad=0 n=0 digest size code len
  while read -r digest size; do
    [[ -n "$digest" ]] || continue
    n=$((n + 1))
    read -r code len <<<"$(_blob_head "$scheme://$resolve/v2/$repo/blobs/$digest")"
    if [[ "$code" != "200" ]]; then
      echo "registry does not serve $what blob $digest (HTTP $code) -- the manifest resolves but its content is gone" >&2
      bad=$((bad + 1))
    elif [[ "$len" != "$size" ]]; then
      echo "registry serves $what blob $digest as $len bytes, the manifest declares $size -- a short blob pulls as 'unexpected EOF'" >&2
      bad=$((bad + 1))
    fi
  done < <(jq -r '([.config] + (.layers // []))[]? | select(.digest != null) | "\(.digest) \(.size)"' "$man")
  if [[ "$n" == "0" ]]; then
    echo "$what names no blobs at all -- refusing rather than calling an empty manifest verified" >&2
    return 1
  fi
  (( bad == 0 )) || { echo "$what is not pullable: $bad of $n blobs missing or short" >&2; return 1; }
  return 0
}

resolve_repo_digest() {
  local registry="$1" repo="$2" tag="$3"
  local resolve="${REGISTRY_RESOLVE_HOST:-$registry}"
  local os="${DEPLOY_PLATFORM%/*}" arch="${DEPLOY_PLATFORM#*/}"
  local accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
  local hdr man digest media n cfg cfgblob
  hdr="$(mktemp)"; man="$(mktemp)"
  curl -fsSL -D "$hdr" -H "Accept: $accept" -o "$man" \
    "${REGISTRY_SCHEME:-http}://$resolve/v2/$repo/manifests/$tag"
  digest="$(awk 'tolower($1)=="docker-content-digest:"{gsub("\r","",$2);print $2}' "$hdr" | tail -1)"
  [[ -n "$digest" ]] || { echo "no Docker-Content-Digest for $repo:$tag" >&2; return 1; }
  media="$(jq -r '.mediaType // ""' "$man")"
  case "$media" in
    *image.index*|*manifest.list*)
      n="$(jq --arg os "$os" --arg arch "$arch" '[.manifests[]? | select(.platform.os==$os and .platform.architecture==$arch)] | length' "$man")"
      [[ "$n" == "1" ]] || { echo "$repo:$tag has $n manifests for $DEPLOY_PLATFORM (need exactly 1)" >&2; return 1; }
      # Descend into the ONE platform manifest and check the blobs IT names. The index itself names
      # no layers, so verifying at this level would verify nothing -- and the sub-manifest is the
      # part a GC strands, because an index's children are reachable only through it.
      local submd subman
      submd="$(jq -r --arg os "$os" --arg arch "$arch" 'first(.manifests[]? | select(.platform.os==$os and .platform.architecture==$arch) | .digest)' "$man")"
      [[ "$submd" == sha256:* ]] || { echo "$repo:$tag index has no digest for $DEPLOY_PLATFORM" >&2; return 1; }
      subman="$(mktemp)"
      curl -fsSL -H "Accept: $accept" -o "$subman" \
        "${REGISTRY_SCHEME:-http}://$resolve/v2/$repo/manifests/$submd" \
        || { echo "$repo:$tag index points at $submd, which the registry does not serve" >&2; return 1; }
      verify_manifest_blobs "$resolve" "$repo" "$subman" "$repo:$tag ($DEPLOY_PLATFORM, $submd)" || return 1
      ;;
    *image.manifest*|*manifest.v2*)
      cfg="$(jq -r '.config.digest // ""' "$man")"
      [[ -n "$cfg" ]] || { echo "$repo:$tag manifest has no config digest" >&2; return 1; }
      cfgblob="$(mktemp)"
      curl -fsSL -o "$cfgblob" "${REGISTRY_SCHEME:-http}://$resolve/v2/$repo/blobs/$cfg"
      jq -e --arg os "$os" --arg arch "$arch" '.os==$os and .architecture==$arch' "$cfgblob" >/dev/null \
        || { echo "$repo:$tag is not $DEPLOY_PLATFORM" >&2; return 1; }
      verify_manifest_blobs "$resolve" "$repo" "$man" "$repo:$tag" || return 1
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
