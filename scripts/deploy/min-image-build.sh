#!/usr/bin/env bash
# require_min_image_build — refuse an image older than a service's declared minimum build.
#
# Sourced by scripts/deploy/service-deploy.sh after the image is digest-pinned and bound. A service
# that must never run an image from before some fix declares it in
#     k8s/services/<service>/MIN_IMAGE_BUILD        e.g.   options-edge-processing 1678
# (the Jenkins job that builds the image, and the lowest acceptable build NUMBER of that job).
# Build numbers are monotonic per job, and the processing Jenkinsfile stamps every image with
# the OCI label  options-edge.jenkins-build=<BUILD_URL>  at build time — so the number is read
# from the IMAGE in the registry, not from a tag name or a comment, and a mutable tag that was
# moved back to an old digest is still refused. First use: oi-next-publication, whose images
# before processing build #1678 (PR #910) created their output topic 32-wide and compacted — an
# error Kafka cannot undo.
#
# Fails CLOSED: no label, an unparseable label, a label from a different job, or a registry read
# failure all refuse the deploy. A service with no MIN_IMAGE_BUILD file is unaffected.
#
# Inputs (environment): SERVICE, PINNED_IMAGE (registry/repo:tag@sha256:…), DEPLOY_PLATFORM,
# REGISTRY_SCHEME; curl + jq. Returns 0 to allow, non-zero to refuse (reason on stderr).
_mib_labels_of_pinned() {
  # Prints the image config's Labels (JSON object) for PINNED_IMAGE on DEPLOY_PLATFORM.
  local ref="$1" registry repo digest os arch man media cfg
  registry="${ref%%/*}"
  repo="${ref#*/}"; repo="${repo%%@*}"; repo="${repo%:*}"
  digest="${ref#*@}"
  os="${DEPLOY_PLATFORM%/*}"; arch="${DEPLOY_PLATFORM#*/}"
  local accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
  man="$(curl -fsSL -H "Accept: $accept" "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/manifests/$digest")" || return 1
  media="$(jq -r '.mediaType // ""' <<<"$man")"
  case "$media" in
    *image.index*|*manifest.list*)
      local sub
      sub="$(jq -r --arg os "$os" --arg arch "$arch" '[.manifests[]? | select(.platform.os==$os and .platform.architecture==$arch)] | .[0].digest // ""' <<<"$man")"
      [ -n "$sub" ] || { echo "min-image-build: $repo@$digest has no $DEPLOY_PLATFORM manifest" >&2; return 1; }
      man="$(curl -fsSL -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
        "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/manifests/$sub")" || return 1
      ;;
    *image.manifest*|*manifest.v2*) ;;
    *) echo "min-image-build: unsupported manifest mediaType '$media' for $repo@$digest" >&2; return 1 ;;
  esac
  cfg="$(jq -r '.config.digest // ""' <<<"$man")"
  [ -n "$cfg" ] || { echo "min-image-build: $repo@$digest manifest has no config digest" >&2; return 1; }
  curl -fsSL "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/blobs/$cfg" | jq -c '.config.Labels // {}'
}

require_min_image_build() {
  local file="k8s/services/${SERVICE:?}/MIN_IMAGE_BUILD" job min labels url label_job num
  [ -f "$file" ] || return 0
  read -r job min < "$file" || true
  if [ -z "$job" ] || ! printf '%s' "$min" | grep -Eq '^[0-9]+$'; then
    echo "min-image-build: $file must contain '<jenkins-job> <build-number>', got '$(cat "$file")'" >&2
    return 1
  fi
  labels="$(_mib_labels_of_pinned "${PINNED_IMAGE:?}")" || {
    echo "min-image-build: could not read the image labels of $PINNED_IMAGE from the registry; refusing" >&2
    return 1
  }
  url="$(jq -r '."options-edge.jenkins-build" // ""' <<<"$labels")"
  if [ -z "$url" ]; then
    echo "min-image-build: $PINNED_IMAGE carries no options-edge.jenkins-build label; it cannot prove it is build >= $min of $job — refusing" >&2
    return 1
  fi
  # .../job/<name>/<number>/  — anything else is unparseable and refused.
  label_job="$(printf '%s' "$url" | sed -nE 's#^.*/job/([^/]+)/([0-9]+)/?$#\1#p')"
  num="$(printf '%s' "$url" | sed -nE 's#^.*/job/([^/]+)/([0-9]+)/?$#\2#p')"
  if [ -z "$label_job" ] || [ -z "$num" ]; then
    echo "min-image-build: cannot parse build number from label '$url' on $PINNED_IMAGE — refusing" >&2
    return 1
  fi
  if [ "$label_job" != "$job" ]; then
    echo "min-image-build: $PINNED_IMAGE was built by job '$label_job', not '$job' — refusing" >&2
    return 1
  fi
  if [ "$num" -lt "$min" ]; then
    echo "min-image-build: $PINNED_IMAGE is $job build #$num; this service requires build >= #$min ($file) — refusing" >&2
    return 1
  fi
  echo "min-image-build: $PINNED_IMAGE is $job build #$num >= #$min — allowed"
}
