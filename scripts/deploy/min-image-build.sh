#!/usr/bin/env bash
# Minimum image build — refuse an image older than a service's declared minimum build.
#
# A service that must never run an image from before some fix declares it in
#     k8s/services/<service>/MIN_IMAGE_BUILD
#         e.g.   http://localhost:8085 options-edge-processing 1678
# — the Jenkins base URL as the build agents see it (the processing Jenkinsfile stamps BUILD_URL,
# which starts with that base), the job that builds the image, and the lowest acceptable build
# NUMBER of that job. Build numbers are monotonic per job, and every image carries the OCI label
#     options-edge.jenkins-build=<BUILD_URL>
# stamped at build time — so the number is read from the IMAGE in the registry, not from a tag
# name or a comment, and a mutable tag moved back to an old digest is still refused. ALL THREE
# parts must match: a label naming the right job and number on some other Jenkins host is not
# this Jenkins's provenance and is refused.
#
# First use: oi-next-publication, whose images before processing build #1678 (PR #910) created
# their output topic 32-wide and compacted — an error Kafka cannot undo.
#
# Two entry points, one per deploy path, both BEFORE any kubectl mutation:
#   require_min_image_build             standalone service-deploy.sh  (SERVICE, PINNED_IMAGE in env)
#   require_min_image_builds_in_render  monolithic apply.sh           (every declaring service, from
#                                                                      the rendered manifest)
# Fails CLOSED: no label, an unparseable label, another host or job, a non-digest image, a registry
# read failure — all refuse. A service with no MIN_IMAGE_BUILD file is unaffected.
# Needs: DEPLOY_PLATFORM, REGISTRY_SCHEME (default http), curl, jq (all already required by pin-image.sh).

_mib_labels_of_pinned() {
  # Prints the image config's Labels (JSON object) for a digest-pinned ref on DEPLOY_PLATFORM.
  local ref="$1" registry repo digest os arch man media cfg sub
  registry="${ref%%/*}"
  repo="${ref#*/}"; repo="${repo%%@*}"; repo="${repo%:*}"
  digest="${ref#*@}"
  os="${DEPLOY_PLATFORM%/*}"; arch="${DEPLOY_PLATFORM#*/}"
  local accept='application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
  man="$(curl -fsSL -H "Accept: $accept" "${REGISTRY_SCHEME:-http}://$registry/v2/$repo/manifests/$digest")" || return 1
  media="$(jq -r '.mediaType // ""' <<<"$man")"
  case "$media" in
    *image.index*|*manifest.list*)
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

# require_min_image_build_for <service> <digest-pinned image> — the gate for one pair.
require_min_image_build_for() {
  local svc="$1" image="$2"
  local file="k8s/services/$svc/MIN_IMAGE_BUILD" base job min labels url label_base label_job num
  [ -f "$file" ] || return 0
  read -r base job min < "$file" || true
  if ! printf '%s' "$base" | grep -Eq '^https?://[^/[:space:]]+$' || [ -z "$job" ] \
      || ! printf '%s' "$min" | grep -Eq '^[0-9]+$'; then
    echo "min-image-build: $file must contain '<jenkins-base-url> <jenkins-job> <build-number>', got '$(cat "$file")'" >&2
    return 1
  fi
  case "$image" in *@sha256:*) ;; *)
    echo "min-image-build: $image is not digest-pinned; the gate reads labels by digest — refusing" >&2
    return 1 ;;
  esac
  labels="$(_mib_labels_of_pinned "$image")" || {
    echo "min-image-build: could not read the image labels of $image from the registry; refusing" >&2
    return 1
  }
  url="$(jq -r '."options-edge.jenkins-build" // ""' <<<"$labels")"
  if [ -z "$url" ]; then
    echo "min-image-build: $image carries no options-edge.jenkins-build label; it cannot prove it is build >= $min of $job — refusing" >&2
    return 1
  fi
  # <base>/job/<name>/<number>/ — exactly; anything else is unparseable and refused.
  local re='^(https?://[^/]+)/job/([^/]+)/([0-9]+)/?$'
  label_base="$(printf '%s' "$url" | sed -nE "s#$re#\1#p")"
  label_job="$(printf '%s' "$url" | sed -nE "s#$re#\2#p")"
  num="$(printf '%s' "$url" | sed -nE "s#$re#\3#p")"
  if [ -z "$label_base" ] || [ -z "$label_job" ] || [ -z "$num" ]; then
    echo "min-image-build: cannot parse '<base>/job/<name>/<number>/' from label '$url' on $image — refusing" >&2
    return 1
  fi
  if [ "$label_base" != "$base" ]; then
    echo "min-image-build: $image was labelled by Jenkins at '$label_base', not '$base' — not this Jenkins's provenance; refusing" >&2
    return 1
  fi
  if [ "$label_job" != "$job" ]; then
    echo "min-image-build: $image was built by job '$label_job', not '$job' — refusing" >&2
    return 1
  fi
  if [ "$num" -lt "$min" ]; then
    echo "min-image-build: $image is $job build #$num; $svc requires build >= #$min ($file) — refusing" >&2
    return 1
  fi
  echo "min-image-build: $svc: $image is $job build #$num >= #$min — allowed"
}

# Standalone service-deploy.sh path.
require_min_image_build() {
  require_min_image_build_for "${SERVICE:?}" "${PINNED_IMAGE:?}"
}

# Monolithic apply.sh path: <rendered-manifest.yaml>. For EVERY service that declares a minimum,
# find its registered Deployments in the render and gate each container image there. A declaring
# service that does not render in this environment (a dev-only service on production) is skipped,
# and says so. Anything else that is not provably new enough refuses.
require_min_image_builds_in_render() {
  local render="$1" file svc deployments dep images img
  for file in k8s/services/*/MIN_IMAGE_BUILD; do
    [ -f "$file" ] || continue
    svc="$(basename "$(dirname "$file")")"
    deployments="$(yq -r ".services[] | select(.name == \"$svc\") | .deployments[]" services.yaml)"
    [ -n "$deployments" ] || { echo "min-image-build: $svc declares a minimum but is not in services.yaml — refusing" >&2; return 1; }
    for dep in $deployments; do
      # containers AND initContainers, exactly the set the digest gate examines.
      images="$(yq -r "select(.kind == \"Deployment\" and .metadata.name == \"$dep\") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)" "$render" | grep -v '^---$' || true)"
      if [ -z "$images" ]; then
        echo "min-image-build: $svc/$dep does not render in this environment — nothing to gate"
        continue
      fi
      for img in $images; do
        require_min_image_build_for "$svc" "$img" || return 1
      done
    done
  done
  return 0
}
