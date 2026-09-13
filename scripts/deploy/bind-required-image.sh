#!/usr/bin/env bash
# bind_required_image — Deployment Permission Rule: deploy the image the PERMITTED child build produced.
#
# Sourced by scripts/deploy/service-deploy.sh after it has digest-pinned the mutable tag. When
# Jenkinsfile.service-deploy triggered the processing image build under PROCESSING_PERMITTED_SHA, it
# read THAT build's image lock and passes the digest-pinned reference in as REQUIRED_IMAGE. The
# mutable tag can have been moved by ANY later processing build before the pin; deploying it would
# silently substitute an image nobody permitted. So the permitted build's digest wins:
#   * the repository must be the one this service renders;
#   * a render that already pins a digest (PIN_IS_AUTHORITATIVE) must agree with it;
#   * the registry must serve that digest for DEPLOY_PLATFORM (resolve_repo_digest accepts a digest as
#     the reference and verifies the manifest) — an image that exists only in a lock is refused;
#   * then the pinned reference is rewritten to that digest.
# Empty REQUIRED_IMAGE (BUILD_IMAGES=false) leaves the mutable-tag pin unchanged.
#
# Inputs (environment): REQUIRED_IMAGE, PINNED_IMAGE, MUTABLE_IMAGE, PIN_IS_AUTHORITATIVE, and the
# resolve_repo_digest function from pin-image.sh. Prints the reference to deploy; non-zero = refuse.
bind_required_image() {
  if [ -z "${REQUIRED_IMAGE:-}" ]; then
    printf '%s\n' "$PINNED_IMAGE"
    return 0
  fi
  local req_digest="${REQUIRED_IMAGE#*@}"
  local req_repo="${REQUIRED_IMAGE%%@*}"; req_repo="${req_repo%:*}"
  printf '%s' "$req_digest" | grep -Eq '^sha256:[0-9a-f]{64}$' \
    || { echo "bind_required_image: REQUIRED_IMAGE '$REQUIRED_IMAGE' carries no valid digest" >&2; return 1; }
  local pin_repo="${PINNED_IMAGE%%@*}"; pin_repo="${pin_repo%:*}"
  if [ "${req_repo##*/}" != "${pin_repo##*/}" ]; then
    echo "bind_required_image: REQUIRED_IMAGE repository '${req_repo##*/}' is not this service's image '${pin_repo##*/}'" >&2
    return 1
  fi
  if [ "${PIN_IS_AUTHORITATIVE:-false}" = true ] && [ "${PINNED_IMAGE#*@}" != "$req_digest" ]; then
    echo "bind_required_image: the render pins ${PINNED_IMAGE#*@} but the permitted build produced $req_digest" >&2
    return 1
  fi
  if [ "${PINNED_IMAGE#*@}" != "$req_digest" ]; then
    echo "NOTE: ${MUTABLE_IMAGE:-$pin_repo} now resolves to ${PINNED_IMAGE#*@} — the tag moved after the permitted build; deploying the permitted build's $req_digest instead" >&2
  fi
  local served
  served="$(resolve_repo_digest "${pin_repo%%/*}" "${pin_repo#*/}" "$req_digest")" \
    || { echo "bind_required_image: the registry does not serve $pin_repo@$req_digest for ${DEPLOY_PLATFORM:-?}" >&2; return 1; }
  [ "$served" = "$req_digest" ] \
    || { echo "bind_required_image: the registry answered $served for $req_digest" >&2; return 1; }
  printf '%s\n' "${PINNED_IMAGE%%@*}@$req_digest"
}
