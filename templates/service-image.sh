#!/usr/bin/env bash
# service-image.sh — builds (and publishes) ONE OptionsEdge service image from the job's workspace.
#
# TEMPLATE. Copy it into a new service repository as scripts/ci/service-image.sh, next to the
# Jenkinsfile copied from options-edge-deploy/templates/Jenkinsfile.new-service — see
# options-edge/new-service-onboarding.md.
#
# This is the body the build job used to carry inline. It lives in a repository script, called by a
# DEDICATED step, because the Deployment Permission Rule (options-edge rule.md) requires every
# source-consuming effect to be one step whose whole body is that one command, immediately preceded
# by the provenance verification of the checkout it consumes (validator rule 9b). The pipeline
# therefore reads:
#
#     timeout { sh 'PERMITTED_SHA="${PERMITTED_SHA:-}" bash scripts/jenkins/verify-permitted-tree.sh --dir . …' }
#     sh 'bash scripts/ci/service-image.sh'
#
# Nothing runs between the verification and this script, and this script consumes only the workspace
# that was just verified.
#
# Inputs (environment, all set by the calling Jenkinsfile's 'Resolve profile' stage):
#   SERVICE_NAME       image/service name (options-edge-<service>)
#   IMAGE_REGISTRY     registry from oeProfile(ENVIRONMENT)
#   BUILD_PLATFORM     linux/arm64 | linux/amd64
#   PUSH_IMAGE         true = publish, false = load locally
#   IMAGE_TAG          explicit tag; empty = the git short SHA of the verified checkout
#   DEV_IMAGE_TAG      extra mutable tag to publish (empty disables it)
#   INSECURE_REGISTRY  true when the profile's registry is plain http
#   DOCKERFILE         path to the Dockerfile (default: Dockerfile)
#   BUILD_CONTEXT      docker build context directory (default: .)
#   BUILDER_PREFIX     short prefix for the local buildx builder name (per job, so two jobs never share one)
set -euo pipefail

[ "${SERVICE_NAME:?SERVICE_NAME is required}" != "CHANGE-ME" ] || { echo "Set SERVICE_NAME" >&2; exit 1; }
: "${IMAGE_REGISTRY:?IMAGE_REGISTRY is required}"
: "${BUILD_PLATFORM:?BUILD_PLATFORM is required}"
DOCKERFILE="${DOCKERFILE:-Dockerfile}"
BUILD_CONTEXT="${BUILD_CONTEXT:-.}"
TAG="${IMAGE_TAG:-$(git rev-parse --short=12 HEAD)}"
IMAGE="$IMAGE_REGISTRY/$SERVICE_NAME:$TAG"

# Only mark the registry insecure (plain http) when the profile says so (INSECURE_REGISTRY); a
# TLS/HTTPS registry is used as-is.
BKCFG=""; CFG_ARG=""
if [ "${INSECURE_REGISTRY:-false}" = "true" ]; then
  BKCFG="$(mktemp)"
  printf '[registry."%s"]\n  http = true\n  insecure = true\n' "$IMAGE_REGISTRY" > "$BKCFG"
  CFG_ARG="--config $BKCFG"
fi
BUILDER="${BUILDER_PREFIX:-oe-newsvc}-${BUILD_NUMBER:-local}"
docker buildx rm "$BUILDER" >/dev/null 2>&1 || true
docker buildx create --name "$BUILDER" --driver docker-container $CFG_ARG --use >/dev/null
cleanup() {
  docker buildx rm "$BUILDER" >/dev/null 2>&1 || true
  [ -n "$BKCFG" ] && rm -f "$BKCFG" || true
}
trap cleanup EXIT

TAGS="-t $IMAGE"
if [ -n "${DEV_IMAGE_TAG:-}" ] && [ "$DEV_IMAGE_TAG" != "$TAG" ]; then
  TAGS="$TAGS -t $IMAGE_REGISTRY/$SERVICE_NAME:$DEV_IMAGE_TAG"
fi
echo "Building $IMAGE (platform=$BUILD_PLATFORM, push=${PUSH_IMAGE:-false}, context=$BUILD_CONTEXT)"
if [ "${PUSH_IMAGE:-false}" = "true" ]; then
  docker buildx build --platform "$BUILD_PLATFORM" $TAGS -f "$DOCKERFILE" --push "$BUILD_CONTEXT"
else
  docker buildx build --platform "$BUILD_PLATFORM" $TAGS -f "$DOCKERFILE" --load "$BUILD_CONTEXT"
fi
