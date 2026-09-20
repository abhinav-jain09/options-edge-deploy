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
# Nothing runs between the verification and this script. The verification covers ONE checkout — the
# workspace root, `--dir .` — so this script must consume nothing outside it, and that is ENFORCED
# below rather than assumed: DOCKERFILE and BUILD_CONTEXT are resolved with symlinks followed, and a
# value that lands anywhere but inside the workspace (absolute, `..`, a symlink pointing out) is
# REFUSED before docker is invoked. Without that check the two parameters are a hole straight through
# the guarantee: the tree verifier would pass on the workspace while docker was handed an unverified
# directory's contents (Codex I1 on options-edge-deploy#1078, reproduced with a sentinel docker).
#
# Inputs (environment, all set by the calling Jenkinsfile's 'Resolve profile' stage):
#   SERVICE_NAME       image/service name (options-edge-<service>)
#   IMAGE_REGISTRY     registry from oeProfile(ENVIRONMENT)
#   BUILD_PLATFORM     linux/arm64 | linux/amd64
#   PUSH_IMAGE         true = publish, false = load locally
#   IMAGE_TAG          explicit tag; empty = the git short SHA of the verified checkout
#   DEV_IMAGE_TAG      extra mutable tag to publish (empty disables it)
#   INSECURE_REGISTRY  true when the profile's registry is plain http
#   DOCKERFILE         path to the Dockerfile, INSIDE the workspace (default: Dockerfile)
#   BUILD_CONTEXT      docker build context directory, INSIDE the workspace (default: .)
#   BUILDER_PREFIX     short prefix for the local buildx builder name; the builder's uniqueness comes
#                      from JOB_NAME + BUILD_NUMBER below, not from this prefix
set -euo pipefail

[ "${SERVICE_NAME:?SERVICE_NAME is required}" != "CHANGE-ME" ] || { echo "Set SERVICE_NAME" >&2; exit 1; }
: "${IMAGE_REGISTRY:?IMAGE_REGISTRY is required}"
: "${BUILD_PLATFORM:?BUILD_PLATFORM is required}"
DOCKERFILE="${DOCKERFILE:-Dockerfile}"
BUILD_CONTEXT="${BUILD_CONTEXT:-.}"

# --- provenance: every path docker is handed must lie inside the VERIFIED checkout -----------------
# The caller verified the workspace root and nothing else. `pwd -P` resolves symlinks, so a component
# that is a symlink out of the workspace is caught here too, not just a literal `..` or `/`.
#
# NEWLINES ARE REFUSED FIRST, and that is not fussiness. Canonicalisation goes through `$( … )`, which
# strips trailing newlines, so for a path whose bytes contain one the value COMPARED here is not the
# value later handed to docker — and the error goes both ways: a sibling directory named "ws\n" next
# to the workspace "ws" compares as inside and is passed out verbatim (Codex I1 r2, executed: both the
# guard and the tree verifier returned PERMITTED and the outside path reached docker), while a
# legitimate inside directory ending in a newline has the file beneath it wrongly refused. Rather than
# carry pathname bytes losslessly through every substitution — WS, dirname and each input — refuse the
# one input shape that cannot survive the substitution. Nothing in this repository needs it.
refuse_newline() {                   # $1 = what it is, $2 = the value
  case "$2" in
    *"$NL"*) echo "REFUSED: $1 contains a newline; such a path cannot be compared faithfully against the verified checkout, so it is not supported. Rename it." >&2; exit 1 ;;
  esac
}
NL="$(printf '\nx')"; NL="${NL%x}"   # a bare newline, without a substitution stripping it
# The workspace's OWN path is checked the same way, captured losslessly (a trailing `x` survives the
# stripping, and only the single newline `pwd` itself prints is removed) — a workspace whose path ends
# in a newline would otherwise silently become a shorter, wrong containment root.
WS="$(pwd -P; printf x)"; WS="${WS%x}"; WS="${WS%"$NL"}"
refuse_newline "the workspace path ('$WS')" "$WS"
refuse_newline "DOCKERFILE ('$DOCKERFILE')" "$DOCKERFILE"
refuse_newline "BUILD_CONTEXT ('$BUILD_CONTEXT')" "$BUILD_CONTEXT"
# Resolution is captured LOSSLESSLY, exactly as WS is, and the resolved value is what gets checked
# for a newline. The raw-input refusal above is not enough on its own: a newline-free input can
# RESOLVE through a symlink to a newline-bearing pathname, and the strip then happens after the input
# check has already passed (Codex I1 r3, executed — `ln -s $'../ws\n' link` committed inside the
# workspace, then BUILD_CONTEXT=link reached docker; `ln -s link chain` shows one indirection is not
# the limit). Refusing the spelling at the boundary refuses a spelling; refusing the resolved
# pathname refuses the class.
resolve_dir() {                      # $1 = directory; sets RESOLVED to its real path, bytes preserved
  local out
  out="$(cd -- "$1" 2>/dev/null && pwd -P && printf x)" || return 1
  out="${out%x}"
  RESOLVED="${out%"$NL"}"
}
inside_workspace() {                 # $1 = directory that must exist, and resolve, inside $WS
  local RESOLVED=""
  resolve_dir "$1" || { echo "not a directory: '$1'" >&2; return 1; }
  case "$RESOLVED" in
    *"$NL"*) echo "REFUSED: '$1' RESOLVES to a pathname containing a newline ('$RESOLVED'); that cannot be compared faithfully against the verified checkout, so it is not supported. Rename the target, or do not point at it." >&2; return 1 ;;
  esac
  case "$RESOLVED" in
    "$WS"|"$WS"/*) return 0 ;;
    *) echo "'$1' resolves to '$RESOLVED', outside the verified workspace '$WS'" >&2; return 1 ;;
  esac
}
case "$DOCKERFILE" in /*) echo "DOCKERFILE must be a path inside the workspace, not absolute ('$DOCKERFILE')" >&2; exit 1 ;; esac
case "$BUILD_CONTEXT" in /*) echo "BUILD_CONTEXT must be a path inside the workspace, not absolute ('$BUILD_CONTEXT')" >&2; exit 1 ;; esac
[ -f "$DOCKERFILE" ] || { echo "Missing Dockerfile '$DOCKERFILE'" >&2; exit 1; }
inside_workspace "$(dirname -- "$DOCKERFILE")" \
  || { echo "REFUSED: the Dockerfile is outside the verified checkout; nothing was built." >&2; exit 1; }
inside_workspace "$BUILD_CONTEXT" \
  || { echo "REFUSED: the docker build context is outside the verified checkout; nothing was built." >&2; exit 1; }
# A symlinked Dockerfile whose TARGET is outside the tree would be read from outside it.
if [ -L "$DOCKERFILE" ]; then
  echo "REFUSED: '$DOCKERFILE' is a symlink; the Dockerfile must be a regular file of the verified checkout." >&2
  exit 1
fi

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
# The builder name must be unique PER JOB: two services building number 1 against the same docker
# daemon would otherwise share `<prefix>-1` and each remove the other's builder (Codex I4). JOB_NAME is
# the job's own identity; SERVICE_NAME is the fallback when this runs outside Jenkins.
#
# The readable part is sanitised, and sanitising is LOSSY — `folder/service` and `folder-service` both
# become `folder-service` and collide again (Codex I4 r2, executed). So the name also carries a digest
# of the ORIGINAL identity. Eight hex characters was not enough either: 32 bits is small enough to
# collide on crafted folder-style identities, and Codex produced a pair that did (I4 r3, executed).
# 32 hex characters is 128 bits. That is not a proof of uniqueness — two identities sharing a sha256
# prefix of that length would still collide — it is a bound small enough to stop claiming otherwise
# about. The readable part is for a human reading `docker buildx ls` and is truncated; the digest is
# what separates two jobs.
JOB_IDENTITY="${JOB_NAME:-$SERVICE_NAME}"
BUILDER_ID="$(printf '%s' "$JOB_IDENTITY" | tr -c 'A-Za-z0-9_.-' '-' | cut -c1-40)"
if command -v sha256sum >/dev/null 2>&1; then
  BUILDER_HASH="$(printf '%s' "$JOB_IDENTITY" | sha256sum | cut -c1-32)"
else
  BUILDER_HASH="$(printf '%s' "$JOB_IDENTITY" | shasum -a 256 | cut -c1-32)"
fi
BUILDER="${BUILDER_PREFIX:-oe}-${BUILDER_ID}-${BUILDER_HASH}-${BUILD_NUMBER:-local}"
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
