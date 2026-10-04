#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
. "$ROOT/scripts/deploy/min-image-build.sh"

IMAGE=${1:-}
if [[ "$IMAGE" != *@sha256:* ]]; then
  echo "read-pinned-image-labels: digest-pinned image required" >&2
  exit 2
fi

DEPLOY_PLATFORM=${DEPLOY_PLATFORM:-linux/amd64}
REGISTRY_SCHEME=${REGISTRY_SCHEME:-http}
export DEPLOY_PLATFORM REGISTRY_SCHEME
_mib_labels_of_pinned "$IMAGE"
