#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
guard=scripts/deploy/require-batch-image-binding.sh
for image in '' 'registry/oe-close-batch:prod' 'registry/another-service:prod@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; do
  if SERVICE=stock-gex-close-scheduler DEPLOY_DRY_RUN=false REQUIRED_IMAGE="$image" bash "$guard" > /dev/null 2>&1; then
    echo 'FAIL: an unbound scheduler image was accepted' >&2;exit 1
  fi
done
SERVICE=stock-gex-close-scheduler DEPLOY_DRY_RUN=false REQUIRED_IMAGE='host.docker.internal:5001/oe-close-batch:dev-1-abc@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' bash "$guard"
SERVICE=stock-gex-close-scheduler DEPLOY_DRY_RUN=true REQUIRED_IMAGE='' bash "$guard"
SERVICE=web DEPLOY_DRY_RUN=false REQUIRED_IMAGE='' bash "$guard"
echo 'PASS: missing/tag-only/wrong-repository batch bindings rejected; pinned batch and dry-run accepted'
