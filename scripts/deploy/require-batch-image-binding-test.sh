#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
guard=scripts/deploy/require-batch-image-binding.sh
export DEPLOY_DRY_RUN=false
for image in '' 'registry/oe-close-batch:prod' 'registry/another-service:prod@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; do
  if SERVICE=stock-gex-close-scheduler ENVIRONMENT=production DEPLOY_DRY_RUN=false REQUIRED_IMAGE="$image" bash "$guard" > /dev/null 2>&1; then
    echo 'FAIL: an unbound scheduler image was accepted' >&2;exit 1
  fi
done
SERVICE=stock-gex-close-scheduler ENVIRONMENT=dev DEPLOY_DRY_RUN=false REQUIRED_IMAGE='host.docker.internal:5001/oe-close-batch:dev-1-abc@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' bash "$guard"
for pair in 'production host.docker.internal:5001/oe-close-batch:dev-1-abc' 'dev 192.168.100.252:5000/oe-close-batch:prod-1-abc'; do
  read -r target image <<< "$pair"
  if SERVICE=stock-gex-close-scheduler ENVIRONMENT="$target" DEPLOY_DRY_RUN=false REQUIRED_IMAGE="$image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" bash "$guard" >/dev/null 2>&1; then
    echo 'FAIL: crossed-environment batch image accepted' >&2;exit 1
  fi
done
SERVICE=stock-gex-close-scheduler ENVIRONMENT=production REQUIRED_IMAGE='192.168.100.252:5000/oe-close-batch:prod-1-abc@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' bash "$guard"
SERVICE=stock-gex-close-scheduler DEPLOY_DRY_RUN=true REQUIRED_IMAGE='' bash "$guard"
SERVICE=web DEPLOY_DRY_RUN=false REQUIRED_IMAGE='' bash "$guard"
echo 'PASS: missing/tag-only/wrong-repository and crossed-environment batch bindings rejected; pinned batch and dry-run accepted'
