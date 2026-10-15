#!/usr/bin/env bash
# A scheduler rollout may consume only the image selected from the guarded batch build's lock.
set -euo pipefail
if [ "${SERVICE:-}" = stock-gex-close-scheduler ] && [ "${DEPLOY_DRY_RUN:-false}" != true ]; then
  if ! [[ "${REQUIRED_IMAGE:-}" =~ /oe-close-batch:[^@[:space:]]+@sha256:[a-f0-9]{64}$ ]]; then
    echo 'FATAL: stock-gex-close-scheduler requires REQUIRED_IMAGE from oe-close-batch batch-image-lock.env' >&2
    exit 1
  fi
fi
