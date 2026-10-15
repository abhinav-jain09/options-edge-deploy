#!/usr/bin/env bash
# A scheduler rollout may consume only the image selected from the guarded batch build's lock.
set -euo pipefail
if [ "${SERVICE:-}" = stock-gex-close-scheduler ] && [ "${DEPLOY_DRY_RUN:-false}" != true ]; then
  case "${ENVIRONMENT:-}" in
    production) prefix='192.168.100.252:5000/oe-close-batch:prod-' ;;
    dev) prefix='host.docker.internal:5001/oe-close-batch:dev-' ;;
    *) echo 'FATAL: unknown batch target environment' >&2; exit 1 ;;
  esac
  image="${REQUIRED_IMAGE:-}"
  suffix="${image#"$prefix"}"
  if [[ "${REQUIRED_IMAGE:-}" != "$prefix"* ]] || ! [[ "$suffix" =~ ^[^@[:space:]]+@sha256:[a-f0-9]{64}$ ]]; then
    echo 'FATAL: stock-gex-close-scheduler requires REQUIRED_IMAGE from oe-close-batch batch-image-lock.env' >&2
    exit 1
  fi
fi
