#!/usr/bin/env bash
# Delete explicitly retired, unmanaged workloads before the owning service is deployed.
# This is intentionally a narrow migration fence, not a general pruning mechanism.
set -euo pipefail

SERVICE="${SERVICE:?SERVICE must be set}"
ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT must be set}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-false}"
NAMESPACE="options-edge"

if [ "$ENVIRONMENT" = "dev" ] && [ "$SERVICE" = "feed-gateway" ]; then
  if [ "$DEPLOY_DRY_RUN" = "true" ]; then
    echo "DRY RUN: would delete retired deployment/es-synthetic-feed"
  else
    echo "Removing retired unmanaged dev deployment/es-synthetic-feed (if present)"
    kubectl -n "$NAMESPACE" delete deployment es-synthetic-feed --ignore-not-found=true
  fi
fi
