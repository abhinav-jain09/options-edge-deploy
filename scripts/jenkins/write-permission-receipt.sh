#!/usr/bin/env bash
# write-permission-receipt.sh <out-file> — the permission receipt of a deploy build, as KEY=VALUE.
#
# Deployment Permission Rule: the record of a deployment names the job, the build, the permitted
# commits and the exact artifact rolled. This writes what THIS build was permitted for and what it
# bound — the guard version it enforced, every permitted SHA it judged or forwarded, the child build
# it retained and the digest-pinned image it will deploy — so the Bugzilla change comment and the
# intent ledger can cite one file archived with the build. Read only from the environment; nothing
# here is a check (the checks ran before), and an unset value is written as empty, never invented.
set -euo pipefail
out="${1:?usage: write-permission-receipt.sh <out-file>}"
mkdir -p "$(dirname "$out")"
guard_version="$(bash "$(dirname "${BASH_SOURCE[0]}")/permitted-sha-guard-version.sh")"
checked_out="$(git rev-parse HEAD 2>/dev/null || echo '')"
{
  echo "PERMISSION_RECEIPT_FORMAT=1"
  echo "JOB_NAME=${JOB_NAME:-}"
  echo "BUILD_URL=${BUILD_URL:-}"
  echo "CHECKED_OUT_SHA=$checked_out"
  echo "PERMITTED_SHA=${PERMITTED_SHA:-}"
  echo "PERMITTED_SHA_GUARD_VERSION_DECLARED=${PERMITTED_SHA_GUARD_VERSION:-}"
  echo "PERMITTED_SHA_GUARD_VERSION_RUNNING=$guard_version"
  echo "PROCESSING_PERMITTED_SHA=${PROCESSING_PERMITTED_SHA:-}"
  echo "CONTRACTS_PERMITTED_SHA=${CONTRACTS_PERMITTED_SHA:-}"
  echo "WEB_PERMITTED_SHA=${WEB_PERMITTED_SHA:-}"
  echo "NIFTY_PERMITTED_SHA=${NIFTY_PERMITTED_SHA:-}"
  echo "CHILD_BUILD_URL=${CHILD_BUILD_URL:-}"
  echo "REQUIRED_IMAGE=${REQUIRED_IMAGE:-}"
  echo "FLAGS=PERMITTED_SHA_GUARD=${PERMITTED_SHA_GUARD:-} DEPLOY_WORKSPACE_PERMITTED=${DEPLOY_WORKSPACE_PERMITTED:-} SECONDARY_PERMISSIONS_PASSED=${SECONDARY_PERMISSIONS_PASSED:-}"
} > "$out"
echo "permission receipt written: $out"
sed 's/^/  receipt: /' "$out"
