#!/usr/bin/env bash
# post{always} of service-deploy's Deploy path — the ONLY post path that can touch the cluster.
#
# Deployment Permission Rule: a build the permitted-commit guard refused must not mutate a target
# through post/finally either. post{always} runs regardless, and the recovery it owns
# (scripts/deploy/vix-unpause.sh) deliberately honours a marker STRANDED by an EARLIER build — so a
# build that failed its deploy-workspace guard would, unguarded, consume that old marker and scale a
# production deployment. Hence this gate: the recovery runs only when THIS build's deploy workspace
# was permitted (DEPLOY_WORKSPACE_PERMITTED, set by nothing but the deploy-workspace guard stage, and
# never by the first guard on the other agent allocation). Otherwise the marker is preserved and
# reported: a later PERMITTED build's post restores it, or a human does.
#
# Exit status: the recovery's own when permitted; 0 otherwise (the build already failed at its guard;
# this path only reports, and touches nothing).
set -uo pipefail
MARK="${1:?usage: post-deploy-recovery.sh <marker-file>}"

if [ "${DEPLOY_WORKSPACE_PERMITTED:-}" = "PASSED" ]; then
  exec bash "$(dirname "$0")/vix-unpause.sh" "$MARK"
fi

if [ -f "$MARK" ]; then
  echo "HUMAN REQUIRED: recovery marker $MARK exists (replicas=$(cat "$MARK" 2>/dev/null || echo '?')) but this build's deploy workspace was never permitted by the commit guard — NOT touching the cluster." >&2
  echo "The marker is kept: the next PERMITTED build's post restores vix-option-inteligence-service from it, or restore it by hand." >&2
  exit 0
fi
echo "post-deploy-recovery: deploy workspace not permitted in this build — no recovery to run, nothing touched."
