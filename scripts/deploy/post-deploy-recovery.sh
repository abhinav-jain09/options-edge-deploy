#!/usr/bin/env bash
# post{always} of service-deploy's Deploy path — the ONLY post path that can touch the cluster.
#
# Deployment Permission Rule: a build that any permission check refused must not mutate a target
# through post/finally either. post{always} runs regardless, and the recovery it owns
# (scripts/deploy/vix-unpause.sh) deliberately honours a marker STRANDED by an EARLIER build — so a
# build refused anywhere after the guards (a missing processing or contracts SHA, an unguarded child,
# a child that refused its own checkout, a lock that does not match) would, unguarded, consume that
# old marker and scale a production deployment. Hence this gate: the recovery runs only when, in THIS
# build, ALL of the following were set — each by exactly one stage, in order:
#   PERMITTED_SHA_GUARD          the first guard, on the deploy host
#   DEPLOY_WORKSPACE_PERMITTED   the deploy-workspace guard (set nowhere else)
#   SECONDARY_PERMISSIONS_PASSED the stage after the image-build trigger: every secondary input
#                                (processing/contracts SHAs, child compatibility, child result, its
#                                image lock) was judged and passed, or was not needed
#   EFFECT_STAGE_STARTED         the first effect stage actually began (the VIX reconcile or the
#                                Deploy stage) — recovery OWNS a marker only once this run reached
#                                the phase that can create one
# Otherwise the marker is preserved and reported: a later PERMITTED build's post restores it, or a
# human does. Every flag must equal PASSED exactly.
#
# Exit status: the recovery's own when permitted; 0 otherwise (the build already failed at its
# refusal; this path only reports, and touches nothing).
set -uo pipefail
MARK="${1:?usage: post-deploy-recovery.sh <marker-file>}"

missing=""
for flag in PERMITTED_SHA_GUARD DEPLOY_WORKSPACE_PERMITTED SECONDARY_PERMISSIONS_PASSED EFFECT_STAGE_STARTED; do
  [ "${!flag:-}" = "PASSED" ] || missing="$missing $flag"
done
if [ -z "$missing" ]; then
  exec bash "$(dirname "$0")/vix-unpause.sh" "$MARK"
fi

if [ -f "$MARK" ]; then
  echo "HUMAN REQUIRED: recovery marker $MARK exists (replicas=$(cat "$MARK" 2>/dev/null || echo '?')) but this build did not pass every permission check before its effect phase (unset:$missing) — NOT touching the cluster." >&2
  echo "The marker is kept: the next PERMITTED build's post restores vix-option-inteligence-service from it, or restore it by hand." >&2
  exit 0
fi
echo "post-deploy-recovery: this build did not reach a permitted effect phase (unset:$missing) — no recovery to run, nothing touched."
