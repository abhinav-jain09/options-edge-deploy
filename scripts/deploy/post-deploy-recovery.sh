#!/usr/bin/env bash
# post{always} of service-deploy's Deploy path — the ONLY post path that can touch the cluster.
#
# Deployment Permission Rule: a build that any permission check refused must not mutate a target
# through post/finally either, and a build may recover only what IT did. post{always} runs regardless,
# so the recovery (scripts/deploy/vix-unpause.sh, restoring vix-option-inteligence-service from the
# pause marker) runs only when ALL of the following hold for THIS build:
#   PERMITTED_SHA_GUARD          the first guard passed, on the deploy host
#   DEPLOY_WORKSPACE_PERMITTED   the deploy-workspace guard passed (set nowhere else)
#   SECONDARY_PERMISSIONS_PASSED every secondary input (processing/contracts SHAs, child definition,
#                                child result, its image lock, a caller's REQUIRED_IMAGE) was judged
#   EFFECT_STAGE_STARTED         the effect phase began (the VIX reconcile or the Deploy stage)
#   and the marker is THIS build's (scripts/deploy/vix-pause-marker.sh owned): written by this job,
#   this BUILD_ID, under this PERMITTED_SHA, for SERVICE=vix-option-inteligence, in a non-dry run.
# A marker stranded by another build — including a VIX marker seen by a web, dry-run or any other run —
# is preserved and reported HUMAN REQUIRED; nothing is scaled. DEPLOY_DRY_RUN never recovers.
#
# Exit status: the recovery's own when it runs; 0 otherwise (a refused build already failed at its
# refusal; this path only reports and touches nothing).
set -uo pipefail
MARK="${1:?usage: post-deploy-recovery.sh <marker-file>}"
here="$(dirname "$0")"

if [ ! -e "$MARK" ]; then
  echo "post-deploy-recovery: no pause marker — no recovery to run, nothing touched."
  exit 0
fi

missing=""
for flag in PERMITTED_SHA_GUARD DEPLOY_WORKSPACE_PERMITTED SECONDARY_PERMISSIONS_PASSED EFFECT_STAGE_STARTED; do
  [ "${!flag:-}" = "PASSED" ] || missing="$missing $flag"
done
if [ -n "$missing" ]; then
  echo "HUMAN REQUIRED: pause marker $MARK exists but this build did not pass every permission check before its effect phase (unset:$missing) — NOT touching the cluster; marker kept." >&2
  sed 's/^/  marker: /' "$MARK" >&2 2>/dev/null || true
  exit 0
fi
if ! why="$(bash "$here/vix-pause-marker.sh" owned "$MARK")"; then
  echo "HUMAN REQUIRED: pause marker $MARK is not this build's to recover (${why#*— }) — NOT touching the cluster; marker kept." >&2
  sed 's/^/  marker: /' "$MARK" >&2 2>/dev/null || true
  exit 0
fi
echo "$why"
exec bash "$here/vix-unpause.sh" "$MARK"
