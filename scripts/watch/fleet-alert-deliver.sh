#!/usr/bin/env bash
# Run one fleet check, compose its Discord message and deliver it (FLEET-ALERT-DISCORD-REQUIREMENT R1).
#
#   FLEET_ENV=dev FLEET_CHECK=crash-loop FLEET_MODE=paging \
#     scripts/watch/fleet-alert-deliver.sh python3 scripts/watch/crash-loop.py --input pods.json
#
# The LOGIC LIVES HERE, NOT IN THE JENKINSFILE, for two reasons: a Groovy pipeline is shell-checked by
# nothing, and the backslash escapes inside a '''...''' sh block are read by Groovy before the shell (see
# scripts/ci/validate-jenkinsfile-groovy-escapes.sh). Keeping the Jenkinsfile to one call per check means
# this path can be tested without Jenkins, which is what fleet-alert-deliver-test.sh does.
#
# OBSERVATION-ONLY means what it says: a FINDING (exit 1) from an observation-only check is printed in the build log
# and does NOT fail the build. Before this, partition-balance (observation-only since 11-07) turned the fleet-watch job
# red on every run because of findings nobody is paged for - so a red build stopped meaning anything. Exit 2 ("could
# not check") is still re-raised in every mode.
#
# THE CHECK'S EXIT STATUS IS THE BUILD'S VERDICT and is re-raised at the end: Discord delivery is ADDED to
# the red build, never substituted for it (R6c). Delivery failure never changes that status (R1d) - alert()
# returns 0 even when curl fails, and the condition it describes is the caller's to act on.
set -uo pipefail

FLEET_ENV="${FLEET_ENV:?FLEET_ENV must be set (dev|production|es4)}"
FLEET_CHECK="${FLEET_CHECK:?FLEET_CHECK must be set, e.g. crash-loop}"
FLEET_MODE="${FLEET_MODE:-paging}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ALERT_SH="${ALERT_SH:-$REPO/scripts/ops/archive/oe-alert.sh}"
COMPOSER="${COMPOSER:-$HERE/fleet-alert.py}"

case "$FLEET_MODE" in
  paging | observation-only) ;;
  *) echo "fleet-alert-deliver: FLEET_MODE must be paging or observation-only (got '$FLEET_MODE')" >&2; exit 2 ;;
esac
if [ "$#" -eq 0 ]; then
  echo "fleet-alert-deliver: no check command given" >&2
  exit 2
fi
for f in "$ALERT_SH" "$COMPOSER"; do
  [ -r "$f" ] || { echo "fleet-alert-deliver: $f is not readable" >&2; exit 2; }
done

# Where the open incidents live (see fleet-alert.py, PAGING DISCIPLINE): FLEET_STATE_DIR wins for the tests; in Jenkins it is the
# .oe-watch directory beside the job workspaces, the same one the stateful checks keep their baselines in. If it cannot be
# made, the composer says so in the message and pages every run - loud, never silent - so this does not exit here.
STATE_DIR="${FLEET_STATE_DIR:-${WORKSPACE:+$(dirname "$WORKSPACE")/.oe-watch}}"
STATE_DIR="${STATE_DIR:-${TMPDIR:-/tmp}/oe-watch-alerts}"
mkdir -p "$STATE_DIR" 2>/dev/null || true

out="$(mktemp)"
trap 'rm -f "$out"' EXIT

# The check's own output is the record: it goes to the build log AND to the composer. Captured with the
# status preserved - `set -o pipefail` plus an explicit rc, because a `tee` in a pipeline would otherwise
# hand us tee's status and every failing check would read as a passing one.
rc=0
"$@" > "$out" 2>&1 || rc=$?
echo "=== $FLEET_CHECK ($FLEET_ENV, $FLEET_MODE) exit $rc ==="
verdict="$rc"
if [ "$FLEET_MODE" = observation-only ] && [ "$rc" = 1 ]; then
  verdict=0
  echo "(observation-only: findings below are logged, not paged, and do not fail the build)"
fi
cat "$out"

msg="$(python3 "$COMPOSER" --env "$FLEET_ENV" --check "$FLEET_CHECK" --rc "$rc" --input "$out" \
        --mode "$FLEET_MODE" --state-dir "$STATE_DIR" ${BUILD_URL:+--build-url "$BUILD_URL"})" || {
  echo "fleet-alert-deliver: the composer itself failed; the condition above still stands" >&2
  exit "$verdict"
}

if [ -z "${msg//[[:space:]]/}" ]; then
  exit "$verdict"     # nothing to say: exit 0, an incident already announced, or a finding in observation-only mode
fi

# oe-alert.sh is the ONE implementation of delivery and declares itself "sourced, never executed". It logs
# the message BEFORE attempting delivery, honours an explicitly-passed DISCORD_WEBHOOK_URL over any file,
# truncates over-long payloads, and returns 0 even when curl fails.
# shellcheck source=/dev/null
. "$ALERT_SH"
alert "$msg"
exit "$verdict"
