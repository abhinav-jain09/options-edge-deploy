#!/usr/bin/env bash
# Resolve the baseline directory and run a stateful watch check, as ONE command the deliverer can wrap.
#
#   FLEET_ENV=dev scripts/watch/stateful-check-run.sh kafka-lag.py [--max-lag 50000 ...]
#
# WHY A WRAPPER, and this is the bug it exists to prevent: the Jenkinsfile used to say
#   STATE="$(dirname "$WORKSPACE")/.oe-watch"; mkdir -p "$STATE" || exit 2; ... fleet-alert-deliver.sh ...
# so a state directory that could not be created exited the Jenkins shell BEFORE the deliverer ever ran -
# and "the check could not run" is the one state that must always reach Discord (R1e, §1b). Found by Codex
# on the implementation review; it is the same shape as the kubectl failure crash-loop-run.sh already
# handles. Doing the setup inside the wrapped command makes it an ordinary exit 2, which IS delivered.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CHECK="${1:?usage: stateful-check-run.sh <check.py> [args...]}"
shift
FLEET_ENV="${FLEET_ENV:?FLEET_ENV must be set}"

# FLEET_STATE_DIR wins, so the tests do not need a WORKSPACE. In Jenkins the baseline lives beside the job
# workspaces - a sibling, so it survives between builds and is writable by the agent user.
STATE="${FLEET_STATE_DIR:-}"
if [ -z "$STATE" ]; then
  if [ -z "${WORKSPACE:-}" ]; then
    echo "ERROR: neither FLEET_STATE_DIR nor WORKSPACE is set - cannot locate the baseline directory"
    exit 2
  fi
  STATE="$(dirname "$WORKSPACE")/.oe-watch"
fi
if ! mkdir -p "$STATE" 2>/dev/null; then
  echo "ERROR: cannot create the baseline directory $STATE"
  exit 2
fi
if [ ! -w "$STATE" ]; then
  echo "ERROR: the baseline directory $STATE is not writable - a baseline that cannot be kept would make every run a first sight"
  exit 2
fi
[ -r "$HERE/$CHECK" ] || { echo "ERROR: $HERE/$CHECK is not readable"; exit 2; }

exec python3 "$HERE/$CHECK" --env "$FLEET_ENV" --state "$STATE" "$@"
