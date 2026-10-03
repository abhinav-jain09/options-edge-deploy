#!/usr/bin/env bash
# verify-archive-unit-staged.sh — the staged copies the suite will read are the committed sources.
#
# WHY THIS EXISTS. Two archive-unit dependencies live outside scripts/ops/archive — the market
# calendar (owned by the close-chain jobs) and the vol-premium open-reference capture (ops tooling,
# tested from scripts/ops) — and the deploy job copies both INTO that directory, because the suite
# runs in a container with only that directory mounted. Both copies are gitignored there.
#
# scripts/ci/validate-archive-unit-completeness.sh checks the job's TEXT for those copies, and no
# text rule can prove a copy executes: `if false; then cp …; fi` satisfies every one of them. Worse,
# Jenkins reuses workspaces, so a copy left by an earlier build makes a staging step that never ran
# look like one that did — the reviewer of PR #1125 was right that an effect check was missing.
#
# WHAT THIS CHECKS, which is the thing that actually matters: the file the suite will read EXISTS and
# is byte-identical to the committed source it is supposed to be. Provenance is a proxy for that. A
# stale copy identical to its source is harmless — the suite reads the right bytes — and a stale copy
# that differs, or an absent one, fails here. Run it on the agent after staging and before the
# container starts.
#
# It compares and reports. It creates nothing and removes nothing.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2

DIR=scripts/ops/archive
fails=0

# Each dependency, and where it is committed. Keep this list in step with the job's staging
# commands; validate-archive-unit-completeness.sh is what notices when one is missing from the job.
check() { # $1=committed source  $2=staged name
  local source="$1" staged="$DIR/$2"
  if [ ! -f "$source" ]; then
    echo "MISSING SOURCE: $source — nothing to stage from" >&2
    fails=$((fails + 1))
    return
  fi
  if [ ! -f "$staged" ]; then
    echo "NOT STAGED: $staged does not exist, so the suite would run without it — the job's copy of $source did not happen" >&2
    fails=$((fails + 1))
    return
  fi
  if ! cmp -s "$source" "$staged"; then
    echo "STALE STAGED COPY: $staged differs from $source — the suite would test bytes nobody committed (a copy left by an earlier build, or a copy that did not run)" >&2
    fails=$((fails + 1))
    return
  fi
  echo "staged: $2 is byte-identical to $source"
}

check scripts/jenkins/market_calendar.py market_calendar.py
check scripts/ops/vol-premium-open-reference-capture.py vol-premium-open-reference-capture.py

if [ "$fails" -ne 0 ]; then
  echo "=== verify-archive-unit-staged: $fails problem(s) ===" >&2
  exit 1
fi
echo "=== verify-archive-unit-staged: OK ==="
