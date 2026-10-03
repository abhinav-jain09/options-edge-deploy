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
# WHAT IT DOES NOT CLOSE, said rather than implied: the interval between this check and the
# container starting. Nothing stands between them in the job but the docker invocation itself, and
# refusing symlinks removes the easy way to change the bytes in that interval, but a concurrent
# writer with access to the workspace could still replace a regular file. The workspace is the
# agent's own and the job holds it for the build; this is the boundary, not a proof.
#
# It compares and reports. It creates nothing and removes nothing.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2

DIR=scripts/ops/archive
fails=0

# Each dependency, and where it is committed. Keep this list in step with the job's staging
# commands; validate-archive-unit-completeness.sh is what notices when one is missing from the job.
# NEITHER PATH MAY BE A SYMLINK, and `[ -f ]` and `cmp` would both have followed one. A staged
# symlink makes the comparison true of a file somewhere else, and the bytes the container reads can
# change after this ran by swapping the target — so comparing them proves nothing about what the
# suite sees. A symlinked SOURCE is refused for the matching reason: what is committed is then a
# pointer, and the comparison is about whatever it currently points at. The job stages with a plain
# `cp`, which produces a regular file, so refusing links costs nothing and closes the indirection.
#
# `[ -L ]` is tested BEFORE `[ -f ]` because `[ -f ]` is true of a symlink to a regular file; a
# directory fails `[ -f ]` and is reported as the absent or unstageable thing it is.
check() { # $1=committed source  $2=staged name
  local source="$1" staged="$DIR/$2"
  if [ -L "$source" ]; then
    echo "SOURCE IS A SYMLINK: $source — what is committed must be the bytes, not a pointer to them" >&2
    fails=$((fails + 1))
    return
  fi
  if [ ! -f "$source" ]; then
    echo "MISSING SOURCE: $source — nothing to stage from (or it is not a regular file)" >&2
    fails=$((fails + 1))
    return
  fi
  if [ -L "$staged" ]; then
    echo "STAGED COPY IS A SYMLINK: $DIR/$2 — the suite would read whatever it points at, which can change after this check; the job stages with cp and must produce a regular file" >&2
    fails=$((fails + 1))
    return
  fi
  if [ ! -f "$staged" ]; then
    echo "NOT STAGED: $staged does not exist as a regular file, so the suite would run without it — the job's copy of $source did not happen" >&2
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
