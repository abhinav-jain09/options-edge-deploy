#!/usr/bin/env bash
# validate-archive-unit-completeness.sh — every script the archive crontab invokes, and every helper
# those scripts source, must be in the repo AND in the deploy job's UNIT.
#
# WHY: oe-archive-verify.sh — the completeness safety net, written after four prod sessions were lost
# in August 2026 — ran on .252 for a month with no repo copy and no place in UNIT (Bugzilla 356). It
# could not be reviewed, a deploy could not update it, and a hand edit on the host was drift that the
# job's own drift check structurally could not see, because a file outside UNIT is outside every check
# the job makes. The sweep that found it also found oe-trading-day.sh, oe-archive-daily.sh,
# oe-archive-dev.sh and ibkr-raw-retention.sh in the same state — one of which made the verifier exit 0
# having verified nothing.
#
# A missing file here is not a style problem. It is a job that silently does not do its job.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
DIR=scripts/ops/archive
JF=Jenkinsfile.archive-scripts-deploy
fails=0
UNIT_LINE="$(grep -m1 '^    UNIT ' "$JF")"

# 1. everything the crontab invokes by path
wanted="$(grep -oE '/home/abhinav/oe-ops/[A-Za-z0-9._-]+\.sh' "$DIR/oe-archive.crontab" | sed 's|.*/||' | sort -u)"
# 2. everything the unit's own scripts source
# The filter used to accept ^oe- and not ^oe_, so oe_corpus_reader.py — the one file BOTH halves import
# — was silently excluded, and the guard passed with it missing from UNIT. A guard that quietly drops the
# thing it is guarding is worse than none. .env files are named too: oe-topics.env and
# calibration-targets.env are read by the unit and must be installed with it.
sourced="$(grep -hoE '[A-Za-z0-9._-]+\.(sh|py|env)' "$DIR"/*.sh 2>/dev/null \
           | sed 's|.*/||' | grep -E '^(oe[-_]|calibration|market_|push-validation|test-archive)' | sort -u)"
# DELIBERATE exclusions, each with the reason it is one. A list like this is only honest if every entry
# has to earn its place — an unexplained name here is how a real gap gets waved through.
#
#   oe-ops.env                    host-only, holds credentials; must never be in the repo
#   calibration-progress-watch.sh runs on the DEV MAC by design (A5.7: a different host and a different
#                                 schedule from the reporter), so it is deliberately not installed on
#                                 .252 — a watchdog sharing its subject's host shares its failures.
#                                 An exemption here is now an OBLIGATION elsewhere: this name was
#                                 excused by a true reason and then checked by nothing, which is how
#                                 the watchdog reached "in git and installed nowhere" (r19 #2). Its
#                                 install path, plist and sourced files are asserted by
#                                 scripts/ci/validate-dev-mac-watchdog.sh, and the deploy job runs it.
#   install-dev-mac-watchdog.sh   the installer for the above, and equally a dev-Mac host action
#   install-dev-mac-watchdog-test.sh  its test; runs in CI, never on .252
EXEMPT="oe-ops.env calibration-progress-watch.sh install-dev-mac-watchdog.sh install-dev-mac-watchdog-test.sh"

# An exemption that no other guard picks up is a hole with a comment on it. Assert the successor
# exists, here, where the excuse is made.
for _guard in scripts/ci/validate-dev-mac-watchdog.sh; do
  [ -f "$_guard" ] || { echo "MISSING GUARD: $_guard — calibration-progress-watch.sh is exempted here on the promise that $_guard checks it" >&2; exit 1; }
done

for f in $wanted $sourced; do
  case " $EXEMPT " in *" $f "*) continue ;; esac
  if [ ! -f "$DIR/$f" ] && [ ! -f "scripts/jenkins/$f" ]; then
    echo "MISSING FROM THE REPO: $f — the crontab or a unit script names it and nothing tracks it" >&2
    fails=$((fails+1))
    continue
  fi
  # The first entry is preceded by a quote, not a space, so a bare substring test misses it — strip the
  # quotes and pad both sides before matching, or the very first unit member reads as absent.
  unit_names=" $(printf '%s' "$UNIT_LINE" | tr -d '"' | sed 's/^.*UNIT *= *//') "
  case "$unit_names" in
    *" $f "*) : ;;
    *) echo "NOT IN UNIT: $f — it is in the repo but the deploy job will not install or update it" >&2
       fails=$((fails+1)) ;;
  esac
done
if [ "$fails" -ne 0 ]; then
  echo "=== validate-archive-unit-completeness: $fails problem(s) ===" >&2
  exit 1
fi
echo "checked $(printf '%s\n' $wanted $sourced | sort -u | wc -l | tr -d ' ') archive-unit dependencies"
echo "=== validate-archive-unit-completeness: OK ==="
