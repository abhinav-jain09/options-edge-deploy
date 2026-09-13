#!/usr/bin/env bash
# Drives the 'Permitted commit (main only)' stage of Jenkinsfile.vol-premium-ledger-publish — the shell block
# EXTRACTED from the Jenkinsfile, never a copy — through the cases the Deployment Permission Rule names, against
# throwaway git repositories. Each case asserts the exit status AND the verdict line, so a refusal for the wrong
# reason is a failure too. The forbidden refs whose names END in /main are here because an unquoted `*/main` in a
# case pattern is a wildcard and admitted feature/main (deploy PR #1044 round 1, I3).
#
# When scripts/jenkins/permitted-sha-guard.sh (PR #1043) lands and the stage calls it instead, retarget this
# file at that script's own test or retire it — but not before.
set -euo pipefail
cd "$(dirname "$0")/../.."
JF="Jenkinsfile.vol-premium-ledger-publish"

# The FIRST sh ''' block of the Jenkinsfile is the guard stage's. Extract it verbatim (the same line-based
# slice validate-jenkinsfile-shell-blocks.sh uses); refuse if the shape ever changes.
BLOCK="$(awk "
  /sh[[:space:]]*'''\$/ && !inblock && !done { inblock = 1; next }
  inblock && /^[[:space:]]*'''[[:space:]]*\$/ { inblock = 0; done = 1; next }
  inblock { print }
" "$JF")"
printf '%s\n' "$BLOCK" | grep -q 'permitted-sha-guard: verdict=PERMITTED' \
  || { echo "FAIL: the first sh block of $JF is not the permitted-commit guard (the Jenkinsfile's stage order changed?)"; exit 1; }
printf '%s\n' "$BLOCK" | grep -qE '"\*/main"' \
  || { echo "FAIL: the guard's ref pattern must quote the literal \"*/main\" — unquoted it is a wildcard that admits feature/main"; exit 1; }

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export HOME="$T/home"; mkdir -p "$HOME"
unset PERMITTED_SHA BRANCH_NAME GIT_BRANCH 2>/dev/null || true
printf '%s\n' "$BLOCK" > "$T/guard.sh"
bash -n "$T/guard.sh"

# origin: main A -> B; feature: C (off A, not on main)
git init -q -b main "$T/seed"
git -C "$T/seed" commit -q --allow-empty -m A; A="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" commit -q --allow-empty -m B; B="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" checkout -q -b feature "$A"
git -C "$T/seed" commit -q --allow-empty -m C; C="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" checkout -q main
git clone -q --bare "$T/seed" "$T/origin.git"
git clone -q "$T/origin.git" "$T/work"                   # the Jenkins workspace, at main (B)
git -C "$T/work" fetch -q origin feature:feature

pass=0; fail=0
# run <name> <expected exit> <expected substring> <checkout ref> [env=val ...]
run() {
  local name="$1" want_rc="$2" want="$3" ref="$4"; shift 4
  git -C "$T/work" checkout -q --detach "$ref"
  local out rc
  out="$(cd "$T/work" && env -i HOME="$HOME" PATH="$PATH" WORKSPACE="$T/work" "$@" bash "$T/guard.sh" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then
    pass=$((pass + 1)); echo "  ok   $name (rc=$rc)"
  else
    fail=$((fail + 1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -4 | sed 's/^/       | /'
  fi
}
run "main, exact SHA"                              0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH=origin/main
run "BRANCH_NAME=main"                             0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" BRANCH_NAME=main
run "literal */main (the branch spec itself)"      0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH='*/main'
run "refs/remotes/origin/main"                     0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH=refs/remotes/origin/main
run "no branch reported, on main"                  0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B"
run "FORBIDDEN feature/main (ends in /main)"       1 "this job publishes only from main" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature/main
run "FORBIDDEN origin/feature/main"                1 "this job publishes only from main" "$B" PERMITTED_SHA="$B" GIT_BRANCH=origin/feature/main
run "FORBIDDEN release/main"                       1 "this job publishes only from main" "$B" PERMITTED_SHA="$B" GIT_BRANCH=release/main
run "FORBIDDEN mainline"                           1 "this job publishes only from main" "$B" PERMITTED_SHA="$B" BRANCH_NAME=mainline
run "FORBIDDEN feature"                            1 "this job publishes only from main" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature
run "commit not on origin/main"                    1 "is not on origin/main" "$C" PERMITTED_SHA="$C"
run "PERMITTED_SHA empty"                          1 "PERMITTED_SHA is empty" "$B" PERMITTED_SHA=
run "PERMITTED_SHA unset"                          1 "PERMITTED_SHA is empty" "$B"
run "PERMITTED_SHA short"                          1 "a short SHA is refused" "$B" PERMITTED_SHA="${B:0:12}"
run "PERMITTED_SHA uppercase"                      1 "not a lowercase 40-hex commit id" "$B" PERMITTED_SHA="$(printf '%s' "$B" | tr a-f A-F)"
run "PERMITTED_SHA is a branch name"               1 "not a lowercase 40-hex commit id" "$B" PERMITTED_SHA=main
run "PERMITTED_SHA with whitespace"                1 "contains whitespace" "$B" PERMITTED_SHA="$B "
run "HEAD is the OLDER main commit, permission for the tip" 1 "is not the permitted commit" "$A" PERMITTED_SHA="$B"
run "HEAD moved past the permitted commit"         1 "is not the permitted commit" "$B" PERMITTED_SHA="$A"
run "stale receipt is removed by the guard"        0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B"
[ ! -e "$T/work/vol-premium-ledger-dry-run.receipt" ] || { echo "  FAIL the guard left a dry-run receipt in place"; fail=$((fail + 1)); }
echo "vol-premium-ledger-publish guard: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== vol-premium-ledger-publish-guard-test: OK ==="; exit 0; }
echo "=== vol-premium-ledger-publish-guard-test: FAILED ==="; exit 1
