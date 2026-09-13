#!/usr/bin/env bash
# Drives the 'Permitted commit (main only)' stage of Jenkinsfile.vol-premium-ledger-publish — the shell block
# EXTRACTED from the Jenkinsfile (never a copy), which runs the VENDORED scripts/jenkins/permitted-sha-guard.sh —
# through the cases the Deployment Permission Rule names, against throwaway git repositories. Each case asserts the
# exit status AND the verdict line, so a refusal for the wrong reason is a failure too.
#
# Two classes of case exist because two defects did: refs that merely END in /main (an unquoted `*/main` was a
# wildcard — deploy PR #1044 round 1, I3), and CONTRADICTORY SCM metadata (BRANCH_NAME=main used to mask a
# forbidden GIT_BRANCH — round 2, I4): each variable is judged on its own now.
#
# The stage also removes any dry-run receipt left by a previous build; the last case SEEDS one (a different build
# id and HEAD) and asserts it is gone afterwards — removing the stage's `rm -f` fails this test.
set -euo pipefail
cd "$(dirname "$0")/../.."
JF="Jenkinsfile.vol-premium-ledger-publish"
GUARD="scripts/jenkins/permitted-sha-guard.sh"
[ -f "$GUARD" ] || { echo "FAIL: $GUARD is missing — the stage calls it"; exit 1; }
bash -n "$GUARD"

# The FIRST sh ''' block of the Jenkinsfile is the guard stage's. Extract it verbatim; refuse if the shape changed.
BLOCK="$(awk "
  /sh[[:space:]]*'''\$/ && !inblock && !done { inblock = 1; next }
  inblock && /^[[:space:]]*'''[[:space:]]*\$/ { inblock = 0; done = 1; next }
  inblock { print }
" "$JF")"
printf '%s\n' "$BLOCK" | grep -q 'bash scripts/jenkins/permitted-sha-guard.sh' \
  || { echo "FAIL: the first sh block of $JF does not run $GUARD (the Jenkinsfile's stage order changed?)"; exit 1; }
printf '%s\n' "$BLOCK" | grep -q 'rm -f "${WORKSPACE}/vol-premium-ledger-dry-run.receipt"' \
  || { echo "FAIL: the guard stage must remove the previous build's dry-run receipt before the guard runs"; exit 1; }
grep -qE '"\*/\$branch"' "$GUARD" \
  || { echo "FAIL: $GUARD must match the literal \"*/\$branch\" — unquoted it is a wildcard that admits feature/main"; exit 1; }

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export HOME="$T/home"; mkdir -p "$HOME"
unset PERMITTED_SHA PERMITTED_BRANCH BRANCH_NAME GIT_BRANCH 2>/dev/null || true
printf '%s\n' "$BLOCK" > "$T/stage.sh"
bash -n "$T/stage.sh"

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
mkdir -p "$T/work/scripts/jenkins" && cp "$GUARD" "$T/work/scripts/jenkins/permitted-sha-guard.sh"   # the stage runs it from the workspace

pass=0; fail=0
# run <name> <expected exit> <expected substring> <checkout ref> [env=val ...]
run() {
  local name="$1" want_rc="$2" want="$3" ref="$4"; shift 4
  git -C "$T/work" checkout -q --detach "$ref"
  local out rc
  out="$(cd "$T/work" && env -i HOME="$HOME" PATH="$PATH" WORKSPACE="$T/work" "$@" bash "$T/stage.sh" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then
    pass=$((pass + 1)); echo "  ok   $name (rc=$rc)"
  else
    fail=$((fail + 1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -4 | sed 's/^/       | /'
  fi
}
run "main, exact SHA"                                  0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH=origin/main
run "BRANCH_NAME=main"                                 0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" BRANCH_NAME=main
run "BRANCH_NAME=main AND GIT_BRANCH=origin/main"      0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/main
run "literal */main (the branch spec itself)"          0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH='*/main'
run "refs/remotes/origin/main"                         0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B" GIT_BRANCH=refs/remotes/origin/main
run "no branch reported, on main"                      0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B"
run "FORBIDDEN feature/main (ends in /main)"           1 "this job deploys only from 'main'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature/main
run "FORBIDDEN origin/feature/main"                    1 "this job deploys only from 'main'" "$B" PERMITTED_SHA="$B" GIT_BRANCH=origin/feature/main
run "FORBIDDEN release/main"                           1 "this job deploys only from 'main'" "$B" PERMITTED_SHA="$B" GIT_BRANCH=release/main
run "FORBIDDEN mainline"                               1 "this job deploys only from 'main'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=mainline
run "FORBIDDEN feature"                                1 "this job deploys only from 'main'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature
run "CONTRADICTION BRANCH_NAME=main + GIT_BRANCH=origin/feature"      1 "GIT_BRANCH is 'origin/feature'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/feature
run "CONTRADICTION BRANCH_NAME=main + GIT_BRANCH=origin/feature/main" 1 "GIT_BRANCH is 'origin/feature/main'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/feature/main
run "CONTRADICTION BRANCH_NAME=feature + GIT_BRANCH=origin/main"      1 "BRANCH_NAME is 'feature'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature GIT_BRANCH=origin/main
run "BRANCH_NAME unset + GIT_BRANCH=origin/feature"    1 "GIT_BRANCH is 'origin/feature'" "$B" PERMITTED_SHA="$B" GIT_BRANCH=origin/feature
run "BRANCH_NAME=feature + GIT_BRANCH unset"           1 "BRANCH_NAME is 'feature'" "$B" PERMITTED_SHA="$B" BRANCH_NAME=feature
run "commit not on origin/main"                        1 "is not on origin/main" "$C" PERMITTED_SHA="$C"
run "PERMITTED_SHA empty"                              1 "PERMITTED_SHA is empty" "$B" PERMITTED_SHA=
run "PERMITTED_SHA unset"                              1 "PERMITTED_SHA is not set" "$B"
run "PERMITTED_SHA short"                              1 "a short SHA is refused" "$B" PERMITTED_SHA="${B:0:12}"
run "PERMITTED_SHA uppercase"                          1 "must be lowercase hex" "$B" PERMITTED_SHA="$(printf '%s' "$B" | tr a-f A-F)"
run "PERMITTED_SHA is a branch name"                   1 "is not a commit id" "$B" PERMITTED_SHA=main
run "PERMITTED_SHA with whitespace"                    1 "contains whitespace" "$B" PERMITTED_SHA="$B "
run "HEAD is the OLDER main commit, permission for the tip" 1 "is not the permitted commit" "$A" PERMITTED_SHA="$B"
run "HEAD moved past the permitted commit"             1 "is not the permitted commit" "$B" PERMITTED_SHA="$A"
# M5: a STALE receipt — another build's, another HEAD's — is seeded, must exist, and must be gone afterwards.
printf 'build=1 kind=calendar version=2026.1 hash=%s file_sha256=%s head=%s\n' "$(printf 'f%.0s' $(seq 64))" "$(printf 'e%.0s' $(seq 64))" "$A" \
  > "$T/work/vol-premium-ledger-dry-run.receipt"
[ -s "$T/work/vol-premium-ledger-dry-run.receipt" ] || { echo "  FAIL could not seed the stale receipt"; fail=$((fail + 1)); }
run "stale receipt from another build is removed by the guard" 0 "verdict=PERMITTED" "$B" PERMITTED_SHA="$B"
if [ -e "$T/work/vol-premium-ledger-dry-run.receipt" ]; then
  echo "  FAIL the guard left the stale dry-run receipt in place"; fail=$((fail + 1))
else
  pass=$((pass + 1)); echo "  ok   the seeded stale receipt is gone after the guard stage"
fi
# and a REFUSED guard removes it too (the rm precedes the verdict), so a later stage can never see it
printf 'build=1 stale\n' > "$T/work/vol-premium-ledger-dry-run.receipt"
run "stale receipt is removed even when the guard refuses" 1 "verdict=REFUSED" "$B" PERMITTED_SHA=
[ ! -e "$T/work/vol-premium-ledger-dry-run.receipt" ] && { pass=$((pass + 1)); echo "  ok   stale receipt gone after a refusal too"; } || { fail=$((fail + 1)); echo "  FAIL stale receipt survived a refusal"; }
echo "vol-premium-ledger-publish guard: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== vol-premium-ledger-publish-guard-test: OK ==="; exit 0; }
echo "=== vol-premium-ledger-publish-guard-test: FAILED ==="; exit 1
