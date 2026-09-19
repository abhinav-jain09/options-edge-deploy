#!/usr/bin/env bash
# Drives the 'Permitted commit guard' stage of Jenkinsfile.vol-premium-ledger-publish — the EXACT command its
# canonical stage runs (extracted from the Jenkinsfile, never a copy), under the guard version the job declares
# (its PERMITTED_SHA_GUARD_VERSION default, also extracted) — through the cases the Deployment Permission Rule
# names, against throwaway git repositories. Each case asserts the exit status AND the verdict line, so a refusal
# for the wrong reason is a failure too. The stage's FORM (canonical guard stage, every later stage gated on its
# flag) is judged by the shared validator, run here on this one file.
#
# Two classes of case exist because two defects did: refs that merely END in /main (an unquoted `*/main` was a
# wildcard — deploy PR #1044 round 1, I3), and CONTRADICTORY SCM metadata (BRANCH_NAME=main used to mask a
# forbidden GIT_BRANCH — round 2, I4): each variable is judged on its own.
#
# The stale dry-run receipt of a previous build is removed by the stage right after the guard ('Clear previous
# dry-run receipt', gated on the guard's flag). The last cases SEED one (another build id and HEAD), run that
# stage's shell block, and assert it is gone — removing its `rm -f` fails this test.
set -euo pipefail
cd "$(dirname "$0")/../.."
JF="Jenkinsfile.vol-premium-ledger-publish"
GUARD="scripts/jenkins/permitted-sha-guard.sh"
[ -f "$GUARD" ] || { echo "FAIL: $GUARD is missing — the stage calls it"; exit 1; }
bash -n "$GUARD"

python3 scripts/jenkins/validate-jenkinsfile-guard.py --root . --manifest scripts/ci/jenkins-permitted-sha-scope.txt --only "$JF" \
  || { echo "FAIL: $JF is not in the canonical guarded form (see above)"; exit 1; }
CMD="$(python3 - "$JF" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
m = re.search(r"stage\('Permitted commit guard'\) \{.*?def rc = sh\(returnStatus: true, script: '([^']+)'\)", t, re.S)
print(m.group(1) if m else "")
PY
)"
[ "$CMD" = "bash scripts/jenkins/permitted-sha-guard.sh" ] || { echo "FAIL: the guard stage of $JF runs [$CMD], not the shared guard"; exit 1; }
DECLARED="$(sed -n "s/.*string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '\([0-9a-f]\{64\}\)'.*/\1/p" "$JF" | head -1)"
[ "$DECLARED" = "$(bash scripts/jenkins/permitted-sha-guard-version.sh)" ] \
  || { echo "FAIL: $JF declares guard version [$DECLARED], but $GUARD hashes to $(bash scripts/jenkins/permitted-sha-guard-version.sh)"; exit 1; }

# The receipt-clearing stage's shell: the FIRST sh ''' block of the Jenkinsfile. Extracted verbatim.
BLOCK="$(awk "
  /sh[[:space:]]*'''\$/ && !inblock && !done { inblock = 1; next }
  inblock && /^[[:space:]]*'''[[:space:]]*\$/ { inblock = 0; done = 1; next }
  inblock { print }
" "$JF")"
printf '%s\n' "$BLOCK" | grep -q 'rm -f "${WORKSPACE}/vol-premium-ledger-dry-run.receipt"' \
  || { echo "FAIL: the first sh block of $JF must remove the previous build's dry-run receipt"; exit 1; }
grep -qE '"\*/\$branch"' "$GUARD" \
  || { echo "FAIL: $GUARD must match the literal \"*/\$branch\" — unquoted it is a wildcard that admits feature/main"; exit 1; }

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export HOME="$T/home"; mkdir -p "$HOME"
unset PERMITTED_SHA PERMITTED_BRANCH BRANCH_NAME GIT_BRANCH 2>/dev/null || true
printf '%s\n' "$CMD" > "$T/stage.sh"
printf '%s\n' "$BLOCK" > "$T/clear-receipt.sh"
bash -n "$T/stage.sh"
bash -n "$T/clear-receipt.sh"

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
  out="$(cd "$T/work" && env -i HOME="$HOME" PATH="$PATH" WORKSPACE="$T/work" PERMITTED_SHA_GUARD_VERSION="$DECLARED" "$@" bash "$T/stage.sh" 2>&1)" && rc=0 || rc=$?
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
# The branch HEAD must BE the permitted commit (Codex I1): an older main commit is refused on the tip
# condition, before the equality test is ever reached, and says which fault it is.
run "HEAD is the OLDER main commit, permission for the tip" 1 "is on origin/main but is NOT its tip" "$A" PERMITTED_SHA="$B"
run "HEAD is the OLDER main commit, permission for THAT commit" 1 "is NOT its tip" "$A" PERMITTED_SHA="$A"
run "HEAD moved past the permitted commit"             1 "is not the permitted commit" "$B" PERMITTED_SHA="$A"
run "a job declaring another guard version"            1 "PERMITTED_SHA_GUARD_VERSION is" "$B" PERMITTED_SHA="$B" PERMITTED_SHA_GUARD_VERSION="$(printf '0%.0s' $(seq 64))"
# M5: a STALE receipt — another build's, another HEAD's — is seeded and must be gone after the receipt-clearing
# stage. That stage runs only after the guard passed; a refused guard error()s the build, and every later stage
# (the dry run and the confirm) is gated on the guard's flag, so no stage can read a receipt it did not write.
printf 'build=1 kind=calendar version=2026.1 hash=%s file_sha256=%s head=%s\n' "$(printf 'f%.0s' $(seq 64))" "$(printf 'e%.0s' $(seq 64))" "$A" \
  > "$T/work/vol-premium-ledger-dry-run.receipt"
[ -s "$T/work/vol-premium-ledger-dry-run.receipt" ] || { echo "  FAIL could not seed the stale receipt"; fail=$((fail + 1)); }
if (cd "$T/work" && env -i HOME="$HOME" PATH="$PATH" WORKSPACE="$T/work" bash "$T/clear-receipt.sh") && [ ! -e "$T/work/vol-premium-ledger-dry-run.receipt" ]; then
  pass=$((pass + 1)); echo "  ok   the seeded stale receipt is gone after the receipt-clearing stage"
else
  fail=$((fail + 1)); echo "  FAIL the receipt-clearing stage left the stale dry-run receipt in place"
fi
if python3 - "$JF" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
names = re.findall(r"^\s*stage\('([^']*)'\)", t, re.M)
want = ["Permitted commit guard", "Clear previous dry-run receipt", "Validate artefact", "Dry run: PUBLISHABLE?", "Publish (--confirm)"]
assert names == want, names
for n in want[1:]:
    body = t[t.index("stage('%s')" % n):]
    body = body[:body.index("steps {")]
    assert re.search(r"expression \{ env\.PERMITTED_SHA_GUARD == 'PASSED'", body), n
PY
then
  pass=$((pass + 1)); echo "  ok   receipt clearing, validation, dry run and confirm follow the guard, each gated on its flag"
else
  fail=$((fail + 1)); echo "  FAIL stage order or gates"
fi
echo "vol-premium-ledger-publish guard: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== vol-premium-ledger-publish-guard-test: OK ==="; exit 0; }
echo "=== vol-premium-ledger-publish-guard-test: FAILED ==="; exit 1
