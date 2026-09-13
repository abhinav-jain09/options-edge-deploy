#!/usr/bin/env bash
# Drives scripts/jenkins/permitted-sha-guard.sh through every case the Deployment Permission Rule's
# acceptance contract names, against throwaway git repositories. A guard nobody exercises is a guard
# that quietly starts permitting everything; each case below asserts BOTH the exit status and the
# reason the guard printed, so a refusal for the wrong reason is a failure too.
#
# Bash rather than Python on purpose: the same file runs unchanged in every repository that carries
# the guard (options-edge-deploy, option-edge-feed-gateway, options-edge-processing), and the
# deploy repository's unittest wraps it.
set -euo pipefail
GUARD="${GUARD:-$(cd "$(dirname "$0")" && pwd)/permitted-sha-guard.sh}"
[ -f "$GUARD" ] || { echo "FAIL: guard script not found at $GUARD"; exit 1; }
bash -n "$GUARD"

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export HOME="$T/home"; mkdir -p "$HOME"   # no user git config can leak in
# Jenkins-shaped environment: nothing from the caller's shell may leak into the guard's verdict.
unset PERMITTED_SHA PERMITTED_BRANCH BRANCH_NAME GIT_BRANCH 2>/dev/null || true

# origin: main A -> B; feature: C (off A, not on main)
git init -q -b main "$T/seed"
git -C "$T/seed" commit -q --allow-empty -m A; A="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" commit -q --allow-empty -m B; B="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" checkout -q -b feature "$A"
git -C "$T/seed" commit -q --allow-empty -m C; C="$(git -C "$T/seed" rev-parse HEAD)"
git -C "$T/seed" checkout -q main
git clone -q --bare "$T/seed" "$T/origin.git"
# work: the Jenkins workspace, a clone of origin at main (B)
git clone -q "$T/origin.git" "$T/work"
git -C "$T/work" fetch -q origin feature:feature

pass=0; fail=0
# run <name> <expected-exit> <expected-substring> <dir> [env=val ...] -- [guard args...]
run() {
  local name="$1" want_rc="$2" want_msg="$3" dir="$4"; shift 4
  local envs=() ; while [ $# -gt 0 ] && [ "$1" != "--" ]; do envs+=("$1"); shift; done
  [ $# -gt 0 ] && shift
  local out rc
  set +e
  out="$(cd "$dir" && env "${envs[@]}" bash "$GUARD" "$@" 2>&1)"; rc=$?
  set -e
  if [ "$rc" -ne "$want_rc" ]; then
    echo "FAIL [$name]: exit $rc, wanted $want_rc"; echo "$out" | sed 's/^/    /'; fail=$((fail+1)); return
  fi
  if ! printf '%s' "$out" | grep -qF -- "$want_msg"; then
    echo "FAIL [$name]: output lacks '$want_msg'"; echo "$out" | sed 's/^/    /'; fail=$((fail+1)); return
  fi
  # Both SHAs must be in the log on every run that got as far as resolving HEAD, permitted or refused.
  # (A non-checkout refuses before that and prints only its reason — documented in the guard.)
  if printf '%s' "$want_msg" | grep -qF "is not a git checkout"; then echo "ok   [$name]"; pass=$((pass+1)); return; fi
  if ! printf '%s' "$out" | grep -qF "checked-out HEAD"; then
    echo "FAIL [$name]: checked-out HEAD line missing"; fail=$((fail+1)); return
  fi
  if ! printf '%s' "$out" | grep -qF "PERMITTED_SHA     ="; then
    echo "FAIL [$name]: PERMITTED_SHA line missing"; fail=$((fail+1)); return
  fi
  echo "ok   [$name]"; pass=$((pass+1))
}

W="$T/work"
# --- the permitted case: A(=B here) permitted, branch main, checkout is that commit ---------------
run "permitted A, branch main, checkout A"   0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B"
run "permitted, Jenkins reports origin/main" 0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" GIT_BRANCH=origin/main
run "permitted, Jenkins reports */main"      0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" GIT_BRANCH='*/main'
# --- missing / empty / invalid permission input: terminate, never substitute ---------------------
run "PERMITTED_SHA unset"                    1 "PERMITTED_SHA is not set"     "$W"
run "PERMITTED_SHA empty"                    1 "PERMITTED_SHA is empty"       "$W" PERMITTED_SHA=
run "PERMITTED_SHA whitespace only"          1 "contains whitespace"          "$W" PERMITTED_SHA='   '
run "PERMITTED_SHA padded"                   1 "contains whitespace"          "$W" PERMITTED_SHA=" $B "
run "short SHA (12)"                         1 "short SHA is refused"         "$W" PERMITTED_SHA="${B:0:12}"
run "short SHA (39)"                         1 "short SHA is refused"         "$W" PERMITTED_SHA="${B:0:39}"
run "41 chars"                               1 "has 41 characters"            "$W" PERMITTED_SHA="${B}0"
run "uppercase SHA"                          1 "must be lowercase hex"        "$W" PERMITTED_SHA="$(printf '%s' "$B" | tr 'a-f' 'A-F')"
run "branch name as SHA"                     1 "not a commit id"              "$W" PERMITTED_SHA=main
run "HEAD literal as SHA"                    1 "not a commit id"              "$W" PERMITTED_SHA=HEAD
run "ref as SHA"                             1 "not a commit id"              "$W" PERMITTED_SHA=origin/main
run "tag name as SHA"                        1 "not a commit id"              "$W" PERMITTED_SHA=v1.2.3
# --- permitted A, actual checkout B: stop ------------------------------------------------------------
run "permitted A, checkout B"                1 "is not the permitted commit"  "$W" PERMITTED_SHA="$A"
# --- the branch advances between queue and checkout: the newer commit cannot deploy under A -------
git clone -q "$T/origin.git" "$T/mover"
git -C "$T/mover" commit -q --allow-empty -m D; D="$(git -C "$T/mover" rev-parse HEAD)"
git -C "$T/mover" push -q origin main
git -C "$W" fetch -q origin && git -C "$W" checkout -q "$D"     # Jenkins checks out the new tip
run "branch advanced B->D, permitted B"      1 "is not the permitted commit"  "$W" PERMITTED_SHA="$B"
run "branch advanced, permitted D"           0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$D"
git -C "$W" checkout -q "$B"
# --- matching SHA but forbidden source ref: the branch restriction stops it on its own -----------
git -C "$W" checkout -q "$C"
run "feature commit C, permitted C"          1 "is not on origin/main"        "$W" PERMITTED_SHA="$C"
run "feature commit C, no SHA: branch first" 1 "is not on origin/main"        "$W"
git -C "$W" checkout -q "$B"
run "main commit but GIT_BRANCH reports feature" 1 "GIT_BRANCH is 'origin/feature'; this job deploys only from 'main'" "$W" PERMITTED_SHA="$B" GIT_BRANCH=origin/feature
run "BRANCH_NAME=feature refused, GIT_BRANCH=origin/main notwithstanding" 1 "BRANCH_NAME is 'feature'" "$W" PERMITTED_SHA="$B" BRANCH_NAME=feature GIT_BRANCH=origin/main
run "BRANCH_NAME=main does NOT mask GIT_BRANCH=origin/feature" 1 "GIT_BRANCH is 'origin/feature'" "$W" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/feature
run "both variables main"                    0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/main
run "--branch dev refuses a main checkout"   1 "deploys only from 'dev'"      "$W" PERMITTED_SHA="$B" GIT_BRANCH=origin/main -- --branch dev
# --- the explicitly SELECTED source ref (--ref): a feature ref pointing at a merged commit is still refused
run "--ref feature at a merged commit"       1 "selected source ref is 'feature'" "$W" PERMITTED_SHA="$B" -- --ref feature
run "--ref main"                             0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" -- --ref main
run "--ref origin/main"                      0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" -- --ref origin/main
run "--ref */main (Jenkins spelling)"        0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" -- --ref '*/main'
run "--ref refs/heads/feature"               1 "selected source ref is 'refs/heads/feature'" "$W" PERMITTED_SHA="$B" -- --ref refs/heads/feature
# --- the branch cannot be confirmed: refuse, do not guess ------------------------------------------
git clone -q "$T/origin.git" "$T/noremote"
git -C "$T/noremote" remote set-url origin "$T/does-not-exist.git"
run "origin unreachable"                     1 "could not fetch origin/main"  "$T/noremote" PERMITTED_SHA="$B"
# --- not a checkout at all ------------------------------------------------------------------------
mkdir -p "$T/plain"
set +e; out="$(cd "$T/plain" && PERMITTED_SHA="$B" bash "$GUARD" 2>&1)"; rc=$?; set -e
if [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -qF "is not a git checkout"; then echo "ok   [not a git checkout]"; pass=$((pass+1)); else echo "FAIL [not a git checkout]: rc=$rc"; echo "$out"; fail=$((fail+1)); fi
# --- not a WORKING checkout: a .git metadata directory and a bare repository both print `false` ----
run ".git metadata dir refused"              1 "is not a git checkout"        "$W" PERMITTED_SHA="$B" -- --dir .git
run "bare repository refused"                1 "is not a git checkout"        "$T" PERMITTED_SHA="$B" -- --dir origin.git
# --- nested application checkout (--dir): its own SHA; the job's GIT_BRANCH does not describe it --
git clone -q "$T/origin.git" "$W/nested-src"
git -C "$W/nested-src" checkout -q "$B"    # origin/main is at D by now; B is still on it
run "nested at B, permitted B (job ref irrelevant)" 0 "verdict=PERMITTED"     "$W" PERMITTED_SHA="$B" GIT_BRANCH=origin/whatever -- --dir nested-src
git -C "$W/nested-src" fetch -q origin feature:feature && git -C "$W/nested-src" checkout -q "$C"
run "nested at C (feature), permitted C"     1 "is not on origin/main"        "$W" PERMITTED_SHA="$C" -- --dir nested-src
git -C "$W/nested-src" checkout -q "$B"
run "nested at B, permitted D"               1 "is not the permitted commit"  "$W" PERMITTED_SHA="$D" -- --dir nested-src
run "nested, no SHA"                         1 "PERMITTED_SHA is not set"     "$W" -- --dir nested-src
run "nested --ref feature at a merged commit" 1 "selected source ref is 'feature'" "$W" PERMITTED_SHA="$B" -- --dir nested-src --ref feature
run "nested --ref main"                      0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$B" -- --dir nested-src --ref main
# --- usage errors are refusals too ---------------------------------------------------------------
set +e; out="$(cd "$W" && PERMITTED_SHA="$B" bash "$GUARD" --bogus 2>&1)"; rc=$?; set -e
if [ "$rc" -eq 2 ]; then echo "ok   [unknown argument]"; pass=$((pass+1)); else echo "FAIL [unknown argument]: rc=$rc"; fail=$((fail+1)); fi

echo "permitted-sha-guard-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ] && [ "$pass" -ge 41 ] && echo "permitted-sha-guard-test: ALL PASS"
[ "$fail" -eq 0 ] && [ "$pass" -ge 41 ]
