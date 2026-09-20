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
unset PERMITTED_SHA PERMITTED_BRANCH BRANCH_NAME GIT_BRANCH PERMITTED_SHA_GUARD_VERSION 2>/dev/null || true
# Every guarded job declares the guard version it enforces; the guard refuses under any other.
VERSION="$(bash "$(dirname "$GUARD")/permitted-sha-guard-version.sh" "$GUARD")"
export PERMITTED_SHA_GUARD_VERSION="$VERSION"

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
  if printf '%s' "$want_msg" | grep -qE "is not a git checkout|PERMITTED_SHA_GUARD_VERSION is not set|guard it executes disagree"; then echo "ok   [$name]"; pass=$((pass+1)); return; fi
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
# --- the declared guard version must be THIS guard: an older/newer/absent declaration refuses ------
run "guard version unset"                    1 "PERMITTED_SHA_GUARD_VERSION is not set" "$W" PERMITTED_SHA="$B" PERMITTED_SHA_GUARD_VERSION=
run "guard version mismatch"                 1 "declared guard version and the guard it executes disagree" "$W" PERMITTED_SHA="$B" PERMITTED_SHA_GUARD_VERSION="0000000000000000000000000000000000000000000000000000000000000000"
run "guard version prefix refused"           1 "declared guard version and the guard it executes disagree" "$W" PERMITTED_SHA="$B" PERMITTED_SHA_GUARD_VERSION="${VERSION:0:12}"
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
# --- THE BRANCH HEAD, not merely a commit on the branch (step 2c) ---------------------------------
# B is a real, merged main commit and the permission names exactly it: only the tip condition separates
# this from a permitted run. The rule is that the branch HEAD must BE the permitted commit, so an older
# main commit is refused — with its OWN reason, distinct from "not on origin/main".
git -C "$W" checkout -q "$B"
run "older main commit B while tip is D"     1 "is on origin/main but is NOT its tip" "$W" PERMITTED_SHA="$B"
run "older main commit, its refusal names the tip" 1 "$D" "$W" PERMITTED_SHA="$B"
run "older main commit, --ref main does not excuse it" 1 "is NOT its tip" "$W" PERMITTED_SHA="$B" -- --ref main
run "older main commit, SCM metadata does not excuse it" 1 "is NOT its tip" "$W" PERMITTED_SHA="$B" BRANCH_NAME=main GIT_BRANCH=origin/main
# The two conditions of step 2c stay distinct: a commit that is BEHIND the tip and a commit that was
# never on the branch are different faults and say so, so a log names which one actually fired.
git -C "$W" checkout -q "$C"
run "off-branch C says off-branch, not behind" 1 "is not on origin/main" "$W" PERMITTED_SHA="$C"
git -C "$W" checkout -q "$D"
run "back at the tip: permitted again"       0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$D"
# --- matching SHA but forbidden source ref: the branch restriction stops it on its own -----------
git -C "$W" checkout -q "$C"
run "feature commit C, permitted C"          1 "is not on origin/main"        "$W" PERMITTED_SHA="$C"
run "feature commit C, no SHA: branch first" 1 "is not on origin/main"        "$W"
git -C "$W" checkout -q "$D"
run "main commit but GIT_BRANCH reports feature" 1 "GIT_BRANCH is 'origin/feature'; this job deploys only from 'main'" "$W" PERMITTED_SHA="$D" GIT_BRANCH=origin/feature
run "BRANCH_NAME=feature refused, GIT_BRANCH=origin/main notwithstanding" 1 "BRANCH_NAME is 'feature'" "$W" PERMITTED_SHA="$D" BRANCH_NAME=feature GIT_BRANCH=origin/main
run "BRANCH_NAME=main does NOT mask GIT_BRANCH=origin/feature" 1 "GIT_BRANCH is 'origin/feature'" "$W" PERMITTED_SHA="$D" BRANCH_NAME=main GIT_BRANCH=origin/feature
run "both variables main"                    0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$D" BRANCH_NAME=main GIT_BRANCH=origin/main
run "--branch dev refuses a main checkout"   1 "deploys only from 'dev'"      "$W" PERMITTED_SHA="$D" GIT_BRANCH=origin/main -- --branch dev
# --- the explicitly SELECTED source ref (--ref): a feature ref pointing at a merged commit is still refused
run "--ref feature at a merged commit"       1 "selected source ref is 'feature'" "$W" PERMITTED_SHA="$D" -- --ref feature
run "--ref main"                             0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$D" -- --ref main
run "--ref origin/main is an alias, refused" 1 "selected source ref is 'origin/main'" "$W" PERMITTED_SHA="$D" -- --ref origin/main
run "--ref */main is an alias, refused"      1 "selected source ref is '*/main'" "$W" PERMITTED_SHA="$D" -- --ref '*/main'
run "--ref refs/heads/main is an alias, refused" 1 "selected source ref is 'refs/heads/main'" "$W" PERMITTED_SHA="$D" -- --ref refs/heads/main
run "--ref refs/remotes/origin/main, refused" 1 "selected source ref is 'refs/remotes/origin/main'" "$W" PERMITTED_SHA="$D" -- --ref refs/remotes/origin/main
run "--ref refs/heads/feature"               1 "selected source ref is 'refs/heads/feature'" "$W" PERMITTED_SHA="$D" -- --ref refs/heads/feature
# --- the branch cannot be confirmed: refuse, do not guess ------------------------------------------
git clone -q "$T/origin.git" "$T/noremote"
git -C "$T/noremote" remote set-url origin "$T/does-not-exist.git"
run "origin unreachable"                     1 "could not fetch refs/heads/main" "$T/noremote" PERMITTED_SHA="$D"
# --- the ref must be the BRANCH, fully qualified. `git fetch origin main` resolves through git's ref
#     disambiguation, so a TAG named main answers for a branch that does not exist — and the guard would
#     then report the tag's commit as "the tip of origin/main". Branch and tag namespaces are separate.
cp -R "$T/origin.git" "$T/tagorigin.git"
git -C "$T/tagorigin.git" tag main "$D"
git -C "$T/tagorigin.git" update-ref -d refs/heads/main
git clone -q "$T/origin.git" "$T/tagwork"
git -C "$T/tagwork" checkout -q "$D"
git -C "$T/tagwork" remote set-url origin "$T/tagorigin.git"
run "a TAG named main is not the branch"     1 "could not fetch refs/heads/main" "$T/tagwork" PERMITTED_SHA="$D"
run "...and the permission being right does not rescue it" 1 "verdict=REFUSED" "$T/tagwork" PERMITTED_SHA="$D" -- --ref main
# --- an EMPTY FETCH_HEAD must not fall back to a ref of that name. `git rev-parse FETCH_HEAD` on an
#     empty FETCH_HEAD file does NOT fail: it falls back to ordinary ref lookup, so a local TAG named
#     FETCH_HEAD answers for the branch tip. Codex reproduced the interleaving with real fetches: the
#     guard fetches main successfully, a concurrent failed fetch empties the file, and the guard then
#     reports the TAG's commit as main's tip. The guard fetches into a ref of its own now and reads that
#     ref by its full path, so there is nothing to fall back to.
REALGIT="$(command -v git)"
fhbin="$T/fhbin"; mkdir -p "$fhbin"
cat > "$fhbin/git" <<EOF
#!/usr/bin/env bash
# forward everything; after a successful fetch, empty FETCH_HEAD as a concurrent failed fetch would
dir="."
prev=""
for a in "\$@"; do
  [ "\$prev" = "-C" ] && dir="\$a"
  prev="\$a"
done
"$REALGIT" "\$@"; rc=\$?
case " \$* " in
  *" fetch "*) [ \$rc -eq 0 ] && : > "\$dir/.git/FETCH_HEAD" ;;
esac
exit \$rc
EOF
chmod +x "$fhbin/git"
# origin/main is at D; the checkout and the permission are the OLDER B; a local tag FETCH_HEAD names B.
git -C "$W" checkout -q "$B"
git -C "$W" tag -f FETCH_HEAD "$B" >/dev/null 2>&1
set +e
out="$(cd "$W" && PATH="$fhbin:$PATH" PERMITTED_SHA="$B" bash "$GUARD" 2>&1)"; rc=$?
set -e
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -qF "is NOT its tip"; then
  echo "ok   [an emptied FETCH_HEAD does not fall back to a tag of that name]"; pass=$((pass+1))
else
  echo "FAIL [an emptied FETCH_HEAD does not fall back to a tag of that name]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
fi
# ...and with the checkout AT the tip, the same interleaving still permits: the capture is our own ref.
git -C "$W" checkout -q "$D"
set +e
out="$(cd "$W" && PATH="$fhbin:$PATH" PERMITTED_SHA="$D" bash "$GUARD" 2>&1)"; rc=$?
set -e
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -qF "verdict=PERMITTED"; then
  echo "ok   [the same interleaving still permits the real tip]"; pass=$((pass+1))
else
  echo "FAIL [the same interleaving still permits the real tip]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
fi
git -C "$W" tag -d FETCH_HEAD >/dev/null 2>&1 || true
# the guard leaves no ref of its own behind
if [ -z "$(git -C "$W" for-each-ref --format='%(refname)' 'refs/oe-guard/*')" ]; then
  echo "ok   [the guard deletes the ref it fetched the tip into]"; pass=$((pass+1))
else
  echo "FAIL [the guard deletes the ref it fetched the tip into]"; fail=$((fail+1))
fi

# --- the capture is an EXACT REF PATH, not a revision name. `git rev-parse --verify refs/oe-guard/tip-N`
#     is a revision NAME: with that ref absent it is satisfied by a tag literally called
#     "refs/oe-guard/tip-N", stored at refs/tags/refs/oe-guard/tip-N. Codex reproduced PERMITTED with the
#     older commit reported as main's tip. `show-ref --verify` resolves only the exact ref path.
refbin="$T/refbin"; mkdir -p "$refbin"
cat > "$refbin/git" <<EOF
#!/usr/bin/env bash
# forward everything; after a successful fetch, delete the destination ref the guard just wrote and
# leave a colliding TAG of that same full name behind — the interleaving Codex demonstrated.
dir="."
prev=""
for a in "\$@"; do
  [ "\$prev" = "-C" ] && dir="\$a"
  prev="\$a"
done
"$REALGIT" "\$@"; rc=\$?
case " \$* " in
  *" fetch "*)
    if [ \$rc -eq 0 ]; then
      for r in \$("$REALGIT" -C "\$dir" for-each-ref --format='%(refname)' 'refs/oe-guard/*'); do
        "$REALGIT" -C "\$dir" update-ref -d "\$r" >/dev/null 2>&1
        "$REALGIT" -C "\$dir" update-ref "refs/tags/\$r" "\$COLLIDING_COMMIT" >/dev/null 2>&1
      done
    fi
    ;;
esac
exit \$rc
EOF
chmod +x "$refbin/git"
git -C "$W" checkout -q "$B"
set +e
out="$(cd "$W" && PATH="$refbin:$PATH" COLLIDING_COMMIT="$B" PERMITTED_SHA="$B" bash "$GUARD" 2>&1)"; rc=$?
set -e
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -qF "did not resolve to a commit id"; then
  echo "ok   [a tag named like the capture ref does not answer for it]"; pass=$((pass+1))
else
  echo "FAIL [a tag named like the capture ref does not answer for it]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
fi
git -C "$W" for-each-ref --format='%(refname)' 'refs/tags/refs/oe-guard/*' | while read -r r; do git -C "$W" update-ref -d "$r" >/dev/null 2>&1; done
git -C "$W" checkout -q "$D"
# --- the capture ref is deleted on EVERY exit path, including a refusal, not only on success.
git -C "$W" checkout -q "$C"
set +e
(cd "$W" && PERMITTED_SHA="$C" bash "$GUARD" >/dev/null 2>&1)
set -e
git -C "$W" checkout -q "$D"
if [ -z "$(git -C "$W" for-each-ref --format='%(refname)' 'refs/oe-guard/*')" ]; then
  echo "ok   [a REFUSED run leaves no capture ref behind]"; pass=$((pass+1))
else
  echo "FAIL [a REFUSED run leaves no capture ref behind]: $(git -C "$W" for-each-ref --format='%(refname)' 'refs/oe-guard/*')"; fail=$((fail+1))
fi
# --- and a failed FETCH, which refuses before the capture, also leaves none.
set +e
(cd "$T/noremote" && PERMITTED_SHA="$D" bash "$GUARD" >/dev/null 2>&1)
set -e
if [ -z "$(git -C "$T/noremote" for-each-ref --format='%(refname)' 'refs/oe-guard/*')" ]; then
  echo "ok   [a run that refuses at the fetch leaves no capture ref behind]"; pass=$((pass+1))
else
  echo "FAIL [a run that refuses at the fetch leaves no capture ref behind]"; fail=$((fail+1))
fi
# --- the ref name carries more than the pid, so two runs sharing a checkout are unlikely to collide.
if grep -q 'tipref="refs/oe-guard/tip-\$\$-\$(od -An -N8' "$GUARD"; then
  echo "ok   [the capture ref name is the pid plus random bytes]"; pass=$((pass+1))
else
  echo "FAIL [the capture ref name is the pid plus random bytes]"; fail=$((fail+1))
fi

# --- AN INTERRUPTION IS A REFUSAL. `$?` inside a SIGNAL trap is the last command's status, so a signal
#     delivered just after a successful command used to leave the guard exiting 0 with the permission
#     never compared — and a caller reading exit 0 as "permitted" would deploy under no permission. The
#     wrapper below delivers a chosen signal to the guard at a chosen point in its run; every point and
#     every signal must give a NONZERO exit and no verdict=PERMITTED line.
sigbin="$T/sigbin"; mkdir -p "$sigbin"
cat > "$sigbin/git" <<EOF
#!/usr/bin/env bash
# forward everything; when the argv matches KILL_AT, signal the guard shell that invoked us
"$REALGIT" "\$@"; rc=\$?
case " \$* " in
  *" \$KILL_AT "*) kill -"\$KILL_SIG" "\$PPID" ;;
esac
exit \$rc
EOF
chmod +x "$sigbin/git"
git -C "$W" checkout -q "$D"
for sig in TERM INT HUP QUIT; do
  for at in "rev-parse" "fetch" "show-ref"; do
    set +e
    out="$(cd "$W" && PATH="$sigbin:$PATH" KILL_SIG="$sig" KILL_AT="$at" PERMITTED_SHA="0000000000000000000000000000000000000000" bash "$GUARD" 2>&1)"; rc=$?
    set -e
    if [ "$rc" -ne 0 ] && ! printf '%s' "$out" | grep -qF "verdict=PERMITTED"; then
      echo "ok   [SIG$sig at the $at call: nonzero exit, no PERMITTED verdict]"; pass=$((pass+1))
    else
      echo "FAIL [SIG$sig at the $at call: nonzero exit, no PERMITTED verdict]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
    fi
  done
done
# ...and the same with a CORRECT permission: an interrupted run must still not report PERMITTED.
set +e
out="$(cd "$W" && PATH="$sigbin:$PATH" KILL_SIG=TERM KILL_AT=fetch PERMITTED_SHA="$D" bash "$GUARD" 2>&1)"; rc=$?
set -e
if [ "$rc" -ne 0 ] && ! printf '%s' "$out" | grep -qF "verdict=PERMITTED"; then
  echo "ok   [an interrupted run with the RIGHT permission still refuses]"; pass=$((pass+1))
else
  echo "FAIL [an interrupted run with the RIGHT permission still refuses]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
fi

# --- ONE fetched snapshot answers both predicates. FETCH_HEAD is a mutable file; a concurrent fetch in
#     the same checkout between the two questions used to make the guard refuse a correct commit as
#     off-branch while printing that same commit as the tip. The wrapper below reproduces exactly that:
#     the guard's read of FETCH_HEAD returns the real tip, and FETCH_HEAD is then rewritten to the
#     off-branch commit C before the containment test runs.
racebin="$T/racebin"; mkdir -p "$racebin"
cat > "$racebin/git" <<EOF
#!/usr/bin/env bash
# forward everything; after the guard resolves FETCH_HEAD, rewrite it as a concurrent fetch would
dir="."
prev=""
for a in "\$@"; do
  [ "\$prev" = "-C" ] && dir="\$a"
  prev="\$a"
done
out=\$("$REALGIT" "\$@"); rc=\$?
case " \$* " in
  *" rev-parse "*FETCH_HEAD*)
    [ \$rc -eq 0 ] && printf '%s\n' "\$OTHER_COMMIT" > "\$dir/.git/FETCH_HEAD"
    ;;
esac
[ -n "\$out" ] && printf '%s\n' "\$out"
exit \$rc
EOF
chmod +x "$racebin/git"
git -C "$W" checkout -q "$D"
set +e
out="$(cd "$W" && PATH="$racebin:$PATH" OTHER_COMMIT="$C" PERMITTED_SHA="$D" bash "$GUARD" 2>&1)"; rc=$?
set -e
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -qF "verdict=PERMITTED"; then
  echo "ok   [FETCH_HEAD rewritten between the two predicates: one captured tip answers both]"; pass=$((pass+1))
else
  echo "FAIL [FETCH_HEAD rewritten between the two predicates: one captured tip answers both]: rc=$rc"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
fi
# --- not a checkout at all ------------------------------------------------------------------------
mkdir -p "$T/plain"
set +e; out="$(cd "$T/plain" && PERMITTED_SHA="$D" bash "$GUARD" 2>&1)"; rc=$?; set -e
if [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -qF "is not a git checkout"; then echo "ok   [not a git checkout]"; pass=$((pass+1)); else echo "FAIL [not a git checkout]: rc=$rc"; echo "$out"; fail=$((fail+1)); fi
# --- not a WORKING checkout: a .git metadata directory and a bare repository both print `false` ----
run ".git metadata dir refused"              1 "is not a git checkout"        "$W" PERMITTED_SHA="$D" -- --dir .git
run "bare repository refused"                1 "is not a git checkout"        "$T" PERMITTED_SHA="$D" -- --dir origin.git
# --- nested application checkout (--dir): its own SHA; the job's GIT_BRANCH does not describe it --
git clone -q "$T/origin.git" "$W/nested-src"
git -C "$W/nested-src" checkout -q "$B"    # origin/main is at D by now; B is still on it
# A nested source is judged by the same rule: on the branch AND its tip. B is merged main work and the
# permission names exactly it, and it is still refused, because the branch has moved past it.
run "nested at an older main commit, permitted B" 1 "is on origin/main but is NOT its tip" "$W" PERMITTED_SHA="$B" GIT_BRANCH=origin/whatever -- --dir nested-src
run "nested older commit, --ref main does not excuse it" 1 "is NOT its tip" "$W" PERMITTED_SHA="$B" -- --dir nested-src --ref main
git -C "$W/nested-src" fetch -q origin feature:feature && git -C "$W/nested-src" checkout -q "$C"
run "nested at C (feature), permitted C"     1 "is not on origin/main"        "$W" PERMITTED_SHA="$C" -- --dir nested-src
git -C "$W/nested-src" fetch -q origin main && git -C "$W/nested-src" checkout -q "$D"
run "nested at the tip, permitted D (job ref irrelevant)" 0 "verdict=PERMITTED" "$W" PERMITTED_SHA="$D" GIT_BRANCH=origin/whatever -- --dir nested-src
run "nested at the tip, permitted B"         1 "is not the permitted commit"  "$W" PERMITTED_SHA="$B" -- --dir nested-src
run "nested, no SHA"                         1 "PERMITTED_SHA is not set"     "$W" -- --dir nested-src
run "nested --ref feature at a merged commit" 1 "selected source ref is 'feature'" "$W" PERMITTED_SHA="$D" -- --dir nested-src --ref feature
run "nested --ref main"                      0 "verdict=PERMITTED"            "$W" PERMITTED_SHA="$D" -- --dir nested-src --ref main
# --- usage errors are refusals too ---------------------------------------------------------------
set +e; out="$(cd "$W" && PERMITTED_SHA="$D" bash "$GUARD" --bogus 2>&1)"; rc=$?; set -e
if [ "$rc" -eq 2 ]; then echo "ok   [unknown argument]"; pass=$((pass+1)); else echo "FAIL [unknown argument]: rc=$rc"; fail=$((fail+1)); fi

echo "permitted-sha-guard-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ] && [ "$pass" -ge 77 ] && echo "permitted-sha-guard-test: ALL PASS"
[ "$fail" -eq 0 ] && [ "$pass" -ge 77 ]
