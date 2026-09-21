#!/usr/bin/env bash
# Suite for scripts/jenkins/effect-shim/_shim.sh — shared byte-for-byte across the four repositories.
#
# Every case here is a CONTROL: it states one claim the shim makes and fails if that claim stops being
# true. The claims this suite is willing to make are exactly the ones it can observe — the shim's own
# header lists what it does not cover, and nothing here pretends otherwise.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
SHIMDIR="$HERE/effect-shim"
pass=0; fail=0
ok()  { pass=$((pass+1)); printf 'ok   [%s]\n' "$1"; }
# A PROTECTION CONTROL must go red when its protection is removed, and effect-shim-sweep.sh enforces
# that for every case reported with `ok`. These three kinds cannot, by construction, and say so here
# rather than being explained away in the sweep's output:
#   POSITIVE   -- asserts the shim does NOT refuse when it should not. Removing a protection cannot make
#                 it fail; ADDING a wrong refusal does, which is what it guards.
#   STRUCTURAL -- asserts a property of the files (a symlink, an absolute shebang) rather than a branch.
#   LIMIT      -- asserts the ABSENCE of a protection. It goes red if protection is ADDED, never removed.
ok_positive()   { pass=$((pass+1)); printf 'ok   [%s]\n' "$1"; printf 'kind POSITIVE   %s\n' "$1" >> "${KIND_LOG:-/dev/null}"; }
ok_structural() { pass=$((pass+1)); printf 'ok   [%s]\n' "$1"; printf 'kind STRUCTURAL %s\n' "$1" >> "${KIND_LOG:-/dev/null}"; }
ok_limit()      { pass=$((pass+1)); printf 'ok   [%s]\n' "$1"; printf 'kind LIMIT      %s\n' "$1" >> "${KIND_LOG:-/dev/null}"; }
bad() { fail=$((fail+1)); printf 'FAIL [%s]\n%s\n' "$1" "$(sed 's/^/    /' <<<"${2:-}")"; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
REALBIN="$T/realbin"; mkdir -p "$REALBIN"
# the "real" tools: they record that they ran, so a case can tell REFUSED from EXECUTED
for t in mvn docker rsync scp helm ansible-playbook kubectl; do
  printf '#!/bin/sh\nprintf "%s %%s\\n" "$*" >> "$RAN_LOG"\nexit 0\n' "$t" > "$REALBIN/$t"
  chmod +x "$REALBIN/$t"
done

# a checkout whose HEAD is a real commit
W="$T/co"; mkdir -p "$W"
git init -q "$W"; git -C "$W" config user.email t@t; git -C "$W" config user.name t
mkdir -p "$W/scripts/jenkins"
cp -R "$HERE/effect-shim" "$W/scripts/jenkins/effect-shim"
cp "$HERE/verify-permitted-tree.sh" "$W/scripts/jenkins/verify-permitted-tree.sh"
echo 'src' > "$W/file.txt"
printf 'target/\n' > "$W/.gitignore"
git -C "$W" add -A >/dev/null; git -C "$W" commit -qm init
SHA="$(git -C "$W" rev-parse HEAD)"
CO_SHIM="$W/scripts/jenkins/effect-shim"

run_shim() {   # run_shim <tool> [env assignments...] ; sets RC and OUT, resets RAN_LOG
  local tool="$1"; shift
  : > "$T/ran"
  set +e
  OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" "$@" "$tool" --version 2>&1)"
  RC=$?
  set -e
}
ran() { [ -s "$T/ran" ]; }

base=(OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW=)

# --- 1. the happy path: a clean permitted tree runs the real binary -----------------------------------
run_shim mvn "${base[@]}"
if [ "$RC" -eq 0 ] && ran && printf '%s' "$OUT" | grep -q "verified"; then
  ok_positive "a clean checkout at the permitted commit runs the real binary"
else
  bad "a clean checkout at the permitted commit runs the real binary" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi

# --- 2. THE CLAIM THIS EXISTS FOR: a modified tree refuses, and the binary does NOT run ---------------
echo 'changed after the guard' >> "$W/file.txt"
for tool in mvn docker rsync scp helm ansible-playbook kubectl; do
  run_shim "$tool" "${base[@]}"
  if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "verdict=REFUSED"; then
    ok "$tool: a tracked file changed after the guard refuses, and $tool never runs"
  else
    bad "$tool: a tracked file changed after the guard refuses, and $tool never runs" "rc=$RC ran=$(cat "$T/ran")
$OUT"
  fi
done
git -C "$W" checkout -q -- file.txt

# --- 3. an UNTRACKED file is a change too, unless the job declared it ---------------------------------
echo 'dropped in' > "$W/extra.txt"
run_shim mvn "${base[@]}"
if [ "$RC" -eq 3 ] && ! ran; then
  ok "an undeclared untracked file refuses"
else
  bad "an undeclared untracked file refuses" "rc=$RC
$OUT"
fi
# …and DECLARING it does not exempt it: the verifier's declarations cover IGNORED paths (its rule 4), so
# a file that is merely untracked is a change to the tree whatever the job says about it. Asserted here
# rather than assumed, because "I declared it" is exactly the kind of belief that goes untested.
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW="--allow-ignored extra.txt"
if [ "$RC" -eq 3 ] && ! ran; then
  ok "declaring a path that is untracked-but-not-ignored does NOT exempt it"
else
  bad "declaring a path that is untracked-but-not-ignored does NOT exempt it" "rc=$RC
$OUT"
fi
rm -f "$W/extra.txt"

# --- 4. build output under an IGNORED directory the job owns ------------------------------------------
mkdir -p "$W/target"; echo 'jar' > "$W/target/app.jar"
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW="--allow-ignored target"
if [ "$RC" -eq 0 ] && ran; then
  ok_positive "a declared build-output directory does not refuse (the dirty-tree case a job legitimately owns)"
else
  bad "a declared build-output directory does not refuse (the dirty-tree case a job legitimately owns)" "rc=$RC
$OUT"
fi
run_shim mvn "${base[@]}"
if [ "$RC" -eq 3 ] && ! ran; then
  ok "the SAME output UNdeclared refuses (the declaration is the job's statement, not a default)"
else
  bad "the SAME output UNdeclared refuses (the declaration is the job's statement, not a default)" "rc=$RC
$OUT"
fi
rm -rf "$W/target"

# --- 5. configuration is required; an unset variable is a refusal, never a default --------------------
run_shim mvn OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW=
[ "$RC" -eq 3 ] && ! ran && ok "OE_SHIM_DIR unset refuses" || bad "OE_SHIM_DIR unset refuses" "rc=$RC
$OUT"
run_shim mvn OE_SHIM_DIR=. OE_SHIM_ALLOW=
[ "$RC" -eq 3 ] && ! ran && ok "OE_SHIM_SHA unset refuses" || bad "OE_SHIM_SHA unset refuses" "rc=$RC
$OUT"
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA="$SHA"
[ "$RC" -eq 3 ] && ! ran && ok "OE_SHIM_ALLOW unset refuses (declaring nothing must be said, not assumed)" \
  || bad "OE_SHIM_ALLOW unset refuses (declaring nothing must be said, not assumed)" "rc=$RC
$OUT"
# The MESSAGE is asserted, not just the refusal: an empty SHA also makes the VERIFIER refuse, so a case
# that checked only the status stayed green with the shim's own emptiness check deleted. Pin the clause
# that is supposed to be doing the work.
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA= OE_SHIM_ALLOW=
if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "OE_SHIM_SHA is empty"; then
  ok "an EMPTY OE_SHIM_SHA refuses in the shim itself (no permission is not a permission)"
else
  bad "an EMPTY OE_SHIM_SHA refuses in the shim itself (no permission is not a permission)" "rc=$RC
$OUT"
fi

# --- 6. the wrong permission refuses even though the tree is clean ------------------------------------
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA=0000000000000000000000000000000000000000 OE_SHIM_ALLOW=
if [ "$RC" -eq 3 ] && ! ran; then
  ok "a clean tree at the WRONG permitted commit refuses"
else
  bad "a clean tree at the WRONG permitted commit refuses" "rc=$RC
$OUT"
fi

# --- 7. no exec loop: the shim never resolves itself as the real binary -------------------------------
: > "$T/ran"
set +e
# The PATH must carry what the VERIFICATION needs (bash, git) and NOT the real tool, or the case never
# reaches binary resolution and tests something else -- which is what it used to do.
OUT="$(cd "$W" && env PATH="$CO_SHIM:/usr/bin:/bin" RAN_LOG="$T/ran" OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" \
      OE_SHIM_ALLOW= "$CO_SHIM/mvn" --version 2>&1)"; RC=$?
set -e
if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "not on PATH outside this shim directory"; then
  ok "with only the shim on PATH, resolution finds nothing and it refuses (no exec loop)"
else
  bad "with only the shim on PATH, resolution finds nothing and it refuses (no exec loop)" "rc=$RC
$OUT"
fi

# --- 8. every name in the directory is the same implementation ----------------------------------------
missing=""
for t in mvn docker rsync scp helm ansible-playbook kubectl; do
  [ -e "$SHIMDIR/$t" ] || missing="$missing $t"
  [ "$(readlink "$SHIMDIR/$t" 2>/dev/null)" = "_shim.sh" ] || missing="$missing $t(not-a-link)"
done
[ -z "$missing" ] && ok_structural "every shimmed tool name is a symlink to the one implementation" \
  || bad "every shimmed tool name is a symlink to the one implementation" "$missing"

# --- 9. the shim can start under a PATH that holds nothing -------------------------------------------
# A `#!/usr/bin/env bash` shim dies with "env: bash: No such file or directory" under a minimal PATH and
# records nothing -- it refuses nothing and permits nothing, which is the worst of the three. The
# interpreter path must be absolute, and this is the case that says so.
shebang="$(head -1 "$SHIMDIR/_shim.sh")"
case "$shebang" in
  "#!/"*) ok_structural "the shim names its interpreter by absolute path ($shebang)" ;;
  *)      bad "the shim names its interpreter by absolute path ($shebang)" "$shebang" ;;
esac
: > "$T/ran"
set +e
OUT="$(cd "$W" && env -i PATH="$CO_SHIM:$REALBIN" RAN_LOG="$T/ran" OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" \
      OE_SHIM_ALLOW= HOME="$T" "$CO_SHIM/mvn" --version 2>&1)"; RC=$?
set -e
# It STARTS (its own refusal line proves that) and it FAILS CLOSED: with no `git` reachable it cannot
# verify, so it refuses and the real binary does not run. Starting is the claim here; the shim is not
# expected to verify without the tools the verifier itself needs.
if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "effect-shim(mvn)"; then
  ok "under a PATH with no git the shim still STARTS and refuses (it never silently permits)"
else
  bad "under a PATH with no git the shim still STARTS and refuses (it never silently permits)" "rc=$RC
$OUT"
fi


# --- 10. DECLARATIONS ARE NOT PATHNAME-EXPANDED --------------------------------------------------------
# `*/target` is a valid declaration (the verifier's grammar allows `*` as a component). An unquoted
# expansion turned it into the directories that happened to exist -- `--allow-ignored a/target b/target`
# -- and the verifier refused the third argument, so a clean, correctly-declared tree was REFUSED.
mkdir -p "$W/a/target" "$W/b/target"
echo 'jar' > "$W/a/target/a.jar"; echo 'jar' > "$W/b/target/b.jar"
printf 'target/\n*/target/\n' > "$W/.gitignore"
git -C "$W" add -A >/dev/null 2>&1; git -C "$W" commit -qm ignore-wildcards >/dev/null 2>&1
SHA="$(git -C "$W" rev-parse HEAD)"
run_shim mvn OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW="--allow-ignored */target"
if [ "$RC" -eq 0 ] && ran; then
  ok "a WILDCARD declaration reaches the verifier intact (no pathname expansion)"
else
  bad "a WILDCARD declaration reaches the verifier intact (no pathname expansion)" "rc=$RC
$OUT"
fi
rm -rf "$W/a" "$W/b"

# --- 11. DESCENDANTS ARE INTERCEPTED TOO, AND THE CHAIN STARTS INSIDE THE SHIM --------------------
# The real binary inherits the ORIGINAL PATH, shim first, so a tool that starts another tool by name is
# verified again. This is the Maven-plugin case: mvn passes verification, the plugin changes a tracked
# file, and the plugin's `kubectl` must refuse.
#
# THE FIRST VERSION OF THIS CASE WAS DECORATIVE. It invoked "$REALBIN/parent" directly with the tree
# already dirty, so nothing passed through the shim's dispatch and the PATH that dispatch hands on was
# never exercised -- restoring `export PATH="$stripped"` before the exec left the whole suite at
# 26 passed, ALL PASS. The chain now STARTS with a shimmed tool invoked BY NAME on a CLEAN tree: the
# shim verifies and execs the real `mvn`, that process dirties the tree and calls `middle`, and
# `middle` calls `kubectl` BY NAME. Only the PATH the shim handed to its child can resolve that call.
cat > "$REALBIN/mvn" <<PEOF
#!/bin/sh
# stands in for a build that runs a plugin: change a tracked file, then invoke another tool by name
printf 'mvn %s\n' "\$*" >> "\$RAN_LOG"
echo 'changed by the running build' >> "$W/file.txt"
exec "$REALBIN/middle"
PEOF
cat > "$REALBIN/middle" <<'MEOF'
#!/bin/sh
kubectl grandchild-effect
MEOF
chmod +x "$REALBIN/mvn" "$REALBIN/middle"
git -C "$W" checkout -q -- file.txt
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn -B test 2>&1)"; RC=$?
set -e
if grep -q '^mvn ' "$T/ran" && ! grep -q '^kubectl' "$T/ran" && printf '%s' "$OUT" | grep -q "verdict=REFUSED"; then
  ok "a GRANDCHILD reached through the shim's own dispatch is verified too (mvn ran; its kubectl refused)"
else
  bad "a GRANDCHILD reached through the shim's own dispatch is verified too (mvn ran; its kubectl refused)" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
# the real mvn stub is restored to the plain recorder the earlier cases expect
printf '#!/bin/sh\nprintf "mvn %%s\\n" "$*" >> "$RAN_LOG"\nexit 0\n' > "$REALBIN/mvn"
chmod +x "$REALBIN/mvn"
git -C "$W" checkout -q -- file.txt

# --- 11b. A RELATIVE PATH ENTRY, AND A CHILD THAT CHANGES DIRECTORY ------------------------------
# Review reproduced this and it was real: with `PATH=scripts/jenkins/effect-shim:...` -- a relative
# entry, which is how a Jenkinsfile writes it without thinking -- a verified child that does `cd /tmp`
# and then runs `kubectl` looks the name up against its NEW cwd, finds no shim there, and reaches the
# real binary UNVERIFIED. The case below is that exact chain. The shim now rewrites the element it was
# found through to this directory's absolute path before handing PATH on, so the lookup survives any
# cwd the child chooses. Delete those lines and this case goes red while every other case stays green.
cat > "$REALBIN/mvn" <<PEOF
#!/bin/sh
printf 'mvn %s\n' "\$*" >> "\$RAN_LOG"
echo 'changed by the running build' >> "$W/file.txt"
exec "$REALBIN/middle-cd"
PEOF
cat > "$REALBIN/middle-cd" <<'MEOF'
#!/bin/sh
# the thing that makes a relative PATH entry stop working: somewhere else entirely
cd /tmp || exit 1
kubectl grandchild-after-cd
MEOF
chmod +x "$REALBIN/mvn" "$REALBIN/middle-cd"
git -C "$W" checkout -q -- file.txt
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="scripts/jenkins/effect-shim:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn -B test 2>&1)"; RC=$?
set -e
if grep -q '^mvn ' "$T/ran" && ! grep -q '^kubectl' "$T/ran" && printf '%s' "$OUT" | grep -q "verdict=REFUSED"; then
  ok "a RELATIVE shim entry still intercepts a grandchild that changed directory"
else
  bad "a RELATIVE shim entry still intercepts a grandchild that changed directory" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
printf '#!/bin/sh\nprintf "mvn %%s\\n" "$*" >> "$RAN_LOG"\nexit 0\n' > "$REALBIN/mvn"
chmod +x "$REALBIN/mvn"
git -C "$W" checkout -q -- file.txt

# --- 11g. AN EMPTY PATH ELEMENT IS THE CURRENT DIRECTORY, AND IT IS A REAL ELEMENT ---------------
# `PATH=:$REALBIN:/usr/bin` puts the current directory first. Splitting on ":" with word splitting
# drops that element, so the rewrite lost it and a descendant that changed directory resolved the
# real binary unverified -- review reproduced exactly that, rc=0 with both mvn and kubectl recorded.
cat > "$REALBIN/mvn" <<PEOF
#!/bin/sh
printf 'mvn %s\n' "\$*" >> "\$RAN_LOG"
echo 'changed by the running build' >> "$W/file.txt"
exec "$REALBIN/middle-cd"
PEOF
chmod +x "$REALBIN/mvn"
git -C "$W" checkout -q -- file.txt
: > "$T/ran"
set +e
OUT="$(cd "$CO_SHIM" && env PATH=":$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR="$W" OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn -B test 2>&1)"; RC=$?
set -e
if grep -q '^mvn ' "$T/ran" && ! grep -q '^kubectl' "$T/ran" && printf '%s' "$OUT" | grep -q "verdict=REFUSED"; then
  ok "an EMPTY PATH element still intercepts a grandchild that changed directory"
else
  bad "an EMPTY PATH element still intercepts a grandchild that changed directory" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
printf '#!/bin/sh\nprintf "mvn %%s\\n" "$*" >> "$RAN_LOG"\nexit 0\n' > "$REALBIN/mvn"
chmod +x "$REALBIN/mvn"
git -C "$W" checkout -q -- file.txt

# --- 11h. AN EMPTY ELEMENT SURVIVES THE REAL-BINARY LOOKUP TOO ----------------------------------
# The rewrite that hands PATH to the child was fixed for empty elements; the STRIPPING pass that
# resolves the real binary still dropped them, so `PATH=<shim>::$REALBIN:...` ran $REALBIN/mvn while
# a tracked ./mvn sat earlier in the surviving PATH. The shim must run the binary the job's PATH
# names, not a different one.
# The local binary lives in a cwd OUTSIDE the checkout, so the tree stays clean and the only thing
# under test is which element the lookup honours.
mkdir -p "$T/cwdbin"
printf '#!/bin/sh\nprintf "LOCAL-MVN %%s\\n" "$*" >> "$RAN_LOG"\nexit 0\n' > "$T/cwdbin/mvn"
chmod +x "$T/cwdbin/mvn"
: > "$T/ran"
set +e
OUT="$(cd "$T/cwdbin" && env PATH="$CO_SHIM::$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR="$W" OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= \
      mvn -B test 2>&1)"; RC=$?
set -e
if grep -q '^LOCAL-MVN' "$T/ran"; then
  ok_positive "an EMPTY PATH element is preserved when resolving the real binary"
else
  bad "an EMPTY PATH element is preserved when resolving the real binary" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
rm -f "$T/cwdbin/mvn" 2>/dev/null || true

# --- 11c. A LOGIN SHELL THAT REBUILDS PATH IS A LIMIT, NOT A PROTECTION -------------------------
# `bash -lc` re-reads the profile, and a profile that ASSIGNS PATH (rather than prepending to it)
# drops the shim entry entirely. The fixture is hermetic: a throwaway HOME whose .bash_profile
# reassigns PATH, and a REAL login shell -- `bash -c` would not have read a profile at all, so the
# case would have been named for something it never exercised. Nothing inside a wrapper can survive its own removal from PATH, so
# this is recorded as a LIMIT with a test rather than described in a sentence. If it ever goes green
# the shim became stronger than its header claims and the header is what needs changing.
: > "$T/ran"
set +e
mkdir -p "$T/loginhome"
printf 'PATH="%s:/usr/bin:/bin"\nexport PATH\n' "$REALBIN" > "$T/loginhome/.bash_profile"
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= HOME="$T/loginhome" \
      bash -lc 'kubectl reset-path-effect' 2>&1)"; RC=$?
set -e
if grep -q '^kubectl' "$T/ran"; then
  ok_limit "DOCUMENTED LIMIT: a shell that REASSIGNS PATH drops the shim and the tool runs unverified"
else
  bad "DOCUMENTED LIMIT: a shell that REASSIGNS PATH drops the shim and the tool runs unverified" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi

# --- 11d. A VERIFIED EFFECT, THEN DRIFT: BOTH FACTS ARE TRUE AT ONCE ----------------------------
# Review found the Jenkinsfile saying "the effects above ran UNVERIFIED" whenever the END inspection
# failed. That is false for the ordinary case: a wrapper verifies AT THE MOMENT IT RUNS, and a drift
# discovered later does not reach back and un-verify it. The two facts coexist, and this case pins
# both together -- the effect ran verified, AND the end inspection still refuses -- so the wording
# can never drift back to the stronger claim without a red test.
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= kubectl apply -f manifest 2>&1)"; RC=$?
set -e
effect_ran=false
grep -q '^kubectl' "$T/ran" && printf '%s' "$OUT" | grep -q "verified" && effect_ran=true
# now the drift, AFTER the effect: an entry the digest does not account for
: > "$CO_SHIM/npm"
chmod +x "$CO_SHIM/npm"
set +e
END_OUT="$(bash "$HERE/effect-shim-integrity.sh" --dir "$W" --when end 2>&1)"; END_RC=$?
set -e
rm -f "$CO_SHIM/npm" 2>/dev/null || true
if $effect_ran && [ "$END_RC" -ne 0 ]; then
  ok "a VERIFIED effect followed by drift: the effect ran verified AND the end inspection still refuses"
else
  bad "a VERIFIED effect followed by drift: the effect ran verified AND the end inspection still refuses" \
      "effect_ran=$effect_ran rc=$RC end_rc=$END_RC ran=$(cat "$T/ran")
$OUT
--- end inspection ---
$END_OUT"
fi

# --- 11e. THE END REFUSAL'S WORDING IS PART OF THE CONTRACT --------------------------------------
# Twice now the temporal overclaim came back through TEXT rather than through logic: first in the
# Jenkinsfile, then in this shared payload after the Jenkinsfile was fixed. Behaviour was correct
# both times; the sentence was wrong. So the sentence is asserted. The end refusal must NOT say an
# effect was unverified, and must say what it can actually establish.
: > "$CO_SHIM/npm"; chmod +x "$CO_SHIM/npm"
set +e
WORD_OUT="$(bash "$HERE/effect-shim-integrity.sh" --dir "$W" --when end 2>&1)"; WORD_RC=$?
set -e
rm -f "$CO_SHIM/npm" 2>/dev/null || true
if [ "$WORD_RC" -ne 0 ] \
   && printf '%s' "$WORD_OUT" | grep -q "cannot attest COVERAGE FOR ALL" \
   && ! printf '%s' "$WORD_OUT" | grep -qE "they were not covered|the (effects|steps) above ran UNVERIFIED|unverified step"; then
  ok "the end refusal claims only what it can establish, and never that a verified effect was unverified"
else
  bad "the end refusal claims only what it can establish, and never that a verified effect was unverified" \
      "rc=$WORD_RC
$WORD_OUT"
fi

# --- 11f. A MISSING WRAPPER CANNOT SAY WHAT AN INVOCATION RESOLVED TO ---------------------------
# The refusal for an absent wrapper used to say "every invocation of $t in this stage resolved to the
# real binary unverified". It cannot know that: PATH may have held another copy of the shim ahead of
# this directory, and that copy may have verified the call. Claiming otherwise is a claim about
# wrapper absence and external copies, which is exactly the scope this mechanism disclaims.
mv "$CO_SHIM/kubectl" "$T/kubectl.parked"
set +e
MISS_OUT="$(bash "$HERE/effect-shim-integrity.sh" --dir "$W" --when end 2>&1)"; MISS_RC=$?
set -e
mv "$T/kubectl.parked" "$CO_SHIM/kubectl"
if [ "$MISS_RC" -ne 0 ] \
   && printf '%s' "$MISS_OUT" | grep -q "cannot be attested" \
   && ! printf '%s' "$MISS_OUT" | grep -qE "resolved to the real binary unverified|every invocation"; then
  ok "a MISSING wrapper refuses without claiming what any invocation resolved to"
else
  bad "a MISSING wrapper refuses without claiming what any invocation resolved to" "rc=$MISS_RC
$MISS_OUT"
fi

# --- 12-14. THE DOCUMENTED LIMITS, PINNED ------------------------------------------------------------
# These three cases assert what the shim does NOT do. They exist because a limit that is only described
# is a sentence someone deletes; a limit with a test is a limit. If one of them ever goes red, the shim
# became stronger than its header claims and the header is what needs updating.
#
# 12. A WRAPPER CANNOT DETECT ITS OWN ABSENCE.
rm -f "$CO_SHIM/kubectl"
echo 'changed after the verification' >> "$W/file.txt"
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= kubectl apply 2>&1)"; RC=$?
set -e
if [ "$RC" -eq 0 ] && ran; then
  ok_limit "DOCUMENTED LIMIT: a DELETED wrapper falls through to the real binary, unverified (nothing here answers this)"
else
  bad "DOCUMENTED LIMIT: a DELETED wrapper falls through to the real binary, unverified (nothing here answers this)" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
ln -s _shim.sh "$CO_SHIM/kubectl"
git -C "$W" checkout -q -- file.txt

# 13. MUTABLE CODE CANNOT ESTABLISH ITS OWN INTEGRITY.
cp "$CO_SHIM/_shim.sh" "$T/_shim.orig"
python3 - "$CO_SHIM/_shim.sh" <<'PEOF'
import sys, re
p = sys.argv[1]
s = open(p).read()
s = s.replace('if ! PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier"', 'if false && ! PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier"', 1)
open(p, "w").write(s)
PEOF
echo 'changed after the verification' >> "$W/file.txt"
run_shim kubectl OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW=
if [ "$RC" -eq 0 ] && ran && printf '%s' "$OUT" | grep -q "verified"; then
  ok_limit "DOCUMENTED LIMIT: an EDITED shim prints 'verified' and runs the tool on a dirty tree"
else
  bad "DOCUMENTED LIMIT: an EDITED shim prints 'verified' and runs the tool on a dirty tree" "rc=$RC ran=$(cat "$T/ran")
$OUT"
fi
cp "$T/_shim.orig" "$CO_SHIM/_shim.sh"
git -C "$W" checkout -q -- file.txt

# 14. A DIFFERENT WRAPPER VERSION OUTSIDE THE CHECKOUT IS NOT VERSION-COMPARED.
OUTSIDE="$T/outside/scripts/jenkins/effect-shim"; mkdir -p "$OUTSIDE"
cp "$HERE/verify-permitted-tree.sh" "$T/outside/scripts/jenkins/verify-permitted-tree.sh"
sed 's/^say "verified/say "OUTSIDE COPY verified/' "$CO_SHIM/_shim.sh" > "$OUTSIDE/_shim.sh"
chmod +x "$OUTSIDE/_shim.sh"; ln -sf _shim.sh "$OUTSIDE/mvn"
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="$OUTSIDE:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn --version 2>&1)"; RC=$?
set -e
if [ "$RC" -eq 0 ] && ran && printf '%s' "$OUT" | grep -q "OUTSIDE COPY verified"; then
  ok_limit "DOCUMENTED LIMIT: a DIFFERENT wrapper copy outside the checkout runs and is never version-compared"
else
  bad "DOCUMENTED LIMIT: a DIFFERENT wrapper copy outside the checkout runs and is never version-compared" "rc=$RC
$OUT"
fi


# --- 15. THE INTEGRITY CHECK'S OWN CONTROLS ----------------------------------------------------------
# Each tamper shape asserts the SPECIFIC message, so each check inside effect-shim-integrity.sh is
# pinned individually. Checking only the status hides a layered refusal: with the executable check
# deleted, `chmod 644` was still refused by the digest/symlink layers, and a status-only case would have
# reported that protection as present when it was not.
# A COPY of the checker inside the temp checkout: it reads the expected digest from its own
# directory, so a case that removes that file must remove the copy's, never the repository's.
cp "$HERE/effect-shim-integrity.sh" "$W/scripts/jenkins/effect-shim-integrity.sh"
cp "$HERE/effect-shim-digest.txt" "$W/scripts/jenkins/effect-shim-digest.txt"
git -C "$W" add -A >/dev/null 2>&1; git -C "$W" commit -qm "the checker, inside the checkout" >/dev/null 2>&1
SHA="$(git -C "$W" rev-parse HEAD)"
INTEG="$W/scripts/jenkins/effect-shim-integrity.sh"

# THE ATTRIBUTION ANCHOR. An untouched shim directory passes both inspections. This is a POSITIVE
# control -- removing a protection cannot make it fail -- and effect-shim-sweep.sh uses exactly that:
# if a mutation turns a POSITIVE control red, the mutant is broken rather than neutralised, and nothing
# that went red under it is attributable to the protection the sweep meant to remove.
set +e
p_start="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; p_start_rc=$?
p_end="$(bash "$INTEG" --dir "$W" --when end 2>&1)"; p_end_rc=$?
set -e
if [ "$p_start_rc" -eq 0 ] && [ "$p_end_rc" -eq 0 ]; then
  ok_positive "integrity: an UNTOUCHED shim directory passes both inspections"
else
  bad "integrity: an UNTOUCHED shim directory passes both inspections" "start_rc=$p_start_rc end_rc=$p_end_rc
$p_start
--- end ---
$p_end"
fi

# A refusal removed from an argument parser can turn its loop into an endless one: `*) usage … ;;` with
# the refusal taken out never shifts. The sweep needs to see a RED CASE, not a hung suite, so the usage
# cases below run under a wall clock and a timeout is a failure like any other.
limited() {   # limited <seconds> <cmd...> ; sets OUT and RC
  local secs="$1"; shift
  local f="$T/limited.out"; : > "$f"
  set +e
  ( "$@" >"$f" 2>&1 & cpid=$!
    ( sleep "$secs"; kill -9 "$cpid" 2>/dev/null ) >/dev/null 2>&1 & wpid=$!
    wait "$cpid"; rc=$?
    kill "$wpid" 2>/dev/null
    exit "$rc" ) >/dev/null 2>&1
  RC=$?
  OUT="$(cat "$f")"
  set -e
}
usage_case() {  # usage_case <label> <expected message fragment> <args...>
  local label="$1" want="$2"; shift 2
  limited 10 bash "$INTEG" "$@"
  if [ "$RC" -eq 2 ] && printf '%s' "$OUT" | grep -qF -- "$want"; then
    ok "integrity: $label"
  else
    bad "integrity: $label" "rc=$RC
$OUT"
  fi
}
integ_case() {  # integ_case <label> <expected message fragment> <tamper> <restore>
  local label="$1" want="$2" tamper="$3" restore="$4" out rc
  eval "$tamper"
  set +e
  out="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; rc=$?
  set -e
  eval "$restore"
  if [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -qF "$want"; then
    ok "integrity: $label"
  else
    bad "integrity: $label" "rc=$rc
$out"
  fi
}
integ_case "a deleted wrapper is named"        "the wrapper 'kubectl' is missing" \
           'rm -f "$CO_SHIM/kubectl"'          'ln -sf _shim.sh "$CO_SHIM/kubectl"'
integ_case "a NON-EXECUTABLE wrapper is named" "is not executable" \
           'chmod 644 "$CO_SHIM/_shim.sh"'     'chmod 755 "$CO_SHIM/_shim.sh"'
integ_case "an edited _shim.sh is named"       "its bytes differ from the digest recorded beside it" \
           'printf "# tampered\n" >> "$CO_SHIM/_shim.sh"' 'cp "$HERE/effect-shim/_shim.sh" "$CO_SHIM/_shim.sh"'
integ_case "a planted executable is named"     "unexpected entry 'npm'" \
           'printf "#!/bin/sh\nexit 0\n" > "$CO_SHIM/npm"; chmod +x "$CO_SHIM/npm"' 'rm -f "$CO_SHIM/npm"'
integ_case "a HIDDEN planted entry is named"   "unexpected entry '.kubectl'" \
           'printf "#!/bin/sh\nexit 0\n" > "$CO_SHIM/.kubectl"' 'rm -f "$CO_SHIM/.kubectl"'
integ_case "a wrapper replaced by a real file" "is not the expected symlink" \
           'rm -f "$CO_SHIM/scp"; printf "#!/bin/sh\nexit 0\n" > "$CO_SHIM/scp"; chmod +x "$CO_SHIM/scp"' \
           'rm -f "$CO_SHIM/scp"; ln -sf _shim.sh "$CO_SHIM/scp"'

# --- 15b. THE LIMIT THE SIX CASES ABOVE DO NOT REACH: THE CHECKER AND THE DIGEST ARE IN THE WORKSPACE -
# Every tamper above is an ACCIDENT -- something changed the shim directory and did not change anything
# else. That is what effect-shim-integrity.sh catches, and it is all it catches. THE JOB OWNS ITS OWN
# PROCESS: _shim.sh, effect-shim-digest.txt and effect-shim-integrity.sh all live in the checkout, so a
# step that changes two of them together passes both inspections with an unverified wrapper in place.
#
# These two cases assert the GREEN result that produces. They exist because the headers used to call
# this DETECTION, and a limit that is only described is a sentence someone deletes. If either goes red,
# the mechanism became stronger than its headers claim and the headers are what need updating -- do NOT
# "fix" the case. The fix for the underlying limit is not available here: see _shim.sh for the four
# alternatives considered and why each fails while the job supplies its own process.
#
# 15b-i. AN EDITED _shim.sh WITH A RE-RECORDED DIGEST.
cp "$CO_SHIM/_shim.sh" "$T/_shim.pre-coordinated"
cp "$W/scripts/jenkins/effect-shim-digest.txt" "$T/digest.pre-coordinated"
python3 - "$CO_SHIM/_shim.sh" <<'PEOF'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace('if ! PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier"',
              'if false && ! PERMITTED_SHA="$OE_SHIM_SHA" bash "$verifier"', 1)
open(p, "w").write(s)
PEOF
if command -v sha256sum >/dev/null 2>&1; then sha256sum "$CO_SHIM/_shim.sh" | cut -d' ' -f1 > "$W/scripts/jenkins/effect-shim-digest.txt"
else shasum -a 256 "$CO_SHIM/_shim.sh" | cut -d' ' -f1 > "$W/scripts/jenkins/effect-shim-digest.txt"; fi
echo 'changed after the verification' >> "$W/file.txt"
: > "$T/ran"
set +e
co_start="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; co_start_rc=$?
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= kubectl apply 2>&1)"; RC=$?
co_end="$(bash "$INTEG" --dir "$W" --when end 2>&1)"; co_end_rc=$?
set -e
if [ "$co_start_rc" -eq 0 ] && [ "$co_end_rc" -eq 0 ] && [ "$RC" -eq 0 ] && ran; then
  ok_limit "DOCUMENTED LIMIT: an edited _shim.sh WITH A RE-RECORDED DIGEST runs kubectl on a dirty tree and BOTH inspections report ok"
else
  bad "DOCUMENTED LIMIT: an edited _shim.sh WITH A RE-RECORDED DIGEST runs kubectl on a dirty tree and BOTH inspections report ok" "rc=$RC ran=$(cat "$T/ran") start_rc=$co_start_rc end_rc=$co_end_rc
$co_start
--- end ---
$co_end
--- shim ---
$OUT"
fi
cp "$T/_shim.pre-coordinated" "$CO_SHIM/_shim.sh"
cp "$T/digest.pre-coordinated" "$W/scripts/jenkins/effect-shim-digest.txt"
git -C "$W" checkout -q -- file.txt

# 15b-ii. AN EDITED CHECKER. The integrity script the stage runs is the one IN the checkout; a step that
# edits it chooses what "ok" means. Here the tamper is the plainest one the six cases above DO catch --
# a deleted wrapper -- and it is reported ok anyway, which is the point: the checker is not a witness.
cp "$INTEG" "$T/integ.pre-coordinated"
python3 - "$INTEG" <<'PEOF'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace('[ -d "$shim" ] || refuse', 'exit 0\n[ -d "$shim" ] || refuse', 1)
open(p, "w").write(s)
PEOF
rm -f "$CO_SHIM/kubectl"
set +e
ed_start="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; ed_start_rc=$?
ed_end="$(bash "$INTEG" --dir "$W" --when end 2>&1)"; ed_end_rc=$?
set -e
ln -s _shim.sh "$CO_SHIM/kubectl"
cp "$T/integ.pre-coordinated" "$INTEG"
if [ "$ed_start_rc" -eq 0 ] && [ "$ed_end_rc" -eq 0 ]; then
  ok_limit "DOCUMENTED LIMIT: an edited effect-shim-integrity.sh reports ok with a wrapper DELETED (the checker is in the workspace the job owns)"
else
  bad "DOCUMENTED LIMIT: an edited effect-shim-integrity.sh reports ok with a wrapper DELETED (the checker is in the workspace the job owns)" "start_rc=$ed_start_rc end_rc=$ed_end_rc
$ed_start
--- end ---
$ed_end"
fi

# THE ARGUMENT PARSER'S OWN REFUSALS. Four branches that refuse a malformed invocation, none of which
# had a case: the sweep's inventory was line-shaped regexes and a refusal spelled `*) usage "…" ;;`
# inside a case branch was not in any of them, so it was never mutated and never missed. Each is
# asserted by its OWN MESSAGE, because every one of them exits 2 and a status-only case cannot tell
# which branch spoke.
usage_case "--dir with no value is refused"  "--dir needs a path"       --dir
usage_case "--when with no value is refused" "--when needs start|end"   --dir "$W" --when
usage_case "an unknown argument is refused"  "unknown argument '--bogus'" --dir "$W" --when start --bogus
usage_case "--when must be start or end"     "--when must be start or end" --dir "$W" --when middle

# …and the END inspection says what the START inspection must not: that the effects already ran.
set +e
end_out="$(rm -f "$CO_SHIM/kubectl"; bash "$INTEG" --dir "$W" --when end 2>&1)"; end_rc=$?
start_out="$(bash "$INTEG" --dir "$W" --when start 2>&1)"
ln -sf _shim.sh "$CO_SHIM/kubectl"
set -e
if [ "$end_rc" -eq 3 ] && printf '%s' "$end_out" | grep -q "ALREADY RAN" && ! printf '%s' "$start_out" | grep -q "ALREADY RAN"; then
  ok "integrity: the END inspection says the effects already ran; the START inspection does not"
else
  bad "integrity: the END inspection says the effects already ran; the START inspection does not" "end_rc=$end_rc
$end_out
--- start ---
$start_out"
fi


# --- 16. BEHAVIOURAL CONTROLS: what the shim HANDS ON, not just whether it refuses -------------------
# Round 3 (Codex) removed four protections that no case covered, and the suite stayed at 33 passed.
# Every one of them is about what happens on the SUCCESS path, which the earlier cases only checked far
# enough to see that the tool ran at all:
#   * `exec "$real" "$@"` without "$@" -- the tool runs with NO ARGUMENTS. `mvn -B test` becomes `mvn`.
#   * `exec` replaced by a call followed by `exit 0` -- the tool's FAILURE becomes success.
#   * the signal traps removed -- an interrupted run dies with no refusal and no status 3.
#   * the empty-OE_SHIM_DIR check removed -- the refusal comes from the verifier's argument parsing
#     instead, so the shim's own clause is never exercised (the empty-SHA shape again).
argbin="$T/argbin"; mkdir -p "$argbin"
cat > "$argbin/mvn" <<'AEOF'
#!/bin/sh
# record each argument on its own line, %q-style, so a lost or merged argument is visible
for a in "$@"; do printf 'ARG %s\n' "$a" >> "$ARG_LOG"; done
printf 'ARGC %s\n' "$#" >> "$ARG_LOG"
exit "${FAKE_EXIT:-0}"
AEOF
chmod +x "$argbin/mvn"

: > "$T/args"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$argbin:/usr/bin:/bin" ARG_LOG="$T/args" \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn -B -f ./pom.xml "a b" test 2>&1)"; RC=$?
set -e
want="ARG -B
ARG -f
ARG ./pom.xml
ARG a b
ARG test
ARGC 5"
if [ "$RC" -eq 0 ] && [ "$(cat "$T/args")" = "$want" ]; then
  ok "every argument reaches the real tool unchanged, including one containing a space"
else
  bad "every argument reaches the real tool unchanged, including one containing a space" "rc=$RC
$(cat "$T/args")
--- expected ---
$want"
fi

: > "$T/args"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$argbin:/usr/bin:/bin" ARG_LOG="$T/args" FAKE_EXIT=7 \
      OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= mvn -B test 2>&1)"; RC=$?
set -e
if [ "$RC" -eq 7 ]; then
  ok "the real tool's FAILURE status reaches the caller (the shim execs, it does not wrap)"
else
  bad "the real tool's FAILURE status reaches the caller (the shim execs, it does not wrap)" "rc=$RC (expected 7)
$OUT"
fi

# A SIGNAL DURING THE VERIFICATION is a refusal with status 3 and a message, not a silent death. The
# fake git below signals the shim while it is verifying, which is the only window the shim owns.
sigbin="$T/sigbin"; mkdir -p "$sigbin"
REALGIT="$(command -v git)"
# EXACTLY ONE SIGNAL, TO EXACTLY ONE PROCESS. Two things are controlled here rather than left to
# chance. The marker file makes the fake git fire ONCE: when a case removes a trap, the shim dies
# mid-verification, the verifier it started is orphaned, and its next `git status` fired again. The
# PIDFILE IS PER-ITERATION for the same reason -- an orphan carries the PREVIOUS iteration's
# SHIM_SIGNAL and SHIM_PIDFILE in its environment, so with one shared pidfile it delivered a stale
# SIGHUP to the NEXT iteration's shim: removing only the HUP trap reddened the QUIT case too, with
# `rc=129`, and the harness named a protection that was present.
cat > "$sigbin/git" <<GEOF
#!/bin/sh
"$REALGIT" "\$@"; rc=\$?
case " \$* " in
  *" status "*)
    if [ ! -e "\$SHIM_PIDFILE.sent" ]; then
      : > "\$SHIM_PIDFILE.sent"
      kill -"\${SHIM_SIGNAL:-TERM}" "\$(cat "\$SHIM_PIDFILE")"
    fi
    ;;
esac
exit \$rc
GEOF
chmod +x "$sigbin/git"
# THE LAUNCHER OWNS THE SIGNAL DISPOSITION, because the shell that started this suite does not have
# to. A signal IGNORED or BLOCKED at exec() is INHERITED THROUGH exec: bash then CANNOT trap it --
# `trap \'on_signal HUP\' HUP` returns 0 and installs nothing, `trap -p HUP` still prints
# `trap -- \'\' SIGHUP` -- and a blocked signal is never delivered at all. Either way the shim verifies
# and runs the tool. SIGHUP is the one that arrives that way in real life: `nohup`, launchd, and
# agents that start their shells detached all ignore it. That is exactly how this case passed here
# and failed on a reviewer\'s machine with `rc=0 ran=mvn -B test`, and the harness could not tell the
# difference between a shim with no trap and a signal that never arrived.
#
# So the launcher RESETS the four signals to SIG_DFL and UNBLOCKS them immediately before exec, and
# records what it inherited, so a failure names the cause instead of reading as a broken shim.
# `$$` is not used: the PID is written by the process that then execs the shim, so what the fake git
# signals is the shim itself and never a command-substitution subshell standing in for it.
cat > "$T/launch-shim.py" <<'LEOF'
import os, signal, sys
SIGS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT)
blocked, inherited = signal.pthread_sigmask(signal.SIG_BLOCK, []), []
for s in SIGS:
    state = [w for w, yes in (("blocked", s in blocked),
                              ("ignored", signal.getsignal(s) is signal.SIG_IGN)) if yes]
    if state:
        inherited.append(signal.Signals(s).name + ":" + "+".join(state))
    signal.signal(s, signal.SIG_DFL)
signal.pthread_sigmask(signal.SIG_UNBLOCK, SIGS)
open(os.environ["SHIM_INHERITED"], "w").write(",".join(inherited) if inherited else "none")
open(os.environ["SHIM_PIDFILE"], "w").write(str(os.getpid()))
os.execv(sys.argv[1], sys.argv[1:])
LEOF
: > "$T/ran"
set +e
# EVERY trapped signal, not just TERM: the sweep found that removing the INT and HUP traps left the
# suite green, because one signal stood in for four.
for sig in TERM INT HUP QUIT; do
  : > "$T/ran"
  set +e
  OUT="$(cd "$W" && env PATH="$sigbin:$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" SHIM_PIDFILE="$T/shimpid.$sig" \
        SHIM_SIGNAL="$sig" OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= \
        SHIM_INHERITED="$T/inherited" python3 "$T/launch-shim.py" "$CO_SHIM/mvn" -B test 2>&1)"; RC=$?
  set -e
  if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "interrupted by SIG$sig"; then
    # THE LABEL IS A CONSTANT. It is the key the sweep matches its inventory against, so a disposition
    # interpolated into it reads as a different case every run and every signal case scores NEVER RED.
    printf 'note [SIG%s launcher inherited: %s]\n' "$sig" "$(cat "$T/inherited" 2>/dev/null)"
    ok "SIG$sig during the verification refuses with status 3 and says so (the tool never runs)"
  else
    bad "SIG$sig during the verification refuses with status 3 and says so (the tool never runs)" "rc=$RC ran=$(cat "$T/ran") inherited-disposition=$(cat "$T/inherited" 2>/dev/null)
$OUT"
  fi
done

# THE HARNESS\'S OWN CONTROL. The four cases above prove a refusal; this one proves the LAUNCHER is
# not what produced it. Same path, same launcher, no signal sent: the tool must run and the status
# must be its own. Without this, a launcher that killed everything it started would read as four
# passing signal controls.
: > "$T/ran"
set +e
OUT="$(cd "$W" && env PATH="$CO_SHIM:$REALBIN:/usr/bin:/bin" RAN_LOG="$T/ran" SHIM_PIDFILE="$T/shimpid.none" \
      SHIM_INHERITED="$T/inherited" OE_SHIM_DIR=. OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW= \
      python3 "$T/launch-shim.py" "$CO_SHIM/mvn" -B test 2>&1)"; RC=$?
set -e
if [ "$RC" -eq 0 ] && ran; then
  ok_positive "through the same launcher, with NO signal sent, the tool runs (the launcher is not the refusal)"
else
  bad "through the same launcher, with NO signal sent, the tool runs (the launcher is not the refusal)" "rc=$RC ran=$(cat "$T/ran") inherited-disposition=$(cat "$T/inherited" 2>/dev/null)
$OUT"
fi

# An EMPTY OE_SHIM_DIR must be refused BY THE SHIM, naming its own clause -- the verifier would refuse it
# too, for its own reason, and a status-only case cannot tell those apart.
run_shim mvn OE_SHIM_DIR= OE_SHIM_SHA="$SHA" OE_SHIM_ALLOW=
if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "OE_SHIM_DIR is empty"; then
  ok "an EMPTY OE_SHIM_DIR refuses in the shim itself"
else
  bad "an EMPTY OE_SHIM_DIR refuses in the shim itself" "rc=$RC
$OUT"
fi


# --- 17. THE PROTECTIONS THE IMPLEMENTATION-DERIVED SWEEP FOUND UNCOVERED ----------------------------
# Each of these guards existed with no case behind it; the sweep removed them one at a time and the
# suite stayed green. That is the same defect as a decorative case, one level up.
mv "$CO_SHIM/../verify-permitted-tree.sh" "$T/verifier-moved"
run_shim mvn "${base[@]}"
if [ "$RC" -eq 3 ] && ! ran && printf '%s' "$OUT" | grep -q "the verifier is missing"; then
  ok "a MISSING verifier refuses (the shim never assumes a verification it could not run)"
else
  bad "a MISSING verifier refuses (the shim never assumes a verification it could not run)" "rc=$RC
$OUT"
fi
mv "$T/verifier-moved" "$CO_SHIM/../verify-permitted-tree.sh"

set +e
out="$(bash "$INTEG" --when start 2>&1)"; rc=$?
set -e
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q -- "--dir is required"; then
  ok "integrity: --dir is required (a usage error is 2, never a silent pass)"
else
  bad "integrity: --dir is required (a usage error is 2, never a silent pass)" "rc=$rc
$out"
fi

mv "$CO_SHIM" "$T/shim-moved"
set +e
out="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; rc=$?
set -e
mv "$T/shim-moved" "$CO_SHIM"
if [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "the shim directory is missing"; then
  ok "integrity: a MISSING shim directory refuses (coverage by this checkout's shim cannot be attested)"
else
  bad "integrity: a MISSING shim directory refuses (coverage by this checkout's shim cannot be attested)" "rc=$rc
$out"
fi

mv "$W/scripts/jenkins/effect-shim-digest.txt" "$T/digest-moved"
set +e
out="$(bash "$INTEG" --dir "$W" --when start 2>&1)"; rc=$?
set -e
mv "$T/digest-moved" "$W/scripts/jenkins/effect-shim-digest.txt"
if [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q "expected digest file is missing"; then
  ok "integrity: a MISSING digest file refuses (an absent expectation is not a met one)"
else
  bad "integrity: a MISSING digest file refuses (an absent expectation is not a met one)" "rc=$rc
$out"
fi

printf 'effect-shim-test: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ] && echo "effect-shim-test: ALL PASS" || exit 1
