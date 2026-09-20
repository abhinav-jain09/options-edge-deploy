#!/usr/bin/env bash
# verify-permitted-tree-test.sh — runtime tests for verify-permitted-tree.sh.
#
# These reproduce, against REAL git checkouts, the source-replacement cases Codex demonstrated (a later git pull, a
# fetch + hard reset, a checkout to another commit, a copy/rsync/tar/unzip over the tree, an archive extracted at the
# workspace root on top of the tree, a new untracked file, a modified tracked file). The guard proves HEAD == permitted
# at one moment; verify-permitted-tree.sh must REFUSE every one of these, because the tree an effect would consume is
# no longer the permitted commit's tree. It must also PASS a clean checkout and read-only inspection, and allow
# declared build output.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFY="$HERE/verify-permitted-tree.sh"
pass=0
fail=0

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

# An "origin" with two commits: A (the permitted commit) and a later B on main.
git init -q --bare origin.git
seed="$work/seed"
git init -q "$seed"; git -C "$seed" config user.email t@t; git -C "$seed" config user.name t
mkdir -p "$seed/src/main/resources" "$seed/k8s"
printf 'v1\n' > "$seed/src/app.txt"; printf 'kind: Deployment\n' > "$seed/k8s/d.yaml"; printf 'target/\n*.log\n.m2/\n__pycache__/\n' > "$seed/.gitignore"
# A REAL resource root, so the suite can ask what happens to an ignored path written UNDER source that the
# build packages: `src/main/resources/target/…` is ignored by `target/` and is a Maven resource input.
printf 'app.name=x\n' > "$seed/src/main/resources/app.properties"
git -C "$seed" add -A; git -C "$seed" commit -q -m A
git -C "$seed" remote add origin "$work/origin.git"; git -C "$seed" push -q origin HEAD:main
A="$(git -C "$seed" rev-parse HEAD)"
printf 'v2\n' > "$seed/src/app.txt"; git -C "$seed" commit -qam B; git -C "$seed" push -q origin HEAD:main
B="$(git -C "$seed" rev-parse HEAD)"

fresh() {  # a fresh checkout of the permitted commit A into ./co
  rm -rf co; git clone -q "$work/origin.git" co >/dev/null 2>&1; git -C co checkout -q "$A"
}

check() {  # check <name> <expected rc: 0|1> <PERMITTED_SHA> [verify args…]
  local name="$1" want="$2" psha="$3"; shift 3
  set +e; PERMITTED_SHA="$psha" bash "$VERIFY" --dir co "$@" >/dev/null 2>&1; local rc=$?; set -e
  local got=0; [ "$rc" -eq 0 ] || got=1
  if [ "$got" -eq "$want" ]; then pass=$((pass+1)); else fail=$((fail+1)); echo "FAIL [$name]: rc=$rc want-refuse=$want"; fi
}

# clean checkout of the permitted commit passes; read-only inspection is fine
fresh; check "clean checkout of the permitted commit passes" 0 "$A"
fresh; git -C co rev-parse HEAD >/dev/null; git -C co log -1 >/dev/null; git -C co status >/dev/null
check "read-only git left the tree clean" 0 "$A"

# wrong permitted sha
fresh; check "HEAD is not the permitted commit is refused" 1 "$B"
fresh; check "an empty PERMITTED_SHA is refused" 1 ""

# the source moved to another commit after checkout — every shape Codex demonstrated
fresh; git -C co pull -q --ff-only origin main;                         check "a later git pull to B is refused" 1 "$A"
fresh; git -C co fetch -q origin main; git -C co reset -q --hard FETCH_HEAD; check "a fetch + hard reset to B is refused" 1 "$A"
fresh; git -C co fetch -q origin main; git -C co checkout -q FETCH_HEAD; check "a checkout to the fetched commit is refused" 1 "$A"
fresh; ( cd co && git fetch -q origin main && git merge -q --ff-only FETCH_HEAD ); check "a fast-forward merge to B is refused" 1 "$A"

# files replaced/added over the tree while HEAD stays A
fresh; printf 'x\n' > co/src/app.txt;               check "a modified tracked file is refused" 1 "$A"
fresh; printf 'x\n' > co/src/new.txt;               check "a new untracked file is refused" 1 "$A"
fresh; rm co/src/app.txt;                           check "a deleted tracked file is refused" 1 "$A"
fresh; cp "$seed/src/app.txt" co/src/app.txt;       check "a cp over a tracked file is refused" 1 "$A"
fresh; mkdir -p other && printf 'replaced-by-rsync\n' > other/app.txt && rsync -a --checksum other/ co/src/ >/dev/null; check "an rsync into the tree is refused" 1 "$A"
fresh; ( cd co && tar -xf <(git -C "$seed" archive HEAD src) ) ; check "a tar extraction over the tree is refused" 1 "$A"
fresh; printf 'x\n' > co/src/staged.txt; git -C co add src/staged.txt; check "a staged (added) file is refused" 1 "$A"
fresh; git -C co checkout -q -B feature "$B"; check "switching the checkout to another branch/commit is refused" 1 "$A"

# THE Codex I10 reproduction: an archive of B extracted at the workspace root, with --prefix naming the checkout, on
# top of the guarded checkout — HEAD is untouched, but the tracked files become B's.
fresh
git -C "$seed" archive --prefix=co/ "$B" | tar -x
check "an archive of another commit extracted over the checkout at the root is refused" 1 "$A"

# ignored build output: refused unless declared, allowed when declared
fresh; mkdir -p co/target && printf 'jar\n' > co/target/app.jar
check "ignored build output present but not declared is refused" 1 "$A"
check "ignored build output present and declared with --allow-ignored is allowed" 0 "$A" --allow-ignored target
fresh; printf 'log\n' > co/debug.log
check "an ignored file that is not the declared build dir is refused" 1 "$A" --allow-ignored target
# A declaration is a PATH anchored at --dir, not a name matched wherever it occurs. The real
# workspace-after-build passes when the module output is declared as what it is (`*/target`), and the same
# workspace is refused when only the root output was declared.
fresh
mkdir -p co/mod-a/target/classes co/mod-b/target co/.m2/repository
printf 'a\n' > co/mod-a/target/classes/A.class; printf 'b\n' > co/mod-b/target/app.jar; printf 'r\n' > co/.m2/repository/x.jar
check "module output declared as */target, with .m2, is allowed" 0 "$A" --allow-ignored target --allow-ignored '*/target' --allow-ignored .m2
check "the same workspace with */target declared but .m2 NOT declared is refused" 1 "$A" --allow-ignored target --allow-ignored '*/target'
check "a root-only 'target' declaration does NOT cover a module's target" 1 "$A" --allow-ignored target --allow-ignored .m2
# `*` is ONE whole component: a module nested one level deeper is not covered by `*/target`, and saying so
# is the whole difference between a declared path and a name matched at any depth.
mkdir -p co/grp/mod-c/target; printf 'c\n' > co/grp/mod-c/target/app.jar
check "*/target does not reach a module two levels down" 1 "$A" --allow-ignored target --allow-ignored '*/target' --allow-ignored .m2
check "...which is declared as */*/target" 0 "$A" --allow-ignored target --allow-ignored '*/target' --allow-ignored '*/*/target' --allow-ignored .m2

# THE ANCHORING CASE (Codex I2). `target/` in .gitignore also ignores src/main/resources/target/, which Maven
# packages as a RESOURCE. A file written there after checkout is an added source INPUT, not build output — and
# it is exactly what an allowance read as "this name anywhere" would let through.
fresh; mkdir -p co/target co/src/main/resources/target
printf 'jar\n' > co/target/app.jar
printf 'probe=1\n' > co/src/main/resources/target/probe.properties
check "an ignored file under a SOURCE directory named target is refused by the real declarations" 1 "$A" --allow-ignored target --allow-ignored '*/target' --allow-ignored .m2
check "...and is still refused when every module output is declared" 1 "$A" --allow-ignored target --allow-ignored '*/target' --allow-ignored '*/*/target'
check "...and is allowed only when THAT path itself is declared" 0 "$A" --allow-ignored target --allow-ignored src/main/resources/target
# The same shape one directory deeper, with the build's own output present and declared: still refused.
fresh; mkdir -p co/mod-a/target co/src/main/resources/target
printf 'b\n' > co/mod-a/target/app.jar; printf 'probe=1\n' > co/src/main/resources/target/probe.properties
check "module output declared, source-resource output still refused" 1 "$A" --allow-ignored target --allow-ignored '*/target'
# An ignored bytecode directory is declared by its path too, never by its name.
fresh; mkdir -p co/scripts/__pycache__ co/src/main/resources/__pycache__
printf 'c\n' > co/scripts/__pycache__/m.pyc; printf 'c\n' > co/src/main/resources/__pycache__/m.pyc
check "declaring scripts/__pycache__ does not cover a __pycache__ under resources" 1 "$A" --allow-ignored scripts/__pycache__
fresh; mkdir -p co/scripts/__pycache__; printf 'c\n' > co/scripts/__pycache__/m.pyc
check "declaring scripts/__pycache__ covers exactly that one" 0 "$A" --allow-ignored scripts/__pycache__

# A declaration that could be read as "this name at any depth", or that is not a checkout-relative path at all,
# is a USAGE ERROR (exit 2) — not a permissive default and not a refusal that reads like a dirty tree.
baddecl() {  # baddecl <name> <declaration>
  local name="$1" decl="$2"
  fresh
  set +e; local out; out="$(PERMITTED_SHA="$A" bash "$VERIFY" --dir co --allow-ignored "$decl" 2>&1)"; local rc=$?; set -e
  if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -qF "unusable --allow-ignored declaration"; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); echo "FAIL [$name]: rc=$rc (want 2 with the usage message)"; printf '%s\n' "$out" | sed 's/^/    /'
  fi
}
baddecl "'**' at any depth is a usage error"      '**/target'
baddecl "a bare '**' is a usage error"            '**'
baddecl "'*' glued into a longer name"            'tar*'
baddecl "an absolute path"                        '/target'
baddecl "a '~' path"                              '~/target'
baddecl "a '..' component"                        '../target'
baddecl "a '.' component"                         './target'
baddecl "an empty component"                      'a//target'
baddecl "an empty declaration"                    ''
baddecl "a trailing slash"                        'target/'
baddecl "a space in a name"                       'x y'
baddecl "a character outside the name set"        'tar@get'
baddecl "a '~' component deeper in the path"      'a/~/target'
# A MISSING operand is the same class of fault as an unusable one: exit 2, not the refusal exit.
fresh
set +e; out_missing="$(PERMITTED_SHA="$A" bash "$VERIFY" --dir co --allow-ignored 2>&1)"; rc_missing=$?; set -e
if [ "$rc_missing" -eq 2 ] && printf '%s' "$out_missing" | grep -qF -- "--allow-ignored needs a path"; then
  pass=$((pass+1)); echo "ok   [a missing --allow-ignored operand exits 2]"
else
  fail=$((fail+1)); echo "FAIL [a missing --allow-ignored operand exits 2]: rc=$rc_missing"; printf '%s\n' "$out_missing" | sed 's/^/    /'
fi

# --dir must be the checkout ROOT. git prints the ignored inventory relative to the REPOSITORY root, so a
# subdirectory would measure every declaration from a different anchor than the usage block promises and
# would refuse the workspace's own declared build output (Codex C, MINOR). It is a usage error now.
fresh; mkdir -p co/sub/target; printf 'jar\n' > co/sub/target/app.jar
set +e; out_sub="$(PERMITTED_SHA="$A" bash "$VERIFY" --dir co/sub --allow-ignored target 2>&1)"; rc_sub=$?; set -e
if [ "$rc_sub" -eq 2 ] && printf '%s' "$out_sub" | grep -qF "is not its root"; then
  pass=$((pass+1)); echo "ok   [--dir pointing inside a checkout, not at its root, exits 2]"
else
  fail=$((fail+1)); echo "FAIL [--dir pointing inside a checkout, not at its root, exits 2]: rc=$rc_sub"; printf '%s\n' "$out_sub" | sed 's/^/    /'
fi
check "...and the same workspace from its root, with the path declared, is allowed" 0 "$A" --allow-ignored sub/target

# FAIL-CLOSED: a git query that fails, or partial output then failure, must REFUSE — never be read as a clean/empty
# inventory. A wrapper `git` on PATH forwards to the real git, except for the ONE invocation whose full argument string
# equals FAIL_ARGS: that one prints PARTIAL (if any), records that it fired, and exits FAIL_RC. Each case then asserts
# (a) the inserted fault fired at that exact query, (b) the verify refused, and (c) it refused for THAT query's reason — so a
# case can no longer pass by failing some earlier, unrelated git call.
REALGIT="$(command -v git)"
fakebin="$work/fakebin"; mkdir -p "$fakebin"
cat > "$fakebin/git" <<EOF
#!/usr/bin/env bash
if [ -n "\${FAIL_ARGS:-}" ] && [ "\$*" = "\$FAIL_ARGS" ]; then
  echo fired >> "\$FAIL_MARK"
  [ -n "\${PARTIAL:-}" ] && printf '%s\n' "\$PARTIAL"
  exit "\${FAIL_RC:-128}"
fi
exec "$REALGIT" "\$@"
EOF
chmod +x "$fakebin/git"

checkfail() {  # checkfail <name> <exact git argument string to fail> <expected refusal text> [PARTIAL]
  local name="$1" args="$2" say="$3" partial="${4:-}"
  fresh
  local mark="$work/fired.$RANDOM"; rm -f "$mark"
  set +e
  local out
  out="$(PATH="$fakebin:$PATH" FAIL_ARGS="$args" FAIL_MARK="$mark" PARTIAL="$partial" PERMITTED_SHA="$A" bash "$VERIFY" --dir co 2>&1)"
  local rc=$?
  set -e
  if [ ! -s "$mark" ]; then
    fail=$((fail+1)); echo "FAIL [$name]: the inserted fault never fired — no git call had the arguments '$args'"
  elif [ "$rc" -eq 0 ]; then
    fail=$((fail+1)); echo "FAIL [$name]: rc=0 (fail-open) — a failed git query was treated as clean"
  elif ! printf '%s' "$out" | grep -qF -- "$say"; then
    fail=$((fail+1)); echo "FAIL [$name]: refused, but not for the faulted query's reason (want '$say'):"; printf '%s\n' "$out" | sed 's/^/    /'
  else
    pass=$((pass+1))
  fi
}
checkfail "a failed --ignored enumeration refuses (not treated as empty)" \
  "-C co status --porcelain=v1 --untracked-files=all --ignored" "cannot enumerate the ignored paths"
checkfail "a partial --ignored output then failure refuses" \
  "-C co status --porcelain=v1 --untracked-files=all --ignored" "cannot enumerate the ignored paths" "!! sneaked-in/"
checkfail "a failed working-tree status refuses" \
  "-C co status --porcelain=v1 --untracked-files=all" "cannot read the working-tree status"
checkfail "a git diff error refuses" \
  "-C co diff --quiet HEAD" "git diff HEAD rc=128"
checkfail "a failed HEAD resolution refuses" \
  "-C co rev-parse HEAD" "cannot resolve HEAD"
checkfail "a failed work-tree probe refuses" \
  "-C co rev-parse --is-inside-work-tree" "is not a git checkout"

echo "verify-permitted-tree-test: $pass passed, $fail failed"
if [ "$fail" -eq 0 ] && [ "$pass" -ge 53 ]; then
  echo "verify-permitted-tree-test: ALL PASS"
  exit 0
fi
exit 1
