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
mkdir -p "$seed/src" "$seed/k8s"
printf 'v1\n' > "$seed/src/app.txt"; printf 'kind: Deployment\n' > "$seed/k8s/d.yaml"; printf 'target/\n*.log\n.m2/\n' > "$seed/.gitignore"
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
# a --allow-ignored directory name covers that directory ANYWHERE (module/target, .m2/repository), so the real
# workspace-after-build (byproducts in nested build dirs) passes when those names are declared.
fresh
mkdir -p co/mod-a/target/classes co/mod-b/target co/.m2/repository
printf 'a\n' > co/mod-a/target/classes/A.class; printf 'b\n' > co/mod-b/target/app.jar; printf 'r\n' > co/.m2/repository/x.jar
check "nested module target/ dirs and .m2/ are covered by their declared names" 0 "$A" --allow-ignored target --allow-ignored .m2
check "the same workspace with target declared but .m2 NOT declared is refused" 1 "$A" --allow-ignored target

# FAIL-CLOSED: a git query that fails, or partial output then failure, must REFUSE — never be read as a clean/empty
# inventory. A wrapper `git` on PATH forwards to the real git, except for the ONE invocation whose full argument string
# equals FAIL_ARGS: that one prints PARTIAL (if any), records that it fired, and exits FAIL_RC. Each case then asserts
# (a) the injection fired at that exact query, (b) the verify refused, and (c) it refused for THAT query's reason — so a
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
    fail=$((fail+1)); echo "FAIL [$name]: the injection never fired — no git call had the arguments '$args'"
  elif [ "$rc" -eq 0 ]; then
    fail=$((fail+1)); echo "FAIL [$name]: rc=0 (fail-open) — a failed git query was treated as clean"
  elif ! printf '%s' "$out" | grep -qF -- "$say"; then
    fail=$((fail+1)); echo "FAIL [$name]: refused, but not for the injected query's reason (want '$say'):"; printf '%s\n' "$out" | sed 's/^/    /'
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
if [ "$fail" -eq 0 ] && [ "$pass" -ge 28 ]; then
  echo "verify-permitted-tree-test: ALL PASS"
  exit 0
fi
exit 1
