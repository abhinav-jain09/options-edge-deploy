#!/usr/bin/env bash
# require_reviewed_source decides whether a source tree may wipe production. Driven over REAL git
# repositories built in a temp directory — a fake "git" stub would prove nothing about the one thing
# this guard does, which is compare a working tree against origin/main.
#
# The case that matters is the one that happened (2026-10-13 05:47): a checkout whose
# scripts/kafka/apply-topics.sh had UNCOMMITTED edits, 89 lines behind main, wiped production and took
# the pre-#1173 failure path. A branch-name check would have passed that tree if the edits had been made
# on main itself, which is why this compares CONTENT per file.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
G="$HERE/reviewed-source-guard.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -r "$G" ] || { echo "FAIL: $G missing" >&2; exit 1; }
export GIT_CONFIG_GLOBAL="$WORK/gitconfig" GIT_CONFIG_NOSYSTEM=1
printf '[user]\n\tname = t\n\temail = t@t\n[init]\n\tdefaultBranch = main\n' > "$WORK/gitconfig"

FILES=(scripts/kafka/apply-topics.sh scripts/ops/prod-clean-slate.sh)

# An "upstream" repo plus a clone whose origin/main points at it — the same shape the real checkout has.
origin="$WORK/origin.git"; git init -q --bare "$origin"
seed="$WORK/seed"; mkdir -p "$seed"
( cd "$seed" && git init -q && git remote add origin "$origin"
  mkdir -p scripts/kafka scripts/ops
  printf 'reviewed apply\n' > scripts/kafka/apply-topics.sh
  printf 'reviewed wrapper\n' > scripts/ops/prod-clean-slate.sh
  git add -A && git commit -qm seed && git push -q origin main ) >/dev/null 2>&1

fresh_clone() { # -> path to a clone that matches origin/main
  local d="$WORK/c$RANDOM"; git clone -q "$origin" "$d" >/dev/null 2>&1; printf '%s\n' "$d"
}
check() { # <dir> -> RC, OUT
  OUT="$(bash -c '. "$1"; require_reviewed_source "$2" "${@:3}"' _ "$G" "$1" "${FILES[@]}" 2>&1)"; RC=$?
}

echo "1. a tree identical to origin/main passes"
c="$(fresh_clone)"; check "$c"
[ "$RC" -eq 0 ] && ok "returns 0" || bad "rc=$RC out=[$OUT]"
printf '%s' "$OUT" | grep -q 'source verified' && ok "and says what it verified against" || bad "out=[$OUT]"

echo "2. THE 05:47 CASE: an uncommitted edit to a file the run uses"
c="$(fresh_clone)"; printf 'local hack\n' >> "$c/scripts/kafka/apply-topics.sh"; check "$c"
[ "$RC" -ne 0 ] && ok "refuses" || bad "returned 0 with an uncommitted edit"
printf '%s' "$OUT" | grep -q 'UNCOMMITTED edits' && ok "and names the kind of difference" || bad "out=[$OUT]"
printf '%s' "$OUT" | grep -q 'apply-topics.sh' && ok "and the file" || bad "out=[$OUT]"
# ...and on main itself, which a branch-name check would have let through.
c="$(fresh_clone)"; ( cd "$c" && git checkout -q main ) ; printf 'local hack\n' >> "$c/scripts/kafka/apply-topics.sh"; check "$c"
[ "$RC" -ne 0 ] && ok "refuses even when the branch IS main (the branch name proves nothing)" || bad "returned 0 on dirty main"

echo "3. committed on another branch but not merged"
c="$(fresh_clone)"
( cd "$c" && git checkout -q -b wip && printf 'wip change\n' >> scripts/ops/prod-clean-slate.sh && git commit -qam wip ) >/dev/null 2>&1
check "$c"
[ "$RC" -ne 0 ] && ok "refuses" || bad "returned 0 for an unmerged commit"
printf '%s' "$OUT" | grep -q 'committed, but not in origin/main' && ok "and says it is committed, not dirty" || bad "out=[$OUT]"

echo "4. a branch that is NOT main but whose files match origin/main is allowed"
c="$(fresh_clone)"
( cd "$c" && git checkout -q -b unrelated && printf 'x\n' > unrelated.txt && git add -A && git commit -qm unrelated ) >/dev/null 2>&1
check "$c"
[ "$RC" -eq 0 ] && ok "passes — unrelated work in the tree is not punished" || bad "rc=$RC out=[$OUT]"

echo "5. the refusals that are not about content"
c="$(fresh_clone)"; rm -f "$c/scripts/kafka/apply-topics.sh"; check "$c"
[ "$RC" -ne 0 ] && ok "a missing file refuses" || bad "returned 0 with a file missing"
c="$(fresh_clone)"; printf 'new\n' > "$c/scripts/ops/extra-helper.sh"
OUT="$(bash -c '. "$1"; require_reviewed_source "$2" scripts/ops/extra-helper.sh' _ "$G" "$c" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "an untracked file refuses" || bad "returned 0 for an untracked file"
printf '%s' "$OUT" | grep -q 'untracked' && ok "and says so" || bad "out=[$OUT]"
d="$WORK/notarepo"; mkdir -p "$d/scripts/kafka" "$d/scripts/ops"; : > "$d/scripts/kafka/apply-topics.sh"; : > "$d/scripts/ops/prod-clean-slate.sh"
check "$d"
[ "$RC" -ne 0 ] && ok "a non-git directory refuses" || bad "returned 0 for a non-git tree"
check "$WORK/does-not-exist"
[ "$RC" -ne 0 ] && ok "a missing directory refuses" || bad "returned 0 for a missing directory"
OUT="$(bash -c '. "$1"; require_reviewed_source' _ "$G" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "no arguments refuses" || bad "returned 0 with no arguments"

echo "6. a FETCH that fails is a refusal, not a pass on a stale ref"
c="$(fresh_clone)"; ( cd "$c" && git remote set-url origin "$WORK/gone.git" ) >/dev/null 2>&1
check "$c"
[ "$RC" -ne 0 ] && ok "refuses when origin/main cannot be fetched" || bad "returned 0 on a stale remote ref"
printf '%s' "$OUT" | grep -q 'could not fetch' && ok "and says why" || bad "out=[$OUT]"

echo "7. MUTATION: without the per-file diff the 05:47 tree passes"
mkdir -p "$WORK/mut"; cp "$G" "$WORK/mut/g.sh"
python3 - "$WORK/mut/g.sh" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
old = '    if ! git -C "$src" diff --quiet origin/main -- "$f" 2>/dev/null; then'
assert old in s, "the mutation did not apply -- the content check has moved"
open(p, "w").write(s.replace(old, '    if false; then', 1))
PY
if [ $? -eq 0 ]; then
  c="$(fresh_clone)"; printf 'local hack\n' >> "$c/scripts/kafka/apply-topics.sh"
  MOUT="$(bash -c '. "$1"; require_reviewed_source "$2" "${@:3}"' _ "$WORK/mut/g.sh" "$c" "${FILES[@]}" 2>&1)"; MRC=$?
  [ "$MRC" -eq 0 ] && ok "the mutant ACCEPTS the dirty tree (case 2 is load-bearing)" \
    || bad "the mutation changed nothing: rc=$MRC out=[$MOUT]"
else bad "the content-check mutation did not apply"; fi

echo "8. the clean slate calls it before anything mutates, and only for a wipe"
code=$(sed 's/#.*//' "$HERE/prod-clean-slate.sh")
printf '%s' "$code" | grep -q 'require_reviewed_source "\$DEPLOY_SRC" "\${DEPLOY_FILES\[@\]}"' \
  && ok "it passes the SAME file list the run uses" || bad "the guard is not called with DEPLOY_FILES"
lineof() { printf '%s\n' "$code" | grep -n -- "$1" | head -1 | cut -d: -f1; }
lg="$(lineof 'require_reviewed_source')"; lp="$(lineof 'pause_mirrors ||')"; lw="$(lineof 'offhours-clean-slate.sh DRY_RUN')"
if [ -n "$lg" ] && [ -n "$lp" ] && [ "$lg" -lt "$lp" ] && [ "$lg" -lt "$lw" ]; then
  ok "and before the pause ($lg < $lp) and the wipe ($lg < $lw)"
else bad "the guard does not precede the pause/wipe (guard=$lg pause=$lp wipe=$lw)"; fi
printf '%s' "$code" | grep -B2 'require_reviewed_source "\$DEPLOY_SRC"' | grep -q '\[ "\$MODE" = wipe \]' \
  && ok "and only for MODE=wipe, so a dry run stays usable on any tree" || bad "the guard is not gated on MODE=wipe"
printf '%s' "$code" | grep -q 'DEPLOY_FILES+=("scripts/kafka/\$_s")' \
  && ok "and apply-topics' siblings are derived into that list" || bad "the siblings are not derived into DEPLOY_FILES"

echo
if [ "$fails" -eq 0 ]; then echo "=== reviewed-source-guard: OK ==="; exit 0; fi
echo "=== reviewed-source-guard: $fails problem(s) ===" >&2; exit 1
