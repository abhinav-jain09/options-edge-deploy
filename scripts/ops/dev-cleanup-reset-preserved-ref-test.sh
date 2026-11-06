#!/usr/bin/env bash
# The dev wipe's RESET-PRESERVED keep-list must come from the REVIEWED COMMIT (TOPICS_ENV_REF's ref,
# i.e. origin/main) and not from the deploy checkout on the dev host.
#
# The 2026-11-06 loss this pins: do_clean sourced $DEPLOY_REPO/scripts/kafka/reset-preserved-topics.sh,
# and that reader reads the topics.env BESIDE IT -- so both halves of the decision came from the working
# tree. That tree was on a feature branch 376 lines behind main and named none of
# es.rithmic.mbo / futures.liquidity-validator.{evaluations,data-gaps,daily}, all four declared preserved
# on main. The wipe deleted all four, and installing a FIXED script to ~/oe-ops changed nothing until the
# working tree was updated by hand as well -- which is the whole reason a destructive path must not read
# one.
#
# dev-cleanup.sh dispatches on $MODE when sourced, so the REAL function is cut out of it by name and run
# against a REAL local git repository (git init, no network, no remote). Nothing here touches a broker.
#
# Section 2 mutates the function and requires THIS FILE to fail: a green test of a correct script proves
# nothing about the regression it exists to catch.
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
RPT_REAL="${RPT_UNDER_TEST:-$HERE/../kafka/reset-preserved-topics.sh}"
TOPICS_ENV_REAL="${TOPICS_ENV_REAL:-$HERE/../kafka/topics.env}"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
for f in "$SRC" "$RPT_REAL" "$TOPICS_ENV_REAL"; do
  [ -f "$f" ] || { echo "FAIL: $f missing" >&2; exit 1; }
done

fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$SRC"; }
BODY="$(fn reset_preserved_from_main)"
[ -n "$BODY" ] || { echo "FAIL: function reset_preserved_from_main not found in $SRC" >&2; exit 1; }
printf '%s\n' "$BODY" > "$WORK/functions.sh"

# ---------- a real git repo whose COMMITTED files differ from its WORKING TREE ----------
# The difference is the whole point: every assertion below distinguishes the two, so a reader that
# silently falls back to the checkout cannot pass.
REPO="$WORK/repo"
mkdir -p "$REPO/scripts/kafka"
cp "$RPT_REAL" "$REPO/scripts/kafka/reset-preserved-topics.sh"
# A topics.env shaped like the real one: an unindented base declaration, then an environment-gated
# INDENTED append -- how es.rithmic.mbo and futures.liquidity-validator.* are declared.
cat > "$REPO/scripts/kafka/topics.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.committed"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_RESET_PRESERVED_TOPICS="$OPTIONS_EDGE_RESET_PRESERVED_TOPICS t.devonly"
fi
EOF
( cd "$REPO" && git init -q . && git -c user.email=t@t -c user.name=t add -A \
  && git -c user.email=t@t -c user.name=t commit -qm c \
  && git update-ref refs/remotes/origin/main HEAD ) >/dev/null 2>&1 \
  || { echo "FAIL: could not build the fixture repository" >&2; exit 1; }
# NOW make the working tree lie. This is the 11-06 state: a checkout that declares something main does
# not, and does not declare what main does.
cat > "$REPO/scripts/kafka/topics.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.worktree"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
EOF

REF=origin/main:scripts/kafka/topics.env
# Runs the real function in its own shell, as do_clean does, and reports stdout and status separately.
# The sentinel proves the CALLER SURVIVED: the helper ends an unresolvable list with `exit 1`, and from a
# sourced file that kills dev-cleanup.sh itself, mid-clean.
call() { # [env assignments...] -> stdout on fd 1, "rc=<n>" and "ALIVE" markers included
  # The caller's assignments come LAST so they OVERRIDE the defaults. They were first in the first
  # draft, so TOPICS_ENV_REF=no/such/ref was silently replaced by the good $REF and three failure
  # cases measured the success path instead -- a test that cannot fail reports what it never checked.
  env DEPLOY_REPO="$REPO" TOPICS_ENV_REF="$REF" FUNCTIONS="$WORK/functions.sh" "$@" \
    bash -c '. "$FUNCTIONS"; out="$(reset_preserved_from_main)"; rc=$?; printf "%s\n" "$out"; echo "rc=$rc"; echo ALIVE' 2>/dev/null
}
got="$(call)"
case " $(printf '%s' "$got" | tr '\n' ' ') " in
  *" t.committed "*) ok "the keep-list comes from the REF (t.committed)" ;;
  *) bad "the ref's declaration is missing: [$got]" ;;
esac
case "$got" in
  *t.worktree*) bad "the WORKING TREE's topics.env was read -- the 11-06 defect: [$got]" ;;
  *) ok "the working tree's topics.env is NOT read" ;;
esac
case " $(printf '%s' "$got" | tr '\n' ' ') " in
  *" t.devonly "*) ok "the INDENTED, APPENDED entry is read (the 11-06 data loss)" ;;
  *) bad "the appended entry was dropped: [$got]" ;;
esac
case "$got" in
  *t.prodkeep*) bad "the prod-only list leaked in with ENVIRONMENT unset: [$got]" ;;
  *) ok "the prod-only list is absent off production" ;;
esac
case "$got" in *"rc=0"*) ok "a resolvable list returns 0" ;; *) bad "rc was non-zero on a good repo: [$got]" ;; esac
# ENVIRONMENT is passed THROUGH, not forced: forcing it could only ever SHRINK the keep-list, and a
# shorter keep-list is the one direction that destroys data.
case " $(call ENVIRONMENT=production | tr '\n' ' ') " in
  *" t.prodkeep "*) ok "ENVIRONMENT=production reaches the helper (keep-list grows)" ;;
  *) bad "ENVIRONMENT was not passed through: [$(call ENVIRONMENT=production)]" ;;
esac
# RPT_TOPICS_ENV is honoured by the helper when EXECUTED, so it must be stripped: a file somebody
# exported hours ago would otherwise decide what a destructive wipe may delete.
cat > "$WORK/evil.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.bogus"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.bogus2"
EOF
evil="$(call RPT_TOPICS_ENV="$WORK/evil.env")"
case "$evil" in
  *t.bogus*) bad "an ambient RPT_TOPICS_ENV decided the keep-list: [$evil]" ;;
  *) ok "an ambient RPT_TOPICS_ENV is ignored" ;;
esac
case " $(printf '%s' "$evil" | tr '\n' ' ') " in
  *" t.committed "*) ok "...and the ref's list is used instead" ;;
  *) bad "the ref's list was lost when RPT_TOPICS_ENV was set: [$evil]" ;;
esac

# ---------- every way the read can fail must be NON-ZERO, EMPTY, and must not kill the caller ----------
fail_case() { # description, [env assignments...]
  local desc="$1"; shift
  local out; out="$(call "$@")"
  # ^rc=$ matched nothing: the marker is rc=<n>. The first draft therefore read every failure's own
  # "rc=1" line as a printed topic list.
  local body; body="$(printf '%s\n' "$out" | grep -vE '^rc=[0-9]+$|^ALIVE$|^$')"
  case "$out" in
    *ALIVE*) : ;;
    *) bad "$desc: the caller DIED (a sourced helper's exit 1 kills dev-cleanup mid-clean)"; return ;;
  esac
  case "$out" in
    *"rc=0"*) bad "$desc: returned 0 -- the wipe would run with no keep-list: [$out]"; return ;;
  esac
  if [ -n "$body" ]; then bad "$desc: printed a list while failing: [$body]"; return; fi
  ok "$desc: non-zero, nothing printed, caller alive"
}
fail_case "a ref that does not exist" TOPICS_ENV_REF=no/such/ref:scripts/kafka/topics.env
( cd "$REPO" && git -c user.email=t@t -c user.name=t rm -q --cached scripts/kafka/reset-preserved-topics.sh \
  && git -c user.email=t@t -c user.name=t commit -qm nohelper && git update-ref refs/remotes/origin/main HEAD ) >/dev/null 2>&1
fail_case "the helper absent at the ref"
( cd "$REPO" && git update-ref refs/remotes/origin/main HEAD~1 ) >/dev/null 2>&1
# A declaration that resolves to nothing. The helper's own FATAL covers this one; the case is here
# because the function must turn that `exit 1` into a status rather than inherit it.
cat > "$REPO/scripts/kafka/topics.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS=""
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
EOF
cp "$RPT_REAL" "$REPO/scripts/kafka/reset-preserved-topics.sh"
( cd "$REPO" && git -c user.email=t@t -c user.name=t add -A && git -c user.email=t@t -c user.name=t commit -qm empty \
  && git update-ref refs/remotes/origin/main HEAD ) >/dev/null 2>&1
fail_case "an empty declaration at the ref"
# A helper that SUCCEEDS and prints nothing. Nothing upstream catches this one -- it is why the function
# checks the output itself, and an empty keep-list is exactly "keep nothing".
printf '#!/usr/bin/env bash\nexit 0\n' > "$REPO/scripts/kafka/reset-preserved-topics.sh"
( cd "$REPO" && git -c user.email=t@t -c user.name=t add -A && git -c user.email=t@t -c user.name=t commit -qm silent \
  && git update-ref refs/remotes/origin/main HEAD ) >/dev/null 2>&1
fail_case "a helper that exits 0 printing nothing"
# Restore the good fixture for the leak check below.
( cd "$REPO" && git update-ref refs/remotes/origin/main HEAD~3 ) >/dev/null 2>&1

# The two files are materialised into a temp dir on every call; a leak would fill /tmp on a host that
# cleans nightly.
before=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'dev-cleanup-rpt.*' 2>/dev/null | grep -c .)
call >/dev/null; call TOPICS_ENV_REF=no/such:scripts/kafka/topics.env >/dev/null
after=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'dev-cleanup-rpt.*' 2>/dev/null | grep -c .)
if [ "$after" -le "$before" ]; then ok "no temp dir is left behind (success and failure paths)"
else bad "leaked $((after-before)) temp dir(s) under ${TMPDIR:-/tmp}"; fi

# ---------- the REAL topics.env, through the REAL helper, at a REAL commit ----------
# The fixtures above are shapes. This one is the file itself: the test would pass on a parser that reads
# shapes nobody writes, so the thing production reads is read once here too.
REALREPO="$(cd "$HERE/../.." && pwd)"
if [ -d "$REALREPO/.git" ] || [ -f "$REALREPO/.git" ]; then
  real="$(env DEPLOY_REPO="$REALREPO" TOPICS_ENV_REF="HEAD:scripts/kafka/topics.env" FUNCTIONS="$WORK/functions.sh" \
          bash -c '. "$FUNCTIONS"; reset_preserved_from_main' 2>/dev/null)"
  realspaced=" $(printf '%s' "$real" | tr '\n' ' ') "
  if [ -z "${real// /}" ]; then bad "the real topics.env at HEAD resolved NOTHING -- every check below would pass vacuously"
  else
    ok "the real topics.env at HEAD resolves $(printf '%s\n' $real | grep -c .) preserved topics"
    for t in futures.liquidity-validator.evaluations futures.liquidity-validator.data-gaps futures.liquidity-validator.daily; do
      case "$realspaced" in
        *" $t "*) ok "the real list keeps $t (indented, appended -- wiped on 11-06)" ;;
        *) bad "the real list does NOT keep $t, which topics.env declares RESET-PRESERVED" ;;
      esac
    done
  fi
else
  bad "$REALREPO is not a git repository, so the real topics.env could not be read at a commit"
fi

# ---------- 2. MUTATION: each check above must be killed by a specific regression ----------
# Guarded against recursion: a mutant re-runs THIS FILE, which must not mutate again.
if [ -z "${RPT_REF_TEST_MUTANT:-}" ]; then
  echo "  --- mutation: each shape below must make this test FAIL ---"
  mutate() { # name, python expression file
    local name="$1" prog="$2" copy="$WORK/mut-$name.sh"
    cp "$SRC" "$copy"
    python3 "$prog" "$copy" || { bad "mutation $name: could not be applied"; return; }
    if RPT_REF_TEST_MUTANT=1 DEV_CLEANUP_UNDER_TEST="$copy" bash "$HERE/$(basename "${BASH_SOURCE[0]}")" >/dev/null 2>&1; then
      bad "mutation $name: the test still PASSED -- it does not check this"
    else
      ok "mutation $name is caught"
    fi
  }
  # M1: the 11-06 code itself -- source the helper out of the working tree.
  cat > "$WORK/m1.py" <<'PYEOF'
import re,sys
p=sys.argv[1]; s=open(p).read()
m=re.search(r'^reset_preserved_from_main\(\) \{.*?^\}\n', s, re.S|re.M)
assert m
s=s[:m.start()]+'reset_preserved_from_main() {\n  . "$DEPLOY_REPO/scripts/kafka/reset-preserved-topics.sh" 2>/dev/null || return 1\n  printf \'%s\\n\' $RESET_PRESERVED_TOPICS\n}\n'+s[m.end():]
open(p,'w').write(s)
PYEOF
  mutate reads-the-working-tree "$WORK/m1.py"
  # M2: honour the ambient RPT_TOPICS_ENV.
  cat > "$WORK/m2.py" <<'PYEOF'
import sys
p=sys.argv[1]; s=open(p).read()
assert s.count('env -u RPT_TOPICS_ENV bash')==1
open(p,'w').write(s.replace('env -u RPT_TOPICS_ENV bash','bash'))
PYEOF
  mutate ambient-rpt-topics-env "$WORK/m2.py"
  # M3: ignore a failed extraction (an unreadable ref then yields an empty keep-list, silently).
  cat > "$WORK/m3.py" <<'PYEOF'
import re,sys
p=sys.argv[1]; s=open(p).read()
old='    if ! git -C "$DEPLOY_REPO" show "$ref:$dir/$f" > "$work/$f" 2>/dev/null || [ ! -s "$work/$f" ]; then'
assert s.count(old)==1
open(p,'w').write(s.replace(old,'    if false; then'))
PYEOF
  mutate unchecked-git-show "$WORK/m3.py"
  # M4: drop the empty-output check, which nothing upstream duplicates.
  cat > "$WORK/m4.py" <<'PYEOF'
import sys
p=sys.argv[1]; s=open(p).read()
old='  if [ -z "$(printf \'%s\' "$out" | tr -d \'[:space:]\')" ]; then'
assert s.count(old)==1, s.count(old)
open(p,'w').write(s.replace(old,'  if false; then'))
PYEOF
  mutate unchecked-empty-output "$WORK/m4.py"
  # M5: ship the helper without the topics.env it pins itself to.
  cat > "$WORK/m5.py" <<'PYEOF'
import sys
p=sys.argv[1]; s=open(p).read()
old='  for f in reset-preserved-topics.sh topics.env; do'
assert s.count(old)==1
open(p,'w').write(s.replace(old,'  for f in reset-preserved-topics.sh; do'))
PYEOF
  mutate helper-without-its-topics-env "$WORK/m5.py"
fi

if [ "$fails" -eq 0 ]; then
  echo "=== dev-cleanup-reset-preserved-ref-test: OK ==="
else
  echo "=== dev-cleanup-reset-preserved-ref-test: $fails FAILED ==="; exit 1
fi
