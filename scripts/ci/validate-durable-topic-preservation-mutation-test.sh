#!/usr/bin/env bash
# Mutation tests for validate-durable-topic-preservation.sh.
#
# A green gate proves nothing about the failures it exists to catch. Each case breaks the
# declaration or the reset script in one specific way and asserts the validator FAILS with the
# right reason. Three of these were live defects when written: the empty-list branch short-circuited
# to OK while every preserve arm stayed wired; a topic could be declared retention=-1 and classified
# nowhere; and the reset script's preservation was inferred by parsing its layout.
#
# EVERY mutation happens inside a COPY of the tree. The first version mutated the real tracked files
# and restored them in an EXIT trap that deleted the backup directory BEFORE copying from it -- so
# any interruption would have left the working tree modified. Nothing here can touch tracked files.
set -euo pipefail
cd "$(dirname "$0")/../.."
SRC="$PWD"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

REPO="$work/repo"
mkdir -p "$REPO/scripts"
cp -R "$SRC/scripts/ci" "$SRC/scripts/kafka" "$SRC/scripts/ops" "$REPO/scripts/"
# The validator also reads a REPO-ROOT file: Jenkinsfile.kafka-reset, because it checks that the job
# shipping daily-kafka-reset.sh to the production host also ships the keep-list files that script now
# sources. A sandbox of scripts/ alone made the UNMUTATED tree fail with "Jenkinsfile.kafka-reset not
# found" (2026-10-13) — the same packaging gap that made two other harnesses report findings about
# topics.env when apply-topics.sh gained a sibling (#1165). Derived from what the validator reads, so a
# future root-level dependency fails here loudly rather than silently.
for _root in $(sed -nE 's/^[A-Z_]+="(Jenkinsfile[A-Za-z0-9._-]*)"$/\1/p' "$SRC/scripts/ci/validate-durable-topic-preservation.sh"); do
  [ -r "$SRC/$_root" ] || { echo "FATAL: the validator names $_root, which is not in $SRC" >&2; exit 1; }
  cp "$SRC/$_root" "$REPO/"
done
# The validator runs apply-topics-vol-premium-safety-test.sh (about 45 mocked apply-topics runs plus its own mutation
# self-checks) on EVERY expect() below. In THIS COPY only it is a stub. Two cases here DO clear the preserved
# declarations, and with them the four vol-premium memberships. But every case asserts a STRUCTURAL finding the
# validator reports on its own (classification, extraction, reset-script coverage), and what the vol-premium suite would
# say about those mutated copies is not what they test. The real test still runs, once, wherever the real validator runs
# (validate-services.sh section 6), instead of eleven more times inside every service-deploy validate stage.
printf '#!/usr/bin/env bash\necho "=== apply-topics-vol-premium-safety: stubbed inside the mutation harness ==="\n' \
  > "$REPO/scripts/kafka/apply-topics-vol-premium-safety-test.sh"

# cleanup-topics-durable-test.sh is stubbed here for the SAME reason, measured 2026-11-11: it is ~110 s of
# mocked cleanup runs (it grew when the dev-clean keep list gained its own sections), and the validator runs
# it on EVERY expect() below -- twenty-one times, ~38 min inside a harness whose cases are all about
# declarations and the validator's own structural checks. Not one case asserts anything this sub-test
# reports, so stubbing it removes cost, not coverage; the real harness still runs, once, wherever the real
# validator runs. The exec bit survives the overwrite (cp -R copied it executable), and the validator
# refuses a non-executable CLEANUP_TEST, so it is set again rather than assumed.
printf '#!/usr/bin/env bash\necho "=== cleanup-topics-durable-test: stubbed inside the mutation harness ==="\n' \
  > "$REPO/scripts/kafka/cleanup-topics-durable-test.sh"
chmod +x "$REPO/scripts/kafka/cleanup-topics-durable-test.sh"

VALIDATOR="$REPO/scripts/ci/validate-durable-topic-preservation.sh"
TOPICS="$REPO/scripts/kafka/topics.env"
CLEANSLATE="$REPO/scripts/ops/offhours-clean-slate.sh"
DEVCLEAN="$REPO/scripts/ops/dev-cleanup.sh"
fail=0

reset_copy() {
  cp "$SRC/scripts/kafka/topics.env" "$TOPICS"
  cp "$SRC/scripts/ops/offhours-clean-slate.sh" "$CLEANSLATE"
  cp "$SRC/scripts/ops/dev-cleanup.sh" "$DEVCLEAN"
  # The VALIDATOR too. Every mutation until now edited only the declarations, so this was never
  # noticed — but a mutation that edits the validator would otherwise leak into every case after it,
  # and those cases would report on a script nobody wrote.
  cp "$SRC/scripts/ci/validate-durable-topic-preservation.sh" "$VALIDATOR"
}

expect() { # description | want-status | expected-substring
  local desc="$1" want="$2" why="$3" got=0
  bash "$VALIDATOR" > "$work/out" 2>&1 || got=$?
  reset_copy
  if [ "$got" != "$want" ]; then
    printf '  FAIL %-54s exit %s, want %s\n' "$desc" "$got" "$want"; fail=1; return
  fi
  if [ -n "$why" ] && ! grep -qF -e "$why" "$work/out"; then
    printf '  FAIL %-54s exit %s but never said %q\n' "$desc" "$got" "$why"; fail=1; return
  fi
  printf '  ok   %-54s exit %s\n' "$desc" "$got"
}

expect "unmutated tree passes" 0 "validate-durable-topic-preservation: OK"

sed -i.bak 's/^OPTIONS_EDGE_RESET_PRESERVED_TOPICS="spx.basis.state /OPTIONS_EDGE_RESET_PRESERVED_TOPICS="/' "$TOPICS"
expect "one declaration deleted" 1 "appears in NEITHER"

# INDENT-TOLERANT seds, since 2026-11-06. These two claim to empty EVERY preserved declaration, and
# the readers now see indented appends -- so a column-0 sed left the three
# futures.liquidity-validator.* entries declared and the case measured something else entirely
# ("emptiness instead of counts" got exit 1, a real classification finding, instead of the exit 3
# harness failure it exists to assert). A mutation that no longer creates its own scenario is a case
# that tests nothing while reporting on it.
sed -i.bak 's/^[[:space:]]*OPTIONS_EDGE_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
sed -i.bak 's/^[[:space:]]*OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
expect "ALL preserved declarations deleted" 1 "appears in NEITHER"

sed -i.bak 's/^OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="/OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="brand.new.topic=-1 /' "$TOPICS"
expect "unclassified retention=-1 topic" 1 "appears in NEITHER"

sed -i.bak 's/^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="/OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="spx.basis.state /' "$TOPICS"
expect "topic classified as BOTH" 1 "BOTH reset-preserved and reset-rebuildable"

sed -i.bak 's/ spx\.basis\.state=-1//' "$TOPICS"
expect "preserved without retention=-1" 1 "has no retention.ms=-1 override"

sed -i.bak 's/^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="/OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="never.declared.anywhere /' "$TOPICS"
expect "rebuildable entry without retention=-1" 1 "no retention.ms=-1 override"

# BZ 362: the -1 extraction used [A-Z_]* and so skipped every OPTIONS_EDGE_ES4_* declaration. The
# guard reported OK while not looking at part of what it guards, and the only way to see that from
# outside was the count cross-check. Narrow it back and the cross-check must catch it.
# ONLY the shell extraction. A first attempt rewrote every occurrence, including the one inside the
# python cross-check — so both narrowed together, agreed with each other, and the mutation SURVIVED
# while looking like it had been applied. A mutation that changes both sides of a comparison tests
# nothing.
# 2026-11-06: this sed used to search for `^[A-Z0-9_]*TOPIC_RETENTION_OVERRIDES`, anchored. Making the
# extraction indent-tolerant turned the real text into `^[[:space:]]*[A-Z0-9_]*TOPIC...`, so the search
# stopped matching and the mutation became a silent no-op — the case would have passed while testing
# nothing. The guard below is what said so, which is the argument for having one on every mutation: a
# change to the code under test can disarm its own test, and only the vacuity check notices.
sed -i.bak '/MINUS_ONE=\$( { sed -nE/s/\[A-Z0-9_\]\*TOPIC_RETENTION_OVERRIDES/[A-Z_]*TOPIC_RETENTION_OVERRIDES/' "$VALIDATOR"
grep -q "\[A-Z_\]\*TOPIC_RETENTION_OVERRIDES" "$VALIDATOR" \
  || { echo "  FAIL ES4 mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "ES4 retention declarations skipped" 3 "independent read"

# And the cross-check must compare COUNTS, not emptiness. An emptiness rule turned the mutation above
# ("ALL preserved declarations deleted") into a harness complaint under a different exit code, hiding
# a real finding behind a complaint about the reader.
sed -i.bak 's/if \[ "\$got" -ne "\$want" \]; then/if [ "$got" -eq 0 ] || [ "$got" -ne "$want" ]; then/' "$VALIDATOR"
sed -i.bak 's/^[[:space:]]*OPTIONS_EDGE_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
sed -i.bak 's/^[[:space:]]*OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
expect "emptiness instead of counts hides a real finding" 3 "independent read"

# The bypasses Codex actually performed: delete the LIVE call but leave the function and its
# comment block; and delete the fail-closed exit while leaving its diagnostic text. Both kept CI
# green under the previous text-matching checks.
perl -0pi -e 's/\n  if is_reset_preserved "\$t"; then\n(?:.*?\n)*?  fi\n//' "$CLEANSLATE"
expect "live is_reset_preserved call deleted" 1 "never BRANCHES"

perl -0pi -e 's/\n\. "\$SCRIPT_DIR\/\.\.\/kafka\/reset-preserved-topics\.sh"\n/\n/' "$CLEANSLATE"
expect "reset script stops sourcing the helper" 1 "does not source"

# The DEV wipe's three checks. dev-cleanup.sh is an estate-wide reset by the validator's own criterion
# and was in neither caller loop until 2026-11-06, when it deleted four topics declared preserved
# because it read its keep-list out of the working tree.
#
# Each case breaks ONE of the three independently, because they fail for different reasons and a single
# mutation that trips all three would not show which one is load-bearing.
perl -0pi -e 's/\n(\s*)RESET_PRESERVED_TOPICS="\$\(reset_preserved_from_main\)" \|\| \{\n(?:.*?\n)*?\s*return 1; \}\n/\n/' "$DEVCLEAN"
grep -q 'RESET_PRESERVED_TOPICS="$(reset_preserved_from_main)"' "$DEVCLEAN" \
  && { echo "  FAIL dev-clean resolve mutation did not apply — the case below would pass vacuously"; fail=1; }
# The function DEFINITION is deliberately left in place: defined-but-never-called is the bypass this
# check exists for, and a mutation that deleted the definition too would be caught by the name alone.
expect "dev wipe stops calling the keep-list resolver" 1 "never CALLS reset_preserved_from_main"

# The 11-06 code itself, restored: source the helper out of the checkout. The resolve call stays, so
# this case is only caught by the check that looks for the working-tree read.
perl -0pi -e 's/\n(\s*)PRESERVE_RE="/\n. "\$DEPLOY_REPO\/scripts\/kafka\/reset-preserved-topics.sh" 2>\/dev\/null || RESET_PRESERVED_TOPICS=""\n${1}PRESERVE_RE="/' "$DEVCLEAN"
grep -q '^\. "\$DEPLOY_REPO/scripts/kafka/reset-preserved-topics.sh"' "$DEVCLEAN" \
  || { echo "  FAIL dev-clean working-tree mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "dev wipe sources the helper from the checkout" 1 "out of \$DEPLOY_REPO (the working tree)"

# Resolved and then ignored: the list is read from the right place and never excluded from the delete.
perl -0pi -e 's/\| \{ \[ -n "\$PRESERVE_RE" \] && grep -vxE "\$PRESERVE_RE" \|\| cat; \}/| cat/' "$DEVCLEAN"
grep -q 'grep -vxE "\$PRESERVE_RE"' "$DEVCLEAN" \
  && { echo "  FAIL dev-clean filter mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "dev wipe ignores the keep-list it resolved" 1 "loaded and then ignored"

# And the named ES delete, which consults no keep-list at all: a preserved topic added to that set is
# deleted every morning with the declaration intact. spx.basis.state is RESET-PRESERVED on main.
sed -i.bak "s/^ES_CLEAN_TOPICS='/ES_CLEAN_TOPICS='spx.basis.state /" "$DEVCLEAN"
grep -q "^ES_CLEAN_TOPICS='spx.basis.state " "$DEVCLEAN" \
  || { echo "  FAIL ES_CLEAN_TOPICS mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "a preserved topic inside ES_CLEAN_TOPICS" 1 "in ES_CLEAN_TOPICS, and it is declared preserved"

# ...and the read of that set must FAIL CLOSED, not iterate nothing. Renaming the variable is the
# cheapest way to make a check that greps for a name report "no finding" instead of "I could not look".
sed -i.bak "s/^ES_CLEAN_TOPICS='/ES_CLEAN_TOPICS_RENAMED='/" "$DEVCLEAN"
grep -q "^ES_CLEAN_TOPICS='" "$DEVCLEAN" \
  && { echo "  FAIL ES_CLEAN_TOPICS rename did not apply — the case below would pass vacuously"; fail=1; }
expect "ES_CLEAN_TOPICS unreadable" 1 "could not read ES_CLEAN_TOPICS"

# THE COLUMN-0 ANCHOR, on each engine in turn. Until 2026-11-06 both list_of and the python recount
# were anchored at ^OPTIONS_EDGE_, so the three futures.liquidity-validator.* topics -- declared
# RESET-PRESERVED by an INDENTED append -- were invisible to this validator, which then reported
# "checked 37 durable topic(s)" on a file declaring 40 and exited OK. It looked clean while not
# looking, which is the one failure mode a gate must not have.
#
# ONE ENGINE AT A TIME, never both: narrowing both would make them agree with each other again and the
# mutation would SURVIVE while appearing to have been applied -- the lesson already recorded for the
# ES4 case above, and the reason the cross-check exists at all.
#
# python, not sed: the python side's anchor holds a literal backslash-t inside a bracket expression,
# and expressing that through sed or perl quoting produced a pattern matching nothing. A mutation that
# does not apply is a case that passes while testing nothing, which is what the "did not apply" guards
# below exist to catch -- and they caught exactly that while this was being written.
python3 - "$VALIDATOR" <<'PYEOF'
import sys
p = sys.argv[1]; s = open(p).read()
old = '  { sed -nE "s/^[[:space:]]*OPTIONS_EDGE_'
new = '  { sed -nE "s/^OPTIONS_EDGE_'
assert s.count(old) == 1, s.count(old)
open(p, "w").write(s.replace(old, new))
PYEOF
grep -q 'sed -nE "s/\^OPTIONS_EDGE_' "$VALIDATOR" \
  || { echo "  FAIL list_of re-anchor did not apply — the case below would pass vacuously"; fail=1; }
expect "list_of blind to an indented declaration" 3 "independent read"

# And the other way round: the python recount narrowed while the sed side stays indent-tolerant.
python3 - "$VALIDATOR" <<'PYEOF'
import sys
p = sys.argv[1]; s = open(p).read()
old = "r'^[ \\t]*OPTIONS_EDGE_(?:PROD_ONLY_)?%s="
new = "r'^OPTIONS_EDGE_(?:PROD_ONLY_)?%s="
assert s.count(old) == 1, s.count(old)
open(p, "w").write(s.replace(old, new))
PYEOF
grep -q "r'\^OPTIONS_EDGE_(?:PROD_ONLY_)?%s=" "$VALIDATOR" \
  || { echo "  FAIL recount re-anchor did not apply — the case below would pass vacuously"; fail=1; }
expect "the recount blind to an indented declaration" 3 "independent read"

# A CROSS-REFERENCE, the case the first version of the $VAR strip swallowed in both engines at once.
# Neither the strip nor the extraction cross-check can catch it -- both readers discard a `$` token the
# same way, so they AGREE -- which is why _assert_only_self_references refuses it before any read.
# Asserted here because a guard nobody mutates is a guard nobody has tested: the first version's
# comment claimed this very case produced a harness failure while the code exited OK.
sed -i.bak 's/^OPTIONS_EDGE_RESET_PRESERVED_TOPICS="/OPTIONS_EDGE_RESET_PRESERVED_TOPICS="$SOME_OTHER_LIST /' "$TOPICS"
grep -q 'OPTIONS_EDGE_RESET_PRESERVED_TOPICS="\$SOME_OTHER_LIST ' "$TOPICS" \
  || { echo "  FAIL cross-reference mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "a cross-reference to another list" 3 "refers to a variable OTHER than the one being declared"

# ...and the self-reference scanner must not be able to match ZERO. A selector that selects nothing
# reports no cross-reference, which reads identically to "there are none" -- the same shape as the
# column-0 anchor this whole change is about, one level up, in the guard added to fix it (Codex r3).
# The selector is narrowed to a name no declaration has; the non-vacuity check must turn that into a
# harness failure rather than a clean pass.
python3 - "$VALIDATOR" <<'PYEOF'
import sys
p = sys.argv[1]; s = open(p).read()
old = "([A-Za-z0-9_]*TOPIC[A-Za-z0-9_]*)"
new = "(ZZ_NO_SUCH_DECLARATION)"
assert s.count(old) == 1, s.count(old)
open(p, "w").write(s.replace(old, new))
PYEOF
grep -q 'ZZ_NO_SUCH_DECLARATION' "$VALIDATOR" \
  || { echo "  FAIL scanner-selector mutation did not apply — the case below would pass vacuously"; fail=1; }
expect "the self-reference scanner matching nothing" 3 "found"

[ $fail -eq 0 ] && echo "=== validate-durable-topic-preservation-mutation-test: OK ===" || { echo "=== validate-durable-topic-preservation-mutation-test: FAILED ==="; exit 1; }
