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

VALIDATOR="$REPO/scripts/ci/validate-durable-topic-preservation.sh"
TOPICS="$REPO/scripts/kafka/topics.env"
CLEANSLATE="$REPO/scripts/ops/offhours-clean-slate.sh"
fail=0

reset_copy() {
  cp "$SRC/scripts/kafka/topics.env" "$TOPICS"
  cp "$SRC/scripts/ops/offhours-clean-slate.sh" "$CLEANSLATE"
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

sed -i.bak 's/^OPTIONS_EDGE_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
sed -i.bak 's/^OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=.*/OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS=""/' "$TOPICS"
expect "ALL preserved declarations deleted" 1 "appears in NEITHER"

sed -i.bak 's/^OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="/OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="brand.new.topic=-1 /' "$TOPICS"
expect "unclassified retention=-1 topic" 1 "appears in NEITHER"

sed -i.bak 's/^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="/OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="spx.basis.state /' "$TOPICS"
expect "topic classified as BOTH" 1 "BOTH reset-preserved and reset-rebuildable"

sed -i.bak 's/ spx\.basis\.state=-1//' "$TOPICS"
expect "preserved without retention=-1" 1 "has no retention.ms=-1 override"

sed -i.bak 's/^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="/OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="never.declared.anywhere /' "$TOPICS"
expect "rebuildable entry without retention=-1" 1 "no retention.ms=-1 override"

# The bypasses Codex actually performed: delete the LIVE call but leave the function and its
# comment block; and delete the fail-closed exit while leaving its diagnostic text. Both kept CI
# green under the previous text-matching checks.
perl -0pi -e 's/\n  if is_reset_preserved "\$t"; then\n(?:.*?\n)*?  fi\n//' "$CLEANSLATE"
expect "live is_reset_preserved call deleted" 1 "never BRANCHES"

perl -0pi -e 's/\n\. "\$SCRIPT_DIR\/\.\.\/kafka\/reset-preserved-topics\.sh"\n/\n/' "$CLEANSLATE"
expect "reset script stops sourcing the helper" 1 "does not source"

[ $fail -eq 0 ] && echo "=== validate-durable-topic-preservation-mutation-test: OK ===" || { echo "=== validate-durable-topic-preservation-mutation-test: FAILED ==="; exit 1; }
