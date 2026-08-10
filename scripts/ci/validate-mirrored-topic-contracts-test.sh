#!/usr/bin/env bash
# Executable proof that validate-mirrored-topic-contracts.sh actually ENFORCES the contract it
# claims to, dimension by dimension.
#
# A validator that prints "checked 5 mirrored topic(s)" proves only that it found five names. Every
# mutation below breaks exactly ONE dimension of ONE topic in a COPY of the real declarations, and
# each must turn the run red — including the mutations that make the validator's own PARSING return
# nothing, which is the failure mode where a checker quietly starts approving everything.
#
# Fixtures are copies. Nothing here touches the repo's own files.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
VALIDATOR="$HERE/validate-mirrored-topic-contracts.sh"
TENV_REL="scripts/kafka/topics.env"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
rc=0

# Checksums of every file a mutation could conceivably reach, taken BEFORE any of them run. Checked
# again at the end. Deliberately self-contained rather than `git diff`: git would also flag a file
# the author has legitimately edited and not yet committed, which is not what this is asking.
shasum_targets() { shasum "$REPO/$TENV_REL" "$REPO"/Jenkinsfile.es-*-mirror; }
shasum_targets >"$WORK/before.sha"

# A pristine fixture root: the topics.env and every mirror Jenkinsfile, and nothing else the
# validator reads.
mkfixture() { # -> prints a fresh root
  local root="$WORK/fx.$RANDOM.$$"
  mkdir -p "$root/scripts/kafka"
  cp "$REPO/$TENV_REL" "$root/$TENV_REL"
  cp "$REPO"/Jenkinsfile.es-*-mirror "$root/"
  printf '%s\n' "$root"
}

run() { MTC_ROOT="$1" bash "$VALIDATOR" >"$WORK/out.txt" 2>&1; }

expect_pass() {
  local root="$1" name="$2"
  if run "$root"; then printf '  ok   %-52s exit 0\n' "$name"
  else printf '  FAIL %-52s expected PASS, got exit 1\n' "$name"; sed 's/^/         /' "$WORK/out.txt"; rc=1; fi
}

expect_fail() {
  local root="$1" name="$2" want="$3"
  if run "$root"; then
    printf '  FAIL %-52s expected FAILURE, validator said OK\n' "$name"; rc=1
  elif grep -q "$want" "$WORK/out.txt"; then
    printf '  ok   %-52s exit 1\n' "$name"
  else
    printf '  FAIL %-52s failed for the WRONG reason (no /%s/)\n' "$name" "$want"
    sed 's/^/         /' "$WORK/out.txt"; rc=1
  fi
}

# sed -i is not portable between GNU and BSD, so every mutation rewrites through a temp file.
edit() { # root, relative path, sed script
  local f="$1/$2"; shift 2
  sed "$1" "$f" >"$f.new" && mv "$f.new" "$f"
}

echo "--- baseline: the REAL declarations must pass ---"
BASE="$(mkfixture)"; expect_pass "$BASE" "unmutated repo declarations"

echo "--- the deletion exposure: a mirrored topic that is not declared at all ---"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.tape-zones\.board:1 / /'
expect_fail "$R" "undeclared mirror target" "NOT declared in"

echo "--- FROZEN schema: all three dimensions are authoritative ---"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/es\.options\.indicators\.bars:8/es.options.indicators.bars:4/'
expect_fail "$R" "partition count below the frozen contract" "freezes it at 8 partition"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/es\.tape-zones\.board=-1/es.tape-zones.board=86400000/'
expect_fail "$R" "retention differing from the frozen contract" "asserts retention.ms=-1"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.tape-zones\.board=-1"/"/'
expect_fail "$R" "retention override dropped entirely" "asserts retention.ms=-1"

echo "--- compaction, in both directions ---"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.options\.indicators\.snapshot\.current es\.futures\.aggressor-flow"/ es.futures.aggressor-flow"/'
expect_fail "$R" "compacted FROZEN topic dropped from the list" "STRIP the compaction"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.futures\.aggressor-flow"/"/'
expect_fail "$R" "compacted COPIED topic dropped from the list" "STRIP the compaction"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.futures\.aggressor-flow"/ es.futures.aggressor-flow es.options.indicators.bars"/'
expect_fail "$R" "append-only topic wrongly compacted" "collapses it to"

echo "--- COPIED schema: es4's declaration is the authority, and it is REQUIRED ---"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.strike-intelligence-by-strike:32 es\.strike-intelligence-dashboard/ es.strike-intelligence-dashboard/'
expect_fail "$R" "COPIED topic missing from the es4 set" "no reviewed"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/es\.tape-zones\.board:1 es\.tape-zones\.cells:4/es.tape-zones.board:4 es.tape-zones.cells:4/'
expect_fail "$R" "source and target disagree on partitions" "must agree"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/ es\.tape-zones\.cells es\.tape-zones\.board"/ es.tape-zones.cells"/'
expect_fail "$R" "source and target disagree on compaction" "compaction disagrees across"

echo "--- the parser itself must fail closed, never silently check less ---"
R="$(mkfixture)"; edit "$R" Jenkinsfile.es-tape-zones-mirror 's/PARTS=1; POLICY=compact,delete; RET=-1/PARTS="1" ; POLICY="compact,delete" ; RET="-1"/'
expect_fail "$R" "frozen arm reformatted out of recognition" "neither a frozen"
R="$(mkfixture)"; edit "$R" Jenkinsfile.es-futures-flow-mirror "s/string(name: 'TOPIC',/string(name: 'MIRROR_TOPIC',/"
expect_fail "$R" "TOPIC parameter renamed" "no TOPIC parameter"
R="$(mkfixture)"; edit "$R" "$TENV_REL" 's/^OPTIONS_EDGE_ES4_COMPACTED_TOPICS=.*$/OPTIONS_EDGE_ES4_COMPACTED_TOPICS=""/'
expect_fail "$R" "a parsed declaration emptied" "parsed an EMPTY"
R="$(mkfixture)"; rm -f "$R"/Jenkinsfile.es-*-mirror
expect_fail "$R" "every mirror job removed" "no Jenkinsfile.es-.*-mirror found"
R="$(mkfixture)"; rm -f "$R/$TENV_REL"
expect_fail "$R" "topics.env unreadable" "cannot read"

echo "--- a mutated fixture must not have leaked into the repo ---"
shasum_targets >"$WORK/after.sha"
if diff -q "$WORK/before.sha" "$WORK/after.sha" >/dev/null; then
  printf '  ok   %-52s\n' "repo files byte-identical before and after"
else
  printf '  FAIL %-52s a mutation escaped the fixture:\n' "repo files byte-identical before and after"
  diff "$WORK/before.sha" "$WORK/after.sha" | sed 's/^/         /'; rc=1
fi

if [ "$rc" -ne 0 ]; then
  echo "=== validate-mirrored-topic-contracts-test: FAILED ==="
  exit 1
fi
echo "=== validate-mirrored-topic-contracts-test: OK ==="
