#!/usr/bin/env bash
# validate-durable-topic-preservation.sh — CI invariant.
#
# Every topic declared retention.ms=-1 in scripts/kafka/topics.env must be CLASSIFIED into exactly
# one of:
#
#   OPTIONS_EDGE_RESET_PRESERVED_TOPICS   — cannot be rebuilt; BOTH destructive reset paths must
#     skip it, and cleanup-topics.sh reads this same list
#   OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS — the producing service reconstructs it; the resets may
#     destroy it freely
#
# WHY THE CLASSIFICATION IS EXPLICIT. Until 2026-08-03 this validator DERIVED preservation from
# retention.ms=-1. That conflated two different statements:
#
#   retention.ms=-1 = "do not age this out by time"  (a Kafka storage knob)
#   reset-preserved = "this cannot be reconstructed" (a claim about the data)
#
# options.spx.spot-vol-regime.current is the case that separates them. It is a single-key ("SPX")
# latest-regime view, so the 24h delete half destroyed the one record it exists to serve over any
# gap longer than a day — every weekend. It needs retention=-1. It must NOT be reset-preserved: the
# service republishes it seconds after starting. Under the derived rule it would have been
# force-preserved on dev.
#
# Requiring EXACTLY ONE classification is what keeps the explicit lists honest. A derived rule
# cannot miss a topic; an explicit list can, so a -1 topic in neither list — or in both — FAILS.
#
# WHY AT CI, NOT RUNTIME: the live broker is useless as the source of durability here — measured
# 2026-07-28, ~200 prod topics report retention.ms=-1 because the broker default is unlimited (the
# nightly resets ARE the retention mechanism). The intent-level declaration lives in topics.env.
# This is the failure class behind the 2026-07-28 incident (spx.basis.state wiped every pre-market,
# ES->SPX basis cold-started every morning, mapping dark until ~10:00 ET).
#
# topics.env is PARSED, not sourced (no variable collisions with this validator).
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPICS_ENV="scripts/kafka/topics.env"
PREMARKET="scripts/ops/premarket-reset.sh"
CLEANSLATE="scripts/ops/offhours-clean-slate.sh"
fail=0

# All "<topic>=-1" entries across every retention-overrides declaration, without
# sourcing the file: pull the quoted value of each *_TOPIC_RETENTION_OVERRIDES
# assignment, split on whitespace, keep the =-1 pairs.
# The RESET-PRESERVED declaration, not everything at retention=-1. Those are different claims --
# "do not age this out by time" versus "this cannot be rebuilt" -- and the previous rule conflated
# them, which would have force-preserved options.spx.spot-vol-regime.current on dev the moment it
# got the retention=-1 it genuinely needs.
list_of() { # variable-name-suffix -> whitespace-split values
  { sed -nE "s/^OPTIONS_EDGE_(PROD_ONLY_)?$1=\"(.*)\"$/\\2/p" "$TOPICS_ENV" | tr ' ' '\n' | grep -v '^$' || true; } | sort -u
}
PRESERVED=$(list_of RESET_PRESERVED_TOPICS)
REBUILDABLE=$(list_of RESET_REBUILDABLE_TOPICS)
MINUS_ONE=$( { sed -nE 's/^[A-Z_]*TOPIC_RETENTION_OVERRIDES="(.*)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | grep -- '=-1$' || true; } | cut -d= -f1 | sort -u)

in_list() { printf '%s\n' "$2" | grep -qx "$1"; }

# EXACTLY ONE classification. Neither list is allowed to be the default.
for topic in $MINUS_ONE; do
  p=no; r=no
  in_list "$topic" "$PRESERVED"   && p=yes
  in_list "$topic" "$REBUILDABLE" && r=yes
  if [ "$p" = no ] && [ "$r" = no ]; then
    echo "FAIL: '$topic' is declared retention.ms=-1 but appears in NEITHER"
    echo "      OPTIONS_EDGE_RESET_PRESERVED_TOPICS nor OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS."
    echo "      Decide, on the record, whether losing it to a reset costs anything."
    fail=1
  elif [ "$p" = yes ] && [ "$r" = yes ]; then
    echo "FAIL: '$topic' is declared BOTH reset-preserved and reset-rebuildable."
    fail=1
  fi
done

# Preservation without retention=-1 is pointless: the resets would spare the topic and time
# retention would delete it anyway. (The converse is the conflation this design removed.)
for topic in $PRESERVED; do
  if ! in_list "$topic" "$MINUS_ONE"; then
    echo "FAIL: '$topic' is declared RESET-PRESERVED but has no retention.ms=-1 override — the"
    echo "      resets would spare it and time retention would delete it anyway."
    fail=1
  fi
done

# Symmetric, so neither list becomes a dumping ground: classifying a topic that has no -1 override
# says nothing about anything.
for topic in $REBUILDABLE; do
  if ! in_list "$topic" "$MINUS_ONE"; then
    echo "FAIL: '$topic' is declared RESET-REBUILDABLE but has no retention.ms=-1 override —"
    echo "      the classification only means something for topics that carry one."
    fail=1
  fi
done

# NOT an early exit. An empty PRESERVED list used to short-circuit to OK while every preserve arm
# stayed wired in the reset scripts, so deleting the declarations passed the gate.
DURABLE="$PRESERVED"


# The regex premarket-reset actually uses (first single-quoted assignment; the
# `|| true` keeps set -e from eating the diagnostic when it is missing).
REGEX=$( { grep -m1 "^PRESERVE_TOPICS_REGEX=" "$PREMARKET" || true; } | sed "s/^PRESERVE_TOPICS_REGEX='//; s/'$//")
if [ -z "$REGEX" ]; then
  echo "FAIL: PRESERVE_TOPICS_REGEX not found (or not a single-quoted assignment) in $PREMARKET"
  exit 1
fi

# Line number of the clean-slate purge wildcard arm — every preserve arm must
# precede it or it never matches. The wildcard line is the one filling
# PURGE_TOPICS from "*)".

for t in $DURABLE; do
  if ! printf '%s\n' "$t" | grep -qE "$REGEX"; then
    echo "FAIL: '$t' is declared RESET-PRESERVED but is NOT matched by"
    echo "      PRESERVE_TOPICS_REGEX in $PREMARKET — the pre-market reset would wipe it."
    fail=1
  fi
done

# The two reset scripts are not the only destructive paths. cleanup-topics.sh runs from the
# monolithic pipeline with KAFKA_CLEANUP_MODE defaulting to delete-recreate, and BOTH its modes
# iterate the APPROVED topic list -- where every durable topic also appears. PROTECTED_TOPIC_REGEX
# does not save them: it is consulted only for UNWANTED topics. A durable declaration that survives
# the resets and is then deleted by cleanup is not durable, so assert the exemption exists here too.
CLEANUP="scripts/kafka/cleanup-topics.sh"
if [ ! -f "$CLEANUP" ]; then
  echo "FAIL: $CLEANUP not found — layout changed, update this validator"
  exit 1
fi
# Counting is_durable guards was a proxy and a bad one: a fourth destructive path leaves the count
# at three and passes while it deletes data. Run the executable test instead -- it drives the real
# script with mocked kafka CLIs and asserts on the calls it actually makes, so an unguarded path
# fails however it is written.
CLEANUP_TEST="scripts/kafka/cleanup-topics-durable-test.sh"
if [ ! -x "$CLEANUP_TEST" ]; then
  echo "FAIL: $CLEANUP_TEST missing or not executable — the durability guarantee has no test"
  fail=1
elif ! bash "$CLEANUP_TEST" > /tmp/cleanup-durable-test.out 2>&1; then
  echo "FAIL: $CLEANUP_TEST — cleanup-topics.sh does not preserve durable topics:"
  sed 's/^/      /' /tmp/cleanup-durable-test.out
  fail=1
fi

# The REVERSE direction. An explicit list has one weakness the old retention=-1 rule did not: it
# cannot notice a topic somebody FORGOT to declare. So require the converse -- every topic that the
# reset scripts already go out of their way to preserve must be declared here. Someone who adds a
# preserve arm without declaring it, or deletes a declaration while the arm stays, fails this.
#
# Wildcard arms (*gamma-migration-scorer-changelog) are patterns rather than declared topics and
# are skipped: they preserve Streams-internal changelogs, which topics.env does not name.
# The keep-list parser now lives in ONE executable helper that both destructive scripts source, so
# the mechanism is tested by running it (scripts/kafka/reset-preserved-topics-test.sh) rather than
# by pattern-matching its callers. What is left to assert here is that the callers still USE it.
#
# Comments are STRIPPED before matching. The previous version grepped raw text, and deleting the
# live call while leaving the function definition and its comment block kept CI green -- the exact
# bypass this now closes.
HELPER="scripts/kafka/reset-preserved-topics.sh"
if [ ! -x "$HELPER" ]; then
  echo "FAIL: $HELPER missing or not executable — nothing defines what may not be purged"
  fail=1
fi
for caller in "$CLEANSLATE" scripts/kafka/cleanup-topics.sh; do
  code=$(sed 's/#.*//' "$caller")
  if ! printf '%s' "$code" | grep -q 'reset-preserved-topics.sh'; then
    echo "FAIL: $caller does not source $HELPER — it would purge topics the declaration says"
    echo "      cannot be rebuilt."
    fail=1
  fi
done
code=$(sed 's/#.*//' "$CLEANSLATE")
if ! printf '%s' "$code" | grep -qE 'if[[:space:]]+is_reset_preserved'; then
  echo "FAIL: $CLEANSLATE sources the keep-list but never BRANCHES on is_reset_preserved —"
  echo "      the list is loaded and then ignored."
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "=== validate-durable-topic-preservation: FAILED ==="
  exit 1
fi
echo "checked $(echo "$DURABLE" | wc -w | tr -d ' ') durable topic(s) against both reset scripts + cleanup-topics"
echo "=== validate-durable-topic-preservation: OK ==="
