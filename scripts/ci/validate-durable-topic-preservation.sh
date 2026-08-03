#!/usr/bin/env bash
# validate-durable-topic-preservation.sh — CI invariant (2026-07-28):
#
#   Every topic DECLARED cross-day durable in scripts/kafka/topics.env — a
#   "<topic>=-1" entry in ANY *_TOPIC_RETENTION_OVERRIDES variable (base,
#   prod-only, es4) — MUST be preserved by BOTH destructive reset paths:
#     * scripts/ops/premarket-reset.sh    — its PRESERVE_TOPICS_REGEX must match it
#     * scripts/ops/offhours-clean-slate.sh — an ANCHORED case arm
#       "^[[:space:]]*<topic>)" that appears BEFORE the purge wildcard arm
#
# WHY AT CI, NOT RUNTIME: the live broker is useless as the source of durability
# here — measured 2026-07-28, ~200 prod topics report retention.ms=-1 because the
# broker default is unlimited (the nightly resets ARE the retention mechanism), so
# "derive the keep-list from the broker" would either no-op the reset or preserve
# nothing. The intent-level declaration lives in topics.env; this check makes it
# IMPOSSIBLE to merge a new durable topic without also preserving it — the failure
# class behind the 2026-07-28 incident (spx.basis.state wiped every pre-market,
# ES->SPX basis cold-started every morning, mapping dark until ~10:00 ET).
#
# Codex r1: P1 — scan EVERY *_TOPIC_RETENTION_OVERRIDES var (the prod-only one
# declared underlying.vix.price=-1 and escaped the first draft); P2 — the case-arm
# check is anchored and position-checked against the purge wildcard, so a comment
# or an arm after "*)" cannot satisfy it. topics.env is PARSED, not sourced (no
# variable collisions with this validator).
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPICS_ENV="scripts/kafka/topics.env"
PREMARKET="scripts/ops/premarket-reset.sh"
CLEANSLATE="scripts/ops/offhours-clean-slate.sh"
fail=0

# All "<topic>=-1" entries across every retention-overrides declaration, without
# sourcing the file: pull the quoted value of each *_TOPIC_RETENTION_OVERRIDES
# assignment, split on whitespace, keep the =-1 pairs.
DURABLE=$(sed -n 's/^[A-Z_]*TOPIC_RETENTION_OVERRIDES="\(.*\)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | sed -n 's/=-1$//p' | sort -u)

if [ -z "$DURABLE" ]; then
  echo "=== validate-durable-topic-preservation: OK (no retention=-1 declarations) ==="
  exit 0
fi

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
WILDCARD_LINE=$( { grep -nE '^\s*\*\)\s+PURGE_TOPICS=' "$CLEANSLATE" || true; } | head -1 | cut -d: -f1)
if [ -z "$WILDCARD_LINE" ]; then
  echo "FAIL: purge wildcard arm ('*)  PURGE_TOPICS=...') not found in $CLEANSLATE — layout changed, update this validator"
  exit 1
fi

for t in $DURABLE; do
  if ! printf '%s\n' "$t" | grep -qE "$REGEX"; then
    echo "FAIL: durable topic '$t' (topics.env retention=-1) is NOT matched by"
    echo "      PRESERVE_TOPICS_REGEX in $PREMARKET — the pre-market reset would wipe it."
    fail=1
  fi
  t_esc=$(printf '%s' "$t" | sed 's/[.[\*^$()+?{}|]/\\&/g')
  ARM_LINE=$( { grep -nE "^[[:space:]]*${t_esc}\)" "$CLEANSLATE" || true; } | head -1 | cut -d: -f1)
  if [ -z "$ARM_LINE" ] || [ "$ARM_LINE" -ge "$WILDCARD_LINE" ]; then
    echo "FAIL: durable topic '$t' (topics.env retention=-1) has no ANCHORED preserve case arm"
    echo "      before the purge wildcard (line $WILDCARD_LINE) in $CLEANSLATE."
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

if [ "$fail" -ne 0 ]; then
  echo "=== validate-durable-topic-preservation: FAILED ==="
  exit 1
fi
echo "checked $(echo "$DURABLE" | wc -w | tr -d ' ') durable topic(s) against both reset scripts + cleanup-topics"
echo "=== validate-durable-topic-preservation: OK ==="
