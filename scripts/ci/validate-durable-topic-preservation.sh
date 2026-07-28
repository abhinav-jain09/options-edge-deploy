#!/usr/bin/env bash
# validate-durable-topic-preservation.sh — CI invariant (2026-07-28):
#
#   Every topic DECLARED cross-day durable in scripts/kafka/topics.env
#   (OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES entry "<topic>=-1") MUST be preserved
#   by BOTH destructive reset paths:
#     * scripts/ops/premarket-reset.sh    — its PRESERVE_TOPICS_REGEX must match it
#     * scripts/ops/offhours-clean-slate.sh — its classification must keep it
#       (an exact "<topic>)" case arm ahead of the purge wildcards)
#
# WHY AT CI, NOT RUNTIME: the live broker is useless as the source of durability
# here — measured 2026-07-28, ~200 prod topics report retention.ms=-1 because the
# broker default is unlimited (the nightly resets ARE the retention mechanism), so
# "derive the keep-list from the broker" would either no-op the reset or preserve
# nothing. The intent-level declaration lives in topics.env; this check makes it
# IMPOSSIBLE to merge a new durable topic without also preserving it — the failure
# class behind the 2026-07-28 incident (spx.basis.state wiped every pre-market,
# ES->SPX basis cold-started every morning, mapping dark until ~10:00 ET).
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPICS_ENV="scripts/kafka/topics.env"
PREMARKET="scripts/ops/premarket-reset.sh"
CLEANSLATE="scripts/ops/offhours-clean-slate.sh"
fail=0

# shellcheck disable=SC1090
. "$TOPICS_ENV"   # defines OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES (and friends)

DURABLE=$(for kv in ${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-}; do
  case "$kv" in *=-1) echo "${kv%=-1}" ;; esac
done)

if [ -z "$DURABLE" ]; then
  echo "=== validate-durable-topic-preservation: OK (no retention=-1 declarations) ==="
  exit 0
fi

# The regex premarket-reset actually uses (first assignment wins; strip quotes).
REGEX=$(grep -m1 "^PRESERVE_TOPICS_REGEX=" "$PREMARKET" | sed "s/^PRESERVE_TOPICS_REGEX='//; s/'$//")
[ -n "$REGEX" ] || { echo "FAIL: PRESERVE_TOPICS_REGEX not found in $PREMARKET"; exit 1; }

for t in $DURABLE; do
  if ! printf '%s\n' "$t" | grep -qE "$REGEX"; then
    echo "FAIL: durable topic '$t' (topics.env retention=-1) is NOT matched by"
    echo "      PRESERVE_TOPICS_REGEX in $PREMARKET — the pre-market reset would wipe it."
    fail=1
  fi
  if ! grep -qF "$t)" "$CLEANSLATE"; then
    echo "FAIL: durable topic '$t' (topics.env retention=-1) has no preserve case arm"
    echo "      ('$t)') in $CLEANSLATE — the clean-slate would purge it."
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "=== validate-durable-topic-preservation: FAILED ==="
  exit 1
fi
echo "checked $(echo "$DURABLE" | wc -w | tr -d ' ') durable topic(s) against both reset scripts"
echo "=== validate-durable-topic-preservation: OK ==="
