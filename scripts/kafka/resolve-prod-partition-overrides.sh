#!/usr/bin/env bash
# Resolve OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES into OPTIONS_EDGE_TOPICS.
#
# WHY THIS EXISTS. topics.env declares ONE partition count per topic, for every environment, and
# apply-topics.sh treats it as a MINIMUM: a live topic below it is refused, because raising a count
# means delete+recreate and that discards the topic's records. That is the right refusal — but it
# leaves no way to say "this topic is legitimately smaller on production", and without one, a single
# topic whose prod shape predates a dev-driven count makes apply-topics.sh exit 1 on every run. On
# 2026-10-07 that was options.spx.strike-invasion.current (prod 1 partition, declared 32 since
# 2026-09-15), and the off-hours clean-slate turned that one exit code into "recreate FAILED — mirrors
# stay PAUSED", leaving all twelve es4->prod mirrors down.
#
# It is sourced by apply-topics.sh and verify-topics.sh INSIDE the same explicit
# ENVIRONMENT=production, default-topic-set branch they already use for the other prod-only sets —
# one implementation, so the applier and the verifier cannot disagree about what prod declares.
#
# Fail closed, both ways:
#   * an override for a topic that is not in OPTIONS_EDGE_TOPICS is a REFUSAL, not a no-op: it means
#     the declaration moved and the override is now describing nothing;
#   * a count that is not a positive integer is a REFUSAL.
# It only ever REPLACES the count of an already-declared topic — it cannot add or remove a topic.
_oe_resolve_prod_partition_overrides() {
  local overrides="${OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES:-}"
  [ -n "${overrides// /}" ] || return 0

  local ov name count entry resolved="" applied=""
  for ov in $overrides; do
    case "$ov" in
      *=*) ;;
      *) echo "resolve-prod-partition-overrides: '$ov' is not topic=partitions" >&2; return 1 ;;
    esac
    name="${ov%%=*}"; count="${ov##*=}"
    # CANONICAL positive integer. A class test plus a literal "0" let `00` through, which resolved
    # the declaration to :00 and made the minimum check vacuous — every live count is >= 00.
    case "$count" in
      [1-9]|[1-9][0-9]*) ;;
      *) echo "resolve-prod-partition-overrides: $name=$count — the count must be a canonical positive integer (1, 2, 32 — not 0, 00 or 032)" >&2; return 1 ;;
    esac
    # Declared in EITHER list. apply-topics.sh merges OPTIONS_EDGE_PROD_ONLY_TOPICS into
    # OPTIONS_EDGE_TOPICS before calling this; verify-topics.sh keeps the two apart and checks the
    # prod-only set separately. Validating against only one of them meant an override for a prod-only
    # topic applied in the applier and was REFUSED in the verifier — shared code, different input.
    case " $OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-} " in
      *" $name:"*) ;;
      *) echo "resolve-prod-partition-overrides: $name is NOT declared in OPTIONS_EDGE_TOPICS or OPTIONS_EDGE_PROD_ONLY_TOPICS — an override that describes nothing is a stale declaration, not a no-op" >&2; return 1 ;;
    esac
  done

  # Both lists are rewritten, for the same reason: whichever one carries the name must come back with
  # the production count, whether the caller merged them first or keeps them apart.
  _oe_rewrite() { # list
    local out="" e n c o
    for e in $1; do
      n="${e%%:*}"; c="${e##*:}"
      for o in $overrides; do
        if [ "${o%%=*}" = "$n" ]; then c="${o##*=}"; applied="$applied $n:$c"; fi
      done
      out="$out $n:$c"
    done
    printf '%s' "${out# }"
  }
  resolved=$(_oe_rewrite "$OPTIONS_EDGE_TOPICS")
  OPTIONS_EDGE_TOPICS="$resolved"
  if [ -n "${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}" ]; then
    OPTIONS_EDGE_PROD_ONLY_TOPICS=$(_oe_rewrite "$OPTIONS_EDGE_PROD_ONLY_TOPICS")
  fi
  echo "[prod-partition-overrides] production declaration adjusted:${applied:- <none>}"
}
