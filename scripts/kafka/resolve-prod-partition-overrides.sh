#!/usr/bin/env bash
# Resolve OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES into OPTIONS_EDGE_TOPICS.
#
# WHY THIS EXISTS. topics.env declares ONE partition count per topic for the dev/prod (default) topic
# set — es4 has its own declaration, OPTIONS_EDGE_ES4_TOPICS, which does not carry this topic at
# all — and
# apply-topics.sh treats it as a MINIMUM: a live topic below it is SKIPPED (and the run exits 1)
# unless KAFKA_RECREATE_MISMATCHED_TOPICS=true. With that flag a non-exact topic is WIDENED in place
# with `kafka-topics --alter --partitions` — no records are deleted; only an EXACT-partition topic
# takes the destructive delete+recreate path. So the cost of raising this one is not data loss, it is
# ROUTING: a keyed topic's key->partition mapping is `hash(key) % partitions`, so widening sends a
# key's future records to a different partition from its history, which breaks per-key ordering for
# every consumer that relies on it (here invasion-postgres-writer) and splits a key's compaction
# lineage across two partitions. Whether to accept that is the owner's call, not this script's.
#
# What was missing either way was a way to say "this topic is legitimately smaller on production":
# without one, a single topic whose prod shape predates a dev-driven count makes apply-topics.sh exit
# 1 on every prod run. On 2026-10-07 that was options.spx.strike-invasion.current (prod 1 partition
# with 554k records, declared 32 since 2026-09-15), and the off-hours clean-slate turned that one exit
# code into "recreate FAILED — mirrors stay PAUSED", leaving all twelve es4->prod mirrors down.
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

  local ov name count applied="" _oe_seen=""
  for ov in $overrides; do
    case "$ov" in
      *=*) ;;
      *) echo "resolve-prod-partition-overrides: '$ov' is not topic=partitions" >&2; return 1 ;;
    esac
    name="${ov%%=*}"; count="${ov##*=}"
    # A REGEX, anchored, not a glob: `case` globs are not numeric validation — [1-9][0-9]* matched
    # `12garbage`, the declaration was rewritten to :12garbage, and the applier's arithmetic then
    # errored on it and could carry on treating the topic as compatible. The bound is stated rather
    # than left open: a count above it is a typo, not a declaration (Kafka would accept it and the
    # partitions would be real).
    # The LENGTH is checked before any arithmetic: `(( count > 1024 ))` on an unbounded string wraps
    # in bash's 64-bit arithmetic, so 18446744073709552640 compared as "not greater than 1024" and
    # passed the bound it was supposed to fail. Four digits cannot overflow, and 1024 partitions is
    # already far past anything this estate declares.
    if ! [[ "$count" =~ ^[1-9][0-9]{0,3}$ ]] || (( count > 1024 )); then
      echo "resolve-prod-partition-overrides: $name=$count — the count must be a canonical integer from 1 to 1024 (not 0, 00, 032, 12garbage, 99999 or 18446744073709552640)" >&2
      return 1
    fi
    # Declared in EITHER list. apply-topics.sh merges OPTIONS_EDGE_PROD_ONLY_TOPICS into
    # OPTIONS_EDGE_TOPICS before calling this; verify-topics.sh keeps the two apart and checks the
    # prod-only set separately. Validating against only one of them meant an override for a prod-only
    # topic applied in the applier and was REFUSED in the verifier — shared code, different input.
    # One entry per topic. Two entries silently applied the LAST one and wrote both into the audit
    # line, so the log said the declaration was adjusted to two different counts.
    case " $_oe_seen " in
      *" $name "*) echo "resolve-prod-partition-overrides: $name appears more than once — one override per topic, or the audit line reports a count that was not applied" >&2; return 1 ;;
    esac
    _oe_seen="$_oe_seen $name"
    case " $OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-} " in
      *" $name:"*) ;;
      *) echo "resolve-prod-partition-overrides: $name is NOT declared in OPTIONS_EDGE_TOPICS or OPTIONS_EDGE_PROD_ONLY_TOPICS — an override that describes nothing is a stale declaration, not a no-op" >&2; return 1 ;;
    esac
  done

  # Both lists are rewritten, for the same reason: whichever one carries the name must come back with
  # the production count, whether the caller merged them first or keeps them apart.
  # The rewrite writes its result to GLOBALS and never runs in a command substitution: `applied` used
  # to be appended inside $( ), i.e. in a subshell, so the audit line always said "<none>" however
  # many topics it had just rewritten — a log that cannot report what it did is worse than no log.
  _oe_rewrite() { # list-variable-name
    local __var="$1" out="" e n c o
    for e in ${!__var}; do
      n="${e%%:*}"; c="${e##*:}"
      for o in $overrides; do
        if [ "${o%%=*}" = "$n" ]; then c="${o##*=}"; applied="$applied $n:$c"; fi
      done
      out="$out $n:$c"
    done
    printf -v "$__var" '%s' "${out# }"
  }
  _oe_rewrite OPTIONS_EDGE_TOPICS
  if [ -n "${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}" ]; then
    _oe_rewrite OPTIONS_EDGE_PROD_ONLY_TOPICS
  fi
  echo "[prod-partition-overrides] production declaration adjusted:${applied:- <none>}"
}
