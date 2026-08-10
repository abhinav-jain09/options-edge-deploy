#!/usr/bin/env bash
# validate-mirrored-topic-contracts.sh — CI invariant.
#
# Every topic an MM1 mirror job copies from es4 onto the dev/prod brokers must be DECLARED in the
# default topic set of scripts/kafka/topics.env, with the SAME shape the mirror job states.
#
# WHY THIS EXISTS. A mirrored topic has two owners that each write its shape, and until they were
# forced to agree they silently did not:
#
#   * cleanup-topics.sh with KAFKA_DELETE_UNWANTED_TOPICS=true deletes every topic that is neither
#     matched by PROTECTED_TOPIC_REGEX nor present in the approved list built from the declared set.
#     An undeclared mirror target is neither, so it was a deletion candidate — and the mirror would
#     re-create it through broker auto-create, i.e. at the DEFAULT shape, which for the compacted
#     ones strips compaction and destroys the last-value semantics the pages read. That is not
#     hypothetical: on 2026-08-09 es.tape-zones.board was found already sitting on prod with no
#     cleanup.policy at all, and on 2026-08-07 dev's two es.options.indicators.* topics were
#     re-created by their own MM1 producer at dev's 32-partition default.
#   * apply-topics.sh's alter_topic_config writes retention.ms + cleanup.policy on EVERY declared
#     topic, on every deploy. So declaring a mirrored topic without its compaction or without its
#     retention is not a partial fix — it actively reconciles the mirror's contract AWAY. Both
#     halves of that were live: es.futures.aggressor-flow was declared for a fortnight while its
#     owner creates it compact,delete, and es.tape-zones.board was declared compacted but left on
#     the environment default retention while its owner asserts retention.ms=-1.
#
# The mirror jobs already fail their own installs on a shape mismatch, but an install-time
# assertion in a job nobody re-runs cannot protect a topic a deploy rewrites nightly. This makes the
# disagreement unmergeable instead.
#
# DISCOVERY IS BY GLOB, on purpose. The exposure was found by enumerating the mirror jobs by hand,
# which is exactly the step that does not happen when the NEXT single-topic mirror is added. A new
# Jenkinsfile.es-*-mirror is picked up here automatically and must bring its declaration with it.
#
# Both files are PARSED, never sourced or executed.
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPICS_ENV="scripts/kafka/topics.env"
fail=0

[ -r "$TOPICS_ENV" ] || { echo "FAIL: cannot read $TOPICS_ENV" >&2; exit 1; }

# --- the declared dev/prod set, resolved without sourcing --------------------------------------
# OPTIONS_EDGE_TOPICS is assigned more than once (a base list plus append lines of the form
# VAR="$VAR more:4"), so take the quoted value of EVERY assignment and concatenate. The es4 set
# lives in its own OPTIONS_EDGE_ES4_* variables and is deliberately NOT matched: apply-topics.sh
# swaps that set in wholesale under TOPIC_SET=es4, and these mirrors target dev/prod.
declared_entries() { sed -nE 's/^OPTIONS_EDGE_TOPICS="(.*)"$/\1/p' "$TOPICS_ENV" | tr ' ' '\n' | grep -v '^\$OPTIONS_EDGE_TOPICS$' | grep -v '^$'; }
compacted_list()   { sed -nE 's/^OPTIONS_EDGE_COMPACTED_TOPICS="(.*)"$/\1/p' "$TOPICS_ENV" | tr ' ' '\n' | grep -v '^\$OPTIONS_EDGE_COMPACTED_TOPICS$' | grep -v '^$'; }
retention_pairs()  { sed -nE 's/^OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="(.*)"$/\1/p' "$TOPICS_ENV" | tr ' ' '\n' | grep -v '^$'; }

DECLARED="$(declared_entries)"
COMPACTED="$(compacted_list)"
RETENTIONS="$(retention_pairs)"

[ -n "$DECLARED" ] || { echo "FAIL: parsed an EMPTY declared topic set from $TOPICS_ENV — the parser and the file have diverged, and every check below would pass vacuously" >&2; exit 1; }

declared_partitions() { # topic -> partition count, empty if undeclared
  printf '%s\n' "$DECLARED" | awk -F: -v t="$1" '$1 == t { print $2 }' | tail -1
}
is_compacted() { printf '%s\n' "$COMPACTED" | grep -qx "$1"; }
retention_of() { # topic -> declared override, or the literal <default> when unlisted
  local v
  v="$(printf '%s\n' "$RETENTIONS" | awk -F= -v t="$1" '$1 == t { print $2 }' | tail -1)"
  printf '%s\n' "${v:-<default>}"
}

# --- what each mirror job says ------------------------------------------------------------------
# The single topic (string param) or frozen allow-list (choice param) the job mirrors.
job_topics() {
  { sed -nE "s/.*(string|choice)\(name: 'TOPIC',.*/&/p" "$1" \
    | grep -oE "es\.[A-Za-z0-9._-]+" || true; } | sort -u
}

# The frozen per-topic contract arms: `<topic>) PARTS=1; POLICY=compact,delete; RET=-1 ;;`.
# A job may repeat its arm (the tape-zones mirror asserts the contract against the source in
# Preflight and against the target when it creates it); identical repeats collapse, and a job that
# contradicts ITSELF is reported rather than resolved by whichever copy sorts first.
job_contracts() {
  { grep -oE "es\.[A-Za-z0-9._-]+\) +PARTS=[0-9]+; *POLICY=[a-z,]+; *RET=-?[0-9]+" "$1" || true; } \
    | sed -E 's/\) +PARTS=/ /; s/; *POLICY=/ /; s/; *RET=/ /' | sort -u
}

# The cleanup.policy the job passes to `kafka-topics --create` for its target. The jobs that carry
# a frozen arm interpolate "$POLICY" there, so a literal value only appears for the jobs that do
# not — which is precisely where it is the only statement of the policy.
job_literal_policy() {
  { grep -oE -- "--config cleanup\.policy=[a-z,]+" "$1" || true; } | sed -E 's/.*cleanup\.policy=//' | sort -u
}

shopt -s nullglob
JOBS=(Jenkinsfile.es-*-mirror)
shopt -u nullglob
if [ "${#JOBS[@]}" -eq 0 ]; then
  echo "FAIL: no Jenkinsfile.es-*-mirror found — the glob and the repo layout have diverged" >&2
  exit 1
fi

checked=0
for job in "${JOBS[@]}"; do
  topics="$(job_topics "$job")"
  if [ -z "$topics" ]; then
    echo "FAIL: $job declares no TOPIC parameter this validator can read — it cannot be checked,"
    echo "      and an unchecked mirror is how this failure class started. Update the parser."
    fail=1
    continue
  fi

  contracts="$(job_contracts "$job")"
  literal_policies="$(job_literal_policy "$job")"

  for topic in $topics; do
    checked=$((checked + 1))

    # 1) DECLARED AT ALL. This is the deletion exposure: undeclared means absent from
    #    cleanup-topics.sh's approved list and unmatched by PROTECTED_TOPIC_REGEX.
    parts="$(declared_partitions "$topic")"
    if [ -z "$parts" ]; then
      echo "FAIL: $job mirrors '$topic' onto the dev/prod brokers, but it is NOT declared in"
      echo "      OPTIONS_EDGE_TOPICS in $TOPICS_ENV. cleanup-topics.sh with"
      echo "      KAFKA_DELETE_UNWANTED_TOPICS=true would delete it as unwanted, and the mirror"
      echo "      would re-create it via broker auto-create at the DEFAULT shape."
      fail=1
      continue
    fi

    # 2) THE FROZEN CONTRACT, where the job states one.
    arm="$(printf '%s\n' "$contracts" | awk -v t="$topic" '$1 == t')"
    n_arms="$(printf '%s' "$arm" | grep -c . || true)"
    if [ "$n_arms" -gt 1 ]; then
      echo "FAIL: $job states MORE THAN ONE contract for '$topic':"
      printf '%s\n' "$arm" | sed 's/^/        /'
      fail=1
      continue
    fi
    if [ "$n_arms" -eq 1 ]; then
      want_parts="$(printf '%s\n' "$arm" | awk '{print $2}')"
      want_policy="$(printf '%s\n' "$arm" | awk '{print $3}')"
      want_ret="$(printf '%s\n' "$arm" | awk '{print $4}')"

      # Partitions must be EQUAL, not merely compatible. apply-topics.sh treats a declared count as
      # a MINIMUM, so an under-declaration is silently accepted against a re-drifted topic — and for
      # a record-copy mirror the target's partition count IS the mirrored key->partition mapping.
      if [ "$parts" != "$want_parts" ]; then
        echo "FAIL: '$topic' is declared :$parts in $TOPICS_ENV but $job freezes it at $want_parts partition(s)."
        fail=1
      fi
      if [ "$(retention_of "$topic")" != "$want_ret" ]; then
        echo "FAIL: '$topic' has retention '$(retention_of "$topic")' in OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"
        echo "      but $job asserts retention.ms=$want_ret. apply-topics.sh writes retention.ms on"
        echo "      every declared topic, so the deploy would reconcile the mirror's contract away."
        fail=1
      fi
      expect_policy="$want_policy"
    else
      # No frozen arm: the create-time --config is the job's only statement of the policy. It is
      # ambiguous if the job passes several, and these jobs mirror one topic each.
      n_pol="$(printf '%s' "$literal_policies" | grep -c . || true)"
      if [ "$n_pol" -ne 1 ]; then
        echo "FAIL: $job states neither a frozen PARTS/POLICY/RET arm for '$topic' nor exactly one"
        echo "      literal --config cleanup.policy= (found ${n_pol}). Its shape cannot be checked."
        fail=1
        continue
      fi
      expect_policy="$literal_policies"
      # Retention is deliberately NOT checked here. These jobs pass a create-time value that they
      # never re-assert, so it binds only a topic that does not exist yet; the enforced value is
      # whatever topics.env declares, and the two are allowed to differ.
    fi

    # 3) COMPACTION. The trap this file's own history is full of: declaring a compacted topic
    #    without listing it makes apply-topics.sh reconcile cleanup.policy to plain delete.
    case "$expect_policy" in
      *compact*) want_compacted=yes ;;
      *)         want_compacted=no  ;;
    esac
    have_compacted=no
    is_compacted "$topic" && have_compacted=yes
    if [ "$want_compacted" != "$have_compacted" ]; then
      if [ "$want_compacted" = yes ]; then
        echo "FAIL: $job creates '$topic' with cleanup.policy=$expect_policy, but it is NOT in"
        echo "      OPTIONS_EDGE_COMPACTED_TOPICS. apply-topics.sh would reconcile it to plain"
        echo "      delete on every deploy and STRIP the compaction."
      else
        echo "FAIL: '$topic' is listed in OPTIONS_EDGE_COMPACTED_TOPICS, but $job creates it with"
        echo "      cleanup.policy=$expect_policy. Compacting an append-only topic collapses it to"
        echo "      one record per key."
      fi
      fail=1
    fi
  done
done

if [ "$fail" -ne 0 ]; then
  echo "=== validate-mirrored-topic-contracts: FAILED ==="
  exit 1
fi
echo "checked $checked mirrored topic(s) across ${#JOBS[@]} mirror job(s) against $TOPICS_ENV"
echo "=== validate-mirrored-topic-contracts: OK ==="
