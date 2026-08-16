#!/usr/bin/env bash
# validate-mirrored-topic-contracts.sh — CI invariant.
#
# Every topic an MM1 mirror job copies from es4 onto the dev/prod brokers must be DECLARED in the
# default topic set of scripts/kafka/topics.env, at the shape its mirror job states.
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
# TWO CONTRACT SCHEMAS, BOTH EXPLICIT. The mirror jobs state their target's shape in one of two
# ways, and a job that fits NEITHER is an error rather than a topic that quietly gets checked less:
#
#   FROZEN  — the job carries a per-topic case arm `<topic>) PARTS=n; POLICY=p; RET=r ;;` and
#             asserts it. All three dimensions are authoritative and all three are checked.
#   COPIED  — the job derives the target's shape from the SOURCE at install time. There is no
#             number in the job to compare against, so the authority is es4's own declaration of
#             the same topic in topics.env: the mirror is a byte-for-byte record copy, so the
#             target's partition count IS the mirrored key->partition mapping and must equal the
#             source's. Membership of the es4 set is REQUIRED under this schema, not optional.
#             Its create-time `retention.ms` is deliberately NOT treated as a contract: --create
#             --if-not-exists only binds a topic that does not exist yet, and the job never
#             re-asserts the value, so the enforced number is whatever topics.env declares. That
#             exemption is REPORTED per topic below rather than left implicit.
#
# Whenever a topic appears in BOTH the dev/prod and es4 sets, the two are cross-checked against each
# other as well — a record copy whose source and target disagree about partitions or compaction is
# the same defect wearing different clothes.
#
# Both files are PARSED, never sourced or executed.
set -euo pipefail
cd "$(dirname "$0")/../.."

# Injectable ONLY so scripts/ci/validate-mirrored-topic-contracts-test.sh can drive this script
# against crafted fixtures. Unlike the reset keep-list's pinned path, an ambient override here is
# harmless: this script reads, reports and exits, and can destroy nothing.
ROOT="${MTC_ROOT:-.}"
TOPICS_ENV="$ROOT/scripts/kafka/topics.env"
fail=0

[ -r "$TOPICS_ENV" ] || { echo "FAIL: cannot read $TOPICS_ENV" >&2; exit 1; }

# --- the declared sets, resolved without sourcing -----------------------------------------------
# Each variable is assigned more than once (a base list plus append lines of the form
# VAR="$VAR more:4"), so take the quoted value of EVERY assignment and concatenate, dropping the
# self-reference token. The es4 set lives in its own OPTIONS_EDGE_ES4_* variables: apply-topics.sh
# swaps that set in wholesale under TOPIC_SET=es4, so the two are read separately and compared,
# never merged.
# The trailing `|| true` is what lets the explicit emptiness guard below do the talking: a grep
# that matches nothing exits 1, and under `set -e -o pipefail` that killed the whole script at the
# assignment — failing closed, but with no diagnostic at all, which is indistinguishable from a
# crash. An empty parse must be REPORTED as an empty parse.
list_of() { # variable name -> its whitespace-split values, minus the "$VAR" self-reference
  { sed -nE "s/^$1=\"(.*)\"$/\\1/p" "$TOPICS_ENV" | tr ' ' '\n' | grep -v "^\\\$$1$" | grep -v '^$'; } || true
}

# U16 (ES-CVD-SPX-LEVELS-DESIGN.md L1/M1): production-only topics are DECLARED topics too — the
# mirror install for one targets the production broker, where the PROD_ONLY sets apply; a dev
# install of a prod-only topic fails its install-time shape assert (fail-closed by design).
# PURE_COMPACT entries carry cleanup.policy=compact (no delete) and count as compacted here.
DECLARED="$(list_of OPTIONS_EDGE_TOPICS)
$(list_of OPTIONS_EDGE_PROD_ONLY_TOPICS)"
COMPACTED="$(list_of OPTIONS_EDGE_COMPACTED_TOPICS)
$(list_of OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS)"
RETENTIONS="$(list_of OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES)
$(list_of OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES)"
ES4_DECLARED="$(list_of OPTIONS_EDGE_ES4_TOPICS)"
ES4_COMPACTED="$(list_of OPTIONS_EDGE_ES4_COMPACTED_TOPICS)"
ES4_RETENTIONS="$(list_of OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES)"

# Fail closed on a parser that has drifted from the file, so no check below can pass vacuously.
for v in DECLARED COMPACTED RETENTIONS ES4_DECLARED ES4_COMPACTED ES4_RETENTIONS; do
  [ -n "${!v}" ] || { echo "FAIL: parsed an EMPTY $v from $TOPICS_ENV — the parser and the file have diverged, and the checks below would pass vacuously" >&2; exit 1; }
done

# NO TOPIC MAY BE DECLARED TWICE IN ONE LIST. This is rejected outright rather than resolved,
# because every resolution rule is a different one and they do not agree:
# apply-topics.sh's topic_retention_ms()/topic_cleanup_policy() RETURN ON THE FIRST match, while its
# main loop iterates EVERY entry of OPTIONS_EDGE_TOPICS and calls alter_topic_config once per
# occurrence — so for a duplicated topic the LAST entry decides the applied config while the FIRST
# decides the retention within each call. A validator that picked either would be reporting a number
# the deploy may not use, i.e. checking less than it claims while printing the contract back.
# There are no duplicates today; this keeps it that way instead of encoding a precedence.
dupes_in() { printf '%s\n' "$1" | sed -E "$2" | sort | uniq -d; }
check_unique() { # human name, list, sed script reducing an entry to its topic name
  local d; d="$(dupes_in "$2" "$3")"
  [ -z "$d" ] && return 0
  echo "FAIL: $1 in $TOPICS_ENV declares the same topic more than once:" >&2
  printf '%s\n' "$d" | sed 's/^/        /' >&2
  echo "      apply-topics.sh resolves a duplicate differently per call site, so no single declared" >&2
  echo "      value can be checked. Remove the duplicate." >&2
  exit 1
}
check_unique OPTIONS_EDGE_TOPICS                   "$DECLARED"      's/:[0-9]+$//'
check_unique OPTIONS_EDGE_ES4_TOPICS               "$ES4_DECLARED"  's/:[0-9]+$//'
check_unique OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES "$RETENTIONS"   's/=.*$//'
check_unique OPTIONS_EDGE_COMPACTED_TOPICS         "$COMPACTED"     's/^//'
check_unique OPTIONS_EDGE_ES4_COMPACTED_TOPICS     "$ES4_COMPACTED" 's/^//'
check_unique OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES "$ES4_RETENTIONS" 's/=.*$//'

# head -1, not tail -1: with duplicates rejected above these are single-valued, but where
# apply-topics.sh does have a precedence it is FIRST match (topic_retention_ms returns on the first
# hit), and a lookup here must never read a different entry than the deploy would.
partitions_in() { # list, topic -> partition count, empty if undeclared
  printf '%s\n' "$1" | awk -F: -v t="$2" '$1 == t { print $2 }' | head -1
}
in_list() { printf '%s\n' "$1" | grep -qx "$2"; }
retention_in() { # list, topic -> declared override, or the literal <default> when unlisted
  local v
  v="$(printf '%s\n' "$1" | awk -F= -v t="$2" '$1 == t { print $2 }' | head -1)"
  printf '%s\n' "${v:-<default>}"
}
retention_of() { retention_in "$RETENTIONS" "$1"; }

# --- what each mirror job says ------------------------------------------------------------------
# The frozen allow-list of topics the job may mirror. ONLY a `choice` parameter counts: a
# `string(name: 'TOPIC', defaultValue: ...)` is a free-text box, so the default is a suggestion and
# not a set. Reading the default as if it were the allow-list is how this validator could report
# full coverage while an operator typed any name they liked into the same job — which would create
# that topic on the target with THIS topic's hardcoded shape, undeclared and therefore invisible
# both to this check and to cleanup-topics.sh's approved list. Enumerating a set the job does not
# actually constrain is a coverage claim that is not true, so an unconstrained job is an error.
job_topics() {
  { sed -nE "s/.*choice\(name: 'TOPIC',.*/&/p" "$1" \
    | grep -oE "es\.[A-Za-z0-9._-]+" || true; } | sort -u
}
job_topic_is_freetext() { grep -qE "string\(name: 'TOPIC'," "$1"; }

# The FROZEN per-topic contract arms: `<topic>) PARTS=1; POLICY=compact,delete; RET=-1 ;;`.
# A job may repeat its arm (the tape-zones mirror asserts the contract against the source in
# Preflight and against the target when it creates it); identical repeats collapse, and a job that
# contradicts ITSELF is reported rather than resolved by whichever copy sorts first.
job_contracts() {
  { grep -oE "es\.[A-Za-z0-9._-]+\) +PARTS=[0-9]+; *POLICY=[a-z,]+; *RET=-?[0-9]+" "$1" || true; } \
    | sed -E 's/\) +PARTS=/ /; s/; *POLICY=/ /; s/; *RET=/ /' | sort -u
}

# The cleanup.policy a COPIED-schema job passes to `kafka-topics --create` for its target. The
# FROZEN jobs interpolate "$POLICY" there, so a literal value only appears in the jobs that have no
# arm — which is precisely where it is the only statement of the policy.
job_literal_policy() {
  { grep -oE -- "--config cleanup\.policy=[a-z,]+" "$1" || true; } | sed -E 's/.*cleanup\.policy=//' | sort -u
}

shopt -s nullglob
JOBS=("$ROOT"/Jenkinsfile.es-*-mirror)
shopt -u nullglob
if [ "${#JOBS[@]}" -eq 0 ]; then
  echo "FAIL: no Jenkinsfile.es-*-mirror found under $ROOT — the glob and the repo layout have diverged" >&2
  exit 1
fi

checked=0
for job in "${JOBS[@]}"; do
  jobname="$(basename "$job")"
  if job_topic_is_freetext "$job"; then
    echo "FAIL: $jobname declares TOPIC as a free-text string parameter, so the set of topics it can"
    echo "      mirror is unbounded and nothing here can cover it — an operator could run it against"
    echo "      any name, and the job would create that topic on the target with the shape hardcoded"
    echo "      for its default. Make it a choice() allow-list, as the sibling mirror jobs are."
    fail=1
    continue
  fi
  topics="$(job_topics "$job")"
  if [ -z "$topics" ]; then
    echo "FAIL: $jobname declares no TOPIC choice() parameter this validator can read — it cannot be"
    echo "      checked, and an unchecked mirror is how this failure class started. Update the parser."
    fail=1
    continue
  fi

  contracts="$(job_contracts "$job")"
  literal_policies="$(job_literal_policy "$job")"

  for topic in $topics; do
    checked=$((checked + 1))

    # 1) DECLARED AT ALL. This is the deletion exposure: undeclared means absent from
    #    cleanup-topics.sh's approved list and unmatched by PROTECTED_TOPIC_REGEX.
    parts="$(partitions_in "$DECLARED" "$topic")"
    if [ -z "$parts" ]; then
      echo "FAIL: $jobname mirrors '$topic' onto the dev/prod brokers, but it is NOT declared in"
      echo "      OPTIONS_EDGE_TOPICS in $TOPICS_ENV. cleanup-topics.sh with"
      echo "      KAFKA_DELETE_UNWANTED_TOPICS=true would delete it as unwanted, and the mirror"
      echo "      would re-create it via broker auto-create at the DEFAULT shape."
      fail=1
      continue
    fi

    arm="$(printf '%s\n' "$contracts" | awk -v t="$topic" '$1 == t')"
    n_arms="$(printf '%s' "$arm" | grep -c . || true)"
    if [ "$n_arms" -gt 1 ]; then
      echo "FAIL: $jobname states MORE THAN ONE contract for '$topic':"
      printf '%s\n' "$arm" | sed 's/^/        /'
      fail=1
      continue
    fi

    es4_parts="$(partitions_in "$ES4_DECLARED" "$topic")"

    if [ "$n_arms" -eq 1 ]; then
      # --- SCHEMA: FROZEN. All three dimensions are authoritative. --------------------------------
      schema=FROZEN
      want_parts="$(printf '%s\n' "$arm" | awk '{print $2}')"
      expect_policy="$(printf '%s\n' "$arm" | awk '{print $3}')"
      want_ret="$(printf '%s\n' "$arm" | awk '{print $4}')"

      # Partitions must be EQUAL, not merely compatible. apply-topics.sh treats a declared count as
      # a MINIMUM, so an under-declaration is silently accepted against a re-drifted topic — and for
      # a record-copy mirror the target's partition count IS the mirrored key->partition mapping.
      if [ "$parts" != "$want_parts" ]; then
        echo "FAIL: '$topic' is declared :$parts in $TOPICS_ENV but $jobname freezes it at $want_parts partition(s)."
        fail=1
      fi
      if [ "$(retention_of "$topic")" != "$want_ret" ]; then
        echo "FAIL: '$topic' has retention '$(retention_of "$topic")' in OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"
        echo "      but $jobname asserts retention.ms=$want_ret. apply-topics.sh writes retention.ms on"
        echo "      every declared topic, so the deploy would reconcile the mirror's contract away."
        fail=1
      fi
      frozen_ret="$want_ret"
      dims="partitions=$parts policy=$expect_policy retention=$want_ret"
    else
      frozen_ret=""
      # --- SCHEMA: COPIED. The source's declaration is the authority. -----------------------------
      schema=COPIED
      n_pol="$(printf '%s' "$literal_policies" | grep -c . || true)"
      if [ "$n_pol" -ne 1 ]; then
        echo "FAIL: $jobname states neither a frozen '<topic>) PARTS=..; POLICY=..; RET=..' arm for"
        echo "      '$topic' nor exactly one literal --config cleanup.policy= (found ${n_pol}), so this"
        echo "      validator cannot tell what shape it intends. Give the job a frozen arm."
        fail=1
        continue
      fi
      expect_policy="$literal_policies"

      # A COPIED job derives the target's partition count from the source, so the ONLY place a
      # reviewable number exists is es4's own declaration. Requiring it here is what keeps this
      # schema from being the weaker one: without the es4 entry there is nothing to check against,
      # and "nothing to check against" must not read as "checked".
      if [ -z "$es4_parts" ]; then
        echo "FAIL: $jobname copies '$topic' from es4 and takes the target's partition count FROM THE"
        echo "      SOURCE, but '$topic' is not declared in OPTIONS_EDGE_ES4_TOPICS — so no reviewed"
        echo "      partition count exists for it anywhere, on either cluster."
        fail=1
        continue
      fi
      dims="partitions=$parts(=es4) policy=$expect_policy retention=NOT-A-CONTRACT(create-time only)"
    fi

    # 2) COMPACTION. The trap this file's own history is full of: declaring a compacted topic
    #    without listing it makes apply-topics.sh reconcile cleanup.policy to plain delete.
    case "$expect_policy" in
      *compact*) want_compacted=yes ;;
      *)         want_compacted=no  ;;
    esac
    have_compacted=no
    in_list "$COMPACTED" "$topic" && have_compacted=yes
    if [ "$want_compacted" != "$have_compacted" ]; then
      if [ "$want_compacted" = yes ]; then
        echo "FAIL: $jobname creates '$topic' with cleanup.policy=$expect_policy, but it is NOT in"
        echo "      OPTIONS_EDGE_COMPACTED_TOPICS. apply-topics.sh would reconcile it to plain"
        echo "      delete on every deploy and STRIP the compaction."
      else
        echo "FAIL: '$topic' is listed in OPTIONS_EDGE_COMPACTED_TOPICS, but $jobname creates it with"
        echo "      cleanup.policy=$expect_policy. Compacting an append-only topic collapses it to"
        echo "      one record per key."
      fi
      fail=1
    fi

    # 3) SOURCE vs TARGET, whenever es4 declares the same name. A byte-for-byte record copy whose
    #    two declarations disagree is the same defect as a target that disagrees with its job.
    if [ -n "$es4_parts" ]; then
      if [ "$es4_parts" != "$parts" ]; then
        echo "FAIL: '$topic' is declared :$parts for dev/prod but :$es4_parts on es4, and $jobname copies"
        echo "      it record-for-record. The target's partition count IS the mirrored key->partition"
        echo "      mapping, so the two declarations must agree."
        fail=1
      fi
      es4_compacted=no
      in_list "$ES4_COMPACTED" "$topic" && es4_compacted=yes
      if [ "$es4_compacted" != "$have_compacted" ]; then
        echo "FAIL: '$topic' compaction disagrees across the clusters it is mirrored between:"
        echo "      OPTIONS_EDGE_ES4_COMPACTED_TOPICS=$es4_compacted, OPTIONS_EDGE_COMPACTED_TOPICS=$have_compacted."
        fail=1
      fi
      # THE SOURCE CARRIES THE FROZEN RETENTION TOO, and it is a SEPARATE declaration:
      # apply-topics.sh swaps OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES in wholesale under
      # TOPIC_SET=es4, which is how scripts/es4/create-es-topics.sh runs it — at
      # KAFKA_TOPIC_RETENTION_MS=43200000, so an unlisted topic is stamped 12h on .4. Checking only
      # the dev/prod list would have left the es4 half of this exact bug ungated: the tape-zones
      # mirror asserts retention.ms against the SOURCE in its Preflight, BEFORE it asserts anything
      # about the target, so an es4 deploy alone can hard-fail the install.
      if [ -n "$frozen_ret" ]; then
        es4_ret="$(retention_in "$ES4_RETENTIONS" "$topic")"
        if [ "$es4_ret" != "$frozen_ret" ]; then
          echo "FAIL: '$topic' has retention '$es4_ret' in OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES but"
          echo "      $jobname freezes it at retention.ms=$frozen_ret, and asserts that against the SOURCE."
          echo "      create-es-topics.sh runs apply-topics.sh with TOPIC_SET=es4, so the es4 declaration"
          echo "      is what gets written to .4 — a separate list, needing the same number."
          fail=1
        fi
        dims="$dims es4Retention=$es4_ret"
      fi
    fi

    echo "  $jobname  $topic  [$schema]  $dims"
  done
done

if [ "$fail" -ne 0 ]; then
  echo "=== validate-mirrored-topic-contracts: FAILED ==="
  exit 1
fi
echo "checked $checked mirrored topic(s) across ${#JOBS[@]} mirror job(s) against $TOPICS_ENV"
echo "=== validate-mirrored-topic-contracts: OK ==="
