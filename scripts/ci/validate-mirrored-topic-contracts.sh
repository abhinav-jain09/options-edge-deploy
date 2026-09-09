#!/usr/bin/env bash
# validate-mirrored-topic-contracts.sh — CI invariant.
#
# Every topic an MM1 mirror job copies between two brokers must be DECLARED in the topic set of the
# cluster it lands on, at the shape its mirror job states — and in the SOURCE cluster's set too.
#
# DIRECTION IS DECLARED, NOT ASSUMED. Until 2026-09-09 every mirror ran es4 -> dev/prod, and this
# script simply assumed that: it checked the default set as the target and es4 as the source, and
# its own discovery glob (`Jenkinsfile.es-*-mirror`) and topic pattern (`es.` prefix) encoded the
# same assumption three more times. The definition-enumeration mirror runs the other way — prod ->
# es4, carrying a topic with no `es.` prefix, because the SPX feed produces it and the auction desk
# on .4 consumes it. Under the old code that job matched no glob, exported no readable topic and was
# checked by NOTHING, which is exactly the failure class this file was written for. So each job now
# states its direction in a `MIRROR-DIRECTION:` marker and a job without one is an error.
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
# PURE-compact is a DIFFERENT policy from compacted, not a stronger flavour of it:
# apply-topics.sh's topic_cleanup_policy() writes "compact" for a pure-compact topic and
# "compact,delete" for a merely-compacted one. Comparing only a compacted boolean approved a
# mirror that freezes POLICY=compact against a declaration that reconciles to compact,delete —
# whose delete half ages out the very record the U16 producer reads back at startup.
PURE_COMPACT="$(list_of OPTIONS_EDGE_PURE_COMPACT_TOPICS)
$(list_of OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS)"
EXACT_PARTITION="$(list_of OPTIONS_EDGE_EXACT_PARTITION_TOPICS)
$(list_of OPTIONS_EDGE_PROD_ONLY_EXACT_PARTITION_TOPICS)"
ES4_DECLARED="$(list_of OPTIONS_EDGE_ES4_TOPICS)"
ES4_COMPACTED="$(list_of OPTIONS_EDGE_ES4_COMPACTED_TOPICS)"
ES4_PURE_COMPACT="$(list_of OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS)"
ES4_EXACT_PARTITION="$(list_of OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS)"
ES4_RETENTIONS="$(list_of OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES)"

# Fail closed on a parser that has drifted from the file, so no check below can pass vacuously.
#
# An empty list has two very different meanings and they must not be conflated:
#   * the parser no longer matches the file  -> every check below passes vacuously; fail.
#   * the list is DECLARED and deliberately empty -> a real policy, and refusing it would force a
#     dummy entry back into a set that is supposed to be empty.
# es4 and prod take the archive and compact nothing served, so their compaction lists are empty by
# design; dev still compacts. So emptiness is allowed only when the assignment is actually present
# in topics.env — a vanished variable still fails, which is the drift this guard was written for.
declared_in_file() { grep -qE "^$1=\"" "$TOPICS_ENV"; }

may_be_empty_when_declared() {
  case "$1" in
    COMPACTED)     declared_in_file OPTIONS_EDGE_COMPACTED_TOPICS ;;
    ES4_COMPACTED) declared_in_file OPTIONS_EDGE_ES4_COMPACTED_TOPICS ;;
    *)             return 1 ;;
  esac
}

for v in DECLARED COMPACTED RETENTIONS PURE_COMPACT EXACT_PARTITION \
         ES4_DECLARED ES4_COMPACTED ES4_PURE_COMPACT ES4_EXACT_PARTITION ES4_RETENTIONS; do
  [ -n "${!v}" ] && continue
  if may_be_empty_when_declared "$v"; then
    echo "NOTE: $v is declared and deliberately empty (nothing served is compacted in that set)"
    continue
  fi
  echo "FAIL: parsed an EMPTY $v from $TOPICS_ENV — the parser and the file have diverged, and the checks below would pass vacuously" >&2
  exit 1
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
check_unique OPTIONS_EDGE_PURE_COMPACT_TOPICS      "$PURE_COMPACT"   's/^//'
check_unique OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS  "$ES4_PURE_COMPACT" 's/^//'
check_unique OPTIONS_EDGE_EXACT_PARTITION_TOPICS   "$EXACT_PARTITION" 's/^//'
check_unique OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS "$ES4_EXACT_PARTITION" 's/^//'
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

# --- the two clusters, addressed by name --------------------------------------------------------
# `default` is the dev/prod set apply-topics.sh uses normally; `es4` is the set it swaps in wholesale
# under TOPIC_SET=es4. A mirror names the two it runs between and the checks below read the right
# one for each role, instead of the roles being fixed by the order they were written in.
set_list() { # set name, role -> the list for that (set, role)
  case "$1/$2" in
    default/declared) printf '%s' "$DECLARED" ;;
    default/compacted) printf '%s' "$COMPACTED" ;;
    default/pure) printf '%s' "$PURE_COMPACT" ;;
    default/exact) printf '%s' "$EXACT_PARTITION" ;;
    default/retentions) printf '%s' "$RETENTIONS" ;;
    es4/declared) printf '%s' "$ES4_DECLARED" ;;
    es4/compacted) printf '%s' "$ES4_COMPACTED" ;;
    es4/pure) printf '%s' "$ES4_PURE_COMPACT" ;;
    es4/exact) printf '%s' "$ES4_EXACT_PARTITION" ;;
    es4/retentions) printf '%s' "$ES4_RETENTIONS" ;;
    *) echo "FAIL: unknown topic set '$1' (role $2)" >&2; exit 1 ;;
  esac
}
set_var() { # set name, role -> the topics.env variable name, for the failure messages
  case "$1/$2" in
    default/declared) echo OPTIONS_EDGE_TOPICS ;;
    default/exact) echo OPTIONS_EDGE_EXACT_PARTITION_TOPICS ;;
    default/retentions) echo OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES ;;
    es4/declared) echo OPTIONS_EDGE_ES4_TOPICS ;;
    es4/exact) echo OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS ;;
    es4/retentions) echo OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES ;;
    *) echo "OPTIONS_EDGE_?_$2" ;;
  esac
}

# The direction a job states. Deliberately REQUIRED: a job that does not say which way it runs
# cannot be checked against the right cluster, and a default would silently re-introduce the very
# assumption this replaced.
job_direction() {
  { grep -oE "^// MIRROR-DIRECTION: *(default|es4)->(default|es4)" "$1" || true; } \
    | sed -E 's|^// MIRROR-DIRECTION: *||' | sort -u
}

# --- what each mirror job says ------------------------------------------------------------------
# The frozen allow-list of topics the job may mirror. ONLY a `choice` parameter counts: a
# `string(name: 'TOPIC', defaultValue: ...)` is a free-text box, so the default is a suggestion and
# not a set. Reading the default as if it were the allow-list is how this validator could report
# full coverage while an operator typed any name they liked into the same job — which would create
# that topic on the target with THIS topic's hardcoded shape, undeclared and therefore invisible
# both to this check and to cleanup-topics.sh's approved list. Enumerating a set the job does not
# actually constrain is a coverage claim that is not true, so an unconstrained job is an error.
# The choices list may WRAP onto further lines. Reading only the PHYSICAL line that carries
# `choice(name: 'TOPIC',` is how four ES Footprint topics were added to a mirror job and then
# checked by nothing (found by Codex, fifth review pass): the validator went on reporting the three
# names that fitted on the first line and called that full coverage, so a partition/policy/retention
# mismatch on any of the four would have merged past this supposedly fail-closed guard. So the
# declaration is read from that line THROUGH the line that closes the list with `]`, and a list that
# is never closed yields the sentinel `<unterminated>` — not a topic name, so the caller refuses the
# job outright instead of proceeding on a truncated set.
job_topics() {
  { awk '
      /choice\(name: .TOPIC.,/ { inside = 1; depth = 0; seen = 0 }
      inside {
        buf = buf $0 "\n"
        # Bracket DEPTH, not "the next line that happens to contain a ]": the ACTION choice() and
        # the [^A-Za-z0-9-] character classes further down every mirror job all carry a ']', so a
        # first-]-wins scan would close an unterminated list on an unrelated line and read a
        # truncated allow-list as a complete one.
        opens = gsub(/\[/, "[")
        closes = gsub(/\]/, "]")
        depth += opens - closes
        if (opens > 0) { seen = 1 }
        if (seen && depth <= 0) { inside = 0 }
      }
      END { if (inside) print "<unterminated>"; else printf "%s", buf }
    ' "$1" | grep -oE "['][A-Za-z0-9][A-Za-z0-9._-]*[.][A-Za-z0-9._-]+[']|<unterminated>" || true; } \
    | tr -d "'" | sort -u
}
job_topic_is_freetext() { grep -qE "string\(name: 'TOPIC'," "$1"; }

# The FROZEN per-topic contract arms: `<topic>) PARTS=1; POLICY=compact,delete; RET=-1 ;;`.
# A job may repeat its arm (the tape-zones mirror asserts the contract against the source in
# Preflight and against the target when it creates it); identical repeats collapse, and a job that
# contradicts ITSELF is reported rather than resolved by whichever copy sorts first.
job_contracts() {
  { grep -oE "[A-Za-z0-9][A-Za-z0-9._-]*\.[A-Za-z0-9._-]+\) +PARTS=[0-9]+; *POLICY=[a-z,]+; *RET=-?[0-9]+" "$1" || true; } \
    | sed -E 's/\) +PARTS=/ /; s/; *POLICY=/ /; s/; *RET=/ /' | sort -u
}

# The cleanup.policy a COPIED-schema job passes to `kafka-topics --create` for its target. The
# FROZEN jobs interpolate "$POLICY" there, so a literal value only appears in the jobs that have no
# arm — which is precisely where it is the only statement of the policy.
job_literal_policy() {
  { grep -oE -- "--config cleanup\.policy=[a-z,]+" "$1" || true; } | sed -E 's/.*cleanup\.policy=//' | sort -u
}

shopt -s nullglob
JOBS=("$ROOT"/Jenkinsfile.*-mirror)
shopt -u nullglob
if [ "${#JOBS[@]}" -eq 0 ]; then
  echo "FAIL: no Jenkinsfile.*-mirror found under $ROOT — the glob and the repo layout have diverged" >&2
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
  case "$topics" in
    *"<unterminated>"*)
      echo "FAIL: $jobname opens a TOPIC choice() list that is never closed with ']' — this validator"
      echo "      can only read a bounded allow-list, and a truncated read would report coverage over"
      echo "      topics it never checked. Close the choices list."
      fail=1
      continue
      ;;
  esac
  if [ -z "$topics" ]; then
    echo "FAIL: $jobname declares no TOPIC choice() parameter this validator can read — it cannot be"
    echo "      checked, and an unchecked mirror is how this failure class started. Update the parser."
    fail=1
    continue
  fi

  direction="$(job_direction "$job")"
  n_dir="$(printf '%s' "$direction" | grep -c . || true)"
  if [ "$n_dir" -ne 1 ]; then
    echo "FAIL: $jobname does not state exactly one direction (found ${n_dir}). Add a line"
    echo "      '// MIRROR-DIRECTION: <source>-><target>' with <source>/<target> in {default, es4}."
    echo "      Without it this validator cannot tell which cluster's declarations govern the target,"
    echo "      and a default would re-introduce the assumption that every mirror runs es4 -> dev/prod."
    fail=1
    continue
  fi
  SRC_SET="${direction%%->*}"
  TGT_SET="${direction##*->}"
  if [ "$SRC_SET" = "$TGT_SET" ]; then
    echo "FAIL: $jobname mirrors '$SRC_SET' onto itself, which is not a mirror."
    fail=1
    continue
  fi
  T_DECLARED="$(set_list "$TGT_SET" declared)"; S_DECLARED="$(set_list "$SRC_SET" declared)"
  T_COMPACTED="$(set_list "$TGT_SET" compacted)"; S_COMPACTED="$(set_list "$SRC_SET" compacted)"
  T_PURE="$(set_list "$TGT_SET" pure)";          S_PURE="$(set_list "$SRC_SET" pure)"
  T_EXACT="$(set_list "$TGT_SET" exact)";        S_EXACT="$(set_list "$SRC_SET" exact)"
  T_RETENTIONS="$(set_list "$TGT_SET" retentions)"; S_RETENTIONS="$(set_list "$SRC_SET" retentions)"

  contracts="$(job_contracts "$job")"
  literal_policies="$(job_literal_policy "$job")"

  for topic in $topics; do
    checked=$((checked + 1))

    # 1) DECLARED AT ALL. This is the deletion exposure: undeclared means absent from
    #    cleanup-topics.sh's approved list and unmatched by PROTECTED_TOPIC_REGEX.
    parts="$(partitions_in "$T_DECLARED" "$topic")"
    if [ -z "$parts" ]; then
      echo "FAIL: $jobname mirrors '$topic' onto the $TGT_SET broker(s), but it is NOT declared in"
      echo "      $(set_var "$TGT_SET" declared) in $TOPICS_ENV. cleanup-topics.sh with"
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

    src_parts="$(partitions_in "$S_DECLARED" "$topic")"

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
      if [ "$(retention_in "$T_RETENTIONS" "$topic")" != "$want_ret" ]; then
        echo "FAIL: '$topic' has retention '$(retention_of "$topic")' in $(set_var "$TGT_SET" retentions)"
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
      if [ -z "$src_parts" ]; then
        echo "FAIL: $jobname copies '$topic' from $SRC_SET and takes the target's partition count FROM THE"
        echo "      SOURCE, but '$topic' is not declared in $(set_var "$SRC_SET" declared) — so no reviewed"
        echo "      partition count exists for it anywhere, on either cluster."
        fail=1
        continue
      fi
      dims="partitions=$parts(=$SRC_SET) policy=$expect_policy retention=NOT-A-CONTRACT(create-time only)"
    fi

    # 2) CLEANUP POLICY, EXACTLY. Not "is it compacted": apply-topics.sh's topic_cleanup_policy()
    #    resolves a PURE-compact declaration to "compact", a merely-compacted one to "compact,delete"
    #    (pure wins — it is checked first), and anything else to the environment default (delete).
    #    Comparing a boolean made `compact` and `compact,delete` interchangeable, which is precisely
    #    the mismatch that silently ages out a compacted-forever attestation.
    effective_policy() { # pure-list, compacted-list, topic -> the policy apply-topics.sh would write
      if in_list "$1" "$3"; then echo "compact"
      elif in_list "$2" "$3"; then echo "compact,delete"
      else echo "delete"; fi
    }
    have_policy="$(effective_policy "$T_PURE" "$T_COMPACTED" "$topic")"
    if [ "$have_policy" != "$expect_policy" ]; then
      echo "FAIL: $jobname creates '$topic' with cleanup.policy=$expect_policy, but topics.env resolves"
      echo "      it to '$have_policy' (pure-compact list: $(in_list "$T_PURE" "$topic" && echo yes || echo no),"
      echo "      compacted list: $(in_list "$T_COMPACTED" "$topic" && echo yes || echo no)). apply-topics.sh"
      echo "      rewrites cleanup.policy on every declared topic, so the deploy would reconcile the"
      echo "      mirror's contract away — and 'compact,delete' where 'compact' is frozen ages out the"
      echo "      latest value per key once retention elapses."
      fail=1
    fi
    have_compacted=no
    case "$have_policy" in *compact*) have_compacted=yes ;; esac

    # 2b) EXACT PARTITION MEMBERSHIP. A frozen PARTS=n is only enforced by the deploy if the topic is
    #     in the exact-partition set; otherwise apply-topics.sh treats the declared count as a FLOOR
    #     and a widened topic reconciles clean while breaking the mirror's key->partition mapping.
    if [ "$n_arms" -eq 1 ] && ! in_list "$T_EXACT" "$topic"; then
      echo "FAIL: $jobname freezes '$topic' at $want_parts partition(s), but it is NOT in"
      echo "      $(set_var "$TGT_SET" exact) (nor the prod-only set), so apply-topics.sh treats"
      echo "      the declared count as a MINIMUM and a widened topic would pass reconciliation."
      fail=1
    fi

    # 3) SOURCE vs TARGET, whenever the source set declares the same name. A byte-for-byte record copy whose
    #    two declarations disagree is the same defect as a target that disagrees with its job.
    if [ -n "$src_parts" ]; then
      if [ "$src_parts" != "$parts" ]; then
        echo "FAIL: '$topic' is declared :$parts on $TGT_SET but :$src_parts on $SRC_SET, and $jobname copies"
        echo "      it record-for-record. The target's partition count IS the mirrored key->partition"
        echo "      mapping, so the two declarations must agree."
        fail=1
      fi
      # The SOURCE resolves its own policy from its OWN lists (apply-topics.sh swaps the es4 set in
      # wholesale under TOPIC_SET=es4). Compare the EXACT policies, not two booleans: a FROZEN job
      # asserts POLICY against the SOURCE in its preflight, so a compact-vs-compact,delete drift here
      # hard-fails the install rather than merely disagreeing on paper.
      src_policy="$(effective_policy "$S_PURE" "$S_COMPACTED" "$topic")"
      if [ "$src_policy" != "$have_policy" ]; then
        echo "FAIL: '$topic' cleanup.policy disagrees across the clusters it is mirrored between:"
        echo "      $SRC_SET resolves '$src_policy', $TGT_SET resolves '$have_policy'."
        fail=1
      fi
      if [ "$n_arms" -eq 1 ] && [ "$src_policy" != "$expect_policy" ]; then
        echo "FAIL: '$topic' resolves to cleanup.policy='$src_policy' on $SRC_SET but $jobname freezes"
        echo "      POLICY=$expect_policy and asserts it against the SOURCE in its preflight."
        fail=1
      fi
      if [ "$n_arms" -eq 1 ] && ! in_list "$S_EXACT" "$topic"; then
        echo "FAIL: '$topic' is not in $(set_var "$SRC_SET" exact), so $SRC_SET reconciliation"
        echo "      treats :$src_parts as a MINIMUM while $jobname freezes the SOURCE at $want_parts"
        echo "      partition(s) — a widened source silently violates the record-copy contract."
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
        src_ret="$(retention_in "$S_RETENTIONS" "$topic")"
        if [ "$src_ret" != "$frozen_ret" ]; then
          echo "FAIL: '$topic' has retention '$src_ret' in $(set_var "$SRC_SET" retentions) but"
          echo "      $jobname freezes it at retention.ms=$frozen_ret, and asserts that against the SOURCE."
          echo "      create-es-topics.sh runs apply-topics.sh with TOPIC_SET=es4, so the es4 declaration"
          echo "      is what gets written to .4 — a separate list, needing the same number."
          fail=1
        fi
        dims="$dims ${SRC_SET}Retention=$src_ret"
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
