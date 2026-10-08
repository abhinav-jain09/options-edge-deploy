#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
# RF must be provided explicitly (Jenkins sources scripts/kafka/load-kafka-settings.sh,
# which derives it from the rendered per-environment configmap). No silent default.
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:?KAFKA_TOPIC_REPLICATION_FACTOR must be set (source scripts/kafka/load-kafka-settings.sh)}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
RECREATE_MISMATCHED="${KAFKA_RECREATE_MISMATCHED_TOPICS:-false}"

# APPLY_TOPICS_RESULT_FILE — the completion attestation (see the block above the main loop). Captured
# here and REMOVED FROM THE ENVIRONMENT immediately, before this script runs anything: an exported
# variable is inherited by every child, so leaving it exported would hand each kafka CLI the path to
# the file that attests this script's own ending (deploy Codex round 2). Nothing below reads the
# variable; write_run_result uses this shell-local copy.
RESULT_FILE="${APPLY_TOPICS_RESULT_FILE:-}"
unset APPLY_TOPICS_RESULT_FILE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"

# TOPIC_SET selects WHICH declared set to apply. Default (empty) = the SPX dev/prod set, i.e.
# byte-for-byte the previous behaviour. TOPIC_SET=es4 applies the es4 (192.168.100.4) set instead.
# es4 must NOT get the SPX set: that would create ~50 unrelated options.* topics on its broker.
TOPIC_SET="${TOPIC_SET:-}"
case "$TOPIC_SET" in
  "")
    ;;
  es4)
    : "${OPTIONS_EDGE_ES4_TOPICS:?OPTIONS_EDGE_ES4_TOPICS missing from topics.env}"
    OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_ES4_TOPICS"
    OPTIONS_EDGE_COMPACTED_TOPICS="${OPTIONS_EDGE_ES4_COMPACTED_TOPICS:-}"
    OPTIONS_EDGE_PURE_COMPACT_TOPICS="${OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS:-}"
    OPTIONS_EDGE_EXACT_PARTITION_TOPICS="${OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS:-}"
    OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="${OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES:-}"
    OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES="${OPTIONS_EDGE_ES4_TOPIC_RETENTION_BYTES_OVERRIDES:-}"
    ;;
  *)
    echo "Unknown TOPIC_SET '$TOPIC_SET' (expected empty for dev/prod, or 'es4')" >&2
    exit 1
    ;;
esac

# --- PROD-ONLY topic sets (VIX feed separation design §6 — prod-scoped, fail-closed) ---
# Exact predicate (r2 finding 7): the prod-only sets are merged iff ENVIRONMENT is
# EXPLICITLY 'production' AND TOPIC_SET is the default (non-es4) set. If ENVIRONMENT is
# unset/empty this fails CLOSED: the prod-only sets are SKIPPED with a loud notice. We
# never rely on load-kafka-settings.sh's default-to-production and never infer the
# environment from a broker address. Dev's topic set and policies are untouched.
if [[ -n "$TOPIC_SET" ]]; then
  echo "[apply-topics] NOTICE: TOPIC_SET='$TOPIC_SET' — prod-only topic sets (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>}) do NOT apply to this topic set and were skipped (VIX feed separation §6: default SPX set on production only)."
fi
if [[ -z "$TOPIC_SET" ]]; then
  if [[ "${ENVIRONMENT:-}" == "production" ]]; then
    echo "[apply-topics] ENVIRONMENT=production: merging prod-only topic sets (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>})"
    OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"
    OPTIONS_EDGE_PURE_COMPACT_TOPICS="${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-} ${OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS:-}"
    OPTIONS_EDGE_EXACT_PARTITION_TOPICS="${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-} ${OPTIONS_EDGE_PROD_ONLY_EXACT_PARTITION_TOPICS:-}"
    OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-} ${OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES:-}"
    # Partition overrides are a REPLACEMENT, not an addition: the merges above can only ADD, and a
    # topic that is legitimately SMALLER on prod than the single declared count has no other lever —
    # the count is read as a minimum, so the topic is refused on every run and (through the
    # clean-slate wrapper) takes the es4->prod mirror set down with it. One resolver, shared with
    # verify-topics.sh, so the applier and the verifier cannot disagree about what prod declares.
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/resolve-prod-partition-overrides.sh"
    _oe_resolve_prod_partition_overrides || exit 1
    # Prod-only SUBTRACTION (archive fidelity, 2026-08-25). The merges above only ever ADD, so a
    # topic that must stop being compacted on prod without changing dev/es4 has no other lever:
    # deleting it from OPTIONS_EDGE_COMPACTED_TOPICS would change every environment. This runs
    # inside the SAME explicit ENVIRONMENT=production branch, so it fails CLOSED just like them.
    # alter_topic_config() re-applies cleanup.policy to EXISTING topics on every run, so a name
    # listed here is actively set back to `delete` — the declaration, not a hand-run
    # `kafka-configs --alter`, is what makes it survive the next bring-up.
    if [[ -n "${OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS:-}" ]]; then
      _oe_kept=""
      _oe_dropped=0
      for _oe_t in ${OPTIONS_EDGE_COMPACTED_TOPICS:-}; do
        _oe_drop=0
        for _oe_u in ${OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS}; do
          if [[ "$_oe_t" == "$_oe_u" ]]; then _oe_drop=1; break; fi
        done
        if [[ "$_oe_drop" -eq 1 ]]; then
          _oe_dropped=$((_oe_dropped + 1))
        else
          _oe_kept="$_oe_kept $_oe_t"
        fi
      done
      OPTIONS_EDGE_COMPACTED_TOPICS="${_oe_kept# }"
      echo "[apply-topics] ENVIRONMENT=production: compaction DROPPED for $_oe_dropped topic(s) (archive fidelity); still compacted: ${OPTIONS_EDGE_COMPACTED_TOPICS:-<none>}"
      unset _oe_kept _oe_dropped _oe_drop _oe_t _oe_u
    fi
  else
    echo "=================================================================================="
    echo "[apply-topics] NOTICE: PROD-ONLY topic sets SKIPPED (fail-closed)."
    echo "  ENVIRONMENT='${ENVIRONMENT:-<unset>}' is not EXPLICITLY 'production'."
    echo "  The prod-only declarations (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>} + policy/"
    echo "  retention memberships) were NOT applied. If this IS a production run, export"
    echo "  ENVIRONMENT=production explicitly — the load-kafka-settings.sh default is"
    echo "  deliberately not trusted here (VIX feed separation design §6)."
    echo "=================================================================================="
  fi
fi

# NEVER-RECREATE guard (VFS-R8): topics whose records must never be destroyed by the
# destructive exact-partition repair, no matter what operator flags are set.
topic_never_recreate() {
  local topic="$1" t
  for t in ${OPTIONS_EDGE_NEVER_RECREATE_TOPICS:-}; do
    [[ "$topic" == "$t" ]] && return 0
  done
  return 1
}

describe_topic() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null | sed -n '/^Topic:/p' || true
}

broker_ids() {
  kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" 2>/dev/null \
    | sed -n 's/.*(id: \([0-9][0-9]*\).*/\1/p' \
    | sort -n \
    | uniq
}

# Preflight for the main loop (Codex review, 2026-10-06): reassign_topic_replication_factor's own
# insufficient-broker check runs AFTER a partition count has already been widened (the caller does
# partition-alter, THEN RF-reassign). Checking here first catches the common case — broker count
# already insufficient before this topic is touched at all — without widening its partitions.
# NOT a full guarantee: Kafka cannot shrink a partition count back down, so a broker that drops
# between this check and the actual reassignment call (or any OTHER reassignment failure — a
# network blip, a timeout) still leaves the topic widened with its old replication factor. That
# narrow window is a pre-existing property of this being two non-transactional admin calls with no
# undo for the first one; it is no worse than the original script, which hit the exact same partial
# state on any post-widen failure and simply never reported it because it exited immediately after.
# What this changeset guarantees, unconditionally: the run no longer abandons every OTHER topic
# over this one's outcome either way.
enough_brokers_for_rf() {
  local -a _brokers
  mapfile -t _brokers < <(broker_ids)
  (( ${#_brokers[@]} >= REPLICATION_FACTOR ))
}

wait_for_topic_absent() {
  local topic="$1"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    if [[ -z "$(describe_topic "$topic")" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  describe_topic "$topic"
  return 1
}

wait_for_topic_shape() {
  local topic="$1"
  local expected_partitions="$2"
  local expected_replication_factor="$3"
  local attempts="${KAFKA_TOPIC_REPAIR_WAIT_SECONDS:-90}"

  for ((i = 1; i <= attempts; i++)); do
    description="$(describe_topic "$topic")"
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"
    if [[ "$current_partitions" == "$expected_partitions" && "$current_replication_factor" == "$expected_replication_factor" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for topic $topic to become partitions=$expected_partitions replicationFactor=$expected_replication_factor" >&2
  describe_topic "$topic"
  return 1
}

reassign_topic_replication_factor() {
  local topic="$1"
  local partitions="$2"
  local tmp
  mapfile -t brokers < <(broker_ids)

  if (( ${#brokers[@]} < REPLICATION_FACTOR )); then
    echo "Cannot assign replication factor $REPLICATION_FACTOR with only ${#brokers[@]} brokers" >&2
    # Returns to the caller rather than exiting the whole script (Codex review, 2026-10-06): this
    # is a per-topic repair step inside the main loop, and an insufficient-broker-count condition
    # on ONE topic's RF repair must not abandon every topic still waiting after it — the same
    # defect this changeset exists to close.
    return 1
  fi

  tmp="$(mktemp)"
  {
    printf '{"version":1,"partitions":['
    for ((partition = 0; partition < partitions; partition++)); do
      if (( partition > 0 )); then
        printf ','
      fi
      printf '{"topic":"%s","partition":%d,"replicas":[' "$topic" "$partition"
      for ((replica = 0; replica < REPLICATION_FACTOR; replica++)); do
        if (( replica > 0 )); then
          printf ','
        fi
        printf '%s' "${brokers[$(((partition + replica) % ${#brokers[@]}))]}"
      done
      printf ']}'
    done
    printf ']}\n'
  } > "$tmp"

  kafka-reassign-partitions --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --reassignment-json-file "$tmp" \
    --execute
  # Codex review, 2026-10-06: `rm -f` was the LAST command in this function, so its (near-always 0)
  # exit status silently became the function's own return value — a genuine --execute failure was
  # masked as success, and the caller would then call wait_for_topic_shape on a reassignment that
  # never happened. Capture --execute's status explicitly, clean up unconditionally, return the
  # CAPTURED status.
  local rc=$?
  rm -f "$tmp"
  return "$rc"
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  local cleanup_policy
  cleanup_policy="$(topic_cleanup_policy "$topic")"

  local delete_retention
  delete_retention="$(topic_delete_retention_ms "$topic")"
  local extra_configs=()
  if [[ -n "$delete_retention" ]]; then
    extra_configs+=(--config "delete.retention.ms=$delete_retention")
  fi
  local retention_bytes
  retention_bytes="$(topic_retention_bytes "$topic")"
  if [[ -n "$retention_bytes" ]]; then
    extra_configs+=(--config "retention.bytes=$retention_bytes")
  fi
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$(topic_retention_ms "$topic")" \
    --config "cleanup.policy=$cleanup_policy" \
    --config "min.insync.replicas=$MIN_ISR" \
    "${extra_configs[@]}"
}

topic_cleanup_policy() {
  local topic="$1"
  # PURE-compact topics keep the latest value per key FOREVER. They must not get the
  # "compact,delete" policy, because the delete half would drop the record once the
  # global retention elapses (spx.basis.state holds the ES-SPX anchor: losing it puts
  # the basis engine back to UNAVAILABLE and fail-closes the ES-on-SPX overlay).
  for pure_topic in ${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-}; do
    if [[ "$topic" == "$pure_topic" ]]; then
      echo "compact"
      return
    fi
  done
  for compacted_topic in ${OPTIONS_EDGE_COMPACTED_TOPICS:-}; do
    if [[ "$topic" == "$compacted_topic" ]]; then
      echo "${KAFKA_COMPACTED_TOPIC_CLEANUP_POLICY:-compact,delete}"
      return
    fi
  done
  echo "$CLEANUP_POLICY"
}

# Topics whose partition count must be EXACT, not "at least". The default policy treats extra
# partitions as harmless (more parallelism), but a topic read via an explicit
# assign(TopicPartition(topic, 0)) is only correct at exactly 1 partition — any record that hashes
# to another partition is invisible. Kafka cannot shrink a topic, so repairing these is DESTRUCTIVE
# (delete + recreate) and therefore still gated behind KAFKA_RECREATE_MISMATCHED_TOPICS=true.
topic_requires_exact_partitions() {
  local topic="$1" t
  for t in ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-}; do
    [[ "$topic" == "$t" ]] && return 0
  done
  return 1
}

# Per-topic delete.retention.ms override (tombstone survival for compacted control topics,
# e.g. "options.ibkr.gex.status=172800000"). Empty for unlisted topics (broker default).
topic_delete_retention_ms() {
  local topic="$1" entry
  for entry in ${OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES:-}; do
    if [[ "${entry%%=*}" == "$topic" ]]; then
      echo "${entry#*=}"
      return
    fi
  done
  echo ""
}

# Per-topic retention.BYTES override, e.g. "es.futures.footprint.strike=-1". Empty for unlisted topics
# (broker default): retention.ms=-1 alone does not stop byte-based deletion (deploy Codex round 2).
topic_retention_bytes() {
  local topic="$1" entry
  for entry in ${OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES:-}; do
    if [[ "${entry%%=*}" == "$topic" ]]; then
      echo "${entry#*=}"
      return
    fi
  done
  echo ""
}

# Per-topic retention override (ms), e.g. "spx.basis.state=-1". Unlisted topics use $RETENTION_MS.
topic_retention_ms() {
  local topic="$1" entry
  for entry in ${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-}; do
    if [[ "${entry%%=*}" == "$topic" ]]; then
      echo "${entry#*=}"
      return
    fi
  done
  echo "$RETENTION_MS"
}

kafka_config_value() {
  local value="$1"
  if [[ "$value" == \[*\] ]]; then
    echo "$value"
  elif [[ "$value" == *,* ]]; then
    echo "[$value]"
  else
    echo "$value"
  fi
}

alter_topic_config() {
  local topic="$1"
  local cleanup_policy="$2"
  local attempts="${KAFKA_TOPIC_CONFIG_RETRY_ATTEMPTS:-10}"

  for ((i = 1; i <= attempts; i++)); do
    if kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --entity-type topics --entity-name "$topic" --alter \
      --add-config "retention.ms=$(topic_retention_ms "$topic"),cleanup.policy=$(kafka_config_value "$cleanup_policy"),min.insync.replicas=$MIN_ISR$( \
        dr="$(topic_delete_retention_ms "$topic")"; [[ -n "$dr" ]] && echo ",delete.retention.ms=$dr")$( \
        rb="$(topic_retention_bytes "$topic")"; [[ -n "$rb" ]] && echo ",retention.bytes=$rb")"; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out updating config for topic $topic" >&2
  return 1
}

# SKIPPED_TOPICS accumulates every topic this run could not reconcile, instead of the OLD
# behaviour of `exit 1` on the FIRST one and silently abandoning every topic after it in
# $OPTIONS_EDGE_TOPICS. On 2026-10-06 a single pre-existing drifted topic
# (options.spx.strike-invasion.current, 1 partition vs. a declared 32) aborted this script partway
# through a 148-topic list; the 27 topics after it — including es.futures.cvd.levels,
# underlying.es.trades.linearized, spx.drop.nowcast — were never created, and that surfaced an hour
# later as four UNRELATED-LOOKING service crash-loops on prod with no single error pointing back
# here. The safety property this script exists to enforce (never silently destroy a mismatched
# topic's data) is preserved: every skip below is still reported, and the script still exits NON-ZERO
# at the end if anything was skipped. What changes is that skipping topic N no longer skips N+1..last.
#
# AND THE EXIT CODE SAYS WHICH OF THE TWO HAPPENED (2026-10-08). "Some topics were skipped, the rest of
# the list was reconciled" and "this run fell over" were both exit 1, so no caller could tell them
# apart. On 2026-10-07 scripts/ops/prod-clean-slate.sh read the skip exit as "the recreate is unusable"
# and left all twelve es4->prod mirrors paused over ONE drifted topic that no mirror produces into.
#
#   exit 0                        every declared topic reconciled
#   exit $SKIPPED_EXIT (below)    the run reached the END of the list; the named topics could not be
#                                 reconciled and every other declared topic WAS created/updated
#   any other non-zero            the run did NOT complete -- set -e aborted it, or a precondition
#                                 refused it. Nothing may be inferred about the rest of the list.
#
# THE STATUS ALONE IS NOT PROOF OF COMPLETION, and must not be treated as such (deploy Codex round 1):
# under `set -e` a child process that itself exits 9 -- a kafka CLI, a future helper -- aborts this
# script mid-loop WITH STATUS 9, which is indistinguishable from the skip ending. Nor is the stdout
# line below proof: it shares a stream with every child's output.
#
# So completion is attested OUT OF BAND, in a file only this script's endings write:
#
#   APPLY_TOPICS_RESULT_FILE=<path>   optional. When set, this script writes exactly one line to it,
#                                     AFTER the declared list has been walked to the end:
#                                       apply-topics: state=ok skipped=
#                                       apply-topics: state=skipped skipped=<name> <name> ...
#
# A caller that acts on a partial reconciliation (scripts/ops/prod-clean-slate.sh) must require BOTH
# the status and that line, as EXACTLY ONE line in the expected shape. The variable is unset at the top
# of this script, before anything runs, so no child inherits the path; an aborted run never reaches the
# write; and a write that FAILS aborts the run under set -e, so such a caller sees a non-zero status
# and an empty file -- the fail-closed direction. (The path is not a secret: it is in this script's own
# environment for an instant and in the caller's process table. What the unset buys is that no child
# can write it by inheriting the variable, which is how a kafka CLI or a future helper would.)
#
# Callers that only test for non-zero -- the Jenkins stages, scripts/es4/create-es-topics.sh -- are
# unaffected: a skip is still a failure, and still fails the build.
SKIPPED_EXIT=9

# One line, written only from the two endings at the bottom of this file. Truncating (>) rather than
# appending keeps a reused path honest: a caller that pre-creates the file EMPTY therefore reads "no
# attestation" from every run that did not reach an ending.
write_run_result() { # <state> <space-separated skipped names>
  [ -n "${RESULT_FILE:-}" ] || return 0
  if ! printf 'apply-topics: state=%s skipped=%s\n' "$1" "$2" > "$RESULT_FILE"; then
    echo "apply-topics.sh: could not write the result file $RESULT_FILE" >&2
    return 1
  fi
}
SKIPPED_TOPICS=()

for entry in $OPTIONS_EDGE_TOPICS; do
  topic="${entry%%:*}"
  partitions="${entry##*:}"
  cleanup_policy="$(topic_cleanup_policy "$topic")"
  description="$(describe_topic "$topic")"
  if [[ -n "$description" ]]; then
    current_partitions="$(echo "$description" | head -1 | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    current_replication_factor="$(echo "$description" | head -1 | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p')"

    if [[ -z "$current_partitions" || -z "$current_replication_factor" ]]; then
      echo "Cannot parse current topic shape for $topic" >&2
      echo "$description" >&2
      SKIPPED_TOPICS+=("$topic (unparseable shape)")
      continue
    fi

    exact_partition_mismatch=false
    if topic_requires_exact_partitions "$topic" && (( current_partitions != partitions )); then
      exact_partition_mismatch=true
    fi

    if [[ "$exact_partition_mismatch" == "true" ]]; then
      # NEVER-RECREATE (VIX feed separation design §6): a global cleanup flag
      # (KAFKA_CLEANUP_TOPICS -> KAFKA_RECREATE_MISMATCHED_TOPICS) must NOT be able to
      # delete+recreate these topics — that would destroy the compacted price history.
      # Hard error EVEN IF KAFKA_RECREATE_MISMATCHED_TOPICS=true; repairing a listed
      # topic requires an explicit, human-run migration, never this script. It does NOT,
      # however, require abandoning every OTHER topic still waiting in this loop.
      if topic_never_recreate "$topic"; then
        echo "HARD ERROR: topic $topic has partitions=$current_partitions but requires EXACTLY $partitions," >&2
        echo "and it is listed in OPTIONS_EDGE_NEVER_RECREATE_TOPICS: the destructive delete+recreate repair" >&2
        echo "is FORBIDDEN for this topic even with KAFKA_RECREATE_MISMATCHED_TOPICS=true (its compacted" >&2
        echo "records must never be destroyed by a global operator flag). Fix this topic manually." >&2
        SKIPPED_TOPICS+=("$topic (NEVER-RECREATE, manual migration required)")
        continue
      fi
      if [[ "$RECREATE_MISMATCHED" != "true" ]]; then
        echo "Topic $topic exists with partitions=$current_partitions but requires EXACTLY $partitions (exact-partition contract: fixed assign() reads or fixed key->partition mapping)." >&2
        echo "Kafka cannot shrink partitions: this needs a destructive delete+recreate." >&2
        echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
        echo "NOTE: recreating $topic DISCARDS its records — any bootstrap state it held must be re-seeded afterwards." >&2
        SKIPPED_TOPICS+=("$topic (exact-partition mismatch: $current_partitions vs $partitions, needs destructive recreate)")
        continue
      fi
      echo "Repairing EXACT-partition topic $topic: partitions=$current_partitions -> $partitions (destructive delete+recreate; records discarded)"
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic"
      wait_for_topic_absent "$topic"
      create_topic "$topic" "$partitions"
      wait_for_topic_shape "$topic" "$partitions" "$REPLICATION_FACTOR"
    elif (( current_partitions < partitions )) || (( current_replication_factor < REPLICATION_FACTOR )); then
      if [[ "$RECREATE_MISMATCHED" != "true" ]]; then
        if (( current_partitions < partitions )); then
          echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected at least partitions=$partitions replicationFactor=$REPLICATION_FACTOR" >&2
          echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
          SKIPPED_TOPICS+=("$topic (partitions=$current_partitions below declared minimum $partitions)")
          continue
        fi
        echo "Topic $topic exists with partitions=$current_partitions replicationFactor=$current_replication_factor; expected at least replicationFactor=$REPLICATION_FACTOR" >&2
        echo "Set KAFKA_RECREATE_MISMATCHED_TOPICS=true only for approved destructive cleanup deployments." >&2
        SKIPPED_TOPICS+=("$topic (replicationFactor=$current_replication_factor below required $REPLICATION_FACTOR)")
        continue
      fi

      if [[ "$current_replication_factor" != "$REPLICATION_FACTOR" ]] && ! enough_brokers_for_rf; then
        echo "Topic $topic needs replicationFactor=$REPLICATION_FACTOR but the cluster does not have enough brokers right now." >&2
        echo "Skipping BEFORE any change (partition widen included) so this topic is not left half-migrated." >&2
        SKIPPED_TOPICS+=("$topic (insufficient brokers for replicationFactor=$REPLICATION_FACTOR)")
        continue
      fi

      echo "Repairing mismatched topic $topic: partitions=$current_partitions replicationFactor=$current_replication_factor -> partitions=$partitions replicationFactor=$REPLICATION_FACTOR"
      desired_partitions="$(printf '%s\n' "$current_partitions" "$partitions" | sort -n | tail -1)"
      if (( current_partitions < partitions )); then
        kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
          --alter \
          --topic "$topic" \
          --partitions "$partitions"
      fi
      if [[ "$current_replication_factor" != "$REPLICATION_FACTOR" ]]; then
        if ! reassign_topic_replication_factor "$topic" "$desired_partitions"; then
          SKIPPED_TOPICS+=("$topic (replication-factor reassignment failed, see log above)")
          continue
        fi
      fi
      wait_for_topic_shape "$topic" "$desired_partitions" "$REPLICATION_FACTOR"
    else
      echo "Topic $topic already exists with compatible partitions=$current_partitions expectedMinimum=$partitions replicationFactor=$REPLICATION_FACTOR"
    fi
  else
    create_topic "$topic" "$partitions"
    wait_for_topic_shape "$topic" "$partitions" "$REPLICATION_FACTOR"
  fi

  alter_topic_config "$topic" "$cleanup_policy"
done

if (( ${#SKIPPED_TOPICS[@]} > 0 )); then
  echo "" >&2
  echo "apply-topics.sh: ${#SKIPPED_TOPICS[@]} topic(s) could NOT be reconciled (every OTHER declared" >&2
  echo "topic above this line WAS still created/updated — this run does not abandon the rest of the" >&2
  echo "list over one drifted topic):" >&2
  for t in "${SKIPPED_TOPICS[@]}"; do echo "  - $t" >&2; done
  # The same facts once more, parseable: the NAMES alone, since the human list above carries a
  # free-text reason in parentheses after each one. Each entry's name is its first whitespace-delimited
  # field, which is how it is built above.
  #
  # On stdout for a reader; in APPLY_TOPICS_RESULT_FILE for a CALLER, which is the only one of the two
  # a child process cannot produce. The write comes first, and a failed write aborts under set -e.
  write_run_result skipped "${SKIPPED_TOPICS[*]%% *}"
  echo "apply-topics.sh: SKIPPED_TOPIC_NAMES:$(printf ' %s' "${SKIPPED_TOPICS[@]%% *}")"
  exit "$SKIPPED_EXIT"
fi

# The OTHER ending: the whole declared list reconciled. Attested the same way, so a caller reading the
# file sees which of the two endings ran rather than inferring it from a status.
write_run_result ok ''
