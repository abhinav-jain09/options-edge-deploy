#!/usr/bin/env bash
# topic-shape.lib.sh — the ONE implementation of "a topic's declared shape and how it is applied": the
# declaration resolution (TOPIC_SET / ENVIRONMENT) and the create / wait / alter functions over topics.env.
# Sourced by scripts/kafka/apply-topics.sh (the fleet pass) and by scripts/kafka/ensure-indicator-topics.sh
# --recreate (the indicator cutover), so a recreated topic can only ever be given the shape the fleet
# pass would give it. The caller sets, before calling anything here:
#   REPLICATION_FACTOR RETENTION_MS CLEANUP_POLICY MIN_ISR   (from load-kafka-settings.sh / the environment)
#   KAFKA_BOOTSTRAP_SERVERS and the topics.env variables (source scripts/kafka/topics.env first)
# and the kafka-* CLIs on PATH. Nothing here runs at source time.

# Resolves the declaration set for TOPIC_SET (empty = dev/prod, es4) and ENVIRONMENT (production merges the
# prod-only sets and drops the prod-only uncompacted topics from the compacted list). Mutates the
# OPTIONS_EDGE_* variables in the caller's shell.
topic_shape_resolve_declarations() {
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
}

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
  if topic_is_recovery_checkpoint "$topic"; then
    extra_configs+=(--config "segment.bytes=16777216" --config "max.compaction.lag.ms=300000")
  fi
  # --if-not-exists: CreateTopics is not idempotent on the CLIENT's own retry. The Kafka admin client retries a
  # request whose reply was lost or slow; when the first attempt had in fact created the topic, the retry answers
  # TopicExistsException and kafka-topics exits 1 for a topic that was created exactly as asked. Seen on prod
  # 2026-10-13 06:05 (delta-flow-10s: ONE create reached the broker, at 06:05:09 with the declared 32 partitions,
  # the error came from the same command 1.1 s later) - the clean-slate run then left every mirror paused and prod
  # down. The caller verifies the shape (wait_for_topic_shape) and applies the declared configs
  # (alter_topic_config) right after, so a pre-existing topic of the WRONG shape is still caught there.
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$(topic_retention_ms "$topic")" \
    --config "cleanup.policy=$cleanup_policy" \
    --config "min.insync.replicas=$MIN_ISR" \
    "${extra_configs[@]}"
}

# The transactional recovery-checkpoint topics: a compacted log of large records that is rewritten every fold, which needs the small-segment
# compaction cadence (16 MiB segments, 5 min max compaction lag) or the active 1 GiB segment is never cleaned. market-structure-service's
# since its first deploy; wave-cycle-service's (ELLIOTT-FIB-CYCLE-TOOLS E-R11.3) takes "the figure the structure service uses" and so the same cadence.
topic_is_recovery_checkpoint() {
  case "$1" in
    options.spx.market-structure.recovery|options.spx.wave-cycle.recovery) return 0 ;;
  esac
  return 1
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
        rb="$(topic_retention_bytes "$topic")"; [[ -n "$rb" ]] && echo ",retention.bytes=$rb")$( \
        topic_is_recovery_checkpoint "$topic" && echo ",segment.bytes=16777216,max.compaction.lag.ms=300000")"; then
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
# the status and that line, as EXACTLY ONE line in the expected shape. Four properties make that
# sound, and each is a case in scripts/kafka/apply-topics-skip-exit-test.sh:
#   * the variable is UNSET at the top of this script, before anything runs, so no child inherits the
#     path (the path is not a secret -- it is in this script's environment for an instant and in the
#     caller's process table -- but no kafka CLI or future helper can write it by inheritance);
#   * the file is EMPTIED here, by this script, so a previous run's line is never read as this one's
#     and no caller has to remember to truncate it;
#   * an aborted run never reaches the write, so there is nothing to read;
#   * a write that FAILS aborts the run under set -e, so the caller sees a non-zero status and an
#     empty file -- the fail-closed direction.
#
# Callers that only test for non-zero -- the Jenkins stages, scripts/es4/create-es-topics.sh -- are
# unaffected: a skip is still a failure, and still fails the build.
