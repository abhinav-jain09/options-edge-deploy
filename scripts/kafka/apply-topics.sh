#!/usr/bin/env bash
set -euo pipefail
# APPLY_TOPICS_RESULT_FILE — the completion attestation (see the block above the main loop). Captured
# here and REMOVED FROM THE ENVIRONMENT immediately, before this script runs anything: an exported
# variable is inherited by every child, so leaving it exported would hand each kafka CLI the path to
# the file that attests this script's own ending (deploy Codex round 2). Nothing below reads the
# variable; write_run_result uses this shell-local copy.
RESULT_FILE="${APPLY_TOPICS_RESULT_FILE:-}"
unset APPLY_TOPICS_RESULT_FILE
# Emptied HERE, by this script, before anything else happens: a caller that reuses a path (or forgets
# to truncate it) would otherwise have a PREVIOUS run's attestation read as this one's. Refusing when
# it cannot be emptied is the fail-closed direction -- better no run than an unattestable one.
if [ -n "$RESULT_FILE" ] && ! : > "$RESULT_FILE"; then
  echo "apply-topics.sh: cannot empty the result file $RESULT_FILE" >&2
  exit 1
fi

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
# RF must be provided explicitly (Jenkins sources scripts/kafka/load-kafka-settings.sh,
# which derives it from the rendered per-environment configmap). No silent default.
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:?KAFKA_TOPIC_REPLICATION_FACTOR must be set (source scripts/kafka/load-kafka-settings.sh)}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
MIN_ISR="${KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS:-1}"
RECREATE_MISMATCHED="${KAFKA_RECREATE_MISMATCHED_TOPICS:-false}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"

# TOPIC_SET selects WHICH declared set to apply. Default (empty) = the SPX dev/prod set, i.e.
# byte-for-byte the previous behaviour. TOPIC_SET=es4 applies the es4 (192.168.100.4) set instead.
# es4 must NOT get the SPX set: that would create ~50 unrelated options.* topics on its broker.
# The shape functions and the declaration resolution are shared with ensure-indicator-topics.sh --recreate
# (scripts/kafka/topic-shape.lib.sh): one implementation, so a recreated topic gets exactly the fleet's shape.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topic-shape.lib.sh"
topic_shape_resolve_declarations

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
