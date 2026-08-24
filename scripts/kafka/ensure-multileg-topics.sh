#!/usr/bin/env bash
# ensure-multileg-topics.sh — the two multileg-structure OUTPUT topics must EXIST with the right
# shape before the service is allowed to produce to them.
#
# Why a deploy barrier (same reason as the OI-anchor and indicator stages in
# Jenkinsfile.service-deploy): apply-topics lives in a DIFFERENT job, so on a first deploy these
# topics do not exist and the broker auto-creates them with cluster defaults. Two specific
# consequences here:
#
#   * The service reads the STATUS topic's partition count at boot and refuses to run if the topic
#     has none — so a missing topic is a crash-loop, not a silent start. This gate stops the deploy
#     before that pod ever ships.
#   * ⛔ NEITHER topic may be compacted. Every observation is a distinct event, and every window
#     status covers a distinct RANGE of window indices — and all of a session's statuses
#     deliberately share ONE key so their order survives Kafka. Under compaction that single key
#     would collapse an entire session's coverage to its last record, destroying exactly the
#     continuity the status stream exists to carry.
#
# CREATE-only if absent, then VERIFY what was created; VERIFY (fail closed) if present. Partition
# count is verified, never changed: Kafka cannot shrink a topic, and recreating one would drop
# records. The ONLY mutating command in this script is `kafka-topics --create`.
set -euo pipefail

OBS_TOPIC="${MULTILEG_OUTPUT_TOPIC:-options.multileg.observations}"
STATUS_TOPIC="${MULTILEG_STATUS_TOPIC:-options.multileg.window.status}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
# Matches scripts/kafka/topics.env, which declares both at 4 (the standing partition policy).
PARTITIONS="${MULTILEG_TOPIC_PARTITIONS:-4}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }

# ⛔ `set -e` does NOT apply inside a function whose caller uses `|| ...`, so every command whose
# failure matters is checked EXPLICITLY here. An unchecked `kafka-topics --create` was exactly how
# a failed create could reach `return 0` and report success.
topic_exists() {
  _list=$(kt --list 2>&1) || {
    echo "FAIL: cannot list topics on $KAFKA_BOOTSTRAP_SERVERS: $_list"
    return 2   # 2 = "could not determine", never "absent"
  }
  grep -qxF "$1" <<<"$_list"
}

partition_count() {
  _desc=$(kt --describe --topic "$1" 2>&1) || {
    # stderr, not stdout: the caller captures this function's stdout, and a diagnostic written
    # there would be swallowed into the value it is trying to read.
    echo "FAIL: cannot describe '$1': $_desc" >&2
    return 1
  }
  awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$_desc"
}

# The EFFECTIVE cleanup policy, or empty when it cannot be proven. The word boundary matters:
# `kafka-configs` prints synonyms such as `DEFAULT_CONFIG:log.cleanup.policy=delete`, and matching
# those would let a topic's real policy go unchecked.
cleanup_policy() {
  _cfg=$(kc --entity-type topics --entity-name "$1" --describe 2>&1) || {
    echo "FAIL: cannot describe configs for '$1': $_cfg" >&2
    return 1
  }
  # ⛔ NO MATCH IS NOT A FAILURE. Under `pipefail` a non-matching grep made this function return
  # non-zero, so the caller's `|| return 1` fired and its "no explicit policy, here is what to do"
  # guidance became unreachable for exactly the case it exists for. Status 1 (no match) is
  # translated to "empty, successfully"; any other status stays a real error.
  _match=$(grep -oE '(^|[^.[:alnum:]_])cleanup\.policy=[a-z,]+' <<<"$_cfg" | head -1)
  _grep_status=$?
  if [ "$_grep_status" -gt 1 ]; then
    echo "FAIL: cannot parse configs for '$1' (grep exited $_grep_status)" >&2
    return 1
  fi
  [ -n "$_match" ] && sed 's/.*cleanup\.policy=//' <<<"$_match"
  return 0
}

verify_topic() {
  topic="$1"
  parts=$(partition_count "$topic") || return 1
  if [ "${parts:-}" != "$PARTITIONS" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected $PARTITIONS."
    echo "      The service pins its assignment at boot and EXITS when the count changes, so a"
    echo "      mismatch here is a restart loop rather than a silent subset. Fix the topic, then"
    echo "      re-run this deploy."
    return 1
  fi
  policy=$(cleanup_policy "$topic") || return 1
  if [ -z "$policy" ]; then
    echo "FAIL: cannot PROVE '$topic' is delete-only — no explicit cleanup.policy is set on it."
    echo "      A topic whose policy is only a broker default is one broker-config change away from"
    echo "      compaction, and compaction would collapse a whole session's statuses (they share one"
    echo "      key) to their last record. Run apply-topics for this topic, then re-run this deploy."
    return 1
  fi
  if [ "$policy" != "delete" ]; then
    echo "FAIL: '$topic' cleanup.policy is '$policy', expected exactly 'delete'. Compaction keeps"
    echo "      only the last record per key — and a whole session's window statuses share ONE key,"
    echo "      so it would erase the coverage record itself."
    return 1
  fi
  echo "ok: '$topic' shape verified ($PARTITIONS partitions, cleanup.policy=$policy)"
}

ensure_topic() {
  topic="$1"
  echo "=== ensure-multileg-topics: $topic on $KAFKA_BOOTSTRAP_SERVERS ==="
  # Captured explicitly rather than read from `$?` in an elif: three outcomes (present / absent /
  # could-not-determine) must stay distinguishable, and "could not determine" must never be
  # treated as "absent" and answered with a create.
  topic_exists "$topic"
  existed=$?
  if [ "$existed" -eq 0 ]; then
    verify_topic "$topic"
    return
  fi
  if [ "$existed" -ne 1 ]; then
    return 1
  fi

  echo "creating '$topic' ($PARTITIONS partitions, RF=$RF, cleanup.policy=delete)"
  if ! kt --create --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$RF" \
       --config cleanup.policy=delete; then
    # A concurrent deploy may have won the race; anything else is a real failure. Either way the
    # answer is the same: VERIFY what is actually there rather than assume what we asked for.
    echo "create returned non-zero for '$topic' — verifying what exists instead of assuming"
  fi
  verify_topic "$topic"
}

rc=0
ensure_topic "$OBS_TOPIC" || rc=1
ensure_topic "$STATUS_TOPIC" || rc=1
exit "$rc"
