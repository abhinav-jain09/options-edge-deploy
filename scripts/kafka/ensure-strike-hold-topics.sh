#!/usr/bin/env bash
# ensure-strike-hold-topics.sh - strike-hold-service's two output topics must be right BEFORE the service publishes (the first-deploy barrier, as
# ensure-liquidity-validator-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   options.spx.strike-hold.current  1 partition, compact,delete, retention.ms EXACTLY -1                                  the newest state of one candidate strike
#   options.spx.strike-hold.scoring  1 partition, delete,         retention.ms EXACTLY -1, retention.bytes EXACTLY 268435456  ONE evidence row per candidate per session
#
# WHY THIS BARRIER EXISTS AT ALL, given the service ensures its own topics at start-up. The service can set the cleanup policy; it cannot set the RETENTION the
# declaration carries. If the pod reached an empty broker first it would create `scoring` at the cluster default retention, and the evidence record would then be
# aged out from under the measurement — silently, because a topic that exists and accepts writes looks healthy. The barrier makes the shape a precondition of the
# deploy rather than something a race decides.
#
# The difference between the two topics is the point and is enforced here, not assumed: `current` is COMPACTED because the newest record per candidate is the whole
# answer, and `scoring` must NOT be, because every record is a distinct (candidate, session) observation and compaction would discard exactly what the topic
# accumulates. An existing topic is never recreated, altered or shrunk: any difference fails closed. The four INPUT topics belong to other owners and are not created here.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-strike-hold-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); strike-hold has no production topics - refusing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
# BOTH of these are required by scripts/kafka/ensure-topic-contract.lib.sh, which runs under
# `set -u`: the library reads MIN_RF and MIN_ISR directly from the caller's environment. Setting
# only MIN_ISR (the first version here) aborted the barrier with "MIN_RF: unbound variable" before
# it checked anything — found by running it against a mock broker rather than by reading it.
MIN_RF="${STRIKE_HOLD_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${STRIKE_HOLD_MIN_INSYNC_REPLICAS:-1}"
PARTITIONS=1
SCORING_BYTES=268435456
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-strike-hold-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

CURRENT="${KAFKA_STRIKE_HOLD_OUTPUT_TOPIC:-options.spx.strike-hold.current}"
SCORING="${KAFKA_STRIKE_HOLD_SCORING_TOPIC:-options.spx.strike-hold.scoring}"

# The declared partition width is EXACT, and this check is STRICTER than the shared routine's rather than a duplicate of it: ensure_topic takes the FIRST
# PartitionCount an awk scan finds, so output carrying two would be judged on whichever came first. Kafka can add partitions but never remove them, so a topic
# already wider than the declaration is a fact to fail on, not to reconcile.
check_partitions() { # <topic>
  local topic="$1" out n count
  if ! out=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$out"; return 1
  fi
  # EVERY PartitionCount the describe printed, not the first. Taking the first would accept output
  # whose leading value happens to be right while a second, different one is present — a prefix
  # match on the topic name, or two topics in one describe. Ambiguous output is a refusal, not a
  # value to pick from, so the count of lines is checked before the value is read at all.
  # A SECOND LINE OF DEFENCE, not the primary one: the shared ensure_topic above already requires
  # the exact token `PartitionCount:` in its own awk, so a lookalike is refused there first. This
  # check exists for the ambiguity the shared routine does not cover — a dump carrying TWO counts,
  # where its awk takes the first.
  #
  # grep -oE with a TOKEN BOUNDARY, not sed. `sed 's/.*PartitionCount: .../'` has a leading `.*`
  # that eats anything, so it would read a value out of `NotPartitionCount: 9` just as happily —
  # the same loose-matching mistake the retention.bytes counter made. grep rather than sed because
  # BSD sed (these dev Macs) has no `\|` alternation in BRE, so a boundary written that way matches
  # NOTHING here while working on the Linux agent: a check that passes for the wrong reason on one
  # platform is worse than no check (the note in scripts/kafka/topic-config-parse.sh says the same).
  n=$(printf '%s\n' "$out" | grep -oE '(^|[[:space:],])PartitionCount:[[:space:]]*[0-9]+' \
        | grep -oE '[0-9]+$')
  # `|| true`, and then normalise. A grep that matches nothing exits 1, and under
  # `set -euo pipefail` that killed the script AT THIS ASSIGNMENT — failing closed, but with no
  # diagnostic, which is indistinguishable from a crash. The emptiness must be REPORTED by the
  # check below, not swallowed by the shell. scripts/ci/validate-mirrored-topic-contracts.sh
  # carries the same lesson in its own comment; this script had not applied it.
  count=$(printf '%s\n' "$n" | grep -c . || true)
  count="${count:-0}"
  if [ "$count" != "1" ]; then
    echo "FAIL: describe of '$topic' reported $count PartitionCount values, expected exactly 1 — refusing to choose:"
    printf '%s\n' "$out"
    return 1
  fi
  case "${n:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports PartitionCount='${n:-<unreadable>}'."; return 1 ;;
  esac
  if [ "$n" != "$PARTITIONS" ]; then
    echo "FAIL: '$topic' has $n partitions, expected exactly $PARTITIONS (the declared shape in scripts/kafka/topics.env; never altered here)."
    return 1
  fi
  echo "ok: '$topic' — partitions=$n"
}

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what the service's own describeConfigs sees) and compare exactly.
check_retention_bytes() { # <topic> <exact retention.bytes>
  local topic="$1" want="$2" cfg bytes
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  # Same refusal as above: `extract` returns the FIRST match, so a config dump carrying two
  # retention.bytes values would be accepted on the strength of whichever came first. Count the
  # occurrences and refuse anything but exactly one before trusting the value.
  #
  # COUNTED WITH extract's OWN PATTERN, and that is the whole point. The first version here used a
  # bare `grep -o 'retention.bytes='`, which on a real `--describe --all` dump counts FIVE: the
  # value itself, `local.retention.bytes`, and three occurrences inside the `synonyms={...}` text.
  # It refused a perfectly correct topic on the first live deploy. A duplicate check that parses
  # differently from the value read is not a stricter check, it is a check of a DIFFERENT question
  # — which is exactly the two-copies-of-a-parser defect scripts/kafka/topic-config-parse.sh was
  # written to prevent, reintroduced one layer up. The boundary `(^|[[:space:],])` is what makes
  # `local.retention.bytes` and the synonym text not count.
  local seen
  seen=$(printf '%s\n' "$cfg" | grep -oE '(^|[[:space:],])retention\.bytes=[^ ]*' | grep -c . || true)
  seen="${seen:-0}"
  if [ "$seen" != "1" ]; then
    echo "FAIL: config of '$topic' carries $seen retention.bytes values, expected exactly 1 — refusing to choose:"
    printf '%s\n' "$cfg"
    return 1
  fi
  bytes=$(extract 'retention\.bytes' "$cfg")
  case "${bytes:-}" in
    ''|*[!0-9-]*|-|?*-*|-[!1]*|-1?*) echo "FAIL: '$topic' reports retention.bytes='${bytes:-<unreadable>}', expected exactly $want."; return 1 ;;
  esac
  if [ "$bytes" != "$want" ]; then
    echo "FAIL: '$topic' has retention.bytes=$bytes, expected exactly $want (the declared shape in scripts/kafka/topics.env; never altered here)."
    return 1
  fi
  echo "ok: '$topic' — retention.bytes=$bytes"
}

create_if_absent() { # <topic> <cleanup.policy> <retention.ms> [<retention.bytes>]
  local topic="$1" policy="$2" ms="$3" bytes="${4:-}" out
  if grep -qxF "$topic" <<<"$LIST"; then
    return 0
  fi
  echo "creating '$topic' ($PARTITIONS partitions, RF=$RF, cleanup.policy=$policy, retention.ms=$ms${bytes:+, retention.bytes=$bytes})"
  local args=(--create --if-not-exists --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$RF"
              --config cleanup.policy="$policy" --config retention.ms="$ms"
              --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR")
  if [ -n "$bytes" ]; then
    args+=(--config retention.bytes="$bytes")
  fi
  if ! out=$(kt "${args[@]}" 2>&1); then
    echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
  fi
  LIST="$LIST"$'\n'"$topic"
}

rc=0
# `current`: compact,delete. create-if-absent=no on the shared routine, so it can never create one at the default shape.
create_if_absent "$CURRENT" compact,delete -1 || rc=1
ensure_topic "$CURRENT" compact,delete "exact:-1" 1048576 no || rc=1
check_partitions "$CURRENT" || rc=1

# `scoring`: plain delete with an unbounded retention and a byte cap. NOT compacted — asserted, because a compacted evidence log would still accept every write
# and still look healthy while deleting the measurement.
create_if_absent "$SCORING" delete -1 "$SCORING_BYTES" || rc=1
ensure_topic "$SCORING" delete "exact:-1" 1048576 no || rc=1
check_partitions "$SCORING" || rc=1
check_retention_bytes "$SCORING" "$SCORING_BYTES" || rc=1

if [ "$rc" -ne 0 ]; then
  echo "=== ensure-strike-hold-topics FAILED - strike-hold must not publish ==="
  exit 1
fi
echo "=== ensure-strike-hold-topics OK ==="
