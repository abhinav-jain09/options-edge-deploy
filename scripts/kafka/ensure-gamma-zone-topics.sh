#!/usr/bin/env bash
# ensure-gamma-zone-topics.sh - gamma-zone-service's two topics must be right BEFORE the service publishes (the first-deploy barrier, as
# ensure-break-line-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   options.spx.gamma-zone.current  1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)  the newest measurement per session
#   options.spx.gamma-zone.log      1 partition, delete,  retention.ms EXACTLY -1, retention.bytes EXACTLY 1073741824 (1 GiB)  EVERY emission, in order: the corpus
#
# WHY THE BARRIER EXISTS AT ALL when the service itself creates what is missing. The service deliberately CREATES-IF-ABSENT and then only CHECKS the partition count: it
# never reconciles a topic's configuration, because a pod start that overwrites a declared shape with whatever the running binary believes is how this fleet has lost a
# compaction setting before. So the declared shape - in particular the 1 GiB byte cap, which the service does not know - has to be applied by something that reads
# topics.env, and that is this script. An existing topic is never recreated, altered or shrunk here either: any difference fails closed.
#
# WHY THE LOG MUST NOT BE COMPACTED, checked here and not merely declared: the key is the session, so compaction would keep one record per session and discard exactly the
# intraday history the topic exists to accumulate - which is the whole reason the service was built. A single wrong cleanup.policy on this topic silently destroys the
# corpus, so it is verified before the writer starts rather than discovered weeks later.
#
# The INPUT topic (es.futures.footprint.bars) belongs to another owner and is not created or altered here.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-gamma-zone-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); gamma-zone has no production topics - refusing."
  exit 1
fi
# The DECLARED names, not overridable. An override would let this script create an arbitrary topic
# with gamma-zone's shape, so the barrier would no longer match topics.env in both directions -- and
# the name it creates is the one nothing else preserves: reset, dev-clean, premarket and
# never-recreate all list these two literals. A renamed log would be swept by every one of those
# paths while looking, from inside this script, perfectly correct.
CURRENT="options.spx.gamma-zone.current"
LOG="options.spx.gamma-zone.log"
if [ -n "${KAFKA_GAMMA_ZONE_CURRENT_TOPIC:-}" ] && [ "${KAFKA_GAMMA_ZONE_CURRENT_TOPIC}" != "$CURRENT" ]; then
  echo "FAIL: KAFKA_GAMMA_ZONE_CURRENT_TOPIC is set to '${KAFKA_GAMMA_ZONE_CURRENT_TOPIC}', but this barrier only ever ensures the declared '$CURRENT' - refusing."
  exit 1
fi
if [ -n "${KAFKA_GAMMA_ZONE_LOG_TOPIC:-}" ] && [ "${KAFKA_GAMMA_ZONE_LOG_TOPIC}" != "$LOG" ]; then
  echo "FAIL: KAFKA_GAMMA_ZONE_LOG_TOPIC is set to '${KAFKA_GAMMA_ZONE_LOG_TOPIC}', but this barrier only ever ensures the declared '$LOG' - refusing. A renamed corpus log is preserved by nothing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${GAMMA_ZONE_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${GAMMA_ZONE_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-gamma-zone-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

LOG_BYTES=1073741824

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what a service's own describeConfigs sees) and compare exactly. Never
# altered here. `--describe --all` prints SYNONYMS as well as the effective value, which is why the comparison goes through extract()'s own boundary rather than a bare
# grep: a bare grep counts one value several times and refuses a correct topic.
check_retention_bytes() { # <topic> <exact retention.bytes>
  local topic="$1" want="$2" cfg bytes
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
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

rc=0

# options.spx.gamma-zone.current — pure compact. Created here WITH retention.bytes=-1, then verified by the shared routine with create-if-absent=no, so this path can
# never leave a topic at the broker's default shape.
if ! grep -qxF "$CURRENT" <<<"$LIST"; then
  echo "creating '$CURRENT' (1 partition, RF=$RF, cleanup.policy=compact, retention.ms=-1, retention.bytes=-1)"
  if ! out=$(kt --create --if-not-exists --topic "$CURRENT" --partitions 1 --replication-factor "$RF" \
                --config cleanup.policy=compact --config retention.ms=-1 --config retention.bytes=-1 \
                --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
    echo "FAIL: could not create '$CURRENT':"; printf '%s\n' "$out"; rc=1
  else
    LIST="$LIST"$'\n'"$CURRENT"
  fi
fi
if [ "$rc" -eq 0 ]; then
  ensure_topic "$CURRENT" compact "exact:-1" 1048576 no || rc=1
  check_retention_bytes "$CURRENT" -1 || rc=1
fi

# options.spx.gamma-zone.log — the corpus. DELETE only, never compacted, retention.ms exactly -1, and the byte cap is the only thing that ever removes a record.
if ! grep -qxF "$LOG" <<<"$LIST"; then
  echo "creating '$LOG' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=-1, retention.bytes=$LOG_BYTES)"
  if ! out=$(kt --create --if-not-exists --topic "$LOG" --partitions 1 --replication-factor "$RF" \
                --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes="$LOG_BYTES" \
                --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
    echo "FAIL: could not create '$LOG':"; printf '%s\n' "$out"; rc=1
  else
    LIST="$LIST"$'\n'"$LOG"
  fi
fi
if [ "$rc" -eq 0 ]; then
  ensure_topic "$LOG" delete "exact:-1" 1048576 no || rc=1
  check_retention_bytes "$LOG" "$LOG_BYTES" || rc=1
fi

if [ "$rc" -ne 0 ]; then
  echo "=== ensure-gamma-zone-topics FAILED - the gamma zone service must not publish ==="
  exit 1
fi
echo "=== ensure-gamma-zone-topics OK ==="
