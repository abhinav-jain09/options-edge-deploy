#!/usr/bin/env bash
# ensure-level-absorption-topics.sh — level-absorption-service's four topics must EXIST with their declared shape BEFORE the
# service rolls out (the same barrier as ensure-break-score-topics). The shape is the one options-edge-processing's
# scripts/kafka/apply-level-absorption-topics.sh applies and the service's own StartupChecks demand:
#   futures.level-absorption.events       1 partition, delete, retention.ms=-1, retention.bytes = the retention artifact's integer
#   futures.level-absorption.voice        1 partition, delete, retention.ms=7 days
#   futures.level-absorption.closures     1 partition, delete, retention.ms=7 days (decisions only time could make, replayed on restart)
#   futures.level-absorption.manual-line  1 partition, delete, retention.ms=7 days (the chart's drawn-line commands, Avro; the producer is built, options-edge PR 1172; the service's input-contract manifest declares it built (processing PR 1354), and startup therefore requires this topic)
# None may be compacted, and the three OUTPUT topics (events, voice, closures) may carry no finite message.timestamp.before.max.ms
# (a restart republishes old timestamps) and no message.timestamp.after.max.ms below the one-hour broker default: the service refuses
# to start on either. The service also needs every one of these (and the three input topics) to keep at least 7 days or forever.
#
# THE EVENTS TOPIC'S retention.bytes IS NEVER TYPED HERE. It is ceil(measuredBytesPerDay x retentionDays x safetyFactor) from the
# retention artifact (docs/level-absorption/phase0/retention-artifact.json in options-edge-processing, copied into the ConfigMap
# k8s/services/level-absorption/base/level-absorption-config.yaml; 21765840 since 2026-10-25) and the service refuses to start unless
# the broker reports exactly that value. Jenkinsfile.service-deploy reads the integer out of the ConfigMap and passes it in as
# LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES with LEVEL_ABSORPTION_REQUIRE_EVENTS_TOPIC=1, so the file stays the one source. So:
#   * events ABSENT and LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES set (a positive whole number, the artifact's retentionBytes)
#       -> created with it, then verified;
#   * events ABSENT and the variable unset -> a WARNING and the topic is skipped (a production-style deploy of the service held at
#       replicas 0; its own startup check is the hard gate). LEVEL_ABSORPTION_REQUIRE_EVENTS_TOPIC=1 turns the warning into a
#       failure; the service-deploy stage sets it;
#   * events EXISTS -> verified (and, when the variable is set, its retention.bytes must equal it exactly). Only with
#       LEVEL_ABSORPTION_ALIGN_EVENTS_RETENTION=1 is a LOWER existing cap RAISED to the artifact's integer (a new Phase 0 artifact); a HIGHER
#       existing cap is refused (lowering can delete evidence).
# Voice, closures and manual-line are created when absent. Every CLI call's status is checked explicitly: ensure_*() runs in an `if`
# context where bash ignores `set -e`, so nothing here relies on errexit.
set -euo pipefail

EVENTS_TOPIC="${KAFKA_LEVEL_ABSORPTION_EVENTS_TOPIC:-futures.level-absorption.events}"
VOICE_TOPIC="${KAFKA_LEVEL_ABSORPTION_VOICE_TOPIC:-futures.level-absorption.voice}"
CLOSURES_TOPIC="${KAFKA_LEVEL_ABSORPTION_CLOSURES_TOPIC:-futures.level-absorption.closures}"
MANUAL_TOPIC="${KAFKA_LEVEL_ABSORPTION_MANUAL_LINE_TOPIC:-futures.level-absorption.manual-line}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${LEVEL_ABSORPTION_MIN_REPLICATION_FACTOR:-1}"
SEVEN_DAYS_MS=604800000
LONG_MAX=9223372036854775807
EVENTS_BYTES="${LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES:-}"
if [ -n "$EVENTS_BYTES" ]; then
  case "$EVENTS_BYTES" in
    *[!0-9]*|0|0*) echo "FAIL: LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES='$EVENTS_BYTES' is not a positive whole number without a leading zero."; exit 1 ;;
  esac
fi

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-level-absorption-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

# create_if_absent <topic> <retention.ms> [<retention.bytes>]
create_if_absent() {
  local topic="$1" ms="$2" bytes="${3:-}" out args
  if grep -qxF "$topic" <<<"$LIST"; then return 0; fi
  args=(--create --topic "$topic" --partitions 1 --replication-factor "$RF" --config cleanup.policy=delete --config retention.ms="$ms")
  if [ -n "$bytes" ]; then args+=(--config retention.bytes="$bytes"); fi
  echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=$ms${bytes:+, retention.bytes=$bytes})"
  if ! out=$(kt "${args[@]}" 2>&1); then
    echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
  fi
}

# verify <topic> <events|log|manual>
#   events: retention.ms exactly -1 and a finite positive retention.bytes (equal to $EVENTS_BYTES when that is set)
#   log   : voice / closures  — retention.ms -1 or >= 7 days
#   manual: manual-line       — retention.ms -1 or >= 7 days; the timestamp limits are not the service's concern for an input
verify() {
  local topic="$1" kind="$2" desc parts rf cfg policy retention bytes before after
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (the service refuses any other count)."
    return 1
  fi
  rf=$(awk '{for (i = 1; i < NF; i++) if ($i == "ReplicationFactor:") { print $(i + 1); exit }}' <<<"$desc")
  case "${rf:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports ReplicationFactor='${rf:-unknown}', which is not a number."; return 1 ;;
  esac
  if [ "$rf" -lt "$MIN_RF" ]; then
    echo "FAIL: '$topic' has ReplicationFactor=$rf, expected >= $MIN_RF."
    return 1
  fi
  # --all: the EFFECTIVE values, which are what the service's own AdminClient describeConfigs sees.
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${policy:-}" != "delete" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly 'delete' (compaction deletes events out of the middle of a session)."
    return 1
  fi
  case "${retention:-}" in
    ''|-1) ;;
    *[!0-9]*) echo "FAIL: '$topic' has retention.ms='$retention', which is not a number."; return 1 ;;
  esac
  if [ "$kind" = "events" ]; then
    if [ "${retention:-}" != "-1" ]; then
      echo "FAIL: '$topic' has retention.ms='${retention:-<unreadable>}', expected exactly -1."
      return 1
    fi
    bytes=$(extract 'retention\.bytes' "$cfg")
    case "${bytes:-}" in
      ''|*[!0-9]*|0) echo "FAIL: '$topic' has retention.bytes='${bytes:-<unreadable>}', expected a finite positive cap (the retention artifact's integer)."; return 1 ;;
    esac
    if [ -n "$EVENTS_BYTES" ] && [ "$bytes" != "$EVENTS_BYTES" ]; then
      echo "FAIL: '$topic' has retention.bytes=$bytes, expected exactly $EVENTS_BYTES (LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES); the service refuses to start on any other value."
      return 1
    fi
  else
    if [ -z "${retention:-}" ]; then
      echo "FAIL: '$topic' has an unreadable retention.ms, expected -1 or >= $SEVEN_DAYS_MS (a restart replays from a session open that can be days old)."
      return 1
    fi
    if [ "$retention" != "-1" ] && [ "$retention" -lt "$SEVEN_DAYS_MS" ]; then
      echo "FAIL: '$topic' has retention.ms=$retention, expected -1 or >= $SEVEN_DAYS_MS (7 days: a restart replays from a session open that can be days old)."
      return 1
    fi
  fi
  if [ "$kind" != "manual" ]; then
    before=$(extract 'message\.timestamp\.before\.max\.ms' "$cfg")
    after=$(extract 'message\.timestamp\.after\.max\.ms' "$cfg")
    if [ -n "${before:-}" ] && [ "$before" != "$LONG_MAX" ]; then
      echo "FAIL: '$topic' has message.timestamp.before.max.ms=$before; a replay publishes events stamped long before the broker's clock and would be rejected (expected unset or $LONG_MAX)."
      return 1
    fi
    if [ -n "${after:-}" ] && [ "$after" != "$LONG_MAX" ]; then
      case "$after" in
        *[!0-9]*) echo "FAIL: '$topic' has an unreadable message.timestamp.after.max.ms='$after'."; return 1 ;;
      esac
      if [ "$after" -lt 3600000 ]; then
        echo "FAIL: '$topic' has message.timestamp.after.max.ms=$after, below the one-hour broker default."
        return 1
      fi
    fi
  fi
  echo "ok: '$topic' — partitions=1 rf=$rf cleanup.policy=delete retention.ms=$retention${bytes:+ retention.bytes=$bytes}"
}

ensure_simple() { # <topic> <log|manual>
  create_if_absent "$1" "$SEVEN_DAYS_MS" || return 1
  verify "$1" "$2"
}

ensure_events() {
  local topic="$EVENTS_TOPIC"
  if ! grep -qxF "$topic" <<<"$LIST"; then
    if [ -n "$EVENTS_BYTES" ]; then
      create_if_absent "$topic" -1 "$EVENTS_BYTES" || return 1
    elif [ "${LEVEL_ABSORPTION_REQUIRE_EVENTS_TOPIC:-0}" = "1" ]; then
      echo "FAIL: '$topic' does not exist and LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES (the retention artifact's retentionBytes) is not set; the service refuses to start without it."
      return 1
    else
      echo "WARN: '$topic' does not exist and LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES is not set (the retention artifact is an owner measurement that does not exist yet): skipped."
      echo "      The service is held at replicas 0 and its StartupChecks refuse to start without this topic. Create it with options-edge-processing's"
      echo "      scripts/kafka/apply-level-absorption-topics.sh --retention-artifact <file>, or re-run this barrier with LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES set."
      return 0
    fi
  fi
  # A NEW retention artifact (a new Phase 0) changes the cap the service demands. RAISING an existing events topic's cap is safe and is done
  # here when LEVEL_ABSORPTION_ALIGN_EVENTS_RETENTION=1 (the Jenkins stage sets it); LOWERING it could delete evidence the cap used to keep,
  # so it is refused and left to a separate, reviewed operation.
  if [ "${LEVEL_ABSORPTION_ALIGN_EVENTS_RETENTION:-0}" = "1" ] && [ -n "$EVENTS_BYTES" ]; then
    local cur out
    if ! out=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
      echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$out"; return 1
    fi
    cur=$(extract 'retention\.bytes' "$out")
    case "${cur:-}" in ''|*[!0-9]*) ;; *)
      if [ "$cur" -lt "$EVENTS_BYTES" ]; then
        echo "raising '$topic' retention.bytes $cur -> $EVENTS_BYTES (never lowered)"
        if ! out=$(kc --entity-type topics --entity-name "$topic" --alter --add-config "retention.bytes=$EVENTS_BYTES" 2>&1); then
          echo "FAIL: could not raise retention.bytes of '$topic':"; printf '%s\n' "$out"; return 1
        fi
      elif [ "$cur" -gt "$EVENTS_BYTES" ]; then
        echo "FAIL: '$topic' has retention.bytes=$cur, above the artifact's $EVENTS_BYTES; lowering a cap can delete evidence and is a separate reviewed operation, not done here."
        return 1
      fi ;;
    esac
  fi
  verify "$topic" events
}

rc=0
ensure_simple "$VOICE_TOPIC" log || rc=1
ensure_simple "$CLOSURES_TOPIC" log || rc=1
ensure_simple "$MANUAL_TOPIC" manual || rc=1
ensure_events || rc=1
if [ "$rc" -ne 0 ]; then
  echo "=== ensure-level-absorption-topics FAILED — the rollout must not proceed ==="
  exit 1
fi
echo "=== ensure-level-absorption-topics OK ==="
