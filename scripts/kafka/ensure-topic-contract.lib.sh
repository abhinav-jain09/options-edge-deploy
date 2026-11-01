#!/usr/bin/env bash
# ensure-topic-contract.lib.sh — the ONE create-then-verify routine behind ensure-es-trade-tools-topics.sh and ensure-wave-cycle-topics.sh
# (the same barrier as ensure-market-structure-topics.sh, parameterised, so the two new services cannot drift from each other).
#
# ensure_topic <topic> <cleanup.policy> <retention-spec> <min max.message.bytes> [create-if-absent: yes|no] [create-time max.message.bytes, default 1048576]
#   retention-spec   exact:<ms>   retention.ms must EQUAL <ms> (-1 included)       — es-trade-tools-service compares exactly
#                    floor:<ms>   retention.ms must be >= <ms> (or -1)             — wave-cycle-service compares with >=
#                    any:<ms>     retention.ms is only CREATED at <ms> and not judged on an existing topic (a compact-only topic: the service
#                                 sets none and accepts the broker default, so a stricter barrier would block a deploy the service would run)
#   cleanup.policy   compared as a SET of its comma-separated members ("compact,delete" == "delete,compact"), as the services do.
# The optional 6th argument is the max.message.bytes an ABSENT topic is created with (break-classification's checkpoint needs 9437184); it never alters an existing topic.
# Every value read is the EFFECTIVE one (`kafka-configs --describe --all`), which is what a service's own describeConfigs returns.
# A topic that EXISTS is never altered, recreated or shrunk: any difference fails closed. A topic that is ABSENT is created with the
# declared shape when create-if-absent is yes, and is then verified like any other. Needs KT/KC functions, LIST, RF, MIN_RF, MIN_ISR
# and extract() from the caller; prints `ok:`/`FAIL:` lines and returns non-zero on the first difference of that topic.
# ensure_topic runs in an `if` context, where bash ignores `set -e`: nothing here relies on errexit.

_policy_set() { printf '%s' "$1" | tr ',' '\n' | sort -u | tr '\n' ','; }

ensure_topic() {
  local topic="$1" want_policy="$2" ret_spec="$3" min_bytes="$4" create="${5:-yes}" create_bytes="${6:-1048576}"
  local ret_mode="${ret_spec%%:*}" want_ret="${ret_spec#*:}" out desc parts rf cfg policy retention maxBytes isr
  if ! grep -qxF "$topic" <<<"$LIST"; then
    if [ "$create" != yes ]; then
      echo "FAIL: '$topic' does not exist and this service never creates it (its boot verification refuses to start without it)."
      return 1
    fi
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=$want_policy, retention.ms=$want_ret)"
    if ! out=$(kt --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy="$want_policy" --config retention.ms="$want_ret" \
                  --config max.message.bytes="$create_bytes" --config min.insync.replicas="$MIN_ISR" 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (the service's boot verification requires exactly 1; Kafka cannot shrink a topic, so this is never repaired here)."
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
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  retention=$(extract 'retention\.ms' "$cfg")
  if [ -z "${policy:-}" ] || [ "$(_policy_set "$policy")" != "$(_policy_set "$want_policy")" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset>}', expected exactly '$want_policy'."
    return 1
  fi
  case "${retention:-}" in
    ''|*[!0-9-]*|-|?*-*|-[!1]*|-1?*) echo "FAIL: '$topic' reports retention.ms='${retention:-<unreadable>}', expected ${ret_mode} ${want_ret}."; return 1 ;;
  esac
  if [ "$ret_mode" = any ]; then
    :
  elif [ "$ret_mode" = exact ]; then
    if [ "$retention" != "$want_ret" ]; then
      echo "FAIL: '$topic' has retention.ms=$retention, expected exactly $want_ret (the service compares for equality)."
      return 1
    fi
  elif [ "$retention" != "-1" ] && { [ "$want_ret" = "-1" ] || [ "$retention" -lt "$want_ret" ]; }; then
    echo "FAIL: '$topic' has retention.ms=$retention, expected >= $want_ret (or -1)."
    return 1
  fi
  maxBytes=$(extract 'max\.message\.bytes' "$cfg")
  case "${maxBytes:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports max.message.bytes='${maxBytes:-<unreadable>}', expected >= $min_bytes."; return 1 ;;
  esac
  if [ "$maxBytes" -lt "$min_bytes" ]; then
    echo "FAIL: '$topic' has max.message.bytes=$maxBytes, expected >= $min_bytes (the service's largest record plus its 4096-byte margin)."
    return 1
  fi
  isr=$(extract 'min\.insync\.replicas' "$cfg")
  case "${isr:-}" in
    ''|*[!0-9]*) echo "FAIL: '$topic' reports min.insync.replicas='${isr:-<unreadable>}', expected >= $MIN_ISR."; return 1 ;;
  esac
  if [ "$isr" -lt "$MIN_ISR" ]; then
    echo "FAIL: '$topic' has min.insync.replicas=$isr, expected >= $MIN_ISR."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 rf=$rf cleanup.policy=$policy retention.ms=$retention (${ret_mode} ${want_ret}) max.message.bytes=$maxBytes min.insync.replicas=$isr"
}
