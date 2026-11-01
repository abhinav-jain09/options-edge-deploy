#!/usr/bin/env bash
# roll-forward-topics.sh — GENERIC destructive half of a JSON -> Avro topic conversion under the SAME topic name.
#
# Owner rule (2026-10-18): there is no topic version; a topic that changes serialization is DELETED AND RECREATED EMPTY under
# its own name and the retained JSON is never read by an Avro reader. Runbook: docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md.
# Generalises roll-forward-underlying-spx-price.sh: no per-topic shape table. The recreate copies the LIVE topic's partition
# count, replication factor and EVERY topic-level config, captured from the broker BEFORE the delete into a state file (so a
# recreate whose create failed, or a later --verify, still knows the shape).
#
# THE OPERATOR ASSERTS EXCLUSION (nothing in Kafka proves a writer is absent): pass --writers-and-readers-stopped (everything
# stopped) OR --avro-writer-live --json-writers-stopped (the Avro writer is running, no JSON writer is).
#
# usage:
#   roll-forward-topics.sh --recreate|--verify --cluster dev|production --topics t1,t2,... \
#       --pre-cutover-id t1=<TopicId> [--pre-cutover-id t2=<TopicId> ...]            (one per topic, read off the live broker)
#       [--new-topic-id t1=<id> ...]                                                  (--verify only: the ids --recreate printed)
#       --registry <schema-registry-url> --subject t1=<record.full.Name> [--subject t2=... ]   (RecordNameStrategy subject per topic)
#       ( --writers-and-readers-stopped | --avro-writer-live --json-writers-stopped )
#       [--topic-absent-after-failed-create] [--state-dir DIR] <bootstrap-servers> [kafka-bin-dir]
# Every topic is checked (id, shape captured) BEFORE any topic is deleted. Topics are then processed in order and the run STOPS at
# the first failure, naming the topics not yet touched: re-run with --topics set to those (a finished topic has a new id and is
# refused by design). A topic whose delete worked but whose post-check failed is finished with --verify.
set -euo pipefail

usage() {
  sed -n '/^# usage:/,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//' >&2
  exit 2
}
PHASE="${1:-}"
case "$PHASE" in --recreate|--verify) ;; *) usage ;; esac
shift
REGISTRY=""; SUBJECTS=""; CLUSTER=""; TOPICS_CSV=""; PRE_IDS=""; NEW_IDS=""; STOPPED=""; JSON_STOPPED=""; AVRO_LIVE=""; ABSENT_OK=""; STATE_BASE="${RF_STATE_DIR:-$HOME/.roll-forward-topics}"; POS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --topics) TOPICS_CSV="${2:-}"; shift 2 ;;
    --pre-cutover-id) PRE_IDS="$PRE_IDS${2:-}"$'\n'; shift 2 ;;
    --new-topic-id) NEW_IDS="$NEW_IDS${2:-}"$'\n'; shift 2 ;;
    --writers-and-readers-stopped) STOPPED=yes; shift ;;
    --json-writers-stopped) JSON_STOPPED=yes; shift ;;
    --avro-writer-live) AVRO_LIVE=yes; shift ;;
    --topic-absent-after-failed-create) ABSENT_OK=yes; shift ;;
    --registry) REGISTRY="${2:-}"; shift 2 ;;
    --subject) SUBJECTS="$SUBJECTS${2:-}"$'\n'; shift 2 ;;
    --state-dir) STATE_BASE="${2:-}"; shift 2 ;;
    --*) echo "FATAL: unknown option '$1'" >&2; exit 2 ;;
    *) POS+=("$1"); shift ;;
  esac
done
case "$CLUSTER" in dev|production) ;; *) usage ;; esac
[ -n "$TOPICS_CSV" ] || usage
if [ "$AVRO_LIVE" = yes ]; then [ "$JSON_STOPPED" = yes ] && [ "$STOPPED" != yes ] || usage; else [ "$STOPPED" = yes ] && [ "$JSON_STOPPED" != yes ] || usage; fi
[ "${#POS[@]}" -ge 1 ] && [ "${#POS[@]}" -le 2 ] || usage
BS="${POS[0]}"
[ -n "$REGISTRY" ] || usage
BIN="${POS[1]:-${KAFKA_BIN_DIR:-}}"

# --cluster is not a label: the bootstrap must BE that cluster's broker (a "dev" wipe must never reach production).
case "$CLUSTER:$BS" in
  dev:localhost:19092|dev:host.docker.internal:19092|dev:192.168.100.102:19092) ;;
  production:192.168.100.252:9092) ;;
  *) echo "FATAL: bootstrap '$BS' is not the $CLUSTER cluster's broker; refusing (nothing was changed)" >&2; exit 2 ;;
esac

# topics: valid names, no duplicates; every one has exactly one --pre-cutover-id (and --new-topic-id for --verify), no strays.
TOPICS=(); IFS=',' read -r -a TOPICS <<< "$TOPICS_CSV"
[ "${#TOPICS[@]}" -gt 0 ] || usage
seen=$'\n'
for t in "${TOPICS[@]}"; do
  case "$t" in ''|*[!A-Za-z0-9._-]*) echo "FATAL: invalid topic name '$t'" >&2; exit 2 ;; esac
  case "$seen" in *$'\n'"$t"$'\n'*) echo "FATAL: topic '$t' listed twice" >&2; exit 2 ;; esac
  seen="$seen$t"$'\n'
done
id_of() { printf '%s' "$1" | awk -F= -v t="$2" '$1 == t {n++; v = substr($0, length(t) + 2)} END {if (n == 1) print v}'; }
check_ids() { # <flag> <list> <required>
  local flag="$1" list="$2" req="$3" t line
  for t in "${TOPICS[@]}"; do
    if [ -z "$(id_of "$list" "$t")" ]; then
      [ "$req" = yes ] && { echo "FATAL: exactly one $flag $t=<id> is required" >&2; exit 2; }
    fi
  done
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$seen" in *$'\n'"${line%%=*}"$'\n'*) ;; *) echo "FATAL: $flag names '${line%%=*}', which is not in --topics" >&2; exit 2 ;; esac
  done <<< "$list"
}
check_ids --pre-cutover-id "$PRE_IDS" yes
check_ids --subject "$SUBJECTS" yes
if [ "$PHASE" = "--verify" ]; then check_ids --new-topic-id "$NEW_IDS" yes; else [ -z "$NEW_IDS" ] || usage; fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"
KT="$(rf_resolve_cli "$BIN")"
command -v jq >/dev/null || { echo "FATAL: jq is required for the exact Schema Registry subject check" >&2; exit 2; }
command -v curl >/dev/null || { echo "FATAL: curl is required for the Schema Registry check" >&2; exit 2; }
KC="$(rf_resolve_cli_named kafka-configs "$BIN")"
KG="$(rf_resolve_cli_named kafka-consumer-groups "$BIN")"
GO="$(rf_resolve_cli_named kafka-get-offsets "$BIN")"
CC="$(rf_resolve_cli_named kafka-console-consumer "$BIN")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }
STATE="$STATE_BASE/$CLUSTER"
mkdir -p "$STATE" || { echo "FATAL: cannot create state dir $STATE" >&2; exit 2; }

# shape file: partitions=N / rf=N / one `config.<key>=<value>` per DYNAMIC_TOPIC_CONFIG override, captured MACHINE-EXACT from
# `kafka-configs --describe` (one config per line: key up to the first '=', value verbatim up to ` sensitive=`). The comma-joined
# `Configs:` field of kafka-topics --describe is NOT parsed for configs: a value may contain commas and '='.
describe_field() { printf '%s\n' "$1" | tr '\t' '\n' | sed -n "s/^$2: //p" | head -1; }
rf_dynamic_configs() { # <topic> -> sorted key=value lines of the topic's dynamic overrides; non-zero if unreadable
  local out
  out="$("$KC" --bootstrap-server "$BS" --describe --entity-type topics --entity-name "$1" 2>&1)" \
    || { echo "FATAL: cannot read the configs of $1: $out" >&2; return 1; }
  printf '%s\n' "$out" | grep -q "^Dynamic configs for topic $1 are:" \
    || { echo "FATAL: unrecognised kafka-configs output for $1: $out" >&2; return 1; }
  printf '%s\n' "$out" | awk '/^[ \t]+[^ \t=]+=/ && / sensitive=(true|false) synonyms=\{DYNAMIC_TOPIC_CONFIG:/ {
    sub(/^[ \t]+/, ""); sub(/ sensitive=(true|false) synonyms=\{.*$/, ""); print }' | LC_ALL=C sort
}
capture_shape() { # <describe-line> <file> <topic-id> <topic>
  local desc="$1" file="$2" parts rf cfg
  parts="$(describe_field "$desc" PartitionCount)"; rf="$(describe_field "$desc" ReplicationFactor)"
  case "$parts" in ''|*[!0-9]*) echo "FATAL: cannot read PartitionCount from: $desc" >&2; return 1 ;; esac
  case "$rf" in ''|*[!0-9]*) echo "FATAL: cannot read ReplicationFactor from: $desc" >&2; return 1 ;; esac
  cfg="$(rf_dynamic_configs "$4")" || return 1
  { echo "cluster=$CLUSTER"; echo "topicid=$3"; echo "partitions=$parts"; echo "rf=$rf"
    [ -z "$cfg" ] || printf '%s\n' "$cfg" | sed 's/^/config./'; } > "$file.tmp" && mv "$file.tmp" "$file" \
    || { echo "FATAL: cannot write $file" >&2; return 1; }
}
# Kafka LIST-typed topic configs: only these take the [a,b] bracket form on --create --config; every other value goes verbatim.
RF_LIST_KEYS=" cleanup.policy leader.replication.throttled.replicas follower.replication.throttled.replicas "
shape_get() { sed -n "s/^$2=//p" "$1" | head -1; }

# the Avro record's 4-byte schema id must be registered under THIS topic's declared subject (not just "starts with 0x00")
check_schema_id() { # <topic> <partition> <offset>
  local topic="$1" p="$2" off="$3" subj tmp id out
  subj="$(id_of "$SUBJECTS" "$topic")"; tmp="$(mktemp)"
  "$CC" --bootstrap-server "$BS" --topic "$topic" --partition "$p" --offset "$off" --max-messages 1 --timeout-ms "${RF_WIRE_TIMEOUT_MS:-15000}" --property "null.literal=__OE_TOMBSTONE__" > "$tmp" 2>/dev/null < /dev/null || true
  # tombstone: ONLY the unique sentinel we asked the consumer to print for a null value; zero bytes is never a tombstone
  if [ "$(tr -d '\n\r' < "$tmp")" = "__OE_TOMBSTONE__" ]; then rm -f "$tmp"; return 0; fi
  id=$(od -An -tu1 -N5 "$tmp" | awk 'NR==1 {print $2*16777216 + $3*65536 + $4*256 + $5}'); rm -f "$tmp"
  case "$id" in ''|*[!0-9]*) echo "FAIL: $topic partition $p offset $off: cannot read a schema id" >&2; return 1 ;; esac
  out="$(curl -fsS "$REGISTRY/schemas/ids/$id/versions" 2>&1)" || { echo "FAIL: registry has no schema id $id (record at $topic:$p@$off): $out" >&2; return 1; }
  printf '%s' "$out" | jq -e --arg s "$subj" 'type == "array" and any(.[]; type == "object" and .subject == $s)' >/dev/null 2>&1 || { echo "FAIL: schema id $id of $topic:$p@$off is not registered under subject $subj: $out" >&2; return 1; }
}

# earliest (-2) and latest (-1) offset snapshots of ONE topic, fully validated: every line is `topic:partition:offset` with integer
# fields, each partition 0..parts-1 exactly once, nothing else. Sets OFF_EARLY / OFF_LATE.
offsets_snapshots() { # <topic> <expected-partition-count>
  local topic="$1" parts="$2" snap want_p got_p bad
  OFF_EARLY="$("$GO" --bootstrap-server "$BS" --topic "$topic" --time -2 2>&1)" || { echo "FAIL: cannot read earliest offsets of $topic: $OFF_EARLY" >&2; return 1; }
  OFF_LATE="$("$GO" --bootstrap-server "$BS" --topic "$topic" --time -1 2>&1)" || { echo "FAIL: cannot read end offsets of $topic: $OFF_LATE" >&2; return 1; }
  want_p="$(seq 0 $((parts - 1)) | paste -sd, -)"
  for snap in "$OFF_EARLY" "$OFF_LATE"; do
    bad="$(printf '%s\n' "$snap" | awk -F: -v t="$topic" 'NF == 0 {next} !($1 == t && NF == 3 && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/) {print}')"
    [ -z "$bad" ] || { echo "FAIL: malformed offset line(s) for $topic (expected $topic:<partition>:<offset>): $bad" >&2; return 1; }
    got_p="$(printf '%s\n' "$snap" | awk -F: -v t="$topic" '$1 == t {print $2}' | sort -n | paste -sd, -)"
    [ "$got_p" = "$want_p" ] || { echo "FAIL: offsets of $topic must name each partition 0..$((parts - 1)) exactly once, got [$got_p]: $snap" >&2; return 1; }
  done
}
# consumer-group rows that name <topic> must be parseable: >= 5 fields, current offset an integer or "-", log-end an integer
group_rows_valid() { # <topic> <describe-all-groups output>
  local bad
  bad="$(printf '%s\n' "$2" | awk -v t="$1" '$2 == t && !(NF >= 5 && ($4 ~ /^[0-9]+$/ || $4 == "-") && $5 ~ /^[0-9]+$/) {print}')"
  [ -z "$bad" ] || { echo "FAIL: unparseable consumer-group row(s) for $1 (cannot judge stale offsets):" >&2; printf '%s\n' "$bad" >&2; return 1; }
}

# committed offsets that the post-recreate gate rejects (ONE predicate, used by preflight and post-check): strict mode = ANY committed
# offset on the topic; live mode = only an offset beyond the log end (a WARN, judged against the NEW log, so not knowable before the delete).
group_stale_rows() { # <topic> <describe-all-groups output>
  printf '%s\n' "$2" | awk -v t="$1" -v live="$AVRO_LIVE" '$2 == t && $4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && (live != "yes" || $4 + 0 > $5 + 0) {print $1 ":" $3 "@" $4 " (log end " $5 ")"}'
}
# BOTH group gates in one place (preflight, absent-recovery, and the last look before each delete/create): rows parseable and, in strict
# mode, no committed offset on the topic (the post-check's own predicate).
groups_gate() { # <topic> <describe-all-groups output>
  local stale_pre
  group_rows_valid "$1" "$2" || return 1
  if [ "$AVRO_LIVE" != yes ]; then
    stale_pre="$(group_stale_rows "$1" "$2")"
    [ -z "$stale_pre" ] || { echo "FAIL: consumer groups hold committed offsets for $1 (group:partition@offset); they survive the delete and the post-check would reject them. DELETE them first (kafka-consumer-groups --delete-offsets --group <g> --topic $1, or delete the stopped group):" >&2; printf '%s\n' "$stale_pre" >&2; return 1; }
  fi
}
# post-checks of ONE topic, from the broker; prints the new id first so a later failure is recoverable with --verify.
post_checks() { # <topic> <pre-id> <want-new-id|"">
  local topic="$1" pre="$2" want="$3" shape="$STATE/$1.shape" desc found parts rf
  [ -f "$shape" ] || { echo "FAIL: no captured shape for $topic at $shape (the --recreate that deleted it ran elsewhere?)" >&2; return 1; }
  [ "$(shape_get "$shape" topicid)" = "$pre" ] && [ "$(shape_get "$shape" cluster)" = "$CLUSTER" ] \
    || { echo "FAIL: the shape of $topic was not captured from id $pre on cluster $CLUSTER" >&2; return 1; }
  desc="$(rf_describe "$KT" "$BS" "$topic")" || return 1
  found="$(rf_topic_id "$desc" "$topic")" || return 1
  echo "NEW TOPIC ID: $topic=$found (was $pre)"
  [ "$found" != "$pre" ] || { echo "FAIL: $topic still has the pre-cutover id $pre" >&2; return 1; }
  [ -z "$want" ] || [ "$found" = "$want" ] || { echo "FAIL: $topic has id $found, not the recreated topic $want" >&2; return 1; }
  parts="$(shape_get "$shape" partitions)"; rf="$(shape_get "$shape" rf)"
  printf '%s\n' "$desc" | tr '\t' '\n' | grep -qx "ReplicationFactor: $rf" || { echo "FAIL: $topic replication factor is not $rf: $desc" >&2; return 1; }
  printf '%s\n' "$desc" | tr '\t' '\n' | grep -qx "PartitionCount: $parts" || { echo "FAIL: $topic partition count is not $parts: $desc" >&2; return 1; }
  # two-way, exact: the broker's dynamic overrides must equal the captured set (no missing, no extra, no altered value)
  local live_cfg want_cfg
  live_cfg="$(rf_dynamic_configs "$topic")" || return 1
  want_cfg="$({ grep '^config\.' "$shape" || true; } | sed 's/^config\.//' | LC_ALL=C sort)"
  [ "$live_cfg" = "$want_cfg" ] || { echo "FAIL: $topic configs differ from the captured live set. broker: [$(echo $live_cfg)] captured: [$(echo $want_cfg)]. The topic exists EMPTY with wrong config: fix it with kafka-configs --alter (captured set: $shape), then run --verify --new-topic-id $topic=$found" >&2; return 1; }
  local early late
  offsets_snapshots "$topic" "$parts" || return 1
  early="$OFF_EARLY"; late="$OFF_LATE"
  local p e l off rc
  while IFS=: read -r p e l; do
    case "$e$l" in ''|*[!0-9]*) echo "FAIL: unreadable offsets for $topic partition $p" >&2; return 1 ;; esac
    if [ "$AVRO_LIVE" != yes ]; then
      [ "$l" -eq 0 ] || { echo "FAIL: $topic partition $p is not empty (end offset $l): a writer is live; stop it and run --recreate again (or pass --avro-writer-live if the AVRO writer is the live one)" >&2; return 1; }
    elif [ "$l" -gt "$e" ]; then
      # the Avro writer is live, so emptiness is not required; the FIRST and LAST record must be Avro (or a tombstone). A sample, not a scan.
      for off in "$e" "$((l - 1))"; do
        rc=0; rf_wire_is_avro "$CC" "$BS" "$topic" "$p" "$off" sentinel || rc=$?
        [ "$rc" -eq 0 ] || { echo "FAIL: $topic partition $p offset $off is not verifiably Avro (verdict $rc: 1 = not Avro, a JSON writer is live; 2 = unreadable). The topic is contaminated: stop the JSON writer and --recreate again with --pre-cutover-id $topic=$found" >&2; return 1; }
        check_schema_id "$topic" "$p" "$off" || return 1
      done
    fi
  done < <(paste -d: <(printf '%s\n' "$early" | awk -F: -v t="$topic" '$1 == t {print $2 ":" $3}' | sort -t: -k1,1n) \
                     <(printf '%s\n' "$late"  | awk -F: -v t="$topic" '$1 == t {print $2 ":" $3}' | sort -t: -k1,1n) | awk -F: '{print $1 ":" $2 ":" $4}')
  # stale committed offsets survive a topic delete (they are keyed by NAME): verified, not assumed
  local groups stale
  groups="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS: $groups" >&2; return 1; }
  group_rows_valid "$topic" "$groups" || return 1
  stale="$(group_stale_rows "$topic" "$groups")"
  if [ -n "$stale" ] && [ "$AVRO_LIVE" = yes ]; then
    echo "WARN: consumer groups with a committed offset BEYOND the new log end of $topic (group:partition@offset); they reset on first poll, the per-reader gate decides:" >&2
    printf '%s\n' "$stale" >&2
  elif [ -n "$stale" ]; then
    echo "FAIL: consumer groups still hold committed offsets for $topic (group:partition@offset):" >&2
    printf '%s\n' "$stale" >&2
    echo "      DELETE them (kafka-consumer-groups --delete-offsets --group <g> --topic $topic, or delete the stopped group) before any reader starts; a reset to 0 is still a committed offset." >&2
    return 1
  fi
  echo "OK: $topic"
}

fail_run() { # <topic> <hint>: stop, name what was not touched
  local t="$1" rest="" seen_t=0 x
  for x in "${TOPICS[@]}"; do [ "$seen_t" = 1 ] && rest="${rest:+$rest,}$x"; [ "$x" = "$t" ] && seen_t=1; done
  echo "STOPPED at $t. $2" >&2
  [ -z "$rest" ] || echo "NOT TOUCHED: $rest  (re-run with --topics $rest and their --pre-cutover-id)" >&2
  exit 1
}

# the subject must exist and be FULL-compatible on the registry BEFORE anything is changed (RecordNameStrategy: subject = record full name)
for t in "${TOPICS[@]}"; do
  subj="$(id_of "$SUBJECTS" "$t")"
  curl -fsS "$REGISTRY/subjects/$subj/versions/latest" >/dev/null 2>&1 || { echo "FATAL: subject $subj (topic $t) has no version on $REGISTRY; register the schema first. Nothing was changed." >&2; exit 1; }
  curl -fsS "$REGISTRY/config/$subj?defaultToGlobal=true" 2>/dev/null | grep -q '"compatibilityLevel":"FULL"' || { echo "FATAL: subject $subj is not FULL compatibility on $REGISTRY. Nothing was changed." >&2; exit 1; }
done

echo "=== roll forward ${TOPICS[*]} on $BS (cluster $CLUSTER) $PHASE ==="
if [ "$PHASE" = "--verify" ]; then
  for t in "${TOPICS[@]}"; do post_checks "$t" "$(id_of "$PRE_IDS" "$t")" "$(id_of "$NEW_IDS" "$t")" || fail_run "$t" "fix the cause and re-run --verify."; done
  echo "=== DONE: verified ${TOPICS[*]} ==="; exit 0
fi

# PREFLIGHT, all topics, before anything is deleted: the topic list is readable, each topic is present with its pre-cutover id (or absent
# with the explicit flag and a captured shape), and the live shape is captured to the state dir. One bad topic means NOTHING is deleted.
LIST="$(rf_list "$KT" "$BS")" || exit 1
GRP_TXT="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || { echo "FAIL: cannot read consumer groups on $BS; the stale-offset post-check would be blind. NOTHING was deleted." >&2; exit 1; }
PRESENT=$'\n'
for t in "${TOPICS[@]}"; do
  pre="$(id_of "$PRE_IDS" "$t")"
  if printf '%s\n' "$LIST" | grep -qx "$t"; then
    PRESENT="$PRESENT$t"$'\n'
    desc="$(rf_describe "$KT" "$BS" "$t")" || exit 1
    cur="$(rf_topic_id "$desc" "$t")" || exit 1
    [ "$cur" = "$pre" ] || { echo "FAIL: $t carries id $cur, not the pre-cutover id $pre. It was already recreated and may hold live Avro; refusing. NOTHING was deleted." >&2; exit 1; }
    capture_shape "$desc" "$STATE/$t.shape" "$cur" "$t" || exit 1
    # every read-only input of the post-checks must be readable AND well-formed NOW, for every topic, before the first delete
    offsets_snapshots "$t" "$(shape_get "$STATE/$t.shape" partitions)" || { echo "NOTHING was deleted." >&2; exit 1; }
    groups_gate "$t" "$GRP_TXT" || { echo "NOTHING was deleted." >&2; exit 1; }
    echo "-- $t id $cur: shape captured -> $(tr '\n' ' ' < "$STATE/$t.shape")"
  else
    [ "$ABSENT_OK" = yes ] || { echo "FAIL: $t does not exist. If a previous run deleted it and failed to create it, re-run with --topic-absent-after-failed-create; otherwise find out who deleted it. NOTHING was deleted." >&2; exit 1; }
    [ -f "$STATE/$t.shape" ] || { echo "FAIL: $t is absent and no captured shape exists at $STATE/$t.shape; the live shape is lost, create it by hand then --verify. NOTHING was deleted." >&2; exit 1; }
    [ "$(shape_get "$STATE/$t.shape" topicid)" = "$pre" ] && [ "$(shape_get "$STATE/$t.shape" cluster)" = "$CLUSTER" ] \
      || { echo "FAIL: the shape at $STATE/$t.shape was not captured from $t id $pre on cluster $CLUSTER (stale or foreign). NOTHING was deleted." >&2; exit 1; }
    groups_gate "$t" "$GRP_TXT" || { echo "NOTHING was created." >&2; exit 1; }
    echo "-- $t absent (recovering a failed create); captured shape $(tr '\n' ' ' < "$STATE/$t.shape")"
  fi
done

for t in "${TOPICS[@]}"; do
  pre="$(id_of "$PRE_IDS" "$t")"
  LIST="$(rf_list "$KT" "$BS")" || fail_run "$t" "broker unreadable."
  if printf '%s\n' "$LIST" | grep -qx "$t"; then
    # last look: Kafka's delete takes a NAME, not a TopicId. ONE describe supplies BOTH the id and the shape compared with the
    # preflight capture, so a same-shape replacement under a new id cannot slip between two reads.
    look="$(rf_describe "$KT" "$BS" "$t")" || fail_run "$t" "cannot re-describe the topic; nothing deleted."
    again="$(rf_topic_id "$look" "$t")" || fail_run "$t" "cannot re-read the topic id."
    [ "$again" = "$pre" ] || { echo "FAIL: $t changed id to $again while checking; nothing deleted." >&2; fail_run "$t" "topic administration is not frozen."; }
    capture_shape "$look" "$STATE/$t.lastlook" "$again" "$t" || fail_run "$t" "cannot capture the shape; nothing deleted."
    cmp -s "$STATE/$t.lastlook" "$STATE/$t.shape" || { echo "FAIL: the shape of $t changed since preflight; nothing deleted. captured: $(tr '\n' ' ' < "$STATE/$t.shape") now: $(tr '\n' ' ' < "$STATE/$t.lastlook")" >&2; fail_run "$t" "topic administration is not frozen; re-run to capture the new shape."; }
    LASTG="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || fail_run "$t" "cannot read consumer groups; nothing deleted."
    groups_gate "$t" "$LASTG" || fail_run "$t" "consumer-group gate failed at the last look; nothing deleted."
    echo "-- delete $t (id $again)"
    kt --delete --topic "$t" || fail_run "$t" "delete failed; nothing created."
    settled=0
    for _ in $(seq 1 60); do
      cur="$(rf_list "$KT" "$BS")" || fail_run "$t" "broker unreadable after delete."
      if ! printf '%s\n' "$cur" | grep -qx "$t"; then settled=1; break; fi
      sleep 2
    done
    [ "$settled" -eq 1 ] || fail_run "$t" "still present 120s after the delete; re-run once the broker settles."
  elif [ "$ABSENT_OK" != yes ]; then
    case "$PRESENT" in
      *$'\n'"$t"$'\n'*) fail_run "$t" "$t was present at preflight but is ABSENT at its turn (deleted by someone else, or an earlier part of this run?). Refusing to recreate it blind; find out why, then re-run with --topic-absent-after-failed-create." ;;
    esac
  fi
  if ! printf '%s\n' "$LIST" | grep -qx "$t"; then   # recovery of an absent topic: the group gates apply before --create too
    LASTG="$("$KG" --bootstrap-server "$BS" --describe --all-groups 2>&1)" || fail_run "$t" "cannot read consumer groups; nothing created."
    groups_gate "$t" "$LASTG" || fail_run "$t" "consumer-group gate failed before create; nothing created."
  fi
  parts="$(shape_get "$STATE/$t.shape" partitions)"; rf="$(shape_get "$STATE/$t.shape" rf)"
  args=(--create --topic "$t" --partitions "$parts" --replication-factor "$rf")
  while IFS='=' read -r k v; do
    k="${k#config.}"
    case "$RF_LIST_KEYS" in *" $k "*) case "$v" in *,*) v="[$v]" ;; esac ;; esac   # only LIST-typed configs take the bracket form; the rest verbatim
    args+=(--config "$k=$v")
  done < <(grep '^config\.' "$STATE/$t.shape" || true)
  echo "-- create $t (${parts} partitions, RF $rf, $(grep -c '^config\.' "$STATE/$t.shape") configs)"
  kt "${args[@]}" || fail_run "$t" "$t was deleted but could not be created; it is ABSENT. Re-run with --topic-absent-after-failed-create."
  post_checks "$t" "$pre" "" || fail_run "$t" "created, but a post-check failed. If no JSON was found run --verify --new-topic-id $t=<printed id>; if JSON was found re-run --recreate with --pre-cutover-id $t=<printed id>."
done
echo "=== DONE: ${TOPICS[*]} recreated. Next: deploy the Avro readers, then run the wire verification ==="
