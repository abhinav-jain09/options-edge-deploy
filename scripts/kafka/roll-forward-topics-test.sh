#!/usr/bin/env bash
# roll-forward-topics-test.sh — roll-forward-topics.sh against a stateful per-topic mock Kafka CLI set.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
cat > "$TMP/bin/kafka-topics.sh" <<'MOCK'
#!/usr/bin/env bash
# state: $MOCK_DIR/<topic>/{id,parts,rf,cfg (one key=value per line)}; $MOCK_DIR/log: deleted:<t> / created:<t> lines
set -u
mode=""; topic=""; prev=""; configs=(); parts=""; rf=""
for a in "$@"; do
  case "$a" in --list) mode=list ;; --describe) mode=describe ;; --delete) mode=delete ;; --create) mode=create ;; esac
  case "$prev" in --topic) topic="$a" ;; --config) configs+=("$a") ;; --partitions) parts="$a" ;; --replication-factor) rf="$a" ;; esac
  prev="$a"
done
d="$MOCK_DIR/$topic"
[ "${MOCK_LIST_RC:-0}" = 0 ] || { [ "$mode" = list ] && { echo "broker down" >&2; exit 1; }; }
case "$mode" in
  list) for t in "$MOCK_DIR"/*/; do [ -f "$t/id" ] && basename "$t"; done; exit 0 ;;
  describe) [ -f "$d/id" ] || exit 1
    # like a broker: list values print WITHOUT brackets, joined by commas
    cfg="$(sed 's/\[\(.*\)\]/\1/' "$d/cfg" 2>/dev/null | paste -sd, -)"
    printf 'Topic: %s\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: %s\tConfigs: %s\n' "$topic" "$(cat "$d/id")" "$(cat "$d/parts")" "$(cat "$d/rf")" "$cfg"; exit 0 ;;
  delete) rm -f "$d/id" "$d/cfg" "$d/parts" "$d/rf"; echo "deleted:$topic" >> "$MOCK_DIR/log"; exit 0 ;;
  create) [ "${MOCK_CREATE_FAIL:-}" = "$topic" ] && { echo "create refused" >&2; exit 1; }
    mkdir -p "$d"; echo "newid-$RANDOM" > "$d/id"; echo "$parts" > "$d/parts"; echo "$rf" > "$d/rf"
    printf '%s\n' "${configs[@]}" > "$d/cfg"; echo "created:$topic" >> "$MOCK_DIR/log"; exit 0 ;;
esac
exit 0
MOCK
cat > "$TMP/bin/kafka-consumer-groups.sh" <<'MOCK'
#!/usr/bin/env bash
[ "${MOCK_GROUPS_RC:-0}" = 0 ] || { echo "coordinator down" >&2; exit 1; }
printf 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n%s\n' "${MOCK_GROUPS:-}"
MOCK
cat > "$TMP/bin/kafka-get-offsets.sh" <<'MOCK'
#!/usr/bin/env bash
# every topic's partition count comes from the mock state; MOCK_END_<topic with . as _> overrides MOCK_END for one topic
topic=""; prev=""; time=""
for a in "$@"; do [ "$prev" = "--topic" ] && topic="$a"; [ "$prev" = "--time" ] && time="$a"; prev="$a"; done
n="$(cat "$MOCK_DIR/$topic/parts")"; v="MOCK_END_${topic//[.-]/_}"; end="${!v:-${MOCK_END:-0}}"
[ "${MOCK_OFFSETS_PARTIAL:-}" = "$topic" ] && n=$((n - 1))
for ((p = 0; p < n; p++)); do
  if [ "$time" = -2 ]; then echo "$topic:$p:${MOCK_START:-0}"; else echo "$topic:$p:$end"; fi
done
MOCK
cat > "$TMP/bin/kafka-console-consumer.sh" <<'MOCK'
#!/usr/bin/env bash
off=""; prev=""; topic=""
for a in "$@"; do [ "$prev" = "--offset" ] && off="$a"; [ "$prev" = "--topic" ] && topic="$a"; prev="$a"; done
eval "specific=\${MOCK_WIRE_AT_${off}:-}"; v="MOCK_WIRE_${topic//[.-]/_}"
case "${specific:-${!v:-${MOCK_WIRE:-avro}}}" in
  unreadable) exit 0 ;;
  avro) printf '\000\000\000\007\310some-avro' ;;
  json) printf '{"symbol":"SPX"}' ;;
esac
exit 0
MOCK
chmod +x "$TMP/bin/"*.sh

pass=0; fail=0
check() { if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "ok    $1"; else fail=$((fail+1)); echo "FAIL  $1: expected '$3' got '$2'"; fi; }
run() { RF_STATE_DIR="$TMP/shapes" MOCK_DIR="$TMP/state" PATH="$TMP/bin:$PATH" bash "$HERE/roll-forward-topics.sh" "$@" >"$TMP/out" 2>&1; echo $?; }
mk() { # <topic> <id> <parts> <rf> <cfg lines...>
  local t="$1"; mkdir -p "$TMP/state/$t"; echo "$2" > "$TMP/state/$t/id"; echo "$3" > "$TMP/state/$t/parts"; echo "$4" > "$TMP/state/$t/rf"
  shift 4; printf '%s\n' "$@" > "$TMP/state/$t/cfg"
}
reset() { rm -rf "$TMP/state" "$TMP/shapes"; mkdir -p "$TMP/state"; }
logn() { wc -l < "$TMP/state/log" 2>/dev/null | tr -d ' ' || echo 0; }
two() { reset; mk a.one OLDA 1 1 "cleanup.policy=[compact,delete]" "retention.ms=-1" "min.insync.replicas=1"; mk b.two OLDB 32 1 "cleanup.policy=delete" "retention.ms=86400000"; mk c.three OLDC 4 1 "segment.bytes=1073741824"; }
STOP=(--writers-and-readers-stopped)
LIVE=(--avro-writer-live --json-writers-stopped)
DEV=(--cluster dev localhost:19092 "$TMP/bin")
ids() { local o=(); for p in "$@"; do o+=(--pre-cutover-id "$p"); done; printf '%s\n' "${o[@]}"; }
# R <flags...>: --recreate a.one,b.two with their right ids
RA=(--recreate --cluster dev --topics a.one,b.two --pre-cutover-id a.one=OLDA --pre-cutover-id b.two=OLDB)

two
check "refuses without any exclusion assertion" "$(run "${RA[@]}" localhost:19092 "$TMP/bin")" 2
check "refuses --json-writers-stopped alone" "$(run "${RA[@]}" --json-writers-stopped localhost:19092 "$TMP/bin")" 2
check "refuses the strict attestation combined with --avro-writer-live" "$(run "${RA[@]}" --avro-writer-live "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "…nothing deleted" "$(logn)" 0
check "unknown cluster is a usage error" "$(run --recreate --cluster staging --topics a.one --pre-cutover-id a.one=OLDA "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "a bootstrap that is not the named cluster's is refused (dev vs prod)" "$(run "${RA[@]}" "${STOP[@]}" 192.168.100.252:9092 "$TMP/bin")" 2
check "production with a dev bootstrap is refused" "$(run --recreate --cluster production --topics a.one --pre-cutover-id a.one=OLDA "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "a missing --pre-cutover-id is a usage error" "$(run --recreate --cluster dev --topics a.one,b.two --pre-cutover-id a.one=OLDA "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "a --pre-cutover-id for a topic not in --topics is refused" "$(run "${RA[@]}" --pre-cutover-id z=Q "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "a repeated --pre-cutover-id for one topic is refused" "$(run "${RA[@]}" --pre-cutover-id a.one=OTHER "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "a duplicate topic is refused" "$(run --recreate --cluster dev --topics a.one,a.one --pre-cutover-id a.one=OLDA "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "an invalid topic name is refused" "$(run --recreate --cluster dev --topics 'a one' --pre-cutover-id 'a one=X' "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "--new-topic-id with --recreate is a usage error" "$(run "${RA[@]}" --new-topic-id a.one=N "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "…still nothing deleted" "$(logn)" 0

two
check "recreates both topics" "$(run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 0
check "a.one has a new id" "$([ "$(cat "$TMP/state/a.one/id")" != OLDA ] && echo yes)" yes
check "b.two has a new id" "$([ "$(cat "$TMP/state/b.two/id")" != OLDB ] && echo yes)" yes
check "c.three (not listed) untouched" "$(cat "$TMP/state/c.three/id")" OLDC
check "live partition count copied (32)" "$(cat "$TMP/state/b.two/parts")" 32
check "live configs copied" "$(paste -sd, - < "$TMP/state/b.two/cfg")" "cleanup.policy=delete,retention.ms=86400000"
check "a comma-valued cleanup.policy is created in bracket form" "$(grep -c '^cleanup.policy=\[compact,delete\]$' "$TMP/state/a.one/cfg")" 1
check "…and every other config of the live topic came with it" "$(grep -c 'retention.ms=-1\|min.insync.replicas=1' "$TMP/state/a.one/cfg")" 2
check "new ids printed for both" "$(grep -c '^NEW TOPIC ID: ' "$TMP/out")" 2

two
check "ONE wrong pre-cutover id refuses the whole run" "$(run --recreate --cluster dev --topics a.one,b.two --pre-cutover-id a.one=OLDA --pre-cutover-id b.two=WRONG "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "…and NOTHING was deleted, even the topic with the right id" "$(logn)" 0
check "…the right-id topic is intact" "$(cat "$TMP/state/a.one/id")" OLDA

two
check "an unlistable broker aborts before any delete" "$(MOCK_LIST_RC=1 run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "…nothing deleted" "$(logn)" 0

two; rm -rf "$TMP/state/b.two"
check "an ABSENT topic is refused by default" "$(run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "…and the present topic was not deleted either" "$(logn)" 0

# multi-topic failure: b.two's create fails after its delete; c.three never touched; recoverable
two
check "a failed create stops the run" "$(MOCK_CREATE_FAIL=b.two run --recreate --cluster dev --topics a.one,b.two,c.three --pre-cutover-id a.one=OLDA --pre-cutover-id b.two=OLDB --pre-cutover-id c.three=OLDC "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "…a.one was completed" "$([ "$(cat "$TMP/state/a.one/id")" != OLDA ] && echo yes)" yes
check "…b.two is absent" "$([ -f "$TMP/state/b.two/id" ] && echo present || echo absent)" absent
check "…c.three is untouched" "$(cat "$TMP/state/c.three/id")" OLDC
check "…the run names the topics not touched" "$(grep -c 'NOT TOUCHED: c.three' "$TMP/out")" 1
check "re-run for the remainder with the absent flag finishes b.two (shape captured before the delete)" "$(run --recreate --cluster dev --topics b.two,c.three --pre-cutover-id b.two=OLDB --pre-cutover-id c.three=OLDC --topic-absent-after-failed-create "${STOP[@]}" localhost:19092 "$TMP/bin")" 0
check "…with the LIVE shape of b.two (32 partitions)" "$(cat "$TMP/state/b.two/parts")" 32
check "…and c.three recreated" "$([ "$(cat "$TMP/state/c.three/id")" != OLDC ] && echo yes)" yes

# post-check failures
two
check "strict: records in a recreated topic fail (a writer is live)" "$(MOCK_END=5 run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "…and the next topic is not touched" "$(cat "$TMP/state/b.two/id")" OLDB
check "…the new id was still printed for --verify" "$(grep -c '^NEW TOPIC ID: a.one=newid-' "$TMP/out")" 1
two
check "strict: a committed offset for the topic fails" "$(MOCK_GROUPS='g a.one 0 6505 6505 0 - - -' run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
two
check "a committed offset for ANOTHER topic does not" "$(MOCK_GROUPS='g other 0 5 5 0 - - -' run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 0
two
check "an unreadable group list fails" "$(MOCK_GROUPS_RC=1 run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
two
check "partial offsets (a partition missing) fail" "$(MOCK_OFFSETS_PARTIAL=b.two run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1

# avro-writer-live
LV=(--avro-writer-live --json-writers-stopped)
two
check "avro-live accepts Avro records already present" "$(MOCK_END=12 run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 0
two
check "avro-live: JSON present fails" "$(MOCK_END=12 MOCK_WIRE=json run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 1
two
check "avro-live: JSON LAST fails" "$(MOCK_END=12 MOCK_WIRE_AT_11=json run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 1
two
check "avro-live: JSON FIRST fails" "$(MOCK_END=12 MOCK_WIRE_AT_0=json run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 1
two
check "avro-live: an unreadable end fails closed" "$(MOCK_END=12 MOCK_WIRE_AT_11=unreadable run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 1
two
check "avro-live reads the EARLIEST retained offset (a compacted-away 0 is not checked)" "$(MOCK_START=5 MOCK_END=12 MOCK_WIRE_AT_0=json run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 0
two
check "avro-live: a reader offset inside the log is fine" "$(MOCK_END=12 MOCK_GROUPS='g a.one 0 7 12 5 - - -' run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 0
two
check "avro-live: an offset BEYOND the log end is a WARN, not a failure" "$(MOCK_END=12 MOCK_GROUPS='g a.one 0 6505 12 0 - - -' run "${RA[@]}" "${LV[@]}" localhost:19092 "$TMP/bin")" 0
check "…and the WARN is printed" "$(grep -c 'WARN: consumer groups' "$TMP/out")" 1

# --verify
two; run "${RA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin" >/dev/null
NA="$(cat "$TMP/state/a.one/id")"; NB="$(cat "$TMP/state/b.two/id")"
VA=(--verify --cluster dev --topics a.one,b.two --pre-cutover-id a.one=OLDA --pre-cutover-id b.two=OLDB --new-topic-id a.one="$NA" --new-topic-id b.two="$NB")
nlog="$(logn)"
check "--verify passes on the recreated topics" "$(run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 0
check "--verify mutates nothing" "$(logn)" "$nlog"
check "--verify without --new-topic-id is a usage error" "$(run --verify --cluster dev --topics a.one --pre-cutover-id a.one=OLDA "${STOP[@]}" localhost:19092 "$TMP/bin")" 2
check "--verify with the wrong new id fails" "$(run --verify --cluster dev --topics a.one --pre-cutover-id a.one=OLDA --new-topic-id a.one=other "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "--verify fails while a group holds an offset" "$(MOCK_GROUPS='g a.one 0 9 9 0 - - -' run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
check "--verify refuses a bootstrap that is not the cluster's broker" "$(run "${VA[@]}" "${STOP[@]}" 192.168.100.252:9092 "$TMP/bin")" 2
echo 3 > "$TMP/state/a.one/rf"
check "--verify rejects a changed replication factor" "$(run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
echo 1 > "$TMP/state/a.one/rf"; echo 10 > "$TMP/state/a.one/parts"
check "--verify rejects 10 partitions (not a prefix match for 1)" "$(run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
echo 1 > "$TMP/state/a.one/parts"; echo "retention.ms=5" > "$TMP/state/a.one/cfg"
check "--verify rejects a different config" "$(run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1
echo "retention.ms=-1" > "$TMP/state/b.two/cfg"
rm -f "$TMP/shapes/dev/a.one.shape"
check "--verify with no captured shape fails closed" "$(run "${VA[@]}" "${STOP[@]}" localhost:19092 "$TMP/bin")" 1

echo "passed=$pass failed=$fail"; [ "$fail" = 0 ]
