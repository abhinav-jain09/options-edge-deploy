#!/usr/bin/env bash
# roll-forward-es-aggressor-flow-test.sh — the destructive script against a stateful mock kafka-topics.sh:
# it refuses without the operator's exclusion assertion, refuses a topic whose id is not the pre-cutover id (already
# recreated, may hold live Avro), deletes + recreates the topic at the declared shape with a NEW id, creates an absent
# topic, and fails when the broker cannot be listed.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
cat > "$TMP/bin/kafka-topics.sh" <<'MOCK'
#!/usr/bin/env bash
# state: $MOCK_DIR/topic (the live topic's id; absent file = no topic), $MOCK_DIR/created (the last create's args)
set -u
mode=""; topic=""; prev=""; configs=()
for a in "$@"; do
  case "$a" in --list) mode=list ;; --describe) mode=describe ;; --delete) mode=delete ;; --create) mode=create ;; esac
  [ "$prev" = "--topic" ] && topic="$a"
  [ "$prev" = "--config" ] && configs+=("$a")
  prev="$a"
done
[ "${MOCK_LIST_RC:-0}" = 0 ] || { [ "$mode" = list ] && { echo "broker down" >&2; exit 1; }; }
case "$mode" in
  list) [ -f "$MOCK_DIR/topic" ] && echo es.futures.aggressor-flow; exit 0 ;;
  describe) [ -f "$MOCK_DIR/topic" ] || exit 1
    printf 'Topic: es.futures.aggressor-flow\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: %s\tConfigs: %s\n' \
      "$(cat "$MOCK_DIR/topic")" "$(cat "$MOCK_DIR/parts" 2>/dev/null || echo 1)" "$(cat "$MOCK_DIR/rf" 2>/dev/null || echo 1)" "$(cat "$MOCK_DIR/cfg" 2>/dev/null || echo)"; exit 0 ;;
  delete) rm -f "$MOCK_DIR/topic" "$MOCK_DIR/cfg" "$MOCK_DIR/parts"; echo deleted >> "$MOCK_DIR/log"; exit 0 ;;
  create) echo "newid-$RANDOM" > "$MOCK_DIR/topic"; echo created >> "$MOCK_DIR/log"
    pp=""; p2=""; for a in "$@"; do [ "$p2" = "--partitions" ] && pp="$a"; p2="$a"; done
    echo "$pp" > "$MOCK_DIR/parts"
    (IFS=,; echo "${configs[*]}") > "$MOCK_DIR/cfg"; exit 0 ;;
esac
exit 0
MOCK
chmod +x "$TMP/bin/kafka-topics.sh"
cat > "$TMP/bin/kafka-consumer-groups.sh" <<'MOCK'
#!/usr/bin/env bash
[ "${MOCK_GROUPS_RC:-0}" = 0 ] || { echo "coordinator down" >&2; exit 1; }
printf 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n%s\n' "${MOCK_GROUPS:-}"
MOCK
chmod +x "$TMP/bin/kafka-consumer-groups.sh"

cat > "$TMP/bin/kafka-get-offsets.sh" <<'MOCK'
#!/usr/bin/env bash
for a in "$@"; do case "$a" in -2) printf 'es.futures.aggressor-flow:0:%s\n' "${MOCK_START:-0}"; exit 0 ;; esac; done
printf 'es.futures.aggressor-flow:0:%s\n' "${MOCK_END:-0}"
MOCK
chmod +x "$TMP/bin/kafka-get-offsets.sh"

cat > "$TMP/bin/kafka-console-consumer.sh" <<'MOCK'
#!/usr/bin/env bash
# MOCK_WIRE applies to every offset; MOCK_WIRE_AT_<offset> overrides one (the asymmetric cases)
off=""; prev=""
for a in "$@"; do [ "$prev" = "--offset" ] && off="$a"; prev="$a"; done
eval "specific=\${MOCK_WIRE_AT_${off}:-}"
case "${specific:-${MOCK_WIRE:-avro}}" in
  unreadable) exit 0 ;;
  avro) printf '\000\000\000\012\215some-avro' ;;
  foreignavro) printf '\000\000\000\007\310some-avro' ;;
  json) printf '{"symbol":"SPX"}' ;;
esac
exit 0
MOCK
chmod +x "$TMP/bin/kafka-console-consumer.sh"

pass=0; fail=0
check() { if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "ok    $1"; else fail=$((fail+1)); echo "FAIL  $1: expected '$3' got '$2'"; fi; }
run() { MOCK_DIR="$TMP/state" PATH="$TMP/bin:$PATH" bash "$HERE/roll-forward-es-aggressor-flow.sh" "$@" >"$TMP/out" 2>&1; echo $?; }
reset() { rm -rf "$TMP/state"; mkdir -p "$TMP/state"; [ -z "${1:-}" ] || echo "$1" > "$TMP/state/topic"; }
ARGS=(--recreate --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")

reset OLDID
check "refuses without the exclusion assertion" "$(run --recreate --cluster dev --pre-cutover-id OLDID localhost:19092 "$TMP/bin")" 2
check "nothing deleted when refused" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0

reset NEWER
check "refuses a topic that is not the pre-cutover one" "$(run "${ARGS[@]}")" 1
check "…and leaves it alone" "$(cat "$TMP/state/topic")" NEWER

reset OLDID
check "recreates the pre-cutover topic" "$(run "${ARGS[@]}")" 0
check "new id differs from the old" "$([ "$(cat "$TMP/state/topic")" != OLDID ] && echo yes)" yes
check "declared shape applied" "$(cat "$TMP/state/cfg")" "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1"

reset
check "an ABSENT topic is refused by default (somebody deleted it)" "$(run "${ARGS[@]}")" 1
check "…and nothing is created" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
reset
check "an absent topic is created only with --topic-absent-after-failed-create" "$(run "${ARGS[@]}" --topic-absent-after-failed-create)" 0

reset OLDID
check "an unlistable broker aborts before any delete" "$(MOCK_LIST_RC=1 run "${ARGS[@]}")" 1
check "…nothing deleted" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
check "an unknown cluster name is a usage error" "$(reset OLDID; run --recreate --cluster staging --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")" 2

reset OLDID
check "a bootstrap that is not the named cluster is refused" "$(run --recreate --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped 192.168.100.252:9092 "$TMP/bin")" 2
check "…and nothing deleted" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
reset OLDID
reset OLDID
check "production is REFUSED (pending the owner's permission) and nothing is deleted" "$(run --recreate --cluster production --pre-cutover-id OLDID --avro-feed-live --json-writers-stopped 192.168.100.252:9092 "$TMP/bin")" 3
check "…nothing deleted" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
check "es4 is REFUSED too" "$(run --recreate --cluster es4 --pre-cutover-id OLDID --writers-and-readers-stopped 192.168.100.4:9092 "$TMP/bin")" 3

reset OLDID
check "a group still holding an offset for the topic fails the run" "$(MOCK_GROUPS='vol-premium es.futures.aggressor-flow 0 6505 6505 0 - - -' run "${ARGS[@]}")" 1
reset OLDID
check "groups holding offsets for OTHER topics do not" "$(MOCK_GROUPS='vol-premium other.topic 0 55 55 0 - - -' run "${ARGS[@]}")" 0
reset OLDID
check "an unreadable group list fails the run" "$(MOCK_GROUPS_RC=1 run "${ARGS[@]}")" 1

# --verify: post-conditions only, no mutation; the recovery path after a failed group check
VARGS=(--verify --cluster dev --pre-cutover-id OLDID --new-topic-id RECREATED --writers-and-readers-stopped localhost:19092 "$TMP/bin")
reset OLDID
check "--verify on the still-pre-cutover topic fails and deletes nothing" "$(run "${VARGS[@]}")" 1
check "…nothing deleted by --verify" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
reset RECREATED; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify passes on a correctly recreated topic with no stale groups" "$(run "${VARGS[@]}")" 0
check "--verify fails while a group still holds an offset" "$(MOCK_GROUPS='g es.futures.aggressor-flow 0 9 9 0 - - -' run "${VARGS[@]}")" 1

check "--verify also refuses a bootstrap that is not the named cluster's broker" "$(run --verify --cluster dev --pre-cutover-id OLDID --new-topic-id RECREATED --writers-and-readers-stopped 192.168.100.252:9092 "$TMP/bin")" 2
reset RECREATED; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=delete,retention.ms=86400000" > "$TMP/state/cfg"
check "--verify rejects a topic whose shape is not the declared one" "$(run "${VARGS[@]}")" 1
reset RECREATED; echo 3 > "$TMP/state/rf"; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify rejects a different replication factor" "$(run "${VARGS[@]}")" 1

reset RECREATED; echo 11 > "$TMP/state/rf"; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify rejects replication factor 11 (not a prefix match for 1)" "$(run "${VARGS[@]}")" 1
reset RECREATED; echo 10 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify rejects 10 partitions (not a prefix match for 1)" "$(run "${VARGS[@]}")" 1
reset RECREATED; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "a recreated topic that already holds records fails (a writer is live)" "$(MOCK_END=31 run "${VARGS[@]}")" 1
reset OLDID
check "…and --recreate fails the same way when records appear after the create" "$(MOCK_END=5 run "${ARGS[@]}")" 1

reset OTHER; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify is bound to the topic --recreate created: another id (not the pre-cutover one) is refused" "$(run "${VARGS[@]}")" 1
check "--verify without --new-topic-id is a usage error" "$(run --verify --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")" 2
reset OLDID; run "${ARGS[@]}" >/dev/null; check "--recreate reports the new topic id for --verify" "$(grep -c 'NEW TOPIC ID: newid-' "$TMP/out")" 1

reset OLDID; MOCK_GROUPS='g es.futures.aggressor-flow 0 9 9 0 - - -' run "${ARGS[@]}" >/dev/null
check "a failed group check still printed the new id for --verify" "$(grep -c 'NEW TOPIC ID: newid-' "$TMP/out")" 1
reset OLDID; MOCK_END=5 run "${ARGS[@]}" >/dev/null
check "a failed emptiness check still printed the new id for --verify" "$(grep -c 'NEW TOPIC ID: newid-' "$TMP/out")" 1

# --avro-feed-live: the Avro feed is writing while the topic is recreated (Jenkins cannot stop it)
LARGS=(--recreate --cluster dev --pre-cutover-id OLDID --avro-feed-live --json-writers-stopped localhost:19092 "$TMP/bin")
reset OLDID
check "without --avro-feed-live a live writer (records present) fails the run" "$(MOCK_END=12 run "${ARGS[@]}")" 1
reset OLDID
check "with --avro-feed-live, Avro records already present are accepted" "$(MOCK_END=12 run "${LARGS[@]}")" 0
reset OLDID
check "with --avro-feed-live, a JSON record present still fails (a JSON writer is live)" "$(MOCK_END=12 MOCK_WIRE=json run "${LARGS[@]}")" 1
reset OLDID
check "avro-live: Avro first / JSON LAST fails (the last endpoint is checked)" "$(MOCK_END=12 MOCK_WIRE_AT_11=json run "${LARGS[@]}")" 1
reset OLDID
check "avro-live: JSON FIRST / Avro last fails (the first endpoint is checked)" "$(MOCK_END=12 MOCK_WIRE_AT_0=json run "${LARGS[@]}")" 1
reset OLDID
check "avro-live: an unreadable endpoint fails closed" "$(MOCK_END=12 MOCK_WIRE_AT_11=unreadable run "${LARGS[@]}")" 1
reset OLDID
check "avro-live reads the EARLIEST retained offset, not 0 (a compacted-away 0 must not matter)" "$(MOCK_START=5 MOCK_END=12 MOCK_WIRE_AT_0=json run "${LARGS[@]}")" 0
reset OLDID
check "avro-live: the retained earliest record being JSON fails even when 0 is Avro" "$(MOCK_START=5 MOCK_END=12 MOCK_WIRE_AT_5=json run "${LARGS[@]}")" 1
reset OLDID
check "avro-live needs --json-writers-stopped, not the strict attestation" "$(run --recreate --cluster dev --pre-cutover-id OLDID --avro-feed-live --writers-and-readers-stopped localhost:19092 "$TMP/bin")" 2
reset OLDID
check "the strict attestation cannot be combined with --json-writers-stopped alone" "$(run --recreate --cluster dev --pre-cutover-id OLDID --json-writers-stopped localhost:19092 "$TMP/bin")" 2
reset OLDID
check "with --avro-feed-live, a reader offset INSIDE the new log is legitimate" "$(MOCK_END=12 MOCK_GROUPS='g es.futures.aggressor-flow 0 7 12 5 - - -' run "${LARGS[@]}")" 0
reset OLDID
check "with --avro-feed-live, a reader offset BEYOND the new log end is reported (WARN), not fatal: step 7 gates each reader" "$(MOCK_END=12 MOCK_GROUPS='g es.futures.aggressor-flow 0 6505 12 0 - - -' run "${LARGS[@]}")" 0
check "…and the WARN names the group" "$(grep -c 'WARN: consumer groups' "$TMP/out")" 1

reset OLDID
check "avro-live: Avro under a FOREIGN schema id fails (only id 2701 passes)" "$(MOCK_END=12 MOCK_WIRE=foreignavro run "${LARGS[@]}")" 1

echo "passed=$pass failed=$fail"; [ "$fail" = 0 ]
