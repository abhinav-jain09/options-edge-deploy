#!/usr/bin/env bash
# roll-forward-underlying-spx-price-test.sh — the destructive script against a stateful mock kafka-topics.sh:
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
  list) [ -f "$MOCK_DIR/topic" ] && echo underlying.spx.price; exit 0 ;;
  describe) [ -f "$MOCK_DIR/topic" ] || exit 1
    printf 'Topic: underlying.spx.price\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: %s\tConfigs: %s\n' \
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

pass=0; fail=0
check() { if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "ok    $1"; else fail=$((fail+1)); echo "FAIL  $1: expected '$3' got '$2'"; fi; }
run() { MOCK_DIR="$TMP/state" PATH="$TMP/bin:$PATH" bash "$HERE/roll-forward-underlying-spx-price.sh" "$@" >"$TMP/out" 2>&1; echo $?; }
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
check "creates an absent topic" "$(run "${ARGS[@]}")" 0

reset OLDID
check "an unlistable broker aborts before any delete" "$(MOCK_LIST_RC=1 run "${ARGS[@]}")" 1
check "…nothing deleted" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
check "an unknown cluster name is a usage error" "$(reset OLDID; run --recreate --cluster staging --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")" 2

reset OLDID
check "a bootstrap that is not the named cluster is refused" "$(run --recreate --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped 192.168.100.252:9092 "$TMP/bin")" 2
check "…and nothing deleted" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
check "production is not enabled (no recorded shape), whatever the bootstrap" "$(run --recreate --cluster production --pre-cutover-id OLDID --writers-and-readers-stopped 192.168.100.252:9092 "$TMP/bin")" 2
check "…and its refusal says why" "$(grep -c 'has not been recorded' "$TMP/out")" 1
check "production with a non-prod bootstrap is refused too" "$(run --recreate --cluster production --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")" 2

reset OLDID
check "a group still holding an offset for the topic fails the run" "$(MOCK_GROUPS='vol-premium underlying.spx.price 0 6505 6505 0 - - -' run "${ARGS[@]}")" 1
reset OLDID
check "groups holding offsets for OTHER topics do not" "$(MOCK_GROUPS='vol-premium other.topic 0 55 55 0 - - -' run "${ARGS[@]}")" 0
reset OLDID
check "an unreadable group list fails the run" "$(MOCK_GROUPS_RC=1 run "${ARGS[@]}")" 1

# --verify: post-conditions only, no mutation; the recovery path after a failed group check
VARGS=(--verify --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped localhost:19092 "$TMP/bin")
reset OLDID
check "--verify on the still-pre-cutover topic fails and deletes nothing" "$(run "${VARGS[@]}")" 1
check "…nothing deleted by --verify" "$(cat "$TMP/state/log" 2>/dev/null | wc -l | tr -d ' ')" 0
reset RECREATED; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify passes on a correctly recreated topic with no stale groups" "$(run "${VARGS[@]}")" 0
check "--verify fails while a group still holds an offset" "$(MOCK_GROUPS='g underlying.spx.price 0 9 9 0 - - -' run "${VARGS[@]}")" 1

check "--verify also refuses a bootstrap that is not the named cluster's broker" "$(run --verify --cluster dev --pre-cutover-id OLDID --writers-and-readers-stopped 192.168.100.252:9092 "$TMP/bin")" 2
reset RECREATED; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=delete,retention.ms=86400000" > "$TMP/state/cfg"
check "--verify rejects a topic whose shape is not the declared one" "$(run "${VARGS[@]}")" 1
reset RECREATED; echo 3 > "$TMP/state/rf"; echo 1 > "$TMP/state/parts"; echo "cleanup.policy=compact,delete,retention.ms=-1,delete.retention.ms=3600000,min.cleanable.dirty.ratio=0.01,segment.ms=3600000,min.insync.replicas=1" > "$TMP/state/cfg"
check "--verify rejects a different replication factor" "$(run "${VARGS[@]}")" 1

echo "passed=$pass failed=$fail"; [ "$fail" = 0 ]
