#!/usr/bin/env bash
# Drives the REAL ensure-market-structure-topics.sh against mocked kafka-topics / kafka-configs that print the
# broker's real --describe format (sensitive=…, synonyms={… DEFAULT_CONFIG:log.cleanup.policy=…}) and that can
# FAIL: absent → create → verified; right shape (either broker default) → ok; wrong shape or any CLI failure →
# fail closed. Same pattern as ensure-hedging-pressure-topics-test.sh, with ONE difference that is the point of
# this barrier: retention is a FLOOR, so a longer retention and -1 both pass while a shorter one does not.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
UI=options.spx.market-structure.ui
SEQ=options.spx.market-structure.sequence
DLQ=options.spx.market-structure.dlq
ALL="$UI $SEQ $DLQ"

# cfgline <policy> <retention> <broker default policy>: the real kafka-configs --describe shape
cfgline() {
  printf 'Dynamic configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=%s}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s, STATIC_BROKER_CONFIG:log.retention.ms=86400000}\n' "$1" "$1" "$3" "$2" "$2"
}

# mock <list> <partitions> <ui cfg> <sequence cfg> <dlq cfg> [fail: list|create|describe|configs]
mock() {
  mkdir -p "$T/bin"; : > "$T/created"
  printf '%s\n' "$3" > "$T/cfg_ui"; printf '%s\n' "$4" > "$T/cfg_seq"; printf '%s\n' "$5" > "$T/cfg_dlq"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${6:-}" = list ] && { echo "Timed out waiting for a node assignment" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${6:-}" = create ] && { echo "Error while executing topic command : Not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created"; echo "Created topic." ;;
  *" --describe "*) [ "${6:-}" = describe ] && { echo "Error: broker unreachable" >&2; exit 1; }
     echo "Topic: x	TopicId: AbCdEfGhIjKlMnOpQrStUv	PartitionCount: $2	ReplicationFactor: 1	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${6:-}" = configs ] && { echo "Error: TimeoutException" >&2; exit 1; }
case " \$* " in
  *"$UI"*) cat "$T/cfg_ui" ;;
  *"$SEQ"*) cat "$T/cfg_seq" ;;
  *) cat "$T/cfg_dlq" ;;
esac
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-market-structure-topics.sh" > "$T/out" 2>&1; }
expect_fail() { run; if [ $? -ne 0 ]; then ok "$1"; else bad "$1"; cat "$T/out"; fi; }

WEEK=$(cfgline delete 604800000 delete)
TWO=$(cfgline delete 172800000 delete)

mock "other.topic" 1 "$WEEK" "$TWO" "$WEEK"; run; rc=$?
{ [ $rc -eq 0 ] && grep -q "$UI.*cleanup.policy=delete.*retention.ms=604800000" "$T/created" \
  && grep -q "$SEQ.*cleanup.policy=delete.*retention.ms=172800000" "$T/created" \
  && grep -q "$DLQ.*cleanup.policy=delete.*retention.ms=604800000" "$T/created" \
  && grep -q "ok: '$UI'" "$T/out"; } \
  && ok "absent → all three created with the declared shape, then verified" || { bad "absent → create"; cat "$T/out"; }

mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"; run; rc=$?
{ [ $rc -eq 0 ] && [ ! -s "$T/created" ]; } && ok "present + right shape → ok, nothing created" || { bad "present ok"; cat "$T/out"; }

mock "$ALL" 1 "$(cfgline delete 604800000 compact)" "$(cfgline delete 172800000 compact)" "$(cfgline delete 604800000 compact)"; run; rc=$?
[ $rc -eq 0 ] && ok "right shape with a COMPACT broker default → ok (synonyms are not read)" || { bad "compact default"; cat "$T/out"; }

# The floor, in both directions: a LONGER retention and -1 pass; one millisecond short does not.
mock "$ALL" 1 "$(cfgline delete 999999999 delete)" "$TWO" "$WEEK"; run; rc=$?
[ $rc -eq 0 ] && ok "ui retention above the floor → ok (the requirement is a floor)" || { bad "above floor"; cat "$T/out"; }

mock "$ALL" 1 "$(cfgline delete -1 delete)" "$TWO" "$WEEK"; run; rc=$?
[ $rc -eq 0 ] && ok "ui infinite retention → ok (-1 satisfies any floor)" || { bad "infinite"; cat "$T/out"; }

mock "$ALL" 1 "$(cfgline delete 604799999 delete)" "$TWO" "$WEEK"; expect_fail "ui one ms below the floor → fail"
mock "$ALL" 1 "$WEEK" "$(cfgline delete 172799999 delete)" "$WEEK"; expect_fail "sequence below its own floor → fail"
mock "$ALL" 32 "$WEEK" "$TWO" "$WEEK";                             expect_fail "32 partitions (the dev auto-create default) → fail"
mock "$ALL" 1 "$(cfgline compact 604800000 delete)" "$TWO" "$WEEK"; expect_fail "ui compacted → fail"
mock "$ALL" 1 "$(cfgline compact,delete 604800000 delete)" "$TWO" "$WEEK"; expect_fail "ui compact,delete → fail"
mock "$ALL" 1 "Dynamic configs for topic x are:" "$TWO" "$WEEK";   expect_fail "ui inherits the cluster retention → fail"
mock "other.topic" 1 "$WEEK" "$TWO" "$WEEK" create;                expect_fail "create fails → fail (never reported as success)"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" list;                         expect_fail "topic list unreadable → fail"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" describe;                     expect_fail "describe fails → fail"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" configs;                      expect_fail "config describe fails → fail"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
