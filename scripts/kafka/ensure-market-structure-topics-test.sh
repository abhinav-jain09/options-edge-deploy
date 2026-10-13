#!/usr/bin/env bash
# Drives the REAL ensure-market-structure-topics.sh against mocked kafka-topics / kafka-configs that print the
# broker's real `--describe --all` format (sensitive=…, synonyms={… DEFAULT_CONFIG:…}) and that can FAIL:
# absent → create → verified; right shape (either broker default) → ok; wrong shape or any CLI failure →
# fail closed. Same pattern as ensure-hedging-pressure-topics-test.sh, with two differences that are the point
# of this barrier: retention is a FLOOR (a longer retention and -1 both pass, one millisecond short does not),
# and the barrier checks the SAME SIX THINGS the service's own boot verification checks — so every one of them
# has a case here, including the three a shape-only check would miss (max.message.bytes, the replication factor
# and min.insync.replicas). A barrier that passed a topic the service refuses would leave the Deployment
# permanently unready, which is the failure this file exists to make impossible.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
UI=options.spx.market-structure.ui
SEQ=options.spx.market-structure.sequence
DLQ=options.spx.market-structure.dlq
REC=options.spx.market-structure.recovery
ALL="$UI $SEQ $DLQ $REC"

# cfgline <policy> <retention> <broker default policy> [max.message.bytes] [min.insync.replicas]:
# the real `kafka-configs --describe --all` shape, whose first key=value per line is the EFFECTIVE value.
cfgline() {
  printf 'All configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=%s}\n  max.message.bytes=%s sensitive=false synonyms={STATIC_BROKER_CONFIG:message.max.bytes=%s, DEFAULT_CONFIG:message.max.bytes=1048588}\n  min.insync.replicas=%s sensitive=false synonyms={STATIC_BROKER_CONFIG:min.insync.replicas=%s, DEFAULT_CONFIG:min.insync.replicas=1}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s}\n' "$1" "$1" "$3" "${4:-1048588}" "${4:-1048588}" "${5:-1}" "${5:-1}" "$2" "$2"
}

# A config read with a key MISSING entirely, which is what an unreadable or absent value looks like.
cfgmissing() {
  printf 'All configs for topic x are:\n  cleanup.policy=delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=delete}\n  retention.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=604800000}\n'
}

# mock <list> <partitions> <ui cfg> <sequence cfg> <dlq cfg> [fail: list|create|describe|configs] [rf]
mock() {
  mkdir -p "$T/bin"; : > "$T/created"; : > "$T/altered"
  printf '%s\n' "$3" > "$T/cfg_ui"; printf '%s\n' "$4" > "$T/cfg_seq"; printf '%s\n' "$5" > "$T/cfg_dlq"
  cfgline compact -1 delete > "$T/cfg_recovery"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${6:-}" = list ] && { echo "Timed out waiting for a node assignment" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${6:-}" = create ] && { echo "Error while executing topic command : Not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created"; echo "Created topic." ;;
  *" --describe "*) [ "${6:-}" = describe ] && { echo "Error: broker unreachable" >&2; exit 1; }
     echo "Topic: x	TopicId: AbCdEfGhIjKlMnOpQrStUv	PartitionCount: $2	ReplicationFactor: ${7:-1}	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${6:-}" = configs ] && { echo "Error: TimeoutException" >&2; exit 1; }
case " \$* " in *" --alter "*)
  [ "${6:-}" = alter ] && { echo "Error: alteration refused" >&2; exit 1; }
  echo "\$*" >> "$T/altered"; exit 0 ;; esac
case " \$* " in
  *"$UI"*) cat "$T/cfg_ui" ;;
  *"$SEQ"*) cat "$T/cfg_seq" ;;
  *"$REC"*) cat "$T/cfg_recovery" ;;
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
  && grep -q "$REC.*cleanup.policy=compact.*retention.ms=-1" "$T/created" \
  && grep -q "ok: '$UI'" "$T/out"; } \
  && ok "absent → all four created with the declared shape, then verified" || { bad "absent → create"; cat "$T/out"; }

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
mock "$ALL" 1 "$(cfgmissing)" "$TWO" "$WEEK";                      expect_fail "ui with no max.message.bytes or min.insync.replicas reported → fail"
mock "$ALL" 1 "$(cfgline delete 604800000 delete 1048575)" "$TWO" "$WEEK"; expect_fail "ui max.message.bytes one byte below an input record's bound → fail"
mock "$ALL" 1 "$(cfgline delete 604800000 delete 1048576)" "$TWO" "$WEEK"; run; rc=$?
[ $rc -eq 0 ] && ok "ui max.message.bytes exactly at the bound → ok" || { bad "max.message.bytes at the bound"; cat "$T/out"; }
mock "$ALL" 1 "$(cfgline delete 604800000 delete 1048588 0)" "$TWO" "$WEEK"; expect_fail "ui min.insync.replicas below the minimum → fail"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" "" 0;                         expect_fail "ReplicationFactor 0 → fail"
# The minimum is the DEPLOYMENT's, not this script's: an estate with three brokers raises it, and a
# single-replica topic must then be refused. Checked here because the service reads the same variable.
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"
if PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 MARKET_STRUCTURE_MIN_REPLICATION_FACTOR=3 \
     bash "$HERE/ensure-market-structure-topics.sh" > "$T/out" 2>&1; then
  bad "RF>=3 refuses a single-replica topic"; cat "$T/out"
else
  ok "a replicated estate's minimum (RF>=3) refuses a single-replica topic"
fi
mock "other.topic" 1 "$WEEK" "$TWO" "$WEEK" create;                expect_fail "create fails → fail (never reported as success)"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" list;                         expect_fail "topic list unreadable → fail"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" describe;                     expect_fail "describe fails → fail"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" configs;                      expect_fail "config describe fails → fail"

mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"
cfgline delete -1 delete > "$T/cfg_recovery"; expect_fail "recovery with delete policy refused"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"
cfgline compact,delete -1 delete > "$T/cfg_recovery"; expect_fail "recovery compact,delete refused"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"
cfgline compact 604800000 delete > "$T/cfg_recovery"; expect_fail "recovery finite retention refused"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"
cfgline compact -1 delete 1048575 > "$T/cfg_recovery"; expect_fail "recovery undersized message bound refused"

mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK" alter; expect_fail "compaction configuration failure refuses rollout"
mock "$ALL" 1 "$WEEK" "$TWO" "$WEEK"; run
if grep -q "$REC.*segment.bytes=16777216,max.compaction.lag.ms=300000" "$T/altered" && [ "$(wc -l < "$T/altered" | tr -d ' ')" = 1 ]; then
  ok "recovery cadence reconciled after reset; other topics never altered"
else bad "recovery cadence reconciliation"; cat "$T/altered"; fi

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
