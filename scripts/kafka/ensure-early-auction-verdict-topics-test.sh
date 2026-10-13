#!/usr/bin/env bash
# Drives the REAL ensure-early-auction-verdict-topics.sh against mocked kafka-topics / kafka-configs
# printing the broker's real `--describe --all` format. Same pattern as
# ensure-market-structure-topics-test.sh, with the one difference that is the point of this barrier:
# the ledger's retention is EXACTLY -1 — a finite retention, however long, silently deletes evidence
# the promotion gate reads, so "big enough" must FAIL here where a rebuildable topic's floor passes.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
LEDGER=early-auction.verdicts.shadow

cfgline() { # <policy> <retention> <broker default policy> [max.message.bytes] [min.insync.replicas]
  printf 'All configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=%s}\n  max.message.bytes=%s sensitive=false synonyms={STATIC_BROKER_CONFIG:message.max.bytes=%s, DEFAULT_CONFIG:message.max.bytes=1048588}\n  min.insync.replicas=%s sensitive=false synonyms={STATIC_BROKER_CONFIG:min.insync.replicas=%s, DEFAULT_CONFIG:min.insync.replicas=1}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s}\n' "$1" "$1" "$3" "${4:-1048588}" "${4:-1048588}" "${5:-1}" "${5:-1}" "$2" "$2"
}

mock() { # <list> <partitions> <cfg> [fail: list|create|describe|configs] [rf]
  mkdir -p "$T/bin"; : > "$T/created"
  printf '%s\n' "$3" > "$T/cfg"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${4:-}" = list ] && { echo "Timed out waiting for a node assignment" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${4:-}" = create ] && { echo "Error while executing topic command : Not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created"; echo "Created topic." ;;
  *" --describe "*) [ "${4:-}" = describe ] && { echo "Error: broker unreachable" >&2; exit 1; }
     echo "Topic: x	TopicId: AbCdEfGhIjKlMnOpQrStUv	PartitionCount: $2	ReplicationFactor: ${5:-1}	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${4:-}" = configs ] && { echo "Error: TimeoutException" >&2; exit 1; }
cat "$T/cfg"
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-early-auction-verdict-topics.sh" > "$T/out" 2>&1; }
expect_fail() { run; if [ $? -ne 0 ]; then ok "$1"; else bad "$1"; cat "$T/out"; fi; }

GOOD=$(cfgline delete -1 delete)

mock "other.topic" 1 "$GOOD"; run; rc=$?
{ [ $rc -eq 0 ] && grep -q -- "$LEDGER.*cleanup.policy=delete.*retention.ms=-1" "$T/created" \
  && grep -q "ok: '$LEDGER'" "$T/out"; } \
  && ok "absent → created with the declared shape, then verified" || { bad "absent → create"; cat "$T/out"; }

mock "$LEDGER" 1 "$GOOD"; run; rc=$?
{ [ $rc -eq 0 ] && [ ! -s "$T/created" ]; } && ok "present + right shape → ok, nothing created" || { bad "present ok"; cat "$T/out"; }

mock "$LEDGER" 1 "$(cfgline delete -1 compact)"; run; rc=$?
[ $rc -eq 0 ] && ok "right shape with a COMPACT broker default → ok (synonyms are not read)" || { bad "compact default"; cat "$T/out"; }

# EXACT -1: a finite retention fails NO MATTER HOW LONG — this is the barrier's whole point.
mock "$LEDGER" 1 "$(cfgline delete 999999999999 delete)"; expect_fail "finite retention, however long → fail (evidence must never be deleted)"
mock "$LEDGER" 1 "$(cfgline delete 604800000 delete)";    expect_fail "one-week retention → fail"
mock "$LEDGER" 32 "$GOOD";                                 expect_fail "32 partitions (the dev auto-create default) → fail"
mock "$LEDGER" 1 "$(cfgline compact -1 delete)";           expect_fail "compacted → fail"
mock "$LEDGER" 1 "$(cfgline compact,delete -1 delete)";    expect_fail "compact,delete → fail"
mock "$LEDGER" 1 "$(cfgline delete -1 delete 1048575)";    expect_fail "max.message.bytes one byte below the bound → fail"
mock "$LEDGER" 1 "$(cfgline delete -1 delete 1048576)"; run; rc=$?
[ $rc -eq 0 ] && ok "max.message.bytes exactly at the bound → ok" || { bad "max.message.bytes at the bound"; cat "$T/out"; }
mock "$LEDGER" 1 "$(cfgline delete -1 delete 1048588 0)";  expect_fail "min.insync.replicas below the minimum → fail"
mock "$LEDGER" 1 "$GOOD" "" 0;                             expect_fail "ReplicationFactor 0 → fail"
mock "$LEDGER" 1 "$GOOD"
if PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 EAV_MIN_REPLICATION_FACTOR=3 \
     bash "$HERE/ensure-early-auction-verdict-topics.sh" > "$T/out" 2>&1; then
  bad "RF>=3 refuses a single-replica topic"; cat "$T/out"
else
  ok "a replicated estate's minimum (RF>=3) refuses a single-replica topic"
fi
mock "other.topic" 1 "$GOOD" create;   expect_fail "create fails → fail (never reported as success)"
mock "$LEDGER" 1 "$GOOD" list;         expect_fail "topic list unreadable → fail"
mock "$LEDGER" 1 "$GOOD" describe;     expect_fail "describe fails → fail"
mock "$LEDGER" 1 "$GOOD" configs;      expect_fail "config describe fails → fail"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
