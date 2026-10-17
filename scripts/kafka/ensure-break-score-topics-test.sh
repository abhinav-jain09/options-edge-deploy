#!/usr/bin/env bash
# Drives the REAL ensure-break-score-topics.sh against mocked kafka-topics / kafka-configs printing the broker's real
# `--describe --all` format. The point of this barrier (the same contract EventTopicContract.verify enforces at service start):
# exactly one partition, cleanup.policy exactly delete, retention.ms exactly -1, retention.bytes a FINITE POSITIVE cap, and
# message.timestamp.type CreateTime. An uncapped durable topic is what grew another board to 64 GiB.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
TOPIC=futures.break-score.events

cfgline() { # <policy> <retention.ms> <retention.bytes|""> <timestamp type> [broker default policy]
  printf 'All configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=%s}\n  message.timestamp.type=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:message.timestamp.type=%s, DEFAULT_CONFIG:log.message.timestamp.type=CreateTime}\n  retention.bytes=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=%s, DEFAULT_CONFIG:log.retention.bytes=-1}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s}\n' "$1" "$1" "${5:-delete}" "$4" "$4" "${3:--1}" "${3:--1}" "$2" "$2"
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
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-break-score-topics.sh" > "$T/out" 2>&1; }
expect_fail() { run; if [ $? -ne 0 ]; then ok "$1"; else bad "$1"; cat "$T/out"; fi; }

GOOD=$(cfgline delete -1 16777216 CreateTime)

mock "other.topic" 1 "$GOOD"; run; rc=$?
{ [ $rc -eq 0 ] && grep -q -- "$TOPIC.*--partitions 1.*cleanup.policy=delete.*retention.ms=-1.*retention.bytes=16777216.*message.timestamp.type=CreateTime" "$T/created" \
  && grep -q "ok: '$TOPIC'" "$T/out"; } \
  && ok "absent → created with the decided shape (retention.bytes 16777216), then verified" || { bad "absent → create"; cat "$T/out"; }

mock "other.topic" 1 "$GOOD"
if PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 BREAK_SCORE_EVENTS_RETENTION_BYTES=33554432 \
     bash "$HERE/ensure-break-score-topics.sh" > "$T/out" 2>&1 && grep -q -- "retention.bytes=33554432" "$T/created"; then
  ok "BREAK_SCORE_EVENTS_RETENTION_BYTES overrides the cap of a topic being created"
else
  bad "retention bytes override"; cat "$T/out"
fi
for badv in 0 -1 abc ""; do
  mock "$TOPIC" 1 "$GOOD"
  if PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 BREAK_SCORE_EVENTS_RETENTION_BYTES="$badv" \
       bash "$HERE/ensure-break-score-topics.sh" > "$T/out" 2>&1; then
    # an empty value falls back to the default, which is valid
    if [ -n "$badv" ]; then bad "BREAK_SCORE_EVENTS_RETENTION_BYTES='$badv' refused"; fi
  else
    ok "BREAK_SCORE_EVENTS_RETENTION_BYTES='$badv' refused"
  fi
done

mock "$TOPIC" 1 "$GOOD"; run; rc=$?
{ [ $rc -eq 0 ] && [ ! -s "$T/created" ]; } && ok "present + right shape → ok, nothing created" || { bad "present ok"; cat "$T/out"; }

mock "$TOPIC" 1 "$(cfgline delete -1 2152919 CreateTime compact)"; run; rc=$?
[ $rc -eq 0 ] && ok "another finite positive cap, and a COMPACT broker default → ok (the cap is owner-adjustable; synonyms are not read)" || { bad "other cap"; cat "$T/out"; }

mock "$TOPIC" 1 "$(cfgline delete -1 -1 CreateTime)";                  expect_fail "retention.bytes unlimited (-1) → fail (an uncapped durable topic)"
mock "$TOPIC" 1 "$(cfgline delete -1 0 CreateTime)";                   expect_fail "retention.bytes 0 → fail"
mock "$TOPIC" 1 "$(cfgline delete 604800000 16777216 CreateTime)";     expect_fail "finite retention.ms → fail (keep by size, not by age)"
mock "$TOPIC" 32 "$GOOD";                                              expect_fail "32 partitions (the dev auto-create default) → fail"
mock "$TOPIC" 1 "$(cfgline compact -1 16777216 CreateTime)";           expect_fail "compacted → fail"
mock "$TOPIC" 1 "$(cfgline compact,delete -1 16777216 CreateTime)";    expect_fail "compact,delete → fail"
mock "$TOPIC" 1 "$(cfgline delete -1 16777216 LogAppendTime)";         expect_fail "LogAppendTime → fail (the event's exchange time must survive)"
mock "$TOPIC" 1 "$GOOD" "" 0;                                          expect_fail "ReplicationFactor 0 → fail"
mock "$TOPIC" 1 "$GOOD"
if PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 BREAK_SCORE_MIN_REPLICATION_FACTOR=3 \
     bash "$HERE/ensure-break-score-topics.sh" > "$T/out" 2>&1; then
  bad "RF>=3 refuses a single-replica topic"; cat "$T/out"
else
  ok "a replicated estate's minimum (RF>=3) refuses a single-replica topic"
fi
mock "other.topic" 1 "$GOOD" create;   expect_fail "create fails → fail (never reported as success)"
mock "$TOPIC" 1 "$GOOD" list;          expect_fail "topic list unreadable → fail"
mock "$TOPIC" 1 "$GOOD" describe;      expect_fail "describe fails → fail"
mock "$TOPIC" 1 "$GOOD" configs;       expect_fail "config describe fails → fail"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
