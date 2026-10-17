#!/usr/bin/env bash
# Drives the REAL ensure-level-absorption-topics.sh against a STATEFUL mock of kafka-topics / kafka-configs that prints the broker's
# real `--describe --all` format and remembers what --create made, so a topic just created is verified from what was asked for.
# The barrier's points: one partition each; cleanup.policy exactly delete; the three inputs-to-replay (voice, closures, manual-line)
# keep >= 7 days or forever; the events topic is retention.ms exactly -1 with a finite positive retention.bytes that equals the
# retention artifact's integer when it is given; no finite message.timestamp.before.max.ms on an output topic; and an events topic
# that is absent WITHOUT the artifact's integer is a warning (the service is held at replicas 0), a failure when it is required.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
EV=futures.level-absorption.events
VO=futures.level-absorption.voice
CL=futures.level-absorption.closures
MA=futures.level-absorption.manual-line
LONG_MAX=9223372036854775807

# cfg_file <topic> <policy> <retention.ms> <retention.bytes|-1> [before.max.ms] [after.max.ms]
cfg_file() {
  printf 'All configs for topic %s are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=delete}\n  message.timestamp.after.max.ms=%s sensitive=false synonyms={DEFAULT_CONFIG:log.message.timestamp.after.max.ms=3600000}\n  message.timestamp.before.max.ms=%s sensitive=false synonyms={DEFAULT_CONFIG:log.message.timestamp.before.max.ms=%s}\n  retention.bytes=%s sensitive=false synonyms={DEFAULT_CONFIG:log.retention.bytes=-1}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s}\n' \
    "$1" "$2" "$2" "${6:-3600000}" "${5:-$LONG_MAX}" "${5:-$LONG_MAX}" "$4" "$3" "$3" > "$T/topics/$1.cfg"
}
# good_topic <topic> [policy ms bytes before after]
topic() { # <topic> <partitions> <policy> <ms> <bytes> [before] [after]
  echo "$2" > "$T/topics/$1.parts"; cfg_file "$1" "$3" "$4" "$5" "${6:-}" "${7:-}"
}
fresh() { # sets up the mock CLIs and an EMPTY cluster; <fail mode: list|create|describe|configs>
  rm -rf "$T/topics" "$T/bin"; mkdir -p "$T/topics" "$T/bin"; : > "$T/created"; echo "${1:-}" > "$T/fail"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
T="$T"; fail=\$(cat "\$T/fail")
case " \$* " in
  *" --list "*) [ "\$fail" = list ] && { echo "Timed out waiting for a node assignment" >&2; exit 1; }
     for f in "\$T"/topics/*.parts; do [ -e "\$f" ] && basename "\$f" .parts; done; exit 0 ;;
  *" --create "*) [ "\$fail" = create ] && { echo "Error while executing topic command : Not authorized" >&2; exit 1; }
     echo "\$*" >> "\$T/created"
     name=\$(sed -E 's/.*--topic ([^ ]+).*/\1/' <<<"\$*"); parts=\$(sed -E 's/.*--partitions ([0-9]+).*/\1/' <<<"\$*")
     ms=\$(sed -E 's/.*retention\.ms=([-0-9]+).*/\1/' <<<"\$*"); by=\$(grep -oE 'retention\.bytes=[0-9]+' <<<"\$*" | sed 's/.*=//'); policy=\$(sed -E 's/.*cleanup\.policy=([a-z,]+).*/\1/' <<<"\$*")
     echo "\$parts" > "\$T/topics/\$name.parts"
     printf 'All configs for topic %s are:\n  cleanup.policy=%s sensitive=false\n  message.timestamp.after.max.ms=3600000 sensitive=false\n  message.timestamp.before.max.ms=$LONG_MAX sensitive=false\n  retention.bytes=%s sensitive=false\n  retention.ms=%s sensitive=false\n' "\$name" "\$policy" "\${by:--1}" "\$ms" > "\$T/topics/\$name.cfg"
     echo "Created topic." ;;
  *" --describe "*) [ "\$fail" = describe ] && { echo "Error: broker unreachable" >&2; exit 1; }
     name=\$(sed -E 's/.*--topic ([^ ]+).*/\1/' <<<"\$*")
     echo "Topic: \$name	TopicId: AbCdEfGhIjKlMnOpQrStUv	PartitionCount: \$(cat "\$T/topics/\$name.parts")	ReplicationFactor: \$(cat "\$T/rf" 2>/dev/null || echo 1)	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
T="$T"
[ "\$(cat "\$T/fail")" = configs ] && { echo "Error: TimeoutException" >&2; exit 1; }
name=\$(sed -E 's/.*--entity-name ([^ ]+).*/\1/' <<<"\$*")
cat "\$T/topics/\$name.cfg"
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"; rm -f "$T/rf"
}
all_good() { # the full cluster, every topic as the barrier wants it
  fresh "${1:-}"
  topic $EV 1 delete -1 2152919; topic $VO 1 delete 604800000 -1; topic $CL 1 delete 604800000 -1; topic $MA 1 delete 604800000 -1
}
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 "$@" bash "$HERE/ensure-level-absorption-topics.sh" > "$T/out" 2>&1; }
runenv() { env PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 "$@" bash "$HERE/ensure-level-absorption-topics.sh" > "$T/out" 2>&1; }
expect_fail() { local m="$1"; shift; runenv "$@"; if [ $? -ne 0 ]; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }
expect_ok() { local m="$1"; shift; runenv "$@"; if [ $? -eq 0 ]; then ok "$m"; else bad "$m"; cat "$T/out"; fi; }

# --- an empty cluster -------------------------------------------------------------------------------------------------------
fresh
runenv; rc=$?
{ [ $rc -eq 0 ] && grep -q "WARN: '$EV' does not exist" "$T/out" \
  && [ "$(grep -c -- '--partitions 1 .*cleanup.policy=delete.*retention.ms=604800000' "$T/created")" = 3 ] \
  && ! grep -q -- "$EV" "$T/created"; } \
  && ok "empty cluster, no artifact integer → voice, closures, manual-line created at 7 days; events skipped with a WARNING, rc 0" || { bad "empty cluster"; cat "$T/out"; }

fresh; expect_fail "empty cluster, events REQUIRED, no artifact integer → fail" LEVEL_ABSORPTION_REQUIRE_EVENTS_TOPIC=1

fresh; runenv LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=2152919; rc=$?
{ [ $rc -eq 0 ] && grep -q -- "--topic $EV --partitions 1.*cleanup.policy=delete.*retention.ms=-1.*retention.bytes=2152919" "$T/created" \
  && grep -q "ok: '$EV'.*retention.ms=-1 retention.bytes=2152919" "$T/out"; } \
  && ok "empty cluster + artifact integer → events created with -1 and that cap, then verified" || { bad "events create"; cat "$T/out"; }

for badv in abc -5 0 01 1.5; do
  fresh; expect_fail "LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES='$badv' → refused" LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES="$badv"
done

# --- a complete, correct cluster --------------------------------------------------------------------------------------------
all_good; runenv; rc=$?
{ [ $rc -eq 0 ] && [ ! -s "$T/created" ]; } && ok "all four present + right shape → ok, nothing created" || { bad "all good"; cat "$T/out"; }
all_good; expect_ok "events cap equal to the given integer → ok" LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=2152919
all_good; expect_fail "events cap different from the given integer → fail (the service refuses any other value)" LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=16777216
all_good; topic $VO 1 delete -1 -1; expect_ok "voice kept forever (-1) → ok"
all_good; topic $VO 1 delete 604800000 -1; expect_ok "voice at exactly 7 days → ok"

# --- the events topic ------------------------------------------------------------------------------------------------------
all_good; topic $EV 1 delete -1 -1;               expect_fail "events retention.bytes unlimited → fail"
all_good; topic $EV 1 delete -1 0;                expect_fail "events retention.bytes 0 → fail"
all_good; topic $EV 1 delete 2592000000 2152919;  expect_fail "events finite retention.ms → fail"
all_good; topic $EV 32 delete -1 2152919;         expect_fail "events at 32 partitions (the dev auto-create default) → fail"
all_good; topic $EV 1 compact -1 2152919;         expect_fail "events compacted → fail"
all_good; topic $EV 1 compact,delete -1 2152919;  expect_fail "events compact,delete → fail"
all_good; topic $EV 1 delete -1 2152919 86400000; expect_fail "events with a finite message.timestamp.before.max.ms → fail (a replay would be rejected)"
all_good; topic $EV 1 delete -1 2152919 $LONG_MAX 60000; expect_fail "events with message.timestamp.after.max.ms below the one-hour default → fail"
all_good; topic $EV 1 delete -1 2152919 $LONG_MAX $LONG_MAX; expect_ok "events with message.timestamp.after.max.ms unlimited → ok"

# --- voice / closures / manual-line -----------------------------------------------------------------------------------------
for t in $VO $CL $MA; do
  all_good; topic $t 1 delete 604799999 -1;  expect_fail "$t retention one millisecond under 7 days → fail"
  all_good; topic $t 3 delete 604800000 -1;  expect_fail "$t at 3 partitions → fail"
  all_good; topic $t 1 compact 604800000 -1; expect_fail "$t compacted → fail"
done
for t in $VO $CL; do
  all_good; topic $t 1 delete 604800000 -1 86400000; expect_fail "$t with a finite message.timestamp.before.max.ms → fail"
done
all_good; topic $MA 1 delete 604800000 -1 86400000; expect_ok "manual-line (an input) with a finite before.max.ms → ok (not the service's concern)"

# --- infrastructure failures are never success ------------------------------------------------------------------------------
all_good; echo 2 > "$T/rf"; expect_fail "a replicated estate's minimum (RF>=3) refuses single-replica topics" LEVEL_ABSORPTION_MIN_REPLICATION_FACTOR=3
fresh create;     expect_fail "create fails → fail (never reported as success)"
all_good list;    expect_fail "topic list unreadable → fail"
all_good describe; expect_fail "describe fails → fail"
all_good configs; expect_fail "config describe fails → fail"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
