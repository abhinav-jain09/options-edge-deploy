#!/usr/bin/env bash
# Drives the REAL ensure-gamma-ladder-path-topics.sh against mocked kafka-topics / kafka-configs that print the
# broker's real --describe format (sensitive=…, synonyms={… DEFAULT_CONFIG:log.cleanup.policy=…}) and that can
# FAIL: absent → create → verified; right shape (either broker default) → ok; wrong shape or any CLI failure →
# fail closed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
S=options.spx.gamma-ladder-path.state
E=options.spx.gamma-ladder-path.events

# cfgline <policy> <retention> <broker default policy>: the real kafka-configs --describe shape
cfgline() {
  printf 'Dynamic configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=%s}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s, STATIC_BROKER_CONFIG:log.retention.ms=86400000}\n' "$1" "$1" "$3" "$2" "$2"
}

# mock <list> <state parts> <state cfg> <events parts> <events cfg> [fail: list|create|describe|configs]
mock() {
  mkdir -p "$T/bin"; : > "$T/created"
  printf '%s\n' "$3" > "$T/cfg_state"; printf '%s\n' "$5" > "$T/cfg_events"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${6:-}" = list ] && { echo "Timed out waiting for a node assignment" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${6:-}" = create ] && { echo "Error while executing topic command : Not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created"; echo "Created topic." ;;
  *" --describe "*) [ "${6:-}" = describe ] && { echo "Error: broker unreachable" >&2; exit 1; }
     case " \$* " in *"$S"*) p=$2 ;; *) p=$4 ;; esac
     echo "Topic: x	TopicId: AbCdEfGhIjKlMnOpQrStUv	PartitionCount: \$p	ReplicationFactor: 1	Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${6:-}" = configs ] && { echo "Error: TimeoutException" >&2; exit 1; }
case " \$* " in *"$S"*) cat "$T/cfg_state" ;; *) cat "$T/cfg_events" ;; esac
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-gamma-ladder-path-topics.sh" > "$T/out" 2>&1; }
expect_fail() { run; if [ $? -ne 0 ]; then ok "$1"; else bad "$1"; cat "$T/out"; fi; }

GS=$(cfgline compact -1 delete)
GE=$(cfgline delete -1 delete)
BOTH="$S $E"

mock "other.topic" 1 "$GS" 1 "$GE"; run; rc=$?
{ [ $rc -eq 0 ] && grep -q "$S.*cleanup.policy=compact.*retention.ms=-1" "$T/created" \
  && grep -q "$E.*cleanup.policy=delete.*retention.ms=-1" "$T/created" && grep -q "ok: '$S'" "$T/out"; } \
  && ok "absent → both created with the declared shape, then verified" || { bad "absent → create"; cat "$T/out"; }

mock "$BOTH" 1 "$GS" 1 "$GE"; run; rc=$?
{ [ $rc -eq 0 ] && [ ! -s "$T/created" ]; } && ok "present + right shape (broker default delete) → ok, nothing created" || { bad "present ok"; cat "$T/out"; }

mock "$BOTH" 1 "$(cfgline compact -1 compact)" 1 "$(cfgline delete -1 compact)"; run; rc=$?
[ $rc -eq 0 ] && ok "right shape with a COMPACT broker default → ok (synonyms are not read)" || { bad "compact default"; cat "$T/out"; }

mock "$BOTH" 4 "$GS" 1 "$GE";                                     expect_fail "state with 4 partitions → fail"
mock "$BOTH" 1 "$(cfgline compact 604800000 delete)" 1 "$GE";     expect_fail "state time retention → fail"
mock "$BOTH" 1 "$(cfgline delete -1 delete)" 1 "$GE";             expect_fail "state not compacted → fail"
mock "$BOTH" 1 "$(cfgline compact,delete -1 delete)" 1 "$GE";     expect_fail "state compact,delete → fail"
mock "$BOTH" 1 "$GS" 1 "$(cfgline compact -1 delete)";            expect_fail "events compacted → fail"
mock "$BOTH" 1 "$GS" 1 "Dynamic configs for topic x are:";         expect_fail "events with no dynamic config (inherits defaults) → fail"
mock "other.topic" 1 "$GS" 1 "$GE" create;                        expect_fail "create fails → fail (never reported as success)"
mock "$BOTH" 1 "$GS" 1 "$GE" list;                                expect_fail "topic list unreadable → fail"
mock "$BOTH" 1 "$GS" 1 "$GE" describe;                            expect_fail "describe fails → fail"
mock "$BOTH" 1 "$GS" 1 "$GE" configs;                             expect_fail "config describe fails → fail"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
