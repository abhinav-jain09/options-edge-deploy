#!/usr/bin/env bash
# Exercise the real zero-DTE topic barrier against Kafka CLI mocks.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
pass=0
fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }
C=context-tape.compression.current
H=context-tape.compression.history
P=context-tape.compression.checkpoint

cfgline() {
  printf 'Dynamic configs for topic x are:\n  cleanup.policy=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=%s, DEFAULT_CONFIG:log.cleanup.policy=delete}\n  retention.ms=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=%s, STATIC_BROKER_CONFIG:log.retention.ms=86400000}\n  retention.bytes=%s sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=%s, DEFAULT_CONFIG:log.retention.bytes=-1}\n' \
    "$1" "$1" "$2" "$2" "$3" "$3"
}

# mock <list> <current parts> <current cfg> <history parts> <history cfg>
#      <checkpoint parts> <checkpoint cfg> [fail: list|create|describe|configs]
mock() {
  mkdir -p "$T/bin"
  : > "$T/created"
  printf '%s\n' "$3" > "$T/cfg_current"
  printf '%s\n' "$5" > "$T/cfg_history"
  printf '%s\n' "$7" > "$T/cfg_checkpoint"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) [ "${8:-}" = list ] && { echo "broker unavailable" >&2; exit 1; }; printf '%s\n' $1 ;;
  *" --create "*) [ "${8:-}" = create ] && { echo "not authorized" >&2; exit 1; }; echo "\$*" >> "$T/created" ;;
  *" --describe "*) [ "${8:-}" = describe ] && { echo "broker unavailable" >&2; exit 1; }
    case " \$* " in *"$C"*) p=$2 ;; *"$H"*) p=$4 ;; *) p=$6 ;; esac
    echo "Topic: x TopicId: AbCd PartitionCount: \$p ReplicationFactor: 1 Configs: " ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
[ "${8:-}" = configs ] && { echo "TimeoutException" >&2; exit 1; }
case " \$* " in *"$C"*) cat "$T/cfg_current" ;; *"$H"*) cat "$T/cfg_history" ;; *) cat "$T/cfg_checkpoint" ;; esac
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}

run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-zero-dte-compression-topics.sh" > "$T/out" 2>&1; }
expect_fail() { run; if [ $? -ne 0 ]; then ok "$1"; else bad "$1"; cat "$T/out"; fi; }

CC=$(cfgline compact -1 -1)
CH=$(cfgline delete -1 -1)
ALL="$C $H $P"

mock other.topic 1 "$CC" 1 "$CH" 1 "$CC"
run; rc=$?
{ [ "$rc" -eq 0 ] \
  && grep -q "$C.*cleanup.policy=compact.*retention.ms=-1.*retention.bytes=-1" "$T/created" \
  && grep -q "$H.*cleanup.policy=delete.*retention.ms=-1.*retention.bytes=-1" "$T/created" \
  && grep -q "$P.*cleanup.policy=compact.*retention.ms=-1.*retention.bytes=-1" "$T/created"; } \
  && ok "absent topics are created with their exact durable shapes" \
  || { bad "absent topics create"; cat "$T/out"; }

mock "$ALL" 1 "$CC" 1 "$CH" 1 "$CC"
run; rc=$?
{ [ "$rc" -eq 0 ] && [ ! -s "$T/created" ]; } \
  && ok "present topics with exact shapes are accepted without mutation" \
  || { bad "present exact topics"; cat "$T/out"; }

mock "$ALL" 2 "$CC" 1 "$CH" 1 "$CC"; expect_fail "wrong partition count fails closed"
mock "$ALL" 1 "$(cfgline delete -1 -1)" 1 "$CH" 1 "$CC"; expect_fail "wrong current policy fails closed"
mock "$ALL" 1 "$CC" 1 "$(cfgline delete 86400000 -1)" 1 "$CC"; expect_fail "time-bounded history fails closed"
mock "$ALL" 1 "$CC" 1 "$CH" 1 "$(cfgline compact -1 1048576)"; expect_fail "byte-bounded checkpoint fails closed"
mock other.topic 1 "$CC" 1 "$CH" 1 "$CC" create; expect_fail "create failure is surfaced"
mock "$ALL" 1 "$CC" 1 "$CH" 1 "$CC" list; expect_fail "list failure is surfaced"
mock "$ALL" 1 "$CC" 1 "$CH" 1 "$CC" describe; expect_fail "describe failure is surfaced"
mock "$ALL" 1 "$CC" 1 "$CH" 1 "$CC" configs; expect_fail "config read failure is surfaced"

echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
