#!/usr/bin/env bash
# Drives ensure-gamma-ladder-path-topics.sh against mocked kafka-topics / kafka-configs: absent → create with the
# declared shape; present + right shape → ok; wrong partitions / retention / policy → fail closed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   $*"; pass=$((pass + 1)); }
bad() { echo "FAIL $*"; fail=$((fail + 1)); }

mock() { # <list> <state parts> <state cfg> <events parts> <events cfg>
  mkdir -p "$T/bin"; : > "$T/created"
  cat > "$T/bin/kafka-topics" <<M
#!/usr/bin/env bash
case " \$* " in
  *" --list "*) printf '%s\n' $1 ;;
  *" --create "*) echo "\$*" >> "$T/created" ;;
  *"gamma-ladder-path.state"*) echo "Topic: x TopicId: y PartitionCount: $2 ReplicationFactor: 1" ;;
  *"gamma-ladder-path.events"*) echo "Topic: x TopicId: y PartitionCount: $4 ReplicationFactor: 1" ;;
esac
M
  cat > "$T/bin/kafka-configs" <<M
#!/usr/bin/env bash
case " \$* " in
  *"gamma-ladder-path.state"*) echo "$3" ;;
  *) echo "$5" ;;
esac
M
  chmod +x "$T/bin/kafka-topics" "$T/bin/kafka-configs"
}
run() { PATH="$T/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$HERE/ensure-gamma-ladder-path-topics.sh" > "$T/out" 2>&1; }

GOOD_S="Dynamic configs for topic x are: cleanup.policy=compact retention.ms=-1"
GOOD_E="Dynamic configs for topic x are: cleanup.policy=delete retention.ms=-1"

mock "other.topic" 1 "$GOOD_S" 1 "$GOOD_E"; run; rc=$?
[ $rc -eq 0 ] && grep -q "gamma-ladder-path.state.*cleanup.policy=compact.*retention.ms=-1" "$T/created" \
  && grep -q "gamma-ladder-path.events.*cleanup.policy=delete.*retention.ms=-1" "$T/created" \
  && ok "absent → both created with the declared shape" || { bad "absent → create"; cat "$T/out" "$T/created"; }

LIST="options.spx.gamma-ladder-path.state options.spx.gamma-ladder-path.events"
mock "$LIST" 1 "$GOOD_S" 1 "$GOOD_E"; run; rc=$?
[ $rc -eq 0 ] && [ ! -s "$T/created" ] && ok "present + right shape → ok, nothing created" || { bad "present ok"; cat "$T/out"; }

mock "$LIST" 4 "$GOOD_S" 1 "$GOOD_E"; run; [ $? -ne 0 ] && ok "state with 4 partitions → fail closed" || bad "partitions"
mock "$LIST" 1 "cleanup.policy=compact retention.ms=604800000" 1 "$GOOD_E"; run; [ $? -ne 0 ] && ok "state time retention → fail closed" || bad "retention"
mock "$LIST" 1 "cleanup.policy=delete retention.ms=-1" 1 "$GOOD_E"; run; [ $? -ne 0 ] && ok "state not compacted → fail closed" || bad "state policy"
mock "$LIST" 1 "$GOOD_S" 1 "cleanup.policy=compact,delete retention.ms=-1"; run; [ $? -ne 0 ] && ok "events compacted → fail closed" || bad "events policy"

echo "passed=$pass failed=$fail"; [ $fail -eq 0 ]
