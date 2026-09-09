#!/usr/bin/env bash
# The A5 calibration ledger is the one topic in this file whose loss cannot be repaired: it is the
# corpus the Candle Direction validation is built from, and NEVER_RECREATE exists for it. Declaring it
# in OPTIONS_EDGE_TOPICS (so its retention override stops being inert and its single partition is
# bound) also puts it inside apply-topics' repair machinery for the first time — which contains a
# delete+recreate branch. This drives the REAL apply-topics.sh against mocked kafka CLIs and asserts
# that branch can never reach this topic, on any environment.
#
# "I read the code and the exact-partition branch cannot fire at 1 == 1" is a belief. This is a test.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LEDGER="context-tape.direction.ledger"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }

# shellcheck source=/dev/null
source "$HERE/topics.env"
DECLARED="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"

# Every kafka CLI call is recorded, so the assertions are about what apply-topics DID, not about what
# its output said. A mocked broker that answers from topics.env means the positive case is "a broker
# matching the declaration", not "a constant typed twice".
run() { # run <ledger-partition-count> <recreate-flag> <environment> ; log in $LOG
  local parts="$1" recreate="$2" env_name="$3" tmp
  tmp="$(mktemp -d)"; LOG="$tmp/calls"; : > "$LOG"
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$LOG"
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  if [ "\$name" = "$LEDGER" ]; then
    echo "Topic: \$name TopicId: ID PartitionCount: $parts ReplicationFactor: 1"
  else
    p=\$(printf '%s' "$DECLARED" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
    echo "Topic: \$name TopicId: ID PartitionCount: \${p:-1} ReplicationFactor: 1"
  fi
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
echo "kafka-configs \$*" >> "$LOG"
exit 0
EOF
  cat > "$tmp/kafka-broker-api-versions" <<'EOF'
#!/usr/bin/env bash
echo "localhost:9092 (id: 1 rack: null) -> ("
exit 0
EOF
  chmod +x "$tmp"/kafka-*
  PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT="$env_name" \
    KAFKA_RECREATE_MISMATCHED_TOPICS="$recreate" KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 \
    bash "$HERE/apply-topics.sh" > "$tmp/out" 2>&1
  RC=$?; OUT="$tmp/out"
}

echo "1. the ledger is declared at all (otherwise this whole file asserts nothing)"
printf '%s\n' $OPTIONS_EDGE_TOPICS | grep -qx "$LEDGER:1" \
  && ok "$LEDGER:1 is in OPTIONS_EDGE_TOPICS" \
  || bad "$LEDGER is not declared — the rest of this test would pass vacuously"
printf '%s\n' ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-} | grep -qx "$LEDGER" \
  && ok "it is bound to EXACTLY its declared partition count" \
  || bad "$LEDGER is not in OPTIONS_EDGE_EXACT_PARTITION_TOPICS"
printf '%s\n' ${OPTIONS_EDGE_NEVER_RECREATE_TOPICS:-} | grep -qx "$LEDGER" \
  && ok "and it is NEVER-RECREATE, which is what makes a mismatch fatal instead of destructive" \
  || bad "$LEDGER is not in OPTIONS_EDGE_NEVER_RECREATE_TOPICS"

echo "2. at the shape production actually has, nothing destructive happens"
# Read from the prod broker 2026-09-09: PartitionCount 1, ReplicationFactor 1, retention.ms=-1,
# cleanup.policy=delete.
for env_name in production dev; do
  run 1 false "$env_name"
  if grep -q -- "--delete --topic $LEDGER" "$LOG"; then
    bad "$env_name: apply-topics DELETED the corpus topic"
  else
    ok "$env_name: no delete was issued for the ledger"
  fi
  grep -q -- "--alter --add-config retention.ms=-1,cleanup.policy=delete" "$LOG" \
    && ok "$env_name: its retention override is now actually applied (it was inert while undeclared)" \
    || bad "$env_name: no retention.ms=-1 alter was issued — the declaration is still inert"
done

echo "3. a WRONG partition count is a hard error, not a repair — even when repairs are enabled"
run 4 true production
if grep -q -- "--delete --topic $LEDGER" "$LOG"; then
  bad "KAFKA_RECREATE_MISMATCHED_TOPICS=true deleted the corpus topic"
else
  ok "the recreate flag did not reach it"
fi
[ "$RC" -ne 0 ] \
  && ok "apply-topics refused (exit $RC) rather than proceeding" \
  || bad "apply-topics exited 0 with the ledger at the wrong partition count"
grep -q "OPTIONS_EDGE_NEVER_RECREATE_TOPICS" "$OUT" \
  && ok "and it said WHY: the never-recreate declaration is what stopped it" \
  || bad "the refusal did not name the never-recreate declaration: $(head -3 "$OUT")"

echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-ledger-safety: OK ==="; exit 0; fi
echo "=== apply-topics-ledger-safety: $fails problem(s) ===" >&2; exit 1
