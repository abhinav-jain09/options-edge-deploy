#!/usr/bin/env bash
# The pure-compact contract, tested against the real verify-topics.sh with mocked kafka CLIs.
#
# This check did not exist, and its absence is the whole story: topics.env declared
# spx.basis.state pure compact while both dev and production ran it at cleanup.policy=delete, for
# long enough that production accumulated 25,788 records on a topic whose consumer reads it from
# the beginning on every restart. apply-topics reconciles the policy; nothing verified that it had.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0

# The declared "topic:partitions" pairs, read from the same file verify-topics reads.
# shellcheck source=/dev/null
source "$HERE/topics.env"
DECLARED="$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"

OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

run() { # policy-for-basis-state -> exit status of verify-topics; output in $OUT
  local policy="$1" tmp; tmp="$(mktemp -d)"
  # Report each topic's DECLARED partition count. A flat "1" fails verify-topics' own
  # at-least-declared check on every 4- and 32-partition topic, which would mask the contract
  # this test is actually about.
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  parts=\$(printf '%s' "$DECLARED" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
  echo "Topic: \$name TopicId: ID PartitionCount: \${parts:-1} ReplicationFactor: 1"
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--entity-name" ] && name="\$a"; prev="\$a"; done
if [ "\$name" = "spx.basis.state" ]; then
  echo "Dynamic configs for topic \$name are: cleanup.policy=$policy sensitive=false, retention.ms=-1 sensitive=false"
else
  echo "Dynamic configs for topic \$name are: cleanup.policy=compact sensitive=false, retention.ms=-1 sensitive=false"
fi
exit 0
EOF
  chmod +x "$tmp/kafka-topics" "$tmp/kafka-configs"
  set +e
  KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 KAFKA_TOPIC_MIN_ISR=1 \
    TOPIC_SET="${SET:-}" PATH="$tmp:$PATH" bash "$HERE/verify-topics.sh" > "$OUT" 2>&1
  local st=$?
  set -e
  rm -rf "$tmp"
  return $st
}

check() { # description | want-status | policy | expected-substring
  local got=0; run "$3" || got=$?
  local why="$4"
  if [ "$got" != "$2" ]; then
    printf '  FAIL %-46s exit %s, want %s\n' "$1" "$got" "$2"; fail=1; return
  fi
  # Status alone is not enough: an unrelated verifier failure exits 1 too and would satisfy every
  # negative case while proving nothing about the pure-compact contract.
  if ! grep -qF -e "$why" "$OUT"; then
    printf '  FAIL %-46s exit %s but output never mentioned %q\n' "$1" "$got" "$why"; fail=1; return
  fi
  printf '  ok   %-46s exit %s\n' "$1" "$got"
}

check "pure compact passes"                    0 compact        "pure-compact contract OK"
check "delete FAILS the contract"              1 delete         "cleanup.policy='delete'"
check "compact,delete FAILS (the delete half)" 1 compact,delete  "cleanup.policy='compact,delete'"
check "delete,compact FAILS"                   1 delete,compact  "cleanup.policy='delete,compact'"

# es4 declares its own topics and NO pure-compact ones, so the SPX contract must not be applied
# to it -- the drift on spx.basis.state must be invisible here because that topic is not es4's.
SET=es4  check "es4 checks its OWN (empty) list"     0 delete "TOPIC_SET='es4'"
SET=bogus check "unknown TOPIC_SET is refused"       1 compact "unknown TOPIC_SET"

[ $fail -eq 0 ] && echo "=== verify-topics-pure-compact-test: OK ===" || { echo "=== verify-topics-pure-compact-test: FAILED ==="; exit 1; }
