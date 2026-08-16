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
# The prod-only topics, names only, and their declared retention overrides. The mocked broker
# answers FROM the declarations, so the positive case means "a broker that matches topics.env"
# rather than "a broker that matches a constant somebody typed here twice".
PROD_ONLY_NAMES="$(printf '%s' "${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}" | tr ' ' '\n' | cut -d: -f1 | tr '\n' ' ')"
PURE_COMPACT_NAMES="${OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS:-}"   # U16: these report compact at the fake broker
RETENTIONS="${OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES:-}"

OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

run() { # policy-for-basis-state -> exit status of verify-topics; output in $OUT
  local policy="$1" tmp; tmp="$(mktemp -d)"
  # One "topic=value" knob per clause of the prod-only contract, so a negative case can break
  # exactly one of them at the mocked broker and leave the rest satisfied.
  local part_ov="${PART_OVERRIDE:-}" pol_ov="${POL_OVERRIDE:-}" ret_ov="${RET_OVERRIDE:-}"
  # Report each topic's DECLARED partition count. A flat "1" fails verify-topics' own
  # at-least-declared check on every 4- and 32-partition topic, which would mask the contract
  # this test is actually about.
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  parts=\$(printf '%s' "$DECLARED" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
  case "$part_ov" in "\$name="*) parts="${part_ov#*=}";; esac
  echo "Topic: \$name TopicId: ID PartitionCount: \${parts:-1} ReplicationFactor: 1"
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--entity-name" ] && name="\$a"; prev="\$a"; done
# Compact everywhere except the prod-only topics, which topics.env declares plain delete —
# EXCEPT the pure-compact prod-only topics (U16), which are compact by contract.
pol=compact
case " $PROD_ONLY_NAMES " in *" \$name "*) pol=delete;; esac
case " $PURE_COMPACT_NAMES " in *" \$name "*) pol=compact;; esac
[ "\$name" = "spx.basis.state" ] && pol="$policy"
case "$pol_ov" in "\$name="*) pol="${pol_ov#*=}";; esac
ret=\$(printf '%s' "$RETENTIONS" | tr ' ' '\\n' | awk -F= -v n="\$name" '\$1 == n {print \$2; exit}')
case "$ret_ov" in "\$name="*) ret="${ret_ov#*=}";; esac
echo "Dynamic configs for topic \$name are: cleanup.policy=\$pol sensitive=false, retention.ms=\${ret:--1} sensitive=false"
exit 0
EOF
  chmod +x "$tmp/kafka-topics" "$tmp/kafka-configs"
  set +e
  # ENVIRONMENT is pinned for the same reason TOPIC_SET is: it selects WHICH contract
  # verify-topics enforces. Inherited from the caller, the production deploy job's own
  # ENVIRONMENT=production turned every run of this test into a prod-only run against a mock
  # that was never built for one -- green on dev, red on production, on identical code.
  KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 KAFKA_TOPIC_MIN_ISR=1 \
    TOPIC_SET="${SET:-}" ENVIRONMENT="${WANT_ENV:-}" \
    PATH="$tmp:$PATH" bash "$HERE/verify-topics.sh" > "$OUT" 2>&1
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

# Production enforces a SECOND contract on top of the pure-compact one, and nothing exercised it.
# The positive case asserts the prod block REACHED ITS OK, not merely that it ran; each negative
# breaks one clause of that contract at the broker and names the clause it expects to trip, so a
# clause deleted from verify-topics.sh cannot hide behind the other three.
WANT_ENV=production check "production passes both contracts" 0 compact \
  "prod-only topic contract OK"
WANT_ENV=production POL_OVERRIDE="underlying.vix.price.shadow=compact" \
  check "prod-only delete policy is enforced" 1 compact \
  "underlying.vix.price.shadow cleanup.policy='compact' but must be exactly 'delete'"
WANT_ENV=production POL_OVERRIDE="underlying.vix.price=compact,delete" \
  check "prod-only pure-compact is enforced" 1 compact \
  "underlying.vix.price cleanup.policy='compact,delete' but must be exactly 'compact'"
WANT_ENV=production RET_OVERRIDE="underlying.vix.price.shadow=-1" \
  check "prod-only retention override is enforced" 1 compact \
  "underlying.vix.price.shadow retention.ms='-1' but the prod-only override declares '86400000'"
WANT_ENV=production PART_OVERRIDE="underlying.vix.price=2" \
  check "prod-only EXACT partition count is enforced" 1 compact \
  "underlying.vix.price has partitions=2 but requires EXACTLY 1"

[ $fail -eq 0 ] && echo "=== verify-topics-pure-compact-test: OK ===" || { echo "=== verify-topics-pure-compact-test: FAILED ==="; exit 1; }
