#!/usr/bin/env bash
# Runs the REAL ensure-oi-anchor-topic.sh against mocked kafka CLIs.
#
# The parser test proves the parser; this proves the SCRIPT — that a wrong shape actually stops the
# deploy, rather than being detected and then not acted on.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
TOPIC=options.databento.oi.anchor-manifest

run() { # exists | partitions | config-line -> exit status of the barrier
  local exists="$1" parts="$2" cfg="$3" tmp; tmp="$(mktemp -d)"
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
if [[ "\$*" == *--list* ]]; then $([ "$exists" = yes ] && echo "echo $TOPIC" || echo ":"); exit 0; fi
if [[ "\$*" == *--describe* ]]; then echo "Topic: $TOPIC TopicId: ID PartitionCount: $parts ReplicationFactor: 1"; exit 0; fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
echo "Dynamic configs for topic $TOPIC are: $cfg"
exit 0
EOF
  chmod +x "$tmp/kafka-topics" "$tmp/kafka-configs"
  set +e
  KAFKA_BOOTSTRAP_SERVERS=mock:9092 PATH="$tmp:$PATH" bash "$HERE/ensure-oi-anchor-topic.sh" >/dev/null 2>&1
  local st=$?
  set -e
  rm -rf "$tmp"
  return $st
}

check() { # description | want-status | args...
  local desc="$1" want="$2"; shift 2
  local got=0; run "$@" || got=$?
  if [ "$got" = "$want" ]; then printf '  ok   %-44s exit %s\n' "$desc" "$got"
  else printf '  FAIL %-44s exit %s, want %s\n' "$desc" "$got" "$want"; fail=1; fi
}

GOOD='cleanup.policy=compact sensitive=false, retention.ms=-1 sensitive=false'
check "correct shape is accepted"        0 yes 1 "$GOOD"
check "absent topic blocks the deploy"   1 no  1 "$GOOD"
check "compact,delete blocks the deploy" 1 yes 1 'cleanup.policy=compact,delete sensitive=false, retention.ms=-1 sensitive=false'
check "finite retention blocks"          1 yes 1 'cleanup.policy=compact sensitive=false, retention.ms=86400000 sensitive=false'
check "unset retention blocks"           1 yes 1 'cleanup.policy=compact sensitive=false'
check "multi-partition blocks"           1 yes 4 "$GOOD"

[ $fail -eq 0 ] && echo "=== ensure-oi-anchor-topic-e2e-test: OK ===" || { echo "=== ensure-oi-anchor-topic-e2e-test: FAILED ==="; exit 1; }
