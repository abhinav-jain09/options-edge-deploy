#!/usr/bin/env bash
# Executable proof that cleanup-topics.sh never destroys a DURABLE topic.
#
# This replaces counting is_durable guards. A count is a proxy: adding a fourth destructive path
# leaves the count at three and passes while the new path deletes data. This runs the REAL script
# against mocked kafka CLIs and asserts on what it actually TRIED to do, so an unguarded path fails
# here however it is written.
#
# Deliberately uses the REAL topics.env declarations rather than synthetic ones -- cleanup-topics.sh
# sources topics.env directly, which overwrites any injected topic list, and asserting on the real
# declarations is what makes this a test of the shipped configuration.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0

DURABLE_ALWAYS="options.databento.oi.anchor-manifest"  # retention=-1 in the base declaration
DURABLE_PROD_ONLY="underlying.vix.price"               # retention=-1 only when ENVIRONMENT=production
SWEPT="options.databento.raw"                          # ordinary topic; must still be cleaned

STATUS_FILE="$(mktemp)"
trap 'rm -f "$STATUS_FILE"' EXIT
run_cleanup() { # mode | environment -> the mocked CLI call log; sets LAST_STATUS
  local mode="$1" env="$2" tmp; tmp="$(mktemp -d)"
  # STATEFUL. The first version reported every topic present forever, so wait_for_topic_absent
  # burned its full retry budget on each deleted topic and the suite took 1m39s -- slow enough that
  # CI would be tempted to drop it, which is how a guard quietly stops running. A deleted topic now
  # disappears, which is also what the script is entitled to assume.
  cat > "$tmp/kafka-topics" <<'EOF'
#!/usr/bin/env bash
prev=""; topic=""
for a in "$@"; do [ "$prev" = "--topic" ] && topic="$a"; prev="$a"; done
if [[ "$*" == *--list* ]]; then exit 0; fi
if [[ "$*" == *--describe* ]]; then
  [ -e "$STATE/deleted-$topic" ] && exit 0          # gone: no description, as Kafka does
  echo "Topic: $topic TopicId: ID-$topic PartitionCount: 1"; exit 0
fi
if [[ "$*" == *--delete* ]]; then
  echo "CALL $*" >> "$LOG"; : > "$STATE/deleted-$topic"; exit 0
fi
echo "CALL $*" >> "$LOG"; exit 0
EOF
  cat > "$tmp/kafka-configs" <<'EOF'
#!/usr/bin/env bash
echo "CALL $*" >> "$LOG"; exit 0
EOF
  chmod +x "$tmp/kafka-topics" "$tmp/kafka-configs"
  export LOG="$tmp/log"; : > "$LOG"
  export STATE="$tmp/state"; mkdir -p "$STATE"
  TOPIC_SET="${TOPIC_SET:-}" PROTECTED_TOPIC_REGEX='^__' KAFKA_BOOTSTRAP_SERVERS="mock:9092" \
  KAFKA_CLEANUP_TOPICS=true KAFKA_CLEANUP_MODE="$mode" ENVIRONMENT="$env" \
  KAFKA_DELETE_UNWANTED_TOPICS=false ALLOW_PROD_KAFKA_CLEANUP="${ALLOW_PROD:-false}" \
  PATH="$tmp:$PATH" \
    bash "$HERE/cleanup-topics.sh" >/dev/null 2>&1
  printf '%s' "$?" > "$STATUS_FILE"
  cat "$LOG" 2>/dev/null || true
  rm -rf "$tmp"
}

# A negative assertion ("durable topic NOT deleted") is satisfied just as well by the script
# crashing on line 1. Assert the run actually succeeded before believing any of them.
assert_status() { # want-status | description
  local got; got="$(cat "$STATUS_FILE" 2>/dev/null || echo '?')"
  if [ "$got" = "$1" ]; then printf '  ok   %s (exit %s)\n' "$2" "$got"
  else printf '  FAIL %s — exit %s, want %s\n' "$2" "$got" "$1"; fail=1; fi
}

assert() { # description | log | pattern | present|absent
  local desc="$1" log="$2" pat="$3" want="$4" got
  if printf '%s' "$log" | grep -qF -e "$pat"; then got=present; else got=absent; fi
  if [ "$got" = "$want" ]; then printf '  ok   %s\n' "$desc"
  else printf '  FAIL %s — %q was %s, want %s\n' "$desc" "$pat" "$got" "$want"; fail=1; fi
}

# Production refuses outright without ALLOW_PROD_KAFKA_CLEANUP -- assert that gate before
# testing behind it, so the durable guard is never mistaken for the only thing protecting prod.
echo "--- production refuses cleanup by default ---"
L="$(run_cleanup delete-recreate production)"
assert "nothing runs on prod without ALLOW_PROD_KAFKA_CLEANUP" "$L" "--delete" absent
assert_status 1 "production cleanup is refused, and refuses LOUDLY"

echo "--- delete-recreate, production (gate opened) ---"
L="$(ALLOW_PROD=true run_cleanup delete-recreate production)"
assert "durable topic NOT deleted"      "$L" "--delete --topic $DURABLE_ALWAYS"    absent
assert "prod-durable vix NOT deleted"   "$L" "--delete --topic $DURABLE_PROD_ONLY" absent
assert "ordinary topic IS deleted"      "$L" "--delete --topic $SWEPT"             present
assert_status 0 "cleanup itself succeeded"

echo "--- retention shrink, production (gate opened) ---"
L="$(ALLOW_PROD=true run_cleanup retention production)"
assert "durable topic NOT shrunk"       "$L" "--entity-name $DURABLE_ALWAYS --alter"    absent
assert "prod-durable vix NOT shrunk"    "$L" "--entity-name $DURABLE_PROD_ONLY --alter" absent
assert "ordinary topic IS shrunk"       "$L" "--entity-name $SWEPT --alter"             present
assert_status 0 "cleanup itself succeeded"

echo "--- dev: vix is durable on PRODUCTION only and must still be swept here ---"
L="$(run_cleanup retention dev)"
assert "durable topic NOT shrunk"       "$L" "--entity-name $DURABLE_ALWAYS --alter"    absent
assert "vix IS shrunk on dev"           "$L" "--entity-name $DURABLE_PROD_ONLY --alter" present
assert_status 0 "cleanup itself succeeded"

echo "--- TOPIC_SET is refused, not silently applied to the wrong cluster ---"
L="$(TOPIC_SET=es4 ALLOW_PROD=true run_cleanup retention dev)"
assert_status 1 "refuses TOPIC_SET=es4"
assert "nothing was touched"            "$L" "--alter" absent

[ $fail -eq 0 ] && echo "=== cleanup-topics-durable-test: OK ===" || { echo "=== cleanup-topics-durable-test: FAILED ==="; exit 1; }
