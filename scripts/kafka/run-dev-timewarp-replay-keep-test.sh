#!/usr/bin/env bash
# The dev time-warp replay deletes the broker's topics and then WAITS for them to be gone. Both halves
# have to agree about the RESET-PRESERVED keep-list, and on 2026-10-13 they did not:
#   * the delete loop deleted every non-system topic and consulted no keep-list at all (deploy Codex
#     round 1 of the trading.order.{intent,status} classification);
#   * the fix for that made the WAIT hang, because "all topics gone" is false by construction once a
#     topic is deliberately kept — it would have timed out and aborted every replay (round 2).
# A structural grep cannot tell those apart, so this drives the real functions with a stubbed broker.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLAY="$HERE/run-dev-timewarp-replay.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -r "$REPLAY" ] || { echo "FAIL: $REPLAY missing" >&2; exit 1; }

mkdir -p "$WORK/bin"
printf '#!/usr/bin/env bash\ncat "$STUB_LIST"\n' > "$WORK/bin/kafka-topics"
chmod +x "$WORK/bin/kafka-topics"

# A preserved topic and an ordinary one, taken from the declaration itself so this cannot drift into
# naming a topic topics.env does not preserve.
# The helper exposes the RESOLVED list as $RESET_PRESERVED_TOPICS once sourced, which is also what
# is_reset_preserved reads — so the fixture comes from the same value the code under test consults.
PRESERVED_ONE="$(bash -c '. "$1/reset-preserved-topics.sh"; printf "%s\n" $RESET_PRESERVED_TOPICS' _ "$HERE" | head -1)"
[ -n "$PRESERVED_ONE" ] || { echo "FAIL: could not read a preserved topic from the declaration" >&2; exit 1; }
echo "preserved topic under test: $PRESERVED_ONE"

wait_with() { # <source-dir-of-replay-script> <list-file> -> RC, OUT
  OUT="$(PATH="$WORK/bin:$PATH" STUB_LIST="$2" TOPIC_DELETE_WAIT_SECONDS=2 KAFKA_BOOTSTRAP_SERVERS=stub:9092 \
    bash -c '
      . "$1/reset-preserved-topics.sh"
      source <(sed -n "/^wait_topics_deleted() {/,/^}/p" "$2")
      wait_topics_deleted' _ "$HERE" "$1/run-dev-timewarp-replay.sh" 2>&1)"
  RC=$?
}

echo "1. when ONLY preserved topics remain, the wait is satisfied"
printf '__consumer_offsets\n__transaction_state\n%s\n' "$PRESERVED_ONE" > "$WORK/only-kept"
wait_with "$HERE" "$WORK/only-kept"
[ "$RC" -eq 0 ] && ok "returns 0 instead of timing out" || bad "rc=$RC out=[$OUT]"

echo "2. an ORDINARY topic still there keeps it waiting, and the diagnostic names only that one"
printf '%s\noptions.databento.raw\n' "$PRESERVED_ONE" > "$WORK/one-left"
wait_with "$HERE" "$WORK/one-left"
[ "$RC" -ne 0 ] && ok "returns non-zero" || bad "returned 0 with an ordinary topic present"
printf '%s' "$OUT" | grep -q 'options.databento.raw' && ok "and names it" || bad "diagnostic: [$OUT]"
printf '%s' "$OUT" | grep -q "$PRESERVED_ONE" && bad "the diagnostic also names the PRESERVED topic" || ok "and does not name the preserved one"

echo "3. without the keep-list loaded it REFUSES rather than waiting on the wrong condition"
OUT="$(PATH="$WORK/bin:$PATH" STUB_LIST="$WORK/only-kept" TOPIC_DELETE_WAIT_SECONDS=2 KAFKA_BOOTSTRAP_SERVERS=stub:9092 \
  bash -c 'source <(sed -n "/^wait_topics_deleted() {/,/^}/p" "$1"); wait_topics_deleted' _ "$REPLAY" 2>&1)"; RC=$?
[ "$RC" -ne 0 ] && ok "refuses (exit $RC)" || bad "returned 0 with no keep-list loaded"
printf '%s' "$OUT" | grep -q 'not loaded' && ok "and says why" || bad "diagnostic: [$OUT]"

echo "4. the DELETE loop keeps a preserved topic, and deletes an ordinary one"
# Driven through the real loop body, with a kafka-topics stub that records its --delete calls.
mkdir -p "$WORK/bin2"
cat > "$WORK/bin2/kafka-topics" <<'EOF'
#!/usr/bin/env bash
for a in "$@"; do [ "$a" = --delete ] && { echo "$*" >> "$STUB_CALLS"; exit 0; }; done
cat "$STUB_LIST"
EOF
chmod +x "$WORK/bin2/kafka-topics"
: > "$WORK/calls"
printf '__consumer_offsets\n%s\noptions.databento.raw\n' "$PRESERVED_ONE" > "$WORK/dlist"
loop="$(sed -n '/^kafka-topics --bootstrap-server "\$KAFKA_BOOTSTRAP_SERVERS" --list/,/    done/p' "$REPLAY" | tail -n +1)"
OUT="$(PATH="$WORK/bin2:$PATH" STUB_LIST="$WORK/dlist" STUB_CALLS="$WORK/calls" KAFKA_BOOTSTRAP_SERVERS=stub:9092 \
  bash -c '. "$1/reset-preserved-topics.sh"; eval "$2"' _ "$HERE" "$loop" 2>&1)"
grep -q -- "--delete --topic options.databento.raw" "$WORK/calls" && ok "the ordinary topic is deleted" || bad "no delete recorded: $(cat "$WORK/calls")"
grep -q -- "--delete --topic $PRESERVED_ONE" "$WORK/calls" && bad "the PRESERVED topic was deleted" || ok "the preserved topic is not deleted"
printf '%s' "$OUT" | grep -q "KEEPING $PRESERVED_ONE" && ok "and the keep is logged by name" || bad "no keep log: [$OUT]"

echo "5. MUTATION: without the keep-branch the preserved topic is deleted again"
mkdir -p "$WORK/mut"; cp "$REPLAY" "$WORK/mut/run-dev-timewarp-replay.sh"; cp "$HERE/reset-preserved-topics.sh" "$WORK/mut/"
python3 - "$WORK/mut/run-dev-timewarp-replay.sh" <<'PY'
import sys
p=sys.argv[1]; s=open(p).read()
old = '''      if is_reset_preserved "$topic"; then
        echo "[replay] KEEPING $topic (RESET-PRESERVED in topics.env)"
        continue
      fi
'''
assert old in s, "the mutation did not apply -- the keep branch has moved"
open(p,"w").write(s.replace(old, "", 1))
PY
if [ $? -eq 0 ]; then
  : > "$WORK/calls2"
  mloop="$(sed -n '/^kafka-topics --bootstrap-server "\$KAFKA_BOOTSTRAP_SERVERS" --list/,/    done/p' "$WORK/mut/run-dev-timewarp-replay.sh")"
  PATH="$WORK/bin2:$PATH" STUB_LIST="$WORK/dlist" STUB_CALLS="$WORK/calls2" KAFKA_BOOTSTRAP_SERVERS=stub:9092 \
    bash -c '. "$1/reset-preserved-topics.sh"; eval "$2"' _ "$HERE" "$mloop" >/dev/null 2>&1
  grep -q -- "--delete --topic $PRESERVED_ONE" "$WORK/calls2" \
    && ok "the mutant DELETES it (case 4 is load-bearing)" || bad "the mutation changed nothing"
else bad "the keep-branch mutation did not apply"; fi

echo
if [ "$fails" -eq 0 ]; then echo "=== run-dev-timewarp-replay-keep: OK ==="; exit 0; fi
echo "=== run-dev-timewarp-replay-keep: $fails problem(s) ===" >&2; exit 1
