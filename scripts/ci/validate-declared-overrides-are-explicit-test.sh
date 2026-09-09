#!/usr/bin/env bash
# Mutation test: each arm of validate-declared-overrides-are-explicit.sh must refuse its own
# violation, and must not refuse a correct state. Driven against a FAKE kafka-configs/kafka-topics
# on PATH, so it needs no broker and reports the same way in CI as on a laptop.
set -euo pipefail
cd "$(dirname "$0")/../.."

GUARD=$PWD/scripts/ci/validate-declared-overrides-are-explicit.sh
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin"

# A fake broker: kafka-topics --list/--describe always succeed; kafka-configs prints whatever the
# fixture file says for the topic, so a topic with no override prints no retention line at all.
cat > "$WORK/bin/kafka-topics" <<'FAKE'
#!/usr/bin/env bash
exit 0
FAKE
cat > "$WORK/bin/kafka-configs" <<'FAKE'
#!/usr/bin/env bash
topic=""
while [ $# -gt 0 ]; do case "$1" in --entity-name) topic=$2; shift 2;; *) shift;; esac; done
line=$(grep -E "^${topic}=" "$FIXTURE" 2>/dev/null | head -1 | sed "s/^${topic}=//")
echo "Dynamic configs for topic $topic are:"
[ -n "$line" ] && echo "  retention.ms=$line sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=$line}"
exit 0
FAKE
chmod +x "$WORK/bin/"*
export PATH="$WORK/bin:$PATH"

run() { FIXTURE=$1 bash "$GUARD" fake:9092 es.futures.footprint.bars es.futures.footprint.outcomes > "$WORK/out" 2>&1 && echo 0 || echo $?; }
expect() {  # $1 = code, $2 = message substring ("" when passing), $3 = label, $4 = fixture
    got=$(run "$4")
    if [ "$got" != "$1" ]; then
        printf 'MUTATION SURVIVED: %s (exited %s, expected %s)\n' "$3" "$got" "$1" >&2
        sed 's/^/    | /' "$WORK/out" >&2; exit 1
    fi
    if [ -n "$2" ] && ! grep -qF -- "$2" "$WORK/out"; then
        printf 'WRONG REASON: %s\n    expected to contain: %s\n' "$3" "$2" >&2
        sed 's/^/    | /' "$WORK/out" >&2; exit 1
    fi
    printf '  killed: %s\n' "$3"
}

printf 'es.futures.footprint.bars=-1\nes.futures.footprint.outcomes=-1\n' > "$WORK/ok"
printf 'es.futures.footprint.outcomes=-1\n'                              > "$WORK/missing"
printf 'es.futures.footprint.bars=43200000\nes.futures.footprint.outcomes=-1\n' > "$WORK/drift"
printf ''                                                                > "$WORK/none"

echo "baseline"
expect 0 "every declared retention override is set on the topic itself" "both overrides set as declared" "$WORK/ok"
echo "mutations"
expect 1 "DECLARED BUT NOT SET: es.futures.footprint.bars" "one topic inheriting the broker default" "$WORK/missing"
expect 1 "DRIFT: es.futures.footprint.bars has retention.ms=43200000" "one topic set to the wrong value" "$WORK/drift"
# name BOTH topics, so "every topic inheriting" cannot pass on one of them
got=$(run "$WORK/none")
[ "$got" = "1" ] || { printf 'MUTATION SURVIVED: every topic inheriting the default (exited %s)\n' "$got" >&2; exit 1; }
for t in es.futures.footprint.bars es.futures.footprint.outcomes; do
    grep -qF "DECLARED BUT NOT SET: $t" "$WORK/out" \
        || { printf 'WRONG REASON: every topic inheriting the default — %s was not reported\n' "$t" >&2
             sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
done
printf '  killed: %s\n' "every topic inheriting the default (both named)"

# a CLI failure must not read as inheritance: the guard skips, it does not report a finding
cat > "$WORK/bin/kafka-topics" <<'DEAD'
#!/usr/bin/env bash
exit 1
DEAD
chmod +x "$WORK/bin/kafka-topics"
got=$(run "$WORK/ok")
[ "$got" = "0" ] || { printf 'an unreachable broker must SKIP, not fail (exited %s)\n' "$got" >&2; exit 1; }
grep -q "SKIP:" "$WORK/out" || { printf 'an unreachable broker must say it skipped\n' >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
printf '  killed: %s\n' "an unreachable broker skips rather than reporting inheritance"
cat > "$WORK/bin/kafka-topics" <<'ALIVE'
#!/usr/bin/env bash
exit 0
ALIVE
chmod +x "$WORK/bin/kafka-topics"
# The guard must REFUSE a caller error before it decides anything about reachability: a typo in
# TOPIC_SET became a silent pass the moment the broker happened to be down.
cat > "$WORK/bin/kafka-topics" <<'DEAD'
#!/usr/bin/env bash
exit 1
DEAD
chmod +x "$WORK/bin/kafka-topics"
# `|| rc=$?` — under `set -e` the assignment alone aborts the script on the very exit code it is
# trying to capture, which is how the previous version of this case never ran at all.
rc=0
out=$(FIXTURE="$WORK/ok" TOPIC_SET=bogus bash "$GUARD" fake:9092 2>&1) || rc=$?
[ "$rc" = "2" ] || { printf 'an unknown TOPIC_SET must be refused even when the broker is down (exited %s)\n' "$rc" >&2; exit 1; }
printf '%s' "$out" | grep -q "Unknown TOPIC_SET" || { printf 'and must say so: %s\n' "$out" >&2; exit 1; }
printf '  killed: %s\n' "an unknown TOPIC_SET is refused before reachability"
cat > "$WORK/bin/kafka-topics" <<'ALIVE'
#!/usr/bin/env bash
exit 0
ALIVE
chmod +x "$WORK/bin/kafka-topics"

# The guard must not silently check NOTHING because a helper went missing: the baseline output
# names how many topics it checked, and zero would be a skip, not a pass.
FIXTURE="$WORK/ok" bash "$GUARD" fake:9092 es.futures.footprint.bars es.futures.footprint.outcomes > "$WORK/out" 2>&1
grep -qE '\(2 checked' "$WORK/out" \
    || { printf 'the guard must report how many topics it actually checked: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "the guard reports what it checked, so checking nothing cannot read as passing"

echo "every arm refused its own violation, for its own reason"
