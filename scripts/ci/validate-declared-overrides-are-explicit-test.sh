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
# `--list --topic NAME` prints the name when the cluster has it and nothing when it does not; the
# guard now uses that to tell "absent" from "could not read", so the fake has to behave the same way.
cat > "$WORK/bin/kafka-topics" <<'FAKE'
#!/usr/bin/env bash
topic=""
listing=0
while [ $# -gt 0 ]; do
  case "$1" in --topic) topic=$2; shift 2;; --list) listing=1; shift;; *) shift;; esac
done
if [ "$listing" = "1" ] && [ -n "$topic" ]; then
  grep -qx "$topic" "${EXISTS:-/dev/null}" && echo "$topic"
fi
exit 0
FAKE
cat > "$WORK/bin/kafka-configs" <<'FAKE'
#!/usr/bin/env bash
topic=""
while [ $# -gt 0 ]; do case "$1" in --entity-name) topic=$2; shift 2;; *) shift;; esac; done
line=$(grep -E "^${topic}=" "$FIXTURE" 2>/dev/null | head -1 | sed "s/^${topic}=//")
echo "Dynamic configs for topic $topic are:"
if [ -n "$line" ]; then
  # the real kafka-configs prints the synonyms list on the same line, repeating broker values that
  # differ from the topic's own — the parser must take the topic's, not whichever comes first
  echo "  retention.ms=$line sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=$line, DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=999, STATIC_BROKER_CONFIG:log.retention.ms=888}"
else
  # a topic with no override still reports the broker's own value as a synonym line
  echo "  min.insync.replicas=1 sensitive=false synonyms={DEFAULT_CONFIG:min.insync.replicas=1}"
fi
exit 0
FAKE
chmod +x "$WORK/bin/"*
export PATH="$WORK/bin:$PATH"

printf 'es.futures.footprint.bars\nes.futures.footprint.outcomes\n' > "$WORK/exists"
run() { FIXTURE=$1 EXISTS="${EXISTS_OVERRIDE:-$WORK/exists}" bash "$GUARD" fake:9092 es.futures.footprint.bars es.futures.footprint.outcomes > "$WORK/out" 2>&1 && echo 0 || echo $?; }
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

# A CONNECTION failure must not read as inheritance: the guard skips, it does not report a finding.
# The fake speaks the way an unreachable broker actually speaks, because that message is now what
# licenses the skip.
cat > "$WORK/bin/kafka-topics" <<'DEAD'
#!/usr/bin/env bash
echo "Connection to node -1 (fake/1.2.3.4:9092) could not be established." >&2
exit 1
DEAD
chmod +x "$WORK/bin/kafka-topics"
got=$(run "$WORK/ok")
[ "$got" = "0" ] || { printf 'an unreachable broker must SKIP, not fail (exited %s)\n' "$got" >&2; exit 1; }
grep -q "SKIP:" "$WORK/out" || { printf 'an unreachable broker must say it skipped\n' >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
printf '  killed: %s\n' "an unreachable broker skips rather than reporting inheritance"

# ...but a failure that is NOT a connection failure must FAIL. Treating every non-zero exit as
# unreachability is the widest fail-open there is: a missing Describe ACL, a broken shim, or a CLI
# that is not the CLI would each make the guard announce "not reachable" and pass.
cat > "$WORK/bin/kafka-topics" <<'DENIED'
#!/usr/bin/env bash
echo "TopicAuthorizationException: Not authorized to access topics: [Topic authorization failed.]" >&2
exit 1
DENIED
chmod +x "$WORK/bin/kafka-topics"
got=$(run "$WORK/ok")
[ "$got" = "1" ] || { printf 'a non-connection probe failure must FAIL, not skip (exited %s)\n' "$got" >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
grep -q "CANNOT READ" "$WORK/out" || { printf 'and must say it could not read: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
grep -q "SKIP:" "$WORK/out" && { printf 'and must NOT call an authorization failure unreachability: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "a probe failure that is not a connection failure is reported, not skipped"
cat > "$WORK/bin/kafka-topics" <<'ALIVE'
#!/usr/bin/env bash
topic=""
listing=0
while [ $# -gt 0 ]; do
  case "$1" in --topic) topic=$2; shift 2;; --list) listing=1; shift;; *) shift;; esac
done
if [ "$listing" = "1" ] && [ -n "$topic" ]; then
  grep -qx "$topic" "${EXISTS:-/dev/null}" && echo "$topic"
fi
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
out=$(FIXTURE="$WORK/ok" EXISTS="$WORK/exists" TOPIC_SET=bogus bash "$GUARD" fake:9092 2>&1) || rc=$?
[ "$rc" = "2" ] || { printf 'an unknown TOPIC_SET must be refused even when the broker is down (exited %s)\n' "$rc" >&2; exit 1; }
printf '%s' "$out" | grep -q "Unknown TOPIC_SET" || { printf 'and must say so: %s\n' "$out" >&2; exit 1; }
printf '  killed: %s\n' "an unknown TOPIC_SET is refused before reachability"
cat > "$WORK/bin/kafka-topics" <<'ALIVE'
#!/usr/bin/env bash
topic=""
listing=0
while [ $# -gt 0 ]; do
  case "$1" in --topic) topic=$2; shift 2;; --list) listing=1; shift;; *) shift;; esac
done
if [ "$listing" = "1" ] && [ -n "$topic" ]; then
  grep -qx "$topic" "${EXISTS:-/dev/null}" && echo "$topic"
fi
exit 0
ALIVE
chmod +x "$WORK/bin/kafka-topics"

# The guard must not silently check NOTHING because a helper went missing: the baseline output
# names how many topics it checked, and zero would be a skip, not a pass.
FIXTURE="$WORK/ok" EXISTS="$WORK/exists" bash "$GUARD" fake:9092 es.futures.footprint.bars es.futures.footprint.outcomes > "$WORK/out" 2>&1
grep -qE '\(2 checked' "$WORK/out" \
    || { printf 'the guard must report how many topics it actually checked: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "the guard reports what it checked, so checking nothing cannot read as passing"

# A per-topic CLI failure is NOT absence. Treating it as absence skipped a real retention failure
# and then reported that nothing existed — a permission or timeout problem reading as a pass.
cat > "$WORK/bin/kafka-configs" <<'BROKEN'
#!/usr/bin/env bash
echo "TimeoutException: describeConfigs" >&2
exit 1
BROKEN
chmod +x "$WORK/bin/kafka-configs"
got=$(run "$WORK/ok")
[ "$got" = "1" ] || { printf 'a failing describe must FAIL the guard, not skip (exited %s)\n' "$got" >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
grep -q "CANNOT READ" "$WORK/out" || { printf 'and must say it could not read: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "a failing describe is reported, not read as absence"

# a topic the cluster genuinely does not have is skipped, quietly
cat > "$WORK/bin/kafka-configs" <<'FAKE2'
#!/usr/bin/env bash
topic=""
while [ $# -gt 0 ]; do case "$1" in --entity-name) topic=$2; shift 2;; *) shift;; esac; done
line=$(grep -E "^${topic}=" "$FIXTURE" 2>/dev/null | head -1 | sed "s/^${topic}=//")
echo "Dynamic configs for topic $topic are:"
[ -n "$line" ] && echo "  retention.ms=$line sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=$line}"
exit 0
FAKE2
chmod +x "$WORK/bin/kafka-configs"
: > "$WORK/none-exist"
got=$(EXISTS_OVERRIDE="$WORK/none-exist" run "$WORK/ok")
[ "$got" = "0" ] || { printf 'a genuinely absent topic must be skipped (exited %s)\n' "$got" >&2; exit 1; }
grep -q "SKIP:" "$WORK/out" || { printf 'and must say it skipped: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "a genuinely absent topic is skipped, and says so"

# A per-topic `--list` failure is not absence either, and — the reason the ordering matters — when
# EVERY per-topic list fails, `checked` never leaves 0. The end-of-run skip used to be tested before
# `failed`, so the guard printed CANNOT READ for every topic and then exited 0 announcing that none
# of the declared topics existed on the broker. The reachability probe still succeeds here; only the
# per-topic calls fail, which is exactly what a topic-scoped Describe ACL looks like.
cat > "$WORK/bin/kafka-topics" <<'PARTLY'
#!/usr/bin/env bash
topic=""
listing=0
while [ $# -gt 0 ]; do
  case "$1" in --topic) topic=$2; shift 2;; --list) listing=1; shift;; *) shift;; esac
done
if [ "$listing" = "1" ] && [ -n "$topic" ]; then
  echo "TopicAuthorizationException: Not authorized to describe topic $topic" >&2
  exit 1
fi
exit 0
PARTLY
chmod +x "$WORK/bin/kafka-topics"
got=$(run "$WORK/ok")
[ "$got" = "1" ] || { printf 'a failing per-topic list must FAIL the guard, not pass as "none exist" (exited %s)\n' "$got" >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
for t in es.futures.footprint.bars es.futures.footprint.outcomes; do
    grep -qF "CANNOT READ: $t" "$WORK/out" \
        || { printf 'and must name each topic it could not read — %s was not reported\n' "$t" >&2
             sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
done
grep -q "none of the declared topics exist" "$WORK/out" \
    && { printf 'and must not then claim the broker has none of them: %s\n' "$(cat "$WORK/out")" >&2; exit 1; }
printf '  killed: %s\n' "every per-topic list failing fails the guard instead of passing as absence"

echo "every arm refused its own violation, for its own reason"
