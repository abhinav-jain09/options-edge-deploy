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

# A missing CLI must FAIL, not skip: a deploy that shipped without this assertion running must not
# read as one that ran it. All three the guard requires are covered, `timeout` included — every
# broker call is wrapped in it, so its absence changes what the probe means.
#
# The tools are hidden by building a PATH that holds ONLY what the guard needs, and leaving one out.
# Stripping directories from the real PATH instead removed /usr/bin — which on a Linux runner holds
# `timeout` AND `bash`, so the guard died with "bash: command not found" and the case proved nothing.
BASH_BIN=$(command -v bash)
mkdir -p "$WORK/sandbox"
for tool in bash env dirname grep sed head tr cut cat ls printf timeout; do
    real=$(command -v "$tool" 2>/dev/null) || continue
    ln -sf "$real" "$WORK/sandbox/$tool"
done
for missing in kafka-topics kafka-configs timeout; do
    rm -rf "$WORK/only"; mkdir -p "$WORK/only"
    for f in "$WORK/sandbox"/* "$WORK/bin"/*; do
        [ "$(basename "$f")" = "$missing" ] && continue
        ln -sf "$f" "$WORK/only/$(basename "$f")"
    done
    rc=0
    out=$(PATH="$WORK/only" FIXTURE="$WORK/ok" EXISTS="$WORK/exists" "$BASH_BIN" "$GUARD" fake:9092 2>&1) || rc=$?
    [ "$rc" = "1" ] || { printf 'a missing %s must FAIL the guard (exited %s)\n' "$missing" "$rc" >&2
                         printf '%s\n' "$out" | sed 's/^/    | /' >&2; exit 1; }
    printf '%s' "$out" | grep -qF "CANNOT READ: $missing is not on PATH" \
        || { printf 'and must name it: %s\n' "$out" >&2; exit 1; }
done
printf '  killed: %s\n' "a missing Kafka CLI or timeout fails the guard instead of skipping"

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

# An unreachable broker FAILS on a deploy path, and skips only when the caller says explicitly that
# this is not a deploy. Every caller of this guard is a deploy — the dev/prod Jenkinsfile stage and
# scripts/es4/create-es-topics.sh — and on that path being unable to reach the broker is being
# unable to check anything, not a reason to proceed.
cat > "$WORK/bin/kafka-topics" <<'DEAD'
#!/usr/bin/env bash
echo "Connection to node -1 (fake/1.2.3.4:9092) could not be established." >&2
exit 1
DEAD
chmod +x "$WORK/bin/kafka-topics"
got=$(run "$WORK/ok")
[ "$got" = "1" ] || { printf 'an unreachable broker must FAIL a deploy (exited %s)\n' "$got" >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
grep -q "CANNOT READ" "$WORK/out" || { printf 'and must say it could not read\n' >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
grep -q "DECLARED BUT NOT SET" "$WORK/out" && { printf 'and must NOT report inheritance it never observed\n' >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
printf '  killed: %s\n' "an unreachable broker fails the deploy rather than reporting inheritance"

got=$(ALLOW_UNREACHABLE_BROKER=true run "$WORK/ok")
[ "$got" = "0" ] || { printf 'ALLOW_UNREACHABLE_BROKER=true must restore the skip (exited %s)\n' "$got" >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
grep -q "SKIP:" "$WORK/out" || { printf 'and must say it skipped\n' >&2; sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
printf '  killed: %s\n' "the skip exists only for a caller that says it is not a deploy"

# Each way a broker is genuinely out of reach must still take the skip. A routing failure is the
# one that was missing: it matched nothing in the allowlist and so failed a deploy for a network
# problem the guard exists to step aside for.
for msg in "java.net.NoRouteToHostException: No route to host" \
           "java.net.ConnectException: Connection refused" \
           "java.net.ConnectException: Connection timed out" \
           "java.net.UnknownHostException: kafka.invalid" \
           "org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment."; do
    printf '#!/usr/bin/env bash\necho "%s" >&2\nexit 1\n' "$msg" > "$WORK/bin/kafka-topics"
    chmod +x "$WORK/bin/kafka-topics"
    got=$(ALLOW_UNREACHABLE_BROKER=true run "$WORK/ok")
    [ "$got" = "0" ] || { printf 'an unreachable broker must be RECOGNISED as unreachable: %s (exited %s)\n' "$msg" "$got" >&2
                          sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
    grep -q "SKIP:" "$WORK/out" || { printf 'and must say so: %s\n' "$msg" >&2
                                     sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
done
printf '  killed: %s\n' "every shape of an out-of-reach broker is recognised, routing failures included"

# ...but a failure that is NOT a connection failure must FAIL. Treating every non-zero exit as
# unreachability is the widest fail-open there is: a missing Describe ACL, a broken shim, or a CLI
# that is not the CLI would each make the guard announce "not reachable" and pass.
# "Connection reset by peer" is the trap: it reads like unreachability and is not. Kafka reports it
# when the broker is right there and rejects the SSL or SASL handshake — a configuration problem
# this guard must report, because skipping it waves through unverified retention state.
# The multi-line forms are the ones that matter: a real client prints its cause and THEN, as it
# keeps retrying, a metadata timeout. If the connectivity allowlist is consulted first, that
# trailing timeout decides and an authentication failure reads as an unreachable broker.
for msg in "TopicAuthorizationException: Not authorized to access topics: [Topic authorization failed.]" \
           "javax.net.ssl.SSLException: Connection reset by peer" \
           "SaslAuthenticationException: Authentication failed: Invalid username or password" \
           "javax.net.ssl.SSLHandshakeException: General SSLEngine problem
[2026-09-09 12:00:01,003] WARN Connection to node -1 (kafka/10.0.0.4:9093) terminated during authentication.
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment." \
           "SaslAuthenticationException: Authentication failed
[2026-09-09 12:00:02,113] WARN Bootstrap broker kafka:9093 disconnected
java.net.ConnectException: Connection timed out" \
           "javax.net.ssl.SSLProtocolException: Unexpected handshake message" \
           "javax.net.ssl.SSLPeerUnverifiedException: peer not authenticated" \
           "sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment." \
           "[2026-09-09 12:00:03,001] WARN Connection to node -1 (kafka/10.0.0.4:9093) terminated during authentication. This may happen due to any of the following reasons: (1) Authentication failed due to invalid credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.
org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment."; do
    printf '#!/usr/bin/env bash\ncat >&2 <<EOM\n%s\nEOM\nexit 1\n' "$msg" > "$WORK/bin/kafka-topics"
    chmod +x "$WORK/bin/kafka-topics"
    got=$(ALLOW_UNREACHABLE_BROKER=true run "$WORK/ok")
    [ "$got" = "1" ] || { printf 'a non-connection probe failure must FAIL even with the opt-in: %s (exited %s)\n' "$msg" "$got" >&2
                          sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
    grep -q "CANNOT READ" "$WORK/out" || { printf 'and must say it could not read: %s\n' "$msg" >&2
                                           sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
    grep -q "SKIP:" "$WORK/out" && { printf 'and must not call it unreachability: %s\n' "$msg" >&2
                                     sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
done
printf '  killed: %s\n' "a probe failure that is not a connection failure is reported, not skipped"
# The last of those is Kafka's stock "terminated during authentication" text, which names invalid
# credentials, a firewall, and a transient network issue as alternative causes of the SAME message.
# It is refused DELIBERATELY. A guard that exists to stop unverified retention state from shipping
# cannot resolve that ambiguity in favour of skipping: a wrong refusal stops a deploy loudly and a
# human re-runs it; a wrong skip ships topics on broker defaults and says nothing.
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

# A declared topic that apply-topics.sh WOULD have created here and is not on the broker is the
# SKIP_KAFKA_TOPICS case: the stage did not run, and a producer will auto-create it on the broker
# default. It fails; it is not "another guard's business".
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
[ "$got" = "1" ] || { printf 'an absent topic that this environment creates must FAIL (exited %s)\n' "$got" >&2
                      sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
for t in es.futures.footprint.bars es.futures.footprint.outcomes; do
    grep -qF "DECLARED BUT ABSENT: $t" "$WORK/out" \
        || { printf 'and must name it — %s was not reported\n' "$t" >&2
             sed 's/^/    | /' "$WORK/out" >&2; exit 1; }
done
printf '  killed: %s\n' "a declared topic this environment creates, absent, fails instead of skipping"

# ...but an override on a topic NOTHING here creates may legitimately be absent: eight dev/prod
# override entries name topics that are mirrored in or created by another path, and failing on those
# would break every dev and prod deploy. The name is read from the declaration itself, so this case
# cannot drift into testing a topic that is in fact created here.
ORPHAN=$(python3 - <<'ORPHAN_PY'
import re
env = open('scripts/kafka/topics.env').read()
def var(n):
    out = []
    for m in re.finditer(rf'^{n}="([^"]*)"$', env, re.M):
        out += m.group(1).split()
    return [v for v in out if not v.startswith('$')]
over = {e.split('=')[0] for e in var('OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES')}
made = {e.rsplit(':', 1)[0] for e in var('OPTIONS_EDGE_TOPICS')}
print(sorted(over - made)[0] if over - made else '')
ORPHAN_PY
)
[ -n "$ORPHAN" ] || { echo "no override names a topic this environment does not create — case cannot run" >&2; exit 1; }
: > "$WORK/none-exist"
rc=0
out=$(FIXTURE="$WORK/ok" EXISTS="$WORK/none-exist" bash "$GUARD" fake:9092 "$ORPHAN" 2>&1) || rc=$?
[ "$rc" = "0" ] || { printf 'an override on a topic nothing here creates must skip when absent: %s (exited %s)\n' "$ORPHAN" "$rc" >&2
                     printf '%s\n' "$out" | sed 's/^/    | /' >&2; exit 1; }
printf '%s' "$out" | grep -q "DECLARED BUT ABSENT" \
    && { printf 'and must not report it: %s\n' "$out" >&2; exit 1; }
printf '  killed: %s\n' "an override on a topic this environment does not create is not a finding when absent"

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
