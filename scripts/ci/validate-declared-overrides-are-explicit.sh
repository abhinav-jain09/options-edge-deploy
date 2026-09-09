#!/usr/bin/env bash
# Every topic in the retention-override declaration that applies to THIS environment must carry that
# override on the topic itself, not inherit it from a broker default.
#
# Scope follows apply-topics.sh exactly, so the guard asks for what that script would have written
# and nothing else: TOPIC_SET=es4 REPLACES the list with OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES,
# and otherwise it is OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES plus the prod-only set when
# ENVIRONMENT=production.
#
# The distinction is not academic. apply-topics.sh writes retention.ms EXPLICITLY for every declared
# topic on every run (alter_topic_config, called unconditionally), so a topic with no override of its
# own is a topic that stage has not reached: it was auto-created by a still-running producer — which
# is how dev lost es.futures.cvd.bars on 2026-08-07 — or the Kafka Topics stage was skipped
# (SKIP_KAFKA_TOPICS) on every deploy since the topic was declared.
#
# Such a topic sits on the broker's defaults. Those defaults may HAPPEN to match what the declaration
# asks for, and then nothing looks wrong: `kafka-configs --describe` shows no drift because it shows
# no topic-level config at all. The agreement is not configuration — it is the absence of it — and it
# ends the moment a broker default moves or a new broker joins, silently, on a history topic that is
# supposed to keep everything.
#
# Checked live against a broker, so it is a state assertion rather than a file comparison. Skips
# cleanly when the broker is unreachable: this runs in CI, and a network failure must not read as a
# configuration failure.
set -euo pipefail
cd "$(dirname "$0")/../.."

BOOTSTRAP="${1:-}"
[ -n "$BOOTSTRAP" ] || { echo "usage: $0 <bootstrap-servers> [topic ...]" >&2; exit 2; }
shift || true

# A caller error is refused BEFORE any reachability decision: otherwise a typo in TOPIC_SET
# becomes a silent pass the moment the broker happens to be unreachable.
case "${TOPIC_SET:-}" in
    "" ) ;;
    es4) ;;
    *) echo "Unknown TOPIC_SET '${TOPIC_SET}' (expected empty for dev/prod, or 'es4')" >&2; exit 2 ;;
esac

# A missing CLI is not an unreachable broker. This exited 0 with a SKIP, which meant that anywhere
# the Kafka tools were not on PATH — a changed confluent install path, a shim directory that did not
# make it onto an agent — the deploy shipped without this assertion running and said only that it
# had skipped. Tool absence is the caller's problem to fix, and the guard's job is to say so.
for cli in kafka-topics kafka-configs timeout; do
    command -v "$cli" >/dev/null 2>&1 || {
        echo "CANNOT READ: $cli is not on PATH, so the declared retention overrides were not checked" >&2
        exit 1
    }
done

# Only a CONNECTION failure licenses the skip. The first version treated every non-zero exit as
# unreachability, which is the widest possible fail-open: a missing Describe ACL, a bad JAAS config,
# a shim that cannot reach its container, a CLI that is not the CLI — each one exits non-zero with
# its own message, and each one made the guard print "not reachable" and pass. Match the failure
# against what an unreachable broker actually says (plus timeout's own 124) and fail on anything
# else, so an environment the guard cannot interrogate is reported rather than waved through.
#
# The list has to cover the ways a broker is genuinely out of reach, not just the ones seen in
# testing: a routing failure surfaces as NoRouteToHostException / "No route to host" and nothing
# else in this list matches it, which would fail a deploy for a network problem the guard is
# supposed to step aside for. `Connection timed out` is a separate form from `Timed out waiting`
# and from TimeoutException: it is what a firewall drop looks like.
#
# It is a list of CONNECTION failures, not of failures that mention a connection. "Connection reset
# by peer" is the one that reads like unreachability and is not: Kafka reports it when the broker is
# right there and rejects the SSL or SASL handshake, which is a configuration problem this guard
# must report rather than skip.
probe_rc=0
probe=$(timeout 30 kafka-topics --bootstrap-server "$BOOTSTRAP" --list 2>&1) || probe_rc=$?
if [ "$probe_rc" -ne 0 ]; then
    # Kafka's stock "terminated during authentication" diagnostic is AMBIGUOUS by the broker's own
    # admission: it lists invalid credentials, a firewall blocking TLS traffic, and a transient
    # network failure as alternative causes of the same message. It therefore lands here, in the
    # refusal, and that is deliberate rather than an oversight. A guard whose whole purpose is to
    # stop unverified retention state from shipping cannot resolve an ambiguity in favour of
    # skipping: a wrong refusal stops a deploy loudly and a human re-runs it, while a wrong skip
    # ships topics sitting on broker defaults and says nothing — which is how dev lost
    # es.futures.cvd.bars on 2026-08-07. Ambiguity is something the guard could not interrogate.
    #
    # Refusal comes FIRST, over the whole output. A real Kafka failure is many lines: an SSL or SASL
    # rejection prints its own cause and then, as the client keeps retrying, a metadata timeout. If
    # the connectivity allowlist is consulted first, that trailing timeout decides — and an
    # authentication failure, with the broker right there, reads as an unreachable broker and waves
    # through unverified retention state. Anything naming authentication, authorization or TLS is
    # something this guard could not interrogate, whatever else the output also says.
    if printf '%s' "$probe" | grep -qE \
        'Authentication|Authoriz|authoriz|Sasl|sasl\.|SSL|TLS|Certificate|CertPath|PKIX|KeyStore|TrustStore|Not authorized|AclAuthorizer|security\.protocol'; then
        printf 'CANNOT READ: kafka-topics --list failed on %s and the failure names authentication, authorization or TLS, not connectivity (exit %s: %s)\n' \
            "$BOOTSTRAP" "$probe_rc" "$(printf '%s' "$probe" | head -1)" >&2
        exit 1
    fi
    if [ "$probe_rc" -eq 124 ] || printf '%s' "$probe" | grep -qE \
        'Connection to node|Connection refused|Connection timed out|Timed out waiting|TimeoutException|Failed to update metadata|UnknownHost|No resolvable bootstrap|could not be established|Network is unreachable|No route to host|NoRouteToHost|Host is down|SocketTimeout'; then
        # Every caller of this guard is a DEPLOY: the dev/prod Jenkinsfile stage and
        # scripts/es4/create-es-topics.sh. On that path an unreachable broker is not a reason to
        # proceed — it is the guard being unable to check anything at all, and the deploy then ships
        # whatever retention state is really there. The skip is now an explicit opt-in for running
        # this by hand somewhere without a broker; no deploy path sets it, so no deploy takes it.
        if [ "${ALLOW_UNREACHABLE_BROKER:-}" = "true" ]; then
            printf 'SKIP: %s is not reachable from here (%s)\n' "$BOOTSTRAP" "$(printf '%s' "$probe" | head -1)"
            exit 0
        fi
        printf 'CANNOT READ: %s is not reachable, so the declared retention overrides were not checked (%s). Set ALLOW_UNREACHABLE_BROKER=true only where this is NOT a deploy.\n' \
            "$BOOTSTRAP" "$(printf '%s' "$probe" | head -1)" >&2
        exit 1
    fi
    printf 'CANNOT READ: kafka-topics --list failed on %s and the failure is not a connection failure (exit %s: %s)\n' \
        "$BOOTSTRAP" "$probe_rc" "$(printf '%s' "$probe" | head -1)" >&2
    exit 1
fi

# The declaration is the source of truth for WHICH topics must be explicit.
# shellcheck disable=SC1091
. scripts/kafka/topics.env

# Mirror apply-topics.sh's own selection exactly. Asking dev for a prod-only override, or asking a
# dev/prod broker for the es4 list, would be a finding the deploy never intended.
if [ "${TOPIC_SET:-}" = "es4" ]; then
    : "${OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES:?OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES missing from topics.env}"
    OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="$OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES"
elif [ "${ENVIRONMENT:-}" = "production" ]; then
    OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-} ${OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES:-}"
fi

# Mirror apply-topics.sh's own selection exactly. Asking dev for a prod-only override, or asking a
# dev/prod broker for the es4 list, would be a finding the deploy never intended.
declared() {
    printf '%s\n' $OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES | tr ' ' '\n' | sed -n 's/^\([^=]*\)=.*/\1/p'
}

# ...and the topic list apply-topics.sh would CREATE for this same scope. The two are not the same
# set: on dev and prod, eight override entries name topics that are not in the creation list at all
# (they are mirrored in, or created by another path), and those may legitimately be absent here.
# For a topic that IS in the creation list, absence is a finding rather than a skip — apply-topics
# creates every topic it declares, so a missing one means that stage did not run, which is exactly
# the SKIP_KAFKA_TOPICS case this guard exists to catch. Treating it as "not this guard's business"
# let a deploy proceed to the point where a producer auto-creates the topic on broker defaults.
if [ "${TOPIC_SET:-}" = "es4" ]; then
    CREATED="${OPTIONS_EDGE_ES4_TOPICS:-}"
elif [ "${ENVIRONMENT:-}" = "production" ]; then
    CREATED="${OPTIONS_EDGE_TOPICS:-} ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"
else
    CREATED="${OPTIONS_EDGE_TOPICS:-}"
fi
would_be_created() {
    local entry
    for entry in $CREATED; do
        [ "${entry%%:*}" = "$1" ] && return 0
    done
    return 1
}

# Topic names contain dots, and a name interpolated into a regex makes `.` match any character —
# so `es.futures.cvd` would also match `esXfuturesXcvd`. Split on the FIRST `=` with the shell's
# own string operators instead of a pattern, so the name is compared literally.
want_of() {
    local entry
    for entry in $OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES; do
        if [ "${entry%%=*}" = "$1" ]; then printf '%s' "${entry#*=}"; return 0; fi
    done
    return 0
}

only=("$@")
failed=0
checked=0
for topic in $(declared); do
    if [ ${#only[@]} -gt 0 ]; then
        # literal comparison, for the same reason want_of does not use a pattern
        wanted=0
        for t in "${only[@]}"; do [ "$t" = "$topic" ] && wanted=1; done
        [ "$wanted" = "1" ] || continue
    fi
    # A topic that does not exist here is not this guard's business — another guard owns creation.
    # But `--describe` also fails on a timeout or a missing Describe ACL, and treating that as
    # "absent" would skip a real retention failure and then report that nothing existed. Ask the
    # broker to LIST instead: an empty list for a name that the cluster has is unambiguous, while a
    # CLI failure is reported and fails the guard.
    listing=$(timeout 30 kafka-topics --bootstrap-server "$BOOTSTRAP" --list --topic "$topic" 2>&1) || {
        printf 'CANNOT READ: %s — kafka-topics --list failed (%s)\n' "$topic" "$(printf '%s' "$listing" | head -1)" >&2
        failed=1
        continue
    }
    if ! printf '%s\n' "$listing" | grep -qx "$topic"; then
        if would_be_created "$topic"; then
            printf 'DECLARED BUT ABSENT: %s does not exist on %s, and apply-topics.sh declares it here — that stage did not run, and a producer will auto-create it on the broker default (declaration says %s)\n' \
                "$topic" "$BOOTSTRAP" "$(want_of "$topic")" >&2
            failed=1
        fi
        continue    # absent and not created here: another path owns it
    fi
    checked=$((checked + 1))
    want=$(want_of "$topic")
    # `|| true`: a topic with NO override is precisely the case this guard exists to report, and
    # grep exits 1 when it matches nothing — under `set -e` that killed the script before it could
    # print the finding, so the check failed silently with no message at all.
    # Read ONLY the authoritative topic-level entry. `--describe` prints a `synonyms={...}` list on
    # the same line that repeats broker-level values, and `head -1` over every match made output
    # order decide the answer — able to invent drift or hide it. Take the value before `sensitive=`,
    # which is the topic's own, and refuse the topic outright if more than one such line appears.
    describe=$(timeout 30 kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics \
                  --entity-name "$topic" --describe 2>&1) || {
        printf 'CANNOT READ: %s — kafka-configs --describe failed (%s)\n' "$topic" "$(printf '%s' "$describe" | head -1)" >&2
        failed=1
        continue
    }
    matches=$(printf '%s\n' "$describe" | grep -cE '^[[:space:]]*retention\.ms=-?[0-9]+ sensitive=' || true)
    if [ "${matches:-0}" -gt 1 ]; then
        printf 'AMBIGUOUS: %s reports %s topic-level retention.ms lines; this guard will not guess\n' "$topic" "$matches" >&2
        failed=1
        continue
    fi
    have=$(printf '%s\n' "$describe" \
           | sed -n 's/^[[:space:]]*retention\.ms=\(-\{0,1\}[0-9]\{1,\}\) sensitive=.*/\1/p' | head -1 || true)
    if [ -z "$have" ]; then
        printf 'DECLARED BUT NOT SET: %s has no topic-level retention.ms; it is inheriting the broker default (declaration says %s)\n' "$topic" "$want" >&2
        failed=1
    elif [ "$have" != "$want" ]; then
        printf 'DRIFT: %s has retention.ms=%s, declaration says %s\n' "$topic" "$have" "$want" >&2
        failed=1
    fi
done

# Findings come FIRST. The skip used to be tested before `failed`, so a run in which every per-topic
# `--list` failed — checked stays 0, failed is 1, and each failure was already printed as CANNOT
# READ — exited 0 announcing that none of the declared topics existed. A guard that has just said it
# could not read the broker must not then report the broker as empty and pass.
[ "$failed" -eq 0 ] || exit 1
[ "$checked" -gt 0 ] || { echo "SKIP: none of the declared topics exist on $BOOTSTRAP"; exit 0; }
printf 'every declared retention override is set on the topic itself (%s checked on %s)\n' "$checked" "$BOOTSTRAP"
