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

command -v kafka-configs >/dev/null 2>&1 || { echo "SKIP: kafka-configs is not on PATH"; exit 0; }
if ! timeout 30 kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo "SKIP: $BOOTSTRAP is not reachable from here"
    exit 0
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
    # a topic that does not exist here is not this guard's business: another guard owns creation
    timeout 30 kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" >/dev/null 2>&1 || continue
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
                  --entity-name "$topic" --describe 2>/dev/null || true)
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

[ "$checked" -gt 0 ] || { echo "SKIP: none of the declared topics exist on $BOOTSTRAP"; exit 0; }
[ "$failed" -eq 0 ] || exit 1
printf 'every declared retention override is set on the topic itself (%s checked on %s)\n' "$checked" "$BOOTSTRAP"
