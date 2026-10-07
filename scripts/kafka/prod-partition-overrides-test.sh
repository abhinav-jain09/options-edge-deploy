#!/usr/bin/env bash
# OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES is the one lever that makes a declared partition count
# SMALLER for production, and it sits in front of two scripts that must agree: apply-topics.sh accepts
# the live topic, and verify-topics.sh must not then call the same topic "below the expected minimum".
# The ways it can go wrong are all quiet ones — the override silently not applying, applying to dev,
# applying to the es4 set, or surviving as a stale name after the declaration moves — so each is a case.
set -euo pipefail
cd "$(dirname "$0")/../.."
R=scripts/kafka/resolve-prod-partition-overrides.sh
[ -r "$R" ] || { echo "FAIL: $R not readable"; exit 1; }
fail=0

count_of() { # topic  [override-list]  [topic-list-var-override]
  local topic="$1" ovr="${2-__keep__}"
  ENVIRONMENT=production OVR="$ovr" TOPIC="$topic" bash -c '
    set -euo pipefail
    source scripts/kafka/topics.env
    [ "$OVR" = "__keep__" ] || OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="$OVR"
    source scripts/kafka/resolve-prod-partition-overrides.sh
    _oe_resolve_prod_partition_overrides >/dev/null
    for e in $OPTIONS_EDGE_TOPICS; do
      case "$e" in "$TOPIC":*) echo "${e##*:}"; exit 0 ;; esac
    done
    echo absent'
}
expect() { # name  expected  actual
  if [ "$2" = "$3" ]; then printf '  ok   %-56s %s\n' "$1" "$3"
  else printf '  FAIL %-56s expected=%s actual=%s\n' "$1" "$2" "$3"; fail=1; fi
}
refuses() { # name  override-list  want-substring
  local out rc
  set +e
  out=$(ENVIRONMENT=production OVR="$2" bash -c '
    source scripts/kafka/topics.env
    OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="$OVR"
    source scripts/kafka/resolve-prod-partition-overrides.sh
    _oe_resolve_prod_partition_overrides' 2>&1)
  rc=$?
  set -e
  if [ "$rc" != 0 ] && printf '%s' "$out" | grep -q -- "$3"; then printf '  ok   %-56s refused\n' "$1"
  else printf '  FAIL %-56s rc=%s out=%s\n' "$1" "$rc" "$out"; fail=1; fi
}

# ---- the live declaration ----
expect "production resolves the declared topic to its prod count" 1 "$(count_of options.spx.strike-invasion.current)"
expect "a topic with no override keeps its declared count" 32 "$(count_of options.spx.strike-sr.current)"

# ---- dev and es4 must be untouched: the resolver is only ever called inside the production branch,
#      so the check is that the raw declaration still says 32 and the es4 set never mentions it ----
dev=$(bash -c 'source scripts/kafka/topics.env; for e in $OPTIONS_EDGE_TOPICS; do case "$e" in options.spx.strike-invasion.current:*) echo "${e##*:}";; esac; done')
expect "the dev/shared declaration still says 32" 32 "$dev"
es4=$(bash -c 'source scripts/kafka/topics.env; case " ${OPTIONS_EDGE_ES4_TOPICS:-} " in *" options.spx.strike-invasion.current:"*) echo present;; *) echo absent;; esac')
expect "the es4 set does not carry the topic at all" absent "$es4"
for s in scripts/kafka/apply-topics.sh scripts/kafka/verify-topics.sh; do
  if grep -q 'ENVIRONMENT:-}" == "production"' "$s" && grep -q '_oe_resolve_prod_partition_overrides' "$s"; then
    printf '  ok   %-56s %s\n' "$s resolves under the production predicate" "wired"
  else
    printf '  FAIL %-56s %s\n' "$s resolves under the production predicate" "not wired"; fail=1
  fi
done

# ---- a second, synthetic override proves the mechanism is not hardcoded to one name ----
expect "an override applies to any declared topic" 4 "$(count_of options.spx.strike-sr.current 'options.spx.strike-sr.current=4')"
expect "...and leaves the others alone" 32 "$(count_of options.spx.strike-invasion.current 'options.spx.strike-sr.current=4')"

# ---- fail closed ----
refuses "an override for an UNDECLARED topic"        'no.such.topic=1'                      'NOT declared'
refuses "a non-numeric count"                        'options.spx.strike-sr.current=many'   'positive integer'
refuses "a zero count"                               'options.spx.strike-sr.current=0'      'zero partitions'
refuses "a malformed entry"                          'options.spx.strike-sr.current'        'not topic=partitions'

[ "$fail" = 0 ] || { echo "prod partition overrides: FAILED"; exit 1; }
echo "prod partition overrides: the production declaration resolves, dev and es4 are untouched, both scripts are wired, and four malformed/stale shapes are refused"
