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
cases=0
refusals=0

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
  cases=$((cases+1))
  if [ "$2" = "$3" ]; then printf '  ok   %-56s %s\n' "$1" "$3"
  else printf '  FAIL %-56s expected=%s actual=%s\n' "$1" "$2" "$3"; fail=1; fi
}
refuses() { # name  override-list  want-substring
  cases=$((cases+1)); refusals=$((refusals+1))
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

# ---- the REAL declaration, whatever it holds ----
# It is EMPTY as of 2026-10-08: the later reading of the production topic (32 partitions) makes the
# 10-07 entry of =1 false — see topics.env for both readings and for what they do and do not
# establish. So this does not assert a particular entry — it asserts
# that whatever the list holds is ACCEPTABLE to the resolver, which is the property that matters: a
# stale name or a malformed count would refuse here, in CI, instead of at the next production run.
real=$(ENVIRONMENT=production bash -c '
  source scripts/kafka/topics.env
  source scripts/kafka/resolve-prod-partition-overrides.sh
  _oe_resolve_prod_partition_overrides >/dev/null && echo accepted || echo refused' 2>&1 | tail -1)
expect "the real override list is accepted by the resolver" accepted "$real"
expect "a topic with no override keeps its declared count" 32 "$(count_of options.spx.strike-sr.current)"

# ---- and the mechanism itself, driven synthetically ----
expect "an override resolves a declared topic to its prod count" 1 \
  "$(count_of options.spx.strike-invasion.current 'options.spx.strike-invasion.current=1')"

# ---- dev and es4 must be untouched: the resolver is only ever called inside the production branch,
#      so the check is that the raw declaration still says 32 and the es4 set never mentions it ----
dev=$(bash -c 'source scripts/kafka/topics.env; for e in $OPTIONS_EDGE_TOPICS; do case "$e" in options.spx.strike-invasion.current:*) echo "${e##*:}";; esac; done')
expect "the dev/shared declaration still says 32" 32 "$dev"
es4=$(bash -c 'source scripts/kafka/topics.env; case " ${OPTIONS_EDGE_ES4_TOPICS:-} " in *" options.spx.strike-invasion.current:"*) echo present;; *) echo absent;; esac')
expect "the es4 set does not carry the topic at all" absent "$es4"
for s in scripts/kafka/apply-topics.sh scripts/kafka/verify-topics.sh; do
  cases=$((cases+1))
  if grep -q 'ENVIRONMENT:-}" == "production"' "$s" && grep -q '_oe_resolve_prod_partition_overrides' "$s"; then
    printf '  ok   %-56s %s\n' "$s resolves under the production predicate" "wired"
  else
    printf '  FAIL %-56s %s\n' "$s resolves under the production predicate" "not wired"; fail=1
  fi
done

# ---- a second, synthetic override proves the mechanism is not hardcoded to one name ----
expect "an override applies to any declared topic" 4 "$(count_of options.spx.strike-sr.current 'options.spx.strike-sr.current=4')"
expect "...and leaves the others alone" 32 "$(count_of options.spx.strike-invasion.current 'options.spx.strike-sr.current=4')"

audit=$(ENVIRONMENT=production bash -c '
  source scripts/kafka/topics.env
  OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="options.spx.strike-invasion.current=1"
  source scripts/kafka/resolve-prod-partition-overrides.sh
  _oe_resolve_prod_partition_overrides' 2>&1 | tail -1)
cases=$((cases+1))
case "$audit" in
  *"options.spx.strike-invasion.current:1"*) printf '  ok   %-56s %s\n' "the audit line names what it rewrote" "named" ;;
  *) printf '  FAIL %-56s %s\n' "the audit line names what it rewrote" "$audit"; fail=1 ;;
esac

# ---- a PROD-ONLY topic: the applier merges that set into OPTIONS_EDGE_TOPICS before resolving, the
#      verifier keeps it separate. Validating against one list only meant an override for such a topic
#      applied in the applier and was REFUSED in the verifier. Both input shapes are driven here. ----
merged=$(ENVIRONMENT=production bash -c '
  source scripts/kafka/topics.env
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS"   # the applier shape
  OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="es.futures.cvd.levels=4"
  source scripts/kafka/resolve-prod-partition-overrides.sh
  _oe_resolve_prod_partition_overrides >/dev/null || exit 1
  for e in $OPTIONS_EDGE_TOPICS; do case "$e" in es.futures.cvd.levels:*) echo "${e##*:}";; esac; done' 2>&1 | tail -1)
expect "a prod-only override resolves in the APPLIER shape" 4 "$merged"
separate=$(ENVIRONMENT=production bash -c '
  source scripts/kafka/topics.env
  OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="es.futures.cvd.levels=4"       # the verifier shape
  source scripts/kafka/resolve-prod-partition-overrides.sh
  _oe_resolve_prod_partition_overrides >/dev/null || exit 1
  for e in ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}; do case "$e" in es.futures.cvd.levels:*) echo "${e##*:}";; esac; done' 2>&1 | tail -1)
expect "...and in the VERIFIER shape, from the prod-only list" 4 "$separate"

# ---- fail closed ----
refuses "an override for an UNDECLARED topic"        'no.such.topic=1'                      'NOT declared'
refuses "a non-numeric count"                        'options.spx.strike-sr.current=many'   'canonical integer'
refuses "a zero count"                               'options.spx.strike-sr.current=0'      'canonical integer'
refuses "a non-canonical 00"                         'options.spx.strike-sr.current=00'     'canonical integer'
refuses "a leading-zero count"                       'options.spx.strike-sr.current=032'    'canonical integer'
refuses "digits followed by garbage"                 'options.spx.strike-sr.current=12garbage' 'canonical integer'
refuses "a count above the stated bound"             'options.spx.strike-sr.current=99999'  'canonical integer'
# bash arithmetic is 64-bit and WRAPS: this value compared as "not greater than 1024" and passed the
# bound it was supposed to fail, so the length is checked before any arithmetic
refuses "a count that overflows bash arithmetic"     'options.spx.strike-sr.current=18446744073709552640' 'canonical integer'
# two entries for one topic silently applied the LAST and wrote both into the audit line
refuses "the same topic declared twice"              'options.spx.strike-sr.current=1 options.spx.strike-sr.current=32' 'more than once'
refuses "a malformed entry"                          'options.spx.strike-sr.current'        'not topic=partitions'
# topic=1=2 quietly produced count=2 and applied it: a typo that moves the partition floor in silence
refuses "an entry with two '='"                      'options.spx.strike-sr.current=1=2'    "more than one '='"
refuses "an empty topic name"                        '=4'                                   'empty topic name'

# ---- and now the SCRIPTS themselves, with mocked Kafka CLIs and a SYNTHETIC override ----
# Nothing below reads the live cluster or the live override list: the declaration is trimmed to two
# topics and the override is supplied by the fixture, so these cases prove the mechanism, not that any
# particular topic is currently smaller on production.
# The cases above read declarations and check that both scripts are wired. That is not the same as
# running them: the override only matters if apply-topics.sh stops listing the topic as unreconciled
# AND verify-topics.sh stops calling it "below the expected minimum" — on production, while dev and
# the es4 set keep refusing it. The declaration is trimmed to two topics so each run is fast; the
# scripts themselves, and the resolver they source, are the real ones.
fixture() { # -> prints a temp dir holding kafka/{apply,verify,resolver,topics.env} + mocked CLIs
  local tmp; tmp=$(mktemp -d); mkdir -p "$tmp/kafka" "$tmp/bin"
  cp scripts/kafka/apply-topics.sh scripts/kafka/verify-topics.sh "$tmp/kafka/"
  # ...plus every sibling each of them sources out of its own directory (topics.env and
  # resolve-prod-partition-overrides.sh today), derived rather than listed: a hard-coded list here is
  # what left two OTHER tests running crippled copies when apply-topics.sh gained a second sibling
  # (#1165), and the failure surfaced as findings about topics.env.
  for _s in apply-topics.sh verify-topics.sh; do
    for _sib in $(bash scripts/kafka/apply-topics-sibling-files.sh "scripts/kafka/$_s"); do
      cp "scripts/kafka/$_sib" "$tmp/kafka/" || return 1
    done
  done
  # trimmed declaration, appended so it wins over everything the real file built up
  cat >> "$tmp/kafka/topics.env" <<'T'
OPTIONS_EDGE_TOPICS="options.spx.strike-invasion.current:32 options.spx.strike-sr.current:32"
OPTIONS_EDGE_ES4_TOPICS="es.futures.cvd:1"
OPTIONS_EDGE_ES4_COMPACTED_TOPICS=""
OPTIONS_EDGE_ES4_PURE_COMPACT_TOPICS=""
OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS=""
OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES=""
OPTIONS_EDGE_ES4_TOPIC_RETENTION_BYTES_OVERRIDES=""
OPTIONS_EDGE_COMPACTED_TOPICS=""
OPTIONS_EDGE_PURE_COMPACT_TOPICS=""
OPTIONS_EDGE_EXACT_PARTITION_TOPICS=""
OPTIONS_EDGE_PROD_ONLY_TOPICS="es.futures.cvd.levels:8"
OPTIONS_EDGE_PROD_ONLY_PURE_COMPACT_TOPICS=""
# KEPT, not emptied: verify-topics.sh checks a prod-only topic's partition count only through this
# list, so with it empty the "both scripts need the override" case proved nothing about the VERIFIER.
OPTIONS_EDGE_PROD_ONLY_EXACT_PARTITION_TOPICS="es.futures.cvd.levels"
OPTIONS_EDGE_PROD_ONLY_TOPIC_RETENTION_OVERRIDES=""
OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS=""
OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES=""
OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES=""
OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES=""
OPTIONS_EDGE_PROD_ONLY_PARTITION_OVERRIDES="options.spx.strike-invasion.current=1 es.futures.cvd.levels=1"
T
  # the broker: SYNTHETIC shapes, not a current production reading — strike-invasion.current is
  # reported at 1 partition so the prod-smaller case has something to resolve, the other at 32
  cat > "$tmp/bin/kafka-topics" <<'K'
#!/usr/bin/env bash
name=""; prev=""
for a in "$@"; do [ "$prev" = "--topic" ] && name="$a"; prev="$a"; done
if [[ "$*" == *--list* ]]; then echo "options.spx.strike-invasion.current"; echo "options.spx.strike-sr.current"; echo "es.futures.cvd"; exit 0; fi
if [[ "$*" == *--describe* ]]; then
  parts=32
  [ "$name" = options.spx.strike-invasion.current ] && parts=1
  [ "$name" = es.futures.cvd ] && parts=1
  [ "$name" = es.futures.cvd.levels ] && parts=1
  echo "Topic: $name TopicId: ID PartitionCount: $parts ReplicationFactor: 1"
fi
exit 0
K
  cat > "$tmp/bin/kafka-configs" <<'K'
#!/usr/bin/env bash
name=""; prev=""
for a in "$@"; do [ "$prev" = "--entity-name" ] && name="$a"; prev="$a"; done
echo "Dynamic configs for topic $name are: cleanup.policy=delete sensitive=false, retention.ms=-1 sensitive=false, retention.bytes=-1 sensitive=false"
exit 0
K
  chmod +x "$tmp/bin/kafka-topics" "$tmp/bin/kafka-configs"
  printf '%s' "$tmp"
}

script_case() { # name  script  env(production|"")  topic-set  expect(pass|fail)  want-substring  [absent-substring]
  cases=$((cases+1))
  local name="$1" script="$2" env="$3" set="$4" expect="$5" want="$6" absent="${7:-}"
  local tmp out rc; tmp=$(fixture)
  set +e
  out=$(cd "$tmp" && PATH="$tmp/bin:$PATH" KAFKA_BOOTSTRAP_SERVERS=mock:9092 \
        KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT="$env" TOPIC_SET="$set" \
        bash "$tmp/kafka/$script" 2>&1)
  rc=$?
  set -e
  rm -rf "$tmp"
  local ok=0
  { [ "$expect" = pass ] && [ "$rc" = 0 ]; } && ok=1
  { [ "$expect" = fail ] && [ "$rc" != 0 ]; } && ok=1
  if [ "$ok" = 1 ] && printf '%s' "$out" | grep -q -- "$want" \
     && { [ -z "$absent" ] || ! printf '%s' "$out" | grep -q -- "$absent"; }; then
    printf '  ok   %-56s rc=%s\n' "$name" "$rc"
  else
    printf '  FAIL %-56s rc=%s want=%s/%s\n' "$name" "$rc" "$expect" "$want"
    printf '%s\n' "$out" | tail -4 | sed 's/^/        /'
    fail=1
  fi
}

script_case "apply-topics ACCEPTS the 1-partition topic on prod"  apply-topics.sh  production "" pass "production declaration adjusted"
script_case "apply-topics still REFUSES it without production"    apply-topics.sh  ""         "" fail "below declared minimum 32"
script_case "...and with ENVIRONMENT=dev, not just unset"          apply-topics.sh  dev        "" fail "below declared minimum 32"
script_case "verify-topics ACCEPTS it on prod"                    verify-topics.sh production "" pass "production declaration adjusted"
script_case "verify-topics still REFUSES it without production"   verify-topics.sh ""         "" fail "expected at least 32"
script_case "...and with ENVIRONMENT=dev, not just unset"          verify-topics.sh dev        "" fail "expected at least 32"
# ABSENCE of the audit line, not merely the presence of an es4 log line: the resolver must not run at
# all under a topic set, and "it printed something about es4" would not have shown that.
script_case "the es4 APPLIER never sees the override"             apply-topics.sh  production es4 pass "TOPIC_SET='es4'" "production declaration adjusted"
script_case "the es4 VERIFIER never sees it either"               verify-topics.sh production es4 pass "TOPIC_SET='es4'" "production declaration adjusted"

[ "$fail" = 0 ] || { echo "prod partition overrides: FAILED"; exit 1; }
echo "prod partition overrides: $cases cases — the REAL override list (empty as of 2026-10-08) is accepted, the mechanism resolves a synthetic override and names it in the audit line, dev and es4 are untouched in BOTH scripts, $refusals malformed, stale or duplicated shapes are refused, and both REAL scripts accept a prod-smaller topic on production while still refusing it off production"
