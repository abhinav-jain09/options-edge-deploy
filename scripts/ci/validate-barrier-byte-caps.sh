#!/usr/bin/env bash
# validate-barrier-byte-caps.sh — CI invariant: A DECLARED BYTE CAP MUST BE THE CAP A DEPLOY APPLIES.
#
# scripts/kafka/topics.env declares retention.bytes per topic. For the topics that have a FINITE
# POSITIVE cap, the thing that actually puts that number on the cluster before a service publishes is
# a topic barrier in scripts/kafka/ensure-*-topics.sh, which Jenkinsfile.service-deploy runs. Each of
# those barriers carries the number as its own literal. Two copies of a number, kept in step by hand.
#
# WHY THIS EXISTS, dated. On 2026-11-13 options.spx.gamma-effort.log's cap was raised from 256 to
# 512 MiB in topics.env (#1637), with the arithmetic for why 256 MiB was not enough corpus depth
# (21.8 sessions against the design's section 11 floor of 20 to 25). ensure-gamma-effort-topics.sh
# kept LOG_BYTES=268435456. Measured on the live dev broker 2026-11-14, a day later, the topic still
# reported retention.bytes=268435456: the raise never reached the cluster, because the barrier's
# literal is what both creates the topic and is checked against it, and the topic already existed.
# Nothing was red. The only tell was that the comment carrying the arithmetic had come to contradict
# itself. Every other declared cap was measured live the same day and did agree, so this was one
# instance, not a fleet-wide drift -- but nothing was checking, which is why it survived a day and
# would have survived indefinitely.
#
# WHAT IS PROVEN HERE, and it is behavioural rather than textual: each barrier is RUN against the
# stateful mocked CLIs (tests/fake_kafka_cli.py, the stand-in its own -test.sh scripts use), and the
# topic it creates is read back. A text rule would have to guess which literal belongs to which
# topic; running the barrier asks the only question that matters -- what cap does a deploy leave on
# this topic -- and cannot be satisfied by a comment.
#
#   (1) For every barrier B and every declared-capped topic T that B CREATES: the cap B creates T
#       with is EXACTLY the one topics.env declares.
#   (2) B must never report OK with T at some OTHER cap. Raising it to the declared value is
#       allowed (no record is lost widening a cap); failing closed is allowed; passing while the
#       topic stays wrong is the defect above and is not.
#   (3) COMPLETENESS, by arithmetic rather than by a flag: every topic with a finite positive
#       declared cap is either created by a barrier checked under (1), or named in APPLIED_ELSEWHERE
#       below with the reason. A new capped topic that is in neither FAILS this validator.
#
# topics.env is PARSED, not sourced: it carries ENVIRONMENT-dependent stripping and would collide
# with this script's own variables.
set -euo pipefail
cd "$(dirname "$0")/../.."

FAKE="tests/fake_kafka_cli.py"
TOPICS_ENV="scripts/kafka/topics.env"
[ -f "$FAKE" ] || { echo "FAIL: $FAKE is missing — this validator drives the real barriers against it"; exit 1; }
[ -f "$TOPICS_ENV" ] || { echo "FAIL: $TOPICS_ENV is missing"; exit 1; }

# Topics whose finite cap is NOT applied by a service's own barrier. Each line: <topic> <reason>.
# These are applied by the topic pass (scripts/kafka/apply-topics.sh and the dev-cleanup recreate,
# which read topics.env directly, so there is no second literal to drift). A capped topic missing
# from both this list and a barrier fails (3) — it has no proven applier at all.
APPLIED_ELSEWHERE=$(cat <<'EOF'
es.tape-zones.board                 no service barrier; the topic pass applies it (measured live on dev 2026-11-14: 8589934592, agrees)
trading.order.intent                no service barrier; the topic pass applies it (measured live 2026-11-14: 1073741824, agrees)
trading.order.status                no service barrier; the topic pass applies it (measured live 2026-11-14: 1073741824, agrees)
early-auction.verdicts.shadow       ensure-early-auction-verdict-topics.sh creates and verifies this ledger but NEITHER applies NOR reads retention.bytes, so the topic pass is what puts the 8 GiB cap on it (measured live 2026-11-14: 8589934592, agrees). That barrier's own header argues the ledger must never be deleted because the promotion gate reads all of it, which a finite byte cap does do; whether the declaration or the comment is wrong is an owner decision and is deliberately NOT settled here.
EOF
)

# Barriers that need something seeded or set before they will run at all. Each line:
# <barrier-basename> <TAB> <seed|env> <TAB> <argument>. A barrier that cannot be driven and is not
# listed here fails: see (3).
SEEDS=$(cat <<'EOF'
ensure-strike-hold-topics.sh	seed	es.mbo.coverage 1 compact -1 -1
ensure-level-absorption-topics.sh	env	LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES=81675540
EOF
)

T="$(mktemp -d)"; trap 'rm -r "$T"' EXIT
mkdir -p "$T/bin"; for n in kafka-topics kafka-configs kafka-get-offsets; do cp "$FAKE" "$T/bin/$n"; chmod +x "$T/bin/$n"; done
fail=0
note() { printf '      %s\n' "$*"; }

# --- (a) the declared finite positive caps, parsed out of topics.env ---------------------------------
# Every *_TOPIC_RETENTION_BYTES_OVERRIDES assignment's quoted value, split on whitespace, keeping the
# pairs whose value is a positive integer. `-1` (unlimited) carries no number to drift and is skipped.
#
# LEADING WHITESPACE IS PART OF THE PATTERN. Several of these declarations sit INSIDE an `if` block
# and are indented (options.spx.strike-hold.scoring is one), and the first version of this parser
# anchored at column zero: it found 13 of the 14 declarations, missed strike-hold's silently, and
# exited OK over a topic it had never looked at. That is the hole
# scripts/ops/dev-cleanup-indented-declaration-test.sh already exists for.
DECLARED="$T/declared"
python3 - "$TOPICS_ENV" > "$DECLARED" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
out = {}
for m in re.finditer(r'^[ \t]*[A-Za-z_]*TOPIC_RETENTION_BYTES_OVERRIDES=(.*)$', text, re.M):
    for tok in m.group(1).strip().strip('"').split():
        if tok.startswith("$"):
            continue
        name, _, val = tok.partition("=")
        if val.isdigit() and int(val) > 0:
            out[name] = val
for k in sorted(out):
    print(k, out[k])
PY
[ -s "$DECLARED" ] || { echo "FAIL: parsed no finite positive retention.bytes declaration out of $TOPICS_ENV - the parser or the file changed shape"; exit 1; }

# THE PARSE IS CROSS-CHECKED AGAINST WHAT BASH ITSELF READS, in a throwaway subshell so nothing is
# imported into this validator. A text parse that quietly misses a declaration reports OK over a
# topic it never looked at, which this validator did on its first run; two independent reads that
# must agree is what makes the count above a measurement rather than a hope. The subshell read is
# the CROSS-CHECK, not the authority: the parse above stays the source of truth.
SOURCED="$T/sourced"
( set +u
  export ENVIRONMENT=dev
  # shellcheck source=/dev/null
  . "$TOPICS_ENV" >/dev/null 2>&1
  for kv in ${OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES:-}; do
    case "${kv#*=}" in
      -1|0|*[!0-9]*) ;;
      *) echo "${kv%%=*} ${kv#*=}" ;;
    esac
  done ) | sort -u > "$SOURCED"
if ! diff -u "$SOURCED" "$DECLARED" > "$T/diff"; then
  echo "FAIL: the text parse of $TOPICS_ENV and what bash reads from it DISAGREE on the finite byte caps."
  note "'-' lines are what bash read and this parser missed; '+' lines are what this parser invented."
  sed 's/^/      /' "$T/diff"
  exit 1
fi
echo "declared finite byte caps in $TOPICS_ENV: $(wc -l < "$DECLARED" | tr -d ' ') (text parse and a sourced read agree)"

# --- (b) which barrier mentions each capped topic ----------------------------------------------------
BARRIERS="$T/barriers"; : > "$BARRIERS"
while read -r topic cap; do
  for b in scripts/kafka/ensure-*.sh; do
    case "$b" in *-test.sh) continue ;; esac
    grep -qF -- "$topic" "$b" && echo "$topic $cap $b"
  done
done < "$DECLARED" > "$BARRIERS" || true

# --- helpers -----------------------------------------------------------------------------------------
reset_state() { echo '{"topics":{}}' > "$T/state.json"; : > "$T/calls"; }
put_topic() { # <name> <partitions> <policy> <retention.ms> <retention.bytes>
  python3 - "$T/state.json" "$@" <<'PY'
import json, sys
f, name, parts, policy, ms, by = sys.argv[1:7]
s = json.load(open(f))
s["topics"][name] = {"partitions": int(parts), "rf": 1, "end": 0,
                     "configs": {"cleanup.policy": policy, "retention.ms": ms, "retention.bytes": by,
                                 "max.message.bytes": "1048576", "min.insync.replicas": "1"}}
json.dump(s, open(f, "w"))
PY
}
get_bytes() { python3 -c 'import json,sys
t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2])
print("<absent>" if t is None else t["configs"].get("retention.bytes","<unset>"))' "$T/state.json" "$2"; }
seed_for() { printf '%s\n' "$SEEDS" | awk -F'\t' -v b="$1" '$1==b && $2=="seed"{print $3}'; }
env_for()  { printf '%s\n' "$SEEDS" | awk -F'\t' -v b="$1" '$1==b && $2=="env"{print $3}'; }
drive() { # <barrier-path> -> rc; output in $T/out
  local b="$1" base extra seedline
  base="$(basename "$b")"
  while IFS= read -r seedline; do [ -n "$seedline" ] && put_topic $seedline; done <<EOF
$(seed_for "$base")
EOF
  extra=()
  while IFS= read -r e; do [ -n "$e" ] && extra+=("$e"); done <<EOF
$(env_for "$base")
EOF
  env -u ENVIRONMENT PATH="$T/bin:$PATH" FAKE_KAFKA_STATE="$T/state.json" FAKE_KAFKA_CALLS="$T/calls" \
      KAFKA_BOOTSTRAP_SERVERS=mock:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT=dev \
      ${extra[@]+"${extra[@]}"} bash "$b" > "$T/out" 2>&1
}

# --- (1) and (2), per barrier ------------------------------------------------------------------------
CHECKED="$T/checked"; : > "$CHECKED"
for b in $(awk '{print $3}' "$BARRIERS" | sort -u); do
  base="$(basename "$b")"
  reset_state
  if ! drive "$b"; then
    echo "note: $base does not complete on an empty mocked cluster; the topics it creates there are still read back"
    note "$(tail -2 "$T/out")"
  fi
  while read -r topic cap bb; do
    [ "$bb" = "$b" ] || continue
    live="$(get_bytes "$T/state.json" "$topic")"
    case "$live" in
      '<absent>'|'<unset>')
        # Not created by this barrier (an input it only verifies, or a topic another path creates).
        continue ;;
    esac
    if [ "$live" != "$cap" ]; then
      echo "FAIL ($base): creates '$topic' with retention.bytes=$live, but $TOPICS_ENV declares $cap."
      note "The barrier's literal is what reaches the cluster, so the declaration is the one that is not applied."
      fail=1
      continue
    fi
    echo "ok   ($base): '$topic' created at the declared retention.bytes=$cap"
    echo "$topic" >> "$CHECKED"

    # (2) the same topic already present at a DIFFERENT cap: raised, or refused, never passed over.
    wrong=$(( cap / 2 )); [ "$wrong" -lt 1048576 ] && wrong=$(( cap * 2 ))
    # The policy and retention.ms a barrier demands differ per topic, and getting either wrong would
    # make it fail for the wrong reason. So take the shape the barrier ITSELF just created and move
    # ONLY retention.bytes off the declared value: that isolates the one thing under test.
    shape="$(python3 -c 'import json,sys
t=json.load(open(sys.argv[1]))["topics"][sys.argv[2]]
print(t["partitions"], t["configs"]["cleanup.policy"], t["configs"]["retention.ms"])' "$T/state.json" "$topic")"
    put_topic "$topic" $shape "$wrong"
    : > "$T/calls"
    if drive "$b"; then
      after="$(get_bytes "$T/state.json" "$topic")"
      if [ "$after" = "$cap" ]; then
        echo "ok   ($base): '$topic' found at $wrong was RAISED to the declared $cap and then accepted"
      else
        echo "FAIL ($base): reports OK with '$topic' at retention.bytes=$after, which is not the declared $cap."
        note "A barrier that passes over a wrong cap is how a declared raise fails to reach the cluster."
        fail=1
      fi
    else
      after="$(get_bytes "$T/state.json" "$topic")"
      if [ "$after" = "$wrong" ]; then
        echo "ok   ($base): '$topic' at $wrong is refused, and left untouched"
      else
        echo "FAIL ($base): refused '$topic' at $wrong but changed it to $after on the way out."
        fail=1
      fi
    fi
  done < "$BARRIERS"
done

# --- (3) completeness --------------------------------------------------------------------------------
while read -r topic cap; do
  grep -qxF "$topic" "$CHECKED" && continue
  if printf '%s\n' "$APPLIED_ELSEWHERE" | grep -qE "^$topic[[:space:]]"; then
    echo "ok   (not a barrier's): '$topic' — $(printf '%s\n' "$APPLIED_ELSEWHERE" | grep -E "^$topic[[:space:]]" | sed "s/^$topic[[:space:]]*//" | cut -c1-120)"
    continue
  fi
  echo "FAIL: '$topic' is declared retention.bytes=$cap in $TOPICS_ENV and NOTHING here proves a deploy applies it."
  note "Either a barrier must create it at that value (and be drivable by this validator), or it must be named in APPLIED_ELSEWHERE with the reason."
  fail=1
done < "$DECLARED"

if [ "$fail" -ne 0 ]; then
  echo "=== validate-barrier-byte-caps FAILED ==="
  exit 1
fi
echo "=== validate-barrier-byte-caps OK: every finite declared byte cap has a proven applier ==="
