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
#       topic stays wrong is the defect above and is not. ONE barrier opts out of this on purpose
#       and is named -- barrier AND topic -- with its reason, in ACCEPTS_ANY_FINITE_CAP below.
#   (2b) And B must not alter T's cap on the way to REFUSING it. Presented with T at 32 partitions
#       -- dev's broker default under auto-create, the shape every barrier here exists to refuse --
#       and a wrong cap, B must refuse and leave the cap alone. The deploy fails either way, so an
#       alter there is pure loss, and the raise added for gamma-effort did exactly that until it
#       was gated on the shape being right.
#   (3) COMPLETENESS, by arithmetic rather than by a flag: every topic with a finite positive
#       declared cap is either created by a barrier checked under (1), or named in APPLIED_ELSEWHERE
#       below with the reason. A new capped topic that is in neither FAILS this validator.
#
# WHAT THIS DOES *NOT* PROVE, stated exactly, because a guard that promises more than its mechanism
# delivers is worse than no guard -- and one that promises less sends the reader looking for a hole
# that is already closed. (2) accepts failing closed as readily as raising, so this validator does
# NOT require that a RAISED declaration reach an ALREADY EXISTING topic -- the half of the
# gamma-effort defect that cost a day. Measured, by doing it: DELETING the raise from
# ensure-gamma-effort-topics.sh leaves this validator green and turns
# scripts/kafka/ensure-gamma-effort-topics-test.sh red, which is the division of labour intended.
# A barrier that chooses to fail closed instead -- gamma-zone, strike-hold, es-liquidity-wall and
# es-liquidity-engine all do -- is making a deliberate choice this validator is not the place to
# overrule, so the raise requirement is pinned per barrier and the per-barrier test is where it
# belongs. What (2b) DOES catch, for every barrier and without knowing anything barrier-specific,
# is a raise that is not gated on the shape: measured, by ungating gamma-effort's, which this
# validator then reports as "refused ... but moved its retention.bytes ... on the way out".
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

# Topics whose barrier DELIBERATELY accepts any finite positive cap, so check (2) does not apply to
# them. Each line: <topic> <reason>. (1) still does -- the value the barrier CREATES the topic with
# must be the declared one -- and these are reported, not hidden: a silent exemption is how a guard
# stops being one.
#
# This list earned itself. The wrong-cap fixture originally replaced a topic's whole config, which
# dropped message.timestamp.type=CreateTime from futures.break-score.events; its barrier then
# refused for the missing timestamp setting and the case recorded a "refusal" that had nothing to do
# with the cap (Codex r2). Fixing the fixture turned the case red and showed the real position:
# ensure-break-score-topics.sh accepts the wrong cap on purpose and says so in its own header.
# Keyed on the BARRIER as well as the topic: an exemption is a statement about one barrier's
# documented behaviour, and matching the topic alone would silently extend it to a second barrier
# that started creating the same topic (Codex r3 -- a future risk, closed here rather than noted).
ACCEPTS_ANY_FINITE_CAP=$(cat <<'EOF'
ensure-break-score-topics.sh futures.break-score.events   accepts ANY finite positive retention.bytes on an existing topic BY DESIGN, stated in its own header: "the cap is an owner-adjustable number (raising it is a deliberate topic change)", and only an absent or unlimited one fails. So a drift on this topic would not be caught by its barrier. The live dev topic measured 16777216 on 2026-11-14 and agrees with the declaration; the CREATE path is still checked by (1) above.
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
# EXACT match on the first field, not a regex. A topic name is full of `.`, which inside a regex
# matches any character, so `grep -E "^$topic[[:space:]]"` let a DIFFERENT topic claim an existing
# exemption: a declaration for `trading.order.statu.` matched the `trading.order.status` row and was
# reported as covered while nothing applied it (Codex r1). Prefix pairs that really exist here
# (es.mbo.level-flow vs es.mbo.level-flow-final) were already safe because of the whitespace, but
# the metacharacter was not.
exempt_reason() { # <topic> -> the reason, or empty
  printf '%s\n' "$APPLIED_ELSEWHERE" | awk -v t="$1" '$1==t{sub(/^[^ \t]+[ \t]+/, ""); print; exit}'
}
any_cap_reason() { # <barrier-basename> <topic> -> the reason check (2) does not apply, or empty
  printf '%s\n' "$ACCEPTS_ANY_FINITE_CAP" | awk -v b="$1" -v t="$2" '$1==b && $2==t{sub(/^[^ \t]+[ \t]+[^ \t]+[ \t]+/, ""); print; exit}'
}

# --- (0) THE TABLES ABOVE ARE THEMSELVES CHECKED, BEFORE THEY ARE TRUSTED --------------------------
# Every row must carry a NON-EMPTY reason, and the readers below take the reason to be whatever
# follows the key fields. A row with the keys and nothing else fails BOTH ways and neither is
# visible: `<barrier> <topic>` with no reason makes the sub() not match, so the reader prints the
# row and the keys themselves are reported as the reason; `<barrier> <topic>   ` with trailing
# whitespace makes it match and print nothing, which silently turns the exemption OFF (Codex r4).
# An exemption is the one thing in this validator that makes a check NOT run, so an exemption
# nobody can read is worse than none. Checked here, once, rather than guessed at each use.
table_rows_well_formed() { # <table text> <key-field-count> <table name>
  printf '%s\n' "$1" | awk -v keys="$2" -v name="$3" '
    { line = $0 }
    NF == 0 { next }
    NF <= keys {
      printf "FAIL: %s has a row with no reason: %s\n", name, line
      bad = 1
      next
    }
    {
      reason = line
      for (i = 1; i <= keys; i++) sub(/^[ \t]*[^ \t]+/, "", reason)
      gsub(/^[ \t]+|[ \t]+$/, "", reason)
      if (reason == "") {
        printf "FAIL: %s has a row whose reason is only whitespace: %s\n", name, line
        bad = 1
      }
    }
    END { exit bad ? 1 : 0 }'
}
table_rows_well_formed "$APPLIED_ELSEWHERE" 1 APPLIED_ELSEWHERE || fail=1
table_rows_well_formed "$ACCEPTS_ANY_FINITE_CAP" 2 ACCEPTS_ANY_FINITE_CAP || fail=1
table_rows_well_formed "$SEEDS" 2 SEEDS || fail=1
if [ "$fail" -ne 0 ]; then
  echo "=== validate-barrier-byte-caps FAILED: its own exemption tables are unreadable, so nothing below is trustworthy ==="
  exit 1
fi

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
# CHANGE ONE KEY, leaving every other config exactly as the barrier left it. put_topic above
# REPLACES a topic's whole config dict, which is right for SEEDING an input that does not exist but
# wrong for the wrong-cap cases: on futures.break-score.events it dropped
# message.timestamp.type=CreateTime, so that barrier refused for the MISSING TIMESTAMP SETTING and
# the case reported the wrong cap as "refused" without the cap having been the reason (Codex r2 --
# the same shape as its r1 finding that a case must exercise what it names). Mutating in place also
# removes the earlier `shape` round-trip, and with it the word-splitting of a cleanup.policy and the
# traceback that an absent topic used to abort the whole validator with.
# ABSENCE HAS ITS OWN EXIT CODE, 2, and nothing else is silenced. The first version returned 1 for
# every failure and the call sites threw stderr away, so malformed JSON or an unwritable state file
# would have been reported as "the topic is absent" -- a false explanation of a real failure
# (Codex r3). It fails closed either way; the point is that the message be true.
mutate_topic() { # <name> <retention.bytes> [<partitions>] -> 2 = absent, 1 = anything else
  python3 - "$T/state.json" "$@" <<'PY'
import json, sys
f, name, by = sys.argv[1:4]
parts = sys.argv[4] if len(sys.argv) > 4 else None
s = json.load(open(f))
t = s["topics"].get(name)
if t is None:
    raise SystemExit(2)
t["configs"]["retention.bytes"] = by
if parts is not None:
    t["partitions"] = int(parts)
json.dump(s, open(f, "w"))
PY
}
# <what the case needs> -> prints the reason it could not be set up, or nothing
mutate_or_report() { # <label> <name> <retention.bytes> [<partitions>]
  local label="$1"; shift
  local rc2=0
  mutate_topic "$@" || rc2=$?
  case "$rc2" in
    0) return 0 ;;
    2) echo "FAIL ($base): '$1' is absent after rebuilding the mocked cluster, so $label cannot run." ;;
    *) echo "FAIL ($base): could not set up $label for '$1' (mutate_topic exit $rc2; its error is above)." ;;
  esac
  return 1
}
topic_parts() { python3 -c 'import json,sys
t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2])
print("" if t is None else t["partitions"])' "$T/state.json" "$1"; }
get_bytes() { python3 -c 'import json,sys
t=json.load(open(sys.argv[1]))["topics"].get(sys.argv[2])
print("<absent>" if t is None else t["configs"].get("retention.bytes","<unset>"))' "$T/state.json" "$2"; }
seed_for() { printf '%s\n' "$SEEDS" | awk -F'\t' -v b="$1" '$1==b && $2=="seed"{print $3}'; }
env_for()  { printf '%s\n' "$SEEDS" | awk -F'\t' -v b="$1" '$1==b && $2=="env"{print $3}'; }
drive() { # <barrier-path> -> rc; output in $T/out
  local b="$1" base extra seedline
  base="$(basename "$b")"
  while IFS= read -r seedline; do [ -n "$seedline" ] && put_topic $seedline || true; done <<EOF
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
# EVERY CASE STARTS FROM A REBUILT CLUSTER. The first version reset the mocked state once per
# BARRIER and then ran one wrong-cap case per topic against whatever the previous case had left
# behind. For a barrier with several capped topics (es-liquidity-engine has four) that made every
# case after the first vacuous: the earlier topic was still sitting at its refused half-cap, so the
# barrier refused for THAT reason and the case printed "refused, and left untouched" without ever
# testing its own topic (Codex r1). A case that can pass without exercising what it names is worse
# than no case, so each one now rebuilds from empty and moves exactly one topic's cap.
CHECKED="$T/checked"; : > "$CHECKED"

rebuild() { # <barrier> -> a cluster the barrier itself just created, from empty
  reset_state
  drive "$1" || return 1
}

for b in $(awk '{print $3}' "$BARRIERS" | sort -u); do
  base="$(basename "$b")"
  if ! rebuild "$b"; then
    echo "note: $base does not complete on an empty mocked cluster; the topics it creates there are still read back"
    note "$(tail -2 "$T/out")"
  fi
  # (1) read back, from this ONE clean run, every declared-capped topic this barrier created.
  CREATED="$T/created"; : > "$CREATED"
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
    echo "$topic $cap" >> "$CREATED"
  done < "$BARRIERS"

  # (2) one case per created topic, each from a rebuilt cluster: raised, or refused, never passed over.
  while read -r topic cap; do
    [ -n "$topic" ] || continue
    wrong=$(( cap / 2 )); [ "$wrong" -lt 1048576 ] && wrong=$(( cap * 2 ))
    any_cap="$(any_cap_reason "$base" "$topic")"
    if [ "$wrong" = "$cap" ]; then
      echo "FAIL ($base): cannot build a wrong cap for '$topic' distinct from the declared $cap — this case would be vacuous."
      fail=1
      continue
    fi
    if [ -n "$any_cap" ]; then
      echo "ok   ($base): '$topic' — check (2) does not apply: $(printf '%s' "$any_cap" | cut -c1-150)"
    else
    rebuild "$b" || true
    # Move ONLY retention.bytes off the declared value, on the topic the barrier itself just
    # created. Everything else -- partitions, cleanup.policy, retention.ms, the message bound, the
    # timestamp type -- stays exactly as the barrier left it, so a refusal here can only be about
    # the cap.
    if ! mutate_or_report "the wrong-cap case" "$topic" "$wrong"; then
      fail=1
      continue
    fi
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
      elif [ "$after" = "$cap" ]; then
        echo "FAIL ($base): refused '$topic' but RAISED its cap to $cap on the way out — a barrier that refuses must not mutate."
        fail=1
      else
        echo "FAIL ($base): refused '$topic' at $wrong but changed it to $after on the way out."
        fail=1
      fi
    fi
    fi

    # (2b) A WRONG SHAPE *AND* A WRONG CAP. Applies to EVERY barrier, including one exempt from (2):
    # how a barrier judges a cap is its own business, but none of them may alter a topic it refuses. Altering a topic on the way to refusing it is the one
    # thing a fail-closed barrier must never do, and the raise added for gamma-effort did exactly
    # that until it was gated on the shape being right (Codex r1). The case is generic because the
    # demand is: every barrier here requires EXACTLY ONE partition, which is also what it created
    # above, so 32 partitions -- dev's broker default under auto-create, and the shape these
    # barriers exist to refuse -- is an invalid shape for all of them without this validator having
    # to know anything barrier-specific. Only run it where the barrier did create one partition, so
    # the rule stays the barrier's own rather than this validator's assumption.
    rebuild "$b" || true
    parts="$(topic_parts "$topic")"
    if [ "$parts" = 1 ]; then
      if ! mutate_or_report "the wrong-shape case" "$topic" "$wrong" 32; then
        fail=1
        continue
      fi
      : > "$T/calls"
      if drive "$b"; then
        echo "FAIL ($base): reports OK with '$topic' at 32 partitions — the dev auto-create default is the shape these barriers exist to refuse."
        fail=1
      else
        after="$(get_bytes "$T/state.json" "$topic")"
        if [ "$after" = "$wrong" ]; then
          echo "ok   ($base): '$topic' at 32 partitions AND $wrong is refused with its cap untouched"
        else
          echo "FAIL ($base): refused '$topic' at 32 partitions but moved its retention.bytes $wrong -> $after on the way out."
          note "A barrier must not alter a topic it is refusing: the deploy fails anyway, so the alter is pure loss."
          fail=1
        fi
      fi
    fi
  done < "$CREATED"
done

# --- (3) completeness --------------------------------------------------------------------------------
while read -r topic cap; do
  grep -qxF "$topic" "$CHECKED" && continue
  reason="$(exempt_reason "$topic")"
  if [ -n "$reason" ]; then
    echo "ok   (not a barrier's): '$topic' — $(printf '%s' "$reason" | cut -c1-120)"
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
