#!/usr/bin/env bash
# The seven vol-premium Gate-1 topics (topics.env; runbook VOL-PREMIUM-DANGER-CLOCK-RUNBOOK.md, rollout step 3),
# driven through the REAL, unmodified apply-topics.sh and cleanup-topics.sh against mocked kafka CLIs — the
# technique of apply-topics-ledger-safety-test.sh and apply-topics-strike-safety-test.sh.
#
#   durable (kept forever: RESET-PRESERVED, NEVER-RECREATE, retention.ms=-1 AND retention.bytes=-1):
#     options.spx.vol-premium.{ivrv,events,warnings,baseline,calendar}
#   rebuildable: options.spx.vol-premium.current (the served last-value view), options.spx.vol-premium.dlq (diagnostics)
#
# For the dev AND the production resolution of topics.env it asserts:
#   1. creation  — each topic is created once, at exactly 1 partition, with EXACTLY the declared config set, then
#                  reconciled once with the same set. The expected values are written out in want_cfg below, not
#                  read back from topics.env, so a wrong declaration cannot vouch for itself.
#   2. reconcile — an existing topic whose every declared config has drifted ends at the declaration.
#   3. drift     — at 4 partitions with KAFKA_RECREATE_MISMATCHED_TOPICS=false, every one of the seven stops the run
#                  before any delete or create. With =true the five durable topics still HARD-STOP (never-recreate)
#                  and only .current and .dlq are deleted and recreated, at 1 partition with the declared config.
#   4. cleanup   — the unwanted sweep, delete-recreate and retention shrink each spare the five durable topics, and
#                  cleanup-topics.sh says its reset-preserved guard is what spared them. .current and .dlq are kept by
#                  the sweep (declared) and are deleted / shrunk by the two destructive modes (approved, not preserved).
#   5. PROTECTED_TOPIC_REGEX alone still keeps the five from the unwanted sweep when every other declaration of them
#                  is gone (the belt-and-braces claim topics.env makes for that regex).
#   6. the test can fail — each membership that protects a durable topic (OPTIONS_EDGE_NEVER_RECREATE_TOPICS,
#                  OPTIONS_EDGE_RESET_PRESERVED_TOPICS, the retention.bytes override) is removed from a COPY of
#                  topics.env, one topic at a time, and so is .current from either compaction list. The matching
#                  section, run on that copy, must report a failure naming the topic. The real file is never touched.
#
# Every run is independent and writes only inside its own directory, so the runs execute in parallel (one per CPU).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
P="options.spx.vol-premium"
DURABLE="$P.ivrv $P.events $P.warnings $P.baseline $P.calendar"
REBUILT="$P.current $P.dlq"
ALL="$DURABLE $REBUILT"
JUNK="vp-safety.undeclared-junk"   # declared nowhere: the unwanted sweep must delete it, or the sweep never ran
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
MAXJ="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; }
is_durable() { case " $DURABLE " in *" $1 "*) return 0 ;; esac; return 1; }

# THE DECLARATION, WRITTEN OUT: the sorted key=value set apply-topics must write for a topic, both as its create
# --config flags and as its reconcile --add-config. .ivrv and .current are served topics: compact,delete on dev,
# delete on production (the engine's ensureServedTopic stamp). min.insync.replicas=1 is the environment's value
# (KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS below), not a topics.env declaration.
want_cfg() { # <env> <topic>
  local pol=delete
  case "$2" in "$P.ivrv"|"$P.current") [ "$1" = production ] || pol=compact,delete ;; esac
  case "$2" in
    "$P.dlq")     echo "cleanup.policy=$pol min.insync.replicas=1 retention.ms=2592000000" ;;
    "$P.current") echo "cleanup.policy=$pol min.insync.replicas=1 retention.ms=-1" ;;
    *)            echo "cleanup.policy=$pol min.insync.replicas=1 retention.bytes=-1 retention.ms=-1" ;;
  esac
}
# The drifted starting state for case 2: every key apply-topics writes holds a wrong value.
seed_cfg() { # <topic>
  case "$1" in
    "$P.current") echo "cleanup.policy=compact min.insync.replicas=2 retention.ms=604800000" ;;
    "$P.dlq")     echo "cleanup.policy=compact min.insync.replicas=2 retention.ms=-1" ;;
    *)            echo "cleanup.policy=compact min.insync.replicas=2 retention.bytes=1073741824 retention.ms=86400000" ;;
  esac
}

# --- mocked kafka CLIs (shared; every per-run input arrives through the environment) -------------------------
BIN="$WORK/bin"; mkdir -p "$BIN"
{ printf '#!%s\n' "$BASH"; cat <<'EOF'; } > "$BIN/kafka-topics"
# A stateful broker. Every call is logged, so the assertions are about what the script DID, not what it printed.
# Absent: deleted and not recreated, or listed in VP_ABSENT. A created topic reports the count it was created
# with; VP_DRIFT_TOPIC reports VP_DRIFT_PARTS; every other topic answers at its declared count (VP_DECLARED).
echo "kafka-topics $*" >> "$VP_LOG"
name=""; parts=""; prev=""
for a in "$@"; do
  [ "$prev" = --topic ] && name="$a"
  [ "$prev" = --partitions ] && parts="$a"
  prev="$a"
done
case " $* " in
  *" --list "*) for t in ${VP_LIST:-}; do echo "$t"; done ;;
  *" --create "*) rm -f "$VP_STATE/deleted-$name"; echo "$parts" > "$VP_STATE/created-$name" ;;
  *" --delete "*) rm -f "$VP_STATE/created-$name"; : > "$VP_STATE/deleted-$name" ;;
  *" --describe "*)
    [ -e "$VP_STATE/deleted-$name" ] && exit 0
    if [ -e "$VP_STATE/created-$name" ]; then read -r p < "$VP_STATE/created-$name"
    else
      case " ${VP_ABSENT:-} " in *" $name "*) exit 0 ;; esac
      if [ "$name" = "${VP_DRIFT_TOPIC:-}" ]; then p="$VP_DRIFT_PARTS"
      else p=1; for e in ${VP_DECLARED:-}; do [ "${e%%:*}" = "$name" ] && { p="${e##*:}"; break; }; done
      fi
    fi
    echo "Topic: $name TopicId: ID-$name PartitionCount: $p ReplicationFactor: 1" ;;
esac
exit 0
EOF
for cli in kafka-configs kafka-reassign-partitions; do
  { printf '#!%s\n' "$BASH"; printf 'echo "%s $*" >> "$VP_LOG"\n' "$cli"; } > "$BIN/$cli"
done
{ printf '#!%s\n' "$BASH"; echo 'echo "localhost:9092 (id: 1 rack: null) -> ("'; } > "$BIN/kafka-broker-api-versions"
chmod +x "$BIN"/kafka-*

cat > "$WORK/calls.py" <<'EOF'
# calls.py <create|alter|effective> <log> <topic> [seed] — what the mocked CLIs were asked to do to ONE topic.
#   create     one line per --create:  partitions=<n> rf=<rf> <sorted --config pairs>
#   alter      one line per --alter:   <sorted --add-config pairs>
#   effective  the config the topic ends with: the seed, replaced by a create, emptied by a delete, updated by each
#              alter in order. apply-topics never reads a topic's config, so replaying its writes IS the end state.
import sys
mode, log, topic = sys.argv[1:4]
state = dict(kv.partition("=")[::2] for kv in (sys.argv[4].split() if len(sys.argv) > 4 else []))

def arg(w, flag):
    i = w.index(flag) if flag in w else -1
    return w[i + 1] if 0 <= i < len(w) - 1 else None

def add_config(s):  # kafka-configs --add-config: entries split on commas, except inside [...]
    out, cur, depth = [], "", 0
    for ch in s:
        if ch == "[": depth += 1; continue
        if ch == "]": depth -= 1; continue
        if ch == "," and depth == 0: out.append(cur); cur = ""; continue
        cur += ch
    out.append(cur)
    return [kv for kv in out if kv]

lines = []
for raw in open(log):
    w = raw.split()
    if len(w) < 2:
        continue
    if w[0] == "kafka-topics" and arg(w, "--topic") == topic:
        if "--create" in w:
            cfg = sorted(w[i + 1] for i, x in enumerate(w[:-1]) if x == "--config")
            if mode == "create":
                lines.append(" ".join(["partitions=%s" % arg(w, "--partitions"), "rf=%s" % arg(w, "--replication-factor")] + cfg))
            state = dict(kv.partition("=")[::2] for kv in cfg)
        elif "--delete" in w:
            state = {}
    elif w[0] == "kafka-configs" and "--alter" in w and arg(w, "--entity-name") == topic:
        kvs = add_config(arg(w, "--add-config") or "")
        if mode == "alter":
            lines.append(" ".join(sorted(kvs)))
        for kv in kvs:
            k, _, v = kv.partition("=")
            state[k] = v
if mode == "effective":
    lines.append(" ".join(sorted("%s=%s" % kv for kv in state.items())))
print("\n".join(lines))
EOF
calls() { python3 "$WORK/calls.py" "$@"; }

cat > "$WORK/mutate.py" <<'EOF'
# mutate.py <in> <out> <var-regex> <name|key|decl> <token>... — remove the tokens from the value of every assignment
# VAR="..." whose name fully matches <var-regex> (plain and "$VAR ..." append forms alike).
#   name: the token itself   key: token=<value>   decl: token, token:<n> or token=<value>
# Exits 1 when it removed nothing, so a mutation that silently misses cannot pass as applied.
import re, sys
src, dst, var_re, kind = sys.argv[1:5]
toks = set(sys.argv[5:])
assign = re.compile(r'^((?:%s)=")(.*)("\s*)$' % var_re)
def hit(t):
    if kind == "name": return t in toks
    if kind == "key": return t.split("=", 1)[0] in toks
    return re.split(r"[:=]", t, 1)[0] in toks
out, removed = [], 0
for line in open(src).read().split("\n"):
    m = assign.match(line)
    if m:
        words = m.group(2).split(" ")
        keep = [w for w in words if not hit(w)]
        removed += len(words) - len(keep)
        line = m.group(1) + " ".join(keep) + m.group(3)
    out.append(line)
open(dst, "w").write("\n".join(out))
if not removed:
    sys.exit("removed nothing: none of %s in %s" % (sorted(toks), var_re))
print("removed %d token(s)" % removed)
EOF
mutate() { python3 "$WORK/mutate.py" "$@"; }

# --- runs --------------------------------------------------------------------------------------------------
declared_for() { # <src-dir> <env> -> the topic:count list apply-topics iterates there (the broker answers from it)
  ( . "$1/topics.env"
    if [ "$2" = production ]; then echo "$OPTIONS_EDGE_TOPICS ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"; else echo "$OPTIONS_EDGE_TOPICS"; fi )
}
resolved() { # <src-dir> <VAR> -> the value apply-topics / cleanup-topics see (topics.env is SOURCED, as they source it)
  ( . "$1/topics.env"; printf '%s\n' ${!2-} )
}
newrun() { mktemp -d "$WORK/run.XXXXXX"; }
rc_of() { cat "$1/rc"; }
n_calls() { grep -c -- "$2" "$1/log" || true; }
rx() { printf '%s' "$1" | sed 's/[.[\*^$]/\\&/g'; }
deleted() { grep -qE -- "--delete --topic $(rx "$2")\$" "$1/log"; }   # anchored: no prefix-sibling match
shrunk() { grep -qF -- "--entity-name $2 --alter" "$1/log"; }

# KAFKA_TOPIC_CLEANUP_POLICY=delete is the options-edge-config value (k8s/infra/base/configmap.yaml).
# KAFKA_TOPIC_RETENTION_MS=86400000 is deliberately a value no vol-premium declaration uses, so a LOST retention
# override shows up as a wrong retention.ms instead of coinciding with an environment default.
# KAFKA_COMPACTED_TOPIC_CLEANUP_POLICY is unset: nothing on the deploy path sets it.
apply_run() { # <run-dir> <src-dir> <env> <recreate-flag> <absent-topics> <drift-topic> <drift-partitions>
  local d="$1" src="$2"; mkdir -p "$d/state"; : > "$d/log"
  env -u KAFKA_COMPACTED_TOPIC_CLEANUP_POLICY -u TOPIC_SET PATH="$BIN:$PATH" \
    VP_LOG="$d/log" VP_STATE="$d/state" VP_ABSENT="$5" VP_DRIFT_TOPIC="$6" VP_DRIFT_PARTS="$7" \
    VP_DECLARED="$(declared_for "$src" "$3")" ENVIRONMENT="$3" KAFKA_RECREATE_MISMATCHED_TOPICS="$4" \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1 \
    KAFKA_TOPIC_CLEANUP_POLICY=delete KAFKA_TOPIC_RETENTION_MS=86400000 \
    KAFKA_TOPIC_DELETE_WAIT_SECONDS=2 KAFKA_TOPIC_REPAIR_WAIT_SECONDS=2 \
    "$BASH" "$src/apply-topics.sh" > "$d/out" 2>&1
  echo "$?" > "$d/rc"
}
cleanup_run() { # <run-dir> <src-dir> <env> <retention|delete-recreate> <delete-unwanted> <topics-the-broker-lists>
  local d="$1" src="$2"; mkdir -p "$d/state"; : > "$d/log"
  env -u TOPIC_SET PATH="$BIN:$PATH" VP_LOG="$d/log" VP_STATE="$d/state" VP_ABSENT= VP_DRIFT_TOPIC= VP_DRIFT_PARTS= \
    VP_LIST="$6" VP_DECLARED="$(declared_for "$src" "$3")" ENVIRONMENT="$3" \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_CLEANUP_TOPICS=true ALLOW_PROD_KAFKA_CLEANUP=true \
    KAFKA_CLEANUP_MODE="$4" KAFKA_DELETE_UNWANTED_TOPICS="$5" KAFKA_TOPIC_DELETE_WAIT_SECONDS=2 \
    "$BASH" "$src/cleanup-topics.sh" > "$d/out" 2>&1
  echo "$?" > "$d/rc"
}
exit_ok() { # <run-dir> <label> — a negative assertion is satisfied just as well by a crash on line 1
  [ "$(rc_of "$1")" = 0 ] && ok "$2: exits 0" || bad "$2: exited $(rc_of "$1"): $(tail -2 "$1/out" | tr '\n' ' ')"
}

# --- sections (each takes the directory holding apply-topics.sh, cleanup-topics.sh and topics.env first) ------
unit_create() { # <src> <env>
  local src="$1" env="$2" d t want got; d="$(newrun)"
  apply_run "$d" "$src" "$env" false "$ALL" "" ""
  exit_ok "$d" "$env"
  [ "$(n_calls "$d" ' --delete ')" = 0 ] && ok "$env: no delete issued" || bad "$env: a delete was issued"
  for t in $ALL; do
    want="partitions=1 rf=1 $(want_cfg "$env" "$t")"; got="$(calls create "$d/log" "$t")"
    [ "$got" = "$want" ] && ok "$env: $t created once: $want" || bad "$env: $t create was [${got:-<none>}], want [$want]"
    want="$(want_cfg "$env" "$t")"; got="$(calls alter "$d/log" "$t")"
    [ "$got" = "$want" ] && ok "$env: $t then reconciled once: $want" || bad "$env: $t reconcile was [${got:-<none>}], want [$want]"
  done
}

unit_reconcile() { # <src> <env>
  local src="$1" env="$2" d t want got; d="$(newrun)"
  apply_run "$d" "$src" "$env" false "" "" ""
  exit_ok "$d" "$env"
  [ "$(n_calls "$d" ' --create ')$(n_calls "$d" ' --delete ')" = 00 ] \
    && ok "$env: no create and no delete (every topic exists at its declared count)" \
    || bad "$env: $(n_calls "$d" ' --create ') create / $(n_calls "$d" ' --delete ') delete call(s)"
  for t in $ALL; do
    want="$(want_cfg "$env" "$t")"; got="$(calls effective "$d/log" "$t" "$(seed_cfg "$t")")"
    [ "$got" = "$want" ] && ok "$env: $t drifted [$(seed_cfg "$t")] -> [$want]" \
      || bad "$env: $t drifted [$(seed_cfg "$t")] ends at [$got], want [$want]"
  done
}

unit_drift() { # <src> <env> <topic> [recreate-flags, default "false true"]
  local src="$1" env="$2" t="$3" f d label want got
  for f in ${4:-false true}; do
    d="$(newrun)"; apply_run "$d" "$src" "$env" "$f" "" "$t" 4
    label="$env: $t at 4 partitions, KAFKA_RECREATE_MISMATCHED_TOPICS=$f"
    if is_durable "$t" || [ "$f" = false ]; then
      [ "$(rc_of "$d")" != 0 ] && ok "$label: refused (exit $(rc_of "$d"))" || bad "$label: exited 0"
      [ "$(n_calls "$d" ' --delete ')$(n_calls "$d" ' --create ')" = 00 ] && ok "$label: no delete and no create of ANY topic" \
        || bad "$label: $(n_calls "$d" ' --delete ') delete / $(n_calls "$d" ' --create ') create call(s)"
      if is_durable "$t"; then
        grep -qF "HARD ERROR: topic $t has partitions=4 but requires EXACTLY 1" "$d/out" \
          && grep -q OPTIONS_EDGE_NEVER_RECREATE_TOPICS "$d/out" \
          && ok "$label: the never-recreate HARD ERROR, naming $t" \
          || bad "$label: not the never-recreate hard error for $t: $(head -2 "$d/out" | tr '\n' ' ')"
      else
        grep -qF "Topic $t exists with partitions=4 but requires EXACTLY 1" "$d/out" \
          && ok "$label: stopped by the exact-partition contract, naming $t" \
          || bad "$label: not the exact-partition refusal for $t: $(tail -3 "$d/out" | tr '\n' ' ')"
      fi
    else
      exit_ok "$d" "$label"
      deleted "$d" "$t" && [ "$(n_calls "$d" ' --delete ')" = 1 ] && ok "$label: $t, and only $t, deleted" \
        || bad "$label: deletes were [$(grep -- ' --delete ' "$d/log" | sed 's/.*--topic //' | tr '\n' ' ')]"
      want="partitions=1 rf=1 $(want_cfg "$env" "$t")"; got="$(calls create "$d/log" "$t")"
      [ "$got" = "$want" ] && [ "$(n_calls "$d" ' --create ')" = 1 ] && ok "$label: recreated once: $want" \
        || bad "$label: recreate was [${got:-<none>}] ($(n_calls "$d" ' --create ') create call(s)), want [$want]"
      want="$(want_cfg "$env" "$t")"; got="$(calls alter "$d/log" "$t")"
      [ "$got" = "$want" ] && ok "$label: then reconciled: $want" || bad "$label: $t reconcile was [${got:-<none>}], want [$want]"
    fi
  done
}

unit_cleanup() { # <src> <env> [modes, default "sweep delete-recreate retention"]
  local src="$1" env="$2" m d t label
  for m in ${3:-sweep delete-recreate retention}; do
    d="$(newrun)"
    case "$m" in
      sweep) cleanup_run "$d" "$src" "$env" retention true "$ALL $JUNK"; label="$env unwanted sweep" ;;
      *)     cleanup_run "$d" "$src" "$env" "$m" false ""; label="$env $m" ;;
    esac
    exit_ok "$d" "$label"
    case "$m" in
      sweep)
        for t in $ALL; do
          if deleted "$d" "$t"; then bad "$label: $t DELETED as unwanted"; else ok "$label: $t kept"; fi
        done
        deleted "$d" "$JUNK" && ok "$label: the undeclared $JUNK IS deleted (the sweep ran)" \
          || bad "$label: the undeclared $JUNK survived: the sweep never ran, so the lines above prove nothing"
        ;;
      delete-recreate)
        for t in $DURABLE; do
          if deleted "$d" "$t"; then bad "$label: durable $t DELETED"
          elif grep -qxF "Keeping DURABLE approved topic (retention.ms=-1): $t" "$d/out"; then
            ok "$label: durable $t reached and spared by the reset-preserved guard"
          else bad "$label: $t not deleted, but the reset-preserved guard never named it (never reached?)"; fi
        done
        for t in $REBUILT; do
          deleted "$d" "$t" && ok "$label: $t IS deleted (approved, not preserved)" || bad "$label: $t was not deleted"
        done
        ;;
      retention)
        for t in $DURABLE; do
          if shrunk "$d" "$t"; then bad "$label: durable $t SHRUNK"
          elif grep -qxF "Keeping DURABLE approved topic (retention.ms=-1), not shrinking: $t" "$d/out"; then
            ok "$label: durable $t reached and spared by the reset-preserved guard"
          else bad "$label: $t not shrunk, but the reset-preserved guard never named it (never reached?)"; fi
        done
        for t in $REBUILT; do
          grep -qF -- "--entity-name $t --alter --add-config retention.ms=1000" "$d/log" \
            && ok "$label: $t IS shrunk to retention.ms=1000" || bad "$label: $t was not shrunk"
        done
        ;;
    esac
  done
}

mkcopy() { # <dir> — the three real scripts, byte for byte, beside a topics.env the caller writes
  mkdir -p "$1" && cp "$HERE/apply-topics.sh" "$HERE/cleanup-topics.sh" "$HERE/reset-preserved-topics.sh" "$1/"
}

unit_protected() { # <env> — the seven removed from EVERY *TOPICS* declaration in a copy; the regex alone keeps five
  local env="$1" m d t; m="$(mktemp -d "$WORK/protected.XXXXXX")"; mkcopy "$m"
  if ! mutate "$HERE/topics.env" "$m/topics.env" '[A-Z0-9_]*TOPICS[A-Z0-9_]*' decl $ALL > "$m/mutate.out" 2>&1; then
    bad "$env: could not build the undeclared copy: $(cat "$m/mutate.out")"; return
  fi
  # Isolation, checked on exactly the lists cleanup-topics.sh consults before deleting an "unwanted" topic: the
  # declared set (OPTIONS_EDGE_TOPICS, plus the prod-only set on production) and the reset-preserved keep-list.
  local v
  for t in $ALL; do
    for v in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_PROD_ONLY_TOPICS OPTIONS_EDGE_RESET_PRESERVED_TOPICS OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS; do
      if resolved "$m" "$v" | sed 's/[:=].*//' | grep -qxF "$t"; then
        bad "$env: $t is still in $v in the copy, so this case would not isolate the regex"; return
      fi
    done
  done
  d="$(newrun)"; cleanup_run "$d" "$m" "$env" retention true "$ALL $JUNK"
  exit_ok "$d" "$env undeclared sweep"
  for t in $DURABLE; do
    if deleted "$d" "$t"; then bad "$env: undeclared durable $t DELETED: PROTECTED_TOPIC_REGEX does not cover it"
    elif grep -qxF "Keeping protected topic: $t" "$d/out"; then ok "$env: undeclared durable $t kept by PROTECTED_TOPIC_REGEX"
    else bad "$env: $t not deleted, but not kept as protected either"; fi
  done
  for t in $REBUILT $JUNK; do
    deleted "$d" "$t" && ok "$env: undeclared $t IS swept (the regex protects only the five)" || bad "$env: undeclared $t was not swept"
  done
}

# mut_unit <VAR> <name|key> <topic> <section> <section args after src...>
# <topic> removed from <VAR> in a copy of topics.env; <section> run on that copy must report a FAIL naming <topic>.
mut_unit() {
  local var="$1" kind="$2" t="$3" m out n before after; shift 3
  m="$(mktemp -d "$WORK/mutant.XXXXXX")"; mkcopy "$m"
  if ! mutate "$HERE/topics.env" "$m/topics.env" "$var" "$kind" "$t" > "$m/mutate.out" 2>&1; then
    bad "mutant [$t out of $var] was not built ($(cat "$m/mutate.out")): the self-check would prove nothing"; return
  fi
  # The copy differs from topics.env in exactly one line, and — read the way apply/cleanup read it, by SOURCING —
  # <topic> was in <VAR> and is not any more. Otherwise a failure below could come from something else.
  [ "$(diff "$HERE/topics.env" "$m/topics.env" | grep -c '^[<>]')" = 2 ] && [ "$(cat "$m/mutate.out")" = "removed 1 token(s)" ] \
    || { bad "mutant [$t out of $var] is not a one-token, one-line change: $(cat "$m/mutate.out")"; return; }
  before="$(resolved "$HERE" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  after="$(resolved "$m" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  [ "$before" = 1 ] && [ "$after" = 0 ] \
    || { bad "mutant [$t out of $var]: resolved membership before=$before after=$after, want 1 then 0"; return; }
  local fn="$1"; shift
  out="$("$fn" "$m" "$@" 2>&1)"
  n="$(printf '%s\n' "$out" | grep '^  FAIL' | grep -cF "$t" || true)"
  if [ "$n" -gt 0 ]; then
    ok "mutant [$t out of $var]: $n assertion(s) on $t FAIL, e.g. $(printf '%s\n' "$out" | grep '^  FAIL' | grep -F "$t" | head -1 | sed 's/^  FAIL //')"
  else
    bad "mutant [$t out of $var] SURVIVED: '$fn $*' passed on the mutated copy"
  fi
}

# --- schedule: every unit in the background, output collected in launch order ------------------------------
UNITS=()
launch() { # <title> <section> <args...>
  local f; f="$WORK/unit.$(printf '%03d' "${#UNITS[@]}")"; UNITS+=("$f")
  while [ "$(jobs -rp | wc -l)" -ge "$MAXJ" ]; do wait -n; done
  { echo "$1"; "${@:2}"; } > "$f" 2>&1 &
}

for env in dev production; do
  launch "1. creation, ENVIRONMENT=$env: all seven absent" unit_create "$HERE" "$env"
done
for env in dev production; do
  launch "2. reconcile, ENVIRONMENT=$env: all seven exist with every declared config drifted" unit_reconcile "$HERE" "$env"
done
for env in dev production; do
  for t in $ALL; do
    launch "3. partition drift, ENVIRONMENT=$env: $t" unit_drift "$HERE" "$env" "$t"
  done
done
for env in dev production; do
  launch "4. cleanup-topics, ENVIRONMENT=$env (production with ALLOW_PROD_KAFKA_CLEANUP=true)" unit_cleanup "$HERE" "$env"
done
for env in dev production; do
  launch "5. PROTECTED_TOPIC_REGEX alone, ENVIRONMENT=$env: the seven undeclared everywhere else" unit_protected "$env"
done
for t in $DURABLE; do
  launch "6. self-check: $t removed from OPTIONS_EDGE_NEVER_RECREATE_TOPICS" \
    mut_unit OPTIONS_EDGE_NEVER_RECREATE_TOPICS name "$t" unit_drift production "$t" true
  launch "6. self-check: $t removed from OPTIONS_EDGE_RESET_PRESERVED_TOPICS" \
    mut_unit OPTIONS_EDGE_RESET_PRESERVED_TOPICS name "$t" unit_cleanup production delete-recreate
  launch "6. self-check: $t removed from OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES" \
    mut_unit OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES key "$t" unit_create dev
done
launch "6. self-check: $P.current removed from OPTIONS_EDGE_COMPACTED_TOPICS" \
  mut_unit OPTIONS_EDGE_COMPACTED_TOPICS name "$P.current" unit_create dev
launch "6. self-check: $P.current removed from OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS" \
  mut_unit OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS name "$P.current" unit_create production
wait

fails=0
for f in "${UNITS[@]}"; do
  cat "$f"
  # A unit that died before asserting anything (a missing function, a crashed subshell) must not read as a pass.
  grep -qE '^  (ok|FAIL) ' "$f" || { bad "the unit above produced no assertion at all"; fails=$((fails+1)); }
  fails=$((fails + $(grep -c '^  FAIL' "$f" || true)))
done
echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-vol-premium-safety: OK ==="; exit 0; fi
echo "=== apply-topics-vol-premium-safety: $fails problem(s) ===" >&2; exit 1
