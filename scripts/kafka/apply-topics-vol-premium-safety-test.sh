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
#                  section runs on that mutant AND on an unmutated copy (the control); the mutant counts as caught
#                  only by the differential rule in nested_verdict. The real file is never touched.
#   7. the harness can fail — every rule below is broken on purpose by a probe, and each probe must be refused by
#                  EXACTLY the rules it breaks; each control breaks none and must be accepted.
#
# HOW A RESULT IS PROVEN (Codex deploy rounds 2-4). A section is a "unit": a shell function run in the background.
#   - Assertions: ok and bad are the only emitters. Each writes exactly one line ("  ok   ..." / "  FAIL ...", a newline
#     in a message is flattened) to file descriptor 3, the unit's ASSERTION CHANNEL — never to stdout. The harness
#     counts the lines on that channel itself; nothing the unit prints on stdout/stderr is counted.
#   - Status: the unit's REAL exit status, observed by its parent (PIPESTATUS in run_captured, then `wait <pid>` in the
#     scheduler). No file carries it, so nothing the unit or a descendant writes can change it.
#   - Completeness: the scheduler DECLARES how many assertions each unit must make (declared_assertions, computed from
#     the unit's inputs — never from its output). A unit passes only with real status 0, EXACTLY that many assertions,
#     no line on its channel that is not an assertion, no stray stdout/stderr (a shell error inside the unit can make a
#     negative assertion pass), and no FAIL. A unit that returns early, aborts, skips or repeats fails.
#   - Finality: run_captured returns only after every process holding the unit's assertion channel — the unit and any
#     descendant it left running — has closed it, so the record it judges cannot change afterwards.
# What this does NOT claim: a unit is shell code in this file, run by this shell, so it can reach what this shell
# reaches — it could call ok for a check it never made, or write this harness's files by path. That an assertion
# checks what its message says is established by reading the unit, and by section 6 (removing each protection must
# turn the unit red). The harness proves that every unit RAN TO COMPLETION and made exactly its declared assertions.
#
# Every run is independent and writes only inside its own directory, so the runs execute in parallel (one per CPU).
set -uo pipefail
# The scripts this test drives need bash >= 4.4: apply-topics.sh uses mapfile (4.0) and expands a possibly-empty array
# under set -u (only safe from 4.4); cleanup-topics.sh uses declare -A (4.0). Under an older bash the scripts THEMSELVES
# fail (macOS /bin/bash 3.2 is refused at apply-topics.sh:201), so a result there would describe the shell, not the
# declarations. Refuse loudly instead: exit 2, never a pass.
if [ "${BASH_VERSINFO[0]}" -lt 4 ] || { [ "${BASH_VERSINFO[0]}" -eq 4 ] && [ "${BASH_VERSINFO[1]}" -lt 4 ]; }; then
  echo "=== apply-topics-vol-premium-safety: REFUSED — needs bash >= 4.4 (the scripts under test do), this is $BASH_VERSION ===" >&2
  exit 2
fi
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
P="options.spx.vol-premium"
DURABLE="$P.ivrv $P.events $P.warnings $P.baseline $P.calendar"
REBUILT="$P.current $P.dlq"
ALL="$DURABLE $REBUILT"
nw() { echo $#; }                 # the number of words in its (unquoted) arguments
N_ALL="$(nw $ALL)"
JUNK="vp-safety.undeclared-junk"   # declared nowhere: the unwanted sweep must delete it, or the sweep never ran
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
MAXJ="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
# The assertion channel is fd 3 (see HOW A RESULT IS PROVEN). One call, one line, whatever the message holds.
assert_line() { local m="${2//$'\n'/ }"; printf '%s %s\n' "$1" "${m//$'\r'/ }" >&3; }
ok()  { assert_line '  ok  ' "$1"; }
bad() { assert_line '  FAIL' "$1"; }
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
# The scripts under test run with the assertion channel CLOSED (3>&-): nothing they start can write to it or hold it.
apply_run() { # <run-dir> <src-dir> <env> <recreate-flag> <absent-topics> <drift-topic> <drift-partitions>
  local d="$1" src="$2"; mkdir -p "$d/state"; : > "$d/log"
  env -u KAFKA_COMPACTED_TOPIC_CLEANUP_POLICY -u TOPIC_SET PATH="$BIN:$PATH" \
    VP_LOG="$d/log" VP_STATE="$d/state" VP_ABSENT="$5" VP_DRIFT_TOPIC="$6" VP_DRIFT_PARTS="$7" \
    VP_DECLARED="$(declared_for "$src" "$3")" ENVIRONMENT="$3" KAFKA_RECREATE_MISMATCHED_TOPICS="$4" \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1 \
    KAFKA_TOPIC_CLEANUP_POLICY=delete KAFKA_TOPIC_RETENTION_MS=86400000 \
    KAFKA_TOPIC_DELETE_WAIT_SECONDS=2 KAFKA_TOPIC_REPAIR_WAIT_SECONDS=2 \
    "$BASH" "$src/apply-topics.sh" > "$d/out" 2>&1 3>&-
  echo "$?" > "$d/rc"
}
cleanup_run() { # <run-dir> <src-dir> <env> <retention|delete-recreate> <delete-unwanted> <topics-the-broker-lists>
  local d="$1" src="$2"; mkdir -p "$d/state"; : > "$d/log"
  env -u TOPIC_SET PATH="$BIN:$PATH" VP_LOG="$d/log" VP_STATE="$d/state" VP_ABSENT= VP_DRIFT_TOPIC= VP_DRIFT_PARTS= \
    VP_LIST="$6" VP_DECLARED="$(declared_for "$src" "$3")" ENVIRONMENT="$3" \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_CLEANUP_TOPICS=true ALLOW_PROD_KAFKA_CLEANUP=true \
    KAFKA_CLEANUP_MODE="$4" KAFKA_DELETE_UNWANTED_TOPICS="$5" KAFKA_TOPIC_DELETE_WAIT_SECONDS=2 \
    "$BASH" "$src/cleanup-topics.sh" > "$d/out" 2>&1 3>&-
  echo "$?" > "$d/rc"
}
exit_ok() { # <run-dir> <label> — a negative assertion is satisfied just as well by a crash on line 1
  [ "$(rc_of "$1")" = 0 ] && ok "$2: exits 0" || bad "$2: exited $(rc_of "$1"): $(tail -2 "$1/out" | tr '\n' ' ')"
}

# --- sections (each takes the directory holding apply-topics.sh, cleanup-topics.sh and topics.env first) ------
# Every unit makes a FIXED number of assertions on every path through it; declared_assertions (below the sections)
# states that number, and the collector refuses a unit that made any other number.
unit_create() { # <src> <env> — 2 + 2 per topic
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

unit_reconcile() { # <src> <env> — 2 + 1 per topic
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

unit_drift() { # <src> <env> <topic> [recreate-flags, default "false true"] — 4 per flag, on the refusal and the recreate path
  local src="$1" env="$2" t="$3" f d label want got
  for f in ${4:-false true}; do
    d="$(newrun)"; apply_run "$d" "$src" "$env" "$f" "" "$t" 4
    label="$env: $t at 4 partitions, KAFKA_RECREATE_MISMATCHED_TOPICS=$f"
    if is_durable "$t" || [ "$f" = false ]; then
      [ "$(rc_of "$d")" != 0 ] && ok "$label: refused (exit $(rc_of "$d"))" || bad "$label: exited 0"
      [ "$(n_calls "$d" ' --delete ')" = 0 ] && ok "$label: no delete of ANY topic" \
        || bad "$label: $(n_calls "$d" ' --delete ') delete call(s)"
      [ "$(n_calls "$d" ' --create ')" = 0 ] && ok "$label: no create of ANY topic" \
        || bad "$label: $(n_calls "$d" ' --create ') create call(s)"
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

unit_cleanup() { # <src> <env> [modes, default "sweep delete-recreate retention"] — sweep 2 + 1 per topic, others 1 + 1 per topic
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
  # 2 (the copy is built, and isolates the regex) + 1 (exit) + 1 per topic + 1 (the junk topic)
  local env="$1" m d t v leaks=""; m="$(mktemp -d "$WORK/protected.XXXXXX")"; mkcopy "$m"
  if mutate "$HERE/topics.env" "$m/topics.env" '[A-Z0-9_]*TOPICS[A-Z0-9_]*' decl $ALL > "$m/mutate.out" 2>&1; then
    ok "$env: undeclared copy built: the seven taken out of every *TOPICS* declaration ($(cat "$m/mutate.out"))"
  else
    bad "$env: could not build the undeclared copy: $(cat "$m/mutate.out")"; return
  fi
  # Isolation, checked on exactly the lists cleanup-topics.sh consults before deleting an "unwanted" topic: the
  # declared set (OPTIONS_EDGE_TOPICS, plus the prod-only set on production) and the reset-preserved keep-list.
  for t in $ALL; do
    for v in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_PROD_ONLY_TOPICS OPTIONS_EDGE_RESET_PRESERVED_TOPICS OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS; do
      if resolved "$m" "$v" | sed 's/[:=].*//' | grep -qxF "$t"; then leaks="$leaks $t@$v"; fi
    done
  done
  [ -z "$leaks" ] && ok "$env: in the copy none of the seven is in the declared set or the reset-preserved keep-list" \
    || { bad "$env: still declared in the copy, so this case would not isolate the regex:$leaks"; return; }
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

# mut_unit <VAR> <name|key> <topic> <section> <section args after src...> — 3 assertions
# <topic> removed from <VAR> in a MUTANT copy of topics.env; <section> runs on that mutant and on a CONTROL copy (the
# same three scripts, topics.env byte for byte), and judge_nested decides whether the mutant was caught.
mut_unit() {
  local var="$1" kind="$2" t="$3" c m before after; shift 3
  c="$(mktemp -d "$WORK/control.XXXXXX")"; mkcopy "$c"; cp "$HERE/topics.env" "$c/topics.env"
  m="$(mktemp -d "$WORK/mutant.XXXXXX")"; mkcopy "$m"
  if ! mutate "$c/topics.env" "$m/topics.env" "$var" "$kind" "$t" > "$m/mutate.out" 2>&1; then
    bad "mutant [$t out of $var] was not built ($(cat "$m/mutate.out")): the self-check would prove nothing"; return
  fi
  # The mutant differs from the control in exactly one line, and — read the way apply/cleanup read it, by SOURCING —
  # <topic> is in <VAR> in the control and not in the mutant. Otherwise a failure below could come from something else.
  if [ "$(diff "$c/topics.env" "$m/topics.env" | grep -c '^[<>]')" = 2 ] && [ "$(cat "$m/mutate.out")" = "removed 1 token(s)" ]; then
    ok "mutant [$t out of $var] built: one token removed, on one line, from a byte-for-byte copy of topics.env"
  else
    bad "mutant [$t out of $var] is not a one-token, one-line change: $(cat "$m/mutate.out")"; return
  fi
  before="$(resolved "$c" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  after="$(resolved "$m" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  [ "$before" = 1 ] && [ "$after" = 0 ] && ok "mutant [$t out of $var]: sourced, $t is in $var once in the control and not in the mutant" \
    || { bad "mutant [$t out of $var]: resolved membership control=$before mutant=$after, want 1 then 0"; return; }
  judge_nested "mutant [$t out of $var]" "$t" "$c" "$m" "$@"
}

# --- proving a run complete ---------------------------------------------------------------------------------
# run_captured <prefix> <unit> <args...>: runs <unit> in a subshell whose fd 3 (the assertion channel) is a PIPE into
# <prefix>.assert and whose stdout+stderr go to <prefix>.log. It returns that subshell's REAL exit status as its parent
# observed it (PIPESTATUS), and it returns only once the pipe's reader has seen end-of-file — i.e. after EVERY process
# holding the channel, the unit and any descendant it left running, has closed it. A failed write of the record
# returns 125, never 0.
run_captured() {
  local p="$1" s; shift
  { ( "$@" ) 3>&1 1>"$p.log" 2>&1; } | cat > "$p.assert"
  s=("${PIPESTATUS[@]}")
  [ "${s[1]}" = 0 ] || return 125
  return "${s[0]}"
}

# why_incomplete <prefix> <real status> <declared count>: prints one "<rule>: <reason>" line per completion rule the run
# broke; no output means the run is COMPLETE.
#   status  its real exit status is 0;
#   count   it made EXACTLY the declared number of assertions: a run that returns early, aborts or skips is short,
#           one that repeats is long;
#   record  every line on its assertion channel is an ok/bad line;
#   output  its own stdout/stderr is empty: an error inside the unit's shell code (a missing file, an unset variable,
#           a command not found) can make a NEGATIVE assertion pass, so it must not go unseen.
why_incomplete() {
  local p="$1" st="$2" want="$3" n x
  [ "$st" = 0 ] || echo "status: its real exit status is $st, not 0"
  n="$(grep -cE '^  (ok   |FAIL )' "$p.assert" 2>/dev/null || true)"
  [ "${n:-0}" = "$want" ] || echo "count: it made ${n:-0} assertion(s); the scheduler declared exactly $want"
  [ -f "$p.assert" ] || echo "record: no assertion record at all"
  x="$(grep -cvE '^  (ok   |FAIL )' "$p.assert" 2>/dev/null || true)"
  [ "${x:-0}" = 0 ] || echo "record: $x line(s) on its assertion channel are not assertions, e.g. $(grep -m1 -vE '^  (ok   |FAIL )' "$p.assert")"
  [ ! -s "$p.log" ] || echo "output: its own shell code wrote to stdout/stderr: $(head -c 300 "$p.log" | tr '\n' ' ')"
}
n_fail() { local n; n="$(grep -c '^  FAIL ' "$1.assert" 2>/dev/null || true)"; echo "${n:-0}"; }
rules_of() { local w; w="$(printf '%s\n' "$@" | sed -n 's/^\([a-z:-]*\): .*/\1/p' | sort -u | tr '\n' ' ')"; echo "${w% }"; }

# judge_unit <prefix> <real status> <declared count>: the collector's verdict on one unit. Prints its record, its stray
# output (as "  | " lines) and one FAIL per problem; sets JUDGED (the number of problems) and JUDGED_RULES (the rules
# that refused it, for section 7): its completion rules and "fail" (a FAIL assertion).
judge_unit() {
  local p="$1" why line nf; why="$(why_incomplete "$@")"; nf="$(n_fail "$p")"
  cat "$p.assert" 2>/dev/null
  [ ! -s "$p.log" ] || sed 's/^/  | /' "$p.log"
  JUDGED="$nf"; JUDGED_RULES=""
  [ "$nf" = 0 ] || JUDGED_RULES="fail: $nf FAIL assertion(s)"
  if [ -n "$why" ]; then
    while IFS= read -r line; do
      printf '  FAIL the unit above is not complete: %s\n' "$line"; JUDGED=$((JUDGED + 1)); JUDGED_RULES="$JUDGED_RULES"$'\n'"$line"
    done <<< "$why"
  fi
  JUDGED_RULES="$(rules_of "$JUDGED_RULES")"
}

# named_rx <topic>: an ERE matching <topic> as a whole name — not as the prefix of a longer one (.ivrv vs .ivrv-v2).
named_rx() { printf '(^|[^A-Za-z0-9._-])%s([^A-Za-z0-9._-]|$)' "$(rx "$1")"; }

# nested_verdict <topic> <control dir> <mutant dir> <declared count> <unit> <args after src...>: runs <unit> on the
# control and on the mutant, and sets NV_REASONS to one "<rule>: <reason>" per rule the pair broke. None = the mutant
# is PROVEN caught (the differential, Codex deploy round 4):
#   control:<rule>  the control run is complete (why_incomplete)   control-fail  the control has zero FAIL
#   mutant:<rule>   the mutant run is complete                     survived      the mutant has at least one FAIL
#   unrelated       EVERY FAIL of the mutant names the mutated topic
# The two runs execute the same scripts, the same unit and the same inputs, and their topics.env differ in the one
# token. With the control complete and clean, a FAIL in the mutant is one that token caused — not a missing command, a
# broken fixture, or a check that fails either way. A FAIL that does not name the topic is not evidence about that
# topic, so a mutant with one is refused too.
nested_verdict() {
  local t="$1" c="$2" m="$3" want="$4" pc pm st why line; shift 4
  NV_REASONS=(); NV_CAUGHT=0; NV_EXAMPLE=""
  pc="$(mktemp -d "$WORK/nested.XXXXXX")/control"; pm="${pc%/control}/mutant"
  run_captured "$pc" "$1" "$c" "${@:2}"; st=$?
  why="$(why_incomplete "$pc" "$st" "$want")"
  [ -z "$why" ] || while IFS= read -r line; do NV_REASONS+=("control:$line"); done <<< "$why"
  [ "$(n_fail "$pc")" = 0 ] || NV_REASONS+=("control-fail: the control run, on the UNMUTATED copy, has $(n_fail "$pc") FAIL, e.g. $(grep -m1 '^  FAIL ' "$pc.assert" | sed 's/^  FAIL //')")
  run_captured "$pm" "$1" "$m" "${@:2}"; st=$?
  why="$(why_incomplete "$pm" "$st" "$want")"
  [ -z "$why" ] || while IFS= read -r line; do NV_REASONS+=("mutant:$line"); done <<< "$why"
  NV_CAUGHT="$(n_fail "$pm")"
  [ "$NV_CAUGHT" -gt 0 ] || NV_REASONS+=("survived: no assertion FAILed on the mutant")
  line="$(grep '^  FAIL ' "$pm.assert" 2>/dev/null | grep -vE "$(named_rx "$t")" | head -1)"
  [ -z "$line" ] || NV_REASONS+=("unrelated: a FAIL of the mutant does not name $t: ${line#  FAIL }")
  NV_EXAMPLE="$(grep -m1 '^  FAIL ' "$pm.assert" 2>/dev/null | sed 's/^  FAIL //')"
}

# judge_nested <label> <topic> <control dir> <mutant dir> <unit> <args after src...>: ONE assertion — caught or not.
judge_nested() {
  local label="$1" t="$2" c="$3" m="$4" want; shift 4
  want="$(declared_assertions "$1" "$c" "${@:2}")"
  nested_verdict "$t" "$c" "$m" "$want" "$@"
  if [ "${#NV_REASONS[@]}" = 0 ]; then
    ok "$label: CAUGHT. '$*' completed on the control with $want/$want ok, and on the mutant with $NV_CAUGHT FAIL, every one naming $t, e.g. $NV_EXAMPLE"
  else
    bad "$label: mutant NOT proven caught by '$*': $(printf '%s; ' "${NV_REASONS[@]}")"
  fi
}

# --- 7. the harness checks itself -----------------------------------------------------------------------------
# Each probe breaks the rules named beside it and must be refused by EXACTLY those rules (so removing any one rule
# from the harness makes a probe here fail by name); "-" marks a control, which breaks none and must be accepted.
T7="$P.ivrv"
COLLECTOR_PROBES=( # <unit> <declared count> <rules that must refuse it>
  "probe_good                     2 -"
  "probe_completes_then_exits_99  2 status"
  "probe_completes_then_returns_3 2 status"
  "probe_forges_markers           2 output status"
  "probe_dies_after_first         2 count status"
  "probe_returns_0_after_first    2 count"
  "probe_exits_0_after_first      2 count"
  "probe_asserts_nothing          2 count"
  "probe_asserts_too_much         2 count"
  "probe_lookalike_on_stdout      2 count output"
  "probe_foreign_line_on_channel  2 record"
  "probe_shell_error              2 output"
  "probe_reports_a_fail           2 fail"
  "probe_descendant_fails_late    2 count fail"
)
probe_good()                     { ok "one"; ok "two"; }
probe_completes_then_exits_99()  { ok "one"; ok "two"; exit 99; }
probe_completes_then_returns_3() { ok "one"; ok "two"; return 3; }
probe_forges_markers() { # every completion marker a unit can reach, forged, then a real exit 99
  ok "one"; ok "two"
  printf '0\n' > "$p.rc"; printf '0\n' > "$p.status"   # the round-3 sidecar (p is run_captured's, visible here)
  echo "__unit_rc=0"                                    # the round-2 in-band marker
  ( sleep 0.3; printf '0\n' > "$p.rc" ) &              # round 4: a descendant rewriting it after the unit is gone
  exit 99
}
probe_dies_after_first()         { ok "one"; exit 99; ok "UNREACHED"; }
probe_returns_0_after_first()    { ok "one"; return 0; ok "UNREACHED"; }
probe_exits_0_after_first()      { ok "one"; exit 0; ok "UNREACHED"; }
probe_asserts_nothing()          { :; }
probe_asserts_too_much()         { ok "one"; ok "two"; ok "three"; }
probe_lookalike_on_stdout()      { ok "one"; printf '  ok   %s\n' "two, printed on stdout"; }
probe_foreign_line_on_channel()  { ok "one"; ok "two"; echo "__unit_rc=0" >&3; }
probe_shell_error()              { cat "$WORK/no-such-file-for-the-selfcheck"; ok "one"; ok "two"; }
probe_reports_a_fail()           { ok "one"; bad "two"; }
# Finality: a descendant the unit left running writes a FAIL after the unit exited 0 with its two ok. A collector that
# read the record when the unit's status arrived would judge it complete and clean; run_captured waits for the channel
# to close, so the FAIL is in the record that is judged.
probe_descendant_fails_late()    { ok "one"; ok "two"; ( sleep 0.3; bad "written by a descendant after the unit exited" ) & exit 0; }

NESTED_PROBES=( # <nested unit> <declared count> <rules that must refuse it>; it runs on <dir>/control and <dir>/mutant
  "nested_caught                         2 -"
  "nested_mutant_completes_then_exits_99 2 mutant:status"
  "nested_mutant_fails_then_aborts       2 mutant:count mutant:status"
  "nested_mutant_unset_abort             2 mutant:count mutant:output mutant:status"
  "nested_mutant_fails_then_exits_0      2 mutant:count"
  "nested_same_fail_in_control           2 control-fail"
  "nested_control_fails                  2 control-fail"
  "nested_control_aborts                 2 control:count control:status"
  "nested_unrelated_fail_in_mutant       2 unrelated"
  "nested_fail_names_a_longer_topic      2 unrelated"
  "nested_survivor                       2 survived"
)
side() { echo "${1##*/}"; }   # control | mutant
nested_caught()                         { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; ok "b"; }; }
nested_mutant_completes_then_exits_99() { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; bad "$T7 b"; exit 99; }; }
nested_mutant_fails_then_aborts()       { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; exit 99; }; }
nested_mutant_unset_abort()             { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; : "$NO_SUCH_VARIABLE_FOR_THE_SELFCHECK"; ok "b"; }; }
nested_mutant_fails_then_exits_0()      { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; exit 0; ok "b"; }; }
# Codex r4's reproduction: a failure that has nothing to do with the mutation (a missing command) fails an assertion
# whose message names the topic — on the control exactly as on the mutant.
nested_same_fail_in_control()           { ok "a"; bad "$T7: not the never-recreate hard error: apply-topics.sh: dependency: command not found"; }
nested_control_fails()                  { [ "$(side "$1")" = control ] && { bad "an unrelated check"; ok "b"; } || { bad "$T7 a"; ok "b"; }; }
nested_control_aborts()                 { [ "$(side "$1")" = control ] && { ok "a"; exit 99; } || { bad "$T7 a"; ok "b"; }; }
nested_unrelated_fail_in_mutant()       { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7 a"; bad "$P.events b"; }; }
nested_fail_names_a_longer_topic()      { [ "$(side "$1")" = control ] && { ok "a"; ok "b"; } || { bad "$T7-v2 a"; ok "b"; }; }
nested_survivor()                       { ok "a"; ok "b"; }

unit_collector_selfcheck() { # one assertion per probe
  local d="$WORK/selfcheck" spec fn want rules st got; mkdir -p "$d"
  for spec in "${COLLECTOR_PROBES[@]}"; do
    read -r fn want rules <<< "$spec"; [ "$rules" = - ] && rules=""
    run_captured "$d/$fn" "$fn" & wait "$!"; st=$?           # the scheduler's path: background, then wait <pid>
    judge_unit "$d/$fn" "$st" "$want" > "$d/$fn.judged"
    got="$JUDGED_RULES"
    if [ "$got" = "$rules" ]; then
      if [ -z "$rules" ]; then ok "collector ACCEPTS control $fn: status 0, $want/$want assertions, no FAIL, no stray output"
      else ok "collector REFUSES $fn by exactly [$rules]"; fi
    else
      bad "collector judged $fn by [${got:-nothing: ACCEPTED}], want [${rules:-nothing: accepted}]: $(grep '^  FAIL the unit' "$d/$fn.judged" | tr '\n' ' ')"
    fi
  done
  for spec in "${NESTED_PROBES[@]}"; do
    read -r fn want rules <<< "$spec"; [ "$rules" = - ] && rules=""
    mkdir -p "$d/$fn/control" "$d/$fn/mutant"
    nested_verdict "$T7" "$d/$fn/control" "$d/$fn/mutant" "$want" "$fn"
    got="$(rules_of "${NV_REASONS[@]}")"
    if [ "$got" = "$rules" ]; then
      if [ -z "$rules" ]; then ok "nested judge ACCEPTS control $fn: control clean and complete, every mutant FAIL names $T7"
      else ok "nested judge REFUSES $fn by exactly [$rules]"; fi
    else
      bad "nested judge judged $fn by [${got:-nothing: ACCEPTED as caught}], want [${rules:-nothing: caught}]: $(printf '%s; ' "${NV_REASONS[@]}")"
    fi
  done
}

# --- what each unit must assert: declared by the scheduler, from the unit's inputs alone -------------------------
declared_assertions() { # <unit> <args...> exactly as the unit is called
  local n=0 m
  case "$1" in
    unit_create)    echo $(( 2 + 2 * N_ALL )) ;;
    unit_reconcile) echo $(( 2 + N_ALL )) ;;
    unit_drift)     echo $(( 4 * $(nw ${5:-false true}) )) ;;
    unit_cleanup)   for m in ${4:-sweep delete-recreate retention}; do
                      case "$m" in sweep) n=$(( n + 2 + N_ALL )) ;; *) n=$(( n + 1 + N_ALL )) ;; esac
                    done; echo "$n" ;;
    unit_protected) echo $(( 2 + 1 + N_ALL + 1 )) ;;
    mut_unit)       echo 3 ;;
    unit_collector_selfcheck) echo $(( ${#COLLECTOR_PROBES[@]} + ${#NESTED_PROBES[@]} )) ;;
    *)              echo "declared_assertions: no count declared for $1" >&2; echo -1 ;;
  esac
}

# --- schedule: every unit in the background; its real status by `wait <pid>`; output collected in launch order ---
UNITS=(); TITLES=(); DECLARED=(); PIDS=(); STATUS=()
reap() { # records, with `wait <pid>`, the real exit status of every launched unit that is no longer running
  local i run; run=" $(jobs -rp | tr '\n' ' ') "
  for i in "${!PIDS[@]}"; do
    [ -z "${STATUS[$i]+set}" ] || continue
    case "$run" in *" ${PIDS[$i]} "*) continue ;; esac
    wait "${PIDS[$i]}"; STATUS[$i]=$?
  done
}
launch() { # <title> <unit> <args...>
  local f; f="$WORK/unit.$(printf '%03d' "${#UNITS[@]}")"
  UNITS+=("$f"); TITLES+=("$1"); shift; DECLARED+=("$(declared_assertions "$@")")
  while [ "$(jobs -rp | wc -l)" -ge "$MAXJ" ]; do sleep 0.1; done   # a poll, no bash-version-specific wait flag
  reap
  run_captured "$f" "$@" &
  PIDS+=("$!")
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
launch "7. harness self-check: each probe refused by exactly the rules it breaks, each control accepted" \
  unit_collector_selfcheck

for i in "${!UNITS[@]}"; do
  [ -n "${STATUS[$i]+set}" ] || { wait "${PIDS[$i]}"; STATUS[$i]=$?; }
done
PROBLEMS=0
for i in "${!UNITS[@]}"; do
  echo "${TITLES[$i]}"
  judge_unit "${UNITS[$i]}" "${STATUS[$i]}" "${DECLARED[$i]}"
  PROBLEMS=$(( PROBLEMS + JUDGED ))
done
echo
if [ "$PROBLEMS" -eq 0 ]; then echo "=== apply-topics-vol-premium-safety: OK (${#UNITS[@]} units, each complete) ==="; exit 0; fi
echo "=== apply-topics-vol-premium-safety: $PROBLEMS problem(s) ===" >&2; exit 1
