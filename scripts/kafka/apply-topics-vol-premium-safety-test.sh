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
#                  only by the differential rule in nested_verdict, which includes the mutation's declared EFFECT on
#                  the recorded broker calls. The real file is never touched.
#   7. the harness can fail — every rule below is broken on purpose by a probe, and each probe must be refused by
#                  EXACTLY the rules it breaks; each control breaks none and must be accepted.
#
# HOW A RESULT IS PROVEN (Codex deploy rounds 2-5). A section is a "unit": a shell function run in the background.
#   - Assertions: ok and bad are the only emitters. Each is called with an assertion ID (one word, naming WHICH check
#     this is: the ok and the bad of one check share it) and writes exactly one line ("  ok   [<id>] ..." /
#     "  FAIL [<id>] ...", a newline in a message is flattened) to file descriptor 3, the unit's ASSERTION CHANNEL —
#     never to stdout. The harness reads that channel itself; nothing the unit prints on stdout/stderr is counted.
#   - Status: the unit's REAL exit status, observed by its parent (PIPESTATUS in run_captured, then `wait <pid>` in the
#     scheduler). No file carries it, so nothing the unit or a descendant writes can change it.
#   - Completeness: the scheduler DECLARES each unit's assertion INVENTORY — the ordered list of assertion IDs it must
#     make, repeats included (declared_inventory, computed from the unit's inputs — never from its output). A unit
#     passes only with real status 0, an assertion sequence whose IDs equal that inventory exactly (same IDs, same
#     order, same multiplicity), no line on its channel that is not a well-formed assertion, no stray stdout/stderr
#     (a shell error inside the unit can make a negative assertion pass), every script run it recorded (apply_run /
#     cleanup_run, in its own newrun directory) ending with one of that script's own statuses (0 or 1) and no output line
#     matching a shell-level diagnostic (SHELL_DIAG_RX), and no FAIL. So a unit that returns early, aborts, skips a check, makes an extra one, or repeats one check in place of
#     another, fails.
#   - Finality: run_captured returns only after every process holding the unit's assertion channel — the unit and any
#     descendant it left running — has closed it, so the record it judges cannot change afterwards.
# What this does NOT claim: a unit is shell code in this file, run by this shell, so it can reach what this shell
# reaches — it could call ok with an ID for a check it never made, give two different checks one ID, run a script
# outside newrun (so the exec and effect rules never see it), or write this harness's files by path. That the check behind an ID is what its message says is established by reading the unit,
# and by section 6 (removing each protection must turn the unit red AND change the recorded broker calls). The harness
# proves that every unit RAN TO COMPLETION and made exactly its declared assertion IDs, in order.
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
# ok|bad <id> <message>. An ID with a space or a bracket makes a malformed line, which the record rule refuses.
assert_line() { local m="${3//$'\n'/ }"; printf '%s [%s] %s\n' "$1" "$2" "${m//$'\r'/ }" >&3; }
ok()  { assert_line '  ok  ' "$1" "$2"; }
bad() { assert_line '  FAIL' "$1" "$2"; }
ASSERT_RX='^  (ok   |FAIL )\[[^] []+\]( |$)'   # a well-formed assertion line
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
# Every script run lives in its own run.XXXXXX directory (log = the broker calls, out = the script's output, rc = its
# exit status) under RUN_ROOT, which run_captured sets to the unit's own <prefix>.runs — so the collector can inspect
# every script execution a unit made (the exec rule) and nested_verdict every one a mutant made (the effect rule).
newrun() { mkdir -p "${RUN_ROOT:-$WORK}" && mktemp -d "${RUN_ROOT:-$WORK}/run.XXXXXX"; }
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
exit_ok() { # <id> <run-dir> <label> — a negative assertion is satisfied just as well by a crash on line 1
  [ "$(rc_of "$2")" = 0 ] && ok "$1" "$3: exits 0" || bad "$1" "$3: exited $(rc_of "$2"): $(tail -2 "$2/out" | tr '\n' ' ')"
}

# --- sections (each takes the directory holding apply-topics.sh, cleanup-topics.sh and topics.env first) ------
# Every unit makes a FIXED sequence of assertion IDs on every path through it; declared_inventory (below the sections)
# states that sequence, and the collector refuses a unit that made any other.
unit_create() { # <src> <env> — exit, no-delete, then per topic: create:<t>, reconcile:<t>
  local src="$1" env="$2" d t want got; d="$(newrun)"
  apply_run "$d" "$src" "$env" false "$ALL" "" ""
  exit_ok exit "$d" "$env"
  [ "$(n_calls "$d" ' --delete ')" = 0 ] && ok no-delete "$env: no delete issued" || bad no-delete "$env: a delete was issued"
  for t in $ALL; do
    want="partitions=1 rf=1 $(want_cfg "$env" "$t")"; got="$(calls create "$d/log" "$t")"
    [ "$got" = "$want" ] && ok "create:$t" "$env: $t created once: $want" || bad "create:$t" "$env: $t create was [${got:-<none>}], want [$want]"
    want="$(want_cfg "$env" "$t")"; got="$(calls alter "$d/log" "$t")"
    [ "$got" = "$want" ] && ok "reconcile:$t" "$env: $t then reconciled once: $want" || bad "reconcile:$t" "$env: $t reconcile was [${got:-<none>}], want [$want]"
  done
}

unit_reconcile() { # <src> <env> — exit, no-create-delete, then per topic: drifted:<t>
  local src="$1" env="$2" d t want got; d="$(newrun)"
  apply_run "$d" "$src" "$env" false "" "" ""
  exit_ok exit "$d" "$env"
  [ "$(n_calls "$d" ' --create ')$(n_calls "$d" ' --delete ')" = 00 ] \
    && ok no-create-delete "$env: no create and no delete (every topic exists at its declared count)" \
    || bad no-create-delete "$env: $(n_calls "$d" ' --create ') create / $(n_calls "$d" ' --delete ') delete call(s)"
  for t in $ALL; do
    want="$(want_cfg "$env" "$t")"; got="$(calls effective "$d/log" "$t" "$(seed_cfg "$t")")"
    [ "$got" = "$want" ] && ok "drifted:$t" "$env: $t drifted [$(seed_cfg "$t")] -> [$want]" \
      || bad "drifted:$t" "$env: $t drifted [$(seed_cfg "$t")] ends at [$got], want [$want]"
  done
}

unit_drift() { # <src> <env> <topic> [recreate-flags, default "false true"] — 4 per flag, IDs <flag>:<check>, on either path
  local src="$1" env="$2" t="$3" f d label want got
  for f in ${4:-false true}; do
    d="$(newrun)"; apply_run "$d" "$src" "$env" "$f" "" "$t" 4
    label="$env: $t at 4 partitions, KAFKA_RECREATE_MISMATCHED_TOPICS=$f"
    if is_durable "$t" || [ "$f" = false ]; then
      [ "$(rc_of "$d")" != 0 ] && ok "$f:refused" "$label: refused (exit $(rc_of "$d"))" || bad "$f:refused" "$label: exited 0"
      [ "$(n_calls "$d" ' --delete ')" = 0 ] && ok "$f:no-delete" "$label: no delete of ANY topic" \
        || bad "$f:no-delete" "$label: $(n_calls "$d" ' --delete ') delete call(s)"
      [ "$(n_calls "$d" ' --create ')" = 0 ] && ok "$f:no-create" "$label: no create of ANY topic" \
        || bad "$f:no-create" "$label: $(n_calls "$d" ' --create ') create call(s)"
      if is_durable "$t"; then
        grep -qF "HARD ERROR: topic $t has partitions=4 but requires EXACTLY 1" "$d/out" \
          && grep -q OPTIONS_EDGE_NEVER_RECREATE_TOPICS "$d/out" \
          && ok "$f:never-recreate-error" "$label: the never-recreate HARD ERROR, naming $t" \
          || bad "$f:never-recreate-error" "$label: not the never-recreate hard error for $t: $(head -2 "$d/out" | tr '\n' ' ')"
      else
        grep -qF "Topic $t exists with partitions=4 but requires EXACTLY 1" "$d/out" \
          && ok "$f:exact-partition-refusal" "$label: stopped by the exact-partition contract, naming $t" \
          || bad "$f:exact-partition-refusal" "$label: not the exact-partition refusal for $t: $(tail -3 "$d/out" | tr '\n' ' ')"
      fi
    else
      exit_ok "$f:exit" "$d" "$label"
      deleted "$d" "$t" && [ "$(n_calls "$d" ' --delete ')" = 1 ] && ok "$f:deleted-only-it" "$label: $t, and only $t, deleted" \
        || bad "$f:deleted-only-it" "$label: deletes were [$(grep -- ' --delete ' "$d/log" | sed 's/.*--topic //' | tr '\n' ' ')]"
      want="partitions=1 rf=1 $(want_cfg "$env" "$t")"; got="$(calls create "$d/log" "$t")"
      [ "$got" = "$want" ] && [ "$(n_calls "$d" ' --create ')" = 1 ] && ok "$f:recreated" "$label: recreated once: $want" \
        || bad "$f:recreated" "$label: recreate was [${got:-<none>}] ($(n_calls "$d" ' --create ') create call(s)), want [$want]"
      want="$(want_cfg "$env" "$t")"; got="$(calls alter "$d/log" "$t")"
      [ "$got" = "$want" ] && ok "$f:reconciled" "$label: then reconciled: $want" || bad "$f:reconciled" "$label: $t reconcile was [${got:-<none>}], want [$want]"
    fi
  done
}

unit_cleanup() { # <src> <env> [modes, default "sweep delete-recreate retention"] — IDs <mode>:<check>[:<topic>]
  local src="$1" env="$2" m d t label
  for m in ${3:-sweep delete-recreate retention}; do
    d="$(newrun)"
    case "$m" in
      sweep) cleanup_run "$d" "$src" "$env" retention true "$ALL $JUNK"; label="$env unwanted sweep" ;;
      *)     cleanup_run "$d" "$src" "$env" "$m" false ""; label="$env $m" ;;
    esac
    exit_ok "$m:exit" "$d" "$label"
    case "$m" in
      sweep)
        for t in $ALL; do
          if deleted "$d" "$t"; then bad "sweep:kept:$t" "$label: $t DELETED as unwanted"; else ok "sweep:kept:$t" "$label: $t kept"; fi
        done
        deleted "$d" "$JUNK" && ok sweep:junk-deleted "$label: the undeclared $JUNK IS deleted (the sweep ran)" \
          || bad sweep:junk-deleted "$label: the undeclared $JUNK survived: the sweep never ran, so the lines above prove nothing"
        ;;
      delete-recreate)
        for t in $DURABLE; do
          if deleted "$d" "$t"; then bad "delete-recreate:spared:$t" "$label: durable $t DELETED"
          elif grep -qxF "Keeping DURABLE approved topic (retention.ms=-1): $t" "$d/out"; then
            ok "delete-recreate:spared:$t" "$label: durable $t reached and spared by the reset-preserved guard"
          else bad "delete-recreate:spared:$t" "$label: $t not deleted, but the reset-preserved guard never named it (never reached?)"; fi
        done
        for t in $REBUILT; do
          deleted "$d" "$t" && ok "delete-recreate:deleted:$t" "$label: $t IS deleted (approved, not preserved)" \
            || bad "delete-recreate:deleted:$t" "$label: $t was not deleted"
        done
        ;;
      retention)
        for t in $DURABLE; do
          if shrunk "$d" "$t"; then bad "retention:spared:$t" "$label: durable $t SHRUNK"
          elif grep -qxF "Keeping DURABLE approved topic (retention.ms=-1), not shrinking: $t" "$d/out"; then
            ok "retention:spared:$t" "$label: durable $t reached and spared by the reset-preserved guard"
          else bad "retention:spared:$t" "$label: $t not shrunk, but the reset-preserved guard never named it (never reached?)"; fi
        done
        for t in $REBUILT; do
          grep -qF -- "--entity-name $t --alter --add-config retention.ms=1000" "$d/log" \
            && ok "retention:shrunk:$t" "$label: $t IS shrunk to retention.ms=1000" || bad "retention:shrunk:$t" "$label: $t was not shrunk"
        done
        ;;
    esac
  done
}

mkcopy() { # <dir> — the three real scripts, byte for byte, beside a topics.env the caller writes
  mkdir -p "$1" && cp "$HERE/apply-topics.sh" "$HERE/cleanup-topics.sh" "$HERE/reset-preserved-topics.sh" "$1/"
}

unit_protected() { # <env> — the seven removed from EVERY *TOPICS* declaration in a copy; the regex alone keeps five
  # copy-built, isolated, exit, kept:<t> per durable topic, swept:<t> per rebuildable topic and the junk topic
  local env="$1" m d t v leaks=""; m="$(mktemp -d "$WORK/protected.XXXXXX")"; mkcopy "$m"
  if mutate "$HERE/topics.env" "$m/topics.env" '[A-Z0-9_]*TOPICS[A-Z0-9_]*' decl $ALL > "$m/mutate.out" 2>&1; then
    ok copy-built "$env: undeclared copy built: the seven taken out of every *TOPICS* declaration ($(cat "$m/mutate.out"))"
  else
    bad copy-built "$env: could not build the undeclared copy: $(cat "$m/mutate.out")"; return
  fi
  # Isolation, checked on exactly the lists cleanup-topics.sh consults before deleting an "unwanted" topic: the
  # declared set (OPTIONS_EDGE_TOPICS, plus the prod-only set on production) and the reset-preserved keep-list.
  for t in $ALL; do
    for v in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_PROD_ONLY_TOPICS OPTIONS_EDGE_RESET_PRESERVED_TOPICS OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS; do
      if resolved "$m" "$v" | sed 's/[:=].*//' | grep -qxF "$t"; then leaks="$leaks $t@$v"; fi
    done
  done
  [ -z "$leaks" ] && ok isolated "$env: in the copy none of the seven is in the declared set or the reset-preserved keep-list" \
    || { bad isolated "$env: still declared in the copy, so this case would not isolate the regex:$leaks"; return; }
  d="$(newrun)"; cleanup_run "$d" "$m" "$env" retention true "$ALL $JUNK"
  exit_ok exit "$d" "$env undeclared sweep"
  for t in $DURABLE; do
    if deleted "$d" "$t"; then bad "kept:$t" "$env: undeclared durable $t DELETED: PROTECTED_TOPIC_REGEX does not cover it"
    elif grep -qxF "Keeping protected topic: $t" "$d/out"; then ok "kept:$t" "$env: undeclared durable $t kept by PROTECTED_TOPIC_REGEX"
    else bad "kept:$t" "$env: $t not deleted, but not kept as protected either"; fi
  done
  for t in $REBUILT $JUNK; do
    deleted "$d" "$t" && ok "swept:$t" "$env: undeclared $t IS swept (the regex protects only the five)" || bad "swept:$t" "$env: undeclared $t was not swept"
  done
}

# mut_unit <VAR> <name|key> <topic> <effect> <section> <section args after src...> — IDs: built, membership, caught
# <topic> removed from <VAR> in a MUTANT copy of topics.env; <section> runs on that mutant and on a CONTROL copy (the
# same three scripts, topics.env byte for byte), and judge_nested decides whether the mutant was caught. <effect> is
# what the removal must observably DO to <topic> in the broker calls the mutant's scripts made (effect_shown).
mut_unit() {
  local var="$1" kind="$2" t="$3" eff="$4" c m before after; shift 4
  c="$(mktemp -d "$WORK/control.XXXXXX")"; mkcopy "$c"; cp "$HERE/topics.env" "$c/topics.env"
  m="$(mktemp -d "$WORK/mutant.XXXXXX")"; mkcopy "$m"
  if ! mutate "$c/topics.env" "$m/topics.env" "$var" "$kind" "$t" > "$m/mutate.out" 2>&1; then
    bad built "mutant [$t out of $var] was not built ($(cat "$m/mutate.out")): the self-check would prove nothing"; return
  fi
  # The mutant differs from the control in exactly one line, and — read the way apply/cleanup read it, by SOURCING —
  # <topic> is in <VAR> in the control and not in the mutant. Otherwise a failure below could come from something else.
  if [ "$(diff "$c/topics.env" "$m/topics.env" | grep -c '^[<>]')" = 2 ] && [ "$(cat "$m/mutate.out")" = "removed 1 token(s)" ]; then
    ok built "mutant [$t out of $var] built: one token removed, on one line, from a byte-for-byte copy of topics.env"
  else
    bad built "mutant [$t out of $var] is not a one-token, one-line change: $(cat "$m/mutate.out")"; return
  fi
  before="$(resolved "$c" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  after="$(resolved "$m" "$var" | sed 's/=.*//' | grep -cxF "$t")"
  [ "$before" = 1 ] && [ "$after" = 0 ] && ok membership "mutant [$t out of $var]: sourced, $t is in $var once in the control and not in the mutant" \
    || { bad membership "mutant [$t out of $var]: resolved membership control=$before mutant=$after, want 1 then 0"; return; }
  judge_nested "mutant [$t out of $var]" "$t" "$eff" "$c" "$m" "$@"
}

# --- proving a run complete ---------------------------------------------------------------------------------
# run_captured <prefix> <unit> <args...>: runs <unit> in a subshell whose fd 3 (the assertion channel) is a PIPE into
# <prefix>.assert and whose stdout+stderr go to <prefix>.log. It returns that subshell's REAL exit status as its parent
# observed it (PIPESTATUS), and it returns only once the pipe's reader has seen end-of-file — i.e. after EVERY process
# holding the channel, the unit and any descendant it left running, has closed it. A failed write of the record
# returns 125, never 0. Inside, RUN_ROOT is <prefix>.runs: every script run the unit makes (newrun) is recorded there.
run_captured() {
  local p="$1" s; shift
  { ( RUN_ROOT="$p.runs"; "$@" ) 3>&1 1>"$p.log" 2>&1; } | cat > "$p.assert"
  s=("${PIPESTATUS[@]}")
  [ "${s[1]}" = 0 ] || return 125
  return "${s[0]}"
}

# exec_failures <runs-root>: one line per recorded script run (apply-topics.sh / cleanup-topics.sh) that did not end the
# way the script itself ends. Both scripts leave only through `exit 1` or completion (exit 0); under their set -e a
# failing command such as a missing one ends them with that command's status instead (127 command not found, 126 not
# executable, >128 a signal). So a run is an EXECUTION failure when its status is not 0 or 1, when no status was
# recorded, or when its output holds a shell-level diagnostic (the diagnostic catches a failure that happened to exit 1).
SHELL_DIAG_RX=': line [0-9]+: |command not found|unbound variable|syntax error|No such file or directory|Permission denied|Traceback \(most recent call last\)'
exec_failures() {
  local r s
  for r in "$1"/run.*; do
    [ -d "$r" ] || continue
    s="$(cat "$r/rc" 2>/dev/null)"
    case "$s" in
      0|1) ;;
      "") echo "${r##*/}: no exit status recorded (the script never finished)"; continue ;;
      *)  echo "${r##*/}: the script exited $s, not one of its own statuses (0, 1): $(tail -1 "$r/out" 2>/dev/null)"; continue ;;
    esac
    grep -m1 -E "$SHELL_DIAG_RX" "$r/out" 2>/dev/null | sed "s|^|${r##*/}: exited $s with a shell-level diagnostic: |"
  done
}

# assertion_ids <prefix>: the IDs of the well-formed assertions on the channel, in order, one per line.
assertion_ids() { sed -nE 's/^  (ok   |FAIL )\[([^] []+)\]( .*)?$/\2/p' "$1.assert" 2>/dev/null; }

# why_incomplete <prefix> <real status> <declared inventory>: prints one "<rule>: <reason>" line per completion rule the
# run broke; no output means the run is COMPLETE. <declared inventory> is the space-separated, ordered list of assertion
# IDs the scheduler declared (declared_inventory).
#   status     its real exit status is 0;
#   count      it made as many well-formed assertions as the inventory has entries: a run that returns early, aborts or
#              skips is short, one that repeats or adds is long;
#   inventory  with the count right, its assertion IDs equal the inventory entry by entry — the same IDs, in the same
#              order, with the same multiplicity: a repeated check standing in for an omitted one is refused here;
#   record     every line on its assertion channel is a well-formed ok/bad line ("  ok   [<id>] ..." / "  FAIL [<id>] ...");
#   output     its own stdout/stderr is empty: an error inside the unit's shell code (a missing file, an unset variable,
#              a command not found) can make a NEGATIVE assertion pass, so it must not go unseen;
#   exec       every script run it recorded ended with one of the script's own statuses and no shell-level diagnostic
#              (exec_failures).
why_incomplete() {
  local p="$1" st="$2" inv="$3" n x i got exp
  [ "$st" = 0 ] || echo "status: its real exit status is $st, not 0"
  read -ra exp <<< "$inv"
  mapfile -t got < <(assertion_ids "$p")
  n="${#got[@]}"
  if [ "$n" != "${#exp[@]}" ]; then
    echo "count: it made $n assertion(s); the scheduler declared exactly ${#exp[@]}"
  else
    for i in "${!exp[@]}"; do
      [ "${got[$i]}" = "${exp[$i]}" ] && continue
      echo "inventory: assertion $((i + 1)) of $n is [${got[$i]}], the scheduler declared [${exp[$i]}] (made: ${got[*]})"; break
    done
  fi
  [ -f "$p.assert" ] || echo "record: no assertion record at all"
  x="$(grep -cvE "$ASSERT_RX" "$p.assert" 2>/dev/null || true)"
  [ "${x:-0}" = 0 ] || echo "record: $x line(s) on its assertion channel are not well-formed assertions, e.g. $(grep -m1 -vE "$ASSERT_RX" "$p.assert")"
  [ ! -s "$p.log" ] || echo "output: its own shell code wrote to stdout/stderr: $(head -c 300 "$p.log" | tr '\n' ' ')"
  x="$(exec_failures "$p.runs")"
  [ -z "$x" ] || echo "exec: $(printf '%s\n' "$x" | wc -l | tr -d ' ') script run(s) failed to execute, e.g. $(printf '%s\n' "$x" | head -1)"
}
n_fail() { local n; n="$(grep -c '^  FAIL ' "$1.assert" 2>/dev/null || true)"; echo "${n:-0}"; }
fail_messages() { grep '^  FAIL ' "$1.assert" 2>/dev/null | sed -E 's/^  FAIL (\[[^] []+\] ?)?//'; }   # the ID stripped
rules_of() { local w; w="$(printf '%s\n' "$@" | sed -n 's/^\([a-z:-]*\): .*/\1/p' | sort -u | tr '\n' ' ')"; echo "${w% }"; }

# judge_unit <prefix> <real status> <declared inventory>: the collector's verdict on one unit. Prints its record, its stray
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

# effect_shown <runs-root> <topic> <effect>: prints the evidence and returns 0 iff some script run recorded under
# <runs-root> shows <effect> on <topic> in its broker-call log; returns 1 if none does, 2 for an undeclared effect.
#   recreated                 an apply run that exited 0 deleted <topic> and created it again
#   deleted                   a run deleted <topic>
#   created-with:<key=value>  a run created <topic> with exactly that config pair among its --config flags
#   created-without:<k=v>     a run created <topic> WITHOUT that config pair
effect_shown() {
  local r got
  for r in "$1"/run.*; do
    [ -f "$r/log" ] || continue
    case "$3" in
      recreated)
        [ "$(cat "$r/rc" 2>/dev/null)" = 0 ] && deleted "$r" "$2" && got="$(calls create "$r/log" "$2")" && [ -n "$got" ] \
          && { echo "${r##*/}: exit 0, $2 deleted and created again [$got]"; return 0; } ;;
      deleted)
        deleted "$r" "$2" && { echo "${r##*/}: $2 deleted"; return 0; } ;;
      created-with:?*|created-without:?*)
        got="$(calls create "$r/log" "$2")"; [ -n "$got" ] || continue
        case "$3" in
          created-with:*)    case " $got " in *" ${3#created-with:} "*) echo "${r##*/}: $2 created [$got]"; return 0 ;; esac ;;
          created-without:*) case " $got " in *" ${3#created-without:} "*) ;; *) echo "${r##*/}: $2 created [$got]"; return 0 ;; esac ;;
        esac ;;
      *) echo "undeclared effect [$3]"; return 2 ;;
    esac
  done
  return 1
}

# nested_verdict <topic> <effect> <control dir> <mutant dir> <declared inventory> <unit> <args after src...>: runs <unit>
# on the control and on the mutant, and sets NV_REASONS to one "<rule>: <reason>" per rule the pair broke. None = the
# mutant is caught (the differential, Codex deploy rounds 4-5):
#   control:<rule>  the control run is complete (why_incomplete, exec included)   control-fail    the control has zero FAIL
#   mutant:<rule>   the mutant run is complete (why_incomplete, exec included)    survived        the mutant has >= 1 FAIL
#   unrelated       EVERY FAIL message of the mutant (its ID aside) names the mutated topic as a whole name
#   effect          a script run the MUTANT made shows <effect> on the topic in its recorded broker calls (effect_shown)
#   control-effect  no script run the CONTROL made shows it
# What that establishes, and no more: the unit ran to completion on both copies, making its declared assertion IDs; every
# apply-topics.sh / cleanup-topics.sh run on both copies ended with the script's own status (0 or 1) and no shell-level
# diagnostic, so none died of a missing command, a bad path or an unset variable; the control's assertions all passed;
# the mutant's broker calls show the behaviour removing that one token must produce (e.g. a never-recreate topic
# actually deleted and created again) and the control's do not; and the mutant has FAILs, each naming the topic. It
# does NOT prove that each FAIL was triggered by that effect rather than by something else in the same mutant run —
# that link is read from the unit (its assertions are about exactly those calls).
nested_verdict() {
  local t="$1" eff="$2" c="$3" m="$4" want="$5" pc pm st why line; shift 5
  NV_REASONS=(); NV_CAUGHT=0; NV_EXAMPLE=""; NV_EFFECT=""
  pc="$(mktemp -d "$WORK/nested.XXXXXX")/control"; pm="${pc%/control}/mutant"
  run_captured "$pc" "$1" "$c" "${@:2}"; st=$?
  why="$(why_incomplete "$pc" "$st" "$want")"
  [ -z "$why" ] || while IFS= read -r line; do NV_REASONS+=("control:$line"); done <<< "$why"
  [ "$(n_fail "$pc")" = 0 ] || NV_REASONS+=("control-fail: the control run, on the UNMUTATED copy, has $(n_fail "$pc") FAIL, e.g. $(fail_messages "$pc" | head -1)")
  line="$(effect_shown "$pc.runs" "$t" "$eff")" && NV_REASONS+=("control-effect: the CONTROL's own broker calls already show [$eff] on $t: $line")
  run_captured "$pm" "$1" "$m" "${@:2}"; st=$?
  why="$(why_incomplete "$pm" "$st" "$want")"
  [ -z "$why" ] || while IFS= read -r line; do NV_REASONS+=("mutant:$line"); done <<< "$why"
  NV_CAUGHT="$(n_fail "$pm")"
  [ "$NV_CAUGHT" -gt 0 ] || NV_REASONS+=("survived: no assertion FAILed on the mutant")
  # COUNTED, not "is the first offending line non-empty": an empty FAIL message is a line grep -v selects, and command
  # substitution would strip it to nothing and skip the refusal. The count sees it; the example is quoted so an empty
  # one is visible as "".
  x="$(fail_messages "$pm" | grep -cvE "$(named_rx "$t")" || true)"
  [ "${x:-0}" = 0 ] || NV_REASONS+=("unrelated: $x FAIL(s) of the mutant do not name $t, e.g. \"$(fail_messages "$pm" | grep -vE "$(named_rx "$t")" | head -1)\"")
  if NV_EFFECT="$(effect_shown "$pm.runs" "$t" "$eff")"; then :
  else NV_REASONS+=("effect: no script run on the mutant shows [$eff] on $t in its broker calls${NV_EFFECT:+ ($NV_EFFECT)}, so the mutation was never shown to act"); fi
  NV_EXAMPLE="$(fail_messages "$pm" | head -1)"
}

# judge_nested <label> <topic> <effect> <control dir> <mutant dir> <unit> <args after src...>: ONE assertion (ID caught).
judge_nested() {
  local label="$1" t="$2" eff="$3" c="$4" m="$5" want; shift 5
  want="$(declared_inventory "$1" "$c" "${@:2}")"
  nested_verdict "$t" "$eff" "$c" "$m" "$want" "$@"
  if [ "${#NV_REASONS[@]}" = 0 ]; then
    ok caught "$label: CAUGHT. '$*' completed on both copies with its $(nw $want) declared assertion IDs and clean script runs; the control has no FAIL and no [$eff]; the mutant shows [$eff] ($NV_EFFECT) and $NV_CAUGHT FAIL, every one naming $t, e.g. $NV_EXAMPLE"
  else
    bad caught "$label: mutant NOT proven caught by '$*': $(printf '%s; ' "${NV_REASONS[@]}")"
  fi
}

# --- 7. the harness checks itself -----------------------------------------------------------------------------
# Each probe breaks the rules named beside it and must be refused by EXACTLY those rules (so removing any one rule
# from the harness makes a probe here fail by name); "-" marks a control, which breaks none and must be accepted.
T7="$P.ivrv"
COLLECTOR_PROBES=( # <unit> <declared inventory, comma-separated> <rules that must refuse it>
  "probe_good                       one,two -"
  "probe_completes_then_exits_99    one,two status"
  "probe_completes_then_returns_3   one,two status"
  "probe_forges_markers             one,two output status"
  "probe_dies_after_first           one,two count status"
  "probe_returns_0_after_first      one,two count"
  "probe_exits_0_after_first        one,two count"
  "probe_asserts_nothing            one,two count"
  "probe_asserts_too_much           one,two count"
  "probe_repeats_one_omits_another  one,two inventory"
  "probe_right_ids_wrong_order      one,two inventory"
  "probe_lookalike_on_stdout        one,two count output"
  "probe_foreign_line_on_channel    one,two record"
  "probe_shell_error                one,two output"
  "probe_reports_a_fail             one,two fail"
  "probe_descendant_fails_late      one,two count fail"
  "probe_scripts_ran_clean          one,two -"
  "probe_script_not_found           one,two exec"
  "probe_script_diagnostic_exit_1   one,two exec"
)
# fake_run <rc> <output> [<broker-call log line>...]: records one script run exactly where apply_run/cleanup_run would
# (a newrun directory with rc, out and log), without running a script.
fake_run() {
  local d; d="$(newrun)"; printf '%s\n' "$1" > "$d/rc"; printf '%s\n' "$2" > "$d/out"; shift 2
  : > "$d/log"; [ $# = 0 ] || printf '%s\n' "$@" > "$d/log"
}
probe_good()                     { ok one "one"; ok two "two"; }
probe_completes_then_exits_99()  { ok one "one"; ok two "two"; exit 99; }
probe_completes_then_returns_3() { ok one "one"; ok two "two"; return 3; }
probe_forges_markers() { # every completion marker a unit can reach, forged, then a real exit 99
  ok one "one"; ok two "two"
  printf '0\n' > "$p.rc"; printf '0\n' > "$p.status"   # the round-3 sidecar (p is run_captured's, visible here)
  echo "__unit_rc=0"                                    # the round-2 in-band marker
  ( sleep 0.3; printf '0\n' > "$p.rc" ) &              # round 4: a descendant rewriting it after the unit is gone
  exit 99
}
probe_dies_after_first()         { ok one "one"; exit 99; ok two "UNREACHED"; }
probe_returns_0_after_first()    { ok one "one"; return 0; ok two "UNREACHED"; }
probe_exits_0_after_first()      { ok one "one"; exit 0; ok two "UNREACHED"; }
probe_asserts_nothing()          { :; }
probe_asserts_too_much()         { ok one "one"; ok two "two"; ok three "three"; }
# Codex r5: a genuine check made twice stands in for one never made — the count is right, the inventory is not.
probe_repeats_one_omits_another() { local value=1
  [ "$value" = 1 ] && ok one "first property checked" || bad one "first property"
  [ "$value" = 1 ] && ok one "first property checked" || bad one "first property"
  return 0
  [ "$value" = 2 ] && ok two "second property checked" || bad two "second property"
}
probe_right_ids_wrong_order()    { ok two "two"; ok one "one"; }
probe_lookalike_on_stdout()      { ok one "one"; printf '  ok   [two] %s\n' "two, printed on stdout"; }
probe_foreign_line_on_channel()  { ok one "one"; ok two "two"; echo "__unit_rc=0" >&3; }
probe_shell_error()              { cat "$WORK/no-such-file-for-the-selfcheck"; ok one "one"; ok two "two"; }
probe_reports_a_fail()           { ok one "one"; bad two "two"; }
# Finality: a descendant the unit left running writes a FAIL after the unit exited 0 with its two ok. A collector that
# read the record when the unit's status arrived would judge it complete and clean; run_captured waits for the channel
# to close, so the FAIL is in the record that is judged.
probe_descendant_fails_late()    { ok one "one"; ok two "two"; ( sleep 0.3; bad two "written by a descendant after the unit exited" ) & exit 0; }
# The exec rule: script runs that ended as the scripts themselves end (0, and a clean refusal 1) are accepted; a run that
# exited 127 with "command not found", or exited 1 carrying a bash "line N:" diagnostic, is an execution failure even
# though the unit's own assertions all passed.
probe_scripts_ran_clean()        { fake_run 0 "applied"; fake_run 1 "HARD ERROR: topic $T7 has partitions=4 but requires EXACTLY 1"; ok one "one"; ok two "two"; }
probe_script_not_found()         { fake_run 127 "apply-topics.sh: dependency: command not found"; ok one "one"; ok two "two"; }
probe_script_diagnostic_exit_1() { fake_run 1 "/src/apply-topics.sh: line 201: OPTIONS_EDGE_TOPICS: unbound variable"; ok one "one"; ok two "two"; }

# The nested probes model the never-recreate mutation: its declared effect is "recreated" (the mutant's apply run deletes
# the topic and creates it again). fake_side records, as a real unit_drift would, a clean never-recreate refusal on the
# control and a successful recreation on the mutant; the probes then vary one thing each.
NESTED_EFFECT=recreated
NESTED_PROBES=( # <nested unit> <declared inventory, comma-separated> <rules that must refuse it>; runs on <dir>/control and <dir>/mutant
  "nested_caught                            a,b -"
  "nested_mutant_completes_then_exits_99    a,b mutant:status"
  "nested_mutant_fails_then_aborts          a,b mutant:count mutant:status"
  "nested_mutant_unset_abort                a,b mutant:count mutant:output mutant:status"
  "nested_mutant_fails_then_exits_0         a,b mutant:count"
  "nested_mutant_repeats_one_omits_another  a,b mutant:inventory"
  "nested_same_fail_in_control              a,b control-fail"
  "nested_control_fails                     a,b control-fail"
  "nested_control_aborts                    a,b control:count control:status"
  "nested_unrelated_fail_in_mutant          a,b unrelated"
  "nested_fail_names_a_longer_topic         a,b unrelated"
  "nested_survivor                          a,b survived"
  "nested_mutant_exec_failure               a,b effect mutant:exec"
  "nested_mutant_refuses_without_effect     a,b effect"
  "nested_mutant_exec_failure_after_effect  a,b mutant:exec"
  "nested_control_exec_failure              a,b control:exec"
  "nested_effect_already_in_control         a,b control-effect"
  "nested_mutant_empty_fail_only            a,b unrelated"
  "nested_mutant_empty_fail_then_unrelated  a,b unrelated"
)
side() { echo "${1##*/}"; }   # control | mutant
fake_refused()   { fake_run 1 "HARD ERROR: topic $T7 has partitions=4 but requires EXACTLY 1 (OPTIONS_EDGE_NEVER_RECREATE_TOPICS)"; }
fake_recreated() {
  fake_run 0 "recreated $T7" "kafka-topics --bootstrap-server localhost:9092 --delete --topic $T7" \
    "kafka-topics --bootstrap-server localhost:9092 --create --topic $T7 --partitions 1 --replication-factor 1 --config cleanup.policy=delete"
}
fake_side() { if [ "$(side "$1")" = control ]; then fake_refused; else fake_recreated; fi; }
NOT_FOUND="apply-topics.sh: dependency: command not found"
nested_caught()                         { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; ok b "b"; }; }
nested_mutant_completes_then_exits_99() { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; bad b "$T7 b"; exit 99; }; }
nested_mutant_fails_then_aborts()       { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; exit 99; }; }
nested_mutant_unset_abort()             { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; : "$NO_SUCH_VARIABLE_FOR_THE_SELFCHECK"; ok b "b"; }; }
nested_mutant_fails_then_exits_0()      { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; exit 0; ok b "b"; }; }
nested_mutant_repeats_one_omits_another() { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; bad a "$T7 a"; }; }
# Codex r4's reproduction: a failure that has nothing to do with the mutation (a missing command) fails an assertion
# whose message names the topic — on the control exactly as on the mutant.
nested_same_fail_in_control()           { fake_side "$1"; ok a "a"; bad b "$T7: not the never-recreate hard error: $NOT_FOUND"; }
nested_control_fails()                  { fake_side "$1"; [ "$(side "$1")" = control ] && { bad a "an unrelated check"; ok b "b"; } || { bad a "$T7 a"; ok b "b"; }; }
nested_control_aborts()                 { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; exit 99; } || { bad a "$T7 a"; ok b "b"; }; }
nested_unrelated_fail_in_mutant()       { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7 a"; bad b "$P.events b"; }; }
nested_fail_names_a_longer_topic()      { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a "$T7-v2 a"; ok b "b"; }; }
nested_survivor()                       { fake_side "$1"; ok a "a"; ok b "b"; }
# Codex r5's reproduction: a healthy control (clean refusal, all ok); on the mutant ONLY, the apply run dies with 127
# "command not found" before reaching the mutated code — zero delete, zero create — and the one mutant FAIL is the
# missing never-recreate diagnostic, naming the topic. Refused: the run did not execute, and nothing was recreated.
nested_mutant_exec_failure() {
  if [ "$(side "$1")" = control ]; then fake_refused; ok a "a"; ok b "b"
  else fake_run 127 "$NOT_FOUND"; ok a "a"; bad b "$T7: not the never-recreate hard error: $NOT_FOUND"; fi
}
# The effect rule alone: the mutant's run executes cleanly (exit 1, no diagnostic) but still never recreates the topic.
nested_mutant_refuses_without_effect() {
  if [ "$(side "$1")" = control ]; then fake_refused; ok a "a"; ok b "b"
  else fake_run 1 "Topic $T7 exists with partitions=4 but requires EXACTLY 1"; ok a "a"; bad b "$T7: not the never-recreate hard error"; fi
}
# The exec rule alone: the mutant recreates the topic, but another script run it made died of a missing command.
nested_mutant_exec_failure_after_effect() {
  fake_side "$1"
  if [ "$(side "$1")" = control ]; then ok a "a"; ok b "b"; else fake_run 127 "$NOT_FOUND"; bad a "$T7 a"; ok b "b"; fi
}
nested_control_exec_failure() {
  fake_side "$1"
  if [ "$(side "$1")" = control ]; then fake_run 127 "$NOT_FOUND"; ok a "a"; ok b "b"; else bad a "$T7 a"; ok b "b"; fi
}
nested_effect_already_in_control() {
  fake_recreated
  if [ "$(side "$1")" = control ]; then ok a "a"; ok b "b"; else bad a "$T7 a"; ok b "b"; fi
}
# Codex r6: a FAIL with an EMPTY message names no topic, but the first-offending-line test lost it to command
# substitution and accepted the mutant. Both shapes: the empty FAIL alone, and an empty FAIL ahead of a non-empty
# unrelated one (which the old "head -1" never reached). Refused by [unrelated] and nothing else.
nested_mutant_empty_fail_only()          { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a ""; ok b "b"; }; }
nested_mutant_empty_fail_then_unrelated() { fake_side "$1"; [ "$(side "$1")" = control ] && { ok a "a"; ok b "b"; } || { bad a ""; bad b "$P.events b"; }; }

unit_collector_selfcheck() { # one assertion per probe: probe:<fn>, then nested:<fn>
  local d="$WORK/selfcheck" spec fn inv rules st got; mkdir -p "$d"
  for spec in "${COLLECTOR_PROBES[@]}"; do
    read -r fn inv rules <<< "$spec"; [ "$rules" = - ] && rules=""; inv="${inv//,/ }"
    run_captured "$d/$fn" "$fn" & wait "$!"; st=$?           # the scheduler's path: background, then wait <pid>
    judge_unit "$d/$fn" "$st" "$inv" > "$d/$fn.judged"
    got="$JUDGED_RULES"
    if [ "$got" = "$rules" ]; then
      if [ -z "$rules" ]; then ok "probe:$fn" "collector ACCEPTS control $fn: status 0, assertion IDs exactly [$inv], no FAIL, no stray output, clean script runs"
      else ok "probe:$fn" "collector REFUSES $fn by exactly [$rules]"; fi
    else
      bad "probe:$fn" "collector judged $fn by [${got:-nothing: ACCEPTED}], want [${rules:-nothing: accepted}]: $(grep '^  FAIL the unit' "$d/$fn.judged" | tr '\n' ' ')"
    fi
  done
  for spec in "${NESTED_PROBES[@]}"; do
    read -r fn inv rules <<< "$spec"; [ "$rules" = - ] && rules=""; inv="${inv//,/ }"
    mkdir -p "$d/$fn/control" "$d/$fn/mutant"
    nested_verdict "$T7" "$NESTED_EFFECT" "$d/$fn/control" "$d/$fn/mutant" "$inv" "$fn"
    got="$(rules_of "${NV_REASONS[@]}")"
    if [ "$got" = "$rules" ]; then
      if [ -z "$rules" ]; then ok "nested:$fn" "nested judge ACCEPTS control $fn: both runs complete and executed cleanly, control clean without [$NESTED_EFFECT], mutant shows it and every mutant FAIL names $T7"
      else ok "nested:$fn" "nested judge REFUSES $fn by exactly [$rules]"; fi
    else
      bad "nested:$fn" "nested judge judged $fn by [${got:-nothing: ACCEPTED as caught}], want [${rules:-nothing: caught}]: $(printf '%s; ' "${NV_REASONS[@]}")"
    fi
  done
}

# --- what each unit must assert: declared by the scheduler, from the unit's inputs alone -------------------------
# declared_inventory <unit> <args...> exactly as the unit is called: the ordered, space-separated assertion IDs it must
# make, repeats included. It mirrors each unit's paths (unit_drift's refusal/recreate branch depends only on its
# inputs); an undeclared unit gets "[undeclared:<unit>]", which no well-formed assertion can match.
declared_inventory() {
  local ids=() t f m spec
  case "$1" in
    unit_create)    ids=(exit no-delete); for t in $ALL; do ids+=("create:$t" "reconcile:$t"); done ;;
    unit_reconcile) ids=(exit no-create-delete); for t in $ALL; do ids+=("drifted:$t"); done ;;
    unit_drift)     for f in ${5:-false true}; do
                      if is_durable "$4" || [ "$f" = false ]; then
                        ids+=("$f:refused" "$f:no-delete" "$f:no-create")
                        if is_durable "$4"; then ids+=("$f:never-recreate-error"); else ids+=("$f:exact-partition-refusal"); fi
                      else ids+=("$f:exit" "$f:deleted-only-it" "$f:recreated" "$f:reconciled"); fi
                    done ;;
    unit_cleanup)   for m in ${4:-sweep delete-recreate retention}; do
                      ids+=("$m:exit")
                      case "$m" in
                        sweep)           for t in $ALL; do ids+=("sweep:kept:$t"); done; ids+=(sweep:junk-deleted) ;;
                        delete-recreate) for t in $DURABLE; do ids+=("delete-recreate:spared:$t"); done
                                         for t in $REBUILT; do ids+=("delete-recreate:deleted:$t"); done ;;
                        retention)       for t in $DURABLE; do ids+=("retention:spared:$t"); done
                                         for t in $REBUILT; do ids+=("retention:shrunk:$t"); done ;;
                        *)               ids+=("[undeclared-mode:$m]") ;;
                      esac
                    done ;;
    unit_protected) ids=(copy-built isolated exit); for t in $DURABLE; do ids+=("kept:$t"); done
                    for t in $REBUILT $JUNK; do ids+=("swept:$t"); done ;;
    mut_unit)       ids=(built membership caught) ;;
    unit_collector_selfcheck)
                    for spec in "${COLLECTOR_PROBES[@]}"; do ids+=("probe:${spec%% *}"); done
                    for spec in "${NESTED_PROBES[@]}"; do ids+=("nested:${spec%% *}"); done ;;
    *)              echo "declared_inventory: no inventory declared for $1" >&2; ids=("[undeclared:$1]") ;;
  esac
  echo "${ids[*]}"
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
  UNITS+=("$f"); TITLES+=("$1"); shift; DECLARED+=("$(declared_inventory "$@")")
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
    mut_unit OPTIONS_EDGE_NEVER_RECREATE_TOPICS name "$t" recreated unit_drift production "$t" true
  launch "6. self-check: $t removed from OPTIONS_EDGE_RESET_PRESERVED_TOPICS" \
    mut_unit OPTIONS_EDGE_RESET_PRESERVED_TOPICS name "$t" deleted unit_cleanup production delete-recreate
  launch "6. self-check: $t removed from OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES" \
    mut_unit OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES key "$t" created-without:retention.bytes=-1 unit_create dev
done
launch "6. self-check: $P.current removed from OPTIONS_EDGE_COMPACTED_TOPICS" \
  mut_unit OPTIONS_EDGE_COMPACTED_TOPICS name "$P.current" created-with:cleanup.policy=delete unit_create dev
launch "6. self-check: $P.current removed from OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS" \
  mut_unit OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS name "$P.current" created-with:cleanup.policy=compact,delete unit_create production
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
