#!/usr/bin/env bash
# dev-cleanup.sh must FIND the Kafka CLI and must STOP, loudly, when it cannot -- never run blind.
#
# The failure this pins (2026-11-05/06): KT was hard-coded to /Users/abhinav/kafka-4.3.0/bin/kafka-topics.sh. That directory is
# the dev broker host's distribution; on the 201 Mac it was deleted on 2026-10-27 and left an EMPTY directory of the same name.
# Every kafka-topics call in the script runs with 2>/dev/null, so a clean there listed nothing, deleted nothing and printed
# "changelog topics: 0 (0 = success)": a measurement that never happened, read as a good one.
#
# The real functions are cut out of dev-cleanup.sh by name and run against fake distributions. Nothing here touches a broker.
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -f "$SRC" ] || { echo "FAIL: $SRC missing" >&2; exit 1; }

fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$SRC"; }
for f in kafka_cli_candidates kafka_cli_java kafka_cli_usable resolve_kafka_cli kafka_broker_answers require_kafka_cli kafka_list_topics topic_count; do
  body="$(fn "$f")"
  [ -n "$body" ] || { echo "FAIL: function $f not found in $SRC" >&2; exit 1; }
  printf '%s\n' "$body"
done > "$WORK/functions.sh"

# A fake distribution. --version prints $VERSION_OUT (default 4.3.0); --list prints $LIST_OUT and exits $LIST_RC.
mkdist() { # <dir> [version-output | FAIL]
  mkdir -p "$1"
  local vo="${2:-4.3.0}" vrc=0
  [ "$vo" != FAIL ] || { vo="java: command not found"; vrc=1; }
  cat > "$1/kafka-topics.sh" <<FAKE
#!/usr/bin/env bash
case " \$* " in
  *" --version "*) echo "$vo"; exit $vrc ;;
  *" --list "*) [ -z "\$LIST_RC" ] || { echo "Timed out waiting for a node assignment" >&2; exit "\$LIST_RC"; }; printf '%s\n' \$LIST_OUT ;;
esac
FAKE
  printf '#!/usr/bin/env bash\nexit 0\n' > "$1/kafka-configs.sh"
  printf '# fake\n' > "$1/kafka-run-class.sh"
  chmod +x "$1/kafka-topics.sh" "$1/kafka-configs.sh"
}
mkdist "$WORK/good/bin"
mkdir -p "$WORK/emptydir"                       # the 201 Mac: the directory exists, the distribution does not
mkdist "$WORK/noversion/bin" FAIL               # tools exist but do not run
mkdist "$WORK/garbage/bin" "Error: no JDK"
mkdist "$WORK/partial/bin"; rm -f "$WORK/partial/bin/kafka-run-class.sh"
mkdist "$WORK/major3/bin" 3.9.0                 # a real, working CLI of another generation

# run_case <env assignments...> -> OUT (stdout+stderr), RC, and KT/KC as seen after
run_case() {
  OUT="$(
    env -i PATH="$PATH" HOME="$WORK/home" JAVA_HOME="${JAVA_HOME:-}" BS=fake:9092 "$@" bash -c '
      . "'"$WORK"'/functions.sh"
      KT=""; KC=""
      require_kafka_cli
      echo "KT=$KT"; echo "KC=$KC"' 2>&1
  )"; RC=$?
}
JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"; [ -n "$JAVA_HOME" ] || JAVA_HOME="$WORK"   # kafka_cli_java only needs bin/java
if [ ! -x "$JAVA_HOME/bin/java" ]; then mkdir -p "$WORK/jdk/bin"; printf '#!/bin/sh\nexit 0\n' > "$WORK/jdk/bin/java"; chmod +x "$WORK/jdk/bin/java"; JAVA_HOME="$WORK/jdk"; fi
export JAVA_HOME

echo "1. the first usable candidate wins; an empty directory of the right name is skipped, not trusted"
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/emptydir $WORK/good/bin" LIST_OUT="a b"
[ "$RC" = 0 ] && ok "resolved (rc 0)" || bad "rc $RC: $OUT"
case "$OUT" in *"KT=$WORK/good/bin/kafka-topics.sh"*) ok "KT is the good distribution's kafka-topics.sh" ;; *) bad "KT: $OUT" ;; esac
case "$OUT" in *"KC=$WORK/good/bin/kafka-configs.sh"*) ok "KC is its sibling kafka-configs.sh" ;; *) bad "KC: $OUT" ;; esac

echo "2. nothing usable: the script STOPS with every candidate and its reason (the 201 Mac)"
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/emptydir $WORK/noversion/bin $WORK/garbage/bin $WORK/partial/bin $WORK/missing"
[ "$RC" != 0 ] && ok "exit $RC" || bad "continued blind: $OUT"
case "$OUT" in *"NO USABLE KAFKA CLI"*) ok "says so" ;; *) bad "no message: $OUT" ;; esac
for want in "$WORK/emptydir -- no executable kafka-topics.sh" "$WORK/noversion/bin -- kafka-topics.sh --version failed" "$WORK/garbage/bin -- kafka-topics.sh --version did not print a version" "$WORK/partial/bin -- no kafka-run-class.sh" "$WORK/missing -- not a directory"; do
  case "$OUT" in *"$want"*) ok "names: ${want##*/}" ;; *) bad "does not name [$want]: $OUT" ;; esac
done
case "$OUT" in *"KT=$WORK"*) bad "KT was set anyway: $OUT" ;; *) ok "KT stays empty" ;; esac

echo "2b. a working CLI of another Kafka generation is refused, with the reason (not accepted because --version prints a number)"
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/major3/bin"
[ "$RC" != 0 ] && ok "refused" || bad "accepted a 3.x CLI: $OUT"
case "$OUT" in *"is not the expected major 4"*) ok "names the expected major" ;; *) bad "no major message: $OUT" ;; esac
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/major3/bin" DEV_CLEANUP_KAFKA_MAJOR=3 LIST_OUT="a"
[ "$RC" = 0 ] && ok "DEV_CLEANUP_KAFKA_MAJOR=3 accepts it deliberately" || bad "override not honoured: $OUT"

echo "3. an explicit KAFKA_CLI_DIR is the ONLY candidate: a bad one is an error, never a silent fall-through to a good one"
run_case KAFKA_CLI_DIR="$WORK/emptydir" DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/good/bin"
[ "$RC" != 0 ] && ok "refused" || bad "fell through: $OUT"
run_case KAFKA_CLI_DIR="$WORK/good/bin" LIST_OUT="a"
[ "$RC" = 0 ] && ok "a good one is used" || bad "rc $RC: $OUT"

echo "4. a CLI that resolves but a broker that does not answer: STOP (a dead tunnel is not an empty cluster)"
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/good/bin" LIST_RC=1
[ "$RC" != 0 ] && ok "exit $RC" || bad "ran against a dead broker: $OUT"
case "$OUT" in *"does not answer kafka-topics --list"*) ok "says the broker does not answer" ;; *) bad "no broker message: $OUT" ;; esac

echo "5. topic_count never turns a failed measurement into a 0"
tc() { # <env assignments...> -- <topic_count args...>
  local envs=()
  while [ "$1" != -- ]; do envs+=("$1"); shift; done; shift
  env -i PATH="$PATH" HOME="$WORK/home" BS=fake:9092 KT="$WORK/good/bin/kafka-topics.sh" "${envs[@]}" bash -c '. "$1"; shift; topic_count "$@"; echo "rc=$?"' _ "$WORK/functions.sh" "$@" 2>&1
}
R="$(tc LIST_RC=1 -- changelog)"
case "$R" in *UNKNOWN*"rc=1") ok "failed list -> UNKNOWN, rc 1" ;; *) bad "failed list printed: $R" ;; esac
R="$(tc LIST_OUT= -- changelog)"
case "$R" in *UNKNOWN*"rc=1") ok "empty list -> UNKNOWN (a broker always holds __consumer_offsets), rc 1" ;; *) bad "empty list printed: $R" ;; esac
R="$(tc LIST_OUT="__consumer_offsets _schemas a-changelog b-changelog c" -- changelog)"
case "$R" in "2"*"rc=0") ok "a real list is counted" ;; *) bad "count: $R" ;; esac
R="$(tc LIST_OUT="__consumer_offsets _schemas a b" -- '^__|^_schemas' v)"
case "$R" in "2"*"rc=0") ok "the inverted count (topics present now) excludes the system topics" ;; *) bad "inverted count: $R" ;; esac
R="$(tc LIST_OUT="__consumer_offsets x" -- changelog)"
case "$R" in "0"*"rc=0") ok "a genuine zero (list read, none match) is still a 0" ;; *) bad "genuine zero: $R" ;; esac

echo "5b. kafka_list_topics: retries a transient failure, and an error or an empty answer is never an empty list"
fl() { env -i PATH="$PATH" HOME="$WORK/home" BS=fake:9092 KT="$WORK/good/bin/kafka-topics.sh" DEV_CLEANUP_LIST_RETRY_SLEEP=0 "$@" bash -c '. "$1"; kafka_list_topics; echo "rc=$?"' _ "$WORK/functions.sh" 2>&1; }
R="$(fl LIST_OUT="__consumer_offsets a b")"
case "$R" in *"a"*"rc=0") ok "a real list is returned, rc 0" ;; *) bad "list: $R" ;; esac
R="$(fl LIST_RC=1)"
case "$R" in "rc=1") ok "a failed list: nothing on stdout, rc 1" ;; *) bad "failed list: $R" ;; esac
R="$(fl LIST_OUT=)"
case "$R" in "rc=1") ok "an empty answer: nothing on stdout, rc 1" ;; *) bad "empty list: $R" ;; esac
# a broker that fails the first two calls and answers the third
mkdir -p "$WORK/flaky/bin"; cp "$WORK/good/bin/"* "$WORK/flaky/bin/"
cat > "$WORK/flaky/bin/kafka-topics.sh" <<FLAKY
#!/usr/bin/env bash
n=\$(cat "$WORK/flaky/n" 2>/dev/null || echo 0); n=\$((n+1)); echo \$n > "$WORK/flaky/n"
[ \$n -ge 3 ] || exit 1
echo __consumer_offsets; echo t
FLAKY
chmod +x "$WORK/flaky/bin/kafka-topics.sh"
R="$(env -i PATH="$PATH" HOME="$WORK/home" BS=fake:9092 KT="$WORK/flaky/bin/kafka-topics.sh" DEV_CLEANUP_LIST_RETRY_SLEEP=0 bash -c '. "$1"; kafka_list_topics; echo "rc=$?"' _ "$WORK/functions.sh" 2>&1)"
case "$R" in *"t"*"rc=0") ok "two transient failures then an answer: returned after retries" ;; *) bad "flaky: $R" ;; esac

echo "6. the script has no hard-coded CLI path left to go stale"
hard="$(grep -n '/kafka-4\.3\.0/bin/kafka-[a-z-]*\.sh' "$SRC" | grep -v '^[0-9]*:[[:space:]]*#' | grep -v 'printf' || true)"
[ -z "$hard" ] && ok "no command invokes a fixed /kafka-4.3.0/bin/kafka-*.sh" || bad "hard-coded: $hard"
for mode_fn in do_start do_start_overnight do_es_down do_clean; do
  fn "$mode_fn" | grep -q 'require_kafka_cli' && ok "$mode_fn requires the CLI first" || bad "$mode_fn does not call require_kafka_cli"
done
fn ensure_topics | grep -q 'require_kafka_cli' && ok "ensure_topics requires the CLI first" || bad "ensure_topics does not call require_kafka_cli"
if sed -n '/^  topics)/,/^    ;;/p' "$SRC" | grep -q 'require_kafka_cli'; then ok "the topics mode requires the CLI first"; else bad "the topics mode does not call require_kafka_cli"; fi

fn do_clean | grep -q 'kafka_list_topics' && ok "the wipe lists topics through kafka_list_topics (a failed list refuses the wipe)" || bad "do_clean does not use kafka_list_topics"
if fn do_clean | grep -n -m1 'require_kafka_cli' | grep -q .; then
  first_req="$(fn do_clean | grep -n 'require_kafka_cli' | head -1 | cut -d: -f1)"; first_stop="$(fn do_clean | grep -n . | grep -v 'require_kafka_cli' | grep -E '^[0-9]+:[[:space:]]*[^#[:space:]].*(scale|clean_logs|delete|rm -)' | head -1 | cut -d: -f1)"
  { [ -z "$first_stop" ] || [ "$first_req" -lt "$first_stop" ]; } && ok "do_clean requires the CLI before its first scale/clean/delete step" || bad "require_kafka_cli (line $first_req) comes after a destructive step (line $first_stop)"
fi
fn ensure_topics | grep -q 'existing=""' && bad "ensure_topics still turns a failed list into an empty one" || ok "ensure_topics no longer reads a failed list as no topics"

echo "N. a JDK is found by the fallback search when JAVA_HOME is unset or invalid, and its absence is reported"
mkdir -p "$WORK/jdk2/bin"; printf '#!/bin/sh\nexit 0\n' > "$WORK/jdk2/bin/java"; chmod +x "$WORK/jdk2/bin/java"
R="$(env -i PATH="$PATH" HOME="$WORK/home" JAVA_HOME="$WORK/not-a-jdk" DEV_CLEANUP_JDK_SEARCH="$WORK/nope $WORK/jdk2" bash -c '. "'"$WORK"'/functions.sh"; kafka_cli_java; echo "rc=$? JH=$JAVA_HOME"')"
case "$R" in "rc=0 JH=$WORK/jdk2") ok "invalid JAVA_HOME -> the first usable search entry is used and exported" ;; *) bad "jdk fallback: $R" ;; esac
R="$(env -i PATH="$PATH" HOME="$WORK/home" DEV_CLEANUP_JDK_SEARCH="$WORK/nope $WORK/nope2" bash -c '. "'"$WORK"'/functions.sh"; kafka_cli_java; echo "rc=$?"')"
case "$R" in "rc=1") ok "no usable JDK anywhere -> return 1" ;; *) bad "no-jdk: $R" ;; esac
run_case DEV_CLEANUP_KAFKA_CLI_SEARCH="$WORK/good/bin" JAVA_HOME="$WORK/not-a-jdk" DEV_CLEANUP_JDK_SEARCH="$WORK/nope" LIST_OUT="a"
[ "$RC" != 0 ] && case "$OUT" in *"no usable JDK"*) ok "the resolver names the missing JDK" ;; *) bad "no JDK message: $OUT" ;; esac || bad "no JDK did not stop the resolver: $OUT"

echo "O. an unset HOME does not crash the candidate list under set -u"
R="$(env -i PATH="$PATH" bash -c 'set -u; . "'"$WORK"'/functions.sh"; kafka_cli_candidates | head -3; echo "rc=$?"' 2>&1)"
case "$R" in *"unbound variable"*) bad "an unset HOME broke the candidate list: $R" ;; *"rc=0"*) ok "HOME unset: candidates listed, no unbound variable" ;; *) bad "HOME unset: $R" ;; esac

R="$(env -i PATH="$PATH" bash -c 'set -u; eval "$(grep -m1 "^LAUNCH_AGENTS_DIR=" "'"$SRC"'")"; echo "dir=$LAUNCH_AGENTS_DIR rc=$?"' 2>&1)"
case "$R" in *"unbound variable"*) bad "an unset HOME broke the LaunchAgents default: $R" ;; "dir=/var/empty/Library/LaunchAgents rc=0") ok "HOME unset: the LaunchAgents default does not crash" ;; *) bad "LaunchAgents default: $R" ;; esac
fn kafka_cli_java | grep -q 'local -a jdks=()' && fn kafka_cli_java | grep -q 'for j in \${jdks\[@\]+' && ok "the default JDK list is an array (a path containing a space stays one candidate)" || bad "the JDK list is not an array again"
R="$(env -i PATH="$PATH" HOME="$WORK/home" DEV_CLEANUP_JDK_SEARCH="   " bash -c 'set -u; . "'"$WORK"'/functions.sh"; kafka_cli_java; echo "rc=$?"' 2>&1)"
case "$R" in *"unbound variable"*) bad "an empty JDK search crashed under set -u: $R" ;; *) ok "an empty (whitespace) JDK search override is handled under set -u" ;; esac

echo "P. the installer's --check fails on a missing live copy and on drift, and passes on an identical copy"
REPO="$(cd "$HERE/../.." && pwd)"
LIVE="$WORK/live/dev-cleanup.sh"; mkdir -p "$WORK/live"
OUT="$(DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" --check 2>&1)"; RC=$?
{ [ "$RC" = 1 ] && case "$OUT" in *"DRIFT: live copy"*"is missing"*) true ;; *) false ;; esac; } && ok "missing live copy -> DRIFT, exit 1" || bad "missing: rc=$RC $OUT"
cp "$REPO/scripts/ops/dev-cleanup.sh" "$LIVE"; echo "# drift" >> "$LIVE"
OUT="$(DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" --check 2>&1)"; RC=$?
{ [ "$RC" = 1 ] && case "$OUT" in *"DRIFT:"*"differs"*) true ;; *) false ;; esac; } && ok "a differing live copy -> DRIFT, exit 1" || bad "drift: rc=$RC $OUT"
# The clean is THREE files: dev-cleanup.sh sources the registry prune, which reads its keep set
# from dev-registry-keep-set.py. A --check that passed on the clean alone proved only part of what
# runs -- and the prune is the half that deletes manifests and garbage-collects the blob store.
DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" >/dev/null 2>&1
OUT="$(DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" --check 2>&1)"; RC=$?
{ [ "$RC" = 0 ] && case "$OUT" in *"OK: dev-cleanup.sh == repo"*) true ;; *) false ;; esac; } && ok "an identical live copy -> OK, exit 0" || bad "identical: rc=$RC $OUT"
case "$OUT" in
  *"dev-registry-prune.sh =="*) ok "--check covers the registry prune too" ;;
  *) bad "--check does not cover dev-registry-prune.sh: $OUT" ;;
esac
# The prune also runs the pull check; a live copy that drifted (or never arrived) must fail --check
# too, or the machine runs a prune whose "after" verdict nobody reviewed.
for f in dev-registry-verify-pinned.sh dev-registry-verify-pinned.py; do
  case "$OUT" in *"$f =="*) ok "--check covers $f" ;; *) bad "--check does not cover $f: $OUT" ;; esac
done
echo "# drift" >> "$WORK/live/dev-registry-verify-pinned.py"
OUT="$(DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" --check 2>&1)"; RC=$?
{ [ "$RC" = 1 ] && case "$OUT" in *"dev-registry-verify-pinned.py"*"differs"*) true ;; *) false ;; esac; } \
  && ok "drift in the PULL CHECK alone -> DRIFT, exit 1" || bad "pull-check drift undetected: rc=$RC $OUT"
cp "$REPO/scripts/ops/dev-registry-verify-pinned.py" "$WORK/live/dev-registry-verify-pinned.py"
echo "# drift" >> "$WORK/live/dev-registry-prune.sh"
OUT="$(DEV_CLEANUP_LIVE="$LIVE" bash "$REPO/scripts/ops/install-dev-cleanup.sh" --check 2>&1)"; RC=$?
{ [ "$RC" = 1 ] && case "$OUT" in *"dev-registry-prune.sh"*"differs"*) true ;; *) false ;; esac; } \
  && ok "drift in the PRUNE alone -> DRIFT, exit 1" || bad "prune drift undetected: rc=$RC $OUT"

[ "$fails" = 0 ] && { echo "dev-cleanup-kafka-cli-test: all ok"; exit 0; } || { echo "dev-cleanup-kafka-cli-test: $fails FAILED"; exit 1; }
