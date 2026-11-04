#!/usr/bin/env bash
# dev-cleanup.sh must apply the per-topic retention.bytes caps that topics.env declares
# (OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES), both when it CREATES a topic and when it RECONCILES one that already exists.
#
# The 2026-10-25..2026-11-04 failure this pins: the script read every other override list and not this one, so each clean rebuilt
# futures.level-absorption.events with no cap and level-absorption-service refused to start (EVENTS_TOPIC_RETENTION_MISMATCH:
# "reports retention.bytes=-1, the artifact declares 81675540"); it was repaired by hand after every clean.
#
# dev-cleanup.sh dispatches on $MODE when sourced, so the REAL functions are cut out of it by name and run against a fake
# kafka-topics / kafka-configs that record their arguments. Nothing here touches a broker.
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -f "$SRC" ] || { echo "FAIL: $SRC missing" >&2; exit 1; }

# fn <name> -> the function's text, from its "name() {" line to the first column-0 "}".
fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$SRC"; }
for f in topic_desired reconcile_declared_topics ensure_topics create_topics_parallel; do
  body="$(fn "$f")"
  [ -n "$body" ] || { echo "FAIL: function $f not found in $SRC" >&2; exit 1; }
  printf '%s\n' "$body"
done > "$WORK/functions.sh"

# Fake brokers. KT answers --list/--describe from files and records --create; KC records --alter.
cat > "$WORK/kt" <<'EOF'
#!/usr/bin/env bash
case " $* " in
  *" --list "*)     cat "$FAKE_DIR/list" ;;
  *" --describe "*) cat "$FAKE_DIR/describe" ;;
  *" --create "*)   echo "$*" >> "$FAKE_DIR/created" ;;
esac
exit 0
EOF
cat > "$WORK/kc" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_DIR/altered"
exit 0
EOF
chmod +x "$WORK/kt" "$WORK/kc"

# The declared world: one capped topic (the level-absorption events shape), one declared unlimited, one with no declaration.
TOPICS_ENV='OPTIONS_EDGE_TOPICS="t.cap:1 t.unl:1 t.plain:1"
OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="t.cap=-1"
OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES="t.cap=81675540 t.unl=-1"'

# run_case <existing topics, one per line> <describe lines> -> sets CREATED / ALTERED / OUT
run_case() {
  export FAKE_DIR="$WORK/run.$RANDOM"; mkdir -p "$FAKE_DIR"
  printf '%s\n' "$1" > "$FAKE_DIR/list"
  printf '%s\n' "$2" > "$FAKE_DIR/describe"
  : > "$FAKE_DIR/created"; : > "$FAKE_DIR/altered"
  OUT="$(
    KT="$WORK/kt"; KC="$WORK/kc"; BS=fake:9092; DEPLOY_REPO=/nonexistent; TOPICS_ENV_REF=origin/main:scripts/kafka/topics.env
    git() { case " $* " in *" show "*) printf '%s\n' "$TOPICS_ENV" ;; esac; return 0; }
    ensure_partition_only_topics() { :; }
    ensure_dev_only_estimate_topics() { :; }
    # shellcheck source=/dev/null
    . "$WORK/functions.sh"
    export TOPICS_ENV KT BS
    ensure_topics 2>&1
  )"
  [ -z "${DEBUG_OUT:-}" ] || printf '%s\n' "$OUT" | sed 's/^/      out: /'
  CREATED="$(cat "$FAKE_DIR/created")"; ALTERED="$(cat "$FAKE_DIR/altered")"
}
desc() { printf 'Topic: %s\tTopicId: x\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: %s\n' "$1" "$2"; }

echo "1. a wiped dev: the topic is CREATED with its declared cap (the 11-04 clean)"
run_case "" ""
case "$CREATED" in *"--topic t.cap "*"--config retention.bytes=81675540"*) ok "t.cap created with retention.bytes=81675540" ;;
  *) bad "t.cap created without its cap: $CREATED" ;; esac
case "$CREATED" in *"--topic t.plain "*retention.bytes*) bad "t.plain (no declaration) got a retention.bytes: $CREATED" ;;
  *) ok "t.plain (no declaration) carries no retention.bytes" ;; esac

echo "2. an existing topic with NO cap (inherits the broker's unlimited) is raised to the declared cap"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1'; desc t.unl 'cleanup.policy=delete'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
case "$ALTERED" in *"--entity-name t.cap"*"retention.bytes=81675540"*) ok "t.cap altered to retention.bytes=81675540" ;;
  *) bad "t.cap NOT altered to its cap: ${ALTERED:-<no alter>}" ;; esac

echo "3. an existing topic already at its cap is left alone"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,retention.bytes=81675540'; desc t.unl 'cleanup.policy=delete'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
[ -z "$ALTERED" ] && ok "no alter issued" || bad "altered a topic already correct: $ALTERED"

echo "4. a declared -1 that the topic inherits is NOT rewritten (no per-run churn over the unlimited topics)"
case "$ALTERED" in *t.unl*) bad "t.unl (declared -1, inherited) was rewritten: $ALTERED" ;; *) ok "t.unl untouched" ;; esac

echo "5. a different finite cap is corrected, and an explicit cap on a declared -1 topic is lifted"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,retention.bytes=21765840'; desc t.unl 'cleanup.policy=delete,retention.bytes=5'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
case "$ALTERED" in *"--entity-name t.cap"*"retention.bytes=81675540"*) ok "t.cap 21765840 -> 81675540" ;; *) bad "t.cap cap not corrected: ${ALTERED:-<no alter>}" ;; esac
case "$ALTERED" in *"--entity-name t.unl"*"retention.bytes=-1"*) ok "t.unl 5 -> -1" ;; *) bad "t.unl explicit cap not lifted: ${ALTERED:-<no alter>}" ;; esac

echo
if [ "$fails" -eq 0 ]; then echo "PASS: dev-cleanup applies declared retention.bytes caps"; exit 0; fi
echo "$fails FAILED"; exit 1
