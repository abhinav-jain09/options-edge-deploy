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
PLAIN_LINE="$(printf '%s\n' "$CREATED" | grep -- '--topic t.plain ')"
case "$PLAIN_LINE" in "") bad "t.plain was not created at all" ;;
  *retention.bytes*) bad "t.plain (no declaration) got a retention.bytes: $PLAIN_LINE" ;;
  *) ok "t.plain (no declaration) carries no retention.bytes" ;; esac

echo "2. an existing UNCAPPED topic is NOT lowered on a live start (the broker would delete records), only reported"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1'; desc t.unl 'cleanup.policy=delete'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
[ -z "$ALTERED" ] && ok "no alter issued" || bad "a live reconcile capped an uncapped topic: $ALTERED"
case "$OUT" in *"t.cap has retention.bytes=unlimited, declared 81675540 — NOT lowered"*) ok "reported by name with the opt-in" ;; *) bad "no warning naming t.cap: $OUT" ;; esac
DEV_CLEANUP_LOWER_RETENTION_BYTES=1 run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
case "$ALTERED" in *"--entity-name t.cap"*"retention.bytes=81675540"*) ok "with DEV_CLEANUP_LOWER_RETENTION_BYTES=1 it is capped" ;; *) bad "opt-in did not cap t.cap: ${ALTERED:-<no alter>}" ;; esac

echo "3. an existing topic already at its cap is left alone, and a declared -1 that the topic inherits is not rewritten"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,retention.bytes=81675540'; desc t.unl 'cleanup.policy=delete'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
[ -z "$ALTERED" ] && ok "no alter issued (t.cap at cap, t.unl inherits -1)" || bad "altered a topic already correct: $ALTERED"

echo "4. a cap BELOW the declaration is raised; an explicit cap on a declared -1 topic is lifted; a cap ABOVE it is reported, not lowered"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,retention.bytes=21765840'; desc t.unl 'cleanup.policy=delete,retention.bytes=5'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
case "$ALTERED" in *"--entity-name t.cap"*"retention.bytes=81675540"*) ok "t.cap 21765840 -> 81675540 (raised)" ;; *) bad "t.cap not raised: ${ALTERED:-<no alter>}" ;; esac
case "$ALTERED" in *"--entity-name t.unl"*"retention.bytes=-1"*) ok "t.unl 5 -> -1 (lifted)" ;; *) bad "t.unl explicit cap not lifted: ${ALTERED:-<no alter>}" ;; esac
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,retention.bytes=999999999'; desc t.unl 'cleanup.policy=delete'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
[ -z "$ALTERED" ] && ok "t.cap above its declaration is not lowered" || bad "lowered a cap on a live reconcile: $ALTERED"
case "$OUT" in *"t.cap has retention.bytes=999999999, declared 81675540 — NOT lowered"*) ok "reported" ;; *) bad "no warning: $OUT" ;; esac

echo "5. the parser reads retention.bytes, not local.retention.bytes, among the other byte/ms keys"
D="$(desc t.cap 'cleanup.policy=delete,retention.ms=-1,segment.bytes=3600000,local.retention.bytes=7,retention.bytes=81675540,max.message.bytes=9437184'; desc t.unl 'cleanup.policy=delete,local.retention.bytes=7'; desc t.plain 'cleanup.policy=delete')"
run_case "$(printf 't.cap\nt.unl\nt.plain')" "$D"
[ -z "$ALTERED" ] && ok "t.cap read as 81675540 and t.unl (only local.retention.bytes) as inherited: nothing altered" || bad "misparsed: $ALTERED"

echo
if [ "$fails" -eq 0 ]; then echo "PASS: dev-cleanup applies declared retention.bytes caps"; exit 0; fi
echo "$fails FAILED"; exit 1
