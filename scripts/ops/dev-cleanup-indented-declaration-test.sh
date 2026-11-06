#!/usr/bin/env bash
# topics.env declares an environment-specific topic by APPENDING inside `if [ "${ENVIRONMENT:-}" != "production" ]`,
# which makes the assignment INDENTED. Every reader that PARSES the file as text (dev-cleanup.sh's two readers and
# scripts/kafka/reset-preserved-topics.sh, which the destructive reset job and cleanup-topics.sh share) must see it.
#
# The 2026-11-06 loss this pins: all three reads were anchored at column 0 (`^OPTIONS_EDGE_...`), so an indented
# assignment was invisible AND the "exactly ONE declaration" checks still SUCCEEDED on the unindented base line.
# es.rithmic.mbo (DEV_CLEAN_PRESERVED) and futures.liquidity-validator.{evaluations,data-gaps,daily}
# (RESET_PRESERVED, under a comment promising "every reset path and the dev clean keep them") were therefore
# neither kept by the wipe nor pre-created after it; es-liquidity-engine-service then crash-looped on
# "es.rithmic.mbo must have exactly one partition, found 0" until the four topics were created by hand.
#
# dev-cleanup.sh dispatches on $MODE when sourced, so the REAL functions are cut out of it by name and run against
# a fake kafka-topics / git that record their arguments. Nothing here touches a broker or a repository.
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
RPT="${RPT_UNDER_TEST:-$HERE/../kafka/reset-preserved-topics.sh}"
TOPICS_ENV_REAL="${TOPICS_ENV_REAL:-$HERE/../kafka/topics.env}"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
[ -f "$SRC" ] || { echo "FAIL: $SRC missing" >&2; exit 1; }

fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$SRC"; }
for f in dev_clean_preserved_topics topic_desired ensure_topics create_topics_parallel kafka_list_topics \
         reconcile_declared_topics ensure_partition_only_topics ensure_dev_only_estimate_topics; do
  body="$(fn "$f")"
  [ -n "$body" ] || { echo "FAIL: function $f not found in $SRC" >&2; exit 1; }
  printf '%s\n' "$body"
done > "$WORK/functions.sh"

# A topics.env shaped like the real one: an unindented base declaration, then an environment-gated INDENTED
# append -- how es.rithmic.mbo and futures.liquidity-validator.* are declared.
cat > "$WORK/topics.env" <<'EOF'
OPTIONS_EDGE_TOPICS="t.base:1"
OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="t.base=-1"
OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="t.base"
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.base"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
OPTIONS_EDGE_COMPACTED_TOPICS=""
OPTIONS_EDGE_PURE_COMPACT_TOPICS=""
OPTIONS_EDGE_EXACT_PARTITION_TOPICS=""
OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES=""
OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES=""
OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES=""
OPTIONS_EDGE_PARTITION_ONLY_TOPICS=""
if [ "${ENVIRONMENT:-}" != "production" ]; then
  OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS t.devonly:1"
  OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES t.devonly=43200000"
  OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS t.devonly"
  OPTIONS_EDGE_RESET_PRESERVED_TOPICS="$OPTIONS_EDGE_RESET_PRESERVED_TOPICS t.devonly"
fi
EOF

# ---------- 1. dev_clean_preserved_topics: the wipe's keep-list ----------
cat > "$WORK/keep-driver.sh" <<'EOF'
set -uo pipefail
DEPLOY_REPO=/nonexistent; TOPICS_ENV_REF=ref
git() { case " $* " in *" fetch "*) return 0 ;; *" show "*) cat "$TOPICS_ENV_FILE" ;; esac; }
. "$FUNCTIONS"
dev_clean_preserved_topics
EOF
KEEP="$(TOPICS_ENV_FILE="$WORK/topics.env" FUNCTIONS="$WORK/functions.sh" bash "$WORK/keep-driver.sh" 2>/dev/null)"
case " $KEEP " in
  *" t.base "*) ok "the keep-list still has the unindented base entry" ;;
  *) bad "base keep entry lost: [$KEEP]" ;;
esac
case " $KEEP " in
  *" t.devonly "*) ok "the keep-list has the INDENTED, APPENDED entry (the 11-06 data loss)" ;;
  *) bad "appended keep entry dropped -- the wipe would delete a topic topics.env declares preserved: [$KEEP]" ;;
esac
case " $KEEP " in
  *'$OPTIONS_EDGE'*) bad "the \$VAR self-reference leaked into the keep-list as a topic name: [$KEEP]" ;;
  *) ok "the \$VAR self-reference is not mistaken for a topic name" ;;
esac
# Fails closed when the declaration is gone entirely, so an unreadable keep-list still refuses the wipe.
grep -v DEV_CLEAN_PRESERVED "$WORK/topics.env" > "$WORK/topics-nokeep.env"
if TOPICS_ENV_FILE="$WORK/topics-nokeep.env" FUNCTIONS="$WORK/functions.sh" bash "$WORK/keep-driver.sh" >/dev/null 2>&1; then
  bad "a topics.env with NO keep-list declaration returned success"
else
  ok "no keep-list declaration at all still fails closed (the wipe is refused)"
fi

# ---------- 2. ensure_topics: the shapes pre-created after a wipe ----------
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
export FAKE_DIR="$WORK/run"; mkdir -p "$FAKE_DIR"
printf '__consumer_offsets\n' > "$FAKE_DIR/list"   # a just-wiped broker still holds its system topics
: > "$FAKE_DIR/describe"; : > "$FAKE_DIR/created"; : > "$FAKE_DIR/altered"

cat > "$WORK/ensure-driver.sh" <<'EOF'
set -uo pipefail
KT="$FAKE_KT"; KC="$FAKE_KC"; BS=fake:9092
DEPLOY_REPO=/nonexistent; TOPICS_ENV_REF=ref
require_kafka_cli() { :; }
topic_count() { echo 0; }
git() { case " $* " in *" fetch "*) return 0 ;; *" show "*) cat "$TOPICS_ENV_FILE" ;; esac; }
. "$FUNCTIONS"
ensure_topics 2>&1
EOF
OUT="$(FAKE_KT="$WORK/kt" FAKE_KC="$WORK/kc" TOPICS_ENV_FILE="$WORK/topics.env" FUNCTIONS="$WORK/functions.sh" \
       bash "$WORK/ensure-driver.sh")"
CREATED="$(cat "$FAKE_DIR/created" 2>/dev/null)"
case "$CREATED" in
  *"--topic t.base "*) ok "ensure_topics still pre-creates the unindented topic" ;;
  *) bad "t.base never created; created=[$CREATED] out=[$OUT]" ;;
esac
case "$CREATED" in
  *"--topic t.devonly "*) ok "ensure_topics pre-creates the INDENTED declaration's topic" ;;
  *) bad "t.devonly never created -- this is why es-liquidity-engine-service crash-looped; created=[$CREATED]" ;;
esac
case "$CREATED" in
  *"--topic t.devonly --partitions 1 "*) ok "it is created at the DECLARED partition count" ;;
  *) bad "t.devonly not created at its declared shape; created=[$CREATED]" ;;
esac
case "$CREATED" in
  *"retention.ms=43200000"*) ok "the appended retention override is applied" ;;
  *) bad "appended retention override dropped; created=[$CREATED]" ;;
esac

# ---------- 3. the shared RESET-PRESERVED parser ----------
if [ -f "$RPT" ]; then
  RPT_OUT="$(RPT_TOPICS_ENV="$WORK/topics.env" bash "$RPT" 2>/dev/null | tr '\n' ' ')"
  case " $RPT_OUT " in
    *" t.devonly "*) ok "reset-preserved-topics.sh reads the INDENTED, APPENDED entry" ;;
    *) bad "reset-preserved-topics.sh dropped the appended entry: [$RPT_OUT]" ;;
  esac
  case " $RPT_OUT " in
    *" t.base "*) ok "reset-preserved-topics.sh still reads the base entry" ;;
    *) bad "reset-preserved-topics.sh lost the base entry: [$RPT_OUT]" ;;
  esac
  # The real file: the production list must never SHRINK because of this change.
  before=$(ENVIRONMENT=production bash "$RPT" 2>/dev/null | tr ' ' '\n' | grep -c .)
  [ "${before:-0}" -ge 36 ] && ok "the real production keep-list is not smaller than before ($before entries)" \
    || bad "the real production keep-list shrank to $before entries"
else
  bad "reset-preserved-topics.sh not found at $RPT"
fi

# ---------- 4. the risk this indent tolerance introduces, guarded ----------
# The `if` lines are not matched by these readers, so an indented declaration gated on `= "production"` would be
# applied on dev as well. topics.env has none; if one is ever added, this fails instead of silently leaking.
if [ -f "$TOPICS_ENV_REAL" ]; then
  leak="$(awk '
    /^[[:space:]]*if[[:space:]]*\[/ { gate = ($0 ~ /[^!=]=[[:space:]]*"production"/) ? "prod" : "other" }
    /^[[:space:]]*fi[[:space:]]*$/  { gate = "" }
    /^[[:space:]]+OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS|PURE_COMPACT_TOPICS|EXACT_PARTITION_TOPICS|TOPIC_RETENTION_OVERRIDES|TOPIC_DELETE_RETENTION_OVERRIDES|TOPIC_MAX_MESSAGE_BYTES_OVERRIDES|TOPIC_RETENTION_BYTES_OVERRIDES|PARTITION_ONLY_TOPICS|DEV_CLEAN_PRESERVED_TOPICS|RESET_PRESERVED_TOPICS)=/ {
      if (gate == "prod") print FILENAME ":" FNR ": " $0 }
  ' "$TOPICS_ENV_REAL")"
  [ -z "$leak" ] && ok "no indented declaration in topics.env is gated on ENVIRONMENT = production" \
    || bad "a production-gated indented declaration would now be applied on dev:
$leak"
else
  bad "topics.env not found at $TOPICS_ENV_REAL"
fi

# ---------- 5. mutation: the old column-0 read must fail these assertions ----------
old_read() { grep -E '^OPTIONS_EDGE_(TOPICS|DEV_CLEAN_PRESERVED_TOPICS)=' "$1"; }
if ( eval "$(old_read "$WORK/topics.env")"
     case " ${OPTIONS_EDGE_TOPICS:-} " in *" t.devonly:1 "*) exit 0 ;; *) exit 1 ;; esac ); then
  bad "mutation ineffective: the old column-0 read also sees the indented append"
else
  ok "mutation proves the point: the old column-0 read does NOT see the indented append"
fi

[ "$fails" = 0 ] && { echo "dev-cleanup-indented-declaration-test: OK"; exit 0; }
echo "dev-cleanup-indented-declaration-test: $fails failure(s)"; exit 1
