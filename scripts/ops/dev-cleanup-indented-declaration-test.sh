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
for f in topics_ref_pin dev_clean_preserved_topics topic_desired ensure_topics create_topics_parallel kafka_list_topics \
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
# rev-parse too, since 2026-11-06: dev_clean_preserved_topics now pins the ref to a commit id through
# topics_ref_pin before reading it, so a fake that answers only fetch and show makes the pin fail and
# every assertion below vacuous. A real-looking 40-hex id, because the pin verifies the shape.
git() { case " $* " in
  *" fetch "*)     return 0 ;;
  *" rev-parse "*) echo 0123456789abcdef0123456789abcdef01234567 ;;
  *" show "*)      cat "$TOPICS_ENV_FILE" ;;
esac; }
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
# rev-parse too, since 2026-11-06: dev_clean_preserved_topics now pins the ref to a commit id through
# topics_ref_pin before reading it, so a fake that answers only fetch and show makes the pin fail and
# every assertion below vacuous. A real-looking 40-hex id, because the pin verifies the shape.
git() { case " $* " in
  *" fetch "*)     return 0 ;;
  *" rev-parse "*) echo 0123456789abcdef0123456789abcdef01234567 ;;
  *" show "*)      cat "$TOPICS_ENV_FILE" ;;
esac; }
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
  # THE REAL FILE, by NAME not by count. A count proves nothing: 37 -> 36 would pass a `>= 36` threshold, and
  # even an equal count could swap one protected topic for another. So: every topic the OLD column-0 read
  # produced must still be produced, in BOTH environments. The old read is recomputed here from the real file,
  # so this assertion keeps working as topics.env grows and needs no hand-maintained baseline.
  old_read_list() { # <env> -> what the pre-2026-11-06 reader produced, one topic per line
    local e="$1" base prod
    base=$(sed -nE 's/^OPTIONS_EDGE_RESET_PRESERVED_TOPICS="(.*)"$/\1/p' "$TOPICS_ENV_REAL")
    prod=$(sed -nE 's/^OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="(.*)"$/\1/p' "$TOPICS_ENV_REAL")
    if [ "$e" = production ]; then printf '%s %s\n' "$base" "$prod"; else printf '%s\n' "$base"; fi
    }
  for e in dev production; do
    ENVIRONMENT="$e" bash "$RPT" 2>/dev/null | tr ' ' '\n' | grep -v '^$' | sort -u > "$WORK/new.$e"
    old_read_list "$e" | tr ' ' '\n' | grep -v '^$' | sort -u > "$WORK/old.$e"
    lost="$(comm -23 "$WORK/old.$e" "$WORK/new.$e")"
    [ -z "$lost" ] \
      && ok "real topics.env, ENVIRONMENT=$e: every topic the old reader kept is still kept ($(grep -c . < "$WORK/new.$e") now, $(grep -c . < "$WORK/old.$e") before)" \
      || bad "real topics.env, ENVIRONMENT=$e: the keep-list LOST $(printf '%s' "$lost" | grep -c .) topic(s):
$lost"
  done
  # And the four topics the 2026-11-06 clean actually destroyed must be declared preserved, by name: "at least
  # one assignment exists" cannot notice an APPENDED declaration being deleted or renamed, so the members that
  # loss cost are pinned here.
  for t in futures.liquidity-validator.evaluations futures.liquidity-validator.data-gaps futures.liquidity-validator.daily; do
    grep -q -F -- "$t" "$WORK/new.dev" \
      && ok "RESET-PRESERVED still names $t" \
      || bad "$t is no longer in the reset-preserved list -- the 2026-11-06 loss would repeat"
  done
  grep -qE '^[[:space:]]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=.*es\.rithmic\.mbo' "$TOPICS_ENV_REAL" \
    && ok "topics.env still declares es.rithmic.mbo DEV_CLEAN_PRESERVED" \
    || bad "es.rithmic.mbo is no longer declared DEV_CLEAN_PRESERVED -- the 2026-11-06 loss would repeat"
else
  bad "reset-preserved-topics.sh not found at $RPT"
fi

# ---------- 4. the risk this indent tolerance introduces, guarded BY WHITELIST ----------
# These readers parse topics.env, they do not interpret it: the `if` lines are not matched, so an indented
# declaration is applied whatever its gate. A declaration inside `if ... = "production"` would reach the dev
# clean, and an indented REPLACEMENT assignment (one that does not read the variable it sets) could even SHRINK
# a list. No regex can decide that safely, so this does not try to detect the bad shapes - it WHITELISTS the
# good one and fails on everything else:
#   * the enclosing line must be one of the two gate forms topics.env uses for a dev-only block, matched whole;
#   * the declaration must sit DIRECTLY inside it - any else/elif, or a nested if/case/while/for in between,
#     fails;
#   * it must APPEND (read the variable it assigns), so it can only grow a list, never replace one.
# elif, else, nesting, `[[`, `test`, a continued-line gate, an unknown gate and a replacement assignment all
# fail this, because none of them is the whitelisted shape. It is a convention check, not a shell parser, and
# that is exactly what makes it fail closed.
if [ -f "$TOPICS_ENV_REAL" ]; then
  leak="$(awk '
    # The ONLY accepted gates, matched as whole lines (leading blanks allowed).
    function known_gate(l) {
      return l ~ /^[[:space:]]*if[[:space:]]+\[[[:space:]]+"\$\{ENVIRONMENT:-\}"[[:space:]]+!=[[:space:]]+"production"[[:space:]]+\][[:space:]]*;[[:space:]]*then[[:space:]]*$/ ||
             l ~ /^[[:space:]]*if[[:space:]]+\[[[:space:]]+"\$\{ENVIRONMENT:-dev\}"[[:space:]]+=[[:space:]]+"dev"[[:space:]]+\][[:space:]]*;[[:space:]]*then[[:space:]]*$/
    }
    # The accepted assignment, as a WHOLE-LINE grammar rather than "a self-reference appears somewhere":
    #   VAR="$VAR tok tok ..."   (or "${VAR} ...") and nothing else on the line.
    # A self-reference test alone accepts VAR="${VAR% tok}" (which SHRINKS the list), a single-quoted
    # assignment (no expansion at all, so the literal text becomes the value) and
    # VAR="$VAR a"; VAR="replaced" (a replacement hidden after a valid-looking append on the same line).
    # Within the declaration convention this file uses, this shape can only ADD tokens to a list.
    # The token charset is this project convention, not Kafka topic syntax: every topic name, shape (name:1)
    # and override (name=43200000) in topics.env today fits it. A future name needing another character has to
    # widen it here - deliberately, rather than being waved through.
    # NOTE: no apostrophe may appear anywhere in this awk program - it is single-quoted in the shell, and one
    # stray quote closes it early and silently leaves awk with a truncated program that flags nothing.
    function append_ok(l, var,   re) {
      re = "^[[:space:]]*" var "=\"\\$(" var "|\\{" var "\\})([[:space:]]+[A-Za-z0-9._:=-]+)+\"[[:space:]]*$"
      return l ~ re
    }
    # NO depth counting, and no list of structural forms to keep complete. Both were holes: counting missed
    # subshells and brace groups, then drifted on the real 1900-line file and refused the very declarations this
    # fix restores; a nearest-structural-line rule still let a here-doc body or a multiline quoted string sit
    # between the gate and the declaration.
    #
    # So the window is a WHITELIST too: a whitelisted gate opens it, and the ONLY lines allowed to follow before
    # a declaration are a comment, a blank line, or another accepted declaration. ANY other line - a here-doc
    # body, a continuation, a nested opener, an else, a case label, ordinary code - closes the window, and a
    # declaration after it is refused. Nothing has to be recognised as structural for that to hold: what is not
    # explicitly permitted ends the window.
    #
    # LIMIT, stated rather than claimed away: this is a convention check over the simple declaration style
    # topics.env uses, not a shell parser. It cannot prove what `eval` will do with a file written in some other
    # style - it can only refuse to let that style through unnoticed.
    { if ($0 ~ /^[[:space:]]*#/ || $0 ~ /^[[:space:]]*$/) next }            # comment / blank: window survives
    known_gate($0) { gate = $0; gate_line = FNR; next }                      # a whitelisted gate opens it
    # EVERY declaration the readers can consume, indented or NOT: their own anchor is `[[:space:]]*`, so a
    # declaration at column 0 inside a gate is read exactly like an indented one. Checking only indented lines
    # left that shape unreported - it closed the window silently and was still applied by the readers.
    /^[[:space:]]*OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS|PURE_COMPACT_TOPICS|EXACT_PARTITION_TOPICS|TOPIC_RETENTION_OVERRIDES|TOPIC_DELETE_RETENTION_OVERRIDES|TOPIC_MAX_MESSAGE_BYTES_OVERRIDES|TOPIC_RETENTION_BYTES_OVERRIDES|PARTITION_ONLY_TOPICS|DEV_CLEAN_PRESERVED_TOPICS|RESET_PRESERVED_TOPICS|RESET_REBUILDABLE_TOPICS)=/ {
      split($0, p, "=")
      var = p[1]; sub(/^[[:space:]]+/, "", var)
      indented = ($0 ~ /^[[:space:]]+/)
      if (gate == "") {
        # No gate open. An unindented line here is an ordinary top-level declaration, which every environment
        # reads and which needs no gate; an indented one is a conditional declaration whose gate is not one we
        # accept, or is not there at all.
        if (indented) { print FNR ": REFUSED no whitelisted dev gate is open here: " $0 }
        next
      }
      if (!indented)           { print FNR ": REFUSED declaration inside a dev gate is at column 0, so it reads as unconditional: " $0; gate = ""; next }
      if (!append_ok($0, var)) { print FNR ": REFUSED not a plain " var "=\"$" var " tokens\" append: " $0; gate = ""; next }
      next                                                                   # an accepted declaration keeps the window open
    }
    { gate = "" }                                                            # anything else closes it
  ' "$TOPICS_ENV_REAL")"
  # THE SEMANTIC HALF, and the one that does not depend on recognising shell shapes at all. Ask BASH what the
  # file assigns in each environment, and compare that, PER VARIABLE, with what each reader of that variable
  # sees:
  #   LEAKED  = a (variable, token) pair a reader sees that bash assigns to THAT variable only under
  #             ENVIRONMENT=production -> a production-only declaration the dev clean would act on. This is the
  #             hazard itself, caught whatever shape hides it: a column-0 declaration inside a production gate,
  #             an elif, a case branch - none of which a pattern rule reliably sees.
  #   UNREAL  = a pair a reader sees that bash assigns to that variable in NEITHER environment -> text that only
  #             looks like a declaration, e.g. a line inside a here-doc or a quoted string.
  # PER VARIABLE, not one pooled set: pooling let a production-only token in one list be excused by the same
  # token appearing in another list on dev, and it could not say which reader would act on it.
  # Each variable is compared against the reader that really consumes it: the nine topic/config lists through
  # the eval ensure_topics does, and the two keep-lists through their own concatenating text parse. What is
  # modelled is each reader EXTRACTION, not its fail-closed validation (a missing declaration, an empty value,
  # the reset parser checking the prod-only list): those can only make a reader REFUSE, never make it act on a
  # declaration this check passed. The
  # production-only reset list (OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS) is deliberately absent - no
  # dev-cleanup reader consumes it, and this check is about the dev readers.
  # The convention check above is still worth having: it refuses styles that bash and the readers AGREE on but
  # that nobody should write (a replacement or a ${VAR%...} shrink inside a dev gate).
  EVAL_VARS='OPTIONS_EDGE_TOPICS OPTIONS_EDGE_COMPACTED_TOPICS OPTIONS_EDGE_PURE_COMPACT_TOPICS OPTIONS_EDGE_EXACT_PARTITION_TOPICS OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES OPTIONS_EDGE_PARTITION_ONLY_TOPICS'
  TEXT_VARS='OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS OPTIONS_EDGE_RESET_PRESERVED_TOPICS'

  # What BASH assigns. A source that FAILS fails this test: topics.env carries its own production guard that
  # ends sourcing with a non-zero status when its token strip matches nothing, and swallowing that would leave
  # an EMPTY production set, which reads as "nothing is production-only" - a false pass of exactly this check.
  env_pairs() { # <ENVIRONMENT value> -> "VAR token" per line
    ( set +u
      ENVIRONMENT="$1"
      . "$TOPICS_ENV_REAL" >/dev/null 2>&1 || exit 1
      for v in $EVAL_VARS $TEXT_VARS; do
        for t in ${!v}; do printf '%s %s\n' "$v" "$t"; done
      done
    ) | sort -u
  }
  # What the READERS see, each through its own reader's semantics.
  reader_pairs() {
    ( set +u
      lines="$(grep -E "^[[:space:]]*($(printf '%s' "$EVAL_VARS" | tr ' ' '|'))=" "$TOPICS_ENV_REAL")" || exit 1
      eval "$lines" || exit 1                       # ensure_topics' model: eval the extracted lines in file order
      for v in $EVAL_VARS; do
        for t in ${!v}; do printf '%s %s\n' "$v" "$t"; done
      done
      for v in $TEXT_VARS; do                       # the keep-list readers' model: concatenate every assignment
        val="$(sed -nE "s/^[[:space:]]*${v}=\"(.*)\"$/\1/p" "$TOPICS_ENV_REAL" \
               | sed -e "s/\\\$${v}//g" -e "s/\\\${${v}}//g" | tr '\n' ' ')" || exit 1
        for t in $val; do printf '%s %s\n' "$v" "$t"; done
      done
    ) | sort -u
  }
  semantic_error=""
  env_pairs dev        > "$WORK/pairs.dev"        || semantic_error="sourcing topics.env with ENVIRONMENT=dev FAILED"
  env_pairs production > "$WORK/pairs.prod"       || semantic_error="sourcing topics.env with ENVIRONMENT=production FAILED (its own guard may have fired)"
  reader_pairs         > "$WORK/pairs.readers"    || semantic_error="extracting/evaluating the readers' view of topics.env FAILED"
  leaked=""; unreal=""
  if [ -z "$semantic_error" ]; then
    comm -23 "$WORK/pairs.prod" "$WORK/pairs.dev" > "$WORK/pairs.prodonly"
    leaked="$(comm -12 "$WORK/pairs.readers" "$WORK/pairs.prodonly")"
    sort -u "$WORK/pairs.dev" "$WORK/pairs.prod" > "$WORK/pairs.any"
    unreal="$(comm -23 "$WORK/pairs.readers" "$WORK/pairs.any")"
  fi

  if [ -z "$leak" ] && [ -z "$leaked" ] && [ -z "$unreal" ] && [ -z "$semantic_error" ]; then
    ok "topics.env declaration check: clean ($(grep -c . < "$WORK/pairs.readers") reader-visible declarations, none production-only, none unreal; convention held)"
  else
    bad "topics.env declaration check: REFUSED
${semantic_error:+could not measure: $semantic_error}
convention:
${leak:-  (none)}
production-only (variable, token) pairs the dev readers would act on:
${leaked:-  (none)}
pairs the readers see that bash assigns in no environment:
${unreal:-  (none)}"
  fi
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
