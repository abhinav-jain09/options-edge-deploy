#!/usr/bin/env bash
# Executable proof that cleanup-topics.sh never destroys a topic it has been told to keep.
#
# TWO keep lists, two questions, one harness:
#   OPTIONS_EDGE_RESET_PRESERVED_TOPICS       -- "this data cannot be rebuilt anywhere" (durable)
#   OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS   -- "the dev clean has been told to keep this" (dev only)
# The second was added on 2026-11-10. Until then this script did not read it at all, and since every
# topic on it is also in OPTIONS_EDGE_TOPICS -- hence in approved_file -- both destructive modes
# iterated all twelve. Only two were spared, by coincidentally also being RESET-PRESERVED.
#
# This replaces counting is_durable guards. A count is a proxy: adding a fourth destructive path
# leaves the count at three and passes while the new path deletes data. This runs the REAL script
# against mocked kafka CLIs and asserts on what it actually TRIED to do, so an unguarded path fails
# here however it is written.
#
# Deliberately uses the REAL topics.env declarations rather than synthetic ones -- cleanup-topics.sh
# sources topics.env directly, which overwrites any injected topic list, and asserting on the real
# declarations is what makes this a test of the shipped configuration.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0

DURABLE_ALWAYS="options.databento.oi.anchor-manifest"  # retention=-1 in the base declaration
DURABLE_PROD_ONLY="underlying.vix.price"               # retention=-1 only when ENVIRONMENT=production
SWEPT="options.databento.raw"                          # ordinary topic; must still be cleaned

STATUS_FILE="$(mktemp)"
trap 'rm -f "$STATUS_FILE"' EXIT
run_cleanup() { # mode | environment -> the mocked CLI call log; sets LAST_STATUS
  local mode="$1" env="$2" tmp; tmp="$(mktemp -d)"
  # STATEFUL. The first version reported every topic present forever, so wait_for_topic_absent
  # burned its full retry budget on each deleted topic and the suite took 1m39s -- slow enough that
  # CI would be tempted to drop it, which is how a guard quietly stops running. A deleted topic now
  # disappears, which is also what the script is entitled to assume.
  cat > "$tmp/kafka-topics" <<'EOF'
#!/usr/bin/env bash
prev=""; topic=""
for a in "$@"; do [ "$prev" = "--topic" ] && topic="$a"; prev="$a"; done
if [[ "$*" == *--list* ]]; then printf '%s\n' ${LIST_TOPICS:-}; exit 0; fi
if [[ "$*" == *--describe* ]]; then
  [ -e "$STATE/deleted-$topic" ] && exit 0          # gone: no description, as Kafka does
  echo "Topic: $topic TopicId: ID-$topic PartitionCount: 1"; exit 0
fi
if [[ "$*" == *--delete* ]]; then
  echo "CALL $*" >> "$LOG"; : > "$STATE/deleted-$topic"; exit 0
fi
echo "CALL $*" >> "$LOG"; exit 0
EOF
  cat > "$tmp/kafka-configs" <<'EOF'
#!/usr/bin/env bash
echo "CALL $*" >> "$LOG"; exit 0
EOF
  chmod +x "$tmp/kafka-topics" "$tmp/kafka-configs"
  export LOG="$tmp/log"; : > "$LOG"
  export STATE="$tmp/state"; mkdir -p "$STATE"
  export LIST_TOPICS="${LIST_TOPICS:-}"
  TOPIC_SET="${TOPIC_SET:-}" PROTECTED_TOPIC_REGEX='^__' KAFKA_BOOTSTRAP_SERVERS="mock:9092" \
  KAFKA_CLEANUP_TOPICS=true KAFKA_CLEANUP_MODE="$mode" ENVIRONMENT="$env" \
  KAFKA_DELETE_UNWANTED_TOPICS="${DELETE_UNWANTED:-false}" ALLOW_PROD_KAFKA_CLEANUP="${ALLOW_PROD:-false}" \
  PATH="$tmp:$PATH" \
    bash "${CLEANUP_DIR:-$HERE}/cleanup-topics.sh" >/dev/null 2>&1
  printf '%s' "$?" > "$STATUS_FILE"
  cat "$LOG" 2>/dev/null || true
  rm -rf "$tmp"
}

# dev_keep_for: the OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS value topics.env yields AT a given
# ENVIRONMENT. SOURCED, not parsed, for the reason the PROD_ONLY derivation below gives at length:
# this list is appended to from inside `if [ "$ENVIRONMENT" != production ]` blocks (es.rithmic.mbo,
# futures.break-line.lines), a one-assignment regex reads only the base declaration, and
# cleanup-topics.sh sources the file -- so only sourcing resolves the value it will really see.
# The subshell keeps topics.env's hundred-odd variables out of this suite's environment.
dev_keep_for() { # environment -> one topic per line
  ( ENVIRONMENT="$1"; . "$HERE/topics.env"; printf '%s\n' ${OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS:-} )
}

# A negative assertion ("durable topic NOT deleted") is satisfied just as well by the script
# crashing on line 1. Assert the run actually succeeded before believing any of them.
assert_status() { # want-status | description
  local got; got="$(cat "$STATUS_FILE" 2>/dev/null || echo '?')"
  if [ "$got" = "$1" ]; then printf '  ok   %s (exit %s)\n' "$2" "$got"
  else printf '  FAIL %s — exit %s, want %s\n' "$2" "$got" "$1"; fail=1; fi
}

# assert() is a SUBSTRING match, which is ambiguous for a topic name that is a PREFIX of another:
# "--delete --topic underlying.vix.price" also matches a line deleting underlying.vix.price.shadow.
# The direction of the error depends on the assertion -- a survival check ("absent") reads the
# sibling's delete as its own and fails spuriously, while a deletion check ("present") is satisfied
# by a topic it never named. Neither is a claim about the topic under test. The delete call puts the
# topic LAST on the line, so anchoring at end-of-line makes the match exact.
assert_delete() { # description | log | topic | present|absent
  local desc="$1" log="$2" topic="$3" want="$4" got
  if printf '%s' "$log" | grep -qE -- "--delete --topic $(printf '%s' "$topic" | sed 's/[.[\*^$]/\\&/g')$"; then got=present; else got=absent; fi
  if [ "$got" = "$want" ]; then printf '  ok   %s\n' "$desc"
  else printf '  FAIL %s — delete of %q was %s, want %s\n' "$desc" "$topic" "$got" "$want"; fail=1; fi
}

assert() { # description | log | pattern | present|absent
  local desc="$1" log="$2" pat="$3" want="$4" got
  if printf '%s' "$log" | grep -qF -e "$pat"; then got=present; else got=absent; fi
  if [ "$got" = "$want" ]; then printf '  ok   %s\n' "$desc"
  else printf '  FAIL %s — %q was %s, want %s\n' "$desc" "$pat" "$got" "$want"; fail=1; fi
}

# Production refuses outright without ALLOW_PROD_KAFKA_CLEANUP -- assert that gate before
# testing behind it, so the durable guard is never mistaken for the only thing protecting prod.
echo "--- production refuses cleanup by default ---"
L="$(run_cleanup delete-recreate production)"
assert "nothing runs on prod without ALLOW_PROD_KAFKA_CLEANUP" "$L" "--delete" absent
assert_status 1 "production cleanup is refused, and refuses LOUDLY"

echo "--- delete-recreate, production (gate opened) ---"
L="$(ALLOW_PROD=true run_cleanup delete-recreate production)"
assert_delete "durable topic NOT deleted"      "$L" "$DURABLE_ALWAYS"    absent
assert_delete "prod-durable vix NOT deleted"   "$L" "$DURABLE_PROD_ONLY" absent
assert_delete "ordinary topic IS deleted"      "$L" "$SWEPT"             present
assert_status 0 "cleanup itself succeeded"

# THE DEV-CLEAN KEEP LIST IS DEV-SCOPED, and that is a decision, not an oversight -- so it is
# asserted, on the run above. Consulting it here would silently stop production delete-recreating
# and retention-shrinking topics it manages today, which is a behaviour change on production made
# by a fix whose entire subject is the dev clean. Production's own unrebuildable data is covered by
# OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS, which reset-preserved-topics.sh merges under its
# own predicate. Same shape as the vix assertions: a keep list that keeps everywhere is not
# environment-scoped, it is just broken.
PROD_APPROVED="$( ENVIRONMENT=production; . "$HERE/topics.env"; for e in $OPTIONS_EDGE_TOPICS; do printf '%s\n' "${e%%:*}"; done )"
PROD_PRESERVED="$(env -u RPT_TOPICS_ENV ENVIRONMENT=production bash "$HERE/reset-preserved-topics.sh")" || PROD_PRESERVED=""
PROD_STILL_MANAGED=""
for t in $(dev_keep_for production); do
  printf '%s\n' "$PROD_APPROVED"  | grep -qxF "$t" || continue   # not reached by these modes at all
  printf '%s\n' "$PROD_PRESERVED" | grep -qxF "$t" && continue   # spared on its own merits, durable
  PROD_STILL_MANAGED="$PROD_STILL_MANAGED $t"
done
if [ -z "${PROD_STILL_MANAGED// /}" ]; then
  echo "  FAIL no dev-clean-preserved topic is managed by production — the scope assertion is vacuous"; fail=1
fi
for t in $PROD_STILL_MANAGED; do
  assert_delete "production still delete-recreates dev-clean-preserved '$t'" "$L" "$t" present
done

echo "--- retention shrink, production (gate opened) ---"
L="$(ALLOW_PROD=true run_cleanup retention production)"
assert "durable topic NOT shrunk"       "$L" "--entity-name $DURABLE_ALWAYS --alter"    absent
assert "prod-durable vix NOT shrunk"    "$L" "--entity-name $DURABLE_PROD_ONLY --alter" absent
assert "ordinary topic IS shrunk"       "$L" "--entity-name $SWEPT --alter"             present
assert_status 0 "cleanup itself succeeded"

# The other half of the production scope. Asserting it only for delete-recreate left a regression that
# applied the dev-keep guard in just one branch passing this suite (Codex r1 MINOR).
for t in $PROD_STILL_MANAGED; do
  assert "production still retention-shrinks dev-clean-preserved '$t'" "$L" "--entity-name $t --alter" present
done

echo "--- dev: vix is durable on PRODUCTION only and must still be swept here ---"
L="$(run_cleanup retention dev)"
assert "durable topic NOT shrunk"       "$L" "--entity-name $DURABLE_ALWAYS --alter"    absent
assert "vix IS shrunk on dev"           "$L" "--entity-name $DURABLE_PROD_ONLY --alter" present
assert_status 0 "cleanup itself succeeded"

# THE DEV-CLEAN KEEP LIST, asserted on the run just above rather than on a run of its own: this
# suite's slowest sections are whole passes over the approved list, and a second `retention dev`
# pass would double one of them to assert something the first pass already decided.
#
# The DEFAULT mode is this one. retention.ms=1000 is not a gentler delete-recreate -- the log
# cleaner drops every segment older than a second, and apply-topics.sh restoring the declared
# retention later in the same build does not bring the records back. So the shrink loop needs the
# exemption exactly as much as the delete loop does.
echo "--- dev: the DEV-CLEAN-PRESERVED topics must survive the default retention mode ---"
DEV_KEEP="$(dev_keep_for dev)"
if [ -z "${DEV_KEEP// /}" ]; then
  echo "  FAIL dev_keep_for dev resolved nothing — every assertion below would pass vacuously"; fail=1
fi
# Vacuity guard, in the shape the reset-preserved section uses: both destructive modes iterate the
# APPROVED list, so a keep-list topic outside it is never reached and its survival proves nothing.
DEV_APPROVED="$( ENVIRONMENT=dev; . "$HERE/topics.env"; for e in $OPTIONS_EDGE_TOPICS; do printf '%s\n' "${e%%:*}"; done )"
reached_keep=0
for t in $DEV_KEEP; do
  printf '%s\n' "$DEV_APPROVED" | grep -qxF "$t" && reached_keep=$((reached_keep + 1))
done
echo "  ($(printf '%s\n' $DEV_KEEP | wc -l | tr -d ' ') dev-clean-preserved topic(s), $reached_keep of them in the approved list these modes iterate)"
if [ "$reached_keep" -eq 0 ]; then
  echo "  FAIL no dev-clean-preserved topic is approved — the exemption below is never exercised"; fail=1
fi
for t in $DEV_KEEP; do
  assert "dev retention shrink spares dev-clean-preserved '$t'" "$L" "--entity-name $t --alter" absent
done
assert "and still shrinks an ordinary approved topic" "$L" "--entity-name $SWEPT --alter" present

echo "--- dev: the DEV-CLEAN-PRESERVED topics must survive delete-recreate ---"
L="$(run_cleanup delete-recreate dev)"
for t in $DEV_KEEP; do
  assert_delete "dev delete-recreate spares dev-clean-preserved '$t'" "$L" "$t" absent
done
assert_delete "and still deletes an ordinary approved topic" "$L" "$SWEPT" present
assert_status 0 "cleanup itself succeeded"

# The unwanted sweep needs its own run: it iterates what the broker LISTS, so the keep-list topics
# have to be on the cluster for the loop to reach them. Every one of them is declared today, so the
# sweep would keep them as "approved" either way -- the exemption is ahead of that check because a
# declaration can be dropped from OPTIONS_EDGE_TOPICS at any time, and on the day it is, this is
# what stops the sweep from calling the topic junk. That is the A5 shape the reset-preserved branch
# below was added for, reached through the other keep list.
echo "--- delete-unwanted: a DEV-CLEAN-PRESERVED topic is never swept as junk on dev ---"
L="$(LIST_TOPICS="$(printf '%s ' $DEV_KEEP) $SWEPT es4.some-mm2-leftover" DELETE_UNWANTED=true \
     run_cleanup retention dev)"
for t in $DEV_KEEP; do
  assert_delete "dev sweep spares dev-clean-preserved '$t'" "$L" "$t" absent
done
assert_delete "and an undeclared topic IS still swept" "$L" "es4.some-mm2-leftover" present
assert_status 0 "cleanup itself succeeded"

# THE ASSERTIONS ABOVE DO NOT PROVE THE GUARD, and that is why this section exists (Codex r1 MAJOR).
# Every keep-list topic is also in OPTIONS_EDGE_TOPICS, so the `declared_file` branch keeps it anyway:
# delete the dev-keep branch from cleanup-topics.sh entirely and the twelve assertions above still
# pass. "The branch is reached" is not "the branch is load-bearing" -- a check satisfied by something
# other than what it names.
#
# So stage a topics.env with ONE keep-list topic removed from OPTIONS_EDGE_TOPICS while it stays on the
# mocked cluster. Nothing else can save it then: it is undeclared, it is not reset-preserved, and
# PROTECTED_TOPIC_REGEX does not match it. If the dev-keep branch goes, this topic is deleted.
echo "--- delete-unwanted: an UNDECLARED dev-clean-preserved topic must survive (the branch, isolated) ---"
# Resolved here rather than taken from the later A5 section: that section sources
# reset-preserved-topics.sh BELOW this point, and under `set -u` reading its variable early aborts the
# suite. Resolved at ENVIRONMENT=dev, which is the environment under test.
DEV_PRESERVED="$(env -u RPT_TOPICS_ENV ENVIRONMENT=dev bash "$HERE/reset-preserved-topics.sh")" || DEV_PRESERVED=""
UNDECLARED_KEEP=""
for t in $DEV_KEEP; do
  printf '%s\n' "$DEV_PRESERVED" | grep -qxF "$t" && continue   # pick one nothing ELSE protects
  UNDECLARED_KEEP="$t"; break
done
if [ -z "$UNDECLARED_KEEP" ]; then
  echo "  FAIL every dev-keep topic is also reset-preserved — this section cannot isolate the branch"; fail=1
else
  ISO_DIR="$(mktemp -d)"
  cp "$HERE/cleanup-topics.sh" "$HERE/reset-preserved-topics.sh" "$ISO_DIR/"
  # Drop it from the APPROVED declaration only; the keep list keeps naming it.
  sed -e "s/ ${UNDECLARED_KEEP}:[0-9]*//g" "$HERE/topics.env" > "$ISO_DIR/topics.env"
  # `grep -c` EXITS 1 WHEN THE COUNT IS 0, and 0 is the wanted answer for `still` -- so without the
  # `|| true` the assignment fails and `set -e` kills the suite exactly when the staging WORKED. It
  # did, on the first run: the log ended at this section's header with 68 of 303 assertions made.
  still="$( ENVIRONMENT=dev; . "$ISO_DIR/topics.env"; printf '%s\n' $OPTIONS_EDGE_TOPICS | sed 's/:[0-9]*$//' | grep -cxF "$UNDECLARED_KEEP" || true )"
  kept="$( ENVIRONMENT=dev; . "$ISO_DIR/topics.env"; printf '%s\n' $OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS | grep -cxF "$UNDECLARED_KEEP" || true )"
  if [ "$still" != "0" ] || [ "$kept" = "0" ]; then
    echo "  FAIL the staged topics.env is wrong (approved=$still want 0, kept=$kept want >=1) — the section would prove nothing"; fail=1
  else
    echo "  (isolating '$UNDECLARED_KEEP': removed from OPTIONS_EDGE_TOPICS, still on the keep list)"
    L="$(CLEANUP_DIR="$ISO_DIR" LIST_TOPICS="$UNDECLARED_KEEP $SWEPT es4.some-mm2-leftover" DELETE_UNWANTED=true \
         run_cleanup retention dev)"
    assert_delete "the keep list ALONE saves undeclared '$UNDECLARED_KEEP'" "$L" "$UNDECLARED_KEEP" absent
    assert_delete "and an undeclared NON-kept topic is still swept" "$L" "es4.some-mm2-leftover" present
    assert_status 0 "cleanup itself succeeded"
  fi
  rm -f "$ISO_DIR"/topics.env "$ISO_DIR"/cleanup-topics.sh "$ISO_DIR"/reset-preserved-topics.sh
  rmdir "$ISO_DIR" 2>/dev/null || true
fi

# THE DELETE-UNWANTED LOOP. Never exercised before: the mocked --list returned nothing, so the loop
# body was unreachable and its "is this topic declared?" question was never asked in a test.
#
# PROD_ONLY_TOPICS are declared for production ONLY, and the approved list used to be built from
# OPTIONS_EDGE_TOPICS alone -- so the one environment those topics exist on was the one environment
# that swept them as unwanted. Two of the three are pure-compact + retention.ms=-1, and the Kafka
# Cleanup stage is NOT gated on SKIP_KAFKA_TOPICS while the Kafka Topics stage that would recreate
# them IS, so a code-only redeploy with cleanup on could delete them and leave the producer's
# auto-create to stamp broker defaults over a last-value view.
echo "--- delete-unwanted: prod-only topics are DECLARED on production and must survive ---"
# DERIVED from topics.env, not hard-coded: naming two of the three would let a partial merge that
# happens to cover the named ones pass, and would stop covering a prod-only topic added later.
#
# SOURCED, not parsed. The first version matched `^OPTIONS_EDGE_PROD_ONLY_TOPICS="(.*)"$` with sed,
# which reads exactly ONE assignment form -- and topics.env routinely appends to its lists. Codex
# demonstrated the hole: appending `OPTIONS_EDGE_PROD_ONLY_TOPICS+=" review.appended.prod.only:1"`
# left cleanup-topics.sh protecting four topics while this suite still tested three and reported OK,
# which is precisely the silent narrowing the comment claimed was impossible. cleanup-topics.sh
# SOURCES this file, so only sourcing resolves the value it will actually see. The command
# substitution is its own subshell, so nothing leaks into the test's environment.
PROD_ONLY="$( . "$HERE/topics.env"; printf '%s\n' ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-} )"
PROD_ONLY="$(printf '%s\n' $PROD_ONLY | sed 's/:[0-9]*$//' | grep -v '^$')"
if [ -z "$PROD_ONLY" ]; then
  echo "  FAIL could not parse OPTIONS_EDGE_PROD_ONLY_TOPICS from topics.env — this whole section would pass vacuously"; fail=1
fi
JUNK="es4.some-mm2-leftover"                    # declared nowhere; the loop exists to remove this
echo "  (prod-only declarations under test: $(printf '%s ' $PROD_ONLY))"
L="$(LIST_TOPICS="$PROD_ONLY $SWEPT $JUNK" DELETE_UNWANTED=true ALLOW_PROD=true \
     run_cleanup retention production)"
for t in $PROD_ONLY; do
  assert_delete "prod-only '$t' NOT deleted as unwanted" "$L" "$t" absent
done
assert_delete "base-declared topic NOT deleted as unwanted"     "$L" "$SWEPT"             absent
assert_delete "genuinely undeclared topic IS deleted"           "$L" "$JUNK"              present
assert_status 0 "cleanup itself succeeded"

# The symmetric half, and the reason this cannot just approve the prod-only list everywhere: on dev
# those topics are genuinely undeclared and the sweep is CORRECT. Same shape as the vix retention
# assertion above -- a keep-list that keeps everywhere is not environment-scoped, it is just broken.
echo "--- delete-unwanted: the same topics are undeclared on dev and MUST still be swept ---"
L="$(LIST_TOPICS="$PROD_ONLY $JUNK" DELETE_UNWANTED=true \
     run_cleanup retention dev)"
for t in $PROD_ONLY; do
  assert_delete "prod-only '$t' IS deleted on dev" "$L" "$t" present
done
assert_delete "undeclared topic IS deleted on dev"         "$L" "$JUNK"              present
assert_status 0 "cleanup itself succeeded"

# The other half of the fix: prod-only topics are DECLARED (not swept as unwanted) but are NOT
# APPROVED (not managed by the destructive modes). Merging the two lists would have been the smaller
# change and would have started delete-recreating them on production -- and since the Kafka Cleanup
# stage is not gated on SKIP_KAFKA_TOPICS while the Kafka Topics stage that recreates them is, a
# code-only redeploy could leave a pure-compact retention=-1 topic absent with nothing to restore
# it. A topic that survives a flag combination today must still survive it after a fix whose whole
# purpose is to stop deleting it.
# A RESET-PRESERVED topic can be UNDECLARED: a service that creates its own outputs at startup never
# appears in OPTIONS_EDGE_TOPICS, so the unwanted sweep saw "not approved" and deleted data that by
# declaration cannot be rebuilt. Preservation is a claim about the DATA; it has to hold on every
# destructive path, not only the ones that iterate the approved list. The A5 calibration ledger is
# exactly that shape, and this asserts on EVERY reset-preserved topic rather than a hand-picked one,
# so the guard cannot rot as the list grows.
echo "--- delete-unwanted: an UNDECLARED but RESET-PRESERVED topic must survive ---"
# shellcheck source=/dev/null
. "$HERE/reset-preserved-topics.sh"
UNDECLARED_DURABLE="$(printf '%s\n' $RESET_PRESERVED_TOPICS | sort -u | tr '\n' ' ')"
L="$(LIST_TOPICS="$UNDECLARED_DURABLE $JUNK" DELETE_UNWANTED=true ALLOW_PROD=true \
     run_cleanup retention production)"
for t in $UNDECLARED_DURABLE; do
  assert_delete "reset-preserved '$t' NOT swept as unwanted" "$L" "$t" absent
done
assert_delete "and an ordinary undeclared topic IS still swept" "$L" "$JUNK" present
assert_status 0 "cleanup itself succeeded"

echo "--- prod-only topics are declared, but NOT swept by the destructive modes ---"
L="$(ALLOW_PROD=true run_cleanup delete-recreate production)"
for t in $PROD_ONLY; do
  assert_delete "prod-only '$t' NOT delete-recreated" "$L" "$t" absent
done
assert_status 0 "cleanup itself succeeded"
L="$(ALLOW_PROD=true run_cleanup retention production)"
for t in $PROD_ONLY; do
  assert "prod-only '$t' NOT retention-shrunk" "$L" "--entity-name $t --alter" absent
done
assert_status 0 "cleanup itself succeeded"

# EVERY reset-preserved topic, in BOTH destructive modes, in BOTH environments. The sections above pin one durable
# topic per mode (plus vix), and only the unwanted sweep was asserted for each preserved topic, so a topic appended to
# OPTIONS_EDGE_RESET_PRESERVED_TOPICS (the vol-premium ledgers, 2026-09-11 — four since 2026-09-13, when the
# calendar moved to Postgres) had no test showing that
# delete-recreate or the retention shrink spares it. DERIVED by EXECUTING reset-preserved-topics.sh under each
# ENVIRONMENT — the parser cleanup-topics.sh itself sources — so the list grows with the declaration and carries the
# production-only entries exactly where cleanup-topics.sh does.
echo "--- delete-recreate and retention shrink spare EVERY reset-preserved topic, dev and production ---"
APPROVED="$( . "$HERE/topics.env"; for e in $OPTIONS_EDGE_TOPICS; do printf '%s\n' "${e%%:*}"; done )"
for env_name in dev production; do
  PRESERVED_ENV="$(env -u RPT_TOPICS_ENV ENVIRONMENT="$env_name" bash "$HERE/reset-preserved-topics.sh")" || PRESERVED_ENV=""
  if [ -z "$PRESERVED_ENV" ]; then
    echo "  FAIL $env_name: reset-preserved-topics.sh resolved nothing — every assertion below would pass vacuously"; fail=1
    continue
  fi
  # These two modes iterate only the APPROVED list (OPTIONS_EDGE_TOPICS). A preserved topic outside it is never
  # reached, so its "not deleted" holds trivially; the count says how many the is_durable guard actually spares.
  reached=0
  for t in $PRESERVED_ENV; do
    if printf '%s\n' "$APPROVED" | grep -qxF "$t"; then reached=$((reached + 1)); fi
  done
  echo "  ($env_name: $(printf '%s\n' $PRESERVED_ENV | wc -l | tr -d ' ') reset-preserved topic(s), $reached of them in the approved list these modes iterate)"
  if [ "$reached" -eq 0 ]; then
    echo "  FAIL $env_name: no reset-preserved topic is approved — the guard below is never exercised"; fail=1
  fi
  L="$(ALLOW_PROD=true run_cleanup delete-recreate "$env_name")"
  for t in $PRESERVED_ENV; do
    assert_delete "$env_name delete-recreate spares reset-preserved '$t'" "$L" "$t" absent
  done
  assert_delete "$env_name delete-recreate still deletes an ordinary approved topic" "$L" "$SWEPT" present
  assert_status 0 "cleanup itself succeeded"
  L="$(ALLOW_PROD=true run_cleanup retention "$env_name")"
  for t in $PRESERVED_ENV; do
    assert "$env_name retention shrink spares reset-preserved '$t'" "$L" "--entity-name $t --alter" absent
  done
  assert "$env_name retention shrink still shrinks an ordinary approved topic" "$L" "--entity-name $SWEPT --alter" present
  assert_status 0 "cleanup itself succeeded"
done

# An exemption filter that can silently match NOTHING is worse than no filter: it reads as
# protection while deleting everything behind it. A lost or emptied declaration must therefore stop
# the run, not shrink the keep list to nothing -- the same fail-closed rule reset-preserved-topics.sh
# applies to its own declarations. Staged in a copy of the three files cleanup-topics.sh reads, so
# the shipped topics.env is never edited to test this.
echo "--- a BLANK dev-clean keep list refuses the run on dev ---"
STAGE_DIR="$(mktemp -d)"
trap 'rm -f "$STATUS_FILE" "$STAGE_DIR"/topics.env "$STAGE_DIR"/cleanup-topics.sh "$STAGE_DIR"/reset-preserved-topics.sh; rmdir "$STAGE_DIR" 2>/dev/null || true' EXIT
cp "$HERE/cleanup-topics.sh" "$HERE/reset-preserved-topics.sh" "$STAGE_DIR/"
grep -vE '^[[:space:]]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=' "$HERE/topics.env" > "$STAGE_DIR/topics.env"
# A SINGLE SPACE, not "". `[[ -z " " ]]` is FALSE, so the first version of the guard -- which tested
# the VARIABLE -- let this through, while the unquoted expansion that follows splits it into no words
# and exempted nothing (Codex r1 MAJOR). Staging the blank value tests the bypass, and an empty value
# is the weaker case it subsumes.
printf 'OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=" "\n' >> "$STAGE_DIR/topics.env"
L="$(CLEANUP_DIR="$STAGE_DIR" run_cleanup delete-recreate dev)"
assert_status 1 "refuses a whitespace-only dev-clean keep list"
assert "nothing was deleted before refusing" "$L" "--delete" absent
# The same staged tree on PRODUCTION must NOT refuse: the list is not consulted there, so demanding
# it would have made an empty dev declaration block a production cleanup for no reason.
L="$(CLEANUP_DIR="$STAGE_DIR" ALLOW_PROD=true run_cleanup delete-recreate production)"
assert_status 0 "production does not require the dev-clean keep list"
assert_delete "and production still deletes an ordinary approved topic" "$L" "$SWEPT" present

echo "--- TOPIC_SET is refused, not silently applied to the wrong cluster ---"
L="$(TOPIC_SET=es4 ALLOW_PROD=true run_cleanup retention dev)"
assert_status 1 "refuses TOPIC_SET=es4"
assert "nothing was touched"            "$L" "--alter" absent

[ $fail -eq 0 ] && echo "=== cleanup-topics-durable-test: OK ===" || { echo "=== cleanup-topics-durable-test: FAILED ==="; exit 1; }
