#!/usr/bin/env bash
# validate-durable-topic-preservation.sh — CI invariant.
#
# Every topic declared retention.ms=-1 in scripts/kafka/topics.env must be CLASSIFIED into exactly
# one of:
#
#   OPTIONS_EDGE_RESET_PRESERVED_TOPICS   — cannot be rebuilt; BOTH destructive reset paths must
#     skip it, and cleanup-topics.sh reads this same list
#   OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS — the producing service reconstructs it; the resets may
#     destroy it freely
#
# WHY THE CLASSIFICATION IS EXPLICIT. Until 2026-08-03 this validator DERIVED preservation from
# retention.ms=-1. That conflated two different statements:
#
#   retention.ms=-1 = "do not age this out by time"  (a Kafka storage knob)
#   reset-preserved = "this cannot be reconstructed" (a claim about the data)
#
# options.spx.spot-vol-regime.current is the case that separates them. It is a single-key ("SPX")
# latest-regime view, so the 24h delete half destroyed the one record it exists to serve over any
# gap longer than a day — every weekend. It needs retention=-1. It must NOT be reset-preserved: the
# service republishes it seconds after starting. Under the derived rule it would have been
# force-preserved on dev.
#
# Requiring EXACTLY ONE classification is what keeps the explicit lists honest. A derived rule
# cannot miss a topic; an explicit list can, so a -1 topic in neither list — or in both — FAILS.
#
# WHY AT CI, NOT RUNTIME: the live broker is useless as the source of durability here — measured
# 2026-07-28, ~200 prod topics report retention.ms=-1 because the broker default is unlimited (the
# nightly resets ARE the retention mechanism). The intent-level declaration lives in topics.env.
# This is the failure class behind the 2026-07-28 incident (spx.basis.state wiped every pre-market,
# ES->SPX basis cold-started every morning, mapping dark until ~10:00 ET).
#
# topics.env is PARSED, not sourced (no variable collisions with this validator).
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPICS_ENV="scripts/kafka/topics.env"
PREMARKET="scripts/ops/premarket-reset.sh"
CLEANSLATE="scripts/ops/offhours-clean-slate.sh"
fail=0

# All "<topic>=-1" entries across every retention-overrides declaration, without
# sourcing the file: pull the quoted value of each *_TOPIC_RETENTION_OVERRIDES
# assignment, split on whitespace, keep the =-1 pairs.
# The RESET-PRESERVED declaration, not everything at retention=-1. Those are different claims --
# "do not age this out by time" versus "this cannot be rebuilt" -- and the previous rule conflated
# them, which would have force-preserved options.spx.spot-vol-regime.current on dev the moment it
# got the retention=-1 it genuinely needs.
list_of() { # variable-name-suffix -> whitespace-split values
  { sed -nE "s/^OPTIONS_EDGE_(PROD_ONLY_)?$1=\"(.*)\"$/\\2/p" "$TOPICS_ENV" | tr ' ' '\n' | grep -v '^$' || true; } | sort -u
}
PRESERVED=$(list_of RESET_PRESERVED_TOPICS)
REBUILDABLE=$(list_of RESET_REBUILDABLE_TOPICS)
# [A-Z0-9_]*, NOT [A-Z_]*. Without the digits this skipped every OPTIONS_EDGE_ES4_* declaration, so
# es.options.indicators.bootstrap.control — declared retention.ms=-1 only in the es4 overrides — was
# invisible to this validator and the classify-or-fail rule never applied to it. Found by adding the
# extraction cross-check below, which is the whole argument for having one.
MINUS_ONE=$( { sed -nE 's/^[A-Z0-9_]*TOPIC_RETENTION_OVERRIDES="(.*)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | grep -- '=-1$' || true; } | cut -d= -f1 | sort -u)

in_list() { case " $(printf '%s' "$2" | tr '\n' ' ') " in *" $1 "*) return 0 ;; esac; return 1; }

# THE EXTRACTION MUST BE CHECKED BEFORE ANY TOPIC IS BLAMED.
# A reviewer saw this validator fail once on es.futures.footprint.bars — "declared RESET-REBUILDABLE
# but has no retention.ms=-1 override" — and pass on an immediate rerun. The topic is declared
# correctly in every list; what went wrong was that MINUS_ONE came back without it, and the validator
# reported that as a fact about topics.env rather than as a failure to read topics.env. I could not
# reproduce it (0 failures in several hundred direct probes of the predicate and in repeated full
# runs), so I am not claiming a root cause. What is certain is the failure MODE: a guard that cannot
# tell "this declaration is wrong" from "I did not read the declaration" accuses the wrong thing, and
# the reader's only recourse is to rerun it — which trains everyone to rerun a red guard.
#
# So each extracted list is cross-checked against an independently-computed count before use. A
# mismatch is a HARNESS failure, said in those words, and nothing downstream runs.
# Independently computed by a DIFFERENT ENGINE, scoped identically to list_of/MINUS_ONE. Two shell
# pipelines drift on regex dialects and quoting — a first attempt counted "retention.ms=-1" out of the
# prose, a second lost the first entry on each line to the opening quote, and both would have failed
# this validator on a perfectly correct file. The cross-check exists only to catch a TRUNCATED or
# PARTIAL read, so it must share no code with the thing it checks.
#
# IT COMPARES COUNTS, NOT EMPTINESS. A non-emptiness rule looked right and was wrong: deleting every
# RESET-PRESERVED declaration is the ATTACK this validator exists to catch, and an empty-list rule
# reported it as a harness problem under a different exit code — hiding the finding behind a
# complaint about the reader. Comparing counts tells a legitimately empty declaration (both agree on
# zero, and the real checks below run) apart from a read that lost entries (they disagree).
_recount() { # _recount <declaration-suffix|MINUS_ONE>
  python3 - "$TOPICS_ENV" "$1" <<'PYEOF'
import re, sys
s, which = open(sys.argv[1]).read(), sys.argv[2]
out = set()
if which == "MINUS_ONE":
    for m in re.finditer(r'^[A-Z0-9_]*TOPIC_RETENTION_OVERRIDES="(.*)"$', s, re.M):
        for tok in m.group(1).split():
            if tok.endswith("=-1"):
                out.add(tok.split("=")[0])
else:
    for m in re.finditer(r'^OPTIONS_EDGE_(?:PROD_ONLY_)?%s="(.*)"$' % re.escape(which), s, re.M):
        out.update(m.group(1).split())
print(len(out))
PYEOF
}
_check_extraction() { # name | extracted-list | independent-count
  local name="$1" got want
  # `grep -c .` EXITS 1 when the count is zero, and `set -e` then killed this script silently at this
  # very line — in exactly the case the check exists to report. A check that cannot fire in its own
  # case is the shape this branch keeps finding; my own neuter caught it here.
  got="$(printf '%s\n' "$2" | grep -c . || true)"; got="${got:-0}"
  want="$3"
  if [ "$got" -ne "$want" ]; then
    echo "HARNESS FAILURE: $name extracted $got entries but an independent read of $TOPICS_ENV finds" >&2
    echo "                 $want. The parse is incomplete, so any per-topic verdict below would name" >&2
    echo "                 the wrong topic. This says nothing about the declarations themselves." >&2
    exit 3
  fi
}

PRESERVED=$(list_of RESET_PRESERVED_TOPICS)
REBUILDABLE=$(list_of RESET_REBUILDABLE_TOPICS)
# [A-Z0-9_]*, NOT [A-Z_]*. Without the digits this skipped every OPTIONS_EDGE_ES4_* declaration, so
# es.options.indicators.bootstrap.control — declared retention.ms=-1 only in the es4 overrides — was
# invisible to this validator and the classify-or-fail rule never applied to it. Found by adding the
# extraction cross-check below, which is the whole argument for having one.
MINUS_ONE=$( { sed -nE 's/^[A-Z0-9_]*TOPIC_RETENTION_OVERRIDES="(.*)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | grep -- '=-1$' || true; } | cut -d= -f1 | sort -u)

in_list() { case " $(printf '%s' "$2" | tr '\n' ' ') " in *" $1 "*) return 0 ;; esac; return 1; }

# THE EXTRACTION MUST BE CHECKED BEFORE ANY TOPIC IS BLAMED.
# A reviewer saw this validator fail once on es.futures.footprint.bars — "declared RESET-REBUILDABLE
# but has no retention.ms=-1 override" — and pass on an immediate rerun. The topic is declared
# correctly in every list; what went wrong was that MINUS_ONE came back without it, and the validator
# reported that as a fact about topics.env rather than as a failure to read topics.env. I could not
# reproduce it (0 failures in several hundred direct probes of the predicate and in repeated full
# runs), so I am not claiming a root cause. What is certain is the failure MODE: a guard that cannot
# tell "this declaration is wrong" from "I did not read the declaration" accuses the wrong thing, and
# the reader's only recourse is to rerun it — which trains everyone to rerun a red guard.
#
# So each extracted list is cross-checked against an independently-computed count before use. A
# mismatch is a HARNESS failure, said in those words, and nothing downstream runs.
# Independently computed by a DIFFERENT ENGINE, scoped identically. Two shell pipelines drift on
# regex dialects and quoting — a first attempt counted "retention.ms=-1" out of the prose, and a

_check_extraction "PRESERVED"   "$PRESERVED"   "$(_recount RESET_PRESERVED_TOPICS)"
_check_extraction "REBUILDABLE"  "$REBUILDABLE"  "$(_recount RESET_REBUILDABLE_TOPICS)"
_check_extraction "MINUS_ONE"    "$MINUS_ONE"    "$(_recount MINUS_ONE)"

# Pre-existing and deliberately NOT classified here: it became visible only when the ES4 declarations
# stopped being skipped (BZ 362), and classifying it is a claim about a service on es4 that I have not
# verified. Its prod twin's stated requirement (topics.env:187) reads as though it applies, but this
# file is not the place for "probably". RESET_REBUILDABLE drives no destructive script, so the cost of
# the delay is a missing sentence rather than data.
UNCLASSIFIED_EXEMPT="es.options.indicators.bootstrap.control"   # BZ 362

# EXACTLY ONE classification. Neither list is allowed to be the default.
for topic in $MINUS_ONE; do
  if in_list "$topic" "$(printf '%s\n' $UNCLASSIFIED_EXEMPT)"; then
    echo "note: '$topic' is retention.ms=-1 and unclassified — known, exempt, tracked as BZ 362"
    continue
  fi
  p=no; r=no
  in_list "$topic" "$PRESERVED"   && p=yes
  in_list "$topic" "$REBUILDABLE" && r=yes
  if [ "$p" = no ] && [ "$r" = no ]; then
    echo "FAIL: '$topic' is declared retention.ms=-1 but appears in NEITHER"
    echo "      OPTIONS_EDGE_RESET_PRESERVED_TOPICS nor OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS."
    echo "      Decide, on the record, whether losing it to a reset costs anything."
    fail=1
  elif [ "$p" = yes ] && [ "$r" = yes ]; then
    echo "FAIL: '$topic' is declared BOTH reset-preserved and reset-rebuildable."
    fail=1
  fi
done

# Preservation without retention=-1 is pointless: the resets would spare the topic and time
# retention would delete it anyway. (The converse is the conflation this design removed.)
for topic in $PRESERVED; do
  if ! in_list "$topic" "$MINUS_ONE"; then
    echo "FAIL: '$topic' is declared RESET-PRESERVED but has no retention.ms=-1 override — the"
    echo "      resets would spare it and time retention would delete it anyway."
    fail=1
  fi
done

# Symmetric, so neither list becomes a dumping ground: classifying a topic that has no -1 override
# says nothing about anything.
for topic in $REBUILDABLE; do
  if ! in_list "$topic" "$MINUS_ONE"; then
    echo "FAIL: '$topic' is declared RESET-REBUILDABLE but has no retention.ms=-1 override —"
    echo "      the classification only means something for topics that carry one."
    fail=1
  fi
done

# A DECLARATION THAT NOTHING APPLIES IS NOT A DECLARATION.
# apply-topics.sh reaches alter_topic_config() exclusively from `for entry in $OPTIONS_EDGE_TOPICS`
# (scripts/kafka/apply-topics.sh:295 -> 372). So "<topic>=-1" for a topic that appears in no *_TOPICS
# list is inert: the topic's retention is whatever the creating service happened to stamp, and the
# line in this file only makes it look governed. That is how context-tape.direction.ledger — the A5
# calibration corpus in transit — carried retention.ms=-1 here while nothing on the deploy path ever
# applied it (BZ 360). The same reasoning binds the partition contract: an undeclared topic cannot be
# in OPTIONS_EDGE_EXACT_PARTITION_TOPICS in any way that runs.
#
# Digits matter in this pattern: [A-Z_]* silently skips every OPTIONS_EDGE_ES4_* list, which made a
# first draft of this check report five false positives. [A-Z0-9_]*.
DECLARED=$( { sed -nE 's/^[A-Z0-9_]*TOPICS[A-Z0-9_]*="(.*)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | grep -E '^[^:[:space:]]+:[0-9]+$' || true; } | sed 's/:[0-9]*$//' | sort -u)

# Pre-existing, and deliberately NOT fixed here: both are live with cleanup.policy=compact and are
# classified in NEITHER the compacted nor the uncompacted lists, so declaring them would make
# apply-topics restamp them 'delete' — changing someone else's topic while claiming to fix a
# retention line. Classify their compaction first, then declare them and delete this exemption.
UNDECLARED_EXEMPT="es.drop.final-summary es.drop.outcome"   # BZ 361
for topic in $PRESERVED; do
  in_list "$topic" "$DECLARED" && continue
  in_list "$topic" "$(printf '%s\n' $UNDECLARED_EXEMPT)" && {
    echo "note: '$topic' is durable but undeclared — known, exempt, tracked as BZ 361"
    continue
  }
  echo "FAIL: '$topic' is declared RESET-PRESERVED with a retention override, but appears in no"
  echo "      *_TOPICS declaration. apply-topics.sh only ever configures topics it iterates, so the"
  echo "      override is INERT — the topic keeps whatever its creator stamped. Declare it as"
  echo "      '<topic>:<partitions>' (use its LIVE partition count) so the deploy path applies it."
  fail=1
done

# NOT an early exit. An empty PRESERVED list used to short-circuit to OK while every preserve arm
# stayed wired in the reset scripts, so deleting the declarations passed the gate.
DURABLE="$PRESERVED"


# The regex premarket-reset actually uses (first single-quoted assignment; the
# `|| true` keeps set -e from eating the diagnostic when it is missing).
REGEX=$( { grep -m1 "^PRESERVE_TOPICS_REGEX=" "$PREMARKET" || true; } | sed "s/^PRESERVE_TOPICS_REGEX='//; s/'$//")
if [ -z "$REGEX" ]; then
  echo "FAIL: PRESERVE_TOPICS_REGEX not found (or not a single-quoted assignment) in $PREMARKET"
  exit 1
fi

# Line number of the clean-slate purge wildcard arm — every preserve arm must
# precede it or it never matches. The wildcard line is the one filling
# PURGE_TOPICS from "*)".

for t in $DURABLE; do
  if ! printf '%s\n' "$t" | grep -qE "$REGEX"; then
    echo "FAIL: '$t' is declared RESET-PRESERVED but is NOT matched by"
    echo "      PRESERVE_TOPICS_REGEX in $PREMARKET — the pre-market reset would wipe it."
    fail=1
  fi
done

# The two reset scripts are not the only destructive paths. cleanup-topics.sh runs from the
# monolithic pipeline with KAFKA_CLEANUP_MODE defaulting to delete-recreate, and BOTH its modes
# iterate the APPROVED topic list -- where every durable topic also appears. PROTECTED_TOPIC_REGEX
# does not save them: it is consulted only for UNWANTED topics. A durable declaration that survives
# the resets and is then deleted by cleanup is not durable, so assert the exemption exists here too.
CLEANUP="scripts/kafka/cleanup-topics.sh"
if [ ! -f "$CLEANUP" ]; then
  echo "FAIL: $CLEANUP not found — layout changed, update this validator"
  exit 1
fi
# Counting is_durable guards was a proxy and a bad one: a fourth destructive path leaves the count
# at three and passes while it deletes data. Run the executable test instead -- it drives the real
# script with mocked kafka CLIs and asserts on the calls it actually makes, so an unguarded path
# fails however it is written.
# A FIXED scratch name here meant two concurrent runs of this validator wrote each other's
# diagnostics — which is how a reviewer saw a baseline "failure" that no run actually produced.
_scratch="$(mktemp -d)"; trap 'rm -rf "$_scratch"' EXIT
CLEANUP_TEST="scripts/kafka/cleanup-topics-durable-test.sh"
if [ ! -x "$CLEANUP_TEST" ]; then
  echo "FAIL: $CLEANUP_TEST missing or not executable — the durability guarantee has no test"
  fail=1
elif ! bash "$CLEANUP_TEST" > "$_scratch/cleanup.out" 2>&1; then
  echo "FAIL: $CLEANUP_TEST — cleanup-topics.sh does not preserve durable topics:"
  sed 's/^/      /' "$_scratch/cleanup.out"
  fail=1
fi

# The provisioning path is the OTHER way a durable topic dies, and it is the one that opened when the
# A5 ledger was declared: apply-topics.sh contains a delete+recreate repair, and declaring a topic is
# what puts it inside that machinery. Reading the branch and concluding "1 == 1 cannot fire" is a
# belief; this drives the real script with mocked kafka CLIs and asserts on the calls it makes.
# Removing the ledger from NEVER_RECREATE makes it delete the corpus topic, which is the point.
LEDGER_TEST="scripts/kafka/apply-topics-ledger-safety-test.sh"
if [ ! -x "$LEDGER_TEST" ]; then
  echo "FAIL: $LEDGER_TEST missing or not executable — nothing proves apply-topics cannot destroy the corpus"
  fail=1
elif ! bash "$LEDGER_TEST" > "$_scratch/ledger.out" 2>&1; then
  echo "FAIL: $LEDGER_TEST — apply-topics.sh can reach the calibration ledger destructively:"
  sed 's/^/      /' "$_scratch/ledger.out"
  fail=1
fi

# The six vol-premium Gate-1 topics: four are kept forever (RESET-PRESERVED, NEVER-RECREATE, retention.bytes=-1),
# two are rebuildable. (The calendar ledger is a Postgres table since 2026-09-13, not a seventh topic.) This drives
# the real apply-topics.sh and cleanup-topics.sh over creation, config reconcile,
# partition drift (recreation off and on) and all three cleanup modes, for dev and production, and proves itself
# sensitive to every membership that protects them by mutating a COPY of topics.env.
VOL_PREMIUM_TEST="scripts/kafka/apply-topics-vol-premium-safety-test.sh"
if [ ! -x "$VOL_PREMIUM_TEST" ]; then
  echo "FAIL: $VOL_PREMIUM_TEST missing or not executable — nothing proves the vol-premium ledgers survive the deploy path"
  fail=1
elif ! bash "$VOL_PREMIUM_TEST" > "$_scratch/vol-premium.out" 2>&1; then
  echo "FAIL: $VOL_PREMIUM_TEST — apply-topics.sh or cleanup-topics.sh does not honour the vol-premium declarations:"
  grep -E '^  FAIL|^===' "$_scratch/vol-premium.out" | sed 's/^/      /'
  fail=1
fi

# The REVERSE direction. An explicit list has one weakness the old retention=-1 rule did not: it
# cannot notice a topic somebody FORGOT to declare. So require the converse -- every topic that the
# reset scripts already go out of their way to preserve must be declared here. Someone who adds a
# preserve arm without declaring it, or deletes a declaration while the arm stays, fails this.
#
# Wildcard arms (*gamma-migration-scorer-changelog) are patterns rather than declared topics and
# are skipped: they preserve Streams-internal changelogs, which topics.env does not name.
# The keep-list parser now lives in ONE executable helper that both destructive scripts source, so
# the mechanism is tested by running it (scripts/kafka/reset-preserved-topics-test.sh) rather than
# by pattern-matching its callers. What is left to assert here is that the callers still USE it.
#
# Comments are STRIPPED before matching. The previous version grepped raw text, and deleting the
# live call while leaving the function definition and its comment block kept CI green -- the exact
# bypass this now closes.
HELPER="scripts/kafka/reset-preserved-topics.sh"
if [ ! -x "$HELPER" ]; then
  echo "FAIL: $HELPER missing or not executable — nothing defines what may not be purged"
  fail=1
fi
# THE FOUR PATHS THIS GATE COVERS, and why those four. These are the ESTATE-WIDE RESET paths: each one
# walks the broker's whole topic list and deletes what it does not recognise, so a keep-list it ignores
# is a keep-list that does not exist. run-dev-timewarp-replay.sh and daily-kafka-reset.sh were added on
# 2026-10-13, when the declaration held on two of the four.
#
# WHAT IS NOT IN THIS LIST, stated so the inventory is not mistaken for "every script that can delete a
# topic" (which it is not -- a sweep of `kafka-topics --delete` finds eleven more):
#   * scripts/kafka/streams-partition-doctor.sh deletes a Streams-internal topic (…-repartition,
#     …-changelog, FK subscription) ONLY when its partition count is the one Streams just refused to
#     start on. That includes the preserved …gamma-migration-scorer-changelog arm of
#     PRESERVE_TOPICS_REGEX -- and keeping it there would mean the app cannot run at all, which is a
#     worse outcome than the rebuild the doctor's own header documents. It is a conditional REPAIR, not
#     a reset, so the keep-list deliberately does not bind there.
#   * scripts/kafka/apply-topics.sh and cleanup-topics.sh are covered by their own executable tests
#     below (the ledger, strike and vol-premium safety tests), not by this caller list.
#   * ensure-indicator-topics.sh, prune-retired-zero-dte-identity.lib.sh,
#     reset-hpsf-stage-b-internal-topics.sh and es-predown.sh delete a NAMED, bounded set they own; none
#     of those names is in the preserved list today. A new preserved topic inside one of those sets
#     would need the same treatment, and nothing here would catch it -- that is the known limit of this
#     gate.
#   * the rest are tests, one-off replay scripts and scripts/ops/archive/.
for caller in "$CLEANSLATE" scripts/kafka/cleanup-topics.sh scripts/kafka/run-dev-timewarp-replay.sh \
             scripts/ops/daily-kafka-reset.sh; do
  code=$(sed 's/#.*//' "$caller")
  if ! printf '%s' "$code" | grep -q 'reset-preserved-topics.sh'; then
    echo "FAIL: $caller does not source $HELPER — it would purge topics the declaration says"
    echo "      cannot be rebuilt."
    fail=1
  fi
done
for caller in "$CLEANSLATE" scripts/kafka/run-dev-timewarp-replay.sh scripts/ops/daily-kafka-reset.sh; do
  code=$(sed 's/#.*//' "$caller")
  if ! printf '%s' "$code" | grep -qE 'if[[:space:]]+is_reset_preserved'; then
    echo "FAIL: $caller sources the keep-list but never BRANCHES on is_reset_preserved —"
    echo "      the list is loaded and then ignored."
    fail=1
  fi
done

# THE DAILY RESET NEEDS BOTH MECHANISMS. Its disposable rule deletes *-changelog, and the preserved
# Streams changelog (...gamma-migration-scorer-changelog) is protected only by PRESERVE_TOPICS_REGEX --
# topics.env cannot name a wildcard arm. Reading the regex is therefore load-bearing there, not a nicety
# (deploy Codex round 2, 2026-10-13: before this, that script deleted what premarket-reset.sh protects).
DAILY="scripts/ops/daily-kafka-reset.sh"
daily_code=$(sed 's/#.*//' "$DAILY")
if ! printf '%s' "$daily_code" | grep -q 'PRESERVE_TOPICS_REGEX='; then
  echo "FAIL: $DAILY does not read PRESERVE_TOPICS_REGEX from $PREMARKET, so it deletes the preserved"
  echo "      Streams changelogs that only the regex names."
  fail=1
fi
if ! printf '%s' "$daily_code" | grep -qE 'grep -qE "\$PRESERVE_TOPICS_REGEX"'; then
  echo "FAIL: $DAILY reads PRESERVE_TOPICS_REGEX but never MATCHES a topic against it."
  fail=1
fi

# AND WHATEVER SHIPS IT MUST SHIP WHAT IT SOURCES. Jenkinsfile.kafka-reset copies
# daily-kafka-reset.sh to the production host and runs it there. It used to copy that ONE file, which
# would now leave the script refusing to run (it fails closed when the keep-list is unreadable) -- the
# fix for one path breaking another, which is the shape this branch keeps finding.
KAFKA_RESET_JOB="Jenkinsfile.kafka-reset"
if [ ! -r "$KAFKA_RESET_JOB" ]; then
  echo "FAIL: $KAFKA_RESET_JOB not found — layout changed, update this validator"; fail=1
else
  # COMMENT LINES ARE STRIPPED FIRST. The block explaining this coupling names the very files it
  # requires, so grepping the raw file passed on the comment while the scp shipped nothing — the
  # "assertion satisfied by both" shape, caught by mutating the scp line.
  kafka_reset_code=$(sed 's|^[[:space:]]*//.*||' "$KAFKA_RESET_JOB")
  for needed in scripts/kafka/reset-preserved-topics.sh scripts/kafka/topics.env scripts/ops/premarket-reset.sh; do
    if ! printf '%s' "$kafka_reset_code" | grep -q "$needed"; then
      echo "FAIL: $KAFKA_RESET_JOB does not ship $needed, which $DAILY sources or reads — the job would"
      echo "      copy a script that refuses to run."
      fail=1
    fi
  done
fi

# THE BYTE HALF OF "KEPT FOREVER". retention.ms=-1 does not stop BYTE-based deletion, so a preserved
# topic declared -1 and nothing else can still be deleted by a finite broker default or a stray
# retention.bytes. Requiring the byte contract is therefore part of preservation, not a separate nicety
# (deploy Codex round 1 of the trading.order classification, 2026-10-13: without this, removing the two
# byte entries left every gate green).
#
# THE EXEMPTION IS THE PRE-EXISTING SET, LISTED. 21 of the 31 preserved -1 topics have never carried a
# byte override; closing that is a change to two live brokers for 21 topics and is NOT done here. They
# are named so the gap is visible and so a NEW preserved topic cannot join them silently.
# es.tape-zones.board LEFT this list on 2026-10-13: it now declares a finite 8 GiB cap (the owner's
# "cap the tape-zones board"; the measurement and what the cap costs are in topics.env beside it).
BYTES_EXEMPT="context-tape.direction.ledger corridor-gauge-event-log es.drop.final-summary es.drop.outcome
es.options.spx.gamma-ladder-path.events es.options.spx.gamma-ladder-path.state
es.options.spx.gamma-migration.crossings es.options.spx.gamma-migration.fragility
es.options.spx.gamma-migration.wall-touch options.databento.oi.anchor-manifest
options.ibkr.gex.status options.spx.gamma-ladder-path.events options.spx.gamma-ladder-path.state
options.spx.gamma-migration.crossings options.spx.gamma-migration.fragility
options.spx.gamma-migration.scoring options.spx.gamma-migration.wall-touch spx.basis.state
spx.drop.final-summary spx.drop.outcome underlying.vix.price"   # BZ 363, latent
# ANY declared byte value counts, not only -1. A preserved topic needs a byte contract so a broker
# default cannot delete it; whether that contract is "never by bytes" (-1) or "never in practice" (a cap
# far above the measured volume) is a disk decision per topic, and the disk cannot grow (owner,
# 2026-10-13). Requiring -1 here would have forbidden the only shape that protects the broker from a
# producer fault.
BYTES_DECLARED=$( { sed -nE 's/^[A-Z0-9_]*TOPIC_RETENTION_BYTES_OVERRIDES="(.*)"$/\1/p' "$TOPICS_ENV" \
  | tr ' ' '\n' | grep -E '=-?[0-9]+$' || true; } | cut -d= -f1 | sort -u)
_bytes_exempt_n=0
for topic in $PRESERVED; do
  in_list "$topic" "$MINUS_ONE" || continue
  in_list "$topic" "$BYTES_DECLARED" && continue
  if in_list "$topic" "$(printf '%s\n' $BYTES_EXEMPT)"; then _bytes_exempt_n=$((_bytes_exempt_n+1)); continue; fi
  echo "FAIL: '$topic' is RESET-PRESERVED at retention.ms=-1 but declares no retention.bytes at all, so a"
  echo "      byte cap can still delete it. Add it to OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES, or"
  echo "      to BYTES_EXEMPT in this validator with the reason."
  fail=1
done
echo "note: $_bytes_exempt_n preserved topic(s) carry no retention.bytes contract — known, exempt, tracked as BZ 363"
# EXACTLY THIS SET AND NO MORE. Without this, a name could be parked in BYTES_EXEMPT and the exemption
# would quietly cover a topic that no longer needs it -- or never did (deploy Codex round 2 NIT).
for topic in $BYTES_EXEMPT; do
  if ! in_list "$topic" "$PRESERVED"; then
    echo "FAIL: BYTES_EXEMPT names '$topic', which is not RESET-PRESERVED — the exemption covers nothing."; fail=1
  elif ! in_list "$topic" "$MINUS_ONE"; then
    echo "FAIL: BYTES_EXEMPT names '$topic', which has no retention.ms=-1 — the rule would not reach it."; fail=1
  elif in_list "$topic" "$BYTES_DECLARED"; then
    echo "FAIL: BYTES_EXEMPT names '$topic', which DOES declare a retention.bytes contract — remove the exemption."; fail=1
  fi
done

# --- RETIRED topics: names that used to be durable and must now be preserved NOWHERE ----------------------------
# options.spx.vol-premium.calendar (2026-09-13): the calendar ledger moved to Postgres. A retirement is complete only
# when every keep-list, protection regex and reset arm has let go of the name — a stale arm in ONE script quietly
# keeps a topic the declaration says does not exist (premarket-reset.sh still preserved it, deploy #1044 round 1).
RETIRED_TOPICS="options.spx.vol-premium.calendar"
for t in $RETIRED_TOPICS; do
  for v in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_PROD_ONLY_TOPICS OPTIONS_EDGE_RESET_PRESERVED_TOPICS OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS \
           OPTIONS_EDGE_NEVER_RECREATE_TOPICS OPTIONS_EDGE_EXACT_PARTITION_TOPICS OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES \
           OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES OPTIONS_EDGE_COMPACTED_TOPICS OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS; do
    if ( . "$TOPICS_ENV"; printf '%s\n' ${!v-} ) | sed 's/[:=].*//' | grep -qxF "$t"; then
      echo "FAIL: retired topic $t is still declared in $v (topics.env)"; fail=1
    fi
  done
  if ( . "$TOPICS_ENV"; printf '%s\n' "$t" | grep -qE "$PROTECTED_TOPIC_REGEX" ); then
    echo "FAIL: retired topic $t is still matched by PROTECTED_TOPIC_REGEX (topics.env)"; fail=1
  fi
  if printf '%s\n' "$t" | grep -qE "$REGEX"; then
    echo "FAIL: retired topic $t is still preserved by PRESERVE_TOPICS_REGEX in $PREMARKET — the pre-market reset would keep it"; fail=1
  fi
done
echo "checked $(echo "$RETIRED_TOPICS" | wc -w | tr -d ' ') retired topic(s): preserved nowhere"

if [ "$fail" -ne 0 ]; then
  echo "=== validate-durable-topic-preservation: FAILED ==="
  exit 1
fi
echo "checked $(echo "$DURABLE" | wc -w | tr -d ' ') durable topic(s) against both reset scripts + cleanup-topics"
echo "=== validate-durable-topic-preservation: OK ==="
