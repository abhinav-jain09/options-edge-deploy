#!/usr/bin/env bash
# dev-cleanup.sh — THE one dev cleanup script. Once a day (end of day, ET-gated) it cleans BOTH
# the logs AND the Kafka data in a single run, and each weekday morning it brings dev back up.
# No 60-second log trimming, no separate scheduler wrapper — this one file + one launchd is the whole thing.
#
# MODES:
#   dev-cleanup           # AUTO — what launchd calls every ~15 min. CALENDAR-aware ET slots (2026-07-11):
#                         #   17:20 ET FIXED (early closes too; after the 17:15 OI evaluation) -> full clean (WIPE topics) THEN
#                         #                immediately bring up ONLY the overnight ES-tracking set ($OVERNIGHT_SET)
#                         #   ~09:17 ET (window 09:15-09:29, weekdays) -> ESDOWN: scale the overnight ES services
#                         #                ($ES_DOWN_SET) to 0 before the 09:30 SPX open (feed-gateway + web stay up)
#                         #   06:15-06:44 ET weekdays -> full start (bring the rest of the pipeline up before the pre-open window)
#   dev-cleanup now       # run the full clean right now (logs + data + topic WIPE); on a trading day the
#                         # clean guard REFUSES 09:30-17:19 ET unless FORCE_CLEAN=1 (see clean_guard)
#   dev-cleanup start     # bring the FULL dev pipeline up now
#   dev-cleanup overnight # bring up ONLY the overnight ES-tracking set now
#   dev-cleanup logs      # clean logs only (manual)
#
# Full clean deletes every non-system topic + every *-streams-state PVC, trims the broker logs, and
# prunes docker-engine build cache. Keeps __consumer_offsets/__transaction_state/_schemas + Keycloak.
# Needs the jenkins-deployer SA (docker-desktop enforces the jenkins-only admission policy). Keep this
# file POSIX-ish/bash-3.2 safe: the shebang resolves against the INHERITED PATH, so under launchd it
# can be /bin/bash 3.2 — no associative arrays, no namerefs.
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

K="kubectl --context docker-desktop -n options-edge --as=system:serviceaccount:options-edge:jenkins-deployer"
KK="kubectl --context docker-desktop -n options-edge"
# The Kafka CLI (kafka-topics.sh / kafka-configs.sh) is RESOLVED, not hard-coded: see resolve_kafka_cli below. KT and KC stay
# empty until require_kafka_cli has found a usable distribution, and every path that touches Kafka calls it first.
KT=""
KC=""
BS=localhost:19092
KAFKA_DATA=/Users/abhinav/development/kafka-options-edge/data
# ---------- the Kafka CLI ----------
# Where the Kafka command-line tools live differs by machine, and a path that was right once goes stale: the dev broker's own host keeps
# its distribution at /Users/abhinav/kafka-4.3.0, while the 201 Mac's copy was deleted on 2026-10-27 and left an EMPTY directory of the
# same name. With a hard-coded path every `kafka-topics` call failed, and because they all run with 2>/dev/null the clean carried on: it
# listed nothing, deleted nothing, and printed "changelog topics: 0 (0 = success)" -- a measurement that never happened, read as a good
# one. So: search the known homes (or $KAFKA_CLI_DIR when the operator names one, which is then the ONLY candidate), accept a directory
# only when its tools exist, run, and report a Kafka version, prove the broker answers, and otherwise STOP with every candidate and why.
# The candidate list is $DEV_CLEANUP_KAFKA_CLI_SEARCH (whitespace separated) when set, for the test.
kafka_cli_candidates() {
  if [ -n "${KAFKA_CLI_DIR:-}" ]; then printf '%s\n' "$KAFKA_CLI_DIR"; return; fi
  if [ -n "${DEV_CLEANUP_KAFKA_CLI_SEARCH:-}" ]; then printf '%s\n' $DEV_CLEANUP_KAFKA_CLI_SEARCH; return; fi
  {
    [ -z "${KAFKA_HOME:-}" ] || printf '%s\n' "$KAFKA_HOME/bin"
    printf '%s\n' /Users/abhinav/kafka-4.3.0/bin
    [ -z "${HOME:-}" ] || printf '%s\n' "$HOME/kafka-4.3.0/bin"   # HOME is unset under some launchd/cron launches, and this script runs with set -u
    printf '%s\n' /Users/abhinav/development/kafka-options-edge/kafka_2.13-4.3.0/bin /opt/kafka/current/bin /opt/homebrew/opt/kafka/libexec/bin
  } | awk '!seen[$0]++'
}

# The CLI scripts need a JDK. Under launchd JAVA_HOME is not inherited, and /usr/bin/java is a stub that fails without a registered JDK.
kafka_cli_java() {
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then return 0; fi
  local j
  # The candidate homes are $DEV_CLEANUP_JDK_SEARCH (whitespace separated) when set, for the test. The default list is an ARRAY so every
  # entry stays one candidate (a Cellar glob match or a java_home answer may contain a space); the glob expands in place as before.
  local -a jdks=()
  if [ -n "${DEV_CLEANUP_JDK_SEARCH:-}" ]; then read -r -a jdks <<< "$DEV_CLEANUP_JDK_SEARCH"
  else jdks=(/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home /opt/homebrew/Cellar/openjdk@21/*/libexec/openjdk.jdk/Contents/Home "$(/usr/libexec/java_home 2>/dev/null)"); fi
  for j in ${jdks[@]+"${jdks[@]}"}; do
    if [ -n "$j" ] && [ -x "$j/bin/java" ]; then JAVA_HOME="$j"; export JAVA_HOME; return 0; fi
  done
  return 1
}

# kafka_cli_usable <bin dir>: prints the Kafka version on stdout and returns 0; otherwise prints WHY on stdout and returns 1.
kafka_cli_usable() {
  local d="$1" v
  [ -d "$d" ] || { echo "not a directory"; return 1; }
  [ -x "$d/kafka-topics.sh" ] || { echo "no executable kafka-topics.sh in it (an empty or partial distribution)"; return 1; }
  [ -x "$d/kafka-configs.sh" ] || { echo "no executable kafka-configs.sh beside kafka-topics.sh"; return 1; }
  [ -f "$d/kafka-run-class.sh" ] || { echo "no kafka-run-class.sh beside kafka-topics.sh (the tools resolve it from their own directory)"; return 1; }
  kafka_cli_java || { echo "no usable JDK (JAVA_HOME unset or invalid, no openjdk@21 under /opt/homebrew)"; return 1; }
  v="$("$d/kafka-topics.sh" --version 2>&1)" || { echo "kafka-topics.sh --version failed: $(printf '%s' "$v" | head -1)"; return 1; }
  printf '%s\n' "$v" | head -1 | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+' || { echo "kafka-topics.sh --version did not print a version: $(printf '%s' "$v" | head -1)"; return 1; }
  # the tools must be of the broker's generation: incremental config alters, KRaft-only flags and the 4.x kafka-topics options are what this script is written against
  local major="${v%%.*}"
  [ "$major" = "${DEV_CLEANUP_KAFKA_MAJOR:-4}" ] || { echo "Kafka CLI version $(printf '%s' "$v" | head -1) is not the expected major ${DEV_CLEANUP_KAFKA_MAJOR:-4} (DEV_CLEANUP_KAFKA_MAJOR)"; return 1; }
  printf '%s\n' "$v" | head -1
}

# Sets KT and KC to the first usable candidate. On failure prints every candidate with its reason and returns 1.
resolve_kafka_cli() {
  local d why tried="" ver
  while IFS= read -r d; do
    [ -n "$d" ] || continue
    if ver="$(kafka_cli_usable "$d")"; then
      KT="$d/kafka-topics.sh"; KC="$d/kafka-configs.sh"
      echo "dev-cleanup: Kafka CLI $d (version ${ver%% *})"
      return 0
    fi
    tried="$tried
    $d -- $ver"
  done <<EOF_CANDIDATES
$(kafka_cli_candidates)
EOF_CANDIDATES
  echo "dev-cleanup: NO USABLE KAFKA CLI. Tried:$tried" >&2
  echo "dev-cleanup: install a Kafka distribution on this machine or run this where the dev broker's CLI is, or set KAFKA_CLI_DIR to its bin directory." >&2
  return 1
}

# The broker must answer too: an unreachable one (a dead tunnel to localhost:19092) answers --list with an error that the callers' 2>/dev/null
# would read as "no topics". Bounded, so a dead tunnel costs seconds and not the client's default minute.
kafka_broker_answers() {
  local props rc
  props="$(mktemp -t oe-kafka-props)" || return 1
  printf 'request.timeout.ms=8000\ndefault.api.timeout.ms=15000\n' > "$props"
  "$KT" --bootstrap-server "$BS" --command-config "$props" --list >/dev/null 2>&1; rc=$?
  rm -f "$props"
  return $rc
}

# Called first by every path that touches Kafka. Stops the whole script (exit 1) rather than let a clean run blind.
require_kafka_cli() {
  [ -z "$KT" ] || return 0
  resolve_kafka_cli || exit 1
  if ! kafka_broker_answers; then
    echo "dev-cleanup: the Kafka broker at $BS does not answer kafka-topics --list (dead tunnel or broker down); refusing to run blind." >&2
    KT=""; KC=""
    exit 1
  fi
}

# The live topic list, with retries: prints it and returns 0, or prints nothing and returns 1. An error and an empty answer are both failures (a broker
# always holds __consumer_offsets and _schemas), so a caller never mistakes "could not list" for "no topics". A transient tunnel or broker hiccup after the
# start-up check costs a few seconds, not a half-finished clean.
kafka_list_topics() {
  local n=0 tries="${DEV_CLEANUP_LIST_RETRIES:-3}" out
  while [ "$n" -lt "$tries" ]; do
    if out="$("$KT" --bootstrap-server "$BS" --list 2>/dev/null)" && [ -n "$out" ]; then printf '%s\n' "$out"; return 0; fi
    n=$((n + 1))
    [ "$n" -ge "$tries" ] || sleep "${DEV_CLEANUP_LIST_RETRY_SLEEP:-5}"
  done
  return 1
}

# Topics matching an ERE ($2 = "v" to count the others). Prints UNKNOWN instead of a number when the list itself could not be read: a broker
# always holds __consumer_offsets and _schemas, so an error or an empty list is a failed measurement, never a count of zero.
topic_count() {
  local out
  out="$("$KT" --bootstrap-server "$BS" --list 2>/dev/null)" || { echo "UNKNOWN (kafka-topics --list failed)"; return 1; }
  [ -n "$out" ] || { echo "UNKNOWN (kafka-topics --list printed nothing)"; return 1; }
  if [ "${2:-}" = v ]; then printf '%s\n' "$out" | grep -vcE "$1" || true; else printf '%s\n' "$out" | grep -cE "$1" || true; fi
}

KEEP='keycloak'                                          # deployments to leave running (regex)
# ibkr-feed-service REMOVED 2026-07-29: now dev-only and REQUIRED up (SPX cash-index tier-1 source
# for both envs, requirement rev4).
# DEV-disabled deployments — held at 0 by do_start (exact full-name match via grep -x). MUST include
# databento-timewarp-snapshot-replay: if it boots even briefly it replays snapshots into
# options.databento.raw and poisons the chain. Keep in sync with premarket-check.sh DISABLED (dev).
# stock-gex-service is PROD-ONLY (USER 2026-08-18): the public Gamma Lab page reads it there,
# and it pulls straight from Databento (billed) rather than from Kafka — a dev copy spends
# money serving a page dev does not host, and it was sitting at 0/1 on dev anyway.
# 2026-10-11: broker-execution-service and amt-order-bridge were REMOVED from this set so the
# auction-to-paper-order chain survived a clean (both lifted to 1 in the DEV OVERLAY, not the shared base).
# 2026-11-02 (owner): broker-execution-service is back IN this set and at replicas 0 in the dev overlay -- the Order
# tab uses the broker on the owner's Mac, there is no shared dev broker. Both halves are needed (#1034's lesson):
# replicas alone would be lifted by the scale-up loop below. amt-order-bridge was NOT changed and stays at 1.
# USER 2026-10-26: corridor-gauge-service is OFF PERMANENTLY on dev and production. It has no *_ENABLED flag, so
# replicas IS the switch and k8s/base/corridor-gauge-deployment.yaml sets 0 — but that base alone is NOT enough,
# because do_start below scales every deployment NOT named here to 1 regardless of what the manifest says. The
# name was added to the operational copy on .102 and never to this versioned one, so the two lists diverged and a
# repo-driven start would have switched the service back on against the decision. trade-evidence-api has no
# manifest in this repo at all; do_start would otherwise lift an unmanaged deployment, so it is held here too.
# 2026-10-31: spread-skew-service REMOVED from this list (owner lifted the hold for DEV only: market-commentary
# needs options.spx.spread-skew.current). Production stays held, so morning-autostart.sh and oe-boot-bringup.sh
# KEEP_DOWN (prod scripts) keep it. spread-skew-postgres-writer stays held here.
# 2026-10-28: thetadata-feed-service and level-absorption-service were on the .102 copy and are deliberately NOT
# here. thetadata-feed runs at 1 on dev (owner decision 2026-09-19, dev is the data source for prod); the
# level-absorption crashloop that justified holding it was fixed and it is ready and emitting. The .102 copy is
# re-synced from this file, which is the single source of truth.
# 2026-11-02 (VWAP-ACCEPTANCE-REJECTION): line-acceptance-service was held here by name while deployed DARK (P0) and was REMOVED the same day: the owner overrode design K9 and switched it on for dev,
# UNVALIDATED, alerts off (k8s/services/line-acceptance/base: replicas 1, LINE_ACCEPTANCE_ENABLED "true"). It is therefore raised by the scale-up loop below like any dev service. This clean recreates its six
# topics from scripts/kafka/topics.env (partitions, policy, retention, topic-level max.message.bytes) but does NOT run scripts/kafka/ensure-line-acceptance-topics.sh: that barrier belongs to service-deploy,
# so re-run the deploy barrier (service-deploy SERVICE=line-acceptance) after a wipe, before relying on the service (the clean never runs per-service barriers).
# USER 2026-10-14: zerodte-research-writer is deployed DARK (Stage-B: its zerodte-writer-identity config map is created
# only by the provisioning job, not yet run), so it can only crash with CreateContainerConfigError. The start script used to scale it to 1 on
# every start and the partition doctor then reported it INCONCLUSIVE (exit 5). Hold it at 0 until the provisioning job has run.
DISABLED_DEV='hpsf-stage-a-service|hpsf-stage-b-service|volume-pace-service|volume-sandwich-service|volume-sandwich-databento-service|databento-timewarp-snapshot-replay|strike-flow-classifier-ibkr|options-edge-integration-test|databento-mission-pressure-service|databento-mission-pace-service|spx-mission-control-service|short-premium-agent-service|spread-skew-postgres-writer|directional-pressure-databento-service|databento-maxpain-service|databento-mission-sandwich-service|directional-pressure-service|option-truth-engine-service|stock-gex-service|hedging-pressure-service|zerodte-research-writer|corridor-gauge-service|trade-evidence-api|break-score-service|broker-execution-service'
# OVERNIGHT ES-tracking set — the ONLY services brought up right after the (17:20 ET, trading days) clean,
# so ES futures are tracked overnight. Everything else stays at 0 until the 06:15 ET full start. (2026-08-03: was 07:30)
# databento-feed + databento-gex-service are kept alive overnight so the pre-market LIVE OI path runs before
# the 09:30 open: while OPRA is closed the feed drains the OI-aware statistics replay and publishes the day's
# settled OI, and the gex service (alive) observes it into the OI baseline the pre-open board reads. Without
# the feed up there is no pre-market OI publisher; without gex up there is no observer. Both are needed at
# ~06:30 ET. The Databento OI fetch is plan-covered ($0), so a 24/7 dev pod adds no data cost. (2026-08-20)
# raw-postgres-writer is kept alive overnight so it is CONSUMING options.databento.raw before 06:30 and
# persists the feed's live-OI capture into Postgres the moment it publishes — without it up at 06:30, the
# pre-open board() has no OI and pre-open GEX cannot compute until the ~07:00 historical fetch. (2026-08-26)
# es-amt-service (2026-11-02, es.futures.auction Avro cutover, docs/ES-AMT-AVRO-CUTOVER.md): dev's OWN writer of the Globex/RTH auction minutes, so it must be up from the 18:00 ET Globex open (18:01 is the first
# minute label), not from the 06:15 full start. It is NOT in ES_DOWN_SET: it publishes the RTH minutes until 16:00 ET. Absent on a dev that has not been cut over yet: do_start_overnight skips an absent deployment.
OVERNIGHT_SET='es-open-direction-service es-open-direction-postgres-writer feed-gateway-service options-edge-web options-edge-databento-feed databento-gex-service raw-postgres-writer es-amt-service'
# ES overnight-tracking services that SHUT DOWN at ~09:17 ET (before the 09:30 SPX open): the overnight ES
# session is over, so these scale to 0. feed-gateway-service + options-edge-web STAY UP (they serve the
# SPX day session). Fired by the ESDOWN calendar slot below.
ES_DOWN_SET='es-open-direction-service es-open-direction-postgres-writer'
# Transient ES Kafka topics CLEANED at 09:17 (overnight ES data, no longer needed once the session ends).
# The Postgres es_* tables (session_history, level_break_history, forecast, outcome, publication) are the
# model's MEMORY / immutable ledgers / TRAINING data — they are NEVER touched here or by the offhours DB
# truncate (excluded there via `tablename !~ '^es_'`).
ES_CLEAN_TOPICS='es.open-direction.forecast es.open-direction.outcome es.open-direction.status underlying.es.trades'
# Topic source-of-truth = the deploy repo's scripts/kafka/topics.env (the SAME file the deploy's
# apply-topics.sh uses), read from origin/main so it is the reviewed, versioned config — NOT an
# autonomous live snapshot. dev-cleanup pre-creates ONLY these platform/feed topics; every service
# self-creates its own output + Kafka-Streams internal topics on startup, exactly like prod.
DEPLOY_REPO="${DEPLOY_REPO:-/Users/abhinav/development/workspace/options-edge-deploy}"
TOPICS_ENV_REF="${TOPICS_ENV_REF:-origin/main:scripts/kafka/topics.env}"
# Set by topics_ref_pin, and by nothing else. Initialised here rather than left to the environment: an
# exported value would pin a destructive clean to a commit nobody in this run chose.
TOPICS_ENV_SHA=""
TOPICS_ENV_PINNED_REF=""
# 2026-07-11: for OVERNIGHT ES-future tracking the nightly clean WIPES all topics (except the sacred
# _schemas/__consumer_offsets/__transaction_state) and the overnight-start recreates them fresh. So
# WIPE_KAFKA defaults to TRUE again. (The _schemas guard in do_start's schema self-heal + the grep
# exclusion below keep the Schema Registry safe.) Set WIPE_KAFKA=false for a no-wipe run.
WIPE_KAFKA="${WIPE_KAFKA:-true}"
# Market calendar (close time, early-close, holidays) — shared with the prod scripts (scripts/jenkins/
# market_calendar.py). Used to fire the CLEAN at a FIXED 17:20 ET on trading days (since 2026-10-06; was close+30;
# early-close 13:00 -> 13:30).
CALENDAR_DIR="${CALENDAR_DIR:-$DEPLOY_REPO/scripts/jenkins}"
LOG=/Users/abhinav/oe-ops/dev-cleanup.log

# ---------- es4 -> dev MM1 mirrors: paused around the topic wipe ----------
# The kafka-mirror-maker launchd agents (es-cvd/-indicator/-strike-intel/-tape-zones/-auction/esgex ...)
# keep producing into dev while the wipe deletes their target topics, and dev auto-creates a topic on
# the first produce at num.partitions=1. 2026-09-14 12:06:34: the delete of es.options.databento.gex.strike
# and .gex.spxbridge was followed 0.3 s later by a mirror's CreateTopics numPartitions=1; ensure_topics'
# declared :4 then hit TOPIC_ALREADY_EXISTS, and es.options.indicators.bars (:8, EXACT) and
# es.strike-intelligence-by-strike (:32) stuck at 1 the same way. es-spx-align-service's Streams app
# sized its repartition/changelog topics from those 1-partition sources and died with "invalid
# partitions: expected: 4; actual: 1" as soon as the shapes were repaired. So the wipe unloads every
# agent that writes to dev BEFORE the delete and reloads them only AFTER ensure_topics has created the
# targets at their declared shape. The paused list is a file so an interrupted clean is still resumed by
# the next start/overnight run. MM1 commits on es4, so a resumed mirror continues from its committed
# offset: the wiped history is NOT back-filled (unchanged behaviour, see topics.env).
LAUNCH_AGENTS_DIR="${LAUNCH_AGENTS_DIR:-${HOME:-/var/empty}/Library/LaunchAgents}"
LAUNCHCTL="${LAUNCHCTL:-launchctl}"
DEV_MIRRORS_PAUSED="${DEV_MIRRORS_PAUSED:-/Users/abhinav/oe-ops/.dev-mirrors-paused}"
# How long a resumed mirror must hold the SAME pid before it counts as running.
DEV_MIRROR_SETTLE_S="${DEV_MIRROR_SETTLE_S:-5}"
# How long to wait for a resumed mirror to reach a pid at all. 60s because 14 JVMs starting
# together on a Mac at load ~24 can take well over 10s, and calling a healthy mirror failed is
# its own outage. Overridable so the CI gate does not spend a minute per case.
DEV_MIRROR_WAIT_S="${DEV_MIRROR_WAIT_S:-60}"
# Set when a resume did not bring every mirror back. Reported at exit so a clean that left dev
# reading frozen topics cannot exit 0; deliberately NOT a mid-clean abort, which would leave the
# wipe half-applied and is a worse failure than a frozen mirror.
DEV_MIRRORS_RESUME_FAILED=0

# The RUNNING pid of a launchd label, or empty. Prints only a numeric pid: `launchctl list` shows '-'
# in that column for a job that is loaded but not running, and the label is matched with an exact
# field comparison so a label containing whitespace cannot shift the column being read.
mirror_agent_pid() {
  "$LAUNCHCTL" list 2>/dev/null | awk -v l="$1" '$3 == l && $1 ~ /^[0-9]+$/ { print $1; exit }'
}
DEV_MIRROR_TARGET_RE='^bootstrap\.servers=(127\.0\.0\.1|localhost):19092[[:space:]]*$'

# "label plist" for every com.optionsedge agent with a ProgramArguments entry whose directory holds a
# producer.properties that targets dev Kafka. Parsed with plistlib, not grepped: most real mirror plists
# are one-line XML (<key>ProgramArguments</key><array><string>...), and a `/bin/bash run.sh` agent keeps
# the script in [1]. The label is read from the plist, never guessed: esgex-mirror* run from es-gex-mirror*/.
dev_mirror_agents() {
  python3 - "$LAUNCH_AGENTS_DIR" "$DEV_MIRROR_TARGET_RE" <<'PY'
import glob, os, plistlib, re, sys
agents_dir, target_re = sys.argv[1], re.compile(sys.argv[2].replace("[[:space:]]", r"\s"))
unclassifiable = []
for plist in sorted(glob.glob(os.path.join(agents_dir, "com.optionsedge.*.plist"))):
    try:
        with open(plist, "rb") as f:
            job = plistlib.load(f)
    except Exception as e:
        # NOT "not a mirror". launchd may well have this job loaded; we simply cannot read what it
        # writes to. Skipping it silently is how a live dev mirror becomes invisible to the pause,
        # and the wipe then deletes its target behind it.
        unclassifiable.append("%s: %s" % (plist, e))
        continue
    if not isinstance(job, dict):
        unclassifiable.append("%s: not a plist dictionary" % plist)
        continue
    for arg in job.get("ProgramArguments") or []:
        if not os.path.isabs(str(arg)):
            continue   # `-lc`, `auto`, `dev`: dirname "" would resolve against the caller's cwd
        props = os.path.join(os.path.dirname(str(arg)), "producer.properties")
        try:
            lines = open(props).read().splitlines()
        except FileNotFoundError:
            continue   # no producer.properties beside that program: it is not a mirror
        except OSError as e:
            # It EXISTS and cannot be read, which is not the same as absent: we cannot tell whether
            # this agent writes to dev.
            unclassifiable.append("%s: %s" % (props, e))
            continue
        if any(target_re.match(line) for line in lines):
            print(job.get("Label") or os.path.basename(plist)[:-len(".plist")], plist)
            break
if unclassifiable:
    sys.stderr.write("   ERROR: cannot classify %d LaunchAgent plist(s), so a loaded dev mirror may be invisible here:\n" % len(unclassifiable))
    for u in unclassifiable:
        sys.stderr.write("          %s\n" % u)
    sys.exit(3)
PY
}

# Is one agent loaded? 0 = loaded, 1 = genuinely NOT loaded, 2 = CANNOT TELL.
#
# The distinction is the whole point. `launchctl list <label>` answering non-zero was read as "not
# loaded", so a launchctl that cannot answer AT ALL — missing, or launchd unreachable — made every
# mirror look absent: nothing was booted out, nothing was reported, and the wipe proceeded behind
# live mirrors. That is the same fail-open this round is closing, one level further down: a status
# that conflates "it is gone" with "I could not look". A negative answer is therefore believed only
# when launchctl is demonstrably still answering.
mirror_label_loaded() {
  "$LAUNCHCTL" list "$1" >/dev/null 2>&1 && return 0
  "$LAUNCHCTL" list >/dev/null 2>&1 || return 2
  return 1
}

pause_dev_mirrors() {
  local uid label plist n=0 i unproven=0
  uid=$(id -u)
  # Only agents that are LOADED now are paused (one someone unloaded on purpose stays unloaded), appended
  # to any list an interrupted clean left behind, so the next resume still reloads those too.
  touch "$DEV_MIRRORS_PAUSED" || { echo "   ERROR: cannot write the paused-mirror list $DEV_MIRRORS_PAUSED" >&2; unproven=1; }
  if ! dev_mirror_agents > "$DEV_MIRRORS_PAUSED.found"; then
    echo "   ERROR: es4->dev mirror discovery failed or could not classify a plist (reason above) — mirrors are NOT paused; their targets may be auto-created at 1 partition"
    unproven=1
  fi
  while read -r label plist; do
    [ -n "$label" ] || continue
    mirror_label_loaded "$label"
    case $? in
      0) printf '%s %s\n' "$label" "$plist" ;;
      2) echo "   ERROR: cannot tell whether $label is loaded (launchctl is not answering) — the mirrors are NOT proven down" >&2
         unproven=1 ;;
    esac
  done < "$DEV_MIRRORS_PAUSED.found" > "$DEV_MIRRORS_PAUSED.new"
  rm -f "$DEV_MIRRORS_PAUSED.found"
  # The list itself is part of the proof: an incomplete one means a later resume will not reload
  # everything this clean took down, so a write failure here is "not proven", not a detail.
  if ! sort -u "$DEV_MIRRORS_PAUSED" "$DEV_MIRRORS_PAUSED.new" > "$DEV_MIRRORS_PAUSED.merged" \
     || ! mv "$DEV_MIRRORS_PAUSED.merged" "$DEV_MIRRORS_PAUSED"; then
    echo "   ERROR: could not record the paused-mirror list at $DEV_MIRRORS_PAUSED — the mirrors are NOT proven down" >&2
    unproven=1
  fi
  while read -r label plist; do
    [ -n "$label" ] || continue
    "$LAUNCHCTL" bootout "gui/$uid/$label" >/dev/null 2>&1
    for i in 1 2 3 4 5 6 7 8 9 10; do
      mirror_label_loaded "$label" || break
      sleep 3
    done
    mirror_label_loaded "$label"
    case $? in
      0) echo "   WARN: mirror agent $label is still loaded; its target topics may be auto-created at 1 partition"
         unproven=1 ;;
      2) echo "   ERROR: cannot tell whether $label unloaded (launchctl is not answering) — the mirrors are NOT proven down"
         unproven=1 ;;
      *) n=$((n + 1)) ;;
    esac
  done < "$DEV_MIRRORS_PAUSED.new"
  rm -f "$DEV_MIRRORS_PAUSED.new"
  [ -s "$DEV_MIRRORS_PAUSED" ] || rm -f "$DEV_MIRRORS_PAUSED"
  echo "   paused $n es4->dev mirror agent(s) (list: $DEV_MIRRORS_PAUSED)"
  # SAY IT IN THE STATUS, not only in the log. Both branches above detect the precise condition this
  # function exists to prevent — a mirror still running while the wipe deletes its target, which
  # re-creates that topic at dev's num.partitions=1 so ensure_topics' declared shape never applies
  # (2026-09-14, es.options.databento.gex.strike, re-created 0.3 s after the delete). Until now they
  # printed ERROR/WARN and returned 0, so do_clean deleted anyway: the function announced the hazard
  # and then permitted it. Same shape as the status bug fixed in #1585 — a failure nobody can read.
  # WHAT THIS DOES NOT PROVE, said out loud so a reader does not stop looking. The verification is a
  # point-in-time observation: an agent with KeepAlive can be reloaded by launchd AFTER the check and
  # before do_clean reaches the delete, and nothing here would see it. Re-checking immediately before
  # the delete would narrow that window, not close it; closing it needs a durable launchd-side fence
  # (disable or remove the job definition, then verify) or proof after the delete that no mirror
  # re-created a topic. Not attempted here: it changes operator-visible launchd state, which is the
  # owner's decision. What IS closed is every case where this function could SEE the hazard and
  # returned success anyway.
  return "$unproven"
}

resume_dev_mirrors() {
  [ -s "$DEV_MIRRORS_PAUSED" ] || return 0
  local uid label plist n=0 failed=0
  uid=$(id -u)
  while read -r label plist; do
    [ -n "$label" ] || continue
    if [ ! -f "$plist" ]; then
      echo "   (agent removed, skipped): $label"
      continue
    fi
    "$LAUNCHCTL" bootstrap "gui/$uid" "$plist" >/dev/null 2>&1   # non-zero when already loaded; judged below
    # ⚠ A RUNNING PID, not `launchctl list <label>`. That exits 0 for a job that is merely
    # REGISTERED with nothing running — measured on this Mac 2026-10-12: 34 com.optionsedge.* agents
    # were loaded with no pid at all. So this check was satisfied by registration, counted every
    # agent as resumed, deleted the pause list, and let the clean report SUCCESS while the es4 -> dev
    # mirrors were not running. That is how dev spent a night with all 14 mirrors down, every
    # mirrored topic frozen and every downstream dev service reading stale data while looking
    # healthy; the auction mirror had logged a clean shutdown at 05:44 and no process existed.
    # The pid column is the only thing that tells those two states apart.
    # A SETTLED pid, not the first one seen. Review was right that the first version was a
    # point-in-time liveness observation: a JVM that starts, gets a pid and then dies in a KeepAlive
    # restart loop passes on its brief pid window, and the retry list is then deleted while the
    # mirror is down moments later. So: wait up to 60s for a NUMERIC pid (14 JVMs starting together
    # on a Mac at load ~24 can take well over 10s — the old budget would have called a healthy
    # mirror failed), then require the SAME pid still present after a settle, which a respawn loop
    # cannot satisfy because each restart issues a new pid.
    local pid="" pid2="" waited=0
    while [ "$waited" -lt "$DEV_MIRROR_WAIT_S" ]; do
      pid=$(mirror_agent_pid "$label")
      case "$pid" in '') waited=$((waited + 2)); sleep 2 ;; *) break ;; esac
    done
    if [ -n "$pid" ]; then
      sleep "$DEV_MIRROR_SETTLE_S"
      pid2=$(mirror_agent_pid "$label")
    fi
    if [ -z "$pid" ]; then
      failed=$((failed + 1))
      echo "   FAIL: mirror agent $label is NOT RUNNING — no pid within ${DEV_MIRROR_WAIT_S}s"
    elif [ "$pid2" != "$pid" ]; then
      failed=$((failed + 1))
      echo "   FAIL: mirror agent $label is NOT RUNNING stably — pid $pid became '${pid2:--}' after ${DEV_MIRROR_SETTLE_S}s (restart loop)"
    else
      n=$((n + 1))
    fi
  done < "$DEV_MIRRORS_PAUSED"
  [ "$failed" -eq 0 ] && rm -f "$DEV_MIRRORS_PAUSED"
  if [ "$failed" -gt 0 ]; then
    echo "   resumed $n es4->dev mirror agent(s), $failed NOT RUNNING"
  else
    echo "   resumed $n es4->dev mirror agent(s)"
  fi
  # Return the verdict. This function used to end on an echo, so it ALWAYS returned 0 and a total
  # failure to resume was indistinguishable from success to every caller.
  [ "$failed" -eq 0 ]
}

# ---------- LOGS: safe, non-destructive (no topic/state data touched) ----------
# (1) launchd stdout + log4j logs grow forever w/ no rotation -> any *.log > 50 MB trimmed to its last
#     10 MB in place (preserves the broker's open fd). (2) Kafka's rotated daily archives
#     (current/logs/*.log.YYYY-...) older than a day are deleted; gc logs self-recycle -> left alone.
clean_logs() {
  echo "logs) trimming Kafka broker logs ..."
  local LOGDIR=/Users/abhinav/development/kafka-options-edge/logs f sz
  if [ -d "$LOGDIR" ]; then
    for f in "$LOGDIR"/*.log; do
      [ -f "$f" ] || continue
      sz=$(stat -f%z "$f" 2>/dev/null || echo 0)
      if [ "$sz" -gt $((50*1024*1024)) ]; then
        tail -c $((10*1024*1024)) "$f" > "$f.tmp" 2>/dev/null && cat "$f.tmp" > "$f" && rm -f "$f.tmp"
      fi
    done
  fi
  # ROTATED log4j files. Two defects until 2026-10-05, which together meant NOTHING ever
  # pruned them: (1) this pointed at .../kafka-options-edge/current/logs, a path that does
  # not exist (the real dir is the LOGDIR above), so the find never ran; (2) the trim loop
  # above globs "*.log", which does not match a rotated name like
  # server.log.2026-09-21-12, so that loop never touched them either. Measured when found:
  # 913 rotated files / 1.5 GB, oldest 2026-09-21, on a Mac whose data volume was at 99%.
  # Broker log4j output is NOT required for Kafka to run (only data/ is) but it IS what a
  # post-incident diagnosis reads, so keep a short forensic window instead of wiping:
  # 2 days of hourly rotations. The ACTIVE files are deliberately left to the truncate loop
  # above -- the broker holds them open, and on macOS deleting an open file frees nothing
  # until the process restarts, so they must be truncated in place, never unlinked.
  # kafkaServer-gc.log.N is deliberately NOT pruned: the JVM rotates it itself with a fixed
  # filecount=10/filesize=100M, so it is already bounded and the JVM reuses those fds.
  if [ -d "$LOGDIR" ]; then
    local nrot
    nrot=$(find "$LOGDIR" -name '*.log.20*' -type f -mtime +2 2>/dev/null | wc -l | tr -d ' ')
    find "$LOGDIR" -name '*.log.20*' -type f -mtime +2 -delete 2>/dev/null
    echo "  rotated log4j files pruned (>2 days): ${nrot}; remaining: $(find "$LOGDIR" -name '*.log.20*' -type f 2>/dev/null | wc -l | tr -d ' ')"
  fi
}

# topic_desired NAME -> sets DPOL/DPARTS/DRET/DDR/DMB from the eval'd topics.env lists. Globals, not an
# associative array: this file's `#!/usr/bin/env bash` resolves against the INHERITED PATH (the
# export at the top of this script runs too late to matter), so under launchd it can be /bin/bash
# 3.2, which has neither `declare -A` nor namerefs. Mirrors apply-topics.sh's topic_cleanup_policy /
# topic_retention_ms / topic_delete_retention_ms so a dev clean and the canonical applier cannot
# disagree about the values BOTH derive from topics.env.
#
# SCOPE, stated so nobody reads more assurance into the check below than it gives: this covers
# cleanup.policy, the per-topic retention.ms / delete.retention.ms OVERRIDES, and partition count —
# the things topics.env alone determines. It does NOT cover the GLOBAL retention.ms or
# min.insync.replicas that apply-topics.sh stamps on every topic, because those come from the
# deploy's own environment (RETENTION_MS / MIN_ISR), which this script has no access to. A topic
# left at a wrong global retention — e.g. by a cleanup-topics.sh retention-shrink run that never
# restored it — is therefore still invisible here and is repaired by the next real deploy.
topic_desired() {
  local name="$1" spec entry
  DPOL=delete; DPARTS=32; DRET=""; DDR=""; DMB=""; DRB=""
  for spec in $OPTIONS_EDGE_TOPICS; do
    [ "${spec%%:*}" = "$name" ] && [ -n "${spec##*:}" ] && DPARTS="${spec##*:}"
  done
  # PURE compact only for the pure-compact list, compact,delete for the ordinary compacted list,
  # delete otherwise. This used to collapse to a bare `compact` for EVERY compacted topic, so a wipe
  # rebuilt them with a policy the canonical applier never assigns — and the es-indicator mirror job,
  # which asserts cleanup.policy=compact,delete before it will install, refuses such a topic.
  case " $OPTIONS_EDGE_COMPACTED_TOPICS " in *" $name "*) DPOL=compact,delete ;; esac
  case " ${OPTIONS_EDGE_PURE_COMPACT_TOPICS:-} " in *" $name "*) DPOL=compact ;; esac
  # Per-topic retention override. Without this a wipe rebuilt every override-carrying topic on the
  # dev broker default (retention.ms=-1), i.e. unbounded — the opposite of the declaration.
  for entry in ${OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES:-}; do
    [ "${entry%%=*}" = "$name" ] && DRET="${entry#*=}"
  done
  # Tombstone survival (R-WIRE.2): the recreate path must honour the SAME per-topic
  # delete.retention.ms contract apply-topics.sh enforces — without it every nightly wipe
  # silently stripped the 48h guarantee.
  for entry in ${OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES:-}; do
    [ "${entry%%=*}" = "$name" ] && DDR="${entry#*=}"
  done
  # Per-topic max.message.bytes (break-classifier's checkpoint must accept >= 8 MiB records; the dev broker default is 1 MiB and stays 1 MiB: the limit is topic-level). Without it a wipe would rebuild that
  # topic at the broker default and the service would refuse to start.
  for entry in ${OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES:-}; do
    [ "${entry%%=*}" = "$name" ] && DMB="${entry#*=}"
  done
  # Per-topic retention.bytes cap. topics.env has declared these since 2026-10-25 (futures.level-absorption.events=81675540, es.tape-zones.board,
  # early-auction.verdicts.shadow, futures.break-score.events, futures.liquidity-wall.events, trading.order.*), but this script never read the list, so
  # every clean rebuilt each of those topics UNCAPPED. level-absorption-service refuses to start unless the broker reports exactly its artifact's
  # integer (EVENTS_TOPIC_RETENTION_MISMATCH), so it crashlooped after every clean and was fixed by hand each time. -1 is the broker default
  # (unlimited): a topic that inherits it already satisfies a declared -1 (see reconcile_declared_topics).
  for entry in ${OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES:-}; do
    [ "${entry%%=*}" = "$name" ] && DRB="${entry#*=}"
  done
}

# reconcile_declared_topics: ONE bulk describe, then fix only the SCOPED fields that differ
# (cleanup.policy + the per-topic retention overrides; partition count is reported, never changed).
#
# This is the race guard for those fields. `--create --if-not-exists` no-ops silently on an existing topic, and the
# create loop above is ~130 Kafka CLI JVM launches long, so an external producer can auto-create its
# topic at ANY point during it — a snapshot taken before the loop cannot see that, which is why this
# is a pass AFTER the loop rather than a per-topic check inside it. It runs for every caller, so a
# transient failure is retried by the next start instead of surviving unnoticed.
#
# Config drift is repaired here with the same incremental add-config the canonical applier makes:
# idempotent, but not free of consequence — raising a topic from an inherited retention.ms=-1 to a
# declared 7d window will purge segments older than that window, and a consumer lagging beyond it
# gets OffsetOutOfRange. That is the declared contract being applied, and dev is entitled to it.
# Partition drift is NOT repaired: Kafka only grows partition counts, and
# ensure_topics also runs from do_start/do_start_overnight where a delete+recreate would destroy
# live data. It is reported loudly instead and left to apply-topics.sh, which fails closed on
# OPTIONS_EDGE_EXACT_PARTITION_TOPICS and carries the gated destructive repair.
reconcile_declared_topics() {
  local desc line spec name cfg have_pol have_ret have_dr have_mb have_rb have_parts need polcfg lowers fixed=0 drift=0
  desc="$($KT --bootstrap-server $BS --describe 2>/dev/null | grep '^Topic:')"
  if [ -z "$desc" ]; then
    # NOT VERIFIED IS NOT VERIFIED OK. This returned 0 while saying the shapes were not checked,
    # so an undescribable cluster passed the shape check and the caller scaled up on it. "I could
    # not tell" must not read as "it is fine" — the same substitution this round removed in three
    # other places.
    echo "  WARNING: could not describe topics — declared shapes NOT verified this run."
    return 1
  fi
  for spec in $OPTIONS_EDGE_TOPICS; do
    name="${spec%%:*}"
    line="$(printf '%s\n' "$desc" | awk -v t="$name" '$2==t && /PartitionCount:/ {print; exit}')"
    if [ -z "$line" ]; then
      echo "  WARNING: declared topic $name is ABSENT right after create — apps may recreate it with broker defaults."
      drift=$((drift+1)); continue
    fi
    topic_desired "$name"
    have_parts="$(printf '%s' "$line" | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
    # cleanup.policy is matched longest-first because "compact,delete" contains "compact"; an empty
    # result means the topic carries no override and inherits the broker default (delete).
    case "$line" in
      *cleanup.policy=compact,delete*) have_pol=compact,delete ;;
      *cleanup.policy=compact*)        have_pol=compact ;;
      *cleanup.policy=delete*)         have_pol=delete ;;
      *)                               have_pol="" ;;
    esac
    # delete.retention.ms ENDS IN retention.ms, so mask it before reading retention.ms itself.
    cfg="$(printf '%s' "$line" | sed 's/delete\.retention\.ms/DELRET/g')"
    have_ret="$(printf '%s' "$cfg" | sed -n 's/.*[,: ]retention\.ms=\(-\{0,1\}[0-9]*\).*/\1/p')"
    have_dr="$(printf '%s' "$cfg" | sed -n 's/.*DELRET=\([0-9]*\).*/\1/p')"
    have_mb="$(printf '%s' "$cfg" | sed -n 's/.*[,: ]max\.message\.bytes=\([0-9]*\).*/\1/p')"
    have_rb="$(printf '%s' "$cfg" | sed -n 's/.*[,: ]retention\.bytes=\(-\{0,1\}[0-9]*\).*/\1/p')"
    need=""
    case "$DPOL" in
      # A declared-plain topic that simply inherits the broker default is CORRECT — only an
      # explicitly wrong policy (e.g. left compacted) is drift. Without this the pass would rewrite
      # ~110 plain topics on every single run.
      delete) [ -n "$have_pol" ] && [ "$have_pol" != delete ] && need="cleanup.policy=delete" ;;
      *)      if [ "$have_pol" != "$DPOL" ]; then
                case "$DPOL" in *,*) polcfg="[$DPOL]" ;; *) polcfg="$DPOL" ;; esac
                need="cleanup.policy=$polcfg"
              fi ;;
    esac
    [ -n "$DRET" ] && [ "$have_ret" != "$DRET" ] && need="$need${need:+,}retention.ms=$DRET"
    [ -n "$DDR" ]  && [ "$have_dr"  != "$DDR"  ] && need="$need${need:+,}delete.retention.ms=$DDR"
    [ -n "$DMB" ]  && [ "$have_mb"  != "$DMB"  ] && need="$need${need:+,}max.message.bytes=$DMB"
    # retention.bytes is reconciled NON-DESTRUCTIVELY. ensure_topics also runs from start/overnight/topics on a live, shared dev, and lowering an
    # existing cap (or capping an unlimited topic) makes the broker delete retained records, which the retention.ms note above does not cover and which the
    # topics' own barriers refuse (ensure-level-absorption-topics.sh never lowers; ensure-es-liquidity-wall-topics.sh never alters an existing cap). So:
    #   * a declared -1 that the topic inherits is satisfied (no per-run rewrite of the unlimited topics);
    #   * a topic with a finite cap BELOW the declaration is raised, and an explicit finite cap on a declared -1 topic is lifted (neither can delete);
    #   * a topic that is uncapped or capped ABOVE the declaration is only REPORTED, unless DEV_CLEANUP_LOWER_RETENTION_BYTES=1.
    # The wipe does not depend on this pass: a recreated topic gets its cap from the create above.
    if [ -n "$DRB" ] && [ "$have_rb" != "$DRB" ] && ! { [ "$DRB" = -1 ] && [ -z "$have_rb" ]; }; then
      lowers=0
      if [ "$DRB" != -1 ]; then
        if [ -z "$have_rb" ] || [ "$have_rb" = -1 ]; then lowers=1
        elif [ "$have_rb" -gt "$DRB" ] 2>/dev/null; then lowers=1; fi
      fi
      if [ "$lowers" = 0 ] || [ "${DEV_CLEANUP_LOWER_RETENTION_BYTES:-0}" = 1 ]; then
        need="$need${need:+,}retention.bytes=$DRB"
      else
        echo "  WARNING: $name has retention.bytes=${have_rb:-unlimited}, declared $DRB — NOT lowered (the broker would delete retained records). A recreate applies it; to lower in place run with DEV_CLEANUP_LOWER_RETENTION_BYTES=1."
        drift=$((drift+1))
      fi
    fi
    if [ -n "$need" ]; then
      # `--add-config` separates entries with commas, so a comma-BEARING value must be bracketed
      # (cleanup.policy=[compact,delete]) or the broker reads "delete" as a second, malformed entry.
      # apply-topics.sh does the same in kafka_config_value(); the `--create --config` form needs no
      # brackets because each --config is already its own argv entry.
      if $KC --bootstrap-server $BS --entity-type topics --entity-name "$name" --alter \
           --add-config "$need" >/dev/null 2>&1; then
        echo "  reconciled $name -> $need"
        fixed=$((fixed+1))
      else
        echo "  WARNING: could not reconcile $name -> $need"
        drift=$((drift+1))
      fi
    fi
    # Partition verdict must use the SAME rule apply-topics.sh uses, or this pass cries wolf. The
    # canonical applier treats EXCESS partitions as compatible (more parallelism) for ordinary
    # topics and demands equality only for OPTIONS_EDGE_EXACT_PARTITION_TOPICS. Warning about
    # something no tool will ever repair just teaches the reader to skip the warnings that matter.
    case " ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-} " in
      *" $name "*)
        if [ "$have_parts" != "$DPARTS" ]; then
          echo "  WARNING: EXACT-partition topic $name has $have_parts partitions, declared $DPARTS — a producer beat the create through auto.create.topics.enable. Kafka cannot shrink partitions, so apply-topics.sh FAILS CLOSED on this until an operator runs the gated destructive repair (KAFKA_RECREATE_MISMATCHED_TOPICS=true)."
          drift=$((drift+1))
        fi ;;
      *)
        if [ "$have_parts" -lt "$DPARTS" ] 2>/dev/null; then
          echo "  WARNING: topic $name has $have_parts partitions, declared at least $DPARTS — a GATED deploy grows it (apply-topics.sh needs KAFKA_RECREATE_MISMATCHED_TOPICS=true; without it the deploy fails closed)."
          drift=$((drift+1))
        fi ;;
    esac
  done
  echo "  shape check: $fixed config(s) reconciled, $drift unresolved."
  # REPORTED, not only printed. This counted unresolved drift — an absent topic, a failed alter, an
  # exact-partition mismatch — and then returned 0 regardless, so every caller read success. That
  # made ensure_topics succeed on a drifted cluster and made the refusal added for Codex deploy r15
  # unable to fire for the failures it exists for: a guard on a status that is never non-zero
  # (Codex deploy r17).
  [ "$drift" -eq 0 ]
}

# create_topics_parallel: read one topic's create arguments per line on stdin ("--topic X --partitions N ...")
# and run the kafka-topics creates DEV_CLEANUP_TOPIC_PARALLELISM at a time; prints how many succeeded.
# WHY: every kafka-topics call is a ~2 s JVM start. ~200 sequential creates after a wipe cost 6-7 minutes of
# a dev clean; 8 in parallel cost well under one. Topic names and config values contain no spaces or quotes
# (Kafka topic names are [a-zA-Z0-9._-]), so xargs' whitespace splitting is exact.
create_topics_parallel() {
  # Trailing blanks are STRIPPED first: `xargs -L 1` treats a line that ends in a blank as continuing onto
  # the next line, so "--config cleanup.policy=delete  " (empty retention overrides) glued several topics into
  # one failing create — 2026-09-16 a dev wipe created 15 of 137 declared topics. Blank lines are dropped too.
  sed -e 's/[[:space:]]*$//' -e '/^$/d' | xargs -P "${DEV_CLEANUP_TOPIC_PARALLELISM:-8}" -L 1 sh -c \
    "$KT --bootstrap-server $BS --create --if-not-exists \"\$@\" >/dev/null 2>&1 && echo CREATED" sh \
    | grep -c '^CREATED$'
}

# ensure_partition_only_topics: create every OPTIONS_EDGE_PARTITION_ONLY_TOPICS entry that is missing at its
# declared partition count (no configs: the owning service stamps policy/retention), grow a smaller existing
# copy, and name any larger one (Kafka cannot shrink a topic; a wipe recreates it). Runs before any service
# starts, so no client can choose these topics' size. One list + one describe, not a JVM per topic.
ensure_partition_only_topics() {
  [ -n "${OPTIONS_EDGE_PARTITION_ONLY_TOPICS:-}" ] || return 0
  local have spec name want cur created=0 grown=0 larger="" failed=0
  local desc
  if ! desc="$($KT --bootstrap-server $BS --describe 2>/dev/null)"; then
    # NOT ENSURED IS NOT ENSURED OK — the same substitution as in reconcile_declared_topics, one
    # function over. Nothing was created blind, which is right; but reporting success for a run that
    # verified nothing let ensure_topics pass and the scale-up proceed (Codex deploy r18).
    echo "  WARNING: could not describe topics — partition-only topics NOT ensured this run (nothing created blind)"
    return 1
  fi
  have="$(printf '%s\n' "$desc" | awk -F'\t' '$1 ~ /^Topic: / {n=$1; sub(/^Topic: /, "", n); for (i = 2; i <= NF; i++) if ($i ~ /^PartitionCount: /) {c=$i; sub(/^PartitionCount: /, "", c); print n, c}}')"
  local to_create="" wanted=0
  for spec in $OPTIONS_EDGE_PARTITION_ONLY_TOPICS; do
    name="${spec%%:*}"; want="${spec##*:}"
    cur="$(printf '%s\n' "$have" | awk -v t="$name" '$1 == t {print $2; exit}')"
    if [ -z "$cur" ]; then
      to_create="$to_create--topic $name --partitions $want --replication-factor 1
"
      wanted=$((wanted+1))
    elif [ "$cur" -lt "$want" ]; then
      # --topic is a regular expression: escape the dots so the alter can only match this topic.
      $KT --bootstrap-server $BS --alter --topic "$(printf '%s' "$name" | sed 's/\./\\./g')" --partitions "$want" >/dev/null 2>&1 \
        && grown=$((grown+1)) || { echo "  WARNING: could not grow $name $cur -> $want"; failed=1; }
    elif [ "$cur" -gt "$want" ]; then
      larger="$larger $name($cur>$want)"
    fi
  done
  if [ "$wanted" -gt 0 ]; then
    created=$(printf '%s' "$to_create" | create_topics_parallel)
    [ "$created" -eq "$wanted" ] || { echo "  WARNING: created $created of $wanted missing partition-only topics"; failed=1; }
  fi
  echo "Partition-only topics: created $created, grown $grown (topics.env OPTIONS_EDGE_PARTITION_ONLY_TOPICS)"
  if [ -n "$larger" ]; then
    echo "  WARNING: larger than declared (Kafka cannot shrink; the next wipe recreates them):$larger"
    failed=1
  fi
  # EVERY WARNING ABOVE NOW CARRIES. A failed grow, an incomplete create and an irreconcilably
  # larger topic each printed and fell through with status 0, and reconcile_declared_topics cannot
  # catch them because it iterates OPTIONS_EDGE_TOPICS and these are in
  # OPTIONS_EDGE_PARTITION_ONLY_TOPICS (Codex deploy r18).
  [ "$failed" -eq 0 ]
}

# ensure_topics: pre-create the platform topics from the deploy repo's topics.env (source of truth).
# Best-effort fetch so we pick up the latest reviewed config; if offline we use the last-fetched origin/main.
#
# After creating, it VERIFIES every declared topic against one bulk describe and reconciles the
# config of any that does not match. That second pass is what makes this race-proof, and the
# reason it is a pass rather than an in-loop check: `--create --if-not-exists` no-ops silently on a
# topic that already exists, and the creating loop is ~130 Kafka CLI JVM launches long, so a
# still-running external producer can auto-create its topic at ANY point during it — including
# after any snapshot taken at the start. The pass runs on every caller, not just the wipe, so a
# transient failure is retried by the next start instead of surviving until someone notices.
# DEV-ONLY overnight ES-basis ESTIMATE topics (indicator-estimate sidecar). They are NOT in the deploy repo's topics.env on purpose (the estimate is
# dev-only; prod must not grow them), so a clean used to leave them for the sidecar to create AFTER context-tape-service had already started, and
# context-tape then subscribed only to the forming topic: the Order tab drew ONE candle that was replaced every minute (2026-10-21 20:3x ET).
# Pre-creating them here, before any service starts, removes the race at its source (context-tape also now picks a late topic up by itself).
# Same shapes the sidecar creates: bars = 1 partition / delete / 3 d retention, bars.current = 1 partition / compact / 1 d retention.
ensure_dev_only_estimate_topics() {
  # Retried: a Kafka admin endpoint that is briefly unavailable right after a restart must not leave the race open. A final failure is a LOUD warning
  # but NOT fatal: a failed ensure_topics would hold the es4 mirrors paused for an optional dev-only topic (context-tape also picks a late topic up by itself).
  local name policy retention i ok
  for spec in "options.indicators.estimate.bars delete 259200000" "options.indicators.estimate.bars.current compact 86400000"; do
    set -- $spec; name=$1; policy=$2; retention=$3; ok=0
    for i in 1 2 3; do
      if $KT --bootstrap-server $BS --create --if-not-exists --topic "$name" --partitions 1 --replication-factor 1 \
          --config cleanup.policy=$policy --config retention.ms=$retention >/dev/null 2>&1; then ok=1; break; fi
      sleep 2
    done
    if [ $ok = 1 ]; then echo "  dev-only estimate topic ready: $name"
    else echo "  WARNING: could not create $name after 3 tries — the Order tab may show one candle until context-tape-service is restarted"; fi
  done
}

ensure_topics() {
  [ -n "$KT" ] || require_kafka_cli
  git -C "$DEPLOY_REPO" fetch -q origin main 2>/dev/null || true
  # Inside a clean, the PINNED commit: the topics recreated after the wipe must come from the commit the
  # keep-lists were read from, and a push between the two would otherwise split them. Unset elsewhere
  # (`dev-cleanup.sh start` or `topics` on their own), where the ref is the only thing there is.
  local tref="${TOPICS_ENV_PINNED_REF:-$TOPICS_ENV_REF}"
  local tenv; tenv="$(git -C "$DEPLOY_REPO" show "$tref" 2>/dev/null)"
  if [ -n "$tenv" ]; then
    # INDENT-TOLERANT on purpose. A dev-only topic is declared in topics.env by APPENDING inside
    # `if [ "${ENVIRONMENT:-}" != "production" ]`, so its assignment is INDENTED; this grep was anchored at
    # column 0 and could not see one. On 2026-11-06 that left es.rithmic.mbo and
    # futures.liquidity-validator.{evaluations,data-gaps,daily} un-created after the wipe, and
    # es-liquidity-engine-service crash-looped on "es.rithmic.mbo must have exactly one partition, found 0"
    # until they were created by hand. The matched lines are eval'd in file order and each append reads the
    # variable it extends, so taking the indented ones too builds the value the file means.
    # THE LIMIT OF THIS, STATED PLAINLY: the `if` lines are not matched, so this eval applies an indented
    # declaration whatever its gate - a declaration inside `if ... = "production"` would be applied on dev, and
    # an indented REPLACEMENT assignment (one that does not read the variable it sets) could even shrink a list.
    # Nothing in this script can prevent that: it parses the file, it does not interpret it.
    # What guards it is a CONVENTION CHECK that runs on every PR (deploy-validation.yml ->
    # dev-cleanup-indented-declaration-test.sh section 4, with -mutation-test.sh holding it to 23 committed shapes - coverage of the shapes we know of, not a proof of impossibility):
    # an indented declaration is accepted only inside one of the two gate forms topics.env uses for a dev-only
    # block, with only comments, blanks and other accepted declarations between gate and declaration, and only
    # as a plain `VAR="$VAR tokens"` append. Everything else - a production gate, else/elif, a nested block, a
    # here-doc body, a continuation, a replacement or a shrink - fails that job. It is a convention check, not a
    # shell parser: it cannot prove what this eval does with a file written in some other style, it can only
    # stop that style landing unnoticed.
    eval "$(printf '%s\n' "$tenv" | grep -E '^[[:space:]]*OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS|PURE_COMPACT_TOPICS|EXACT_PARTITION_TOPICS|TOPIC_RETENTION_OVERRIDES|TOPIC_DELETE_RETENTION_OVERRIDES|TOPIC_MAX_MESSAGE_BYTES_OVERRIDES|TOPIC_RETENTION_BYTES_OVERRIDES|PARTITION_ONLY_TOPICS)=')"
    local n=0 spec name extra rt existing missing=0
    # One --list, then create only what is missing (in parallel). A failed list falls back to creating every
    # declared topic: --if-not-exists makes that safe, only slower. Config of EXISTING topics is the
    # reconcile pass's job below, exactly as before.
    # A list that cannot be read is NOT "no topics": creating "everything" against a broker that did not answer would only hide that it did not.
    existing="$(kafka_list_topics)" || {
      echo "  ERROR: kafka-topics --list failed or printed nothing after retries ($BS); no topic was created or reconciled. The es4 mirrors stay paused and apps will create their topics on startup -- re-run 'dev-cleanup.sh topics' once the broker answers." >&2
      return 1; }
    n=$(for spec in $OPTIONS_EDGE_TOPICS; do
      name="${spec%%:*}"
      printf '%s\n' "$existing" | grep -qxF "$name" && continue
      topic_desired "$name"
      extra=""; [ -n "$DDR" ]  && extra="--config delete.retention.ms=$DDR"
      rt="";    [ -n "$DRET" ] && rt="--config retention.ms=$DRET"
      [ -n "$DMB" ] && extra="$extra --config max.message.bytes=$DMB"
      [ -n "$DRB" ] && extra="$extra --config retention.bytes=$DRB"
      echo "--topic $name --partitions $DPARTS --replication-factor 1 --config cleanup.policy=$DPOL $extra $rt"
    done | create_topics_parallel)
    echo "Pre-created $n platform topics from deploy config ($tref); apps self-create the rest on startup."
    local rc=0
    ensure_partition_only_topics || rc=1
    # The shape check's verdict now carries. Before this it was printed and dropped.
    reconcile_declared_topics || rc=1
    ensure_dev_only_estimate_topics || rc=1
    echo "  topics present now: $(topic_count '^__|^_schemas' v)"
    if [ "$rc" -ne 0 ]; then
      echo "  topics: at least one declared topic is MISSING or DRIFTED — see the warnings above."
      return 1
    fi
  else
    echo "  WARNING: could not read deploy topics.env ($DEPLOY_REPO $tref) — apps will create their topics on startup (slower to READY)."
    return 1   # resume_dev_mirrors_if_declared keeps the mirrors paused: nothing was created at its declared shape
  fi
}

# Reload the paused mirrors only once ensure_topics has created their targets; otherwise they would
# auto-create them at 1 partition — the exact wedge the pause exists to prevent. They stay paused
# (list kept) until a later start/overnight run whose ensure_topics succeeds.
resume_dev_mirrors_if_declared() {
  if [ "$1" -eq 0 ]; then
    if ! resume_dev_mirrors; then
      # LOUD, because silent is exactly what happened. With the mirrors down, es4 -> dev carries
      # nothing and every downstream dev service reads a frozen topic while reporting healthy.
      echo "   ERROR: es4->dev mirrors did NOT all come back RUNNING — dev will read FROZEN es4"
      echo "          topics until they do. Recover with:"
      echo "            launchctl load -w ~/Library/LaunchAgents/com.optionsedge.*127-0-0-1-19092*.plist"
      echo "          then confirm each has a PID (not '-'): launchctl list | grep 127-0-0-1-19092"
      # RECORDED, so the script's own exit status says it. My first version ended this branch on an
      # echo, so the wrapper returned 0 and every caller — and cron, and Jenkins — still saw success
      # while the mirrors were down. That is the same "a failure nobody can read" shape as the bug
      # being fixed, one level up.
      DEV_MIRRORS_RESUME_FAILED=1
      return 1
    fi
  elif [ -s "$DEV_MIRRORS_PAUSED" ]; then
    echo "   WARN: es4->dev mirrors stay PAUSED — topics.env was not applied (list: $DEV_MIRRORS_PAUSED)"
  fi
}

# ensure_topics_or_refuse: recreate the declared topics and REFUSE to go further if that failed.
#
# The old form was `ensure_topics` followed by `resume_dev_mirrors_if_declared "$?"`, and it discarded
# the failure. The resume helper takes the status as an argument and returns 0 whenever it is
# non-zero — deliberately, so a failed topic pass does not strand the es4 mirrors — but the CALLER
# then read 0 and carried on into the scale-up. So a wipe that deleted every topic and then failed to
# recreate them was followed by services coming up against topics that did not exist: Kafka Streams
# apps crash-loop, and anything with auto-creation gets the broker DEFAULT shape, which for
# es.mbo.coverage means a delete-retention log where a compacted per-contract fact belongs. The
# barrier that would catch that shape runs at DEPLOY time, not here (Codex deploy r15).
#
# The mirror behaviour is unchanged. What changes is that the caller can no longer mistake a failed
# recreation for a successful one.
ensure_topics_or_refuse() {
  local rc=0
  ensure_topics || rc=$?
  resume_dev_mirrors_if_declared "$rc"
  if [ "$rc" -ne 0 ]; then
    echo "ERROR: ensure_topics FAILED (status $rc) - the declared topics are NOT all present at their" >&2
    echo "       declared shapes. Nothing may be scaled up on this: a workload started against a" >&2
    echo "       missing topic crash-loops, and anything auto-created takes the broker DEFAULT shape" >&2
    echo "       instead of the one scripts/kafka/topics.env declares (for es.mbo.coverage, a" >&2
    echo "       delete-retention log where a compacted per-contract fact belongs)." >&2
    echo "       The es4->dev mirrors stay PAUSED by design until a later run's topic pass succeeds." >&2
    return 1
  fi
}

# RETURNS, NEVER EXITS. The first version called `exit 1` here, which killed the whole process — and
# on the automatic CLEAN slot the very next act is the overnight ES start, i.e. the script's own
# recovery attempt (a second topic pass). Exiting inside the helper skipped it, so a failed
# recreation left dev fully down with the mirrors paused until the next scheduled full start, which
# on a manual run is never (Codex deploy r16). The same shape as the bug being fixed: a status that
# stops the wrong thing. Each caller now refuses its OWN scale-up and the CLEAN slot still reaches
# its recovery.

# run_partition_doctor: repair Kafka Streams apps that refuse an internal topic's partition count
# ("Existing internal topic ... has invalid partitions: expected: E; actual: A"). Same source of truth as
# ensure_topics (the reviewed script on origin/main). Waits for the named deployments to be READY (or up
# to DOCTOR_WAIT_SECONDS) so Streams has had its first assignment — the rejection is logged only then.
# Non-fatal for the bring-up, loud in the log: an app it cannot repair is named with the reason.
run_partition_doctor() {
  local script deploys="$*" d waited=0 rcd
  script="$(mktemp -t oe-partition-doctor)" || return 0
  git -C "$DEPLOY_REPO" show "${PARTITION_DOCTOR_REF:-origin/main:scripts/kafka/streams-partition-doctor.sh}" > "$script" 2>/dev/null
  if [ ! -s "$script" ]; then
    echo "  WARNING: could not read streams-partition-doctor.sh from $DEPLOY_REPO — Streams apps with a wrong internal topic stay NOT READY"
    rm -f "$script"; return 0
  fi
  [ -n "$deploys" ] || deploys=$($KK get deploy -o jsonpath='{range .items[?(@.spec.replicas>0)]}{.metadata.name}{" "}{end}' 2>/dev/null)
  # Wait for the apps to become READY (Streams logs a rejection at its first assignment), but stop as soon
  # as the not-ready set has not changed for DOCTOR_STABLE_SECONDS: an app that cannot start for an
  # unrelated reason (no IBKR gateway, an epoch fence, a feed that starts at 07:00 ET) must not hold the
  # whole bring-up for the full timeout. One `get deploy` per pass, not two kubectl calls per app.
  local pending="" last="" stable=0
  while [ "$waited" -lt "${DOCTOR_WAIT_SECONDS:-300}" ]; do
    pending=$($KK get deploy -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.replicas}{" "}{.status.readyReplicas}{"\n"}{end}' 2>/dev/null \
      | awk -v want=" $deploys " 'index(want, " " $1 " ") && ($3 == "" ? 0 : $3) < $2 {printf "%s ", $1}')
    [ -z "$pending" ] && break
    if [ "$pending" = "$last" ]; then stable=$((stable + 15)); else stable=0; last="$pending"; fi
    [ "$stable" -ge "${DOCTOR_STABLE_SECONDS:-60}" ] && { echo "  not ready (unchanged ${stable}s, not waiting longer): $pending"; break; }
    sleep 15; waited=$((waited + 15))
  done
  sleep "${DOCTOR_SETTLE_SECONDS:-30}"
  echo "Kafka Streams partition doctor (repairs 'invalid partitions' internal topics) ..."
  KUBECTL="$KK" KUBECTL_SCALE="$K" KAFKA_TOPICS="$KT --bootstrap-server $BS" /bin/bash "$script" --repair $deploys 2>&1 | sed 's/^/  /'
  rcd=${PIPESTATUS[0]}
  [ "$rcd" -eq 0 ] || echo "  ⚠ partition doctor exit $rcd — see the UNREPAIRABLE/FAILED/INCONCLUSIVE lines above"
  rm -f "$script"
}

# apply_internal_topic_configs: give every Streams changelog/repartition topic its
# compaction + retention policy, the SAME way the monolith deploy does.
#
# Why this exists (2026-07-30): the deploy's Jenkinsfile runs
# scripts/kafka/apply-internal-topic-configs.sh, but a dev *clean* does not — and a
# clean is what recreates the topics. Streams internal topics therefore came back
# with nothing but broker defaults (cleanup.policy=delete, log.retention.hours=24,
# log.segment.bytes=1G) and stayed that way until the next full deploy, which can be
# days. Measured cost on 2026-07-30: ALL 52 dev changelogs were plain `delete`, dev
# Kafka grew at 176.8 GB/hour (2.19 GB pre-open -> 29.5 GB by 10:05 ET), and the disk
# was ~2.3 h from full. Applying compaction dropped it to ~6 GB/hour.
#
# Runs the reviewed script from origin/main rather than a local copy (same source of
# truth as ensure_topics), through a PATH shim because that script calls the bare
# `kafka-topics` / `kafka-configs` names that exist inside the deploy image but not
# on the Mac. Best-effort: never fail a clean over this.
apply_internal_topic_configs() {
  local script shim
  script="$(mktemp -t oe-internal-topics)" || return 0
  git -C "$DEPLOY_REPO" show "${INTERNAL_TOPIC_SCRIPT_REF:-origin/main:scripts/kafka/apply-internal-topic-configs.sh}" \
    > "$script" 2>/dev/null
  if [ ! -s "$script" ]; then
    echo "  WARNING: could not read apply-internal-topic-configs.sh from $DEPLOY_REPO — Streams internal topics keep broker defaults (delete policy, 1 GB segments). Dev Kafka will grow fast."
    rm -f "$script"; return 0
  fi
  shim="$(mktemp -d -t oe-kafka-shim)" || { rm -f "$script"; return 0; }
  # Wrappers, NOT symlinks. Kafka's CLI scripts resolve their sibling kafka-run-class.sh from
  # their OWN directory ($(dirname $0)), so a symlink into a temp dir makes them look for
  # kafka-run-class.sh inside the shim, where it does not exist:
  #   oe-kafka-shim.XXXX/kafka-topics: line 17: .../kafka-run-class.sh: No such file or directory
  # The failure was quiet — the script then reported "No Kafka Streams internal topics found yet"
  # and the clean finished green while EVERY changelog kept broker defaults (delete policy,
  # 1 GB segments), which is the 177 GB/hour growth this function exists to prevent.
  # Observed on the 2026-08-03 dev clean. Exec'ing the real path keeps $0 correct.
  for tool in kafka-topics kafka-configs; do
    printf '#!/usr/bin/env bash\nexec "%s/%s.sh" "$@"\n' "$(dirname "$KT")" "$tool" > "$shim/$tool"
    chmod +x "$shim/$tool"
  done
  # Prove the shim actually works before trusting it — a broken wrapper must fail LOUD, not
  # silently leave every changelog unconfigured.
  if ! "$shim/kafka-topics" --bootstrap-server "$BS" --list >/dev/null 2>&1; then
    echo "  WARNING: kafka CLI shim is not functional — Streams internal topics keep broker defaults (delete policy, 1 GB segments). Dev Kafka will grow fast."
    rm -rf "$shim" "$script"; return 0
  fi
  echo "Applying Streams internal-topic policy (compaction + 1h segments) ..."
  # 10h retention = dev's KAFKA_MAX_RETENTION_MS cap (the Jenkinsfile derives the same
  # value from the rendered configmap); 1h segments so compaction/retention/Streams'
  # own repartition purge can actually reclaim — none of them touch the ACTIVE segment,
  # which at the 1 GB broker default means nothing is reclaimable in-session.
  PATH="$shim:$PATH" \
  KAFKA_BOOTSTRAP_SERVERS="$BS" \
  KAFKA_TOPIC_MIN_IN_SYNC_REPLICAS=1 \
  KAFKA_CHANGELOG_RETENTION_MS="${DEV_CHANGELOG_RETENTION_MS:-36000000}" \
  KAFKA_STREAMS_INTERNAL_RETENTION_MS="${DEV_CHANGELOG_RETENTION_MS:-36000000}" \
  KAFKA_STREAMS_INTERNAL_SEGMENT_MS=3600000 \
  KAFKA_CHANGELOG_DELETE_RETENTION_MS=3600000 \
  KAFKA_CHANGELOG_MIN_CLEANABLE_DIRTY_RATIO=0.01 \
    bash "$script" 2>&1 | tail -3 | sed 's/^/  /'
  rm -rf "$shim" "$script"
}

# ---------- OVERNIGHT START: right after the clean, bring up ONLY the ES-tracking set ----------
# Recreate topics, then scale up ONLY $OVERNIGHT_SET so ES futures are tracked overnight; every other
# service stays at 0 until the 06:15 ET full start. (These persist/serve ES — they need a producer for
# live ES data; see the note where OVERNIGHT_SET is defined.)
do_start_overnight() {
  require_kafka_cli
  ensure_topics_or_refuse || {
    echo "overnight start: REFUSED — the declared topics are not right, so the ES-tracking set stays at 0." >&2
    echo "                 Fix the topic pass and re-run 'dev-cleanup overnight'." >&2
    return 1
  }
  echo "Overnight start: ES-tracking set only ($OVERNIGHT_SET); all other services stay at 0 until 06:15 ET."
  local d
  for d in $OVERNIGHT_SET; do
    if $KK get deploy "$d" >/dev/null 2>&1; then
      $K scale deploy/"$d" --replicas=1 >/dev/null 2>&1 && echo "  up: $d"
    else
      echo "  (absent, skipped): $d"
    fi
  done
  echo "Overnight ES-tracking set up."
  run_partition_doctor $OVERNIGHT_SET
}

# ---------- ES DOWN: at ~09:17 ET (before the 09:30 open) scale the overnight ES services to 0 ----------
# The overnight ES session is over; feed-gateway + web stay up for the SPX day session (untouched here).
do_es_down() {
  require_kafka_cli
  echo "ES-down (pre-open): scaling overnight ES services to 0 ($ES_DOWN_SET) ..."
  local d
  for d in $ES_DOWN_SET; do
    if $KK get deploy "$d" >/dev/null 2>&1; then
      $K scale deploy/"$d" --replicas=0 >/dev/null 2>&1 && echo "  down: $d"
    else
      echo "  (absent, skipped): $d"
    fi
  done
  # Clean the transient ES Kafka topics (services are down -> nothing recreates them until the 17:20 clean).
  # Postgres es_* training/ledger tables are NOT touched.
  echo "ES-down: cleaning transient ES topics ($ES_CLEAN_TOPICS) ..."
  local t
  for t in $ES_CLEAN_TOPICS; do
    if $KT --bootstrap-server $BS --describe --topic "$t" >/dev/null 2>&1; then
      $KT --bootstrap-server $BS --delete --topic "$t" >/dev/null 2>&1 && echo "  cleaned topic: $t"
    fi
  done
  echo "ES-down complete (Postgres es_* training tables preserved)."
}

# ---------- FULL START: pre-create source topics, then scale ALL active apps up READY ----------

# indicator-service owns its own epoch recovery: on a non-prod profile whose durable state topics were never
# written (a wipe), the service mints the epoch itself and logs "EPOCH GENESIS SELF-AUTHORIZED". This script no
# longer toggles the epoch-genesis env flag and no external helper job exists (2026-11-02: the old toggle and its
# launchd job both read the wrong container and never fired, leaving dev without bars for ~7 h).
#
# pod_ready_report NAME -> prints "NAME ready/total containers" for its pod and returns 0 only when a pod exists
# with EVERY container ready. The containers are named by the pod, never read through `kubectl logs deploy/...`
# (a bare deploy log read picks the FIRST container).
pod_ready_report() {
  local name=$1 lines line ts flags pods=0 bad=0 ready=0 total=0 f
  # one line per pod: "<deletionTimestamp>|<ready flag per container>". A TERMINATING pod (deletionTimestamp set) is
  # a rollout leftover and is ignored; EVERY remaining pod must have every container ready, so an old ready pod can
  # never vouch for a new one that is not (and a pod with no container statuses yet counts as not ready).
  lines=$($KK get pods -l "app.kubernetes.io/name=$name" \
    -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.containerStatuses[*].ready}{"\n"}{end}' 2>/dev/null)
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    ts=${line%%|*}; flags=${line#*|}
    [ -n "$ts" ] && continue
    pods=$((pods + 1))
    [ -n "$flags" ] || bad=1
    for f in $flags; do
      total=$((total + 1))
      if [ "$f" = true ]; then ready=$((ready + 1)); else bad=1; fi
    done
  done <<EOF2
$lines
EOF2
  echo "$name $ready/$total containers ready ($pods pod(s))"
  [ "$pods" -gt 0 ] && [ "$bad" -eq 0 ]
}

# verify_start_readiness: indicator-service must reach all containers ready (2/2: indicator-estimate +
# indicator-service) or `start` FAILS LOUDLY (non-zero). market-structure-service readiness is only REPORTED:
# it can legitimately wait for its schedule outside the session, so it is not a failure by itself.
verify_start_readiness() {
  local i rep rc=0
  if echo "$DISABLED_DEV" | grep -qxE "indicator-service" 2>/dev/null; then
    echo "  indicator-service is in the held-down set: readiness check skipped"
  else
    for i in $(seq 1 "${INDICATOR_READY_POLLS:-60}"); do
      rep=$(pod_ready_report indicator-service) && break
      sleep "${INDICATOR_READY_SLEEP:-5}"
    done
    if pod_ready_report indicator-service >/dev/null; then
      echo "  OK  $(pod_ready_report indicator-service)"
    else
      echo "  ##### FAILED: indicator-service did NOT become fully ready: $(pod_ready_report indicator-service) #####" >&2
      echo "  It owns its epoch recovery; read the container: kubectl -n options-edge logs deploy/indicator-service -c indicator-service --tail=100" >&2
      rc=1
    fi
  fi
  echo "  INFO $(pod_ready_report market-structure-service || true)  (reported only)"
  return $rc
}

# session_bound_line DEPLOY CONTAINER LOG_PATTERN WAITS_FOR: one line saying whether DEPLOY is ready, and when it is
# not, whether the pod log CONFIRMS the known session-bound condition (LOG_PATTERN). Three outcomes, never conflated:
#   WAIT  the log was read and shows the condition  -> WAITS_FOR says what it needs and when that arrives;
#   ???   the log was read and does NOT show it     -> not confirmed: something else is wrong, read the log;
#   ???   the log could not be read (RBAC, no such container, pod gone) -> not confirmed, and says so: an unreadable
#         log is not evidence that the condition is absent, so it never claims the cause is unrelated.
# It states the CONDITION found, not that a wipe caused it: `start` also runs on its own, with no wipe before it.
# Prints nothing for a held-down or absent deployment. Reported only: never changes the exit status.
session_bound_line() {
  local d=$1 c=$2 pat=$3 waits=$4 des rr cur prev crc
  des=$($KK get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null) || return 0
  rr=$($KK get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null) || rr=0
  [ "${des:-0}" -gt 0 ] || return 0
  if [ "${rr:-0}" -ge "${des:-0}" ]; then echo "  OK   $d ready"; return 0; fi
  cur=$($KK logs "deploy/$d" -c "$c" 2>/dev/null); crc=$?
  prev=$($KK logs "deploy/$d" -c "$c" --previous 2>/dev/null) || prev=""   # no previous container is normal
  if printf '%s\n%s\n' "$cur" "$prev" | grep -qE "$pat"; then
    echo "  WAIT $d NOT ready; its log shows a known session-bound condition: $waits"
  elif [ "$crc" -ne 0 ]; then
    echo "  ???  $d NOT ready and its log could NOT be read (kubectl logs rc=$crc), so the known session-bound condition ('$pat') is neither confirmed nor ruled out: kubectl -n options-edge logs deploy/$d -c $c --tail=100"
  else
    echo "  ???  $d NOT ready and its log does NOT show the known session-bound condition ('$pat'): read kubectl -n options-edge logs deploy/$d -c $c --tail=100"
  fi
}

# report_session_bound_services: two services legitimately cannot be ready until a session supplies their input. Say so
# by name and condition instead of leaving them as unexplained 0/1 in a readiness list (the 2026-11-10 bring-up did,
# and both were chased as faults). Reported only.
report_session_bound_services() {
  echo "Session-bound services (cannot be ready until a session supplies their input):"
  session_bound_line es-amt-service es-amt-service 'ES_AMT_RETENTION_SHORT' \
    "the prior session's ES tape (underlying.es.trades) is missing -- every clean wipes that topic; it clears at the next 18:00 ET roll once a full session is in it"
  session_bound_line market-structure-service market-structure 'this session has already ended' \
    "its bootstrap date (today, ET) is a session that has already ended, so it exits; it starts once the ET date rolls, then waits AWAITING_SCHEDULE for the first bar of that session (the estimate container is separate and unaffected)"
}

do_start() {
  require_kafka_cli
  ensure_topics_or_refuse || {
    echo "start: REFUSED — the declared topics are not right, so nothing is scaled up." >&2
    echo "       Fix the topic pass and re-run 'dev-cleanup start'." >&2
    return 1
  }
  # Scale UP everything EXCEPT the DEV-disabled set. This is load-bearing: if we scaled ALL to 1 and
  # re-zeroed the disabled ones afterwards, databento-timewarp-snapshot-replay would come up in the gap
  # and REPLAY historical snapshots into options.databento.raw (its TIMEWARP_SNAPSHOT_TOPIC), poisoning
  # the freshly-cleaned chain with stale replay data. So disabled services are pinned to 0 from the start.
  echo "Scaling deployments up (disabled set stays at 0) ..."
  for d in $($KK get deploy -o name 2>/dev/null | sed 's#.*/##'); do
    if echo "$d" | grep -qxE "$DISABLED_DEV"; then
      $K scale deploy/"$d" --replicas=0 >/dev/null 2>&1
    else
      $K scale deploy/"$d" --replicas=1 >/dev/null 2>&1
    fi
  done

  # Self-heal boot-order races: because we pre-create ONLY the topics.env platform topics (not all 90),
  # a few Streams apps that enforce topic retention at startup can die (Streams ERROR, process hung,
  # readiness fails, k8s does NOT auto-restart) if one of their own output topics isn't created yet —
  # the app/peer creates it moments later, so a single restart brings them up clean. Settle, then
  # restart any active deploy still not Ready. Idempotent + bounded (one restart pass).
  echo "Settling (90s), then self-healing any boot-order stragglers ..."
  sleep 90
  healed=0
  for d in $($KK get deploy -o name 2>/dev/null | sed 's#.*/##'); do
    echo "$d" | grep -qxE "$DISABLED_DEV" && continue
    des=$($KK get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null)
    rr=$($KK get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
    if [ "${des:-0}" -gt 0 ] && [ "${rr:-0}" -lt "${des:-0}" ]; then
      echo "  boot-order straggler: $d (${rr:-0}/${des}) — restarting"
      $K rollout restart deploy/"$d" >/dev/null 2>&1; healed=$((healed+1))
    fi
  done
  echo "  self-heal restarted $healed straggler(s)"

  # Schema self-heal: all Schema-Registry schemas live in the compacted _schemas topic; if it gets
  # wiped (the known dev SR-wipe → gateway-wedge, dev-schema-registry-wipe-gateway-wedge), producers
  # emit records with dead CACHED schema IDs and the gateway can't deserialize them (Schema N not
  # found; error 40403) → blank UI even though data is flowing. After start, if the gateway is throwing
  # schema errors, restart the Avro PRODUCERS so they re-register their schemas, then restart the
  # gateway so it reads fresh. On a clean boot (fresh topics, producers register on first produce) this
  # is a no-op; it only fires when the schema state is inconsistent.
  echo "Checking Schema Registry / gateway health ..."
  sleep 20
  # Root-cause guard: _schemas (the Schema Registry backing store) must be compact + INFINITE retention.
  # A stray finite retention.ms on it is what silently ages out schemas → SR wipe → gateway 40403 →
  # blank UI (dev-schema-registry-wipe-gateway-wedge). Enforce compact + retention.ms=-1 every start.
  "$KC" --bootstrap-server "$BS" --alter --entity-type topics \
    --entity-name _schemas --add-config "retention.ms=-1,cleanup.policy=compact" >/dev/null 2>&1 || true
  apply_internal_topic_configs
  local gwerr; gwerr=$($KK logs deploy/feed-gateway-service -c feed-gateway --since=60s 2>/dev/null | grep -cE '40403|Schema .* not found' || true)
  if [ "${gwerr:-0}" -gt 0 ]; then
    echo "  ⚠ gateway has $gwerr schema-deserialization errors — SR lost schemas; re-registering producers"
    local d2
    for d2 in raw-to-display-databento-service databento-gex-service databento-gex-history-service \
              volume-pace-databento-service directional-pressure-databento-service strike-flow-classifier-databento \
              strike-flow-avro-adapter databento-mission-sandwich-service databento-volume-aggregator \
              spx-mission-control-service unified-sr-service; do
      echo "$d2" | grep -qxE "$DISABLED_DEV" && continue
      $K rollout restart deploy/"$d2" >/dev/null 2>&1
    done
    sleep 45
    $K rollout restart deploy/feed-gateway-service >/dev/null 2>&1
    echo "  schema self-heal done — producers re-registered + gateway restarted"
  else
    echo "  Schema Registry / gateway OK (no schema errors)"
  fi
  run_partition_doctor
  echo "Verifying indicator-service / market-structure-service readiness ..."
  local verify_rc=0
  verify_start_readiness || verify_rc=$?
  report_session_bound_services
  echo "Done — source topics exist; active apps reach RUNNING, disabled set held at 0 (no replay injection)."
  return $verify_rc
}

# ---------- FULL CLEAN: LOGS + DATA, leave dev down ----------
# ---------- CLEAN GUARD: never wipe dev before the day's next-publication OI evaluation ----------
# oi-next-publication-service scores the session at 17:00 ET (Curb close) + 15 min grace = 17:15 and
# its writer lands the result seconds later. do_clean scales every deployment to 0 and DELETES every
# non-system topic, the service's ledger included, so a clean that runs earlier than that on a
# trading day destroys the day's OI with no way back — it did, at 16:26 ET on 2026-10-02 and
# 2026-10-05, 47-48 min before the evaluation, and Monday/Tuesday had no estimate.
# Rule: on a trading day do_clean REFUSES to run between 09:30 ET and 17:20 ET unless FORCE_CLEAN=1.
# The bound is FIXED, not close-relative: the service's evaluation is a fixed 17:00 + 15 min local
# time, early-close days included (its session end is not calendar-aware), so an early-close day is
# guarded exactly like a normal one. The auto CLEAN slot is [17:20 .. 17:49] for the same reason
# (it was close+30); a tick after 17:40 does nothing automatically (see the auto case: the 18:00
# ET CME reopen). market_calendar.py decides whether the day is a trading day.
clean_guard() {
  local verdict
  verdict=$(CALENDAR_DIR="$CALENDAR_DIR" NOW_ET="${NOW_ET:-}" NOW_DATE="${NOW_DATE:-}" python3 - <<'PY'
import os, sys
from datetime import datetime, timedelta, time, date
from zoneinfo import ZoneInfo
sys.path.insert(0, os.environ.get("CALENDAR_DIR", ""))
try:
    from market_calendar import MarketCalendar
except Exception:
    print("REFUSE calendar-import-failed: cannot prove it is safe to wipe"); sys.exit(0)
tz = ZoneInfo("America/New_York")
rn = datetime.now(tz); d = rn.date()
if os.environ.get("NOW_DATE"):
    s = os.environ["NOW_DATE"]; d = date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
if os.environ.get("NOW_ET"):
    hm = os.environ["NOW_ET"].zfill(4); now = datetime.combine(d, time(int(hm[0:2]), int(hm[2:4])), tz)
else:
    now = datetime.combine(d, rn.time(), tz)
cal = MarketCalendar()
if not cal.is_trading_day(d):
    print("OK not-a-trading-day"); sys.exit(0)
lo = datetime.combine(d, time(9, 30), tz)
hi = datetime.combine(d, time(17, 20), tz)   # oi-next-publication evaluates at a FIXED 17:15 ET, early closes included
if lo <= now < hi:
    print(f"REFUSE trading-day {now.strftime('%H:%M')} ET is before 17:20 ET: the day's OI evaluation (17:15) has not happened")
else:
    print(f"OK {now.strftime('%H:%M')} ET outside [09:30, 17:20)")
PY
)
  case "$verdict" in
    OK*) echo "clean guard: $verdict"; return 0 ;;
    *)
      if [ "${FORCE_CLEAN:-0}" = 1 ]; then
        echo "clean guard: $verdict — OVERRIDDEN by FORCE_CLEAN=1 (the day's next-publication OI is forfeited)"
        return 0
      fi
      echo "clean guard: $verdict"
      echo "clean guard: REFUSING do_clean. Re-run after the evaluation, or FORCE_CLEAN=1 to accept losing today's OI."
      return 1 ;;
  esac
}

# topics_ref_pin: fetch, then resolve TOPICS_ENV_REF's ref to an IMMUTABLE COMMIT, once, and publish it
# as $TOPICS_ENV_SHA / $TOPICS_ENV_PINNED_REF for everything the clean reads afterwards.
#
# A branch name is not a version. Each `git show origin/main:<path>` resolves the ref again, so two
# reads a few seconds apart can come from two different commits if anything fetches in between -- and
# this clean makes four of them: the dev-clean keep-list, then the two halves of the reset-preserved
# bundle (a parser and the declarations it parses, which have to agree), then the shapes ensure_topics
# recreates after the wipe. Pinning once means a mid-clean push cannot split them.
#
# The fetch MUST succeed: a stale origin/main could lack a newly required keep entry, and these lists
# guard a destructive wipe. Retried; a final failure refuses the wipe (the caller says why).
topics_ref_pin() {
  local try fetched=0 sha
  for try in 1 2 3; do
    git -C "$DEPLOY_REPO" fetch -q origin '+refs/heads/main:refs/remotes/origin/main' 2>/dev/null && { fetched=1; break; }
    sleep 2
  done
  [ "$fetched" = 1 ] || return 1
  sha="$(git -C "$DEPLOY_REPO" rev-parse --verify "${TOPICS_ENV_REF%%:*}^{commit}" 2>/dev/null)" || return 1
  # A full commit id, not whatever a stub or a broken rev-parse printed: an unverified value here would
  # be pasted into every `git show` below and fail four reads with four different messages. 40 hex for
  # sha1, 64 for a sha256 repository; an abbreviation is refused because it can become ambiguous.
  case "$sha" in *[!0-9a-f]* | "") return 1 ;; esac
  [ "${#sha}" -ge 40 ] || return 1
  TOPICS_ENV_SHA="$sha"
  TOPICS_ENV_PINNED_REF="$sha:${TOPICS_ENV_REF#*:}"
}

# dev_clean_preserved_topics -> the OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS keep list (see topics.env), read at
# the commit topics_ref_pin resolved, never from the working tree: the clone on the dev host can be many commits
# behind. FAILS (non-zero, nothing printed) unless AT LEAST ONE declaration exists and the concatenated
# value is non-empty, and the caller REFUSES the wipe then: an unreadable keep list must stop a destructive clean,
# not mean "keep nothing". It was "exactly one" until 2026-11-06, which is what made every APPENDED declaration
# invisible while still reporting success.
dev_clean_preserved_topics() {
  local env n val
  # Already pinned by the caller (do_clean pins in ITS shell, because this function runs inside a command
  # substitution and an assignment made in that subshell never reaches the shell that reads it later).
  [ -n "${TOPICS_ENV_SHA:-}" ] || topics_ref_pin || return 1
  env="$(git -C "$DEPLOY_REPO" show "$TOPICS_ENV_PINNED_REF" 2>/dev/null)" || return 1
  # INDENT-TOLERANT, and EVERY assignment is concatenated rather than exactly one: an entry added to this
  # keep-list by appending inside `if [ "${ENVIRONMENT:-}" != "production" ]` is INDENTED, so the column-0
  # anchor could not see it while the "exactly one" check still passed on the base declaration alone.
  # es.rithmic.mbo is declared DEV_CLEAN_PRESERVED exactly that way and the 2026-11-06 wipe deleted it.
  # The `$VAR` self-reference an append opens with is dropped; this keep-list may only ever grow.
  n=$(printf '%s\n' "$env" | grep -cE '^[[:space:]]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="' || true)
  [ "${n:-0}" -ge 1 ] || return 1
  val=$(printf '%s\n' "$env" | sed -nE 's/^[[:space:]]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="(.*)"$/\1/p' \
        | sed -e 's/\${OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS}//g' -e 's/\$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS//g' \
        | tr '\n' ' ' | tr -s ' ' | sed -e 's/^ *//' -e 's/ *$//')
  [ -n "${val// /}" ] || return 1
  printf '%s\n' "$val"
}

# reset_preserved_from_main -> the RESET-PRESERVED keep list, resolved from the SAME reviewed commit as
# everything else this clean reads (TOPICS_ENV_REF, i.e. origin/main), NEVER from the working tree.
#
# Until 2026-11-06 the wipe sourced $DEPLOY_REPO/scripts/kafka/reset-preserved-topics.sh out of the
# checkout, and that reader reads the topics.env BESIDE IT -- also the checkout's. The clone on the dev
# host sits on whatever branch someone left it on: on 11-06 its topics.env was 376 lines behind main and
# carried no es.rithmic.mbo at all, so installing a fixed script to ~/oe-ops changed nothing until the
# working tree was updated too. A destructive wipe deciding what may be purged from an unreviewed,
# arbitrarily stale file is the same defect class as the column-0 anchor it sat next to; the sibling
# keep-list (dev_clean_preserved_topics) has read the ref, and refused when it could not, all along.
#
# The two files are materialised TOGETHER into one temp dir and the script is run FROM there. That is
# deliberate: reset-preserved-topics.sh pins its declaration file to "the topics.env beside me" and
# ignores RPT_TOPICS_ENV when sourced, on purpose -- an ambient override on a destructive path was
# rejected in its own review -- so the pin is honoured rather than bypassed, and both halves come from
# the same commit. It is the same bundle Jenkinsfile.kafka-reset and prod-clean-slate.sh ship to a host.
#
# EXECUTED, not sourced: the helper ends an unresolvable list with `exit 1`, which from a sourced file
# kills dev-cleanup.sh itself, mid-clean, with its stderr suppressed by the old `2>/dev/null`. Executing
# it in a command substitution turns that into an ordinary non-zero status this function can report.
# RPT_TOPICS_ENV is stripped because executed mode honours it: with it set, a file somebody exported
# hours ago would decide what the wipe may delete.
#
# ENVIRONMENT is passed through untouched. On a dev host it is not "production", so the prod-only list
# is absent exactly as before; forcing a value here could only ever SHRINK the keep-list, which is the
# one direction that destroys data.
#
# No fetch of its own: every caller is do_clean's WIPE_KAFKA branch, where dev_clean_preserved_topics
# has already run topics_ref_pin -- a successful fetch, with retries, and the commit id this function
# then reads -- and refused the wipe outright if either failed. A second fetch would be a second
# chance for the two keep-lists to come from different commits, which is what the pin exists to stop.
reset_preserved_from_main() {
  local ref dir work f out rc
  # The PINNED commit, not the branch name: a parser and the declarations it parses must come from one
  # commit, and `git show origin/main:<path>` resolves the ref afresh on every call. Required rather
  # than defaulted -- a caller that has not pinned has not fetched either, and guessing which commit a
  # destructive wipe should trust is the whole defect being fixed.
  ref="${TOPICS_ENV_SHA:-}"
  if [ -z "$ref" ]; then
    echo "reset-preserved: TOPICS_ENV_SHA is not set — topics_ref_pin must run first" >&2
    return 1
  fi
  dir="$(dirname "${TOPICS_ENV_REF#*:}")"          # scripts/kafka
  work="$(mktemp -d "${TMPDIR:-/tmp}/dev-cleanup-rpt.XXXXXX")" || {
    echo "reset-preserved: cannot create a temp dir" >&2; return 1; }
  for f in reset-preserved-topics.sh topics.env; do
    if ! git -C "$DEPLOY_REPO" show "$ref:$dir/$f" > "$work/$f" 2>/dev/null || [ ! -s "$work/$f" ]; then
      echo "reset-preserved: cannot read $ref:$dir/$f from $DEPLOY_REPO" >&2
      rm -rf "$work"; return 1
    fi
  done
  # BASH_ENV is unset as well as RPT_TOPICS_ENV: non-interactive bash SOURCES $BASH_ENV before the
  # script, so a file named there could export RPT_TOPICS_ENV back into the helper and hand the wipe a
  # well-formed but incomplete keep-list -- `env -u RPT_TOPICS_ENV` alone did not close that (Codex r1).
  out="$(env -u RPT_TOPICS_ENV -u BASH_ENV -u ENV bash "$work/reset-preserved-topics.sh" 2>&1)"; rc=$?
  rm -rf "$work"
  if [ "$rc" -ne 0 ]; then
    echo "reset-preserved: $ref:$dir/reset-preserved-topics.sh refused to resolve the list (rc=$rc):" >&2
    printf '%s\n' "$out" | sed 's/^/  /' >&2
    return 1
  fi
  # Non-empty is asserted HERE as well as inside the helper: rc=0 with nothing on stdout would otherwise
  # hand the wipe an empty keep-list, which is precisely "keep nothing".
  if [ -z "$(printf '%s' "$out" | tr -d '[:space:]')" ]; then
    echo "reset-preserved: $ref:$dir resolved an EMPTY list" >&2
    return 1
  fi
  printf '%s\n' $out
}

# DEV-ONLY overnight estimate topics the wipe must also keep (their owners: the indicator-estimate and
# market-structure-estimate sidecars). They are not in the shared topics.env on purpose (prod must not grow them),
# so their names live here, next to ensure_dev_only_estimate_topics.
DEV_ONLY_ESTIMATE_KEEP="options.indicators.estimate.bars options.indicators.estimate.bars.current options.spx.market-structure.estimate.recovery options.spx.market-structure.estimate.patterns options.spx.market-structure.estimate.ui options.spx.market-structure.estimate.sequence options.spx.market-structure.estimate.tools options.spx.market-structure.estimate.geometry"

# Topics a full DEV wipe RESETS although the shared reset-preserved list keeps them (owner decision 2026-11-10,
# "reset on every dev wipe"). futures.level-absorption.events is declared preserved because on PROD it is evidence
# that cannot be rebuilt. On dev the wipe also deletes the inputs level-absorption-service replays from, and the
# service then refuses to start (REPLAY_SHORT: the replay no longer reproduces the log it kept) -- so every dev
# clean ended with a crash-looping service and a hand repair (delete events + closures, recreate through the
# barrier). Not preserving them lets the wipe delete them with everything else; ensure_topics then recreates both
# at their declared shape (retention.ms=-1 and the artifact's exact retention.bytes for events, 7 days for
# closures), which is what the service's own start-up check demands. This list only subtracts names from what
# the wipe KEEPS on dev: prod's list and prod's scripts are not read or changed.
DEV_CLEAN_RESET_TOPICS="futures.level-absorption.events futures.level-absorption.closures"

# dev_wipe_keep_list NAME...: the names the dev wipe must keep = the arguments minus DEV_CLEAN_RESET_TOPICS,
# EXACT names only (a prefix or a sibling such as futures.level-absorption.voice is not touched), one per line.
dev_wipe_keep_list() {
  local reset
  reset="$(printf '%s\n' $DEV_CLEAN_RESET_TOPICS)"
  printf '%s\n' "$@" | tr ' ' '\n' | grep -v '^$' | grep -vxF -e "$reset" || true
}

do_clean() {
  clean_guard || return 1
  require_kafka_cli   # before anything is stopped or deleted: a clean must not run blind
  echo "=== dev FULL clean (logs + data)  $(date) ==="
  local DEV_KEEP=""
  if [ "$WIPE_KAFKA" = true ]; then
    # Pin HERE, in this shell, before the first keep-list read. Every later read (the dev keep-list inside
    # $(...), reset_preserved_from_main, ensure_topics) takes TOPICS_ENV_SHA from this variable; pinning
    # inside the $(...) left it empty in this shell and the wipe refused every time ("TOPICS_ENV_SHA is not
    # set"), which is how 2026-11-09's clean found it.
    topics_ref_pin || {
      echo "REFUSING the wipe: cannot pin ${TOPICS_ENV_REF%%:*} to a commit (fetch of origin/main failed or the ref does not resolve)." >&2
      return 1; }
    DEV_KEEP="$(dev_clean_preserved_topics)" || {
      echo "REFUSING the wipe: cannot read OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS from $TOPICS_ENV_REF (at least one declaration, non-empty once concatenated, is required)." >&2
      return 1; }
    DEV_KEEP="$DEV_KEEP $DEV_ONLY_ESTIMATE_KEEP"
    echo "dev-clean keep list: $DEV_KEEP"
    # BOTH keep-lists are resolved HERE, before clean_logs and before a single deployment is scaled
    # down or a mirror paused, so a wipe that cannot say what it must keep refuses while dev is still
    # whole. The reset-preserved read used to sit at step 4 instead -- after the scale-down and after
    # pause_dev_mirrors -- where its failure left dev down with the mirrors held paused.
    RESET_PRESERVED_TOPICS="$(reset_preserved_from_main)" || {
      echo "REFUSING the wipe: cannot resolve the RESET-PRESERVED keep list from ${TOPICS_ENV_REF%%:*} at $TOPICS_ENV_SHA (reason above). These topics hold data that by declaration cannot be rebuilt, so a wipe that does not know them must not run." >&2
      return 1; }
    # The COMMIT is logged, not just the branch: a line saying "origin/main" does not say which
    # origin/main, and this log is the only record of which declarations a given night's wipe obeyed.
    echo "keep-lists resolved at ${TOPICS_ENV_REF%%:*} = $TOPICS_ENV_SHA"
    echo "reset-preserved keep list: $RESET_PRESERVED_TOPICS"
  fi
  clean_logs
  local BEFORE AFTER APPS LIST NEWMAN d i up
  BEFORE=$(du -sh "$KAFKA_DATA" 2>/dev/null | awk '{print $1}')

  # 1. scale every Kafka-client deployment to 0 (all except KEEP) so nothing produces/recreates
  APPS=$($KK get deploy -o name 2>/dev/null | sed 's#.*/##' | grep -vE "$KEEP")
  echo "1) scaling $(echo "$APPS" | grep -c .) deployments to 0 (keeping: $KEEP)"
  for d in $APPS; do $K scale deploy/"$d" --replicas=0 >/dev/null 2>&1; done

  # 2. wait for their pods to terminate (release PVCs, stop all Kafka clients)
  echo "2) waiting for pods to terminate ..."
  for i in $(seq 1 48); do
    up=$($KK get pods --no-headers 2>/dev/null | grep -viE "$KEEP|Completed" | grep -ic running)
    [ "$up" -eq 0 ] && { echo "   all down after ~$((i*5))s"; break; }
    sleep 5
  done

  # 3. reset every streams-state PVC (delete + recreate empty RocksDB) — ONLY on a hard WIPE_KAFKA reset.
  if [ "$WIPE_KAFKA" != true ]; then
    echo "3) keeping Streams-state PVCs (retention-based cleanup — WIPE_KAFKA=false)"
  else
  echo "3) resetting streams-state PVCs ..."
  $KK get pvc -o json 2>/dev/null | python3 -c '
import sys,json
for p in json.load(sys.stdin)["items"]:
    n=p["metadata"]["name"]
    if "streams-state" in n:
        print(n, p["spec"].get("storageClassName","standard"), p["spec"]["resources"]["requests"]["storage"])
' | while read -r pvc sc sz; do
    $K delete pvc "$pvc" --wait=true --timeout=60s >/dev/null 2>&1
    printf 'apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: options-edge\n  labels:\n    app.kubernetes.io/component: kafka-streams-state\nspec:\n  accessModes: [ReadWriteOnce]\n  storageClassName: %s\n  resources:\n    requests:\n      storage: %s\n' "$pvc" "$sc" "$sz" | $K apply -f - >/dev/null 2>&1
  done
  fi

  # 3b. (removed 2026-07-08) The topic list is NO LONGER auto-captured from live state. 'start' now
  #     pre-creates the deploy repo's topics.env platform topics (the reviewed, versioned config);
  #     every service self-creates its own output + Streams-internal topics on startup, like prod.
  #     See do_start() + TOPICS_ENV_REF. This removes the autonomous drift where a clean run silently
  #     re-learned whatever happened to be present.

  # 4. CLEAN + RECREATE the topics — ONLY on a WIPE_KAFKA reset (default true, 2026-07-11). Retention is
  #    now ETERNAL (-1), so data is deleted HERE (manual wipe), NOT by a TTL. After deleting every
  #    non-system topic we RECREATE the deploy repo's platform/feed topics (ensure_topics) so they exist
  #    empty at their declared shape (per-topic retention overrides included; unlisted topics inherit
  #    the broker default); each service self-creates its own output + Streams-internal topics on
  #    startup. (_schemas + __consumer_offsets/__transaction_state are never deleted.)
  if [ "$WIPE_KAFKA" != true ]; then
    echo "4) keeping topics (WIPE_KAFKA=false)"
  else
    echo "4) deleting all non-system topics ..."
    # REFUSE rather than delete behind a mirror that is still up. The wipe has not started yet at
    # this point, so dev is left scaled down but WHOLE — the same state, and the same recovery, as
    # the kafka_list_topics refusal below. Agents that were unloaded stay in the paused list and the
    # next start/overnight run whose topic pass succeeds reloads them, by design.
    pause_dev_mirrors || {
      echo "REFUSING the wipe: the es4->dev mirrors could not be proven DOWN (reason above). Deleting a topic while its mirror runs re-creates it at dev's num.partitions=1, so the declared shape never applies and the wedge is invisible afterwards." >&2
      echo "  State now: logs cleaned, apps scaled down, Kafka NOT wiped. Fix the mirror agents, then re-run 'dev-cleanup.sh now'; 'dev-cleanup.sh start' brings dev back up." >&2
      return 1; }
    # RESET-PRESERVED topics survive here too. They hold data that by declaration cannot be rebuilt —
    # the A5 calibration ledger accrues until the archive carries it to the NAS, and a wipe before that
    # loses the day with no way to notice. dev and prod must agree on what "preserved" means, or the
    # two environments stop being comparable, which is the whole reason the evidence is split by env.
    # THE ONE DECLARED EXCEPTION is DEV_CLEAN_RESET_TOPICS (level-absorption's events + closures, owner 2026-11-10):
    # dev deletes the inputs that service replays from, so keeping its log makes it unstartable. It is subtracted
    # below, by exact name, and nothing else is.
    # RESET_PRESERVED_TOPICS was resolved at the top of do_clean, at the commit topics_ref_pin pinned,
    # and the wipe was refused outright if it could not be. It is NOT re-read here: this point is past
    # the scale-down and past pause_dev_mirrors, so there is no safe way to fail.
    # The keep set = both lists MINUS DEV_CLEAN_RESET_TOPICS (level-absorption's events + closures: see the note
    # above dev_wipe_keep_list). They are deleted with everything else and recreated by the topic pass below.
    local KEEP_NAMES RESET_NOW
    KEEP_NAMES="$(dev_wipe_keep_list $RESET_PRESERVED_TOPICS $DEV_KEEP)"
    RESET_NOW="$(printf '%s\n' $RESET_PRESERVED_TOPICS $DEV_KEEP | grep -xF -e "$(printf '%s\n' $DEV_CLEAN_RESET_TOPICS)" | sort -u | paste -sd' ' -)"
    PRESERVE_RE="$(printf '%s\n' $KEEP_NAMES | sed 's/[.[\*^$()+?{|]/\\&/g' | paste -sd'|' -)"
    [ -n "$PRESERVE_RE" ] && echo "   preserving (reset-preserved + dev-clean keep list, minus the dev reset set): $(printf '%s ' $KEEP_NAMES)"
    [ -n "$RESET_NOW" ] && echo "   RESETTING (declared preserved, but derived from inputs this wipe deletes): $RESET_NOW"
    local all_topics
    all_topics="$(kafka_list_topics)" || {
      echo "REFUSING the wipe: kafka-topics --list failed or printed nothing after retries, so there is nothing to prove was deleted." >&2
      echo "  State now: logs cleaned, apps scaled down, Kafka NOT wiped. 'dev-cleanup.sh start' brings dev back up; re-run 'dev-cleanup.sh now' once the broker answers." >&2
      return 1; }
    printf '%s\n' "$all_topics" | grep -vE '^__|^_schemas' \
      | { [ -n "$PRESERVE_RE" ] && grep -vxE "$PRESERVE_RE" || cat; } \
      | xargs -P 8 -I{} $KT --bootstrap-server $BS --delete --topic {} >/dev/null 2>&1
    sleep 8   # let the deletions settle before recreating (avoid create-vs-delete races)
    echo "4d) recreating platform topics (clean + recreate) ..."
    # Recorded, not fatal here: the CLEAN slot's next act is the overnight start, which runs the
    # topic pass again and refuses its own scale-up if it still fails. Aborting mid-clean would
    # leave the wipe half-applied, which is worse.
    ensure_topics_or_refuse || CLEAN_TOPICS_FAILED=1
  fi

  # 4b. safe docker-ENGINE image housekeeping (build side only — NOT the k8s containerd store).
  #     (The ~90 GB k8s containerd images + ~90 GB registry are a SEPARATE, deliberate GC — docker-desktop
  #     ships no crictl/ctr and Docker.raw won't shrink without a manual Docker Desktop purge/compact.)
  echo "4b) docker-engine build-side prune (dangling images + build cache) ..."
  docker image prune -f   >/dev/null 2>&1
  docker builder prune -f >/dev/null 2>&1

  # 4b2. local registry: keep ONLY :dev + deployed image per repo, delete old build tags + GC (reclaimed
  #      182 GB→366 MB once — the registry never GC's itself, so it re-bloats every build/deploy).
  echo "4b2) pruning local registry to latest-only ..."
  [ -x /Users/abhinav/oe-ops/dev-registry-prune.sh ] && /opt/homebrew/bin/bash /Users/abhinav/oe-ops/dev-registry-prune.sh 2>&1 | sed 's/^/   /'

  # 4c. thin APFS local Time Machine snapshots (macOS GOTCHA): the Mac takes hourly TM snapshots, so the
  #     ~160 GB of Kafka data just deleted stays PINNED as "used" (disk creeps toward full) until thinned.
  echo "4c) thinning APFS local snapshots to actually reclaim the freed space ..."
  snaps=$(tmutil listlocalsnapshots / 2>/dev/null | grep -c com.apple.TimeMachine)
  tmutil thinlocalsnapshots / 600000000000 4 >/dev/null 2>&1
  echo "    thinned $snaps snapshots"

  # 5. verify + report
  sleep 10
  AFTER=$(du -sh "$KAFKA_DATA" 2>/dev/null | awk '{print $1}')
  DFREE=$(df -h /System/Volumes/Data 2>/dev/null | tail -1 | awk '{print $4" free ("$5" full)"}')
  echo "=== RESULT ==="
  echo "  Kafka data:         $BEFORE -> $AFTER   (data purged)"
  echo "  changelog topics:   $(topic_count changelog)   (0 = success)"
  echo "  repartition topics: $(topic_count repartition) (0 = success)"
  echo "  streams-state PVCs:  reset to empty"
  echo "  logs:                trimmed"
  echo "  disk after thin:     ${DFREE:-?}"
  echo "  apps: LEFT DOWN — 'dev-cleanup.sh start' is REQUIRED to bring dev back up (until then there are no bars,"
  echo "        no market structure and no Order-page chart). It verifies indicator-service comes up fully ready."
  echo "DONE $(date)"
  # THE TOPIC FAILURE CARRIES OUT. Setting a variable nobody reads is the same silence this round is
  # about, so do_clean's own status says it: the CLEAN slot then prints its "clean: FAILED" line and
  # still proceeds to the overnight start, whose topic pass runs again and refuses its own scale-up
  # if it still fails. The wipe itself is already complete at this point, so this REPORTS rather
  # than aborts — a mid-clean abort would leave the wipe half-applied, which is worse.
  if [ "${CLEAN_TOPICS_FAILED:-0}" = 1 ]; then
    echo "  topics: NOT all recreated at their declared shapes — nothing may be scaled up until a"
    echo "          topic pass succeeds ('dev-cleanup topics', then 'dev-cleanup start')."
    return 1
  fi
}

# ---------- dispatch ----------
MODE=${1:-auto}
# MAIN_RC carries the verdict of the directly requested mode to the exit below. These cases used to end on the bare
# function call, and nothing after the case read its status, so `dev-cleanup now` exited 0 after REFUSING the
# wipe (the 2026-11-09 clean: only the log said so) and `start` exited 0 after verify_start_readiness failed.
MAIN_RC=0
case "$MODE" in
  logs)      clean_logs ;;
  start)     do_start || MAIN_RC=$? ;;
  overnight) do_start_overnight || MAIN_RC=$? ;;
  es-down)   do_es_down ;;
  now|clean) if [ "${DKC_DRYRUN:-0}" = 1 ]; then clean_guard || MAIN_RC=$?; [ "$MAIN_RC" -eq 0 ] && echo "DRYRUN: would do_clean (WIPE)"; else do_clean || MAIN_RC=$?; fi ;;
  auto)
    # Calendar-aware slots (ET), 2026-07-11. market_calendar.py says whether the day trades; the CLEAN
    # slot is a FIXED 17:20 on every trading day (early closes included) since 2026-10-06:
    #   CLEAN slot [17:20 .. 17:49] ET on a trading day -> do_clean (WIPE) THEN overnight ES-tracking start;
    #   a tick after 17:40 (CLEAN_LATE: the 17:20-17:35 tick was missed) does nothing and says so.
    #   (was close+30 until 2026-10-06: that wiped the oi-next-publication ledger before its 17:15 ET
    #   evaluation two sessions running. FIXED, not close-relative, because the service's evaluation is
    #   a fixed 17:15 even on a 13:00 early close.)
    #   FULL  slot [06:15 .. 06:44]       on a trading day -> do_start (2026-08-03: 07:30 -> 06:15 per
    #   USER D13/D15 in OVERNIGHT-IBKR-GEX-GATE1-REQUIREMENT.md — the dev Mac hosts the IBKR feed
    #   whose pre-open window opens at 06:15 ET, and prod's chain data hops FROM dev).
    # launchd runs every 15 min so at least one tick lands in each 30-min window; markers keyed by the
    # trading-date make it idempotent. Dry-test: NOW_ET=1615 NOW_DATE=20260713 DKC_DRYRUN=1 dev-cleanup
    SLOTINFO=$(CALENDAR_DIR="$CALENDAR_DIR" NOW_ET="${NOW_ET:-}" NOW_DATE="${NOW_DATE:-}" python3 - <<'PY'
import os, sys
from datetime import datetime, timedelta, time, date
from zoneinfo import ZoneInfo
sys.path.insert(0, os.environ.get("CALENDAR_DIR", ""))
try:
    from market_calendar import MarketCalendar
except Exception:
    print("SLOT=OFF TD=00000000 CLOSE=0000 ERR=cal-import"); sys.exit(0)
tz = ZoneInfo("America/New_York")
rn = datetime.now(tz)
d = rn.date()
if os.environ.get("NOW_DATE"):
    s = os.environ["NOW_DATE"]; d = date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
if os.environ.get("NOW_ET"):
    hm = os.environ["NOW_ET"].zfill(4); now = datetime.combine(d, time(int(hm[0:2]), int(hm[2:4])), tz)
else:
    now = datetime.combine(d, rn.time(), tz)
cal = MarketCalendar()
td = cal.current_trading_date(now).strftime("%Y%m%d")
if not cal.is_trading_day(d):
    print(f"SLOT=OFF TD={td} CLOSE=0000"); sys.exit(0)
close = cal.close_time(d)
close_dt = datetime.combine(d, close, tz)
# FIXED 17:20..17:49 ET on every trading day (early closes included): after the oi-next-publication
# evaluation at a fixed 17:15 (see clean_guard). 30 minutes wide so an every-15-min launchd tick of
# ANY phase lands at least once — and in [17:20, 17:35), i.e. before the wipe deadline below. A tick
# after 17:40 (CLEAN_LATE, only reachable if that tick was missed) does NOTHING automatically: the
# wipe's own duration is not bounded by this script (measured 5-10 min on 2026-07-15..20) and the
# overnight ES set must be up for the 18:00 ET CME reopen, but starting it un-wiped is unsafe.
clean_lo = datetime.combine(d, time(17, 20), tz); clean_hi = clean_lo + timedelta(minutes=29)
wipe_by = datetime.combine(d, time(17, 40), tz)
full_lo = datetime.combine(d, time(6, 15), tz); full_hi = full_lo + timedelta(minutes=29)
# ESDOWN: ~09:17 ET, before the 09:30 open. 15-min window [09:15..09:29] so the every-15-min launchd
# reliably lands a tick before the open; ends 09:29 so it never fires after the bell.
esdown_lo = datetime.combine(d, time(9, 15), tz); esdown_hi = datetime.combine(d, time(9, 29), tz)
if clean_lo <= now <= clean_hi: slot = "CLEAN" if now <= wipe_by else "CLEAN_LATE"
elif full_lo <= now <= full_hi: slot = "FULL"
elif esdown_lo <= now <= esdown_hi: slot = "ESDOWN"
else: slot = "OFF"
print(f"SLOT={slot} TD={td} CLOSE={close.strftime('%H%M')}")
PY
)
    SLOT=$(printf '%s' "$SLOTINFO" | grep -oE 'SLOT=[A-Z_]+' | cut -d= -f2)
    TD=$(printf '%s' "$SLOTINFO" | grep -oE 'TD=[0-9]+' | cut -d= -f2)
    CLEAN_MARK=/tmp/.dev-cleanup-clean-$TD                     # idempotent, keyed by trading-date
    START_MARK=/tmp/.dev-cleanup-start-$TD
    ESDOWN_MARK=/tmp/.dev-cleanup-esdown-$TD
    case "$SLOT" in
      CLEAN)
        if [ -f "$CLEAN_MARK" ]; then :
        elif [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: CLEAN slot ($SLOTINFO) -> do_clean (WIPE) + overnight ES start"
        else
          : > "$CLEAN_MARK"
          { clean_rc=0; do_clean || clean_rc=$?
            if [ "$clean_rc" -eq 0 ]; then
              echo "clean: OK"
            elif [ "${CLEAN_TOPICS_FAILED:-0}" = 1 ]; then
              # TELL THE TRUTH ABOUT WHICH FAILURE THIS IS. The old message said the overnight start
              # "proceeds on the UN-WIPED state", which is right when the wipe itself was refused and
              # WRONG for the failure the topic guard added: there the deletion SUCCEEDED and the
              # recreation did not, so dev is down, the mirrors are paused, and the non-preserved
              # topics are gone or only partly back. An operator reading "un-wiped" would do exactly
              # the wrong thing (Codex deploy r17).
              echo "clean: FAILED (rc=$clean_rc) — the WIPE RAN and the topic recreation did NOT finish."
              echo "       Non-preserved topics are deleted or only partly recreated, dev is down and the"
              echo "       es4->dev mirrors are PAUSED. The overnight start below will refuse to scale"
              echo "       anything up until a topic pass succeeds. Recover with 'dev-cleanup topics'"
              echo "       (repeat until it exits 0), then 'dev-cleanup overnight'."
            else
              echo "clean: FAILED (rc=$clean_rc) — the overnight ES start proceeds on the UN-WIPED state;"
              echo "       re-run 'dev-cleanup now' by hand after 17:20 ET if a wipe is needed"
            fi
            echo "--- overnight ES-tracking start ---"
            start_rc=0; do_start_overnight || start_rc=$?
            # THE COMBINED VERDICT CARRIES. This block's status was discarded, so launchd could read
            # success after BOTH the clean and the overnight start had failed.
            if [ "$clean_rc" -ne 0 ] || [ "$start_rc" -ne 0 ]; then
              AUTO_SLOT_FAILED=1
              echo "auto CLEAN slot: FAILED (clean rc=$clean_rc, overnight start rc=$start_rc)"
            fi; } >> "$LOG" 2>&1
        fi ;;
      CLEAN_LATE)
        # Past the wipe deadline (17:40 ET): too late to start a 5-10 min wipe and still have the
        # overnight ES set up for the 18:00 ET CME reopen — and starting that set WITHOUT the wipe
        # is not safe either (daytime deployments still up, stale ES topics/offsets/Streams state).
        # So nothing runs automatically: say so loudly, once, and leave it to an operator
        # ('dev-cleanup now' then 'dev-cleanup overnight'). With an every-15-min launchd tick this
        # path is reached only when the 17:20-17:35 tick itself was missed (Mac asleep, launchd
        # backlog) — not on a normal day.
        if [ -f "$CLEAN_MARK" ]; then :
        elif [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: CLEAN_LATE ($SLOTINFO) -> NOTHING runs: wipe skipped (after 17:40 ET) and overnight ES start skipped (unsafe un-wiped)"
        else
          : > "$CLEAN_MARK"
          echo "=== CLEAN_LATE $(date): the 17:20-17:40 ET tick was missed. NO wipe (too close to the 18:00 ET CME reopen) and NO overnight ES start (unsafe on an un-wiped environment). Operator: 'dev-cleanup now' then 'dev-cleanup overnight'. ===" >> "$LOG" 2>&1
        fi ;;
      FULL)
        if [ -f "$START_MARK" ]; then :
        elif [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: FULL slot ($SLOTINFO) -> do_start (full pipeline)"
        else : > "$START_MARK"; do_start >> "$LOG" 2>&1; fi ;;
      ESDOWN)
        if [ -f "$ESDOWN_MARK" ]; then :
        elif [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: ESDOWN slot ($SLOTINFO) -> do_es_down (scale $ES_DOWN_SET to 0)"
        else : > "$ESDOWN_MARK"; do_es_down >> "$LOG" 2>&1; fi ;;
      *) [ "${DKC_DRYRUN:-0}" = 1 ] && echo "DRYRUN: off-slot ($SLOTINFO) -> nothing to do" ;;
    esac
    ;;
  topics)
    # TOPICS-ONLY: apply the topics.env contract and nothing else — no wipe, no scaling, no
    # service restarts. Exists because the contract had NO non-destructive applier: it rode on
    # `now` (which WIPES Kafka) or `start` (which scales the whole dev stack up), so the only way
    # to repair a drifted topic was to accept a side effect nobody asked for. On 2026-08-14 that
    # gap cost two dev deploy attempts — `databento-gex` fails closed on
    # options.databento.oi.anchor-manifest being PURE compact with retention -1 (still true: the
    # barrier is Jenkinsfile.service-deploy's 'Verify OI anchor manifest topic' stage, though since
    # d845a415 (options-edge-processing, 2026-09-02) that service no longer PRODUCES to the topic), and dev had it as
    # `delete` with no retention override because a client auto-created it (dev leaves
    # auto.create.topics.enable at its TRUE default and num.partitions=1, which is exactly the
    # shape that appeared).
    #
    # ensure_topics creates what is missing and reconcile_declared_topics — the bulk-describe pass
    # inside it — repairs the config of what already exists, which is the half a bare
    # `--create --if-not-exists` can never do.
    #
    # WHAT THIS DOES AND DOES NOT PROMISE. It issues no delete of any kind: no topic is dropped and
    # no record is explicitly removed. It is NOT, however, a no-op on data. Turning a topic's
    # cleanup.policy to `compact` AUTHORISES the log cleaner to drop superseded values for a key,
    # asynchronously and on its own schedule — compaction is not gated on consumer offsets, and
    # `retention.ms=-1` disables TIME-based deletion, not compaction. For the topics this applies
    # it to that is the declared intent (they are keyed state whose contract IS "latest value per
    # key"), but a consumer that needs every historical transition of such a key, or a tombstone
    # older than delete.retention.ms, can legitimately lose it. So: safe for the declared contract,
    # not unconditionally safe for an arbitrary reader — check what reads a topic before widening
    # this to one whose history someone depends on.
    require_kafka_cli
    ensure_topics_or_refuse || exit 1
    ;;
  *) echo "usage: dev-cleanup [ | now | start | overnight | topics | logs ]  (no arg = calendar-gated auto for launchd)"; exit 2 ;;
esac

# EXIT STATUS CARRIES THE MIRROR VERDICT. A clean that finished its work but left the es4 -> dev
# mirrors down must not exit 0: cron and Jenkins read only this number, and the night this was
# written they read 0 while every mirrored dev topic was frozen. The work is already complete by
# this point, so this REPORTS rather than aborts — a mid-clean abort would leave the wipe
# half-applied, which is worse than a frozen mirror.
if [ "${DEV_MIRRORS_RESUME_FAILED:-0}" != 0 ]; then
  echo "dev-cleanup: FINISHED WITH FROZEN MIRRORS — es4 -> dev is not flowing (see the ERROR above)"
  exit 3
fi
# AND THE AUTOMATIC SLOT'S VERDICT. The CLEAN slot runs its work inside a redirected block whose
# status was dropped, so launchd read 0 after both the clean and the overnight start had failed
# (Codex deploy r17). Same reasoning as the mirrors above: the work is over, so this REPORTS.
if [ "${AUTO_SLOT_FAILED:-0}" != 0 ]; then
  echo "dev-cleanup: the automatic slot FAILED — see $LOG (nothing may be scaled up until a topic pass exits 0)"
  exit 4
fi
# AND THE DIRECT MODE'S OWN VERDICT (now / start / overnight), after the two more specific codes above.
if [ "${MAIN_RC:-0}" != 0 ]; then
  echo "dev-cleanup $MODE: FAILED (rc=$MAIN_RC) — see the lines above"
  exit "$MAIN_RC"
fi
