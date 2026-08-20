#!/usr/bin/env bash
# dev-cleanup.sh — THE one dev cleanup script. Once a day (end of day, ET-gated) it cleans BOTH
# the logs AND the Kafka data in a single run, and each weekday morning it brings dev back up.
# No 60-second log trimming, no separate scheduler wrapper — this one file + one launchd is the whole thing.
#
# MODES:
#   dev-cleanup           # AUTO — what launchd calls every ~15 min. CALENDAR-aware ET slots (2026-07-11):
#                         #   close+30 ET (16:30 normal / 13:30 early-close) -> full clean (WIPE topics) THEN
#                         #                immediately bring up ONLY the overnight ES-tracking set ($OVERNIGHT_SET)
#                         #   ~09:17 ET (window 09:15-09:29, weekdays) -> ESDOWN: scale the overnight ES services
#                         #                ($ES_DOWN_SET) to 0 before the 09:30 SPX open (feed-gateway + web stay up)
#                         #   06:15-06:44 ET weekdays -> full start (bring the rest of the pipeline up before the pre-open window)
#   dev-cleanup now       # run the full clean right now (logs + data + topic WIPE), ignoring the time gate
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
KT=/Users/abhinav/kafka-4.3.0/bin/kafka-topics.sh
# kafka-configs is the SIBLING of $KT on purpose: ensure_topics reconciles topic config with the
# same CLI generation that creates the topics. `--alter --add-config` is INCREMENTAL here (Kafka
# 2.3+ uses incrementalAlterConfigs for topic entities), so it merges rather than replacing the
# config set — the legacy whole-set alter is what silently dropped cleanup.policy=compact off 12
# dealer-ledger changelogs on 2026-08-08.
KC="$(dirname "$KT")/kafka-configs.sh"
BS=localhost:19092
KAFKA_DATA=/Users/abhinav/development/kafka-options-edge/data
KEEP='keycloak'                                          # deployments to leave running (regex)
# ibkr-feed-service REMOVED 2026-07-29: now dev-only and REQUIRED up (SPX cash-index tier-1 source
# for both envs, requirement rev4).
# DEV-disabled deployments — held at 0 by do_start (exact full-name match via grep -x). MUST include
# databento-timewarp-snapshot-replay: if it boots even briefly it replays snapshots into
# options.databento.raw and poisons the chain. Keep in sync with premarket-check.sh DISABLED (dev).
# stock-gex-service is PROD-ONLY (USER 2026-08-18): the public Gamma Lab page reads it there,
# and it pulls straight from Databento (billed) rather than from Kafka — a dev copy spends
# money serving a page dev does not host, and it was sitting at 0/1 on dev anyway.
DISABLED_DEV='hpsf-stage-a-service|hpsf-stage-b-service|volume-pace-service|volume-pace-databento-service|volume-sandwich-service|volume-sandwich-databento-service|databento-timewarp-snapshot-replay|strike-flow-classifier-ibkr|options-edge-integration-test|databento-mission-pressure-service|databento-mission-pace-service|spx-mission-control-service|short-premium-agent-service|spread-skew-service|spread-skew-postgres-writer|directional-pressure-databento-service|databento-maxpain-service|databento-mission-sandwich-service|directional-pressure-service|option-truth-engine-service|stock-gex-service'
# OVERNIGHT ES-tracking set — the ONLY services brought up right after the (calendar-aware, close+30) clean,
# so ES futures are tracked overnight. Everything else stays at 0 until the 06:15 ET full start. (2026-08-03: was 07:30)
# databento-feed + databento-gex-service are kept alive overnight so the pre-market LIVE OI path runs before
# the 09:30 open: while OPRA is closed the feed drains the OI-aware statistics replay and publishes the day's
# settled OI, and the gex service (alive) observes it into the OI baseline the pre-open board reads. Without
# the feed up there is no pre-market OI publisher; without gex up there is no observer. Both are needed at
# ~06:30 ET. The Databento OI fetch is plan-covered ($0), so a 24/7 dev pod adds no data cost. (2026-08-20)
OVERNIGHT_SET='es-open-direction-service es-open-direction-postgres-writer feed-gateway-service options-edge-web databento-feed databento-gex-service'
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
# 2026-07-11: for OVERNIGHT ES-future tracking the nightly clean WIPES all topics (except the sacred
# _schemas/__consumer_offsets/__transaction_state) and the overnight-start recreates them fresh. So
# WIPE_KAFKA defaults to TRUE again. (The _schemas guard in do_start's schema self-heal + the grep
# exclusion below keep the Schema Registry safe.) Set WIPE_KAFKA=false for a no-wipe run.
WIPE_KAFKA="${WIPE_KAFKA:-true}"
# Market calendar (close time, early-close, holidays) — shared with the prod scripts (scripts/jenkins/
# market_calendar.py). Used to fire the CLEAN at close+30 ET on trading days (normal 16:00 -> 16:30;
# early-close 13:00 -> 13:30).
CALENDAR_DIR="${CALENDAR_DIR:-$DEPLOY_REPO/scripts/jenkins}"
LOG=/Users/abhinav/oe-ops/dev-cleanup.log

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
  local LOGDIR2=/Users/abhinav/development/kafka-options-edge/current/logs
  [ -d "$LOGDIR2" ] && find "$LOGDIR2" -name '*.log.20*' -type f -mtime +1 -delete 2>/dev/null
}

# topic_desired NAME -> sets DPOL/DPARTS/DRET/DDR from the eval'd topics.env lists. Globals, not an
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
  DPOL=delete; DPARTS=32; DRET=""; DDR=""
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
  local desc line spec name cfg have_pol have_ret have_dr have_parts need polcfg fixed=0 drift=0
  desc="$($KT --bootstrap-server $BS --describe 2>/dev/null | grep '^Topic:')"
  if [ -z "$desc" ]; then
    echo "  WARNING: could not describe topics — declared shapes NOT verified this run."
    return 0
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
ensure_topics() {
  git -C "$DEPLOY_REPO" fetch -q origin main 2>/dev/null || true
  local tenv; tenv="$(git -C "$DEPLOY_REPO" show "$TOPICS_ENV_REF" 2>/dev/null)"
  if [ -n "$tenv" ]; then
    eval "$(printf '%s\n' "$tenv" | grep -E '^OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS|PURE_COMPACT_TOPICS|EXACT_PARTITION_TOPICS|TOPIC_RETENTION_OVERRIDES|TOPIC_DELETE_RETENTION_OVERRIDES)=')"
    local n=0 spec name extra rt
    for spec in $OPTIONS_EDGE_TOPICS; do
      name="${spec%%:*}"
      topic_desired "$name"
      extra=""; [ -n "$DDR" ]  && extra="--config delete.retention.ms=$DDR"
      rt="";    [ -n "$DRET" ] && rt="--config retention.ms=$DRET"
      $KT --bootstrap-server $BS --create --if-not-exists --topic "$name" \
        --partitions "$DPARTS" --replication-factor 1 --config cleanup.policy="$DPOL" $extra $rt >/dev/null 2>&1 && n=$((n+1))
    done
    echo "Pre-created $n platform topics from deploy config ($TOPICS_ENV_REF); apps self-create the rest on startup."
    reconcile_declared_topics
    echo "  topics present now: $($KT --bootstrap-server $BS --list 2>/dev/null | grep -vcE '^__|^_schemas')"
  else
    echo "  WARNING: could not read deploy topics.env ($DEPLOY_REPO $TOPICS_ENV_REF) — apps will create their topics on startup (slower to READY)."
  fi
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
  ensure_topics
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
}

# ---------- ES DOWN: at ~09:17 ET (before the 09:30 open) scale the overnight ES services to 0 ----------
# The overnight ES session is over; feed-gateway + web stay up for the SPX day session (untouched here).
do_es_down() {
  echo "ES-down (pre-open): scaling overnight ES services to 0 ($ES_DOWN_SET) ..."
  local d
  for d in $ES_DOWN_SET; do
    if $KK get deploy "$d" >/dev/null 2>&1; then
      $K scale deploy/"$d" --replicas=0 >/dev/null 2>&1 && echo "  down: $d"
    else
      echo "  (absent, skipped): $d"
    fi
  done
  # Clean the transient ES Kafka topics (services are down -> nothing recreates them until close+30).
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
do_start() {
  ensure_topics
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
  /Users/abhinav/kafka-4.3.0/bin/kafka-configs.sh --bootstrap-server "$BS" --alter --entity-type topics \
    --entity-name _schemas --add-config "retention.ms=-1,cleanup.policy=compact" >/dev/null 2>&1 || true
  apply_internal_topic_configs
  local gwerr; gwerr=$($KK logs deploy/feed-gateway-service --since=60s 2>/dev/null | grep -cE '40403|Schema .* not found' || true)
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
  echo "Done — source topics exist; active apps reach RUNNING, disabled set held at 0 (no replay injection)."
}

# ---------- FULL CLEAN: LOGS + DATA, leave dev down ----------
do_clean() {
  echo "=== dev FULL clean (logs + data)  $(date) ==="
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
    $KT --bootstrap-server $BS --list 2>/dev/null | grep -vE '^__|^_schemas' \
      | xargs -P 8 -I{} $KT --bootstrap-server $BS --delete --topic {} >/dev/null 2>&1
    sleep 8   # let the deletions settle before recreating (avoid create-vs-delete races)
    echo "4d) recreating platform topics (clean + recreate) ..."
    ensure_topics
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
  LIST=$($KT --bootstrap-server $BS --list 2>/dev/null)
  AFTER=$(du -sh "$KAFKA_DATA" 2>/dev/null | awk '{print $1}')
  DFREE=$(df -h /System/Volumes/Data 2>/dev/null | tail -1 | awk '{print $4" free ("$5" full)"}')
  echo "=== RESULT ==="
  echo "  Kafka data:         $BEFORE -> $AFTER   (data purged)"
  echo "  changelog topics:   $(echo "$LIST" | grep -c changelog)   (0 = success)"
  echo "  repartition topics: $(echo "$LIST" | grep -c repartition) (0 = success)"
  echo "  streams-state PVCs:  reset to empty"
  echo "  logs:                trimmed"
  echo "  disk after thin:     ${DFREE:-?}"
  echo "  apps: LEFT DOWN — 'dev-cleanup start' to bring dev back up"
  echo "DONE $(date)"
}

# ---------- dispatch ----------
MODE=${1:-auto}
case "$MODE" in
  logs)      clean_logs ;;
  start)     do_start ;;
  overnight) do_start_overnight ;;
  es-down)   do_es_down ;;
  now|clean) do_clean ;;
  auto)
    # Calendar-aware slots (ET), 2026-07-11. market_calendar.py gives the real close time so the CLEAN
    # fires at close+30 on BOTH normal (16:00->16:30) and early-close (13:00->13:30) days:
    #   CLEAN slot [close+30 .. close+59] on a trading day -> do_clean (WIPE) THEN overnight ES-tracking start.
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
clean_lo = close_dt + timedelta(minutes=30); clean_hi = clean_lo + timedelta(minutes=29)
full_lo = datetime.combine(d, time(6, 15), tz); full_hi = full_lo + timedelta(minutes=29)
# ESDOWN: ~09:17 ET, before the 09:30 open. 15-min window [09:15..09:29] so the every-15-min launchd
# reliably lands a tick before the open; ends 09:29 so it never fires after the bell.
esdown_lo = datetime.combine(d, time(9, 15), tz); esdown_hi = datetime.combine(d, time(9, 29), tz)
if clean_lo <= now <= clean_hi: slot = "CLEAN"
elif full_lo <= now <= full_hi: slot = "FULL"
elif esdown_lo <= now <= esdown_hi: slot = "ESDOWN"
else: slot = "OFF"
print(f"SLOT={slot} TD={td} CLOSE={close.strftime('%H%M')}")
PY
)
    SLOT=$(printf '%s' "$SLOTINFO" | grep -oE 'SLOT=[A-Z]+' | cut -d= -f2)
    TD=$(printf '%s' "$SLOTINFO" | grep -oE 'TD=[0-9]+' | cut -d= -f2)
    CLEAN_MARK=/tmp/.dev-cleanup-clean-$TD                     # idempotent, keyed by trading-date
    START_MARK=/tmp/.dev-cleanup-start-$TD
    ESDOWN_MARK=/tmp/.dev-cleanup-esdown-$TD
    case "$SLOT" in
      CLEAN)
        if [ -f "$CLEAN_MARK" ]; then :
        elif [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: CLEAN slot ($SLOTINFO) -> do_clean (WIPE) + overnight ES start"
        else : > "$CLEAN_MARK"; { do_clean; echo "--- overnight ES-tracking start ---"; do_start_overnight; } >> "$LOG" 2>&1; fi ;;
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
    # options.databento.oi.anchor-manifest being PURE compact with retention -1, and dev had it as
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
    ensure_topics
    ;;
  *) echo "usage: dev-cleanup [ | now | start | overnight | topics | logs ]  (no arg = calendar-gated auto for launchd)"; exit 2 ;;
esac
