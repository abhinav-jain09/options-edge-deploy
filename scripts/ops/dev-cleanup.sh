#!/usr/bin/env bash
# dev-cleanup.sh — THE one dev cleanup script. Once a day (end of day, ET-gated) it cleans BOTH
# the logs AND the Kafka data in a single run, and each weekday morning it brings dev back up.
# No 60-second log trimming, no separate scheduler wrapper — this one file + one launchd is the whole thing.
#
# MODES:
#   dev-cleanup           # AUTO — what launchd calls every ~15 min. ET wall-clock gated:
#                         #   20:30-20:59 ET nightly  -> full clean (logs + data wipe), leave dev DOWN
#                         #   09:00-09:29 ET weekdays -> start (pre-create topics, scale apps up)
#   dev-cleanup now       # run the full clean right now (logs + data), ignoring the time gate
#   dev-cleanup start     # bring dev up now
#   dev-cleanup logs      # clean logs only (manual)
#
# Full clean deletes every non-system topic + every *-streams-state PVC, trims the broker logs, and
# prunes docker-engine build cache. Keeps __consumer_offsets/__transaction_state/_schemas + Keycloak.
# Needs bash 4+ and the jenkins-deployer SA (docker-desktop enforces the jenkins-only admission policy).
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

K="kubectl --context docker-desktop -n options-edge --as=system:serviceaccount:options-edge:jenkins-deployer"
KK="kubectl --context docker-desktop -n options-edge"
KT=/Users/abhinav/kafka-4.3.0/bin/kafka-topics.sh
BS=localhost:19092
KAFKA_DATA=/Users/abhinav/development/kafka-options-edge/data
KEEP='keycloak'                                          # deployments to leave running (regex)
# DEV-disabled deployments — held at 0 by do_start (exact full-name match via grep -x). MUST include
# databento-timewarp-snapshot-replay: if it boots even briefly it replays snapshots into
# options.databento.raw and poisons the chain. Keep in sync with premarket-check.sh DISABLED (dev).
DISABLED_DEV='hpsf-stage-a-service|hpsf-stage-b-service|volume-pace-service|volume-sandwich-service|volume-sandwich-databento-service|unusual-whales-gex-service|unusual-whales-gex-history-service|databento-timewarp-snapshot-replay|ibkr-feed-service|strike-flow-classifier-ibkr|options-edge-integration-test|databento-mission-pressure-service|databento-mission-pace-service|spx-mission-control-service'
# Topic source-of-truth = the deploy repo's scripts/kafka/topics.env (the SAME file the deploy's
# apply-topics.sh uses), read from origin/main so it is the reviewed, versioned config — NOT an
# autonomous live snapshot. dev-cleanup pre-creates ONLY these platform/feed topics; every service
# self-creates its own output + Kafka-Streams internal topics on startup, exactly like prod.
DEPLOY_REPO="${DEPLOY_REPO:-/Users/abhinav/development/workspace/options-edge-deploy}"
TOPICS_ENV_REF="${TOPICS_ENV_REF:-origin/main:scripts/kafka/topics.env}"
# 2026-07-08: Kafka now self-expires data via a 12h broker retention (log.retention.ms=43200000),
# so we NO LONGER delete+recreate topics or reset Streams-state PVCs nightly (that churn caused the
# _schemas wipe + boot-order races). WIPE_KAFKA=false skips the topic-delete + PVC-reset; the nightly
# run just scales apps down + trims logs/registry/snapshots. Set WIPE_KAFKA=true for a hard reset.
WIPE_KAFKA="${WIPE_KAFKA:-false}"
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

# ---------- START: pre-create source topics, then scale apps up READY ----------
do_start() {
  # Pre-create the platform topics from the deploy repo's topics.env (source of truth). Best-effort
  # fetch so we pick up the latest reviewed config; if offline we use the last-fetched origin/main.
  git -C "$DEPLOY_REPO" fetch -q origin main 2>/dev/null || true
  local tenv; tenv="$(git -C "$DEPLOY_REPO" show "$TOPICS_ENV_REF" 2>/dev/null)"
  if [ -n "$tenv" ]; then
    eval "$(printf '%s\n' "$tenv" | grep -E '^OPTIONS_EDGE_(TOPICS|COMPACTED_TOPICS)=')"
    local n=0 spec name parts pol
    for spec in $OPTIONS_EDGE_TOPICS; do
      name="${spec%%:*}"; parts="${spec##*:}"; pol=delete
      case " $OPTIONS_EDGE_COMPACTED_TOPICS " in *" $name "*) pol=compact ;; esac
      $KT --bootstrap-server $BS --create --if-not-exists --topic "$name" \
        --partitions "${parts:-32}" --replication-factor 1 --config cleanup.policy="$pol" >/dev/null 2>&1 && n=$((n+1))
    done
    echo "Pre-created $n platform topics from deploy config ($TOPICS_ENV_REF); apps self-create the rest on startup."
    echo "  topics present now: $($KT --bootstrap-server $BS --list 2>/dev/null | grep -vcE '^__|^_schemas')"
  else
    echo "  WARNING: could not read deploy topics.env ($DEPLOY_REPO $TOPICS_ENV_REF) — apps will create their topics on startup (slower to READY)."
  fi
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

  # 4. delete ALL non-system topics — ONLY on a hard WIPE_KAFKA reset. Normally Kafka's 12h retention
  #    expires data itself, so topics + Streams state PERSIST across the nightly run (no recreate churn).
  if [ "$WIPE_KAFKA" != true ]; then
    echo "4) keeping topics (Kafka 12h retention expires data itself — WIPE_KAFKA=false)"
  else
    echo "4) deleting all non-system topics ..."
    $KT --bootstrap-server $BS --list 2>/dev/null | grep -vE '^__|^_schemas' \
      | xargs -P 8 -I{} $KT --bootstrap-server $BS --delete --topic {} >/dev/null 2>&1
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
  now|clean) do_clean ;;
  auto)
    # launchd has NO timezone -> compute ET wall-clock ourselves (DST-proof). Overridable for dry-testing:
    #   NOW_ET=2035 NOW_DOW=3 DKC_DRYRUN=1 dev-cleanup   # dry-test the nightly clean slot
    HHMM=${NOW_ET:-$(TZ=America/New_York date '+%H%M')}
    DOW=${NOW_DOW:-$(TZ=America/New_York date '+%u')}          # 1=Mon .. 7=Sun
    DATE=${NOW_DATE:-$(TZ=America/New_York date '+%Y%m%d')}
    HH=$((10#$HHMM))                                           # strip leading zero (0900 would be octal)
    CLEAN_MARK=/tmp/.dev-cleanup-clean-$DATE                   # idempotent: once per calendar day
    START_MARK=/tmp/.dev-cleanup-start-$DATE
    if [ "$HH" -ge 2030 ] && [ "$HH" -le 2059 ] && [ ! -f "$CLEAN_MARK" ]; then
      if [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: CLEAN slot -> do_clean (logs+data, leave down)"; \
      else do_clean >> "$LOG" 2>&1; : > "$CLEAN_MARK"; fi
    elif [ "$HH" -ge 900 ] && [ "$HH" -le 929 ] && [ "$DOW" -le 5 ] && [ ! -f "$START_MARK" ] && ls /tmp/.dev-cleanup-clean-* >/dev/null 2>&1; then
      if [ "${DKC_DRYRUN:-0}" = 1 ]; then echo "DRYRUN: START slot -> do_start (bring dev up)"; \
      else do_start >> "$LOG" 2>&1; : > "$START_MARK"; fi
    else
      [ "${DKC_DRYRUN:-0}" = 1 ] && echo "DRYRUN: off-slot (ET=$HHMM DOW=$DOW) -> nothing to do"
    fi
    ;;
  *) echo "usage: dev-cleanup [ | now | start | logs ]  (no arg = ET-gated auto for launchd)"; exit 2 ;;
esac
