#!/bin/bash
# "clean up prod" (owner rule 2026-09-16, "make prod similar to dev"): the dev-cleanup.sh flow on prod.
#
#   1. pause the es4->prod MirrorMaker launchd agents on this Mac (they produce into topics that are about to go)
#   2. on .252 as root: scripts/ops/offhours-clean-slate.sh with TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none —
#      scale every pipeline deployment to 0 (Keycloak exempt), DELETE every non-whitelisted topic (state and
#      data; the reset-preserved whitelist from topics.env incl. the prod-only entry and the system topics are
#      never touched), delete the idle consumer groups, EMPTY every Streams-state PVC in place (PVCs kept)
#   3. from this Mac (exactly what the Jenkins deploy does): scripts/kafka/apply-topics.sh with
#      ENVIRONMENT=production recreates the declared set at its declared shape, then
#      scripts/kafka/ensure-partition-only-topics.sh creates the config-less partition-only topics
#   4. resume the mirrors — only if step 3 succeeded (otherwise they stay paused and this says so loudly)
#   5. full bring-up on .252 (systemctl restart oe-boot-bringup: waves + partition doctor) unless `down`
#
#   prod-clean-slate.sh dry            steps 2 (DRY_RUN) + what 1/3 would do; changes nothing
#   prod-clean-slate.sh wipe           steps 1-5
#   prod-clean-slate.sh wipe down      steps 1-4, leave prod at 0 (Keycloak up)
#
# The calendar guard inside the host script refuses to run inside market hours. Source tree: DEPLOY_SRC
# (default: the deploy repo checkout) — the host gets the script, reset-preserved-topics.sh, topics.env and
# market_calendar.py in the repo layout it sources (~/offhours/{ops,kafka,jenkins}).
set -u
MODE="${1:-dry}"; AFTER="${2:-up}"
DEPLOY_SRC="${DEPLOY_SRC:-/Users/abhinav/development/workspace/options-edge-deploy}"
HOST=abhinav@192.168.100.252
PROD_BS=192.168.100.252:9092
PW=$(cat ~/oe-ops/.prod-ssh-pw)
WEBHOOK=$( [ -r ~/oe-ops/.prod-discord-webhook ] && cat ~/oe-ops/.prod-discord-webhook || true )
PAUSED_LIST="${PROD_MIRRORS_PAUSED:-$HOME/oe-ops/.prod-mirrors-paused}"
LOG=~/oe-ops/logs/prod-clean-slate.log; mkdir -p ~/oe-ops/logs
case "$MODE" in dry) DRY=true; WIPE=false ;; wipe) DRY=false; WIPE=true ;; *) echo "usage: $0 dry|wipe [up|down]"; exit 2 ;; esac
say() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }
ssh_root() { sshpass -p "$PW" ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 "$HOST" "su - root -c '$1'" <<EOF 2>&1 | grep -vE "^Password: *$|WARNING: |vulnerable|openssh|^\*\*" | sed 's/^Password: //'
$PW
EOF
}

# ---- es4->prod mirror agents: the launchd jobs whose producer.properties target the prod broker ----
prod_mirror_agents() {
  python3 - "$HOME/Library/LaunchAgents" "$PROD_BS" <<'PY'
import glob, os, plistlib, re, sys
agents_dir, bs = sys.argv[1], re.escape(sys.argv[2])
target = re.compile(r"^\s*bootstrap\.servers\s*=\s*" + bs + r"\s*$")
for plist in sorted(glob.glob(os.path.join(agents_dir, "com.optionsedge.*.plist"))):
    try:
        job = plistlib.load(open(plist, "rb"))
    except Exception:
        continue
    for arg in (job.get("ProgramArguments") or []) if isinstance(job, dict) else []:
        if not os.path.isabs(str(arg)):
            continue
        props = os.path.join(os.path.dirname(str(arg)), "producer.properties")
        try:
            lines = open(props).read().splitlines()
        except OSError:
            continue
        if any(target.match(l) for l in lines):
            print(job.get("Label") or os.path.basename(plist)[:-6], plist)
            break
PY
}
pause_mirrors() {
  local uid label plist n=0 i; uid=$(id -u); touch "$PAUSED_LIST"
  prod_mirror_agents | while read -r label plist; do launchctl list "$label" >/dev/null 2>&1 && printf '%s %s\n' "$label" "$plist"; done > "$PAUSED_LIST.new"
  sort -u "$PAUSED_LIST" "$PAUSED_LIST.new" > "$PAUSED_LIST.merged" && mv "$PAUSED_LIST.merged" "$PAUSED_LIST"
  while read -r label plist; do
    [ -n "$label" ] || continue
    launchctl bootout "gui/$uid/$label" >/dev/null 2>&1
    for i in 1 2 3 4 5 6 7 8 9 10; do launchctl list "$label" >/dev/null 2>&1 || break; sleep 3; done
    launchctl list "$label" >/dev/null 2>&1 && say "   WARN: mirror agent $label is still loaded" || n=$((n+1))
  done < "$PAUSED_LIST.new"; rm -f "$PAUSED_LIST.new"
  say "paused $n es4->prod mirror agent(s) (list: $PAUSED_LIST)"
}
resume_mirrors() {
  [ -s "$PAUSED_LIST" ] || return 0
  local uid label plist n=0 failed=0; uid=$(id -u)
  while read -r label plist; do
    [ -n "$label" ] || continue
    [ -f "$plist" ] || { say "   (agent removed, skipped): $label"; continue; }
    launchctl bootstrap "gui/$uid" "$plist" >/dev/null 2>&1
    launchctl list "$label" >/dev/null 2>&1 && n=$((n+1)) || { failed=$((failed+1)); say "   WARN: mirror agent $label did not load"; }
  done < "$PAUSED_LIST"
  [ "$failed" -eq 0 ] && rm -f "$PAUSED_LIST"
  say "resumed $n es4->prod mirror agent(s)"
}

# ---- 0. ship the repo layout the host script sources ----
for f in scripts/ops/offhours-clean-slate.sh scripts/kafka/reset-preserved-topics.sh scripts/kafka/topics.env scripts/jenkins/market_calendar.py scripts/kafka/apply-topics.sh scripts/kafka/ensure-partition-only-topics.sh scripts/kafka/load-kafka-settings.sh; do
  [ -r "$DEPLOY_SRC/$f" ] || { echo "missing $DEPLOY_SRC/$f"; exit 1; }
done
say "=== prod clean-slate $MODE (after=$AFTER) source=$DEPLOY_SRC ==="
sshpass -p "$PW" ssh -o StrictHostKeyChecking=no "$HOST" 'mkdir -p ~/offhours/ops ~/offhours/kafka ~/offhours/jenkins' </dev/null
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/ops/offhours-clean-slate.sh" "$HOST:offhours/ops/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/kafka/reset-preserved-topics.sh" "$DEPLOY_SRC/scripts/kafka/topics.env" "$HOST:offhours/kafka/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/jenkins/market_calendar.py" "$HOST:offhours/jenkins/"

# ---- 1. mirrors ----
if [ "$MODE" = wipe ]; then pause_mirrors; else say "dry: would pause $(prod_mirror_agents | wc -l | tr -d ' ') es4->prod mirror agents"; fi

# ---- 2. wipe on the host ----
say "host: offhours-clean-slate.sh DRY_RUN=$DRY WIPE_ENABLED=$WIPE TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none STATE_RESET_MODE=contents ENVIRONMENT=production"
ssh_root "chmod +x /home/abhinav/offhours/ops/offhours-clean-slate.sh && cd /home/abhinav/offhours/ops && \
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml CALENDAR_DIR=/home/abhinav/offhours/jenkins ENVIRONMENT=production \
  DRY_RUN=$DRY WIPE_ENABLED=$WIPE EXPECTED_ENV=prod TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none STATE_RESET_MODE=contents \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_BIN=/opt/kafka/current/bin KAFKA_LOG_DIR=/home/kafka-logs \
  EXPECTED_KAFKA_CLUSTER_ID=X4faQOI0QBq-htkrXTQISA RECLAIM_DOCKER=false DOCKER_CONTAINER_LOG_GLOB= \
  DISCORD_WEBHOOK_URL=\"$WEBHOOK\" \
  ./offhours-clean-slate.sh" | tee -a "$LOG"
RC=${PIPESTATUS[0]}
say "host wipe exit=$RC"
if [ "$MODE" = dry ]; then
  . "$DEPLOY_SRC/scripts/kafka/topics.env"
  say "dry: would recreate $(echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | wc -w | tr -d ' ') declared topics (apply-topics.sh ENVIRONMENT=production) + partition-only $(echo $OPTIONS_EDGE_PARTITION_ONLY_TOPICS | tr ' ' '\n' | grep -vc -- '-dev-') (skipping $(echo $OPTIONS_EDGE_PARTITION_ONLY_TOPICS | tr ' ' '\n' | grep -c -- '-dev-') dev-named), then bring-up=$AFTER"
  exit "$RC"
fi
[ "$RC" -eq 0 ] || { say "host wipe FAILED (exit $RC) — mirrors stay paused, no recreate, no bring-up. Fix, then rerun."; exit "$RC"; }

# ---- 3. recreate the declared set from this Mac (== Jenkins deploy path) ----
say "recreate: apply-topics.sh (ENVIRONMENT=production) + ensure-partition-only-topics.sh against $PROD_BS"
( cd "$DEPLOY_SRC" && export ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=$PROD_BS && . scripts/kafka/load-kafka-settings.sh \
  && KAFKA_RECREATE_MISMATCHED_TOPICS=false scripts/kafka/apply-topics.sh \
  && scripts/kafka/ensure-partition-only-topics.sh ) 2>&1 | grep -vE '^\s*$' | tail -25 | tee -a "$LOG"
RRC=${PIPESTATUS[0]}
( cd "$DEPLOY_SRC" && . scripts/kafka/topics.env && want=$(echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | tr ' ' '\n' | sed 's/:.*//' | sort -u) \
  && have=$(kafka-topics --bootstrap-server $PROD_BS --list 2>/dev/null) \
  && missing=$(comm -23 <(echo "$want") <(echo "$have" | sort -u)) \
  && say "declared topics present: $(( $(echo "$want" | wc -l) - $(echo "$missing" | grep -c .) ))/$(echo "$want" | wc -l | tr -d ' ')${missing:+  MISSING: $(echo $missing)}" )
if [ "$RRC" -ne 0 ]; then say "recreate FAILED (exit $RRC) — mirrors stay PAUSED (list: $PAUSED_LIST); no bring-up."; exit "$RRC"; fi

# ---- 4. mirrors back ----
resume_mirrors

# ---- 5. bring-up ----
if [ "$AFTER" = up ]; then
  say "full bring-up: systemctl restart oe-boot-bringup (waves + partition doctor)"
  ssh_root "systemctl restart oe-boot-bringup; for i in \$(seq 1 90); do [ \"\$(systemctl is-active oe-boot-bringup)\" != activating ] && break; sleep 10; done; journalctl -u oe-boot-bringup --since \"-25 min\" --no-pager -o cat | grep -E \"wave:|result:|doctor|REPAIR|UNREPAIRABLE|done\" | tail -8" | tee -a "$LOG"
else
  say "left at 0 (wipe down) — bring up with: ssh root@.252 systemctl restart oe-boot-bringup"
fi
say "=== prod clean-slate DONE ==="
