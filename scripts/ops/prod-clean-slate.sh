#!/bin/bash
# "clean up prod" (owner rule 2026-09-16): wipe EVERY Kafka topic on prod and start over — Streams changelog /
# repartition topics DELETED, every other non-system topic PURGED to zero records (the topic itself, its partition
# count and config stay, so nothing is re-created at 1 partition), consumer groups reset, Streams state EMPTIED in
# place on each PVC (PVCs are never deleted), reset-preserved topics kept. Runs the repo's reviewed
# scripts/ops/offhours-clean-slate.sh ON .252 as root (k3s admin + local-path dirs) with the prod wiring.
#
#   prod-clean-slate.sh dry            log what would be wiped, change nothing (default)
#   prod-clean-slate.sh wipe           wipe, then bring up ONLY the overnight ES set (script default)
#   prod-clean-slate.sh wipe up        wipe, then full bring-up (systemctl restart oe-boot-bringup) + lag-drain check
#
# Refuses inside market hours (calendar guard in the script). Source tree: DEPLOY_SRC (default: the deploy repo's
# origin/main checkout) — the script, reset-preserved-topics.sh, topics.env and market_calendar.py are shipped to
# ~/offhours/{ops,kafka,jenkins} on .252 in the repo layout the script expects.
set -u
MODE="${1:-dry}"; BRING_UP="${2:-}"
DEPLOY_SRC="${DEPLOY_SRC:-/Users/abhinav/development/workspace/options-edge-deploy}"
HOST=abhinav@192.168.100.252
PW=$(cat ~/oe-ops/.prod-ssh-pw)
WEBHOOK=$( [ -r ~/oe-ops/.prod-discord-webhook ] && cat ~/oe-ops/.prod-discord-webhook || true )
case "$MODE" in dry) DRY=true; WIPE=false ;; wipe) DRY=false; WIPE=true ;; *) echo "usage: $0 dry|wipe [up]"; exit 2 ;; esac
say() { echo "[$(date '+%H:%M:%S')] $*"; }
ssh_root() { sshpass -p "$PW" ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 "$HOST" "su - root -c '$1'" <<EOF 2>&1 | grep -vE "^Password: *$|WARNING: |vulnerable|openssh|^\*\*" | sed 's/^Password: //'
$PW
EOF
}
say "shipping scripts from $DEPLOY_SRC to $HOST:~/offhours (repo layout)"
for f in scripts/ops/offhours-clean-slate.sh scripts/kafka/reset-preserved-topics.sh scripts/kafka/topics.env scripts/jenkins/market_calendar.py; do
  [ -r "$DEPLOY_SRC/$f" ] || { echo "missing $DEPLOY_SRC/$f"; exit 1; }
done
sshpass -p "$PW" ssh -o StrictHostKeyChecking=no "$HOST" 'mkdir -p ~/offhours/ops ~/offhours/kafka ~/offhours/jenkins' </dev/null
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/ops/offhours-clean-slate.sh" "$HOST:offhours/ops/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/kafka/reset-preserved-topics.sh" "$DEPLOY_SRC/scripts/kafka/topics.env" "$HOST:offhours/kafka/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/jenkins/market_calendar.py" "$HOST:offhours/jenkins/"
say "running offhours-clean-slate.sh on .252 as root: DRY_RUN=$DRY WIPE_ENABLED=$WIPE STATE_RESET_MODE=contents"
ssh_root "chmod +x /home/abhinav/offhours/ops/offhours-clean-slate.sh && cd /home/abhinav/offhours/ops && \
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml CALENDAR_DIR=/home/abhinav/offhours/jenkins \
  DRY_RUN=$DRY WIPE_ENABLED=$WIPE EXPECTED_ENV=prod STATE_RESET_MODE=contents \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_BIN=/opt/kafka/current/bin KAFKA_LOG_DIR=/home/kafka-logs \
  EXPECTED_KAFKA_CLUSTER_ID=X4faQOI0QBq-htkrXTQISA RECLAIM_DOCKER=false DOCKER_CONTAINER_LOG_GLOB= \
  DISCORD_WEBHOOK_URL=\"$WEBHOOK\" \
  ./offhours-clean-slate.sh" | tee -a ~/oe-ops/logs/prod-clean-slate.log
RC=${PIPESTATUS[0]}
say "clean-slate exit=$RC"
if [ "$MODE" = wipe ] && [ "$BRING_UP" = up ]; then
  say "full bring-up: systemctl restart oe-boot-bringup (waves + partition doctor)"
  ssh_root "systemctl restart oe-boot-bringup; for i in \$(seq 1 90); do [ \"\$(systemctl is-active oe-boot-bringup)\" != activating ] && break; sleep 10; done; journalctl -u oe-boot-bringup --since \"-20 min\" --no-pager -o cat | grep -E \"wave:|result:|doctor|REPAIR|done\" | tail -8"
fi
exit "$RC"
