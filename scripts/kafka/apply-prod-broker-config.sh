#!/usr/bin/env bash
# apply-prod-broker-config.sh — turn OFF broker topic auto-creation on the PROD Kafka (192.168.100.252).
#
# WHY: with `auto.create.topics.enable=true` (Kafka's default; prod never set it) any client that asks for
# metadata about a missing topic makes the broker create it at `num.partitions=1` with default config.
# That is how Kafka Streams sources ended up at 1 partition while their readers needed 4/8/32, and the
# apps then refused to start ("invalid partitions" / co-partitioning) — the recurring failure behind the
# 2026-09-15 partition audit. es4 already runs with auto-create OFF (infra/es4/docker-compose.yml) and dev
# is switched by apply-dev-broker-config.sh. With it OFF, Streams still creates its internal topics
# through the AdminClient, services still create their own topics (ensureTopic), deploys still run
# apply-topics.sh; only a topic NOBODY created fails loudly instead of appearing at a size nobody chose.
#
# Prod does not wipe topics, so every topic a running service needs already exists; this changes what
# happens to the NEXT missing topic, not to any existing one.
#
# SAFETY: runs ON the prod host only (guards below), dry run unless --apply, timestamped backup, and the
# edit is reverted if the broker does not come back. Restarting drops every client connection for the
# broker's shutdown + recovery time: run OUTSIDE market hours.
#
#   sudo scripts/kafka/apply-prod-broker-config.sh            # dry run
#   sudo scripts/kafka/apply-prod-broker-config.sh --apply    # edit + restart kafka.service + verify
set -uo pipefail

PROPS="${PROD_KAFKA_PROPERTIES:-/opt/kafka/current/config/server.properties}"
EXPECT_LOG_DIRS="${PROD_KAFKA_LOG_DIRS:-/home/kafka/kraft-combined-logs}"
UNIT="${PROD_KAFKA_UNIT:-kafka}"
BOOTSTRAP="${PROD_KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${PROD_KAFKA_BIN:-/opt/kafka/current/bin}"
WAIT_SECONDS="${PROD_KAFKA_WAIT_SECONDS:-900}"
APPLY=false
[ "${1:-}" = "--apply" ] && APPLY=true

die() { echo "ERROR: $*" >&2; exit 1; }

# Put the backup back, restart, and WAIT until the broker answers again before giving up.
revert() {
  cp -p "$BACKUP" "$PROPS"
  systemctl restart "$UNIT"
  local w=0
  until "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
    [ "$w" -ge "$WAIT_SECONDS" ] && die "$1 — REVERTED $BACKUP but the broker is STILL not answering after ${WAIT_SECONDS}s: investigate now"
    sleep 10; w=$((w + 10))
  done
  die "$1 — reverted to $BACKUP; broker answering again after ${w}s"
}

[ -f "$PROPS" ] || die "broker properties not found: $PROPS"
grep -qF "log.dirs=$EXPECT_LOG_DIRS" "$PROPS" \
  || die "$PROPS does not point at the prod KRaft log dir ($EXPECT_LOG_DIRS). Refusing — this script is prod-only."
grep -qE '^listeners=.*:9092' "$PROPS" || die "$PROPS does not listen on 9092. Refusing."
NODE_ID=$(awk -F= '/^node.id=/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "$PROPS")
[ -n "$NODE_ID" ] || die "no node.id in $PROPS. Refusing."
systemctl cat "$UNIT" >/dev/null 2>&1 || die "systemd unit $UNIT not found. Refusing."

echo "prod broker properties : $PROPS"
echo "current setting        : $(grep -E '^auto.create.topics.enable' "$PROPS" || echo '<unset — Kafka defaults to TRUE>')"
if grep -qE '^auto.create.topics.enable=false[[:space:]]*$' "$PROPS"; then
  echo "already disabled — nothing to do."
  exit 0
fi
if [ "$APPLY" != "true" ]; then
  echo
  echo "DRY RUN. Would set 'auto.create.topics.enable=false' in $PROPS and restart $UNIT."
  exit 0
fi

BACKUP="$PROPS.bak-$(date +%Y%m%d-%H%M%S)"
cp -p "$PROPS" "$BACKUP" || die "could not write backup $BACKUP"
echo "backup                 : $BACKUP"
if grep -qE '^[#[:space:]]*auto.create.topics.enable=' "$PROPS"; then
  sed -i -E 's|^[#[:space:]]*auto.create.topics.enable=.*$|auto.create.topics.enable=false|' "$PROPS"
else
  cat >> "$PROPS" <<'EOF'

# Managed by scripts/kafka/apply-prod-broker-config.sh (options-edge-deploy).
# Topic auto-creation is OFF: a topic nobody created fails loudly instead of appearing at
# num.partitions=1 with default config. Application topics: scripts/kafka/topics.env.
auto.create.topics.enable=false
EOF
fi
grep -qE '^auto.create.topics.enable=false[[:space:]]*$' "$PROPS" \
  || { cp -p "$BACKUP" "$PROPS"; die "edit did not take effect — restored $BACKUP"; }

echo "restarting $UNIT ..."
systemctl restart "$UNIT" || revert "systemctl restart failed"
waited=0
until "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  [ "$waited" -ge "$WAIT_SECONDS" ] && revert "broker did not answer within ${WAIT_SECONDS}s"
  sleep 10; waited=$((waited + 10))
done
echo "broker answering after ${waited}s"
effective=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type brokers --entity-name "$NODE_ID" --describe --all 2>/dev/null \
              | grep -oE 'auto.create.topics.enable=[a-z]+' | head -1)
echo "broker reports         : ${effective:-<could not read — verify by hand>}"
[ "$effective" = "auto.create.topics.enable=false" ] || die "broker does not report auto.create.topics.enable=false"
echo "DONE."
