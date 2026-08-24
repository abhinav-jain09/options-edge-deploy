#!/usr/bin/env bash
# apply-dev-broker-config.sh — turn OFF broker topic auto-creation on the LOCAL dev Kafka.
#
# WHY (incident 2026-07-30)
# ------------------------
# The dev broker runs with `auto.create.topics.enable=true` and `num.partitions=32`.
# Any client that merely asks for metadata about a topic that does not exist yet makes
# the broker create it — with BROKER DEFAULTS. Kafka Streams then finds its changelog
# already present and only validates the partition count; it never rewrites the config.
# Result measured on dev: ALL 52 `*-changelog` topics were `cleanup.policy=delete` with
# 24 h retention instead of `compact`. Nothing was ever compacted or deleted in-session,
# dev Kafka grew at 176.8 GB/hour, and the Mac was ~2.3 h from a full disk.
#
# The same mechanism is why 15 application topics ended up 32 partitions on dev and 1 on
# prod (see the audit block in topics.env) — each cluster stamped its own `num.partitions`.
#
# With auto-create OFF, Streams creates its own internal topics through the AdminClient
# with the correct `cleanup.policy=compact`, and an undeclared application topic fails
# LOUDLY instead of silently materialising with the wrong shape. es4 already runs this way
# (infra/es4/docker-compose.yml, KAFKA_AUTO_CREATE_TOPICS_ENABLE=false).
#
# PREREQUISITE: every application topic must be declared in scripts/kafka/topics.env.
# The 2026-07-30 audit added the 15 that were not. Do NOT run this without that change.
#
# SAFETY: dev-only. The guard below refuses to touch anything whose log.dirs is not the
# dev KRaft directory, so it cannot be pointed at prod or es4 by accident. It writes
# nothing unless --apply is passed, and always keeps a timestamped backup.
#
#   scripts/kafka/apply-dev-broker-config.sh            # dry run: show what would change
#   scripts/kafka/apply-dev-broker-config.sh --apply    # edit + restart the broker
#
# Restarting the broker drops every client connection, so run it OUTSIDE market hours.
set -uo pipefail

PROPS="${DEV_KAFKA_PROPERTIES:-/Users/abhinav/development/kafka-options-edge/config/server.properties}"
EXPECT_LOG_DIRS="${DEV_KAFKA_LOG_DIRS:-/Users/abhinav/development/kafka-options-edge/data/kraft-combined-logs}"
PLIST="${DEV_KAFKA_PLIST:-/Users/abhinav/Library/LaunchAgents/local.options-edge.kafka.plist}"
BOOTSTRAP="${DEV_KAFKA_BOOTSTRAP:-localhost:19092}"
KAFKA_BIN="${DEV_KAFKA_BIN:-/Users/abhinav/kafka-4.3.0/bin}"
APPLY=false
[ "${1:-}" = "--apply" ] && APPLY=true

die() { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------- guards (dev only)
[ -f "$PROPS" ] || die "broker properties not found: $PROPS"
grep -qF "log.dirs=$EXPECT_LOG_DIRS" "$PROPS" \
  || die "$PROPS does not point at the dev KRaft log dir ($EXPECT_LOG_DIRS). Refusing — this script is dev-only."
grep -qE '^listeners=.*:19092' "$PROPS" \
  || die "$PROPS does not listen on 19092. Refusing — this script is dev-only."

echo "dev broker properties : $PROPS"
echo "current setting       : $(grep -E '^auto.create.topics.enable' "$PROPS" || echo '<unset — Kafka defaults to TRUE>')"

if grep -qE '^auto.create.topics.enable=false[[:space:]]*$' "$PROPS"; then
  echo "already disabled — nothing to do."
  exit 0
fi

if [ "$APPLY" != "true" ]; then
  echo
  echo "DRY RUN. Would set 'auto.create.topics.enable=false' in $PROPS and restart the broker."
  echo "Re-run with --apply (outside market hours) to make the change."
  exit 0
fi

# ---------------------------------------------------------------- edit (idempotent)
BACKUP="$PROPS.bak-$(date +%Y%m%d-%H%M%S)"
cp "$PROPS" "$BACKUP" || die "could not write backup $BACKUP"
echo "backup                : $BACKUP"

if grep -qE '^[#[:space:]]*auto.create.topics.enable=' "$PROPS"; then
  # rewrite the existing key (commented or not) in place
  sed -i '' -E 's|^[#[:space:]]*auto.create.topics.enable=.*$|auto.create.topics.enable=false|' "$PROPS"
else
  cat >> "$PROPS" <<'EOF'

# Managed by scripts/kafka/apply-dev-broker-config.sh (options-edge-deploy).
# Topic auto-creation is OFF so Kafka Streams creates its own changelogs with
# cleanup.policy=compact, and an undeclared topic fails loudly instead of being
# materialised with broker defaults. Application topics live in scripts/kafka/topics.env.
auto.create.topics.enable=false
EOF
fi

grep -qE '^auto.create.topics.enable=false[[:space:]]*$' "$PROPS" \
  || { cp "$BACKUP" "$PROPS"; die "edit did not take effect — restored $BACKUP"; }
echo "new setting           : auto.create.topics.enable=false"

# ---------------------------------------------------------------- restart + verify
if [ -f "$PLIST" ]; then
  echo "restarting dev Kafka via launchctl ..."
  launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1
  sleep 5
  launchctl bootstrap "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 \
    || echo "  WARNING: launchctl bootstrap reported an error — check 'launchctl list | grep kafka'"
else
  echo "  NOTE: $PLIST not found — restart the dev broker by hand for the change to take effect."
fi

echo -n "waiting for the broker to accept connections "
for _ in $(seq 1 60); do
  if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo " up."
    break
  fi
  echo -n "."; sleep 2
done

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 \
  || die "broker did not come back on $BOOTSTRAP. Restore with: cp '$BACKUP' '$PROPS' and restart."

effective=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type brokers \
              --entity-default --describe --all 2>/dev/null | grep -oE 'auto.create.topics.enable=[a-z]+' | head -1)
echo "broker reports        : ${effective:-<could not read — verify by hand>}"
echo
echo "DONE. Next: watch for 'UNKNOWN_TOPIC_OR_PARTITION' in app logs — that now means a topic"
echo "is missing from scripts/kafka/topics.env rather than being silently auto-created wrong."
