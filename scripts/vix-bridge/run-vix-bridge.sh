#!/usr/bin/env bash
#
# run-vix-bridge.sh — live-mirror underlying.vix.price from PROD Kafka into DEV Kafka.
# Dev has no IBKR feed, so it pulls VIX from prod and republishes it on the dev topic.
#
# Runs continuously (idles silently when VIX isn't ticking, e.g. off-hours). Meant to run as a
# daemon (see com.optionsedge.vixbridge.plist) or in the foreground for a quick test.
#
#   env: SOURCE_BOOTSTRAP  prod Kafka   (default 192.168.100.252:9092)
#        TARGET_BOOTSTRAP  dev Kafka    (default localhost:19092)
#        TOPIC             (default underlying.vix.price)
#        PRIME=0           don't seed the latest retained value on start
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SOURCE_BOOTSTRAP="${SOURCE_BOOTSTRAP:-192.168.100.252:9092}"
TARGET_BOOTSTRAP="${TARGET_BOOTSTRAP:-localhost:19092}"
TOPIC="${TOPIC:-underlying.vix.price}"
VENV="${OE_KAFKA_VENV:-$HOME/.oe-kafka-archive-venv}"   # reuse the kafka-archive venv (kafka-python)

log(){ printf '\033[1;36m[vix-bridge]\033[0m %s\n' "$*"; }

# reuse (or create) the pure-python kafka venv already used by scripts/kafka-archive
if [ ! -x "$VENV/bin/python" ]; then
  log "creating venv $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip kafka-python lz4
fi
# lz4 is REQUIRED: prod produces underlying.vix.price with lz4 compression; without it kafka-python
# throws UnsupportedCodecError on every message → reconnect loop → FD leak → bridge wedges (VIX stops
# mirroring to dev). Ensure both kafka-python AND lz4 are present.
"$VENV/bin/python" -c 'import kafka, lz4.frame' 2>/dev/null || "$VENV/bin/pip" install --quiet kafka-python lz4

PRIME_FLAG=""; [ "${PRIME:-1}" = "0" ] && PRIME_FLAG="--no-prime"
log "mirror $TOPIC:  $SOURCE_BOOTSTRAP  ->  $TARGET_BOOTSTRAP  ${PRIME_FLAG:-(prime latest)}"
exec "$VENV/bin/python" "$HERE/vix_bridge.py" \
  --source "$SOURCE_BOOTSTRAP" --target "$TARGET_BOOTSTRAP" --topic "$TOPIC" $PRIME_FLAG
