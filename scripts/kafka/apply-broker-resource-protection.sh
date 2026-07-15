#!/usr/bin/env bash
# Apply resource protection to the native Kafka broker used by dev (macOS launchd)
# or production (Linux systemd). Set RESTART_KAFKA=true to activate immediately.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RESTART_KAFKA="${RESTART_KAFKA:-false}"

case "$(uname -s)" in
  Darwin)
    label="${KAFKA_LAUNCHD_LABEL:-local.options-edge.kafka}"
    plist="${KAFKA_LAUNCHD_PLIST:-$HOME/Library/LaunchAgents/$label.plist}"
    [ -f "$plist" ] || { echo "Kafka launchd plist not found: $plist" >&2; exit 1; }

    /usr/libexec/PlistBuddy -c "Delete :ProcessType" "$plist" >/dev/null 2>&1 || true
    /usr/libexec/PlistBuddy -c "Add :ProcessType string Interactive" "$plist"
    /usr/libexec/PlistBuddy -c "Delete :LowPriorityIO" "$plist" >/dev/null 2>&1 || true
    /usr/libexec/PlistBuddy -c "Add :LowPriorityIO bool false" "$plist"
    plutil -lint "$plist"

    if [ "$RESTART_KAFKA" = "true" ]; then
      launchctl kickstart -k "gui/$(id -u)/$label"
    fi
    echo "Applied macOS Kafka protection (ProcessType=Interactive, LowPriorityIO=false)."
    ;;
  Linux)
    unit="${KAFKA_SYSTEMD_UNIT:-kafka.service}"
    dropin_dir="/etc/systemd/system/$unit.d"
    sudo install -d -m 0755 "$dropin_dir"
    sudo install -m 0644 \
      "$ROOT_DIR/infra/kafka/systemd/kafka-resource-protection.conf" \
      "$dropin_dir/98-resource-protection.conf"
    sudo systemctl daemon-reload

    if [ "$RESTART_KAFKA" = "true" ]; then
      sudo systemctl restart "$unit"
    fi
    sudo systemctl show "$unit" \
      -p CPUWeight -p IOWeight -p MemoryLow -p MemorySwapMax -p OOMScoreAdjust
    ;;
  *)
    echo "Unsupported broker host OS: $(uname -s)" >&2
    exit 1
    ;;
esac
