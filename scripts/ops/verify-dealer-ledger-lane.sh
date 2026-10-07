#!/usr/bin/env bash
# Is the dealer-ledger OFFLOAD LANE actually delivering to a target, so holding the local ledger
# down removes a DUPLICATE producer rather than the only one?
#
# WHY THIS EXISTS. The hold restored by this repo's revert of #1148 is correct only once the lane
# feeds the target. Codex r1 on that revert made the point that the ordering lived in commit prose:
# "deploy #1159 first" is not enforceable by a sentence. This turns it into an exit status.
#
#   scripts/ops/verify-dealer-ledger-lane.sh prod      # .74 -> prod   (leg 2)
#   scripts/ops/verify-dealer-ledger-lane.sh dev       # .74 -> dev    (leg 3)
#
# Exit 0 = the lane is delivering to that target and the local ledger may be held down.
# Exit 1 = it is NOT; holding the local ledger down would take the target's profile to zero.
# Read-only: it reads offsets and launchd state. It changes nothing and never scales anything.
set -uo pipefail

TARGET="${1:-}"
case "$TARGET" in
  prod) TGT_BOOTSTRAP="${PROD_BOOTSTRAP:-192.168.100.252:9092}" ;;
  dev)  TGT_BOOTSTRAP="${DEV_BOOTSTRAP:-host.docker.internal:19092}" ;;
  *)    echo "usage: $0 prod|dev" >&2; exit 2 ;;
esac
SRC_BOOTSTRAP="${MAC74_BOOTSTRAP:-192.168.100.74:9092}"
KBIN="${KBIN:-$HOME/development/confluent-7.3.1/bin}"
TOPIC="${LANE_TOPIC:-dealer-ledger-profile}"
SETTLE="${LANE_SETTLE_SECONDS:-60}"

fail() { echo "LANE NOT DELIVERING to $TARGET: $*" >&2; exit 1; }
[ -x "$KBIN/kafka-get-offsets" ] || fail "no kafka CLI at $KBIN"

# A missing topic prints NOTHING and an awk sum of nothing is 0, which is indistinguishable from
# empty. Establish existence separately — conflating the two is how six topics were once reported
# empty when they did not exist.
offs() { # $1 bootstrap
  "$KBIN/kafka-topics" --bootstrap-server "$1" --describe --topic "$TOPIC" >/dev/null 2>&1 || { echo ABSENT; return; }
  local o; o=$("$KBIN/kafka-get-offsets" --bootstrap-server "$1" --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  [ -n "$o" ] && echo "$o" || echo ABSENT
}

echo "lane check: $SRC_BOOTSTRAP (.74, the ledger) -> $TGT_BOOTSTRAP ($TARGET), topic $TOPIC"

src=$(offs "$SRC_BOOTSTRAP"); echo "  source .74 : $src"
[ "$src" = ABSENT ] && fail "$TOPIC does not exist on .74 — the ledger is not producing there"
[ "$src" -gt 0 ] 2>/dev/null || fail "$TOPIC is empty on .74 — nothing to mirror"

t0=$(offs "$TGT_BOOTSTRAP"); echo "  target t0  : $t0"
[ "$t0" = ABSENT ] && fail "$TOPIC does not exist on $TARGET — the mirror has never delivered"

# A non-zero target proves history, not a live leg: the local ledger may have written it. The only
# evidence that the MIRROR is delivering is movement while the local producer is irrelevant, so
# sample twice.
echo "  sampling for ${SETTLE}s to prove the target is ADVANCING, not merely non-empty..."
sleep "$SETTLE"
t1=$(offs "$TGT_BOOTSTRAP"); echo "  target t1  : $t1"
[ "$t1" = ABSENT ] && fail "$TOPIC vanished from $TARGET mid-check"
[ "$t1" -gt "$t0" ] || fail "$TOPIC did not advance on $TARGET over ${SETTLE}s (t0=$t0 t1=$t1) — either the mirror is not running or .74 is idle (outside RTH this can be a false negative; re-run during a session)"

echo "LANE DELIVERING to $TARGET: target advanced $((t1 - t0)) records in ${SETTLE}s."
echo "Holding the local ledger down now removes a duplicate producer, not the only one."
