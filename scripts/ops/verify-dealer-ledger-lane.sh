#!/usr/bin/env bash
# Is the dealer-ledger OFFLOAD LANE actually delivering to a target, so holding the local ledger
# down removes a DUPLICATE producer rather than the only one?
#
#   scripts/ops/verify-dealer-ledger-lane.sh prod      # .74 -> prod   (leg 2)
#   scripts/ops/verify-dealer-ledger-lane.sh dev       # .74 -> dev    (leg 3)
#
# Exit 0 = the MIRROR is demonstrably delivering; the local ledger may be held down.
#      1 = it is not; holding the local ledger down would take the target's profile to zero.
#      2 = usage.
# Read-only: it reads Kafka topic metadata, topic offsets and consumer-group state. Nothing else —
# no launchd, no kubectl, no scaling.
#
# ⚠ WHY IT DOES NOT JUST WATCH THE TARGET OFFSET. The first version of this script did exactly that
# and it was WRONG in the dangerous direction (Codex r2 on deploy#1161, P0): Kafka offsets carry no
# producer attribution, so while prod's LOCAL ledger is still running it advances
# dealer-ledger-profile itself and a two-sample target check returns 0 with leg 2 dead or absent.
# That is a false PASS on a release precondition — precisely the failure the check exists to prevent.
#
# WHAT ESTABLISHES PROVENANCE INSTEAD. kafka-mirror-maker commits its SOURCE offsets only after the
# producer has accepted the batch, so forward progress of the MIRROR'S OWN CONSUMER GROUP at .74 is
# evidence that records left .74 and were produced to the target. That group is the only signal here
# that no local producer can forge. So the verdict rests on: the group exists, has a live member, and
# its committed position ADVANCES. The target's own offset is reported for the operator but is NOT
# sufficient, and the script says so when the local producer is still up.
set -uo pipefail

TARGET="${1:-}"
case "$TARGET" in
  prod) TGT_BOOTSTRAP="${PROD_BOOTSTRAP:-192.168.100.252:9092}"; TGT_GROUP="${PROD_LANE_GROUP:-oe-prod-mirror-from-mac74}" ;;
  dev)  TGT_BOOTSTRAP="${DEV_BOOTSTRAP:-host.docker.internal:19092}"; TGT_GROUP="${DEV_LANE_GROUP:-oe-dev-mirror-from-mac74}" ;;
  *)    echo "usage: $0 prod|dev" >&2; exit 2 ;;
esac
SRC_BOOTSTRAP="${MAC74_BOOTSTRAP:-192.168.100.74:9092}"
KBIN="${KBIN:-$HOME/development/confluent-7.3.1/bin}"
TOPIC="${LANE_TOPIC:-dealer-ledger-profile}"
# ⚠ MUST EXCEED MirrorMaker v1's offset.commit.interval.ms, which defaults to 60000 and is not
# declared in any mirror's consumer.properties. The verdict below is the mirror GROUP's committed
# position, and a window at or under the commit interval can see zero movement while records are
# flowing — a FALSE NEGATIVE that would block a legitimate deploy forever. Measured on leg 3 on
# 2026-10-07: a 45s window saw +0 while the target advanced 81 records; a 150s window saw +623.
SETTLE="${LANE_SETTLE_SECONDS:-150}"

fail() { echo "LANE NOT DELIVERING to $TARGET: $*" >&2; exit 1; }
[ -x "$KBIN/kafka-get-offsets" ] || fail "no kafka CLI at $KBIN"
# Refuse a window that cannot distinguish "not committing" from "has not committed yet".
case "$SETTLE" in ''|*[!0-9]*) echo "LANE_SETTLE_SECONDS must be an integer" >&2; exit 2 ;; esac
[ "$SETTLE" -gt 60 ] || { echo "LANE_SETTLE_SECONDS=$SETTLE is <= MirrorMaker's 60s commit interval; it would report a false negative. Use >60." >&2; exit 2; }

# A missing topic prints NOTHING and an awk sum of nothing is 0, indistinguishable from empty.
# Establish existence separately — conflating the two is how six topics were once called empty when
# they did not exist.
offs() { # $1 bootstrap
  "$KBIN/kafka-topics" --bootstrap-server "$1" --describe --topic "$TOPIC" >/dev/null 2>&1 || { echo ABSENT; return; }
  local o; o=$("$KBIN/kafka-get-offsets" --bootstrap-server "$1" --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  [ -n "$o" ] && echo "$o" || echo ABSENT
}
# Sum of the mirror group's COMMITTED offsets at the source, across the lane's topics.
# ⚠ SCOPED TO $TOPIC (Codex r3, P0). The first version summed committed offsets for EVERY topic in
# the group. The .74->prod group carries four — profile, state, signal-fired, outcome-scored — so
# movement on any ONE of them passed a gate that claims the PROFILE is being delivered, which is the
# only one close-direction needs. That is a false pass, in the direction that matters.
grp_pos() { "$KBIN/kafka-consumer-groups" --bootstrap-server "$SRC_BOOTSTRAP" --group "$TGT_GROUP" --describe 2>/dev/null \
    | awk -v t="$TOPIC" 'NR>1 && $2==t && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}'; }
# `--describe --state` on a NON-EXISTENT group prints prose ("Consumer group 'x' does not exist."),
# not an empty result, so a naive last-two-fields awk returns "not exist." — a non-empty string that
# sails past an emptiness test and lands in the catch-all branch. Detect the prose explicitly and
# only then parse the table.
grp_raw() { "$KBIN/kafka-consumer-groups" --bootstrap-server "$SRC_BOOTSTRAP" --group "$TGT_GROUP" --describe --state 2>/dev/null; }
grp_members() { local r; r=$(grp_raw)
  case "$r" in *"does not exist"*) echo ABSENT; return ;; esac
  # Match the row for THIS group rather than "any line after the first": the tool emits a blank
  # line and then a header, so NR>1 printed "STATE #MEMBERS".
  printf '%s\n' "$r" | awk -v g="$TGT_GROUP" '$1==g && NF>3 {print $(NF-1), $NF}'; }

echo "lane check: $SRC_BOOTSTRAP (.74, the ledger) -> $TGT_BOOTSTRAP ($TARGET), topic $TOPIC"
echo "            mirror group at source: $TGT_GROUP"

src=$(offs "$SRC_BOOTSTRAP"); echo "  source .74        : $src"
[ "$src" = ABSENT ] && fail "$TOPIC does not exist on .74 — the ledger is not producing there"
[ "$src" -gt 0 ] 2>/dev/null || fail "$TOPIC is empty on .74 — nothing to mirror"

state=$(grp_members); echo "  mirror group      : ${state:-<no such group>}"
case "$state" in
  ABSENT|"") fail "consumer group $TGT_GROUP does not exist at .74 — the mirror has never run. This is the expected state until deploy#1159 provisions leg 2." ;;
  *Stable*) : ;;
  *Empty*) fail "consumer group $TGT_GROUP exists at .74 but has NO members — the mirror is not running" ;;
  *)  echo "  (group state is not Stable; continuing to the progress test, which decides)" ;;
esac

t0=$(offs "$TGT_BOOTSTRAP"); g0=$(grp_pos); s0=$(offs "$SRC_BOOTSTRAP")
echo "  target t0         : $t0"
echo "  group committed g0: $g0   (scoped to $TOPIC)"
echo "  source t0         : $s0"
[ "$t0" = ABSENT ] && fail "$TOPIC does not exist on $TARGET — the mirror has never delivered"

echo "  sampling ${SETTLE}s for MIRROR progress (group position), not merely target offset..."
sleep "$SETTLE"
t1=$(offs "$TGT_BOOTSTRAP"); g1=$(grp_pos); s1=$(offs "$SRC_BOOTSTRAP")
echo "  target t1         : $t1"
echo "  group committed g1: $g1   (scoped to $TOPIC)"
echo "  source t1         : $s1"

# THE VERDICT IS THE GROUP, SCOPED TO THIS TOPIC — only that attributes records to the mirror.
#
# But a non-advancing group has TWO causes and they are not the same thing (Codex r3, P1): a broken
# mirror, or a source that simply produced nothing in the window. The earlier version blamed
# "off-hours", which is only one way to have an idle source — a sparse source mid-session reads
# identically. So snapshot the SOURCE too and separate the three outcomes. All non-PASS outcomes
# still BLOCK, because the gate's job is positive proof; what changes is what the operator is told.
if [ "${g1:-0}" -le "${g0:-0}" ]; then
  if [ "${s1:-0}" -le "${s0:-0}" ]; then
    fail "INCONCLUSIVE, not a failure: the SOURCE did not advance either (s0=$s0 s1=$s1), so there was
      no traffic to mirror in ${SETTLE}s and this window proves nothing either way. The ledger on .74
      is idle — expected off-hours, and possible mid-session on a sparse topic. RETRY during active
      flow. Blocking anyway, because deployment needs positive proof, not absence of evidence."
  fi
  echo "  (target moved $(( ${t1:-0} - ${t0:-0} )) records, but that is NOT attributable to the mirror)" >&2
  fail "MIRROR FAILURE: the source advanced $(( ${s1:-0} - ${s0:-0} )) records for $TOPIC in ${SETTLE}s
      but the mirror group $TGT_GROUP committed nothing (g0=$g0 g1=$g1). Records exist and are not
      being carried. Check the agent on the host that runs this leg."
fi

echo "LANE DELIVERING to $TARGET: for $TOPIC the mirror group advanced $((g1 - g0)) committed records at .74"
echo "in ${SETTLE}s, which it commits only after the target's producer accepted them. Target moved"
echo "$(( ${t1:-0} - ${t0:-0} )). Holding the local ledger down now removes a duplicate producer."
