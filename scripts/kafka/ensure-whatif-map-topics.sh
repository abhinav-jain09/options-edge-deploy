#!/usr/bin/env bash
# ensure-whatif-map-topics.sh - whatif-map-service's ONE output topic must be right BEFORE the service publishes (the first-deploy barrier, as
# ensure-gamma-zone-topics.sh and ensure-gamma-effort-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to
# run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shape is the one scripts/kafka/topics.env declares (one partition, exact):
#   options.spx.whatif.map  1 partition, delete, retention.ms EXACTLY 604800000 (7 days), retention.bytes EXACTLY -1 (NO byte cap)  one record per
#       evaluation, carrying the WHOLE board, keyed by the CYCLE (fingerprint.valuationTime, epoch millis, as a string).
#
# WHY NOT COMPACT, which is the question this topic invites. The key is the CYCLE and not the session or the strike, so successive cycles have
# DIFFERENT keys and compaction would keep them all -- it would do nothing except cost the cleaner a pass. That is correct for a snapshot stream and
# it is what the design publishes: "a consumer cannot read the current state of strike K from a compacted topic". A per-strike topic would be a
# different record shape, which is a DESIGN change and not a deploy choice, so it is not quietly introduced here by setting a cleanup policy.
#
# WHY THERE IS NO BYTE CAP, stated rather than left as an omission. Every finite retention.bytes in topics.env is a MEASURED figure, backed by an
# EmissionSizeTest in the owning service that fails when a later field pushes the record past its budget (gamma-zone 6,992 bytes at a 1 GiB cap,
# gamma-effort 6,394 at 512 MiB). whatif-map-service has NO such test yet and this record's size has never been measured -- it carries the whole
# board, every strike and every level at once, so it is the largest emission of the three and the least safe to guess at. Declaring a round number
# here would put an unmeasured deletion contract on the topic, and a cap that is too small deletes the oldest records at once on the very first
# apply. So the TIME bound is what deletes, and the byte cap is -1 until the measurement exists. That measurement is the follow-up this barrier
# names: once whatif-map-service carries an emission-size test, declare the cap in topics.env and raise it here.
#
# WHY SEVEN DAYS. This topic is NOT a calibration corpus -- nothing is re-derived from it, no threshold is settled on it, and its one declared
# consumer (the fight-zone service, R-PUB-3/R-FZ-1) reads the CURRENT board and not the history. It is a Dev stream a human reads the day it is
# written and argues about the morning after, which is the same reading life as options.spx.gamma-effort.swing-alerts and
# options.spx.market-structure.pattern-alerts, both declared at this figure. It is therefore in NEITHER preservation list, exactly as
# futures.level-absorption.outcomes is not: a finite retention means a reset or a dev clean may take it and nothing is lost that mattered.
#
# WHY THE BARRIER EXISTS AT ALL. The service does not create this topic and does not check it: the producer would create it at the BROKER's default
# shape on the first send -- which on this cluster is a 7-day retention it did not choose and a cleanup policy nobody declared -- and a pod start that
# writes a topic into existence with whatever the running binary implies is how this fleet has lost a compaction setting before. The declared shape
# has to be applied by something that reads topics.env, and that is this script. An existing topic is never recreated, altered down or shrunk here:
# any difference fails closed.
#
# The INPUT topic (options.databento.gex.strike) belongs to another owner and is not created or altered here. The RADAR topic
# (options.spx.whatif.radar, WhatIfGridTopics.WHATIF_RADAR) is deliberately NOT created: Service B does not exist, and a topic created before its
# writer is a topic every reader can subscribe to and never hear from, which is indistinguishable from a broken writer.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-whatif-map-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); whatif-map has no production topic - refusing."
  exit 1
fi
# The DECLARED name, not overridable. An override would let this script create an arbitrary topic with whatif-map's shape, so the barrier would no
# longer match topics.env in both directions -- and the name it creates is the one nothing else declares: apply-topics, reset, dev-clean and
# premarket all work from the literal below.
MAP="options.spx.whatif.map"
if [ -n "${OE_WHATIF_MAP_TOPIC:-}" ] && [ "${OE_WHATIF_MAP_TOPIC}" != "$MAP" ]; then
  echo "FAIL: OE_WHATIF_MAP_TOPIC is set to '${OE_WHATIF_MAP_TOPIC}', but this barrier only ever ensures the declared '$MAP' - refusing. A renamed output topic is declared by nothing and read by nobody."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
MIN_RF="${WHATIF_MAP_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${WHATIF_MAP_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-whatif-map-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

MAP_RETENTION_MS=604800000

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what a service's own describeConfigs sees) and
# compare exactly. Never altered here. `--describe --all` prints SYNONYMS as well as the effective value, which is why the comparison goes through
# extract()'s own boundary rather than a bare grep: a bare grep counts one value several times and refuses a correct topic.
check_retention_bytes() { # <topic> <exact retention.bytes>
  local topic="$1" want="$2" cfg bytes
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  bytes=$(extract 'retention\.bytes' "$cfg")
  case "${bytes:-}" in
    ''|*[!0-9-]*|-|?*-*|-[!1]*|-1?*) echo "FAIL: '$topic' reports retention.bytes='${bytes:-<unreadable>}', expected exactly $want."; return 1 ;;
  esac
  if [ "$bytes" != "$want" ]; then
    echo "FAIL: '$topic' has retention.bytes=$bytes, expected exactly $want (the declared shape in scripts/kafka/topics.env; never altered here)."
    return 1
  fi
  echo "ok: '$topic' — retention.bytes=$bytes"
}

rc=0

# options.spx.whatif.map — the board stream. Created here WITH retention.bytes=-1 so the time bound is the only thing that deletes, then verified by
# the shared routine with create-if-absent=no, so this path can never leave a topic at the broker's default shape.
if ! grep -qxF "$MAP" <<<"$LIST"; then
  echo "creating '$MAP' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=$MAP_RETENTION_MS, retention.bytes=-1)"
  if ! out=$(kt --create --if-not-exists --topic "$MAP" --partitions 1 --replication-factor "$RF" \
                --config cleanup.policy=delete --config retention.ms="$MAP_RETENTION_MS" --config retention.bytes=-1 \
                --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
    echo "FAIL: could not create '$MAP':"; printf '%s\n' "$out"; rc=1
  else
    LIST="$LIST"$'\n'"$MAP"
  fi
fi
if [ "$rc" -eq 0 ]; then
  ensure_topic "$MAP" delete "exact:$MAP_RETENTION_MS" 1048576 no || rc=1
  check_retention_bytes "$MAP" -1 || rc=1
fi

if [ "$rc" -ne 0 ]; then
  echo "=== ensure-whatif-map-topics FAILED - the what-if map service must not publish ==="
  exit 1
fi
echo "=== ensure-whatif-map-topics OK ==="
