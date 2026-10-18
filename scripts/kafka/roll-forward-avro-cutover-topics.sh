#!/usr/bin/env bash
# roll-forward-avro-cutover-topics.sh — the DESTRUCTIVE half of the 2026-10-18 Avro roll-forward.
#
# Topics in this estate are ROLLED FORWARD: a topic that must change its serialization is DELETED
# AND RECREATED under its own name, and any versioned sibling that was minted is deleted (owner,
# 2026-10-18: "there is no version, it is always rolled forward ... and no version in the system",
# with explicit authorisation to wipe these topics).
#
# WHY A SCRIPT AND NOT A ONE-OFF COMMAND. The deletion is a PRECONDITION of the rollout, not a side
# effect of it: on dev and production both topics are retention.ms=-1, and gamma-ladder-path-service
# reads oi-status as a Kafka Streams source with an AVRO serde and no explicit auto.offset.reset, so
# Streams replays from the beginning of the topic after any state loss. Roll the workloads onto a
# topic that still holds the old JSON and that replay feeds JSON to an Avro deserializer — the exact
# case the Kafka Serialization Rule forbids. An operation a rollout depends on belongs in the
# repository, where it can be reviewed and repeated, and it has a fail-closed partner:
# scripts/kafka/verify-avro-rollforward.sh refuses the rollout until this has run.
#
# WHAT IS LOST, so nobody runs this expecting otherwise: options.ibkr.gex.status was the only store
# of databento-gex-service's generation/epoch identity ledger (12,461,124 records / 1.31 GiB on prod,
# measured 2026-10-17). Wiping it makes the next mint start from zero and reissue numbers the old
# topic had spent. That is safe because a publication is bounded to a five-second event-time window
# in LadderPathProcessor, so two publications separated by the wipe cannot merge
# (options-edge-documents design/KAFKA-AVRO-CUTOVER-IBKR-GEX-STATUS.md section 12.2).
#
# THE CLUSTER IS EXPLICIT. Three brokers carry these topics and each carries a different set:
# dev and production carry the `options.*` names, es4 carries the `es.` twin its gamma-ladder-path
# slice reads. The set and the shapes live in scripts/kafka/avro-rollforward.lib.sh, shared with the
# barrier so the two cannot disagree. apply-topics.sh remains the reconciler of record; this script
# exists for the one destructive step the reconciler will not do — it never deletes a topic.
#
# TWO PHASES, because the rollout sits between them.
#
#   --recreate         delete and recreate the rolled-forward topics, then verify them.
#   --retire-siblings  delete the versioned siblings. Running it before the new image is live does
#                      not stick: the old image recreates its sibling within seconds (dev's broker
#                      has auto-create on with num.partitions=32).
#   --all              both, in order.
#
# ORDER: ROLL THE NEW IMAGE FIRST, THEN --recreate, THEN --retire-siblings.
#
# An earlier version of this header said --recreate was safe to run while the old image was still
# live, on the reasoning that the old image writes only to its VERSIONED sibling so the unversioned
# topics sit idle. THAT WAS WRONG, and it cost a contaminated topic: on 2026-10-18 the dev topic was
# recreated at 15:10 while a PRE-CUTOVER image — one that predates the versioned sibling entirely and
# writes JSON to the topic's own name — was still running. It wrote 31 hand-built JSON heartbeats
# into the fresh topic before the new image rolled at 15:59, which is precisely the retained JSON
# this whole exercise removes. "The old image" is not one thing: whatever is actually deployed may be
# older than the cutover that introduced the sibling.
#
# So the only writer that may be live when --recreate runs is the Avro one. Roll the new image first;
# verify-avro-rollforward.sh then samples both ends of each non-empty partition and refuses if
# anything non-Avro got in.
#
# Usage:
#   roll-forward-avro-cutover-topics.sh <phase> --cluster dev|production|es4 <bootstrap> [bin-dir]
# Fails closed: any unverifiable condition aborts before a versioned sibling is removed, and a
# broker that cannot be listed aborts immediately rather than looking like an empty cluster.
set -euo pipefail

usage() {
  echo "usage: roll-forward-avro-cutover-topics.sh --recreate|--retire-siblings|--all \\" >&2
  echo "         --cluster dev|production|es4 <bootstrap-servers> [kafka-bin-dir]" >&2
  echo "       the phase is REQUIRED: --retire-siblings is destructive and must not be a default." >&2
  echo "       the cluster is REQUIRED: each broker carries a different topic set." >&2
  exit 2
}
PHASE="${1:-}"
case "$PHASE" in --recreate|--retire-siblings|--all) ;; *) usage ;; esac
[ "${2:-}" = "--cluster" ] || usage
CLUSTER="${3:-}"; [ -n "$CLUSTER" ] || usage
BS="${4:-${KAFKA_BOOTSTRAP_SERVERS:-}}"; [ -n "$BS" ] || usage
BIN="${5:-${KAFKA_BIN_DIR:-}}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$HERE/avro-rollforward.lib.sh"

KT="$(rf_resolve_cli "$BIN")"
SPECS="$(rf_specs "$CLUSTER")"
kt() { "$KT" --bootstrap-server "$BS" "$@"; }

echo "=== roll forward on $BS (cluster $CLUSTER, phase $PHASE) ==="

if [ "$PHASE" = "--retire-siblings" ]; then
  # The recreate phase must already have happened, or retiring the sibling would leave the old image
  # with nowhere to write AND the JSON still in the unversioned topic. Assert it, never assume it.
  #
  # RF_ALLOW_VERSIONED_SIBLINGS=1 is not a loosening here, it is the PREMISE of this phase: the
  # siblings exist, which is why we are retiring them. Without it this precondition can never pass,
  # because the barrier refuses a surviving versioned topic — exactly the state --retire-siblings is
  # called to end. The topic-id, shape and WIRE-FORMAT checks all still apply unchanged; only the
  # sweep for the very topics about to be deleted is suppressed.
  if ! RF_ALLOW_VERSIONED_SIBLINGS=1 "$HERE/verify-avro-rollforward.sh" --cluster "$CLUSTER" "$BS" ${BIN:+"$BIN"}; then
    echo "FAIL: the rolled-forward topics do not verify, so the siblings are NOT retired." >&2
    echo "      Run --recreate first." >&2
    exit 1
  fi
fi

if [ "$PHASE" = "--recreate" ] || [ "$PHASE" = "--all" ]; then
  # ONE TOPIC AT A TIME: delete, wait for it to settle, create, then move on.
  #
  # The first cut deleted every topic and only then created them, so a create or broker failure
  # part-way left SEVERAL required topics absent at once (Codex). Per topic, the window in which
  # anything is missing covers one topic, and the barrier names exactly which one.
  #
  # The recovery procedure is to RUN THIS PHASE AGAIN: it is idempotent by construction — an absent
  # topic is created fresh, and a present one is deleted and recreated. There is nothing to roll back
  # to, because the pre-wipe content is what the roll-forward exists to remove. A topic left absent
  # is also created by its own ensure script and by apply-topics.sh, so a missed re-run degrades to
  # the broker defaults rather than to nothing — which is precisely what the barrier then refuses.
  while IFS='|' read -r topic parts cfgs pre_id; do
    [ -n "$topic" ] || continue
    LIST="$(rf_list "$KT" "$BS")" || exit 1
    if printf '%s\n' "$LIST" | grep -qx "$topic"; then
      echo "-- delete $topic"
      kt --delete --topic "$topic"
      settled=0
      for _ in $(seq 1 60); do
        # Explicit status check: a list failure must NOT be read as "the topic is gone".
        cur="$(rf_list "$KT" "$BS")" || exit 1
        if ! printf '%s\n' "$cur" | grep -qx "$topic"; then settled=1; break; fi
        sleep 2
      done
      if [ "$settled" -ne 1 ]; then
        echo "FAIL: $topic is still present 120s after the delete. Nothing further is touched; this" >&2
        echo "      phase is re-runnable once the broker settles." >&2
        exit 1
      fi
    else
      echo "-- $topic absent, creating fresh"
    fi
    args=(--create --topic "$topic" --partitions "$parts" --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-1}")
    IFS=',' read -ra kvs <<<"$cfgs"
    for kv in "${kvs[@]}"; do args+=(--config "$kv"); done
    echo "-- create $topic ($parts partitions, $cfgs)"
    if ! kt "${args[@]}"; then
      echo "FAIL: $topic was deleted but could NOT be created. It is absent now. Re-run this phase:" >&2
      echo "      roll-forward-avro-cutover-topics.sh --recreate --cluster $CLUSTER $BS" >&2
      exit 1
    fi
  done <<<"$SPECS"

  # VERIFY through the barrier itself, not a second copy of the rules: if the barrier would refuse
  # the rollout, this run has not finished its job, whatever the create commands reported.
  echo "=== verify (through the rollout barrier) ==="
  if ! "$HERE/verify-avro-rollforward.sh" --cluster "$CLUSTER" "$BS" ${BIN:+"$BIN"}; then
    echo "FAIL: the recreated topics do not satisfy the rollout barrier." >&2
    exit 1
  fi
fi

if [ "$PHASE" = "--recreate" ]; then
  echo "=== DONE on $BS: topics recreated. Roll the new image, then run --retire-siblings ==="
  exit 0
fi

echo "=== retire the versioned siblings ==="
LIST="$(rf_list "$KT" "$BS")"
SIBLINGS="$(rf_siblings "$CLUSTER")"
while read -r t; do
  [ -n "$t" ] || continue
  if printf '%s\n' "$LIST" | grep -qx "$t"; then
    echo "-- delete $t"; kt --delete --topic "$t"
  else
    echo "-- $t already absent"
  fi
done <<<"$SIBLINGS"

while read -r t; do
  [ -n "$t" ] || continue
  settled=0
  for _ in $(seq 1 60); do
    cur="$(rf_list "$KT" "$BS")" || exit 1
    if ! printf '%s\n' "$cur" | grep -qx "$t"; then settled=1; break; fi
    sleep 2
  done
  if [ "$settled" -ne 1 ]; then
    echo "FAIL: $t is still present 120s after the delete — something recreated it, which means a" >&2
    echo "      writer is still configured for it. Check that the new image is actually live." >&2
    exit 1
  fi
done <<<"$SIBLINGS"

FINAL="$(rf_list "$KT" "$BS")"
if printf '%s\n' "$FINAL" | grep -E '\.v[0-9]+$'; then
  echo "FAIL: a versioned topic name survives on $BS (listed above)." >&2
  exit 1
fi
echo "confirmed: no versioned topic names on $BS"
echo "=== DONE on $BS ==="
