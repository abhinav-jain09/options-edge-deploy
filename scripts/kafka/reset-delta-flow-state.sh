#!/usr/bin/env bash
# reset-delta-flow-state.sh — the Streams-state half of the delta-flow Avro cutover
# (docs/DELTA-FLOW-AVRO-CUTOVER.md; Kafka Serialization Rule, cutover order step 4c), run while delta-flow-service is scaled to 0
# and BEFORE its public topics are wiped and recreated:
#
#   1. the Streams application's consumer group must be ABSENT or EMPTY (a running member would re-commit behind us);
#   2. the group is DELETED. Its committed offsets are NOT reset to 0: delta-flow-service is deliberately a cold-start-at-the-live-end
#      service (DELTA_FLOW_STREAMS_AUTO_OFFSET_RESET=latest, see DeltaFlowSettings.streamsProperties), and a replay of the retained
#      options.opra.tcbbo from offset 0 cannot be event-time faithful: the greek surface is a global store restored to its LATEST
#      value, so every replayed trade older than the greek max age is rejected as LOOKAHEAD_GREEK/STALE_GREEK (zero lookahead
#      tolerance). With no group, tcbbo resumes at its live end and the session aggregates rebuild from the first accepted print;
#   3. the application's own INTERNAL topics are deleted by exact name, never by wildcard: the repartition topics
#      <appId>-trades-by-symbol-repartition and <appId>-quotes-by-symbol-repartition (String/String JSON today, Avro after) and the
#      eight store changelogs <appId>-<store>-changelog (JSON text today). A purge would keep a JSON tail restorable (and the Avro
#      serdes cannot restore it); deletion does not. The service recreates them (Avro) on its first start (KafkaTopics.ensureTopic /
#      Streams). Topics that do not exist (the quote path, the optional positioning store) are reported absent, not an error.
#
# Inputs: KAFKA_BOOTSTRAP_SERVERS, DELTA_FLOW_APP_ID (the EFFECTIVE Streams application id: base id + KAFKA_APPLICATION_ID_SUFFIX +
# the DeltaFlowSettings.TOPOLOGY_VERSION_SUFFIX -r2: options-edge-delta-flow-service-dev-r2 | ...-prod-r2 | ...-es4-r2; read it off the
# broker with kafka-consumer-groups --list), DEPLOY_DRY_RUN, GROUP_EMPTY_WAIT_S (default 120), KAFKA_TOPIC_DELETE_WAIT_SECONDS (default 90).
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset}"
: "${DELTA_FLOW_APP_ID:?DELTA_FLOW_APP_ID unset — the Streams application id names the group and the internal topics}"
DRY="${DEPLOY_DRY_RUN:-false}"
KCG="${KAFKA_CONSUMER_GROUPS_CMD:-kafka-consumer-groups}"
KT="${KAFKA_TOPICS_CMD:-kafka-topics}"
APP="$DELTA_FLOW_APP_ID"
case "$APP" in options-edge-delta-flow-service-dev-r2|options-edge-delta-flow-service-prod-r2|options-edge-delta-flow-service-es4-r2) ;;
  *) echo "FAIL: DELTA_FLOW_APP_ID '$APP' is not options-edge-delta-flow-service-dev-r2|-prod-r2|-es4-r2 — refusing to guess which topics to delete"; exit 1 ;; esac
TOPICS="$APP-trades-by-symbol-repartition $APP-quotes-by-symbol-repartition"
for s in latest-quote-store cumulative-delta-flow-store strike-delta-flow-store rolling-delta-flow-store delta-flow-duplicate-trade-store delta-flow-accepted-watermark-store delta-flow-trading-date-index-store positioning-store; do TOPICS="$TOPICS $APP-$s-changelog"; done
echo "=== reset-delta-flow-state: group=$APP on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="

group_state() {
  DRC=0; DESC=$("$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group "$APP" --state 2>&1) || DRC=$?
  if printf '%s' "$DESC" | grep -qiE "Consumer group '$APP' does not exist"; then
    STATE=absent
  elif [ "$DRC" -ne 0 ]; then
    STATE="describe-failed(rc=$DRC)"
  else
    STATE=$(printf '%s\n' "$DESC" | awk -v g="$APP" '$1 == g {print $(NF-1)}' | head -1)
  fi
}
WAIT_S="${GROUP_EMPTY_WAIT_S:-120}"
waited=0
group_state
while [ "$STATE" != absent ] && [ "$STATE" != Empty ] && [ "$waited" -lt "$WAIT_S" ]; do
  case "$STATE" in
    Stable|PreparingRebalance|CompletingRebalance) echo "group $APP: $STATE — waiting for the last member to leave (${waited}s/${WAIT_S}s)"; sleep 5; waited=$((waited + 5)); group_state ;;
    *) break ;;
  esac
done
case "$STATE" in
  absent|Empty) echo "group $APP: $STATE" ;;
  *) echo "FAIL: consumer group $APP is '${STATE:-unknown}' — only an absent or Empty group may be reset (scale delta-flow-service to 0 first):"; printf '%s\n' "$DESC"; exit 1 ;;
esac

# the broker's topic list, captured once with its status checked: a failed list is an UNKNOWN, never "nothing to delete"
list_topics() { LIST=$("$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>&1) || { echo "FAIL: cannot list topics on $KAFKA_BOOTSTRAP_SERVERS:"; printf '%s\n' "$LIST"; exit 1; }; }
list_topics
PRESENT=""
for t in $TOPICS; do printf '%s\n' "$LIST" | grep -qxF "$t" && PRESENT="$PRESENT $t"; done

if [ "$DRY" = true ]; then
  echo "DEPLOY_DRY_RUN=true — would delete group $APP (state $STATE) and the internal topics present:${PRESENT:- none}"
  exit 0
fi

if [ "$STATE" = Empty ]; then
  "$KCG" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --group "$APP"
  echo "group $APP deleted"
else
  echo "group $APP absent"
fi
for t in $PRESENT; do
  "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$t"
  echo "deleted $t"
done
for _ in $(seq 1 "${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"); do
  list_topics
  LEFT=""
  for t in $PRESENT; do printf '%s\n' "$LIST" | grep -qxF "$t" && LEFT="$LEFT $t"; done
  [ -z "$LEFT" ] && break
  sleep 1
done
if [ -n "${LEFT:-}" ]; then echo "FAIL: still present after delete:$LEFT"; exit 1; fi
for t in $TOPICS; do case " $PRESENT " in *" $t "*) ;; *) echo "absent $t" ;; esac; done
echo "=== reset-delta-flow-state: OK ==="
