#!/usr/bin/env bash
# reset-strike-invasion-state.sh — the Streams-state half of the strike-invasion Avro cutover
# (docs/STRIKE-INVASION-AVRO-CUTOVER.md; Kafka Serialization Rule, cutover order step 4c), run while
# strike-invasion-service is scaled to 0 and BEFORE its public topics are wiped and recreated:
#
#   1. the Streams application's consumer group must be ABSENT or EMPTY (a running member would re-commit behind us);
#   2. the group is DELETED. Its committed offsets are NOT reset to 0 (unlike reset-indicator-state.sh): the service's
#      stores are evidence caches excluded after STRIKE_INVASION_MAX_SOURCE_AGE_MS (120 s) and the manifest runs
#      auto.offset.reset=latest, so replaying the six inputs from offset 0 would re-emit days of historic invasion
#      events into the fresh events topic. With no group, every source resumes at its live end and the stores rebuild
#      within the age bound;
#   3. the application's own INTERNAL topics are deleted by exact name, never by wildcard: the seven repartition topics
#      <appId>-repart-{spot,sr,gex,flow,behavior,pace,wall} (String/String JSON today) and the five store changelogs
#      <appId>-{sr,gex,flow,behavior,pace}-store-changelog. A purge would keep a JSON tail restorable; deletion does not.
#      The service recreates them (Avro) on its first start (KafkaTopics.ensureTopic / Streams).
#
# Inputs: KAFKA_BOOTSTRAP_SERVERS, STRIKE_INVASION_APP_ID (options-edge-strike-invasion-service-dev | -prod),
# DEPLOY_DRY_RUN, GROUP_EMPTY_WAIT_S (default 120), KAFKA_TOPIC_DELETE_WAIT_SECONDS (default 90).
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset}"
: "${STRIKE_INVASION_APP_ID:?STRIKE_INVASION_APP_ID unset — the Streams application id names the group and the internal topics}"
DRY="${DEPLOY_DRY_RUN:-false}"
KCG="${KAFKA_CONSUMER_GROUPS_CMD:-kafka-consumer-groups}"
KT="${KAFKA_TOPICS_CMD:-kafka-topics}"
APP="$STRIKE_INVASION_APP_ID"
case "$APP" in options-edge-strike-invasion-service-dev|options-edge-strike-invasion-service-prod) ;;
  *) echo "FAIL: STRIKE_INVASION_APP_ID '$APP' is not options-edge-strike-invasion-service-dev|-prod — refusing to guess which topics to delete"; exit 1 ;; esac
TOPICS=""
for s in spot sr gex flow behavior pace wall; do TOPICS="$TOPICS $APP-repart-$s"; done
for s in sr gex flow behavior pace; do TOPICS="$TOPICS $APP-$s-store-changelog"; done
echo "=== reset-strike-invasion-state: group=$APP on $KAFKA_BOOTSTRAP_SERVERS (dry_run=$DRY) ==="

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
  *) echo "FAIL: consumer group $APP is '${STATE:-unknown}' — only an absent or Empty group may be reset (scale strike-invasion to 0 first):"; printf '%s\n' "$DESC"; exit 1 ;;
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
echo "=== reset-strike-invasion-state: OK ==="
