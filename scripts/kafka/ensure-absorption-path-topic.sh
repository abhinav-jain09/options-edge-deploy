#!/usr/bin/env bash
# First-deploy barrier for rebuildable, compacted SPX path snapshots. Jenkins only.
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?required}"
path_topic=futures.level-absorption.path
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"
listing=$(kt --list)
if ! grep -qxF "$path_topic" <<<"$listing"; then
  kt --create --topic "$path_topic" --partitions 1 --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-1}" \
    --config cleanup.policy=compact --config retention.ms=-1 --config retention.bytes=-1 \
    --config message.timestamp.before.max.ms=9223372036854775807
fi
description=$(kt --describe --topic "$path_topic")
if ! grep -Eq 'PartitionCount:[[:space:]]*1([[:space:]]|$)' <<<"$description"; then
  echo 'FAIL: absorption path topic requires exactly one partition'; exit 1
fi
configuration=$(kc --describe --all --entity-type topics --entity-name "$path_topic")
for pair in cleanup.policy=compact retention.ms=-1 retention.bytes=-1; do
  key=${pair%%=*}; wanted=${pair#*=}; got=$(extract "$key" "$configuration")
  if [ "$got" != "$wanted" ]; then echo "FAIL: $path_topic $key=$got, expected $wanted"; exit 1; fi
done
echo "ok: $path_topic compact latest state, one partition"
