#!/usr/bin/env bash
# zn-gex's KAFKA_APPLICATION_ID was renamed from `zn-gex-v1` to `zn-gex` (the One Service One Identity Rule).
#
# WHY THIS SCRIPT EXISTS. Renaming a Kafka Streams application id abandons the consumer group AND the internal
# topics that hold the state stores and repartitions: a service that has ever run under the old id loses its
# state and re-reads from its reset policy. The rename was safe when it was made because zn-gex had never been
# built (no image in either registry, no workload on any cluster, no consumer group on the broker) — but that
# was an OBSERVATION, and a commit message saying "land this before the first build" is not enforcement.
#
# So the condition is enforced here, before every zn-gex rollout: if the broker still carries ANYTHING under
# the old identity, this refuses and names it. The operator then has a real decision to make — migrate the
# state or accept its loss — instead of discovering it from an empty store.
#
# It is deliberately NOT a one-shot: a deploy from an older commit, a rollback, or a second environment could
# each create the old identity again, and a check that only runs once would not see it.
set -euo pipefail

LEGACY_ID="${ZN_GEX_LEGACY_APPLICATION_ID:-zn-gex-v1}"
CURRENT_ID="${ZN_GEX_APPLICATION_ID:-zn-gex}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required (load-kafka-settings.sh sets it)}"

command -v kafka-consumer-groups >/dev/null 2>&1 || { echo "zn-gex-preflight: kafka-consumer-groups is not on PATH" >&2; exit 2; }
command -v kafka-topics >/dev/null 2>&1 || { echo "zn-gex-preflight: kafka-topics is not on PATH" >&2; exit 2; }

echo "zn-gex-preflight: the application id is '$CURRENT_ID'; refusing if '$LEGACY_ID' still exists on $BOOTSTRAP"

# (1) the consumer GROUP. A Streams application's group id IS its application id.
groups="$(kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null || true)"
legacy_group="$(printf '%s\n' "$groups" | grep -Fx -- "$LEGACY_ID" || true)"

# (2) the INTERNAL topics. Streams prefixes every changelog and repartition topic with the application id, so
#     these outlive the group and are the durable half of the state.
topics="$(kafka-topics --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null || true)"
legacy_topics="$(printf '%s\n' "$topics" | grep -E "^${LEGACY_ID}-" || true)"

if [ -z "$legacy_group" ] && [ -z "$legacy_topics" ]; then
  echo "zn-gex-preflight: OK — no consumer group and no internal topic under '$LEGACY_ID'; the rename abandons nothing"
  exit 0
fi

echo "zn-gex-preflight: REFUSED — the broker still carries the OLD identity '$LEGACY_ID'." >&2
[ -n "$legacy_group" ] && echo "    consumer group : $legacy_group" >&2
if [ -n "$legacy_topics" ]; then
  echo "    internal topics:" >&2
  printf '%s\n' "$legacy_topics" | sed 's/^/      /' >&2
fi
echo "    Deploying '$CURRENT_ID' over this abandons that group and those state stores." >&2
echo "    Decide deliberately: migrate the state, or delete the old identity and accept the reset." >&2
exit 1
