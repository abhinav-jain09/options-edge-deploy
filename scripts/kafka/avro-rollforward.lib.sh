#!/usr/bin/env bash
# avro-rollforward.lib.sh — ONE declaration of what the 2026-10-18 Avro roll-forward covers, shared
# by the destructive script and by the rollout barrier, so the two cannot disagree about it.
#
# THE CLUSTER IS EXPLICIT, never inferred. Three brokers carry these topics and each carries a
# DIFFERENT set: dev and production carry the `options.*` names, es4 carries the `es.` twin that the
# es4 gamma-ladder-path slice reads. Inferring the set from which topics happen to exist would make
# an absent topic look like "not applicable here", which is exactly the hole that let the es4 twin be
# missed in the first place (Codex round 2). So the caller states the cluster, every topic declared
# for that cluster MUST exist and verify, and nothing else is touched.
#
# PRE-CUTOVER TOPIC IDS were read off each live broker on 2026-10-18 BEFORE the wipe. A topic id is
# assigned at creation and is a different value after any delete-and-recreate, so a live topic still
# carrying its pre-cutover id is the original JSON-bearing topic.
#
# Shapes MIRROR scripts/kafka/topics.env (and, for es4, the 12 h org retention that
# scripts/es4/create-es-topics.sh applies to every delete-cleanup topic).

# rf_specs <cluster> -> one line per topic: name|partitions|configs|pre-cutover-topic-id
# A pre-cutover id of "-" means the topic had no pre-cutover instance on that broker.
rf_specs() {
  case "$1" in
    dev)
      cat <<'SPECS'
options.ibkr.gex.status|1|cleanup.policy=compact,retention.ms=-1,delete.retention.ms=172800000,min.insync.replicas=1|yCF-rRJwQo-VVdaMHfiUkg
options.databento.gex.oi-status|32|cleanup.policy=delete,retention.ms=-1,min.insync.replicas=1|YevJMOiKSCeVwF5LHm8x1A
rates.databento.zn.options.raw|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1|-
rates.databento.zn.options.gex.strike|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1|-
SPECS
      ;;
    production)
      cat <<'SPECS'
options.ibkr.gex.status|1|cleanup.policy=compact,retention.ms=-1,delete.retention.ms=172800000,min.insync.replicas=1|5OTMCxITTtWRiljc0z-kxA
options.databento.gex.oi-status|32|cleanup.policy=delete,retention.ms=-1,min.insync.replicas=1|75HJ4F03QFy7763nzdSYPA
rates.databento.zn.options.raw|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1|-
rates.databento.zn.options.gex.strike|1|cleanup.policy=delete,retention.ms=86400000,min.insync.replicas=1|-
SPECS
      ;;
    es4)
      # 12 h retention, not -1: es4 applies the org delete-cleanup default to every such topic
      # (scripts/es4/create-es-topics.sh RETENTION_MS), and topics.env declares no es4 override here.
      cat <<'SPECS'
es.options.databento.gex.oi-status|32|cleanup.policy=delete,retention.ms=43200000,min.insync.replicas=1|Etuw6A2-RZiWnbq4oPLHLw
SPECS
      ;;
    *) echo "FATAL: unknown cluster '$1' (expected dev, production or es4)" >&2; return 1 ;;
  esac
}

# rf_siblings <cluster> -> the versioned topic names to retire on that broker, one per line.
# Listed per cluster for the same reason as the specs: the es4 twin is a different name.
rf_siblings() {
  case "$1" in
    dev|production)
      printf '%s\n' options.ibkr.gex.status.v2 options.databento.gex.oi-status.v2 \
                    rates.databento.zn.options.raw.v1 rates.databento.zn.options.gex.strike.v1
      ;;
    es4)
      printf '%s\n' es.options.databento.gex.oi-status.v2
      ;;
    *) echo "FATAL: unknown cluster '$1' (expected dev, production or es4)" >&2; return 1 ;;
  esac
}

# rf_resolve_cli [bin-dir] -> echoes the kafka-topics command to use, or fails.
# The deploy images put the CLI on PATH without the .sh suffix; a developer Mac has a bin dir.
rf_resolve_cli() {
  local bin="${1:-${KAFKA_BIN_DIR:-}}"
  if [ -n "$bin" ]; then
    if [ ! -x "$bin/kafka-topics.sh" ]; then
      echo "FATAL: $bin/kafka-topics.sh is not executable" >&2; return 1
    fi
    printf '%s\n' "$bin/kafka-topics.sh"
  elif command -v kafka-topics >/dev/null 2>&1; then
    printf '%s\n' kafka-topics
  elif command -v kafka-topics.sh >/dev/null 2>&1; then
    printf '%s\n' kafka-topics.sh
  else
    echo "FATAL: no kafka-topics CLI found (pass a bin dir or set KAFKA_BIN_DIR)" >&2; return 1
  fi
}

# rf_list <cli> <bootstrap> -> the topic list on stdout, NON-ZERO if the broker could not be listed.
#
# Callers must capture this into a variable and check the status, never pipe it into a conditional.
# `if kt --list | grep -q x` suppresses errexit, so a broker failure there is indistinguishable from
# an absent topic and a verification can "pass" on an unreachable cluster (Codex round 2).
rf_list() {
  local cli="$1" bs="$2" out
  if ! out="$("$cli" --bootstrap-server "$bs" --list 2>&1)"; then
    echo "FATAL: cannot list topics on $bs — refusing to act on an unverified cluster:" >&2
    printf '%s\n' "$out" >&2
    return 1
  fi
  printf '%s\n' "$out"
}

# rf_topic_id <cli> <bootstrap> <topic> -> the topic id on stdout, NON-ZERO if it cannot be read.
rf_topic_id() {
  local cli="$1" bs="$2" topic="$3" desc id
  if ! desc="$("$cli" --bootstrap-server "$bs" --describe --topic "$topic" 2>&1 | head -1)"; then
    echo "FATAL: cannot describe $topic on $bs" >&2; return 1
  fi
  id="$(printf '%s\n' "$desc" | awk -F'\t' '{for(i=1;i<=NF;i++) if ($i ~ /^TopicId: /) {sub(/^TopicId: /,"",$i); print $i}}')"
  if [ -z "$id" ]; then
    echo "FATAL: could not parse a topic id for $topic out of: $desc" >&2; return 1
  fi
  printf '%s\n' "$id"
}
