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

# rf_describe <cli> <bootstrap> <topic> -> the FIRST line of --describe, NON-ZERO if it fails.
#
# NO PIPE around the CLI. `cli --describe | head -1` looks equivalent and is not: under `pipefail`,
# head closing the pipe SIGPIPEs the producer and the pipeline reports failure. That is invisible on
# a short output and fatal through the es4 kafka-cli-shim, which `exec`s `docker exec` and writes a
# line per partition — 32 of them for oi-status. Found by running this against the real shim rather
# than assuming the two were the same. So: capture it all, THEN take the first line.
rf_describe() {
  local cli="$1" bs="$2" topic="$3" raw
  if ! raw="$("$cli" --bootstrap-server "$bs" --describe --topic "$topic" 2>&1)"; then
    echo "FATAL: cannot describe $topic on $bs:" >&2
    printf '%s\n' "$raw" >&2
    return 1
  fi
  printf '%s\n' "$raw" | sed -n '1p'
}

# rf_topic_id <describe-first-line> <topic> -> the topic id on stdout, NON-ZERO if absent.
rf_topic_id() {
  local desc="$1" topic="$2" id
  id="$(printf '%s\n' "$desc" | awk -F'\t' '{for(i=1;i<=NF;i++) if ($i ~ /^TopicId: /) {sub(/^TopicId: /,"",$i); print $i}}')"
  if [ -z "$id" ]; then
    echo "FATAL: could not parse a topic id for $topic out of: $desc" >&2; return 1
  fi
  printf '%s\n' "$id"
}

# rf_split_declared <declared-configs> -> one `key=value` per line.
#
# The declared string is comma-separated AND a value may itself contain a comma
# (cleanup.policy=compact,delete). A field with no `=` is therefore a CONTINUATION of the previous
# value, not a new key — splitting naively turns `compact,delete` into a key called `delete`.
rf_split_declared() {
  printf '%s' "$1" | awk 'BEGIN{RS=","} {
    if (index($0,"=")) { if (NR>1) printf "\n"; printf "%s", $0 }
    else { printf ",%s", $0 }
  } END{ printf "\n" }'
}

# rf_config_value <describe-first-line> <key> -> that key's value out of the `Configs:` field.
#
# Anchored on a start-or-comma boundary so `retention.ms` never reads `delete.retention.ms`, and the
# value runs to the next `,<key>=` or end of field — so `cleanup.policy` yields `compact,delete`
# whole. Empty when the key is absent, which is a legitimate answer the caller compares against.
rf_config_value() {
  printf '%s' "$1" | awk -v key="$2" '
    {
      i = index($0, "Configs: ")
      if (i == 0) exit
      cfg = substr($0, i + 9)
      sub(/[\t\r].*$/, "", cfg)
      n = split(cfg, f, ",")
      out = ""; have = 0
      for (j = 1; j <= n; j++) {
        eq = index(f[j], "=")
        if (eq > 0) {
          k = substr(f[j], 1, eq - 1)
          if (have) break
          if (k == key) { have = 1; out = substr(f[j], eq + 1) }
        } else if (have) {
          out = out "," f[j]
        }
      }
      if (have) print out
    }'
}

# rf_resolve_cli_named <base-name> [bin-dir] -> the command to run for that Kafka CLI.
# Same resolution as rf_resolve_cli, for the other tools the barrier needs.
rf_resolve_cli_named() {
  local base="$1" bin="${2:-${KAFKA_BIN_DIR:-}}"
  if [ -n "$bin" ]; then
    if [ ! -x "$bin/$base.sh" ]; then
      echo "FATAL: $bin/$base.sh is not executable" >&2; return 1
    fi
    printf '%s\n' "$bin/$base.sh"
  elif command -v "$base" >/dev/null 2>&1; then
    printf '%s\n' "$base"
  elif command -v "$base.sh" >/dev/null 2>&1; then
    printf '%s\n' "$base.sh"
  else
    echo "FATAL: no $base CLI found (pass a bin dir or set KAFKA_BIN_DIR)" >&2; return 1
  fi
}

# rf_nonempty_partitions <get-offsets-cli> <bootstrap> <topic> -> partition numbers that hold records.
#
# Derived from earliest vs latest offsets rather than by polling every partition: a 32-partition topic
# would otherwise cost 32 consumer timeouts to discover that 31 of them are empty.
rf_nonempty_partitions() {
  local cli="$1" bs="$2" topic="$3" early late
  if ! early="$("$cli" --bootstrap-server "$bs" --topic "$topic" --time -2 2>&1)"; then
    echo "FATAL: cannot read earliest offsets for $topic on $bs:" >&2; printf '%s\n' "$early" >&2; return 1
  fi
  if ! late="$("$cli" --bootstrap-server "$bs" --topic "$topic" --time -1 2>&1)"; then
    echo "FATAL: cannot read latest offsets for $topic on $bs:" >&2; printf '%s\n' "$late" >&2; return 1
  fi
  # A successful-but-PARTIAL response must not read as authoritative: a snapshot missing a partition
  # would silently omit a contaminated one and read as a clean pass (Codex). Both snapshots must name
  # every partition from 0..expected-1 exactly once.
  local expected="${4:-}"
  if [ -n "$expected" ]; then
    local n_early n_late
    n_early="$(printf '%s\n' "$early" | awk -F: -v t="$topic" '$1==t{c++} END{print c+0}')"
    n_late="$(printf '%s\n' "$late" | awk -F: -v t="$topic" '$1==t{c++} END{print c+0}')"
    if [ "$n_early" != "$expected" ] || [ "$n_late" != "$expected" ]; then
      echo "FATAL: offsets for $topic are PARTIAL — earliest named $n_early partition(s), latest" >&2
      echo "       named $n_late, expected $expected. Refusing rather than treating a partial" >&2
      echo "       response as authoritative." >&2
      return 1
    fi
  fi
  printf '%s\n%s\n' "$early" "$late" | awk -F: -v t="$topic" '
    $1==t { if (seen[$2]++) { if ($3+0 > first[$2]+0) print $2 ":" first[$2] ":" $3 } else { first[$2]=$3 } }'
}

# rf_wire_is_avro <console-consumer-cli> <bootstrap> <topic> <partition> <from-offset>
#   -> 0 when the record at that offset is Confluent-framed Avro OR a tombstone,
#      1 when it is something else, 2 when nothing could be read at that offset.
#
# WHY THIS EXISTS, and why the topic-id check is not enough. A new topic id proves the ORIGINAL
# JSON-bearing topic is gone. It does NOT prove nothing has written JSON SINCE. On 2026-10-18 that
# gap was real and self-inflicted: the dev topic was recreated at 15:10 and the still-running
# PRE-CUTOVER image then wrote 31 hand-built JSON heartbeats into it before the new image rolled at
# 15:59. The topic id was post-wipe the whole time, so the barrier passed while the topic held
# exactly the retained JSON the Kafka Serialization Rule forbids an Avro consumer to meet.
#
# A Confluent-framed Avro value begins with the magic byte 0x00 followed by a 4-byte schema id; the
# hand-built JSON this estate is retiring begins with 0x7b, "{".
#
# A TOMBSTONE PASSES. A null value is a valid record, not a malformed one, and
# options.ibkr.gex.status is pure compact with delete.retention.ms=172800000 — it RETAINS tombstones
# for 48 h by design, so an earliest tombstone is reachable and refusing it would block a correct
# topic (Codex). kafka-console-consumer renders a null value as its null literal, so that literal is
# accepted explicitly rather than by accident.
#
# WHAT THIS DOES NOT PROVE, stated because the first cut of this comment overclaimed it: sampling the
# first and last records of a partition is not a full scan, so it does not prove that NO retained
# JSON exists anywhere in the log. A full scan is not available through this CLI — records are
# newline-delimited in its output while Avro bodies contain newlines, so a line-based scan over
# binary values is unsound, not merely slow. What rules out a non-Avro writer is the CODE: the
# producer serializer refuses a non-Avro payload (IbkrMixedSerializer) and the contract refuses a
# versioned topic name (KafkaTopicNames). This check is the operational backstop for the one failure
# those cannot see — a stale image still running at wipe time — and the two ends of the log are where
# that shows up: the start if it was writing when the topic was recreated, the end if it still is.
rf_wire_is_avro() {
  local cli="$1" bs="$2" topic="$3" part="$4" off="$5" tmp first
  tmp="$(mktemp)"
  # < /dev/null is REQUIRED, not hygiene: kafka-console-consumer reads stdin, and without this it
  # swallows the caller's `while read ... <<<` herestring — the outer topic loop then silently
  # processes one topic and stops, which looks exactly like a clean pass.
  if ! "$cli" --bootstrap-server "$bs" --topic "$topic" --partition "$part" \
        --offset "$off" --max-messages 1 --timeout-ms "${RF_WIRE_TIMEOUT_MS:-15000}" \
        > "$tmp" 2>/dev/null < /dev/null; then
    # A compacted-away offset reads as empty rather than as a failure; treat unreadable as unknown.
    if [ ! -s "$tmp" ]; then rm -f "$tmp"; return 2; fi
  fi
  if [ ! -s "$tmp" ]; then rm -f "$tmp"; return 2; fi
  first="$(od -An -tx1 -N1 "$tmp" | tr -d ' \n')"
  ids="$(od -An -tx1 -j1 -N4 "$tmp" | tr -d ' \n')"
  # A tombstone: the consumer renders a null value as its null literal on a line of its own.
  if [ "$(tr -d '\n\r' < "$tmp")" = "${RF_NULL_LITERAL:-null}" ]; then
    rm -f "$tmp"; return 0
  fi
  rm -f "$tmp"
  if [ "$first" = "00" ]; then
    # Optional exact-id gate (a topic with ONE fixed contract): the four bytes after the magic must be the expected schema id.
    if [ -n "${RF_EXPECT_ID_HEX:-}" ] && [ "$ids" != "$RF_EXPECT_ID_HEX" ]; then
      echo "    value at offset $off is Avro under schema id 0x$ids, expected 0x$RF_EXPECT_ID_HEX (a foreign writer)" >&2
      return 1
    fi
    return 0
  fi
  echo "    value at offset $off starts 0x$first (Avro is 0x00, a tombstone renders as" >&2
  echo "    '${RF_NULL_LITERAL:-null}', hand-built JSON is 0x7b '{')" >&2
  return 1
}

# rf_wire_is_avro_at_log_end <cli> <bs> <topic> <partition> <latest-offset> <earliest-offset>
#   -> 0 / 1 / 2 with the same meaning as rf_wire_is_avro, but for the last DELIVERED record of a
#      partition rather than for one fixed offset.
#
# WHY A PLAIN READ AT high-watermark-1 IS NOT THAT RECORD. On a topic written under exactly-once
# semantics the offset below the high watermark is very often a transaction CONTROL record — a
# commit or abort marker. A marker occupies an offset in the log but is delivered to a consumer in
# NEITHER isolation level, so a read at that offset returns nothing and rf_wire_is_avro reports 2,
# "nothing could be read". The barrier then failed the topic for being unverifiable.
#
# That is not hypothetical: it blocked the 19 Oct production gamma rollout twice. Partition 0 of
# options.ibkr.gex.status ended on a commit marker (offset 319 on the first attempt, 657 on the
# second), the gate printed "could not be read at its latest offset", every later stage including
# Deploy was skipped — on a topic whose every value was correct Avro. A gate that refuses a correct
# topic is not strictness, it is an outage with a reassuring message.
#
# So walk BACK from the end over offsets that deliver nothing, and judge the first record that does
# arrive. Each step is its own single-record read, so this still never scans binary values
# line-by-line — that remains unsound, because Avro bodies contain newlines.
#
# WHAT STEPPING BACK DOES NOT WEAKEN: the walk stops at the FIRST offset that delivers, so a
# contaminated record at the log end is still met and still fails. Nothing is skipped over except
# offsets that deliver no record at all, which carry no wire format to check. What it does change is
# that the record judged may sit a few offsets below the high watermark — and when every offset
# above it is a marker, that record IS the most recent record the partition has.
#
# The window is bounded rather than open: consecutive transactions each leave their own marker, so
# the bound cannot be 1, but a partition whose last RF_LOG_END_PROBE_BACK offsets ALL deliver
# nothing is still refused, not assumed Avro. The walk also never crosses below the partition's
# earliest offset, so a short partition cannot be probed into negative offsets.
rf_wire_is_avro_at_log_end() {
  local cli="$1" bs="$2" topic="$3" part="$4" latest="$5" earliest="$6"
  local floor off rc
  floor=$((latest - ${RF_LOG_END_PROBE_BACK:-8}))
  [ "$floor" -lt "$earliest" ] && floor="$earliest"
  off="$latest"
  while [ "$off" -ge "$floor" ]; do
    rc=0
    rf_wire_is_avro "$cli" "$bs" "$topic" "$part" "$off" || rc=$?
    if [ "$rc" -ne 2 ]; then
      if [ "$off" != "$latest" ]; then
        echo "    (last delivered record is at offset $off; offsets $((off + 1))-$latest deliver" >&2
        echo "     nothing, which is what a transaction commit/abort marker reads as)" >&2
      fi
      return "$rc"
    fi
    off=$((off - 1))
  done
  return 2
}
