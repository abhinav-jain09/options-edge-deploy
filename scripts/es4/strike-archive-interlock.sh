#!/usr/bin/env bash
# strike-archive-interlock.sh — may es4's Kafka data dir be wiped without losing unarchived strike history?
# SOURCED by cleanup-es4.sh (and by its test); defines strike_archive_interlock, strike_archive_broker_readable
# and their private helpers (_sai_*), and runs nothing.
#
# WHY (deploy Codex final review, finding 2): es.futures.footprint.strike carries retention.ms=-1 and
# retention.bytes=-1, which was offered as proof that a range the nightly archive skipped could always
# be recaptured from the source. It cannot: cleanup-es4.sh deletes the WHOLE Kafka data directory, and
# after that the same offsets name a different log. Source preservation therefore depends on the
# archive having completed first, and this is the check that makes the wipe wait for it.
#
# HOW: the archiver (scripts/ops/archive/oe-archive-kafka.sh, ENV=es4) commits, after every durable
# strike capture and checkpoint, the captured boundary as the offset of consumer group
# oe-archive-committed-boundary on THIS broker — the only archive state readable from the es4 box, which
# cannot see the NAS. The wipe is safe iff, for every partition of every protected topic, that
# committed offset has reached the log end. Every fact is READ, never inferred from silence:
#   1. presence    kafka-topics --list, read successfully (exit 0, no diagnostic, every line a topic
#                  name). A topic NOT in that list is CONFIRMED absent and has nothing to protect.
#   2. partitions  kafka-topics --describe: exactly one PartitionCount N, and partition lines 0..N-1.
#   3. log ends    kafka-get-offsets: exit 0, NO diagnostic on stderr, every line "topic:p:offset", and
#                  exactly the partitions 0..N-1 from step 2 — each once.
#   4. marker      kafka-consumer-groups --describe --group oe-archive-committed-boundary.
# Anything else refuses:
#   * the group is absent, or has no offset for a non-empty partition  -> nothing is known archived
#   * committed < log end                                            -> (end - committed) offsets unarchived
#   * committed > log end                                            -> a marker from a re-created topic
#   * any of 1-4 cannot be READ, or reads short                       -> unknown is not safe (fail closed)
#
# WHY STEPS 1-3 ARE SEPARATE (re-review round 2, findings 1 and 3). GetOffsetShell — the same
# fetchOffsets code in Kafka 3.7 (cp-kafka 7.7.1, the es4 container this runs in through kafka-cli-shim/)
# and in 4.3 (the prod host) — has two answers that look like "nothing here" and are not:
#   * a partition whose offset lookup fails is printed to stderr as "Skip getting offsets for
#     topic-partition <t>-<p> due to error: ...", OMITTED from stdout, and the command still exits 0.
#     Parsing stdout alone read that as "absent" (one partition: the whole wipe was authorised) or left
#     the failed partition unchecked (several). Hence: diagnostics refuse, and the partition set must
#     equal the one the topic's own metadata declares.
#   * a topic that does not exist is "Error occurred: Could not match any topic-partitions with the
#     specified filters" at exit 1 — indistinguishable by exit status from an unreachable broker. So
#     absence is never concluded from kafka-get-offsets; only from a successfully read topic list.
# Both are recorded from the real CLIs (4.3.0 and 3.9.0) in
# scripts/ops/archive/broker-test/cli-fixtures/, which strike-archive-interlock-test.sh replays.
#
# The comparison uses the high-water mark (kafka-get-offsets' default), not the stable offset: records
# of a transaction that is still open are not archived yet, so they must hold the wipe too. After the
# producers are quiesced such a transaction is aborted by its timeout; one more archive run then walks
# the boundary over it and records the marker.
#
# OPT-OUT, deliberate only: ES4_STRIKE_ARCHIVE_INTERLOCK=off (Jenkins es4-deploy
# ACCEPT_UNARCHIVED_STRIKE_LOSS=true). The wipe then proceeds and this says, loudly, what it discards.
#
# Knobs: ES4_STRIKE_ARCHIVE_INTERLOCK on|off (default on); ES4_STRIKE_ARCHIVE_TOPICS (default
# es.futures.footprint.strike); ES4_STRIKE_ARCHIVE_GROUP (default oe-archive-committed-boundary — a
# contract with OE_ARCHIVE_MARK_GROUP in oe-archive-kafka.sh); ES4_STRIKE_ARCHIVE_BOOTSTRAP (default the
# in-container listener create-es-topics.sh uses). DEPLOY_DRY_RUN=true reports the verdict and never
# blocks. Uses kafka-topics, kafka-get-offsets and kafka-consumer-groups from PATH (on the box:
# kafka-cli-shim/).
#
# Returns 0 = the wipe may proceed (proved, or explicitly accepted), 1 = REFUSE.

# What the Kafka CLIs print when a read did not (fully) succeed: GetOffsetShell's per-partition "Skip
# getting offsets ... due to error", the tools' "Error occurred:" / "Error while executing topic command",
# the AdminClient's "Connection to node -1 ... could not be established" while it retries, and every
# exception name. Matched case-insensitively against STDERR; stdout is held to an exact shape instead.
_SAI_DIAG='skip|error|exception|fail|timed out|timeout|could not|unable|not available|refused'

# _sai_why <rc> <file-prefix> <stdout-shape-regex or ''> -> prints why the answer cannot be used, or nothing
_sai_why() {
  local rc="$1" f="$2" shape="$3" diag bad
  diag=$(grep -m1 -iE "$_SAI_DIAG" "$f.err" 2>/dev/null | cut -c1-240)
  if [ "$rc" -ne 0 ]; then
    bad=$(grep -m1 -iE 'error' "$f.out" 2>/dev/null | cut -c1-240)
    printf 'exit %s: %s' "$rc" "${bad:-${diag:-$(tail -1 "$f.err" 2>/dev/null | cut -c1-240)}}"
    return
  fi
  if [ -n "$diag" ]; then printf 'exit 0 but it reported: %s' "$diag"; return; fi
  bad=$(grep -m1 -E '^Error' "$f.out" 2>/dev/null | cut -c1-240)
  if [ -n "$bad" ]; then printf 'exit 0 but it printed: %s' "$bad"; return; fi
  if [ -n "$shape" ]; then
    bad=$(grep -v -E "$shape" "$f.out" 2>/dev/null | grep -m1 . | cut -c1-240)
    [ -z "$bad" ] || printf 'unexpected output line: %s' "$bad"
  fi
}
_sai_seq() { local i=0; while [ "$i" -lt "$1" ]; do printf '%s ' "$i"; i=$((i + 1)); done; }

strike_archive_interlock() {   # $1 = where in the reset this check runs (for the log)
  local where="${1:-}" mode topics group bootstrap dry topic d rc why n parts ends got groups groups_rc
  local part end committed refuse=0 reasons="" accepted=""
  mode="${ES4_STRIKE_ARCHIVE_INTERLOCK:-on}"
  topics="${ES4_STRIKE_ARCHIVE_TOPICS:-es.futures.footprint.strike}"
  group="${ES4_STRIKE_ARCHIVE_GROUP:-oe-archive-committed-boundary}"
  bootstrap="${ES4_STRIKE_ARCHIVE_BOOTSTRAP:-localhost:29092}"
  dry="${DEPLOY_DRY_RUN:-false}"
  case "$mode" in
    on|off) ;;
    *) echo "STRIKE ARCHIVE INTERLOCK: ES4_STRIKE_ARCHIVE_INTERLOCK must be 'on' or 'off', got '$mode' — refusing" >&2
       return 1 ;;
  esac
  d=$(mktemp -d "${TMPDIR:-/tmp}/strike-interlock.XXXXXX") \
    || { echo "STRIKE ARCHIVE INTERLOCK: cannot create a scratch directory — refusing" >&2; return 1; }

  # 1. PRESENCE — from the broker's topic list, read successfully, or nothing at all is concluded.
  kafka-topics --bootstrap-server "$bootstrap" --list > "$d/list.out" 2> "$d/list.err"; rc=$?
  why=$(_sai_why "$rc" "$d/list" '^[A-Za-z0-9._-]+$')
  if [ -n "$why" ]; then
    refuse=1
    for topic in $topics; do accepted="$accepted $topic:presence-unknown"; done
    reasons="$reasons
  the broker's topic list cannot be read ($why) — whether any protected topic exists is unknown"
    topics=""
  fi

  for topic in $topics; do
    if ! grep -Fxq -- "$topic" "$d/list.out"; then
      echo "  strike archive interlock: $topic is not in the broker's topic list (read successfully) — confirmed absent, nothing to protect"
      continue
    fi

    # 2. PARTITIONS — from the topic's own metadata, independently of the offsets it is checked against.
    kafka-topics --bootstrap-server "$bootstrap" --describe --topic "$topic" > "$d/desc.out" 2> "$d/desc.err"; rc=$?
    why=$(_sai_why "$rc" "$d/desc" '')
    n=""
    if [ -z "$why" ]; then
      # "Topic: T<TAB>TopicId: X<TAB>PartitionCount: N<TAB>..." then "<TAB>Topic: T<TAB>Partition: p<TAB>...".
      # --describe --topic is a regex in the CLI, so lines of any other topic it matches are ignored.
      n=$(awk -F'\t' -v t="Topic: $topic" '$1 == t { for (i = 2; i <= NF; i++) if ($i ~ /^PartitionCount: [0-9]+$/) { sub(/^PartitionCount: /, "", $i); print $i } }' "$d/desc.out")
      parts=$(awk -F'\t' -v t="Topic: $topic" '$1 == "" && $2 == t && $3 ~ /^Partition: [0-9]+$/ { sub(/^Partition: /, "", $3); print $3 }' "$d/desc.out" | sort -n | tr '\n' ' ')
      case "$n" in
        ''|*[!0-9]*|0) why="no single PartitionCount for $topic in its description" ;;
        *) [ "$parts" = "$(_sai_seq "$n")" ] || why="PartitionCount $n, but partition lines [${parts% }]" ;;
      esac
    fi
    if [ -n "$why" ]; then
      refuse=1; accepted="$accepted $topic:partitions-unknown"
      reasons="$reasons
  $topic: its partitions cannot be established (kafka-topics --describe: $why) — an unreadable source is not a proven-archived one"
      continue
    fi

    # 3. LOG ENDS — every partition from step 2, each exactly once, with nothing reported on the side.
    kafka-get-offsets --bootstrap-server "$bootstrap" --topic "$topic" > "$d/ends.out" 2> "$d/ends.err"; rc=$?
    why=$(_sai_why "$rc" "$d/ends" '^[^:]+:[0-9]+:[0-9]+$')
    ends=""
    if [ -z "$why" ]; then
      ends=$(awk -F: -v t="$topic" '$1 == t { print $2 " " $3 }' "$d/ends.out" | sort -n)
      got=$(printf '%s\n' "$ends" | awk 'NF { printf "%s ", $1 }')
      [ "$got" = "$(_sai_seq "$n")" ] \
        || why="log ends came back for partitions [${got% }] of the $n the topic has — a partition that is not read is not checked"
    fi
    if [ -n "$why" ]; then
      refuse=1; accepted="$accepted $topic:ends-unknown"
      reasons="$reasons
  $topic: the log end cannot be read (kafka-get-offsets: $why) — an unreadable source is not a proven-archived one"
      continue
    fi

    # 4. THE MARKER. An ABSENT group is a state, not an error: every non-empty partition then refuses
    # below. Kafka 3.x says "Consumer group 'G' does not exist."; 4.x prints GroupIdNotFoundException and
    # exits 0. Any OTHER error text is unreadable-state even at exit 0 (4.x reports every describe
    # failure that way — an unreachable broker included, recorded in cli-fixtures/), and unreadable is
    # never read as "archived".
    groups=$(kafka-consumer-groups --bootstrap-server "$bootstrap" --describe --group "$group" 2>&1)
    groups_rc=$?
    case "$groups" in
      *"does not exist"*|*GroupIdNotFoundException*) groups="" ;;
      *"Error:"*|*Exception*)
        refuse=1; accepted="$accepted $topic:marker-unknown"
        reasons="$reasons
  $topic: the archive marker group '$group' cannot be read (kafka-consumer-groups rc=$groups_rc: $(printf '%s' "$groups" | grep -m1 -E 'Error|Exception' | cut -c1-240))"
        continue ;;
      *) if [ "$groups_rc" -ne 0 ]; then
           refuse=1; accepted="$accepted $topic:marker-unknown"
           reasons="$reasons
  $topic: the archive marker group '$group' cannot be read (kafka-consumer-groups rc=$groups_rc: $(printf '%s' "$groups" | tail -1 | cut -c1-240))"
           continue
         fi ;;
    esac
    while read -r part end; do
      [ -n "$part" ] || continue
      if [ "$end" -eq 0 ]; then
        echo "  strike archive interlock: $topic p$part is empty (log end 0) — nothing to protect"
        continue
      fi
      committed=$(printf '%s\n' "$groups" | awk -v t="$topic" -v p="$part" '$2 == t && $3 == p { print $4; exit }')
      case "$committed" in
        ''|*[!0-9]*)
          refuse=1; accepted="$accepted $topic:$part:0-$end"
          reasons="$reasons
  $topic p$part: log end $end, but NO archive marker (group '$group' has no committed offset) — nothing is known archived" ;;
        *)
          if [ "$committed" -lt "$end" ]; then
            refuse=1; accepted="$accepted $topic:$part:$committed-$end"
            reasons="$reasons
  $topic p$part: archived through $committed, log end $end — $((end - committed)) offset(s) not archived"
          elif [ "$committed" -gt "$end" ]; then
            # A marker can never pass the log it was taken from (the archived boundary is at most the
            # high-water mark), so this is a marker from an EARLIER incarnation of a re-created topic:
            # it says nothing about the records the current log holds.
            refuse=1; accepted="$accepted $topic:$part:0-$end"
            reasons="$reasons
  $topic p$part: the marker ($committed) is AHEAD of the log end $end — the topic was re-created since it was written; none of its $end offset(s) is known archived"
          else
            echo "  strike archive interlock: $topic p$part archived through $committed, log end $end — covered"
          fi ;;
      esac
    done <<< "$ends"
  done
  rm -rf "$d"

  if [ "$refuse" -eq 0 ]; then
    echo "  strike archive interlock ($where): every protected record is archived — the wipe may proceed"
    return 0
  fi
  if [ "$mode" = "off" ]; then
    echo "!!! STRIKE ARCHIVE INTERLOCK OFF ($where): ES4_STRIKE_ARCHIVE_INTERLOCK=off — wiping WITHOUT proof that the strike log is archived." >&2
    echo "!!! ACCEPTED LOSS, recorded here because nothing else will record it:$reasons" >&2
    echo "!!! These ranges are unrecoverable once the data dir is deleted (offsets on the new log name different records): $accepted" >&2
    return 0
  fi
  if [ "$dry" = "true" ]; then
    echo "DRY: strike archive interlock ($where) WOULD REFUSE the wipe:$reasons"
    return 0
  fi
  echo "STRIKE ARCHIVE INTERLOCK ($where): REFUSING to wipe es4's Kafka data dir:$reasons" >&2
  echo "  Remedy: run the es4 archive on the prod host (the 17:01 cron line in scripts/ops/archive/oe-archive.crontab;" >&2
  echo "  it is incremental and idempotent), confirm 'MARK es.futures.footprint.strike' in archive-es4.log, then rerun" >&2
  echo "  the reset — an interrupted reset resumes from its state file, and the resume brings Kafka (and only Kafka)" >&2
  echo "  back up before this check, so both the archive and this check can read the log. To wipe anyway and LOSE" >&2
  echo "  those records, set ES4_STRIKE_ARCHIVE_INTERLOCK=off (Jenkins es4-deploy ACCEPT_UNARCHIVED_STRIKE_LOSS=true)." >&2
  return 1
}

# RESUME (re-review round 2, finding 4). An interrupted reset that already ran `docker compose down` resumes
# with Kafka STOPPED and the durable phase still WIPING. The authoritative interlock then could not read the
# log, refused, and the reset could never get past it — and its own remedy (run the archive, which reads the
# same broker) could not work either. So before that check the broker is made READABLE: if es4-kafka is not
# healthy, ONLY the kafka service is started (--no-deps: never mm2, never schema-registry, never an app).
# That is safe because every producer is already proven at 0 by then — the k8s Deployments by the quiesce
# step, mm2 because it only exists as a compose service started after topic reconciliation, and the prod
# es-feed by the Jenkins-side absence proof — so the log the check reads is the log the wipe would delete.
# It is the same `docker compose up -d kafka` the reset itself runs after the wipe, minus the rest.
#
# Returns 0 once es4-kafka reports healthy, 1 otherwise. The CALLER decides what a 1 means; cleanup-es4.sh
# logs it and still runs the interlock, which then reads an unreachable broker and refuses (fail closed) —
# or, under the explicit opt-out, proceeds and records the loss. That keeps one gate, not two.
# Knobs: ES4_KAFKA_CONTAINER (default es4-kafka), ES4_KAFKA_HEALTH_POLLS (40), ES4_KAFKA_HEALTH_SLEEP (5s).
strike_archive_broker_readable() {   # $1 = the compose directory (INFRA_DIR)
  local dir="$1" c="${ES4_KAFKA_CONTAINER:-es4-kafka}" polls="${ES4_KAFKA_HEALTH_POLLS:-40}"
  local pause="${ES4_KAFKA_HEALTH_SLEEP:-5}" h i=0
  h=$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null) || h="not running"
  if [ "$h" = healthy ]; then
    echo "  strike archive interlock: Kafka ($c) is healthy — the log is readable"
    return 0
  fi
  echo "  strike archive interlock: Kafka ($c) is '${h:-unknown}' (an interrupted reset resuming past 'compose down'?) —"
  echo "  starting ONLY the kafka service so the interlock can read the log; every producer is already proven at 0"
  if ! (cd "$dir" && docker compose up -d --no-deps kafka); then
    echo "  strike archive interlock: 'docker compose up -d --no-deps kafka' FAILED in $dir" >&2
    return 1
  fi
  while [ "$i" -lt "$polls" ]; do
    h=$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null) || h="not running"
    if [ "$h" = healthy ]; then
      echo "  strike archive interlock: Kafka ($c) is healthy — the log is readable"
      return 0
    fi
    i=$((i + 1))
    sleep "$pause"
  done
  echo "  strike archive interlock: Kafka ($c) is still '$h' after $polls polls — the log cannot be read" >&2
  return 1
}
