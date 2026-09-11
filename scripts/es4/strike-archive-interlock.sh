#!/usr/bin/env bash
# strike-archive-interlock.sh — may es4's Kafka data dir be wiped without losing unarchived strike history?
# SOURCED by cleanup-es4.sh (and by its test); defines strike_archive_interlock and nothing else.
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
# committed offset has reached the log end. Anything else refuses:
#   * the group is absent, or has no offset for a non-empty partition  -> nothing is known archived
#   * committed < log end                                            -> (end - committed) offsets unarchived
#   * the log end or the group cannot be READ                         -> unknown is not safe (fail closed)
# An empty or absent topic has nothing to protect and passes.
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
# blocks. Uses kafka-get-offsets and kafka-consumer-groups from PATH (on the box: kafka-cli-shim/).
#
# Returns 0 = the wipe may proceed (proved, or explicitly accepted), 1 = REFUSE.

strike_archive_interlock() {   # $1 = where in the reset this check runs (for the log)
  local where="${1:-}" mode topics group bootstrap dry topic ends ends_rc groups groups_rc
  local line part end committed refuse=0 reasons="" accepted=""
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

  for topic in $topics; do
    ends=$(kafka-get-offsets --bootstrap-server "$bootstrap" --topic "$topic" 2>&1)
    ends_rc=$?
    if [ "$ends_rc" -ne 0 ]; then
      refuse=1; reasons="$reasons
  $topic: the log end cannot be read (kafka-get-offsets rc=$ends_rc: $(printf '%s' "$ends" | tail -1)) — an unreadable source is not a proven-archived one"
      continue
    fi
    ends=$(printf '%s\n' "$ends" | awk -F: -v t="$topic" 'NF >= 3 && $1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { print $2 " " $3 }')
    if [ -z "$ends" ]; then
      echo "  strike archive interlock: $topic is absent on this broker — nothing to protect"
      continue
    fi
    groups=$(kafka-consumer-groups --bootstrap-server "$bootstrap" --describe --group "$group" 2>&1)
    groups_rc=$?
    # An ABSENT group is a state, not an error: every non-empty partition then refuses below. Kafka
    # 3.x says "Consumer group 'G' does not exist."; 4.x prints GroupIdNotFoundException and exits 0.
    # Any OTHER error text is unreadable-state even at exit 0 (4.x reports every describe failure
    # that way), and unreadable is never read as "archived".
    case "$groups" in
      *"does not exist"*|*GroupIdNotFoundException*) groups="" ;;
      *"Error:"*|*Exception*)
        refuse=1; reasons="$reasons
  $topic: the archive marker group '$group' cannot be read (kafka-consumer-groups rc=$groups_rc: $(printf '%s' "$groups" | grep -m1 -E 'Error|Exception'))"
        continue ;;
      *) if [ "$groups_rc" -ne 0 ]; then
           refuse=1; reasons="$reasons
  $topic: the archive marker group '$group' cannot be read (kafka-consumer-groups rc=$groups_rc: $(printf '%s' "$groups" | tail -1))"
           continue
         fi ;;
    esac
    while read -r part end; do
      [ -n "$part" ] || continue
      [ "$end" -gt 0 ] || continue
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
  echo "  the reset — an interrupted reset resumes from its state file. To wipe anyway and LOSE those records, set" >&2
  echo "  ES4_STRIKE_ARCHIVE_INTERLOCK=off (Jenkins es4-deploy ACCEPT_UNARCHIVED_STRIKE_LOSS=true)." >&2
  return 1
}
