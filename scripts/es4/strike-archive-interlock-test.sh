#!/usr/bin/env bash
# strike-archive-interlock-test.sh — drives the REAL strike_archive_interlock (sourced from
# strike-archive-interlock.sh, exactly as cleanup-es4.sh sources it) against mocked kafka-get-offsets and
# kafka-consumer-groups, and pins how cleanup-es4.sh and Jenkinsfile.es4-deploy wire it.
#
# The contract (deploy Codex final review, finding 2): es4's clean-reset deletes the whole Kafka data dir,
# so it may only proceed once the archive's marker (consumer group oe-archive-committed-boundary, written
# by oe-archive-kafka.sh after each durable strike capture) has reached the strike log end. Unknown is
# never "archived"; the only way past a refusal is the explicit opt-out, which must say what it discards.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
W="$(mktemp -d)"; trap 'rm -rf "$W"' EXIT
MOCK="$W/bin"; mkdir -p "$MOCK"
# The mocks answer from files each case writes, and record what they were asked.
cat > "$MOCK/kafka-get-offsets" <<'SH'
#!/usr/bin/env bash
echo "get-offsets $*" >> "$FIX/calls"
cat "$FIX/ends" 2>/dev/null
exit "$(cat "$FIX/ends_rc" 2>/dev/null || echo 0)"
SH
cat > "$MOCK/kafka-consumer-groups" <<'SH'
#!/usr/bin/env bash
echo "consumer-groups $*" >> "$FIX/calls"
cat "$FIX/groups" 2>/dev/null
exit "$(cat "$FIX/groups_rc" 2>/dev/null || echo 0)"
SH
chmod +x "$MOCK"/*
export FIX="$W/fix"; mkdir -p "$FIX"

T=es.futures.footprint.strike
G=oe-archive-committed-boundary
describe() { # <topic> <partition> <current-offset> [<log-end>]  -> a kafka-consumer-groups --describe body
  printf '\nConsumer group '"'"'%s'"'"' has no active members.\n\n' "$G"
  printf 'GROUP                         TOPIC                        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID HOST CLIENT-ID\n'
  printf '%s %s %s %s %s %s - - -\n' "$G" "$1" "$2" "$3" "${4:-$3}" "$(( ${4:-$3} - $3 ))"
}
case_() { # <label> ; resets the mocked broker
  LABEL="$1"; rm -f "$FIX"/*; : > "$FIX/calls"
}
check() { # <expected rc> [env assignments...] ; runs the interlock in a subshell
  local want="$1"; shift
  OUT=$(env PATH="$MOCK:$PATH" "$@" bash -c '. "$0"; strike_archive_interlock test' "$HERE/strike-archive-interlock.sh" 2>&1)
  RC=$?
  if [ "$RC" = "$want" ]; then ok "$LABEL -> rc $RC"; else bad "$LABEL -> rc $RC, wanted $want"; printf '%s\n' "$OUT" | sed 's/^/      | /'; fi
}
says()    { printf '%s' "$OUT" | grep -Fq -- "$1" && ok "  says: $1" || { bad "  missing text: $1"; printf '%s\n' "$OUT" | sed 's/^/      | /'; }; }
calls()   { grep -c -- "$1" "$FIX/calls" 2>/dev/null || true; }

echo "1. nothing to protect"
case_ "topic absent on the broker (empty answer)";           : > "$FIX/ends";                                 check 0
says "absent on this broker"
case_ "topic present but EMPTY (end 0), no group at all";     echo "$T:0:0" > "$FIX/ends"; echo "Consumer group '$G' does not exist." > "$FIX/groups"; check 0

echo "2. archived through the log end -> the wipe may proceed"
case_ "marker == log end";                                    echo "$T:0:1234" > "$FIX/ends"; describe "$T" 0 1234 > "$FIX/groups"; check 0
says "archived through 1234, log end 1234"
case_ "marker AHEAD of the log end (a re-created topic) refuses"; echo "$T:0:10" > "$FIX/ends"; describe "$T" 0 5000 > "$FIX/groups"; check 1
says "AHEAD of the log end 10"

echo "3. anything short of the log end REFUSES"
case_ "marker behind the log end";                            echo "$T:0:1234" > "$FIX/ends"; describe "$T" 0 1100 1234 > "$FIX/groups"; check 1
says "archived through 1100, log end 1234 — 134 offset(s) not archived"
says "REFUSING to wipe"
says "ES4_STRIKE_ARCHIVE_INTERLOCK=off"
case_ "group absent (Kafka 3.x wording), log non-empty";      echo "$T:0:50" > "$FIX/ends";   echo "Consumer group '$G' does not exist." > "$FIX/groups"; check 1
says "NO archive marker"
case_ "group absent (Kafka 4.x GroupIdNotFoundException, rc 0)"; echo "$T:0:50" > "$FIX/ends"
printf 'Error: Executing consumer group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group %s not found.\n' "$G" > "$FIX/groups"; check 1
says "NO archive marker"
case_ "group exists but holds no offset for the strike topic"; echo "$T:0:50" > "$FIX/ends"; describe other.topic 0 99 > "$FIX/groups"; check 1
case_ "two partitions, one covered and one short";            printf '%s\n' "$T:0:100" "$T:1:100" > "$FIX/ends"
{ describe "$T" 0 100; printf '%s %s 1 90 100 10 - - -\n' "$G" "$T"; } > "$FIX/groups"; check 1
says "$T p1: archived through 90, log end 100"

echo "4. an unreadable state is NOT an archived one (fail closed)"
case_ "kafka-get-offsets fails";                              echo "boom" > "$FIX/ends"; echo 1 > "$FIX/ends_rc"; check 1
says "the log end cannot be read"
[ "$(calls consumer-groups)" = 0 ] && ok "  and the group was not even consulted" || bad "  consulted the group after an unreadable end"
case_ "kafka-consumer-groups fails (rc 1)";                   echo "$T:0:50" > "$FIX/ends"; echo "Error: connection refused" > "$FIX/groups"; echo 1 > "$FIX/groups_rc"; check 1
says "cannot be read"
case_ "kafka-consumer-groups reports a TimeoutException at rc 0 (Kafka 4.x)"; echo "$T:0:50" > "$FIX/ends"
echo "Error: Executing consumer group command failed due to org.apache.kafka.common.errors.TimeoutException: Timed out" > "$FIX/groups"; check 1
says "cannot be read"
case_ "an invalid mode value";                                echo "$T:0:50" > "$FIX/ends"; describe "$T" 0 50 > "$FIX/groups"; check 1 ES4_STRIKE_ARCHIVE_INTERLOCK=maybe
says "must be 'on' or 'off'"

echo "5. the explicit opt-out proceeds, and records what it discards"
case_ "ES4_STRIKE_ARCHIVE_INTERLOCK=off with an unarchived tail"; echo "$T:0:1234" > "$FIX/ends"; describe "$T" 0 1100 1234 > "$FIX/groups"
check 0 ES4_STRIKE_ARCHIVE_INTERLOCK=off
says "INTERLOCK OFF"
says "ACCEPTED LOSS"
says "$T:0:1100-1234"

echo "6. a DRY run reports the verdict and never blocks"
case_ "DEPLOY_DRY_RUN=true with an unarchived tail";          echo "$T:0:1234" > "$FIX/ends"; describe "$T" 0 1100 1234 > "$FIX/groups"
check 0 DEPLOY_DRY_RUN=true
says "WOULD REFUSE"

echo "7. it asks the broker the right questions"
case_ "bootstrap and group are the documented contract";      echo "$T:0:1" > "$FIX/ends"; describe "$T" 0 1 > "$FIX/groups"; check 0
grep -q -- "get-offsets --bootstrap-server localhost:29092 --topic $T" "$FIX/calls" && ok "  log end read from the in-container listener" || bad "  unexpected get-offsets call: $(cat "$FIX/calls")"
grep -q -- "consumer-groups --bootstrap-server localhost:29092 --describe --group $G" "$FIX/calls" && ok "  marker read from group $G" || bad "  unexpected consumer-groups call: $(cat "$FIX/calls")"
arch_group=$(sed -n 's/^OE_ARCHIVE_MARK_GROUP="\${OE_ARCHIVE_MARK_GROUP:-\(.*\)}"$/\1/p' "$ROOT/scripts/ops/archive/oe-archive-kafka.sh")
[ "$arch_group" = "$G" ] && ok "  the archiver writes the SAME group ($arch_group)" || bad "  archiver group '$arch_group' != interlock group '$G' — the marker would never be found"

echo "8. wiring: cleanup-es4.sh runs it before the first mutation AND immediately before the wipe"
S="$ROOT/scripts/es4/cleanup-es4.sh"
pos() { grep -n -F -- "$1" "$S" | head -1 | cut -d: -f1; }
p_src=$(pos '. "$SCRIPT_DIR/strike-archive-interlock.sh"')
p_pre=$(pos 'strike_archive_interlock "preflight"')
p_capture=$(pos 'log "capturing es4 Deployment replica counts')
p_scale=$(pos 'log "scaling es4 Deployments to 0')
p_final=$(pos 'strike_archive_interlock "before the wipe"')
p_down=$(pos 'log "docker compose down')
p_rm=$(pos "run \"sudo -n find '\$KAFKA_DATA'")
for v in p_src p_pre p_capture p_scale p_final p_down p_rm; do
  [ -n "${!v}" ] || bad "  marker for $v not found in cleanup-es4.sh"
done
if [ -n "$p_src$p_pre$p_capture$p_scale$p_final$p_down$p_rm" ]; then
  [ "${p_src:-0}" -lt "${p_pre:-0}" ] && [ "${p_pre:-0}" -lt "${p_capture:-0}" ] \
    && ok "  preflight check is sourced and runs before the replica capture (no mutation yet)" \
    || bad "  preflight check is not ahead of the first mutation (src=$p_src pre=$p_pre capture=$p_capture)"
  [ "${p_scale:-0}" -lt "${p_final:-0}" ] && [ "${p_final:-0}" -lt "${p_down:-0}" ] && [ "${p_down:-0}" -lt "${p_rm:-0}" ] \
    && ok "  authoritative check runs after quiescing and before compose down / the data-dir delete" \
    || bad "  final check is out of order (scale=$p_scale final=$p_final down=$p_down rm=$p_rm)"
fi
grep -q 'strike_archive_interlock "preflight" *\\$' "$S" && grep -q 'strike_archive_interlock "before the wipe" *\\$' "$S" \
  && grep -A1 'strike_archive_interlock "preflight"' "$S" | grep -q '|| die' \
  && grep -A1 'strike_archive_interlock "before the wipe"' "$S" | grep -q '|| die' \
  && ok "  both calls die on refusal" || bad "  a refusal is not fatal at one of the call sites"
bash -n "$S" && ok "  cleanup-es4.sh still parses" || bad "  cleanup-es4.sh has a syntax error"

echo "9. wiring: Jenkins can only turn it off through the explicit parameter"
J="$ROOT/Jenkinsfile.es4-deploy"
grep -q "booleanParam(name: 'ACCEPT_UNARCHIVED_STRIKE_LOSS', defaultValue: false" "$J" \
  && ok "  ACCEPT_UNARCHIVED_STRIKE_LOSS exists and defaults to false" || bad "  the opt-out parameter is missing or not default-false"
[ "$(grep -c 'ES4_STRIKE_ARCHIVE_INTERLOCK=\$STRIKE_INTERLOCK bash /home/es4/repo/scripts/es4/cleanup-es4.sh' "$J")" = 2 ] \
  && ok "  both the DRY and the real on-box invocation pass the interlock mode" || bad "  an on-box cleanup invocation does not pass ES4_STRIKE_ARCHIVE_INTERLOCK"
grep -q 'if \[ "${ACCEPT_UNARCHIVED_STRIKE_LOSS:-false}" = "true" \]; then STRIKE_INTERLOCK=off; fi' "$J" \
  && grep -q '^ *STRIKE_INTERLOCK=on$' "$J" \
  && ok "  the mode is 'on' unless that parameter is ticked" || bad "  the mode is not derived from the parameter as expected"

echo
if [ "$fails" -eq 0 ]; then echo "=== strike-archive-interlock: OK ==="; exit 0; fi
echo "=== strike-archive-interlock: $fails problem(s) ===" >&2; exit 1
