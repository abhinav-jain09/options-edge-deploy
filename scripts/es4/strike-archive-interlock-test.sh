#!/usr/bin/env bash
# strike-archive-interlock-test.sh — drives the REAL strike_archive_interlock and strike_archive_broker_readable
# (sourced from strike-archive-interlock.sh, exactly as cleanup-es4.sh sources them) against mocked Kafka CLIs
# and docker, drives the REAL cleanup-es4.sh through an interrupted-reset RESUME, and pins how cleanup-es4.sh
# and Jenkinsfile.es4-deploy wire them.
#
# The contract (deploy Codex final review, finding 2): es4's clean-reset deletes the whole Kafka data dir,
# so it may only proceed once the archive's marker (consumer group oe-archive-committed-boundary, written
# by oe-archive-kafka.sh after each durable strike capture) has reached the strike log end. Unknown is
# never "archived"; the only way past a refusal is the explicit opt-out, which must say what it discards.
#
# Re-review round 2 adds: (finding 1) a partition whose log end is not read — GetOffsetShell's "Skip
# getting offsets" at exit 0, or simply a missing line — refuses; (finding 3) a topic confirmed absent from a
# successfully read topic list passes, while unreadable metadata refuses; (finding 4) a reset interrupted
# after `compose down` can resume: the broker is brought up (kafka only) before the check. Section 0 replays
# REAL CLI answers — Kafka 4.3.0 (the prod host) and 3.9.0 (whose GetOffsetShell is identical to es4's 3.7.1)
# — recorded by scripts/ops/archive/broker-test/strike-reader-broker-test.sh into cli-fixtures/.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CLI="$ROOT/scripts/ops/archive/broker-test/cli-fixtures"
for v in 4.3.0 3.9.0; do
  [ -f "$CLI/$v/VERSION" ] || { echo "FATAL: no recorded CLI answers in $CLI/$v — rerun strike-reader-broker-test.sh with FIXTURE_OUT" >&2; exit 1; }
done
[ -f "$CLI/constructed/skip-diagnostic.err" ] || { echo "FATAL: $CLI/constructed/skip-diagnostic.err missing" >&2; exit 1; }
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
W="$(cd "$(mktemp -d)" && pwd -P)"; trap 'rm -rf "$W"' EXIT
MOCK="$W/bin"; mkdir -p "$MOCK"
export FIX="$W/fix"; mkdir -p "$FIX"
export FIXCLI="$CLI/4.3.0"    # which recorded version an UNREACHABLE broker replays

# ---- mocks: each answers from files a case writes, records what it was asked, and — when the case says Kafka
# is not healthy ($FIX/kafka_state) — replays the REAL answer of an unreachable broker.
for c in kafka-topics:topics kafka-get-offsets:get-offsets kafka-consumer-groups:consumer-groups; do
  cat > "$MOCK/${c%%:*}" <<SH
#!/usr/bin/env bash
echo "${c#*:} \$*" >> "\$FIX/calls"
st=\$(cat "\$FIX/kafka_state" 2>/dev/null || echo healthy)
case "${c#*:}:\$*" in
  topics:*--list*)     k=list; u=unreach-list ;;
  topics:*--describe*) k=desc; u=unreach-describe ;;
  get-offsets:*)       k=ends; u=unreach-ends ;;
  consumer-groups:*)   k=groups; u=unreach-groups ;;
  *) echo "mock: unexpected ${c%%:*} \$*" >&2; exit 99 ;;
esac
if [ "\$st" != healthy ]; then cat "\$FIXCLI/\$u.out"; cat "\$FIXCLI/\$u.err" >&2; exit "\$(cat "\$FIXCLI/\$u.rc")"; fi
[ -f "\$FIX/\$k.out" ] && cat "\$FIX/\$k.out"
[ -f "\$FIX/\$k.err" ] && cat "\$FIX/\$k.err" >&2
exit "\$(cat "\$FIX/\$k.rc" 2>/dev/null || echo 0)"
SH
done
# docker: `inspect` reports $FIX/kafka_state; `compose up -d --no-deps kafka` applies $FIX/after_up
# (healthy | starting:N | unhealthy) unless $FIX/compose_up_fails; `compose down` stops Kafka; the
# datastore start that follows the wipe exits 42, which is where the end-to-end runs below stop.
cat > "$MOCK/docker" <<'SH'
#!/usr/bin/env bash
echo "docker $* [cwd=$PWD]" >> "$FIX/calls"
case "$1" in
  inspect)
    name="${*: -1}"
    [ "$name" = es4-kafka ] || { echo healthy; exit 0; }
    st=$(cat "$FIX/kafka_state" 2>/dev/null || echo healthy)
    case "$st" in
      absent) echo "Error: No such object: $name" >&2; exit 1 ;;
      starting)
        n=$(cat "$FIX/starting_polls" 2>/dev/null || echo 0)
        if [ "$n" -gt 0 ]; then echo $((n - 1)) > "$FIX/starting_polls"; echo starting; exit 0; fi
        echo healthy > "$FIX/kafka_state"; echo healthy; exit 0 ;;
      *) echo "$st"; exit 0 ;;
    esac ;;
  compose)
    shift
    case "$*" in
      "up -d --no-deps kafka")
        [ -f "$FIX/compose_up_fails" ] && { echo "Error response from daemon: mock" >&2; exit 1; }
        a=$(cat "$FIX/after_up" 2>/dev/null || echo healthy)
        case "$a" in starting:*) echo starting > "$FIX/kafka_state"; echo "${a#starting:}" > "$FIX/starting_polls" ;; *) echo "$a" > "$FIX/kafka_state" ;; esac
        exit 0 ;;
      config) echo "        source: $E2E_KAFKA_DATA"; exit 0 ;;
      down) echo absent > "$FIX/kafka_state"; exit 0 ;;
      "up -d kafka schema-registry postgres redis") exit 42 ;;
      *) echo "docker mock: unexpected compose $*" >&2; exit 98 ;;
    esac ;;
  *) echo "docker mock: unexpected $*" >&2; exit 98 ;;
esac
SH
# sudo -n: k3s goes to the k3s mock; /var paths are accepted and never touched; everything else really runs
# (test, readlink, and the find that empties the TEST Kafka data dir).
cat > "$MOCK/sudo" <<'SH'
#!/usr/bin/env bash
[ "$1" = -n ] && shift
echo "sudo $*" >> "$FIX/calls"
case "$1" in
  /usr/local/bin/k3s) shift; exec k3s "$@" ;;
  test|find) case "$*" in *" /var/"*) exit 0 ;; esac; exec "$@" ;;
  *) exec "$@" ;;
esac
SH
cat > "$MOCK/k3s" <<'SH'
#!/usr/bin/env bash
[ "$1" = kubectl ] && shift
[ "$1" = -n ] && shift 2
echo "k3s $*" >> "$FIX/calls"
case "$*" in
  "get namespace options-edge -o jsonpath={.metadata.uid}") echo 11111111-2222-3333-4444-555555555555 ;;
  version) echo mock ;;
  "get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers") printf 'svc-a 0\nsvc-b 0\n' ;;
  scale\ *) : ;;
  "get deploy -o jsonpath="*) printf 'svc-a 0\nsvc-b 0\n' ;;
  "get pods --no-headers") : ;;
  get\ deploy/*) : ;;
  *) echo "k3s mock: unexpected: $*" >&2; exit 97 ;;
esac
SH
printf '#!/usr/bin/env bash\nexit 0\n' > "$MOCK/flock"
chmod +x "$MOCK"/*

T=es.futures.footprint.strike
G=oe-archive-committed-boundary
case_() { LABEL="$1"; rm -f "$FIX"/*; : > "$FIX/calls"; }
check() { # <expected rc> [env assignments...] ; runs the interlock in a subshell
  local want="$1"; shift
  OUT=$(env PATH="$MOCK:$PATH" "$@" bash -c '. "$0"; strike_archive_interlock test' "$HERE/strike-archive-interlock.sh" 2>&1)
  RC=$?
  if [ "$RC" = "$want" ]; then ok "$LABEL -> rc $RC"; else bad "$LABEL -> rc $RC, wanted $want"; printf '%s\n' "$OUT" | sed 's/^/      | /' | tail -25; fi
}
says()  { printf '%s' "$OUT" | grep -Fq -- "$1" && ok "  says: $1" || { bad "  missing text: $1"; printf '%s\n' "$OUT" | sed 's/^/      | /' | tail -25; }; }
calls() { grep -c -- "$1" "$FIX/calls" 2>/dev/null || true; }
first() { grep -n -F -- "$1" "$FIX/calls" 2>/dev/null | head -1 | cut -d: -f1; }

# ---- answers. `real` replays a RECORDED answer (placeholders mapped to the strike topic and group unless
# other sed expressions are given); the rest synthesize the same layouts with chosen numbers.
real() { # <kind: list|desc|ends|groups> <version> <fixture> [sed expressions...]
  local k="$1" v="$2" n="$3"; shift 3
  if [ $# -eq 0 ]; then
    set -- -e "s/@TOPIC3@/$T/g" -e "s/@TOPIC1@/$T/g" -e "s/@ABSENT@/$T/g" -e "s/@GROUP3@/$G/g" -e "s/@GROUP1@/$G/g" -e "s/@NOGROUP@/$G/g"
  fi
  sed "$@" "$CLI/$v/$n.out" > "$FIX/$k.out"; sed "$@" "$CLI/$v/$n.err" > "$FIX/$k.err"; cp "$CLI/$v/$n.rc" "$FIX/$k.rc"
}
listed()    { printf '%s\n' "$@" > "$FIX/list.out"; }
described() { # <topic> <partition count> — the recorded 4.3.0 --describe layout
  { printf 'Topic: %s\tTopicId: o7rwSey8StqIYif6i-zIgQ\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: retention.ms=-1\n' "$1" "$2"
    local i=0; while [ "$i" -lt "$2" ]; do printf '\tTopic: %s\tPartition: %s\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n' "$1" "$i"; i=$((i+1)); done
  } > "$FIX/desc.out"
}
ends()      { printf '%s\n' "$@" > "$FIX/ends.out"; }
describe()  { # <topic> <partition> <current-offset> [<log-end>]  -> a kafka-consumer-groups --describe body
  printf '\nConsumer group '"'"'%s'"'"' has no active members.\n\n' "$G"
  printf 'GROUP                         TOPIC                        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID HOST CLIENT-ID\n'
  printf '%s %s %s %s %s %s - - -\n' "$G" "$1" "$2" "$3" "${4:-$3}" "$(( ${4:-$3} - $3 ))"
}
world() { # <log end> <marker> — one strike partition, fully readable
  listed other.topic "$T"; described "$T" 1; ends "$T:0:$1"; describe "$T" 0 "$2" "$1" > "$FIX/groups.out"
}

echo "0. REAL CLI answers (recorded against a real broker), per CLI version"
for v in 4.3.0 3.9.0; do
  case_ "$v: 3 partitions, each archived through its log end"
  real list "$v" list -e "s/@TOPIC3@/$T/g" -e "s/@TOPIC1@/other.topic/g"
  real desc "$v" describe-3p; real ends "$v" ends-3p; real groups "$v" groups-3p
  check 0
  says "$T p0 archived through 2, log end 2 — covered"; says "$T p1 archived through 3, log end 3 — covered"; says "$T p2 archived through 4, log end 4 — covered"

  case_ "$v: finding 3 — the topic does not exist (absent from a topic list read successfully)"
  real list "$v" list -e "s/@TOPIC3@/other.one/g" -e "s/@TOPIC1@/other.two/g"
  real ends "$v" ends-absent        # what kafka-get-offsets WOULD say: exit 1, "Could not match ..."
  check 0
  says "$T is not in the broker's topic list (read successfully) — confirmed absent, nothing to protect"
  [ "$(calls get-offsets)" = 0 ] && ok "  absence was not inferred from kafka-get-offsets" || bad "  kafka-get-offsets was consulted for an absent topic"
  [ "$(cat "$CLI/$v/ends-absent.rc")" = 1 ] && grep -q 'Could not match any topic-partitions' "$CLI/$v/ends-absent.err" \
    && ok "  (the recorded $v kafka-get-offsets answer for that topic is exit 1 'Could not match ...' — the round-1 parser refused on it)" \
    || bad "  the recorded absent-topic answer is not what finding 3 describes"

  case_ "$v: finding 1 — 'Skip getting offsets' for p1 at EXIT 0, p1 missing from stdout"
  real list "$v" list -e "s/@TOPIC3@/$T/g" -e "s/@TOPIC1@/other.topic/g"
  real desc "$v" describe-3p; real ends "$v" ends-3p; real groups "$v" groups-3p
  grep -v ":1:" "$FIX/ends.out" > "$FIX/ends.t" && mv "$FIX/ends.t" "$FIX/ends.out"
  sed "s/@TOPIC3@/$T/g" "$CLI/constructed/skip-diagnostic.err" > "$FIX/ends.err"; echo 0 > "$FIX/ends.rc"
  check 1
  says "the log end cannot be read"; says "Skip getting offsets for topic-partition $T-1"
  [ "$(calls consumer-groups)" = 0 ] && ok "  and the marker was not even consulted" || bad "  consulted the marker after an unread partition"

  case_ "$v: finding 1 — a partition missing with NOTHING reported (exit 0, clean stderr)"
  real list "$v" list -e "s/@TOPIC3@/$T/g" -e "s/@TOPIC1@/other.topic/g"
  real desc "$v" describe-3p; real ends "$v" ends-3p; real groups "$v" groups-3p
  grep -v ":2:" "$FIX/ends.out" > "$FIX/ends.t" && mv "$FIX/ends.t" "$FIX/ends.out"
  check 1
  says "log ends came back for partitions [0 1] of the 3 the topic has"

  case_ "$v: an UNREACHABLE broker (the recorded answers: ~60s AdminClient timeouts)"
  echo absent > "$FIX/kafka_state"
  FIXCLI="$CLI/$v" check 1
  says "the broker's topic list cannot be read (exit 1: Error while executing topic command : Timed out waiting for a node assignment"
  [ "$(calls get-offsets)$(calls consumer-groups)" = 00 ] && ok "  and nothing further was concluded" || bad "  it went on to read offsets from an unreadable broker"

  case_ "$v: listed, then gone before --describe (recorded 'does not exist as expected', exit 1)"
  listed "$T"; real desc "$v" describe-absent
  check 1
  says "its partitions cannot be established (kafka-topics --describe: exit 1: Error while executing topic command"

  case_ "$v: described, then gone before kafka-get-offsets (recorded exit 1 'Could not match')"
  listed "$T"; real desc "$v" describe-1p; real ends "$v" ends-absent
  check 1
  says "kafka-get-offsets: exit 1: Error occurred: Could not match any topic-partitions"

  case_ "$v: a real 1-partition log (end 16) and NO marker group (recorded answer)"
  listed "$T"; real desc "$v" describe-1p; real ends "$v" ends-1p; real groups "$v" groups-absent
  check 1
  says "$T p0: log end 16, but NO archive marker"

  case_ "$v: marker group unreadable — recorded exit 0 'Error: ... TimeoutException' of an unreachable broker"
  listed "$T"; real desc "$v" describe-1p; real ends "$v" ends-1p; real groups "$v" unreach-groups
  check 1
  says "the archive marker group '$G' cannot be read (kafka-consumer-groups rc=0: Error: Executing consumer group command failed"
done

echo "1. nothing to protect"
case_ "topic present but EMPTY (end 0), no group at all";     listed "$T"; described "$T" 1; ends "$T:0:0"; echo "Consumer group '$G' does not exist." > "$FIX/groups.out"; check 0
says "$T p0 is empty (log end 0)"

echo "2. archived through the log end -> the wipe may proceed"
case_ "marker == log end";                                    world 1234 1234; check 0
says "archived through 1234, log end 1234"
case_ "marker AHEAD of the log end (a re-created topic) refuses"; world 10 5000; check 1
says "AHEAD of the log end 10"

echo "3. anything short of the log end REFUSES"
case_ "marker behind the log end";                            world 1234 1100; check 1
says "archived through 1100, log end 1234 — 134 offset(s) not archived"
says "REFUSING to wipe"
says "ES4_STRIKE_ARCHIVE_INTERLOCK=off"
case_ "group absent (Kafka 3.x wording), log non-empty";      world 50 0; echo "Consumer group '$G' does not exist." > "$FIX/groups.out"; check 1
says "NO archive marker"
case_ "group absent (Kafka 4.x GroupIdNotFoundException, rc 0)"; world 50 0
printf 'Error: Executing consumer group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group %s not found.\n' "$G" > "$FIX/groups.out"; check 1
says "NO archive marker"
case_ "group exists but holds no offset for the strike topic"; world 50 0; describe other.topic 0 99 > "$FIX/groups.out"; check 1
case_ "two partitions, one covered and one short";            listed "$T"; described "$T" 2; ends "$T:0:100" "$T:1:100"
{ describe "$T" 0 100; printf '%s %s 1 90 100 10 - - -\n' "$G" "$T"; } > "$FIX/groups.out"; check 1
says "$T p1: archived through 90, log end 100"

echo "4. an unreadable or short answer is NOT an archived one (fail closed)"
case_ "topic list at exit 0 but with a line that is no topic name"; world 50 50; printf '%s\n' "$T" "WARNING: something went wrong" > "$FIX/list.out"; check 1
says "the broker's topic list cannot be read (unexpected output line: WARNING: something went wrong)"
case_ "topic list at exit 0 with an error on stderr";         world 50 50; echo "org.apache.kafka.common.errors.TimeoutException: Timed out" > "$FIX/list.err"; check 1
says "exit 0 but it reported: org.apache.kafka.common.errors.TimeoutException"
case_ "describe says PartitionCount 2 but lists only partition 0"; world 50 50; described "$T" 2; grep -v 'Partition: 1' "$FIX/desc.out" > "$FIX/d.t"; mv "$FIX/d.t" "$FIX/desc.out"; check 1
says "PartitionCount 2, but partition lines [0]"
case_ "describe without a PartitionCount";                    world 50 50; echo "Topic: $T" > "$FIX/desc.out"; check 1
says "no single PartitionCount"
case_ "the same partition twice, another missing";             world 50 50; described "$T" 2; ends "$T:0:50" "$T:0:50"; check 1
says "partitions [0 0] of the 2"
case_ "an extra partition the metadata does not have";         world 50 50; ends "$T:0:50" "$T:1:50"; check 1
says "partitions [0 1] of the 1"
case_ "a non-numeric log end";                                 world 50 50; ends "$T:0:abc"; check 1
says "unexpected output line: $T:0:abc"
case_ "kafka-get-offsets fails (exit 1)";                       world 50 50; echo "boom" > "$FIX/ends.err"; echo 1 > "$FIX/ends.rc"; : > "$FIX/ends.out"; check 1
says "the log end cannot be read"
[ "$(calls consumer-groups)" = 0 ] && ok "  and the group was not even consulted" || bad "  consulted the group after an unreadable end"
case_ "kafka-consumer-groups fails (rc 1)";                   world 50 50; echo "Error: connection refused" > "$FIX/groups.out"; echo 1 > "$FIX/groups.rc"; check 1
says "cannot be read"
case_ "kafka-consumer-groups reports a TimeoutException at rc 0 (Kafka 4.x)"; world 50 50
echo "Error: Executing consumer group command failed due to org.apache.kafka.common.errors.TimeoutException: Timed out" > "$FIX/groups.out"; check 1
says "cannot be read"
case_ "an invalid mode value";                                world 50 50; check 1 ES4_STRIKE_ARCHIVE_INTERLOCK=maybe
says "must be 'on' or 'off'"

echo "5. the explicit opt-out proceeds, and records what it discards"
case_ "ES4_STRIKE_ARCHIVE_INTERLOCK=off with an unarchived tail"; world 1234 1100
check 0 ES4_STRIKE_ARCHIVE_INTERLOCK=off
says "INTERLOCK OFF"
says "ACCEPTED LOSS"
says "$T:0:1100-1234"
case_ "ES4_STRIKE_ARCHIVE_INTERLOCK=off with an UNREADABLE broker"; echo absent > "$FIX/kafka_state"
check 0 ES4_STRIKE_ARCHIVE_INTERLOCK=off
says "$T:presence-unknown"

echo "6. a DRY run reports the verdict and never blocks"
case_ "DEPLOY_DRY_RUN=true with an unarchived tail";          world 1234 1100
check 0 DEPLOY_DRY_RUN=true
says "WOULD REFUSE"

echo "7. it asks the broker the right questions"
case_ "bootstrap and group are the documented contract";      world 1 1; check 0
for q in "topics --bootstrap-server localhost:29092 --list" "topics --bootstrap-server localhost:29092 --describe --topic $T" \
         "get-offsets --bootstrap-server localhost:29092 --topic $T" "consumer-groups --bootstrap-server localhost:29092 --describe --group $G"; do
  grep -qxF -- "$q" "$FIX/calls" && ok "  asked: $q" || bad "  never asked '$q' (calls: $(tr '\n' '|' < "$FIX/calls"))"
done
arch_group=$(sed -n 's/^OE_ARCHIVE_MARK_GROUP="\${OE_ARCHIVE_MARK_GROUP:-\(.*\)}"$/\1/p' "$ROOT/scripts/ops/archive/oe-archive-kafka.sh")
[ "$arch_group" = "$G" ] && ok "  the archiver writes the SAME group ($arch_group)" || bad "  archiver group '$arch_group' != interlock group '$G' — the marker would never be found"

echo "8. strike_archive_broker_readable — the resume brings up Kafka, and only Kafka (finding 4)"
ready() { # <expected rc> [env...]
  local want="$1"; shift
  OUT=$(cd "$W" && env PATH="$MOCK:$PATH" ES4_KAFKA_HEALTH_SLEEP=0 "$@" bash -c '. "$0"; strike_archive_broker_readable "$1"' "$HERE/strike-archive-interlock.sh" "$W/infra" 2>&1)
  RC=$?
  if [ "$RC" = "$want" ]; then ok "$LABEL -> rc $RC"; else bad "$LABEL -> rc $RC, wanted $want"; printf '%s\n' "$OUT" | sed 's/^/      | /'; fi
}
mkdir -p "$W/infra"
case_ "Kafka already healthy";                  echo healthy > "$FIX/kafka_state"; ready 0
[ "$(calls 'docker compose')" = 0 ] && ok "  nothing started" || bad "  it ran docker compose on a healthy broker"
case_ "Kafka stopped (a resume past compose down)"; echo absent > "$FIX/kafka_state"; echo healthy > "$FIX/after_up"; ready 0
says "starting ONLY the kafka service"
grep -qxF "docker compose up -d --no-deps kafka [cwd=$W/infra]" "$FIX/calls" && ok "  ran exactly 'docker compose up -d --no-deps kafka' in the compose dir" \
  || bad "  unexpected compose call: $(grep 'docker compose' "$FIX/calls")"
[ "$(calls 'docker compose')" = 1 ] && ok "  and no other compose command (never mm2, never the rest)" || bad "  more than one compose command"
case_ "Kafka starting for 2 polls, then healthy"; echo absent > "$FIX/kafka_state"; echo starting:2 > "$FIX/after_up"; ready 0 ES4_KAFKA_HEALTH_POLLS=5
case_ "Kafka never becomes healthy";            echo absent > "$FIX/kafka_state"; echo unhealthy > "$FIX/after_up"; ready 1 ES4_KAFKA_HEALTH_POLLS=3
says "still 'unhealthy' after 3 polls"
case_ "docker compose up fails";                 echo absent > "$FIX/kafka_state"; touch "$FIX/compose_up_fails"; ready 1
says "FAILED"

echo "9. wiring: cleanup-es4.sh runs it before the first mutation AND, broker readable, immediately before the wipe"
S="$ROOT/scripts/es4/cleanup-es4.sh"
pos() { grep -n -F -- "$1" "$S" | head -1 | cut -d: -f1; }
p_src=$(pos '. "$SCRIPT_DIR/strike-archive-interlock.sh"')
p_pre=$(pos 'strike_archive_interlock "preflight"')
p_capture=$(pos 'log "capturing es4 Deployment replica counts')
p_scale=$(pos 'log "scaling es4 Deployments to 0')
p_quiet=$(pos 'app not fully quiesced')
p_ready=$(pos 'strike_archive_broker_readable "$INFRA_DIR"')
p_final=$(pos 'strike_archive_interlock "before the wipe"')
p_down=$(pos 'log "docker compose down')
p_rm=$(pos "run \"sudo -n find '\$KAFKA_DATA'")
for v in p_src p_pre p_capture p_scale p_quiet p_ready p_final p_down p_rm; do
  [ -n "${!v}" ] || bad "  marker for $v not found in cleanup-es4.sh"
done
if [ -n "$p_src$p_pre$p_capture$p_scale$p_quiet$p_ready$p_final$p_down$p_rm" ]; then
  [ "${p_src:-0}" -lt "${p_pre:-0}" ] && [ "${p_pre:-0}" -lt "${p_capture:-0}" ] \
    && ok "  preflight check is sourced and runs before the replica capture (no mutation yet)" \
    || bad "  preflight check is not ahead of the first mutation (src=$p_src pre=$p_pre capture=$p_capture)"
  [ "${p_scale:-0}" -lt "${p_quiet:-0}" ] && [ "${p_quiet:-0}" -lt "${p_ready:-0}" ] && [ "${p_ready:-0}" -lt "${p_final:-0}" ] \
    && [ "${p_final:-0}" -lt "${p_down:-0}" ] && [ "${p_down:-0}" -lt "${p_rm:-0}" ] \
    && ok "  quiesce proof -> broker made readable -> authoritative check -> compose down -> data-dir delete" \
    || bad "  out of order (scale=$p_scale quiesced=$p_quiet ready=$p_ready final=$p_final down=$p_down rm=$p_rm)"
fi
sed -n "$((p_ready - 1))p" "$S" | grep -q 'if \[ "$DRY" != "true" \]; then' \
  && ok "  the broker start is skipped on a DRY run" || bad "  the broker start is not guarded by DRY"
sed -n "$((p_ready + 1))p" "$S" | grep -q '|| echo "  WARNING: Kafka is NOT readable' \
  && ok "  a broker that will not start falls through to the interlock (which fails closed), not around it" \
  || bad "  the broker-start failure path is not the documented fall-through"
grep -q 'strike_archive_interlock "preflight" *\\$' "$S" && grep -q 'strike_archive_interlock "before the wipe" *\\$' "$S" \
  && grep -A1 'strike_archive_interlock "preflight"' "$S" | grep -q '|| die' \
  && grep -A1 'strike_archive_interlock "before the wipe"' "$S" | grep -q '|| die' \
  && ok "  both interlock calls die on refusal" || bad "  a refusal is not fatal at one of the call sites"
bash -n "$S" && ok "  cleanup-es4.sh still parses" || bad "  cleanup-es4.sh has a syntax error"

echo "10. wiring: Jenkins can only turn it off through the explicit parameter"
J="$ROOT/Jenkinsfile.es4-deploy"
grep -q "booleanParam(name: 'ACCEPT_UNARCHIVED_STRIKE_LOSS', defaultValue: false" "$J" \
  && ok "  ACCEPT_UNARCHIVED_STRIKE_LOSS exists and defaults to false" || bad "  the opt-out parameter is missing or not default-false"
[ "$(grep -c 'ES4_STRIKE_ARCHIVE_INTERLOCK=\$STRIKE_INTERLOCK bash /home/es4/repo/scripts/es4/cleanup-es4.sh' "$J")" = 2 ] \
  && ok "  both the DRY and the real on-box invocation pass the interlock mode" || bad "  an on-box cleanup invocation does not pass ES4_STRIKE_ARCHIVE_INTERLOCK"
grep -q 'if \[ "${ACCEPT_UNARCHIVED_STRIKE_LOSS:-false}" = "true" \]; then STRIKE_INTERLOCK=off; fi' "$J" \
  && grep -q '^ *STRIKE_INTERLOCK=on$' "$J" \
  && ok "  the mode is 'on' unless that parameter is ticked" || bad "  the mode is not derived from the parameter as expected"

echo "11. END TO END: the REAL cleanup-es4.sh resuming an interrupted reset (finding 4)"
# The script is run from a copy of scripts/es4 in which exactly two lines differ: ES4_HOME and KAFKA_DATA point
# into this test's directory. Everything else — the preflight, the state-file resume, the quiesce proof, the
# broker start, the interlock, compose down and the data-dir delete — is the committed code, against the mocks
# above (sudo/k3s/docker; the Kafka CLIs answer as in the sections above). A run that passes the interlock
# stops with 42 at the datastore start AFTER the wipe (the mock fails it), so every assertion is about what
# happened before and at the wipe.
E2E="$W/e2e"; EH="$W/home"
cp -R "$ROOT/scripts/es4" "$E2E"
awk -v h="$EH" '{
  if ($0 == "ES4_HOME=/home/es4") { print "ES4_HOME=" h; next }
  if ($0 == "KAFKA_DATA=/home/es4/volumes/kafka") { print "KAFKA_DATA=" h "/volumes/kafka"; next }
  print }' "$ROOT/scripts/es4/cleanup-es4.sh" > "$E2E/cleanup-es4.sh"
[ "$(diff "$ROOT/scripts/es4/cleanup-es4.sh" "$E2E/cleanup-es4.sh" | grep -c '^>')" = 2 ] \
  && grep -qx "ES4_HOME=$EH" "$E2E/cleanup-es4.sh" && grep -qx "KAFKA_DATA=$EH/volumes/kafka" "$E2E/cleanup-es4.sh" \
  && ok "the copy differs from the committed script in exactly the two path lines" \
  || { bad "the path rewrite did not apply exactly (the end-to-end cases would test something else)"; diff "$ROOT/scripts/es4/cleanup-es4.sh" "$E2E/cleanup-es4.sh" | head; }
# The pre-fix wiring, for contrast: the same copy WITHOUT the broker start.
awk '{ if ($0 ~ /^  strike_archive_broker_readable "\$INFRA_DIR" \\$/) { print "  : \\"; next } print }' "$E2E/cleanup-es4.sh" > "$E2E/cleanup-es4.no-ready.sh"
[ "$(diff "$E2E/cleanup-es4.sh" "$E2E/cleanup-es4.no-ready.sh" | grep -c '^>')" = 1 ] \
  && ok "the contrast copy differs only in the broker-start line" || bad "the contrast copy did not neutralise exactly one line"
home() { # [resume] — a Kafka data dir holding a strike segment, and (resume) a WIPING state file
  rm -rf "$EH"; mkdir -p "$EH/infra" "$EH/volumes/kafka/$T-0"
  echo "services: {}" > "$EH/infra/docker-compose.yml"
  echo segment > "$EH/volumes/kafka/$T-0/00000000000000000000.log"
  [ "${1:-}" = resume ] && printf 'WIPING\nsvc-a 2\nsvc-b 1\n' > "$EH/.es4-cleanup.state"
  return 0
}
e2e() { # <script> [env...]
  local s="$1"; shift
  OUT=$(env PATH="$MOCK:$PATH" E2E_KAFKA_DATA="$EH/volumes/kafka" ES4_EXPECTED_NS_UID=11111111-2222-3333-4444-555555555555 \
        ES4_FEED_FENCED=1 DEPLOY_DRY_RUN=false ES4_KAFKA_HEALTH_SLEEP=0 ES4_KAFKA_HEALTH_POLLS=3 "$@" bash "$s" 2>&1)
  RC=$?
}
wiped()   { [ -z "$(ls -A "$EH/volumes/kafka" 2>/dev/null)" ] && echo wiped || echo intact; }
e2e_rc()  { if [ "$RC" = "$1" ]; then ok "$LABEL -> rc $RC"; else bad "$LABEL -> rc $RC, wanted $1"; printf '%s\n' "$OUT" | sed 's/^/      | /' | tail -30; fi; }

case_ "resume, Kafka STOPPED, strike archived through the log end"
home resume; echo absent > "$FIX/kafka_state"; echo healthy > "$FIX/after_up"; world 1234 1234
e2e "$E2E/cleanup-es4.sh"
e2e_rc 42
says "resuming interrupted reset (phase=WIPING)"
says "starting ONLY the kafka service"
says "every protected record is archived — the wipe may proceed"
f_up=$(first "docker compose up -d --no-deps kafka"); f_list=$(first "topics --bootstrap-server localhost:29092 --list")
f_down=$(first "docker compose down"); f_rm=$(first "sudo find $EH/volumes/kafka")
[ -n "$f_up" ] && [ -n "$f_list" ] && [ -n "$f_down" ] && [ -n "$f_rm" ] && [ "$f_up" -lt "$f_list" ] && [ "$f_list" -lt "$f_down" ] && [ "$f_down" -lt "$f_rm" ] \
  && ok "  Kafka started -> log read -> compose down -> data dir deleted, in that order" \
  || bad "  order wrong or missing (up=$f_up list=$f_list down=$f_down rm=$f_rm)"
[ "$(grep -c 'docker compose up' "$FIX/calls" | tr -d ' ')" = 2 ] && [ "$(grep -c 'scale deploy/svc-a --replicas=0' "$FIX/calls")" -ge 1 ] \
  && [ "$(first 'scale deploy/svc-a --replicas=0')" -lt "$f_up" ] \
  && ok "  producers were scaled to 0 BEFORE Kafka started, and the only other 'up' is the post-wipe datastore start" \
  || bad "  unexpected start order: $(grep -nE 'compose up|replicas=0' "$FIX/calls" | tr '\n' '|')"
[ "$(wiped)" = wiped ] && ok "  the Kafka data dir was wiped" || bad "  the data dir was not wiped"
[ "$(head -1 "$EH/.es4-cleanup.state")" = WIPING ] && grep -qx 'svc-a 2' "$EH/.es4-cleanup.state" \
  && ok "  the state file still holds phase WIPING and the ORIGINAL replica counts" || bad "  the state file changed: $(tr '\n' '|' < "$EH/.es4-cleanup.state")"

case_ "resume, Kafka STOPPED, strike archive SHORT of the log end"
home resume; echo absent > "$FIX/kafka_state"; echo healthy > "$FIX/after_up"; world 1234 1100
e2e "$E2E/cleanup-es4.sh"
e2e_rc 1
says "archived through 1100, log end 1234 — 134 offset(s) not archived"
says "Kafka is up so the archive can read it"
[ "$(calls 'docker compose down')" = 0 ] && [ "$(wiped)" = intact ] && ok "  no compose down, the data dir is intact" || bad "  it went on to the wipe after refusing"
[ "$(cat "$FIX/kafka_state")" = healthy ] && ok "  Kafka is left RUNNING, so the prescribed archive run can read it" || bad "  Kafka is not left up"
[ "$(head -1 "$EH/.es4-cleanup.state")" = WIPING ] && ok "  the state file still says WIPING (the reset stays resumable)" || bad "  the state file was lost"
LABEL="  ... the archive then runs (marker reaches 1234) and the reset is rerun"
describe "$T" 0 1234 1234 > "$FIX/groups.out"; : > "$FIX/calls"
e2e "$E2E/cleanup-es4.sh"
e2e_rc 42
[ "$(calls 'docker compose up -d --no-deps kafka')" = 0 ] && ok "  (Kafka was already up: nothing started)" || bad "  started Kafka again although it was healthy"
[ "$(wiped)" = wiped ] && ok "  the rerun resumed and wiped" || bad "  the rerun did not wipe"

case_ "resume, Kafka will NOT start"
home resume; echo absent > "$FIX/kafka_state"; touch "$FIX/compose_up_fails"; world 1234 1234
e2e "$E2E/cleanup-es4.sh"
e2e_rc 1
says "WARNING: Kafka is NOT readable"
says "the broker's topic list cannot be read"
[ "$(calls 'docker compose down')" = 0 ] && [ "$(wiped)" = intact ] && ok "  refused: no compose down, data intact" || bad "  wiped with an unreadable broker"
LABEL="  ... the same with ES4_STRIKE_ARCHIVE_INTERLOCK=off"
: > "$FIX/calls"
e2e "$E2E/cleanup-es4.sh" ES4_STRIKE_ARCHIVE_INTERLOCK=off
e2e_rc 42
says "ACCEPTED LOSS"
[ "$(wiped)" = wiped ] && ok "  the explicit opt-out wipes, and says what it could not prove" || bad "  the opt-out did not proceed"

case_ "a FRESH reset with Kafka healthy"
home; echo healthy > "$FIX/kafka_state"; world 1234 1234
e2e "$E2E/cleanup-es4.sh"
e2e_rc 42
[ "$(calls 'topics --bootstrap-server localhost:29092 --list')" = 2 ] && ok "  the interlock ran twice (preflight, and before the wipe)" || bad "  expected 2 interlock reads, got $(calls 'topics --bootstrap-server localhost:29092 --list')"
[ "$(calls 'docker compose up -d --no-deps kafka')" = 0 ] && ok "  and started nothing before the wipe" || bad "  started Kafka on a fresh run"

case_ "CONTRAST — the round-1 wiring (no broker start), resume with Kafka STOPPED"
home resume; echo absent > "$FIX/kafka_state"; world 1234 1234
e2e "$E2E/cleanup-es4.no-ready.sh"
e2e_rc 1
says "the broker's topic list cannot be read"
[ "$(calls 'docker compose up')" = 0 ] && [ "$(wiped)" = intact ] \
  && ok "  it refuses with nothing started — and would on every retry: the reviewer's finding 4, reproduced" || bad "  the contrast did not reproduce the finding"

echo
if [ "$fails" -eq 0 ]; then echo "=== strike-archive-interlock: OK ==="; exit 0; fi
echo "=== strike-archive-interlock: $fails problem(s) ===" >&2; exit 1
