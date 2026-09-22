#!/usr/bin/env bash
# Self-heal the OptionsEdge pipeline after an unclean stop (power cut, hard reset, OOM kill).
#
# WHY (measured 2026-09-21): prod lost power at 17:18 CEST. Every systemd unit came back on its
# own — kafka, k3s, docker and oe-boot-bringup all started and the fleet scaled up — and yet no
# data flowed for hours, because the damage was INSIDE Kafka and inside the Streams state, where
# systemd and Kubernetes cannot see it:
#
#   1. three ABANDONED transactions on __consumer_offsets, left by producers that died in the cut.
#      A Streams client fetching its committed offsets under read_committed cannot see past one:
#      it retries forever inside ConsumerCoordinator.fetchCommittedOffsets, the group stays
#      "Stable", the pod stays READY, and it consumes nothing. `kafka-transactions.sh find-hanging`
#      reports NONE of them (verified over a 48-hour window); only describe-producers on the
#      group's coordinator partition shows them. databento-volume-aggregator — the sole producer of
#      options.databento.normalized, the trunk of everything downstream — sat like this for hours;
#      aborting the three transactions drained 2.3M lag in sixty seconds. Restarts had not helped.
#   2. RocksDB stores on the streams-state PVCs were killed mid-write, so their checkpoints no
#      longer matched the changelogs (unified-sr crash-looped on "Invalid state during store open").
#   3. those pods reported READY the whole time. The probes check the HTTP port, not whether the
#      Streams client is RUNNING — so Kubernetes never restarted them and nothing alerted.
#      "All pods ready, zero records produced" is the exact shape of this failure.
#
# The one rule above every other: NEVER restart a healthy service. Absence of progress is not
# evidence of a wedge — a slow, paused, batching or rebalancing consumer looks identical by the
# offsets alone. So nothing here acts on absence, and nothing acts on a single observation either.
# A stalled group is only a CANDIDATE; every action needs POSITIVE, ATTRIBUTED, PERSISTENT evidence:
#
#   wedge      in a thread dump taken NOW (`kill -3 1`, read back from the pod's stdout within the
#              last minute — not a historical log line) a StreamThread of THIS deployment's pod is
#              parked in a RETRY, not merely inside a call: fetchCommittedOffsets with a sleep frame
#              in the same stack (the coordinator retry loop), or initTransactions together with
#              repeated "Timeout exception ... initialize transactions"/"Reattempting initialization"
#              lines in the last ten minutes. A live transaction that happens to be in flight when
#              the dump lands has neither. And the SAME finding must recur on the next cycle.
#   corruption an "Invalid state during store open" / TaskCorruptedException / ProcessorStateException
#              in THIS container's log, on two consecutive cycles.
#   dead owner for the abort: a transactional.id that is EXACTLY "<group>-<processUUID>-<n>", whose
#              process UUID is absent from the group's members on two consecutive cycles while the
#              group reports Stable (a rebalance hides live members, so a rebalancing group is never
#              judged), plus the wedge above, plus STALE_TX_MINUTES of silence.
#
#   the abort   fixes the offset wedge without touching the service
#   strike 1    rollout restart      — needs the wedge
#   strike 2    reset the state dir  — needs the corruption signature; PVC kept, the directory is
#               SWAPPED for an empty one atomically (rename aside, recreate) so no process can ever
#               observe a half-deleted tree, and the old tree is deleted only after every pod that
#               could have bound it is gone
#   otherwise   the stall is logged loudly and NOTHING is touched. A fault this script cannot name
#               is a human's call, not a restart.
#
# The residual, stated so it is a decision and not a surprise: a service that commits nothing for
# CONFIRM_CYCLES+WEDGE_CYCLES cycles (an hour at the defaults) against a moving source, parked in the
# same coordinator retry loop on WEDGE_CYCLES consecutive dumps, while that coordinator answers a
# state query normally, is restarted. A latency incident long and one-sided enough to look like that
# is not distinguishable from a wedge by any signal this host can read, and the restart is the
# mildest action taken here.
#
# Group → deployment is an exact transformation or an explicit mapping, never a fuzzy match; pods
# are the deployment's own, through ReplicaSet ownerReferences, never by name prefix.
#
# Run from oe-boot-bringup (asynchronously, via the unit) and from oe-pipeline-selfheal.timer every
# 10 minutes, because a mid-session crash produces the identical damage. Every external is an env
# override so the decision logic is exercised against stubs (tests/test_pipeline_selfheal.py).
#
# Fails LOUD and does nothing silently: every decision is logged with the evidence behind it.
set -uo pipefail

KUBECTL="${KUBECTL:-k3s kubectl -n options-edge}"
SA="${SA:---as=system:serviceaccount:options-edge:jenkins-deployer}"
BS="${BS:-localhost:9092}"
KBIN="${KBIN:-/opt/kafka/current/bin}"
LOG="${LOG:-/var/log/oe-pipeline-selfheal.log}"
STATEDIR="${STATEDIR:-/var/lib/oe-selfheal}"
STORAGE="${STORAGE:-/home/options-edge/data/k3s/storage}"
GROUP_MAP="${GROUP_MAP:-/etc/oe-selfheal/groups.map}"   # optional "group deployment" lines

SAMPLE_SECONDS="${SAMPLE_SECONDS:-90}"   # gap between the two offset samples
LAG_FLOOR="${LAG_FLOOR:-2000}"           # below this, a still group is just a quiet topic
MAX_ACTIONS="${MAX_ACTIONS:-3}"          # never roll the whole fleet at once (2026-09-21: 9 at
                                         # once drove load to 32.6 and readiness to 10/54); also
                                         # bounds the thread dumps and coordinator reads per cycle
LOAD_CEILING="${LOAD_CEILING:-30}"       # 24 cores; above this, remediate nothing this cycle
GRACE_MINUTES="${GRACE_MINUTES:-20}"     # a pod this young is presumed to be still starting up
CONFIRM_CYCLES="${CONFIRM_CYCLES:-3}"    # consecutive stalled verdicts before evidence is even gathered
EXEMPT_GROUPS="${EXEMPT_GROUPS:-}"       # space-separated group ids this script must never judge
STALE_TX_MINUTES="${STALE_TX_MINUTES:-15}"  # extra guard on top of "owner process is dead"
EVIDENCE_MIN_SECONDS="${EVIDENCE_MIN_SECONDS:-300}"    # two pieces of evidence closer than this are one observation
EVIDENCE_MAX_SECONDS="${EVIDENCE_MAX_SECONDS:-3600}"   # older than this, the earlier evidence is stale
WEDGE_CYCLES="${WEDGE_CYCLES:-3}"        # consecutive cycles the SAME park must be seen on before a restart:
                                         # with CONFIRM_CYCLES this is an hour of zero commits against a
                                         # moving source while parked in the same retry loop
RETRY_LINES_MIN="${RETRY_LINES_MIN:-2}"  # initTransactions counts as a wedge only with this many retry lines in 10 min
RX_ALIVE_KIB="${RX_ALIVE_KIB:-512}"      # bytes a pod must RECEIVE across the sample window to count as fetching:
                                         # measured 2026-09-22, a live consumer of a small topic pulls ~1.3 MiB
                                         # per 30 s and a busy one ~14 MiB; a parked one only heartbeats
POD_GONE_WAIT_SECONDS="${POD_GONE_WAIT_SECONDS:-300}"  # strike 2: how long to wait for the old pod to leave
DUMP_SETTLE_SECONDS="${DUMP_SETTLE_SECONDS:-3}"        # kill -3 to stdout latency
CLI_TIMEOUT="${CLI_TIMEOUT:-60}"         # per Kafka CLI call; MAX_ACTIONS bounds how many are made
DRY_RUN="${DRY_RUN:-false}"

UUID_RE='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
SLEEP_RE='Thread\.sleep|Utils\.sleep|Timer\.sleep|SystemTime\.sleep'
RETRY_RE='Timeout exception caught trying to initialize transactions|Reattempting initialization'
CORRUPT_RE='Invalid state during store open|TaskCorruptedException|ProcessorStateException'

mkdir -p "$STATEDIR"
log() { printf '[%s] %s\n' "$(date '+%F %T %Z')" "$*" | tee -a "$LOG"; }
run() { if [ "$DRY_RUN" = true ]; then log "DRY: $*"; return 0; fi; "$@"; }
# A dry run that WRITES is not a dry run. On 2026-09-21 two DRY_RUN passes silently advanced two
# groups to strike 2, so the first real run opened at strike 3 and declared healthy services defective.
remember() { [ "$DRY_RUN" = true ] || printf '%s\n' "$2" > "$1"; }
forget()   { [ "$DRY_RUN" = true ] || rm -f "$1" 2>/dev/null; }
now_s()    { date +%s; }

# ---------- exactly one instance, without util-linux ----------
# A destructive remediator must not run twice at once, and must not depend on flock being installed
# to guarantee that. The lock is a SYMLINK whose target is the owner's pid: `ln -s` is one atomic
# system call, so there is no instant at which a lock exists without its owner recorded (a mkdir
# followed by a pid write had exactly that gap, and a second run could take over in it). A lock
# whose owner is no longer alive is stale; contenders do not delete it — they RENAME it away, and
# rename is atomic too, so only one contender can succeed and go on to claim the lock.
LOCK="$STATEDIR/.lock"
take_lock() {
  local owner attempt
  for attempt in 1 2 3; do
    ln -s "$$" "$LOCK" 2>/dev/null && return 0
    owner=$(readlink "$LOCK" 2>/dev/null || echo "")
    if [ -n "$owner" ] && kill -0 "$owner" 2>/dev/null; then return 1; fi
    echo "stale lock from pid ${owner:-?} — taking over"
    mv "$LOCK" "$LOCK.stale.$$" 2>/dev/null && rm -f "$LOCK.stale.$$"
  done
  return 1
}
take_lock || { echo "another self-heal run is active (pid $(readlink "$LOCK" 2>/dev/null)) — leaving"; exit 0; }
release_lock() { [ "$(readlink "$LOCK" 2>/dev/null)" = "$$" ] && rm -f "$LOCK"; return 0; }

# Strike 2 scales a deployment to 0 for the reset. If this process dies in between (timeout, SIGTERM,
# a crash), the deployment must not stay at 0: the intended replica count is written to a .down
# marker BEFORE the scale-down, the trap restores from it on any exit, and the next run restores any
# marker it finds before doing anything else. Only a verified restore removes the marker.
restore_down_markers() {
  local m dep reps
  for m in "$STATEDIR"/*.down; do
    [ -e "$m" ] || continue
    dep=$(basename "$m" .down); reps=$(cat "$m" 2>/dev/null || echo 1)
    log "RESTORE: $dep was left at 0 replicas by an interrupted state reset — scaling back to $reps"
    if run $KUBECTL $SA scale "deploy/$dep" --replicas="$reps" >>"$LOG" 2>&1; then rm -f "$m"
    else log "RESTORE FAILED: $dep is still at 0 replicas — scale it by hand: $KUBECTL $SA scale deploy/$dep --replicas=$reps"; fi
  done
}
trap 'restore_down_markers; release_lock' EXIT

log "=== self-heal start (load $(awk '{print $1}' /proc/loadavg 2>/dev/null || echo '?'), uptime $(uptime -p 2>/dev/null || true)) ==="

# ---------- gate: Kafka must ANSWER and the k3s API must be up ----------
if ! timeout 30 "$KBIN/kafka-broker-api-versions.sh" --bootstrap-server "$BS" >/dev/null 2>&1; then
  log "Kafka is not answering on $BS yet — nothing to diagnose; will retry next cycle"; exit 0
fi
if ! timeout 30 $KUBECTL get deploy --no-headers >/dev/null 2>&1; then
  log "k3s API is not answering yet — nothing to diagnose; will retry next cycle"; exit 0
fi
restore_down_markers

# ---------- CLI output is parsed by COLUMN NAME, never by position ----------
# `tail -n +2` / NR>1 would silently drop a real row whenever the header is absent, and silently read
# the wrong column whenever it moves. The header is located by the names it must carry, the columns
# are resolved from it, and output without that header is REFUSED (exit 3 → caller logs, no rows).
columns() {
  local text="$1"; shift
  printf '%s\n' "$text" | awk -F'\t' -v want="$*" '
    BEGIN { n = split(want, w, " ") }
    !hdr { for (i = 1; i <= NF; i++) { gsub(/^ +| +$/, "", $i); col[$i] = i }
           ok = 1; for (k = 1; k <= n; k++) if (!(w[k] in col)) ok = 0
           if (ok) hdr = 1; next }
    { out = ""; for (k = 1; k <= n; k++) { v = $col[w[k]]; gsub(/^ +| +$/, "", v); out = out (k > 1 ? " " : "") v }
      if (out != "") print out }
    END { if (!hdr) exit 3 }'
}

# ---------- phase 1: hanging transactions on data partitions — REPORTED, not aborted ----------
# `find-hanging --max-transaction-timeout N` is an assertion about the largest transaction.timeout.ms
# any producer is configured with, not a fact about the transaction it lists: a healthy producer with
# a long timeout that is legitimately busy for a few minutes is "hanging" by that definition, and an
# abort would discard its live work. So nothing listed here is aborted on the strength of the listing.
# The rows are logged, and a row is acted on only later, through the same proof the coordinator
# partition gets — a stuck group, its own transactional producer, its process gone from a Stable
# group on two consecutive cycles (abort_dead_data_transactions).
HANGING_ROWS=""
raw=$(timeout 180 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" find-hanging --broker-id 1 --max-transaction-timeout 60 2>&1); frc=$?
if [ "$frc" -ne 0 ]; then
  log "find-hanging FAILED (rc=$frc): $(printf '%s' "$raw" | tail -1) — cannot list data-partition transactions this cycle"
else
  HANGING_ROWS=$(columns "$raw" Topic Partition ProducerId StartOffset); hrc=$?
  if [ "$hrc" -eq 3 ]; then
    HANGING_ROWS=""; log "find-hanging output carried no recognisable header — REFUSING to parse it"
  elif [ -n "$HANGING_ROWS" ]; then
    log "find-hanging lists $(printf '%s\n' "$HANGING_ROWS" | wc -l | tr -d ' ') open transaction(s) older than its threshold — reported only; an abort needs proof the owner is dead:"
    printf '%s\n' "$HANGING_ROWS" | while read -r topic partition producerId startOffset; do [ -n "$topic" ] && log "  $topic-$partition producerId=$producerId startOffset=$startOffset"; done
  else
    log "find-hanging: nothing listed"
  fi
fi

# ---------- the deployment's OWN pods, through ownerReferences ----------
# A pod belongs to a deployment only through a ReplicaSet the deployment owns. A name prefix does
# not: "foo-canary-<hash>-<id>" starts with "foo-", and evidence read from it would restart "foo".
# Output per pod: "<name> <phase> <creationTimestamp>".
deployment_pods() {
  local dep="$1" rss
  rss=$($KUBECTL get rs -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.ownerReferences[0].kind}{"/"}{.metadata.ownerReferences[0].name}{"\n"}{end}' 2>/dev/null \
        | awk -v d="Deployment/$dep" '$2==d {print $1}')
  [ -n "$rss" ] || return 0
  $KUBECTL get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.ownerReferences[0].kind}{"/"}{.metadata.ownerReferences[0].name}{" "}{.status.phase}{" "}{.metadata.creationTimestamp}{"\n"}{end}' 2>/dev/null \
    | awk -v rss="$rss" 'BEGIN { n = split(rss, r, "\n"); for (i = 1; i <= n; i++) own["ReplicaSet/" r[i]] = 1 } ($2 in own) { print $1, $3, $4 }'
}
running_pod() { deployment_pods "$1" | awk '$2=="Running" {print $1; exit}'; }
pods_of()     { deployment_pods "$1" | awk '{print $1}'; }
age_minutes() {   # $1 = RFC3339 creationTimestamp
  python3 -c 'import sys,datetime; t=datetime.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")); print(int((datetime.datetime.now(datetime.timezone.utc)-t).total_seconds()//60))' "$1" 2>/dev/null || echo 999
}

# ---------- evidence readers ----------
coordinator_partition() {   # Kafka's Utils.abs(hashCode) % n — the bit-mask abs, not Math.abs
  python3 -c '
import sys
h = 0
for ch in sys.argv[1]:
    h = (31 * h + ord(ch)) & 0xFFFFFFFF
print((h & 0x7FFFFFFF) % int(sys.argv[2]))' "$2" "$1"
}
offsets_partitions() {
  timeout "$CLI_TIMEOUT" "$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic __consumer_offsets 2>/dev/null \
    | grep -oE 'PartitionCount:[[:space:]]*[0-9]+' | grep -oE '[0-9]+' | head -1
}
# The consumer-group tables are whitespace-ALIGNED, not delimited, and whether they carry a leading
# GROUP column depends on the Kafka version. So a column is read by the character span its header
# occupies — from the header word's start to the next header word's start — never by position.
under_header() {   # $1 = text, $2 = header word  -> the value under that header for each data row
  printf '%s\n' "$1" | awk -v h="$2" '
    !found && index($0, h) > 0 {
      found = 1; start = index($0, h); rest = substr($0, start + length(h))
      if (match(rest, /[^ ]/)) { end = start + length(h) + RSTART - 1 } else { end = 0 }
      next }
    found && NF { v = (end ? substr($0, start, end - start) : substr($0, start)); gsub(/^ +| +$/, "", v); if (v != "") print v }'
}
group_state() {
  under_header "$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BS" --describe --group "$1" --state 2>/dev/null)" STATE | head -1
}
live_processes() {   # process UUIDs of the group's members (a Streams member id carries its process UUID)
  under_header "$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BS" --describe --group "$1" --members 2>/dev/null)" CONSUMER-ID \
    | grep -oE "$UUID_RE" | sort -u
}
# ATTRIBUTED evidence of a PARK, not merely of a call: a thread dump is triggered now and read back
# from the last minute of the pod's stdout only. Per thread block whose NAME contains StreamThread,
# the stack frames ("\tat ...") are examined:
#   - fetchCommittedOffsets counts only with a sleep frame in the same block — the coordinator's
#     retry loop sleeps between attempts; a live in-flight fetch does not sleep;
#   - initTransactions counts only with RETRY_LINES_MIN "Timeout exception ... initialize
#     transactions"/"Reattempting initialization" lines in the last ten minutes — a first, live
#     initialisation logs none.
# Prints the confirmed wedge kinds, or nothing.
wedge_frames() {
  local dep="$1" pod parked retries
  pod=$(running_pod "$dep"); [ -n "$pod" ] || return 0
  $KUBECTL exec "$pod" -- kill -3 1 >/dev/null 2>&1 || true
  sleep "$DUMP_SETTLE_SECONDS"
  parked=$($KUBECTL logs "$pod" --since=1m 2>/dev/null | awk -v sleep_re="$SLEEP_RE" '
    function flush() { if (name != "" && name ~ /StreamThread/) {
        if (fetch && slept) print "fetchCommittedOffsets"
        if (init) print "initTransactions" }
      name = ""; fetch = 0; init = 0; slept = 0 }
    /^"/ { flush(); name = $0; next }
    /^[ \t]+at / { if ($0 ~ /fetchCommittedOffsets/) fetch = 1
                   if ($0 ~ /initTransactions|initializeTransactions/) init = 1
                   if ($0 ~ sleep_re) slept = 1 }
    END { flush() }' | sort -u)
  if printf '%s\n' "$parked" | grep -qx initTransactions; then
    retries=$($KUBECTL logs "$pod" --since=10m 2>/dev/null | grep -cE "$RETRY_RE")
    [ "${retries:-0}" -ge "$RETRY_LINES_MIN" ] || parked=$(printf '%s\n' "$parked" | grep -vx initTransactions)
  fi
  printf '%s\n' "$parked" | awk 'NF' | tr '\n' ',' | sed 's/,$//'
}
corruption_lines() {   # in THIS container's log (no --previous), recent
  local dep="$1" pod
  pod=$(running_pod "$dep"); [ -n "$pod" ] || return 0
  $KUBECTL logs "$pod" --since=10m 2>/dev/null | grep -oE "$CORRUPT_RE" | sort -u | tr '\n' ',' | sed 's/,$//'
}
# PERSISTENT evidence: "<what>" observed for <key> now extends a run only if the SAME <what> was
# recorded for <key> on an earlier cycle between EVIDENCE_MIN and EVIDENCE_MAX seconds ago; the
# run is "confirmed" once it is $3 observations long (default 2). Anything else starts a new run.
persist() {   # $1 = file, $2 = what, $3 = cycles needed -> prints "confirmed" or "seen <n>"
  # file format: "<epoch> <count> <what...>" — the text is LAST because it may contain spaces and
  # `read` hands the remainder of the line to its final variable
  local f="$1" what="$2" need="${3:-2}" prev_t prev_w prev_n age n
  read -r prev_t prev_n prev_w < <(cat "$f" 2>/dev/null || echo "0 0 -")
  age=$(( $(now_s) - ${prev_t:-0} ))
  if [ "$prev_w" = "$what" ] && [ "$age" -ge "$EVIDENCE_MIN_SECONDS" ] && [ "$age" -le "$EVIDENCE_MAX_SECONDS" ]; then n=$(( ${prev_n:-1} + 1 )); else n=1; fi
  remember "$f" "$(now_s) $n $what"
  if [ "$n" -ge "$need" ]; then echo confirmed; else echo "seen $n"; fi
}
# A retry loop is only a WEDGE if the coordinator is answering everyone else: during a broker or
# coordinator latency incident every consumer backs off and retries, and restarting one of them
# fixes nothing. If this group's coordinator does not answer a state query within CLI_TIMEOUT, the
# incident is broker-side and nothing is acted on.
coordinator_answers() { [ -n "$(group_state "$1")" ]; }

# ---------- the abort: only THIS group's transaction, only from a DEAD process, only when Stable ----------
ABORTED_MARKER="$(mktemp)"; trap 'rm -f "$ABORTED_MARKER"; restore_down_markers; release_lock' EXIT
PENDING_DEAD=0
unblock_group_offsets() {   # $1 = group; appends a line to $ABORTED_MARKER for each VERIFIED abort
  local g="$1" nparts part rows listing owned live now_ms out rc state verdict
  : > "$ABORTED_MARKER"; PENDING_DEAD=0
  state=$(group_state "$g")
  if [ "$state" != "Stable" ] && [ "$state" != "Empty" ]; then
    log "  $g: group is '$state', not Stable — a rebalance hides live members, so ownership cannot be judged; no abort attempted"; return 0
  fi
  nparts=$(offsets_partitions); [ -n "$nparts" ] || { log "  $g: could not read the __consumer_offsets partition count — no abort attempted"; return 0; }
  part=$(coordinator_partition "$nparts" "$g" 2>/dev/null); [ -n "$part" ] || return 0
  listing=$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" list 2>/dev/null)
  listing=$(columns "$listing" TransactionalId ProducerId); rc=$?
  [ "$rc" -eq 3 ] && { log "  $g: transaction listing carried no recognisable header — REFUSING to parse it; no abort attempted"; return 0; }
  # exact grammar of a Streams producer id: "<application.id>-<processUUID>-<threadIndex>", nothing
  # more — "<group>-canary-<uuid>-<n>" belongs to another application and must not match
  owned=$(printf '%s\n' "$listing" | awk -v g="$g" -v u="$UUID_RE" '$1 ~ ("^" g "-" u "-[0-9]+$") { print $2, $1 }')
  [ -n "$owned" ] || { log "  $g: no transactional producer named for this group — no abort attempted"; return 0; }
  live=$(live_processes "$g")
  now_ms=$(( $(now_s) * 1000 ))
  rows=$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" describe-producers --topic __consumer_offsets --partition "$part" 2>/dev/null)
  rows=$(columns "$rows" ProducerId LastTimestamp CurrentTransactionStartOffset); rc=$?
  [ "$rc" -eq 3 ] && { log "  $g: describe-producers output carried no recognisable header — REFUSING to parse it; no abort attempted"; return 0; }
  rows=$(printf '%s\n' "$rows" | awk -v now="$now_ms" -v stale="$STALE_TX_MINUTES" -v owned="$owned" -v u="$UUID_RE" '
    BEGIN { n = split(owned, o, "\n"); for (i = 1; i <= n; i++) { split(o[i], f, " "); tx[f[1]] = f[2] } }
    ($1 in tx) && $3 != "None" && (now - $2) > stale * 60000 { match(tx[$1], u); print $1, $2, $3, substr(tx[$1], RSTART, RLENGTH) }')
  [ -z "$rows" ] && return 0
  while read -r pid last start proc; do
    [ -n "$pid" ] || continue
    if printf '%s\n' "$live" | grep -qx "$proc"; then
      log "  $g: open transaction of producer $pid is owned by LIVE member process $proc — in flight, not abandoned; NOT aborted"
      forget "$STATEDIR/${g}.dead-$proc"; continue
    fi
    # absent once may be a member mid-rejoin; absent on two cycles, ten minutes apart, with the
    # group Stable both times, is a process that is gone
    verdict=$(persist "$STATEDIR/${g}.dead-$proc" "absent" 2)
    if [ "$verdict" != confirmed ]; then
      log "  $g: producer $pid's process $proc is not a member right now — must still be absent next cycle before it counts as dead; NOT aborted yet"
      PENDING_DEAD=1; continue
    fi
    log "  $g: ABANDONED transaction — producer $pid, process $proc absent from a Stable group on two consecutive cycles, idle $(( (now_ms-last)/60000 ))m, on __consumer_offsets-$part — aborting"
    if [ "$DRY_RUN" = true ]; then log "DRY: abort __consumer_offsets-$part start-offset $start"; continue; fi
    out=$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" abort --topic __consumer_offsets --partition "$part" --start-offset "$start" 2>&1); rc=$?
    [ -n "$out" ] && log "  $out"
    if [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -qiE "could not find|error|exception|failed"; then
      echo "$pid" >> "$ABORTED_MARKER"; forget "$STATEDIR/${g}.dead-$proc"
      abort_dead_data_transactions "$g" "$pid"
    else
      log "  $g: abort did NOT succeed (rc=$rc) — the escalation path stays open"
    fi
  done <<<"$rows"
  return 0
}
# The same dead producer may hold open transactions on DATA partitions (find-hanging listed them);
# with its death proven above, those can go too — and only those: the listing alone never suffices.
abort_dead_data_transactions() {   # $1 = group, $2 = producerId proven dead
  local g="$1" pid="$2" out rc
  [ -n "$HANGING_ROWS" ] || return 0
  printf '%s\n' "$HANGING_ROWS" | awk -v p="$pid" '$3==p' | while read -r topic partition producerId startOffset; do
    [ -n "$topic" ] || continue
    log "  $g: dead producer $pid also holds an open transaction on $topic-$partition — aborting it"
    if [ "$DRY_RUN" = true ]; then log "DRY: abort $topic-$partition start-offset $startOffset"; continue; fi
    out=$(timeout "$CLI_TIMEOUT" "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" abort --topic "$topic" --partition "$partition" --start-offset "$startOffset" 2>&1); rc=$?
    [ -n "$out" ] && log "  $out"
    [ "$rc" -eq 0 ] || log "  $g: abort on $topic-$partition did NOT succeed (rc=$rc)"
  done
}

# ---------- phase 2: candidates — groups that did not commit while their source moved ----------
sample() {
  timeout 300 "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BS" --describe --all-groups 2>/dev/null \
  | awk 'NF>=6 && $1!="GROUP" && $4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {cur[$1]+=$4; end[$1]+=$5; lag[$1]+=$6}
         END{for (g in cur) printf "%s %d %d %d\n", g, cur[g], end[g], lag[g]}'
}
statesizes() { du -sk "$STORAGE"/*_options-edge_*-streams-state 2>/dev/null | awk '{print $2" "$1}'; }
# Bytes RECEIVED by a pod, all interfaces but lo, from its own /proc/net/dev. A consumer that is
# fetching a moving source pulls megabytes per minute; one parked in a retry loop only heartbeats.
# Sampled for every Running pod at t0 and t1 ("<pod> <bytes>" per line).
rxsizes() {
  $KUBECTL get pods --no-headers 2>/dev/null | awk '$3=="Running"{print $1}' | while read -r pod; do
    printf '%s %s\n' "$pod" "$($KUBECTL exec "$pod" -- cat /proc/net/dev 2>/dev/null | awk -F'[: ]+' 'NR>2 && $2!="lo" {s+=$3} END{print s+0}')"
  done
}

# A group that commits rarely can commit BETWEEN two cycles and look flat inside each one. So the
# committed sum at the end of every cycle is remembered, and a group whose committed sum moved since
# the previous cycle is progress, whatever the two samples inside this cycle say.
OFFSETS_MEMO="$STATEDIR/committed.last"
log "sampling committed offsets (t0)"; s0=$(sample); d0=$(statesizes); r0=$(rxsizes)
[ -z "$s0" ] && { log "no consumer groups reported offsets — nothing to judge"; exit 0; }
sleep "$SAMPLE_SECONDS"
log "sampling committed offsets (t1, +${SAMPLE_SECONDS}s)"; s1=$(sample); d1=$(statesizes); r1=$(rxsizes)

memo=$(cat "$OFFSETS_MEMO" 2>/dev/null || true)
stuck=$(awk -v floor="$LAG_FLOOR" -v exempt=" $EXEMPT_GROUPS " -v memo="$memo" '
  BEGIN { n = split(memo, m, "\n"); for (i = 1; i <= n; i++) { split(m[i], f, " "); if (f[1] != "") last[f[1]] = f[2] } }
  NR==FNR {c0[$1]=$2; e0[$1]=$3; next}
  ($1 in c0) && index(exempt, " " $1 " ") == 0 && $4 >= floor && $2 <= c0[$1] && $3 > e0[$1] && !(($1 in last) && $2 > last[$1]) {
    printf "%s %d %d %d\n", $1, $4, $2-c0[$1], $3-e0[$1] }
' <(echo "$s0") <(echo "$s1"))
between=$(awk -v memo="$memo" '
  BEGIN { n = split(memo, m, "\n"); for (i = 1; i <= n; i++) { split(m[i], f, " "); if (f[1] != "") last[f[1]] = f[2] } }
  NR==FNR {c0[$1]=$2; next}
  ($1 in c0) && $2 <= c0[$1] && ($1 in last) && $2 > last[$1] { print $1 }
' <(echo "$s0") <(echo "$s1"))
[ -n "$between" ] && log "committed BETWEEN cycles (flat inside this one, but ahead of last cycle — a slow committer, not a stuck one): $(echo $between | tr '\n' ' ')"
[ "$DRY_RUN" = true ] || printf '%s\n' "$s1" | awk '{print $1, $2}' > "$OFFSETS_MEMO"
quiet=$(awk -v floor="$LAG_FLOOR" '
  NR==FNR {c0[$1]=$2; e0[$1]=$3; next}
  ($1 in c0) && $4 >= floor && $2 <= c0[$1] && $3 <= e0[$1] { print $1 }
' <(echo "$s0") <(echo "$s1"))
[ -n "$quiet" ] && log "not judged (lag but the SOURCE did not move either — a paused or quiet topic, not a stuck consumer): $(echo $quiet | tr '\n' ' ')"

if [ -z "$stuck" ]; then
  log "every group with lag on a moving source advanced — pipeline is moving"
  [ "$DRY_RUN" = true ] || find "$STATEDIR" -maxdepth 1 \( -name '*.strikes' -o -name '*.observed' -o -name '*.wedge' -o -name '*.corrupt' -o -name '*.dead-*' \) -delete 2>/dev/null
  log "=== self-heal done: nothing to do ==="
  exit 0
fi
if [ "$DRY_RUN" != true ]; then
  for sf in "$STATEDIR"/*.strikes "$STATEDIR"/*.observed "$STATEDIR"/*.wedge "$STATEDIR"/*.corrupt "$STATEDIR"/*.dead-*; do
    [ -e "$sf" ] || continue
    gname=$(basename "$sf"); gname=${gname%%.strikes}; gname=${gname%%.observed}; gname=${gname%%.wedge}; gname=${gname%%.corrupt}; gname=${gname%%.dead-*}
    echo "$stuck" | awk -v g="$gname" '$1==g{f=1} END{exit !f}' || { log "  $gname recovered — clearing its history"; rm -f "$sf"; }
  done
fi
log "STALLED (lag >= $LAG_FLOOR, no commit in ${SAMPLE_SECONDS}s while the source moved) — candidates only, evidence decides:"
echo "$stuck" | while read -r g lag delta srcdelta; do log "  $g lag=$lag committed+$delta log-end+$srcdelta"; done

# ---------- phase 3: evidence, then escalation ----------
load=$(awk '{print int($1)}' /proc/loadavg 2>/dev/null || echo 0)
if [ "$load" -ge "$LOAD_CEILING" ]; then
  log "load $load >= ceiling $LOAD_CEILING — remediating NOTHING this cycle; retrying next timer"
  exit 0
fi

deploys=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{print $1}')
# group -> deployment by EXACT transformation only: strip the namespace prefix (options-edge-,
# options-flow-) and the environment suffix (-prod, -dev); the result must equal a deployment name
# or that name + "-service". Anything else needs an explicit line in GROUP_MAP ("group deployment")
# or it resolves to nothing and is left alone. No prefix, substring or extension matching: a group
# that does not follow the convention must never pick a neighbour by resemblance.
resolve() {
  local g="$1" core mapped
  if [ -r "$GROUP_MAP" ]; then
    mapped=$(awk -v g="$g" '$1==g && NF>=2 {print $2; exit}' "$GROUP_MAP")
    if [ -n "$mapped" ]; then echo "$deploys" | grep -qx "$mapped" && { echo "$mapped"; return; }; echo ""; return; fi
  fi
  core=${g%-prod}; core=${core%-dev}
  core=${core#options-edge-}; core=${core#options-flow-}
  if echo "$deploys" | grep -qx "$core"; then echo "$core"; return; fi
  if echo "$deploys" | grep -qx "$core-service"; then echo "$core-service"; return; fi
  echo ""
}
desired() { $KUBECTL get deploy "$1" -o jsonpath='{.spec.replicas}' 2>/dev/null; }
canon()   { python3 -c 'import os,sys; p=os.path.realpath(sys.argv[1]); sys.exit(1) if not os.path.isdir(p) else print(p)' "$1" 2>/dev/null; }
hpa_on()  { $KUBECTL get hpa -o jsonpath='{range .items[*]}{.spec.scaleTargetRef.name}{"\n"}{end}' 2>/dev/null | grep -qx "$1"; }
# Anything ELSE that mounts, or is templated to mount, the same claim: a ReadWriteOnce local volume
# is node-scoped, so on this single node a second pod can hold it while the target's pods are gone.
claim_of()  { $KUBECTL get deploy "$1" -o jsonpath='{range .spec.template.spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null | grep -- '-streams-state$' | head -1; }
other_users_of_claim() {   # $1 = claim, $2 = the deployment allowed to own it
  local claim="$1" dep="$2" own
  own=$(pods_of "$dep" | tr '\n' ' ')
  $KUBECTL get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{" "}{end}{"\n"}{end}' 2>/dev/null \
    | awk -v c="$claim" -v own=" $own " '{ for (i = 2; i <= NF; i++) if ($i == c && index(own, " " $1 " ") == 0) print "pod/" $1 }'
  $KUBECTL get deploy,sts -o jsonpath='{range .items[*]}{.kind}{"/"}{.metadata.name}{" "}{range .spec.template.spec.volumes[*]}{.persistentVolumeClaim.claimName}{" "}{end}{"\n"}{end}' 2>/dev/null \
    | awk -v c="$claim" -v d="Deployment/$dep" '{ for (i = 2; i <= NF; i++) if ($i == c && $1 != d) print $1 }'
}
same_filesystem() { python3 -c 'import os,sys; sys.exit(0 if os.stat(sys.argv[1]).st_dev == os.stat(os.path.dirname(sys.argv[1].rstrip("/"))).st_dev else 1)' "$1" 2>/dev/null; }
# deployment -> claim -> bound PV -> host path, CANONICALISED and required to live under the
# canonical STORAGE root; exactly one streams-state claim, or nothing (fail closed).
state_dir_of() {
  local dep="$1" claims claim pv path root
  claims=$($KUBECTL get deploy "$dep" -o jsonpath='{range .spec.template.spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null | grep -- '-streams-state$')
  [ "$(printf '%s\n' "$claims" | awk 'NF' | wc -l | tr -d ' ')" = 1 ] || return 1
  claim=$(printf '%s\n' "$claims" | awk 'NF')
  pv=$($KUBECTL get pvc "$claim" -o jsonpath='{.spec.volumeName}' 2>/dev/null); [ -n "$pv" ] || return 1
  path=$($KUBECTL get pv "$pv" -o jsonpath='{.spec.local.path}{.spec.hostPath.path}' 2>/dev/null); [ -n "$path" ] || return 1
  root=$(canon "$STORAGE") || return 1
  path=$(canon "$path") || return 1
  case "$path" in "$root"/?*) [ -d "$path" ] && printf '%s\n' "$path" && return 0 ;; esac
  return 1
}
# The reset itself: the directory is RENAMED aside and an empty one with the same mode and owner
# is created in its place — one atomic rename, so at no instant does the live path hold a
# half-deleted tree. A pod that starts during this window binds either the old tree (whole, and the
# corrupt one it already had) or the new empty one; never a partial. The old tree is deleted only
# afterwards, and only once every pod that could have bound it is gone. (rm -rf is forbidden here.)
swap_state_dir() {   # $1 = dir -> prints the aside path; on failure the live path is restored
  local dir="$1" aside="$1.reset-$(now_s)"
  mv "$dir" "$aside" || return 1
  if ! mkdir "$dir"; then
    # the live path must never be left ABSENT: put the old tree back where it was
    if mv "$aside" "$dir"; then log "  swap of $dir rolled back: the empty directory could not be created; the old tree is back in place"
    else log "  CRITICAL: $dir is ABSENT — the old tree is at $aside and could not be moved back; restore it by hand: mv $aside $dir"; fi
    return 1
  fi
  if ! python3 -c 'import os,sys; s=os.stat(sys.argv[1]); os.chmod(sys.argv[2], s.st_mode & 0o7777); os.chown(sys.argv[2], s.st_uid, s.st_gid)' "$aside" "$dir" 2>/dev/null; then
    # the app must not come back to a directory it cannot write: undo the swap
    rmdir "$dir" 2>/dev/null
    if mv "$aside" "$dir"; then log "  swap of $dir rolled back: the empty directory could not be given the old mode/owner; the old tree is back in place"
    else log "  CRITICAL: $dir is ABSENT — the old tree is at $aside and could not be moved back; restore it by hand: mv $aside $dir"; fi
    return 1
  fi
  printf '%s\n' "$aside"
}

acted=0
while read -r g lag delta srcdelta; do
  [ -z "$g" ] && continue
  [ "$acted" -ge "$MAX_ACTIONS" ] && { log "  $g: MAX_ACTIONS=$MAX_ACTIONS reached — left for the next cycle"; continue; }
  dep=$(resolve "$g")
  if [ -z "$dep" ]; then log "  $g: resolves to no deployment by exact transformation and has no GROUP_MAP entry — NOT touched (add a line to $GROUP_MAP if this group should be judged)"; continue; fi
  reps=$(desired "$dep")
  if [ "${reps:-0}" = "0" ]; then log "  $g -> $dep is at 0 replicas (held down by decision) — NOT started"; continue; fi

  # ---- signs of life: any ONE means "working, just not committing yet" ----
  alive=""
  created=$(deployment_pods "$dep" | awk '{print $3; exit}')
  if [ -n "$created" ]; then
    agemin=$(age_minutes "$created")
    [ "${agemin:-999}" -lt "$GRACE_MINUTES" ] && alive="pod is only ${agemin}m old (grace ${GRACE_MINUTES}m)"
  fi
  if [ -z "$alive" ]; then
    dir=$(state_dir_of "$dep" 2>/dev/null || true)
    if [ -n "$dir" ]; then
      b0=$(echo "$d0" | awk -v k="$dir" '$1==k{print $2}'); b1=$(echo "$d1" | awk -v k="$dir" '$1==k{print $2}')
      [ -n "${b0:-}" ] && [ -n "${b1:-}" ] && [ "$b1" -gt "$b0" ] && alive="local state grew $((b1-b0)) KiB in ${SAMPLE_SECONDS}s (restoring)"
    fi
  fi
  if [ -z "$alive" ]; then
    pod=$(running_pod "$dep")
    [ -n "$pod" ] && $KUBECTL logs "$pod" --since=3m 2>/dev/null | grep -qiE "restor(ing|ed|ation)" && alive="logged changelog restoration within the last 3 minutes"
  fi
  if [ -z "$alive" ] && [ -n "${pod:-}" ]; then
    x0=$(echo "$r0" | awk -v k="$pod" '$1==k{print $2}'); x1=$(echo "$r1" | awk -v k="$pod" '$1==k{print $2}')
    if [ -n "${x0:-}" ] && [ -n "${x1:-}" ] && [ $(( (x1-x0)/1024 )) -ge "$RX_ALIVE_KIB" ]; then
      alive="pod received $(( (x1-x0)/1024 )) KiB in ${SAMPLE_SECONDS}s (fetching; a parked consumer only heartbeats)"
    fi
  fi
  if [ -n "$alive" ]; then
    log "  $g -> $dep: NOT stuck — $alive. Left alone."
    forget "$STATEDIR/${g}.strikes"; forget "$STATEDIR/${g}.observed"; forget "$STATEDIR/${g}.wedge"; forget "$STATEDIR/${g}.corrupt"
    continue
  fi

  # ---- a stall must repeat before any evidence is even gathered ----
  o="$STATEDIR/${g}.observed"; seen=$(cat "$o" 2>/dev/null || echo 0); seen=$((seen+1))
  remember "$o" "$seen"
  if [ "$seen" -lt "$CONFIRM_CYCLES" ]; then
    log "  $g -> $dep: stalled on $seen of $CONFIRM_CYCLES consecutive checks — confirming before looking closer"
    continue
  fi

  # ---- POSITIVE, ATTRIBUTED, PERSISTENT evidence — or nothing happens ----
  acted=$((acted+1))   # the evidence gathering below costs a thread dump and CLI calls; MAX_ACTIONS bounds it
  frames=$(wedge_frames "$dep"); corrupt=$(corruption_lines "$dep")
  wedge=""; corruption=""
  if [ -n "$frames" ]; then
    verdict=$(persist "$STATEDIR/${g}.wedge" "$frames" "$WEDGE_CYCLES")
    case "$verdict" in
      confirmed) wedge="$frames" ;;
      *) log "  $g -> $dep: a StreamThread is parked in a $frames retry RIGHT NOW ($verdict of $WEDGE_CYCLES) — a retry loop is only a wedge if it outlasts every backoff; must show the same next cycle" ;;
    esac
  else forget "$STATEDIR/${g}.wedge"; fi
  if [ -n "$corrupt" ]; then
    case "$(persist "$STATEDIR/${g}.corrupt" "$corrupt" 2)" in
      confirmed) corruption="$corrupt" ;;
      *) log "  $g -> $dep: container log shows $corrupt — must repeat next cycle before it counts" ;;
    esac
  else forget "$STATEDIR/${g}.corrupt"; fi
  if [ -z "$wedge" ] && [ -z "$corruption" ]; then
    [ -z "$frames" ] && [ -z "$corrupt" ] && log "  $g -> $dep: stalled $seen checks but no StreamThread is parked in a retry and the container log is clean — slow or paused, not wedged. NOT touched. If this is a real fault it needs a human: $KUBECTL logs $(running_pod "$dep")"
    continue
  fi
  if [ -n "$wedge" ] && ! coordinator_answers "$g"; then
    log "  $g -> $dep: parked in $wedge but the group's COORDINATOR is not answering — a broker-side incident, not a wedge in this service. NOT touched."
    continue
  fi
  log "  $g -> $dep: CONFIRMED evidence —${wedge:+ wedge: $wedge (${WEDGE_CYCLES} consecutive cycles)}${corruption:+ corruption: $corruption (2 cycles)}"

  # ---- the abort comes first: it fixes the offset wedge WITHOUT bouncing the service ----
  PENDING_DEAD=0
  case ",$wedge," in *fetchCommittedOffsets*)
    unblock_group_offsets "$g"
    if [ -s "$ABORTED_MARKER" ]; then log "  $g -> $dep: transaction aborted; not restarting this cycle — next check decides"; forget "$STATEDIR/${g}.wedge"; continue; fi
    # a restart now would replace the very process whose absence is being confirmed, and reset the
    # wedge evidence with it; the abort is the gentler fix, so it gets its confirming cycle first
    if [ "$PENDING_DEAD" = 1 ]; then log "  $g -> $dep: an abandoned-transaction verdict is pending — holding the restart for one cycle"; continue; fi ;;
  esac

  f="$STATEDIR/${g}.strikes"; strikes=$(cat "$f" 2>/dev/null || echo 0); strikes=$((strikes+1))
  # corruption without a wedge does not call for a restart: the restart is skipped and the evidence
  # goes straight to the state check
  if [ "$strikes" = 1 ] && [ -z "$wedge" ]; then
    log "  $g -> $dep STRIKE 1 skipped: corruption without a wedge — a restart is not what the evidence calls for; state check instead"
    strikes=2
  fi
  remember "$f" "$strikes"
  case "$strikes" in
    1)
      log "  $g -> $dep STRIKE 1: rollout restart (new producer epoch; clears an epoch fight or a wedged rebalance) — evidence: $wedge"
      run $KUBECTL $SA rollout restart "deploy/$dep" 2>&1 | tee -a "$LOG"; forget "$STATEDIR/${g}.wedge" ;;
    2)
      if [ -z "$corruption" ]; then
        log "  $g -> $dep STRIKE 2 withheld: a restart did not help and there is NO confirmed state-corruption signature — a state reset would not be justified by evidence. Not touched again; investigate $dep."
        continue
      fi
      if hpa_on "$dep"; then log "  $g -> $dep STRIKE 2 withheld: an HPA targets this deployment — a state reset cannot be made safe beside an autoscaler"; continue; fi
      dir=$(state_dir_of "$dep" 2>/dev/null || true)
      if [ -z "$dir" ]; then log "  $g -> $dep STRIKE 2: no single streams-state volume resolves through its PVC under $STORAGE — NOT resetting anything"; continue; fi
      others=$(other_users_of_claim "$(claim_of "$dep")" "$dep" | sort -u | tr '\n' ' ')
      if [ -n "$others" ]; then log "  $g -> $dep STRIKE 2 withheld: the claim is also mounted or templated by $others — the volume is not this deployment's alone; NOT resetting"; continue; fi
      if ! same_filesystem "$dir"; then log "  $g -> $dep STRIKE 2 withheld: $dir is a mount point of its own — NOT resetting a mounted filesystem"; continue; fi
      log "  $g -> $dep STRIKE 2: confirmed state-corruption signature ($corruption) — scaling $reps->0, swapping $dir for an empty directory, scaling back to $reps"
      remember "$STATEDIR/${dep}.down" "$reps"
      if ! run $KUBECTL $SA scale "deploy/$dep" --replicas=0 >>"$LOG" 2>&1; then
        log "  $g -> $dep: scale to 0 FAILED — nothing reset; strike stands"; forget "$STATEDIR/${dep}.down"; continue
      fi
      gone=false; waited=0
      while [ "$waited" -le "$POD_GONE_WAIT_SECONDS" ]; do
        [ -z "$(pods_of "$dep")" ] && { gone=true; break; }
        [ "$DRY_RUN" = true ] && { gone=true; break; }
        sleep 5; waited=$((waited+5))
      done
      if [ "$gone" != true ]; then
        log "  $g -> $dep: a pod is still present after ${POD_GONE_WAIT_SECONDS}s — NOT resetting the state dir under a live pod"
      elif [ "$DRY_RUN" != true ] && { [ "$(desired "$dep")" != 0 ] || [ -n "$(pods_of "$dep")" ]; }; then
        log "  $g -> $dep: something scaled the deployment back up between the check and the reset — NOT resetting"
      elif [ "$DRY_RUN" != true ] && [ -n "$(other_users_of_claim "$(claim_of "$dep")" "$dep")" ]; then
        log "  $g -> $dep: another workload took the claim during the wait — NOT resetting"
      elif [ "$DRY_RUN" = true ]; then
        log "DRY: swap $dir aside and recreate it empty"
      else
        aside=$(swap_state_dir "$dir")
        if [ -z "$aside" ]; then
          log "  $g -> $dep: swapping $dir aside FAILED — see the line above for what is on disk; scaling back"
        else
          log "  $g -> $dep: $dir is now empty; old tree parked at $aside"
          # anything that bound the OLD tree during the swap window is stopped, and waited for,
          # before that tree is deleted — the deletion never runs under a live process
          intruder=$(pods_of "$dep")
          if [ -n "$intruder" ]; then
            log "  $g -> $dep: CRITICAL — pod(s) $(echo $intruder | tr '\n' ' ') appeared DURING the reset: something other than this script scales this deployment. Stopping them before the old tree is removed."
            for p in $intruder; do run $KUBECTL $SA delete pod "$p" --wait=true --timeout=120s >>"$LOG" 2>&1; done
          fi
          if [ -n "$(pods_of "$dep")" ]; then
            log "  $g -> $dep: a pod is STILL present — the old tree stays parked at $aside for a human; not deleting under it"
          elif find "$aside" -xdev -mindepth 1 -delete && rmdir "$aside"; then
            log "  $g -> $dep: old tree removed"
          else
            log "  $g -> $dep: removing the old tree at $aside FAILED (partial) — the live directory is unaffected; clean it up by hand"
          fi
        fi
      fi
      restored=false
      for i in 1 2 3; do
        run $KUBECTL $SA scale "deploy/$dep" --replicas="$reps" >>"$LOG" 2>&1 && { restored=true; break; }; sleep 10
      done
      if [ "$restored" = true ]; then forget "$STATEDIR/${dep}.down"; forget "$STATEDIR/${g}.corrupt"; log "  $g -> $dep: scaled back to $reps"
      else log "  $g -> $dep: SCALE BACK TO $reps FAILED THREE TIMES — deployment is at 0; marker kept, every later run retries until it succeeds. Scale it by hand: $KUBECTL $SA scale deploy/$dep --replicas=$reps"; fi ;;
    *)
      log "  $g -> $dep STRIKE $strikes: a restart AND a state reset both failed — this is a DEFECT, not a transient. Not touching it again; investigate $dep." ;;
  esac
done <<<"$stuck"

log "=== self-heal done: $acted candidate(s) examined ==="
