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
# So this script watches the only signal that cannot lie: whether committed offsets ADVANCE while
# the source they read from is itself moving. Remediation escalates and is remembered across runs:
#
#   unblock   abort an abandoned transaction of THIS group on its coordinator partition (no restart)
#   strike 1  rollout restart      — new producer epoch; clears an epoch fight or a wedged rebalance
#   strike 2  empty the state dir  — PVC kept, contents emptied in place, Streams rebuilds from changelog
#   strike 3  stop and shout       — two failed attempts is a real defect, not a transient
#
# The thing that must NEVER happen is restarting a HEALTHY service. So a group is only "stuck" when
# ALL of these hold: it carries real lag, its committed offsets did not move across the sample window,
# its source's log-end offset DID move (a paused/quiet source is not a stuck consumer), it is not
# exempted by name, no sign of life vetoes it (young pod, growing local state, restoration logged),
# and the same verdict repeats on CONFIRM_CYCLES consecutive runs. Restarting a restoring app restarts
# its restoration — an infinite loop, and how the market-carry liveness probe took that service down.
#
# Run from oe-boot-bringup (once, after the partition doctor) and from oe-pipeline-selfheal.timer
# every 10 minutes, because a mid-session crash produces the identical damage. Every external is an
# env override so the decision logic can be exercised against stubs (tests/test_pipeline_selfheal.py).
#
# Fails LOUD and does nothing silently: every decision is logged with the numbers behind it.
set -uo pipefail

KUBECTL="${KUBECTL:-k3s kubectl -n options-edge}"
SA="${SA:---as=system:serviceaccount:options-edge:jenkins-deployer}"
BS="${BS:-localhost:9092}"
KBIN="${KBIN:-/opt/kafka/current/bin}"
LOG="${LOG:-/var/log/oe-pipeline-selfheal.log}"
STATEDIR="${STATEDIR:-/var/lib/oe-selfheal}"
STORAGE="${STORAGE:-/home/options-edge/data/k3s/storage}"

SAMPLE_SECONDS="${SAMPLE_SECONDS:-90}"   # gap between the two offset samples
LAG_FLOOR="${LAG_FLOOR:-2000}"           # below this, a still group is just a quiet topic
MAX_ACTIONS="${MAX_ACTIONS:-3}"          # never roll the whole fleet at once (2026-09-21: 9 at
                                         # once drove load to 32.6 and readiness to 10/54)
LOAD_CEILING="${LOAD_CEILING:-30}"       # 24 cores; above this, remediate nothing this cycle
GRACE_MINUTES="${GRACE_MINUTES:-20}"     # a pod this young is presumed to be still starting up
CONFIRM_CYCLES="${CONFIRM_CYCLES:-3}"    # consecutive stuck verdicts (10 min apart) before ANY action:
                                         # 30 minutes without a commit against a moving source. No
                                         # Streams app on this estate commits less often than that; one
                                         # that legitimately does belongs in EXEMPT_GROUPS, not here.
EXEMPT_GROUPS="${EXEMPT_GROUPS:-}"       # space-separated group ids this script must never judge
                                         # (standby replicas, deliberately paused consumers)
STALE_TX_MINUTES="${STALE_TX_MINUTES:-15}"  # an open transaction idle this long is abandoned, not in flight
POD_GONE_WAIT_SECONDS="${POD_GONE_WAIT_SECONDS:-300}"  # strike 2: how long to wait for the old pod to leave
DRY_RUN="${DRY_RUN:-false}"

mkdir -p "$STATEDIR"
log() { printf '[%s] %s\n' "$(date '+%F %T %Z')" "$*" | tee -a "$LOG"; }
run() { if [ "$DRY_RUN" = true ]; then log "DRY: $*"; return 0; fi; "$@"; }
# A dry run that WRITES is not a dry run. On 2026-09-21 two DRY_RUN passes silently advanced two
# groups to strike 2, so the first real run opened at strike 3 and declared healthy services defective.
remember() { [ "$DRY_RUN" = true ] || printf '%s\n' "$2" > "$1"; }
forget()   { [ "$DRY_RUN" = true ] || rm -f "$1" 2>/dev/null; }

# ---------- one instance at a time, and a scaled-down deployment is never left behind ----------
# oe-boot-bringup calls this script DIRECTLY while the timer may also fire it, and systemd's
# one-instance-per-unit rule does not cover that path. The second arrival leaves at once rather than
# queueing behind a lock for minutes (a queued run under TimeoutStartSec would be killed mid-way).
# flock is util-linux: where it is absent the run continues and SAYS so — it must not fail closed
# (indistinguishable from "another run is active", i.e. never running again) nor fail silently.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$STATEDIR/.lock"
  if ! flock -n 9; then echo "another self-heal run is active — leaving"; exit 0; fi
else
  echo "WARN: flock is not installed — concurrent runs are NOT prevented on this host"
fi

# Strike 2 scales a deployment to 0 for the wipe. If this process dies in between (timeout, SIGTERM,
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
trap 'restore_down_markers' EXIT

log "=== self-heal start (load $(awk '{print $1}' /proc/loadavg 2>/dev/null || echo '?'), uptime $(uptime -p 2>/dev/null || true)) ==="

# ---------- gate: Kafka must ANSWER and the k3s API must be up ----------
# Everything below reads Kafka and k3s. If either is still coming up there is nothing to diagnose
# yet and a "stuck" verdict would be a false positive, so leave quietly and let the timer retry.
if ! timeout 30 "$KBIN/kafka-broker-api-versions.sh" --bootstrap-server "$BS" >/dev/null 2>&1; then
  log "Kafka is not answering on $BS yet — nothing to diagnose; will retry next cycle"; exit 0
fi
if ! timeout 30 $KUBECTL get deploy --no-headers >/dev/null 2>&1; then
  log "k3s API is not answering yet — nothing to diagnose; will retry next cycle"; exit 0
fi
restore_down_markers

# ---------- CLI output is parsed by COLUMN NAME, never by position ----------
# `tail -n +2` / NR>1 would silently drop a real row whenever the header is absent, and silently read
# the wrong column whenever it moves. The header is located, the needed columns are resolved from it,
# and output that does not carry the expected header is REFUSED (logged, treated as no rows) rather
# than guessed at. Usage: columns "<text>" "ColA" "ColB" ... -> prints the selected columns per row.
columns() {
  local text="$1"; shift
  printf '%s\n' "$text" | awk -F'\t' -v want="$*" '
    BEGIN { n = split(want, w, " ") }
    !hdr { for (i = 1; i <= NF; i++) { gsub(/^ +| +$/, "", $i); col[$i] = i }
           ok = 1; for (k = 1; k <= n; k++) if (!(w[k] in col)) ok = 0
           if (ok) { hdr = 1; next } else { next } }
    { out = ""; for (k = 1; k <= n; k++) { v = $col[w[k]]; gsub(/^ +| +$/, "", v); out = out (k > 1 ? " " : "") v }
      if (out != "") print out }
    END { if (!hdr) exit 3 }'
}

# ---------- phase 1: hanging transactions on data partitions ----------
# A hanging transaction blocks read_committed consumers on that partition FOREVER and keeps every
# producer that inherits the id in an epoch fight. Aborting is safe: the records were never
# committed, so no committed data is lost — what is discarded is exactly the half-written batch
# the power cut interrupted.
raw=$(timeout 180 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" find-hanging --broker-id 1 --max-transaction-timeout 60 2>/dev/null)
hanging=$(columns "$raw" Topic Partition ProducerId StartOffset); hrc=$?
if [ "$hrc" -eq 3 ]; then
  log "find-hanging output carried no recognisable header — REFUSING to parse it; nothing aborted this cycle"
elif [ -n "$hanging" ]; then
  log "HANGING TRANSACTIONS: $(printf '%s\n' "$hanging" | wc -l | tr -d ' ') found — aborting (uncommitted batches only; no committed data is lost)"
  while read -r topic partition producerId startOffset; do
    [ -n "$topic" ] || continue
    log "  abort topic=$topic partition=$partition producerId=$producerId startOffset=$startOffset"
    run timeout 60 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" abort \
        --topic "$topic" --partition "$partition" --start-offset "$startOffset" 2>&1 | tee -a "$LOG"
  done <<<"$hanging"
else
  log "hanging transactions: none"
fi

# ---------- phase 1b: the abandoned offset-commit transactions find-hanging does NOT report ----------
# Only the stuck group's OWN coordinator partition is read (fifty describe-producers calls take
# minutes), and on it only a transaction that is (a) owned by THIS group — its producerId maps, via
# `kafka-transactions.sh list`, to a transactional.id that starts with the group id, which is how a
# Streams application names its producers — and (b) idle for STALE_TX_MINUTES is aborted. Other
# groups share that partition; their in-flight work is never touched. Kafka's partition choice is
# reproduced exactly: Utils.abs(groupId.hashCode()) % partitionCount, where Utils.abs is the
# bit-mask form (h & 0x7fffffff), NOT Math.abs — they differ for Integer.MIN_VALUE.
coordinator_partition() {
  local n="$1"
  python3 -c '
import sys
h = 0
for ch in sys.argv[1]:
    h = (31 * h + ord(ch)) & 0xFFFFFFFF
print((h & 0x7FFFFFFF) % int(sys.argv[2]))' "$2" "$n"
}
offsets_partitions() {
  timeout 60 "$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic __consumer_offsets 2>/dev/null \
    | grep -oE 'PartitionCount:[[:space:]]*[0-9]+' | grep -oE '[0-9]+' | head -1
}
ABORTED_MARKER="$(mktemp)"; trap 'rm -f "$ABORTED_MARKER"; restore_down_markers' EXIT
unblock_group_offsets() {   # $1 = group; appends a line to $ABORTED_MARKER for each VERIFIED abort
  local g="$1" nparts part rows owned now_ms out
  : > "$ABORTED_MARKER"
  nparts=$(offsets_partitions); [ -n "$nparts" ] || { log "  $g: could not read the __consumer_offsets partition count — no abort attempted"; return 0; }
  part=$(coordinator_partition "$nparts" "$g" 2>/dev/null) || return 0
  [ -n "$part" ] || return 0
  # producerIds that belong to this group (transactional.id = "<application.id>-<process>-<thread>")
  owned=$(timeout 120 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" list 2>/dev/null)
  owned=$(columns "$owned" TransactionalId ProducerId | awk -v g="$g-" 'index($1,g)==1 {print $2}')
  [ -n "$owned" ] || { log "  $g: no transactional producer of this group is known to the coordinator — no abort attempted"; return 0; }
  now_ms=$(( $(date +%s) * 1000 ))
  rows=$(timeout 120 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" describe-producers --topic __consumer_offsets --partition "$part" 2>/dev/null)
  rows=$(columns "$rows" ProducerId LastTimestamp CurrentTransactionStartOffset); rc=$?
  [ "$rc" -eq 3 ] && { log "  $g: describe-producers output carried no recognisable header — REFUSING to parse it"; return 0; }
  rows=$(printf '%s\n' "$rows" | awk -v now="$now_ms" -v stale="$STALE_TX_MINUTES" -v owned="$owned" '
    BEGIN { n = split(owned, o, "\n"); for (i = 1; i <= n; i++) mine[o[i]] = 1 }
    ($1 in mine) && $3 != "None" && (now - $2) > stale * 60000 { print $1, $2, $3 }')
  [ -z "$rows" ] && return 0
  while read -r pid last start; do
    [ -n "$pid" ] || continue
    log "  $g: ABANDONED transaction of its own producer $pid on __consumer_offsets-$part (idle $(( (now_ms-last)/60000 ))m) blocks its committed-offset fetch — aborting"
    if [ "$DRY_RUN" = true ]; then log "DRY: abort __consumer_offsets-$part start-offset $start"; continue; fi
    out=$(timeout 120 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" abort --topic __consumer_offsets --partition "$part" --start-offset "$start" 2>&1); rc=$?
    [ -n "$out" ] && log "  $out"
    # Only a CHECKED success counts. A marker written before the attempt (the first cut of this)
    # would have suppressed every later remediation for as long as the abort kept failing.
    if [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -qiE "could not find|error|exception|failed"; then
      echo "$pid" >> "$ABORTED_MARKER"
    else
      log "  $g: abort did NOT succeed (rc=$rc) — the escalation path stays open"
    fi
  done <<<"$rows"
  return 0
}

# ---------- phase 2: which groups actually MOVED, against a source that itself moved ----------
# The pod's own opinion of its health is what failed on 2026-09-21, so it is not consulted here.
# Per group, two samples of (sum of committed offsets, sum of log-end offsets, sum of lag). Only a
# group whose committed sum stayed flat WHILE its log-end sum advanced, with lag above the floor,
# is a candidate: a consumer of a paused or quiet topic has nothing to commit and is left alone.
sample() {
  timeout 300 "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BS" --describe --all-groups 2>/dev/null \
  | awk 'NF>=6 && $1!="GROUP" && $4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {cur[$1]+=$4; end[$1]+=$5; lag[$1]+=$6}
         END{for (g in cur) printf "%s %d %d %d\n", g, cur[g], end[g], lag[g]}'
}
# Growing local state = restoration in progress. This is the only sign of life that works for an app
# that logs NOTHING (databento-volume-aggregator ships a no-op SLF4J binder and printed 7 lines all
# day on 2026-09-21), so it is sampled for every service, not only the quiet ones.
# -sk, not -sb: --apparent-size/-b is GNU-only, and on a host without it every size comes back
# empty, which turns this veto into a silent no-op — the failure mode where a guard looks present
# and protects nothing.
statesizes() { du -sk "$STORAGE"/*_options-edge_*-streams-state 2>/dev/null | awk '{print $2" "$1}'; }

log "sampling committed offsets (t0)"; s0=$(sample); d0=$(statesizes)
[ -z "$s0" ] && { log "no consumer groups reported offsets — nothing to judge"; exit 0; }
sleep "$SAMPLE_SECONDS"
log "sampling committed offsets (t1, +${SAMPLE_SECONDS}s)"; s1=$(sample); d1=$(statesizes)

stuck=$(awk -v floor="$LAG_FLOOR" -v exempt=" $EXEMPT_GROUPS " '
  NR==FNR {c0[$1]=$2; e0[$1]=$3; next}
  ($1 in c0) && index(exempt, " " $1 " ") == 0 && $4 >= floor && $2 <= c0[$1] && $3 > e0[$1] {
    printf "%s %d %d %d\n", $1, $4, $2-c0[$1], $3-e0[$1] }
' <(echo "$s0") <(echo "$s1"))
quiet=$(awk -v floor="$LAG_FLOOR" '
  NR==FNR {c0[$1]=$2; e0[$1]=$3; next}
  ($1 in c0) && $4 >= floor && $2 <= c0[$1] && $3 <= e0[$1] { print $1 }
' <(echo "$s0") <(echo "$s1"))
[ -n "$quiet" ] && log "not judged (lag but the SOURCE did not move either — a paused or quiet topic, not a stuck consumer): $(echo $quiet | tr '\n' ' ')"

if [ -z "$stuck" ]; then
  log "every group with lag on a moving source advanced — pipeline is moving"
  [ "$DRY_RUN" = true ] || find "$STATEDIR" -maxdepth 1 \( -name '*.strikes' -o -name '*.observed' \) -delete 2>/dev/null
  log "=== self-heal done: nothing to do ==="
  exit 0
fi
# A group that recovered must lose its history even while OTHERS are still stuck, or a group that
# was stuck an hour ago carries a strike into an unrelated incident next week.
if [ "$DRY_RUN" != true ]; then
  for sf in "$STATEDIR"/*.strikes "$STATEDIR"/*.observed; do
    [ -e "$sf" ] || continue
    gname=$(basename "$sf"); gname=${gname%.strikes}; gname=${gname%.observed}
    echo "$stuck" | awk -v g="$gname" '$1==g{f=1} END{exit !f}' || { log "  $gname recovered — clearing its history"; rm -f "$sf"; }
  done
fi
log "NOT ADVANCING (lag >= $LAG_FLOOR, zero commit progress in ${SAMPLE_SECONDS}s while the source moved):"
echo "$stuck" | while read -r g lag delta srcdelta; do log "  $g lag=$lag committed+$delta log-end+$srcdelta"; done

# ---------- phase 3: escalating remediation ----------
# Above the load ceiling, restarting things is what makes the box unreachable rather than what
# fixes it (measured 2026-08-17: saturation stopped sshd, the k3s API and Kafka answering at all).
load=$(awk '{print int($1)}' /proc/loadavg 2>/dev/null || echo 0)
if [ "$load" -ge "$LOAD_CEILING" ]; then
  log "load $load >= ceiling $LOAD_CEILING — remediating NOTHING this cycle; retrying next timer"
  exit 0
fi

# group -> deployment. The Streams application.id on this estate is the service name wrapped in a
# namespace prefix and an environment suffix: options-edge-unified-sr-prod -> unified-sr-service,
# options-flow-databento-volume-aggregator-prod -> databento-volume-aggregator. Unwrapping it is a
# guess, so a guess is only ACTED on when exactly ONE deployment matches: an ambiguous core name
# ("gex") must never pick a service by sort order and restart the wrong one.
deploys=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{print $1}')
resolve() {
  local g="$1" core hits
  core=${g%-prod}; core=${core%-dev}
  core=${core#options-edge-}; core=${core#options-flow-}
  hits=$(echo "$deploys" | awk -v c="$core" '
      $1==c                       {print; next}          # exact
      $1==c"-service"             {print; next}          # core + the conventional suffix
      index($1,c)==1              {print; next}          # deployment extends the core
      index(c,$1)==1 && length($1)>8 {print}')           # core extends the deployment
  hits=$(echo "$hits" | awk 'NF' | sort -u)
  case "$(echo "$hits" | awk 'NF' | wc -l | tr -d ' ')" in
    1) echo "$hits" ;;
    0) echo "" ;;
    *) echo "AMBIGUOUS $(echo $hits | tr '\n' ',')" ;;
  esac
}
# The DESIRED replica count, from spec — never the table's READY column, whose first number is the
# ready count and reads 0 for a crash-looping deployment that is very much meant to be up.
desired() { $KUBECTL get deploy "$1" -o jsonpath='{.spec.replicas}' 2>/dev/null; }
pods_of() { $KUBECTL get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | awk -v d="$1-" 'index($1,d)==1'; }
# The state directory is resolved through Kubernetes — deployment -> claim -> bound PV -> its host
# path — so the wipe hits the volume this deployment is ACTUALLY mounting. A name glob over the
# storage dir would pick the lexically first of two stale directories that share a claim name.
state_dir_of() {
  local dep="$1" claim pv path
  claim=$($KUBECTL get deploy "$dep" -o jsonpath='{range .spec.template.spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null | grep -- '-streams-state$' | head -1)
  [ -n "$claim" ] || return 1
  pv=$($KUBECTL get pvc "$claim" -o jsonpath='{.spec.volumeName}' 2>/dev/null); [ -n "$pv" ] || return 1
  path=$($KUBECTL get pv "$pv" -o jsonpath='{.spec.local.path}{.spec.hostPath.path}' 2>/dev/null)
  case "$path" in "$STORAGE"/*) [ -d "$path" ] && printf '%s\n' "$path" && return 0 ;; esac
  return 1
}

acted=0
while read -r g lag delta srcdelta; do
  [ -z "$g" ] && continue
  [ "$acted" -ge "$MAX_ACTIONS" ] && { log "  $g: MAX_ACTIONS=$MAX_ACTIONS reached — left for the next cycle"; continue; }
  read -r dep rest <<<"$(resolve "$g")"
  if [ "${dep:-}" = "AMBIGUOUS" ]; then log "  $g: matches more than one deployment ($rest) — NOT touched; name the mapping explicitly before this can be automatic"; continue; fi
  if [ -z "${dep:-}" ]; then log "  $g: no deployment matches this group — NOT touched (external or renamed consumer)"; continue; fi
  reps=$(desired "$dep")
  if [ "${reps:-0}" = "0" ]; then log "  $g -> $dep is at 0 replicas (held down by decision) — NOT started"; continue; fi

  # ---- signs of life: any ONE of these means "working, just not committing yet" ----
  alive=""
  age=$($KUBECTL get pods --no-headers 2>/dev/null | awk -v d="$dep-" 'index($1,d)==1 {print $NF; exit}')
  agemin=$(echo "${age:-0}" | awk '{ s=0; t=$0
      while (match(t,/^[0-9]+[dhms]/)) { v=substr(t,RSTART,RLENGTH); u=substr(v,length(v)); n=substr(v,1,length(v)-1)
        if(u=="d") s+=n*1440; else if(u=="h") s+=n*60; else if(u=="m") s+=n; else s+=n/60
        t=substr(t,RSTART+RLENGTH) } print int(s) }')
  [ -n "$age" ] && [ "${agemin:-999}" -lt "$GRACE_MINUTES" ] && alive="pod is only ${agemin}m old (grace ${GRACE_MINUTES}m)"

  if [ -z "$alive" ]; then
    dir=$(state_dir_of "$dep" 2>/dev/null || true)
    if [ -n "$dir" ]; then
      b0=$(echo "$d0" | awk -v k="$dir" '$1==k{print $2}'); b1=$(echo "$d1" | awk -v k="$dir" '$1==k{print $2}')
      [ -n "${b0:-}" ] && [ -n "${b1:-}" ] && [ "$b1" -gt "$b0" ] && \
        alive="local state grew $((b1-b0)) KiB in ${SAMPLE_SECONDS}s (restoring)"
    fi
  fi

  if [ -z "$alive" ]; then
    pod=$($KUBECTL get pods --no-headers 2>/dev/null | awk -v d="$dep-" 'index($1,d)==1 && $3=="Running"{print $1; exit}')
    if [ -n "$pod" ] && $KUBECTL logs "$pod" --since=3m 2>/dev/null | grep -qiE "restor(ing|ed|ation)"; then
      alive="logged changelog restoration within the last 3 minutes"
    fi
  fi

  if [ -n "$alive" ]; then
    log "  $g -> $dep: NOT stuck — $alive. Left alone."
    forget "$STATEDIR/${g}.strikes"; forget "$STATEDIR/${g}.observed"
    continue
  fi

  # ---- clear the blocker before reaching for a restart ----
  # Aborting the transaction that is holding this group fixes it WITHOUT bouncing the service, so
  # it is always tried first; after a VERIFIED abort the strike is skipped for a cycle to let the
  # group prove it recovered. A failed or absent abort leaves the escalation path open.
  unblock_group_offsets "$g"
  if [ -s "$ABORTED_MARKER" ]; then
    log "  $g -> $dep: transaction aborted; not restarting this cycle — next check decides"
    continue
  fi

  # ---- one bad sample must never be enough to act ----
  # A transient (a rebalance, a slow commit, a restore that ended between samples) clears by itself;
  # a wedge does not. The SAME verdict on CONFIRM_CYCLES consecutive runs is required.
  o="$STATEDIR/${g}.observed"; seen=$(cat "$o" 2>/dev/null || echo 0); seen=$((seen+1))
  remember "$o" "$seen"
  if [ "$seen" -lt "$CONFIRM_CYCLES" ]; then
    log "  $g -> $dep: stuck on $seen of $CONFIRM_CYCLES consecutive checks — confirming before acting"
    continue
  fi

  f="$STATEDIR/${g}.strikes"; strikes=$(cat "$f" 2>/dev/null || echo 0); strikes=$((strikes+1))
  remember "$f" "$strikes"

  case "$strikes" in
    1)
      why=""
      lp=$($KUBECTL get pods --no-headers 2>/dev/null | awk -v d="$dep-" 'index($1,d)==1 && $3=="Running"{print $1; exit}')
      [ -n "$lp" ] && why=$($KUBECTL logs "$lp" --since=10m 2>/dev/null | grep -oE "trying to initialize transactions|OutOfOrderSequence|InvalidProducerEpoch|TaskCorrupted|Invalid state during store open|poll timeout has expired" | sort -u | tr '\n' ',')
      log "  $g -> $dep STRIKE 1: rollout restart (new producer epoch; clears an epoch fight or a wedged rebalance)${why:+ — signature: $why}"
      run $KUBECTL $SA rollout restart "deploy/$dep" 2>&1 | tee -a "$LOG"
      acted=$((acted+1)) ;;
    2)
      # The PVC is kept; only its CONTENTS go. Streams rebuilds the store from the changelog, which
      # is the whole point of the changelog. (rm -rf is forbidden on this estate — find -delete.)
      # Every step is CHECKED: the wipe runs only after a successful scale-down AND zero pods of the
      # deployment remain; a failed scale-back is retried, then shouted, and the .down marker keeps
      # the next run (or the exit trap) restoring it. The deployment goes back to its DESIRED count.
      dir=$(state_dir_of "$dep" 2>/dev/null || true)
      if [ -z "$dir" ]; then
        log "  $g -> $dep STRIKE 2: no streams-state volume resolves through its PVC — repeating the restart instead"
        run $KUBECTL $SA rollout restart "deploy/$dep" 2>&1 | tee -a "$LOG"; acted=$((acted+1)); continue
      fi
      log "  $g -> $dep STRIKE 2: local state is inconsistent with the changelog — scaling $reps->0, emptying $dir, scaling back to $reps"
      remember "$STATEDIR/${dep}.down" "$reps"
      if ! run $KUBECTL $SA scale "deploy/$dep" --replicas=0 >>"$LOG" 2>&1; then
        log "  $g -> $dep: scale to 0 FAILED — nothing wiped; strike stands"; forget "$STATEDIR/${dep}.down"; acted=$((acted+1)); continue
      fi
      gone=false; waited=0
      while [ "$waited" -le "$POD_GONE_WAIT_SECONDS" ]; do
        [ -z "$(pods_of "$dep")" ] && { gone=true; break; }
        [ "$DRY_RUN" = true ] && { gone=true; break; }
        sleep 5; waited=$((waited+5))
      done
      if [ "$gone" != true ]; then
        log "  $g -> $dep: a pod is still present after ${POD_GONE_WAIT_SECONDS}s — NOT emptying the state dir under a live pod"
      elif ! run find "$dir" -mindepth 1 -delete; then
        log "  $g -> $dep: emptying $dir FAILED (partial wipe possible) — scaling back regardless; investigate the volume"
      else
        log "  $g -> $dep: state dir emptied"
      fi
      restored=false
      for i in 1 2 3; do
        run $KUBECTL $SA scale "deploy/$dep" --replicas="$reps" >>"$LOG" 2>&1 && { restored=true; break; }; sleep 10
      done
      if [ "$restored" = true ]; then forget "$STATEDIR/${dep}.down"; log "  $g -> $dep: scaled back to $reps"
      else log "  $g -> $dep: SCALE BACK TO $reps FAILED THREE TIMES — deployment is at 0; marker kept, every later run retries until it succeeds. Scale it by hand: $KUBECTL $SA scale deploy/$dep --replicas=$reps"; fi
      acted=$((acted+1)) ;;
    *)
      log "  $g -> $dep STRIKE $strikes: a restart AND a state wipe both failed — this is a DEFECT, not a transient. Not touching it again; investigate $dep."
      ;;
  esac
done <<<"$stuck"

log "=== self-heal done: $acted action(s) taken ==="
