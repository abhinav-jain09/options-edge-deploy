#!/usr/bin/env bash
# Self-heal the OptionsEdge pipeline after an unclean stop (power cut, hard reset, OOM kill).
#
# WHY (measured 2026-09-21): prod lost power at 17:18 CEST. Every systemd unit came back on its
# own — kafka, k3s, docker and oe-boot-bringup all started and the fleet scaled up — and yet no
# data flowed for hours, because the damage was INSIDE Kafka and inside the Streams state, where
# systemd and Kubernetes cannot see it:
#
#   1. producers killed mid-transaction left HANGING transactions on the coordinator; every
#      restarted producer then looped on OutOfOrderSequenceException / InvalidProducerEpoch.
#      This cannot self-heal: it needs the transaction aborted or a new producer epoch.
#   2. RocksDB stores on the streams-state PVCs were killed mid-write, so their checkpoints no
#      longer matched the changelogs. unified-sr crash-looped ("Invalid state during store open");
#      databento-volume-aggregator sat in REBALANCING with 1.8M lag.
#   3. BOTH of those pods reported READY the whole time. The probes check the HTTP port, not
#      whether the Streams client is RUNNING — so Kubernetes never restarted them and nothing
#      alerted. "All pods ready, zero records produced" is the exact shape of this failure.
#
# So this script watches the only signal that cannot lie: whether committed offsets actually
# ADVANCE. A group that holds real lag and does not move across two samples is stuck, whatever
# its pod says about itself. Remediation escalates and is remembered across runs:
#
#   strike 1  rollout restart      — new producer epoch; clears (1) and a plain wedged rebalance
#   strike 2  empty the state dir  — clears (2); PVC is kept, contents are emptied in place
#   strike 3  stop and shout       — two failed attempts is a real defect, not a transient
#
# The one thing that must NEVER be mistaken for a stuck app is a HEALTHY one rebuilding its state:
# a Streams client in RESTORING commits nothing, so by the offset test alone it looks identical to a
# wedged one (measured 2026-09-21: unified-sr, strike-intelligence and strike-liquidity-heatmap were
# all at zero progress with 1.7M lag while restoring perfectly normally). Restarting a restoring app
# restarts its restoration, which is an infinite loop, and is exactly how the market-carry liveness
# probe took that service down. So three independent signs of life each VETO a strike, and a veto
# costs nothing but one more cycle.
#
# Run from oe-boot-bringup (once, after the partition doctor) and from oe-pipeline-selfheal.timer
# every 10 minutes, because a mid-session crash produces the identical damage.
#
# Fails LOUD and does nothing silently: every decision is logged with the numbers behind it.
set -uo pipefail

# Every external is injectable so the decision logic can be exercised against stubs in CI. The
# DEFAULTS are production; a test overrides them, nothing else does.
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
CONFIRM_CYCLES="${CONFIRM_CYCLES:-2}"    # consecutive stuck observations required before ANY action
DRY_RUN="${DRY_RUN:-false}"

mkdir -p "$STATEDIR"
# A dry run that WRITES is not a dry run. On 2026-09-21 two DRY_RUN passes silently advanced two
# groups to strike 2, so the first real run opened at strike 3 and declared healthy services defective.
remember() { [ "${DRY_RUN:-false}" = true ] || printf '%s\n' "$2" > "$1"; }
forget()   { [ "${DRY_RUN:-false}" = true ] || rm -f "$1" 2>/dev/null; }
log() { printf '[%s] %s\n' "$(date '+%F %T %Z')" "$*" | tee -a "$LOG"; }
run() { if [ "$DRY_RUN" = true ]; then log "DRY: $*"; else "$@"; fi; }

log "=== self-heal start (load $(awk '{print $1}' /proc/loadavg), uptime $(uptime -p)) ==="

# ---------- gate: Kafka must ANSWER and the k3s API must be up ----------
# Everything below reads Kafka and k3s. If either is still coming up there is nothing to diagnose
# yet and a "stuck" verdict would be a false positive, so leave quietly and let the timer retry.
if ! timeout 30 "$KBIN/kafka-broker-api-versions.sh" --bootstrap-server "$BS" >/dev/null 2>&1; then
  log "Kafka is not answering on $BS yet — nothing to diagnose; will retry next cycle"; exit 0
fi
if ! timeout 30 $KUBECTL get deploy --no-headers >/dev/null 2>&1; then
  log "k3s API is not answering yet — nothing to diagnose; will retry next cycle"; exit 0
fi

# ---------- phase 1: hanging transactions ----------
# A hanging transaction blocks read_committed consumers on that partition FOREVER and keeps every
# producer that inherits the id in an epoch fight. Aborting is safe: the records were never
# committed, so no committed data is lost — what is discarded is exactly the half-written batch
# the power cut interrupted.
hanging=$(timeout 180 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" find-hanging \
            --broker-id 1 --max-transaction-timeout 60 2>/dev/null | tail -n +2 | awk 'NF')
if [ -n "$hanging" ]; then
  n=$(echo "$hanging" | wc -l | tr -d ' ')
  log "HANGING TRANSACTIONS: $n found — aborting (uncommitted batches only; no committed data is lost)"
  echo "$hanging" | while read -r topic partition producerId _rest; do
    startOffset=$(echo "$_rest" | awk '{print $3}')
    log "  abort topic=$topic partition=$partition producerId=$producerId startOffset=$startOffset"
    run timeout 60 "$KBIN/kafka-transactions.sh" --bootstrap-server "$BS" abort \
        --topic "$topic" --partition "$partition" --start-offset "$startOffset" 2>&1 | tee -a "$LOG"
  done
else
  log "hanging transactions: none"
fi

# ---------- phase 2: which groups actually MOVED ----------
# The pod's own opinion of its health is what failed on 2026-09-21, so it is not consulted here.
# Committed offsets are read twice; only a group that holds real lag and advanced by ZERO across
# the whole window is called stuck.
sample() {
  timeout 300 "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BS" --describe --all-groups 2>/dev/null \
  | awk 'NF>=6 && $1!="GROUP" && $4 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {cur[$1]+=$4; lag[$1]+=$6}
         END{for (g in cur) printf "%s %d %d\n", g, cur[g], lag[g]}'
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

stuck=$(awk -v floor="$LAG_FLOOR" '
  NR==FNR {c0[$1]=$2; next}
  ($1 in c0) && $3 >= floor && $2 <= c0[$1] { printf "%s %d %d\n", $1, $3, $2-c0[$1] }
' <(echo "$s0") <(echo "$s1"))

if [ -z "$stuck" ]; then
  log "every group with lag advanced — pipeline is moving"
  # a clean observation clears the escalation memory, so an old strike cannot fire months later
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
log "NOT ADVANCING (lag >= $LAG_FLOOR, zero progress in ${SAMPLE_SECONDS}s):"
echo "$stuck" | while read -r g lag delta; do log "  $g lag=$lag delta=$delta"; done

# ---------- phase 3: escalating remediation ----------
# Above the load ceiling, restarting things is what makes the box unreachable rather than what
# fixes it (measured 2026-08-17: saturation stopped sshd, the k3s API and Kafka answering at all).
load=$(awk '{print int($1)}' /proc/loadavg)
if [ "$load" -ge "$LOAD_CEILING" ]; then
  log "load $load >= ceiling $LOAD_CEILING — remediating NOTHING this cycle; retrying next timer"
  exit 0
fi

# group -> deployment. The Streams application.id on this estate is the service name wrapped in a
# namespace prefix and an environment suffix: options-edge-unified-sr-prod -> unified-sr-service,
# options-flow-databento-volume-aggregator-prod -> databento-volume-aggregator. Unwrapping it is a
# guess, so a guess is only ACTED on when exactly ONE deployment matches: an ambiguous core name
# ("gex") must never pick a service by sort order and restart the wrong one.
deploys=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{split($2,a,"/"); print $1" "a[2]}')
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

acted=0
while read -r g lag delta; do
  [ -z "$g" ] && continue
  [ "$acted" -ge "$MAX_ACTIONS" ] && { log "  $g: MAX_ACTIONS=$MAX_ACTIONS reached — left for the next cycle"; continue; }
  read -r dep reps <<<"$(resolve "$g")"
  if [ "${dep:-}" = "AMBIGUOUS" ]; then log "  $g: matches more than one deployment ($reps) — NOT touched; name the mapping explicitly before this can be automatic"; continue; fi
  if [ -z "${dep:-}" ]; then log "  $g: no deployment matches this group — NOT touched (external or renamed consumer)"; continue; fi
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
    dir=$(ls -d "$STORAGE"/*_options-edge_"${dep}"-streams-state 2>/dev/null | head -1)
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

  # ---- defect 2: one bad sample must not be enough to act ----
  # A transient (a rebalance, a slow commit, a restore that ended between samples) clears by itself;
  # a wedge does not. Requiring the SAME group to look stuck on CONFIRM_CYCLES consecutive runs costs
  # 10 minutes and removes the entire class of one-shot false positives.
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
      dir=$(ls -d "$STORAGE"/*_options-edge_"${dep}"-streams-state 2>/dev/null | head -1)
      if [ -z "$dir" ]; then
        log "  $g -> $dep STRIKE 2: no streams-state PVC dir found — repeating the restart instead"
        run $KUBECTL $SA rollout restart "deploy/$dep" 2>&1 | tee -a "$LOG"
      else
        log "  $g -> $dep STRIKE 2: local state is inconsistent with the changelog — scaling to 0, emptying $dir, scaling back"
        run $KUBECTL $SA scale "deploy/$dep" --replicas=0 2>&1 | tee -a "$LOG"
        # the dir must not be emptied under a live pod, or the new state is corrupt on arrival
        # label selectors do not match every deployment on this estate (measured 2026-09-17), so the
        # pod is tracked by name prefix, which always does.
        gone=false
        for i in $(seq 1 60); do
          [ -z "$($KUBECTL get pods --no-headers 2>/dev/null | awk -v d="$dep-" 'index($1,d)==1')" ] && { gone=true; break; }
          sleep 5
        done
        if [ "$gone" != true ] && [ "$DRY_RUN" != true ]; then
          log "  $g -> $dep: pod still present after 300s — NOT emptying the state dir under a live pod; scaling back and leaving the strike in place"
          run $KUBECTL $SA scale "deploy/$dep" --replicas=1 2>&1 | tee -a "$LOG"
          acted=$((acted+1)); continue
        fi
        run find "$dir" -mindepth 1 -delete
        run $KUBECTL $SA scale "deploy/$dep" --replicas=1 2>&1 | tee -a "$LOG"
      fi
      acted=$((acted+1)) ;;
    *)
      log "  $g -> $dep STRIKE $strikes: a restart AND a state wipe both failed — this is a DEFECT, not a transient. Not touching it again; investigate $dep."
      ;;
  esac
done <<<"$stuck"

log "=== self-heal done: $acted action(s) taken ==="
