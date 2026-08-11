#!/usr/bin/env bash
# dev-feed-memory-watch.sh — sample the DEV databento-feed container's memory over time so the
# question the 2026-08-10 OOM left open can actually be answered.
#
# WHY THIS EXISTS. On 2026-08-10 the dev feed was OOMKilled (exit 137) at 09:32:16 ET, ~3 minutes
# into the RTH open; the ~2 minute restart produced zero options.databento.raw records, so no live
# GEX reached the board and the UI held "Switching to live data…". The limit was raised 2Gi -> 3Gi
# (deploy #781) as a MITIGATION — but a single OOM sample cannot distinguish a genuine open-burst
# peak from a leak or an unbounded buffer, and both reviewers said so. Dev has no metrics-server,
# so there was no way to tell them apart. This produces the missing series.
#
# HOW TO READ THE OUTPUT. One CSV row per sample:
#   ts_utc,ts_et,phase,pod,restarts,current_bytes,peak_bytes,limit_bytes,pct_of_limit
# Bounded memory that rises into the open and then plateaus => open-burst peak; the raised ceiling
# is the right fix. Memory that keeps climbing across the session, or whose peak creeps up day
# over day, => leak/unbounded buffer, and RAISING THE LIMIT AGAIN IS THE WRONG MOVE (see the
# stop-rule in k8s/overlays/dev/databento-feed-dev-patch.yaml).
#
# `peak_bytes` is the cgroup's own high-water mark since container start (memory.peak on cgroup v2,
# memory.max_usage_in_bytes on v1) — it survives between samples, so a spike BETWEEN two samples is
# still caught. That is what makes a 60s interval honest rather than a hope.
#
# READ-ONLY: kubectl get/exec only. Never scales, patches, or deletes anything. Safe to run at any
# time; it no-ops quietly when dev is scaled down (off-hours) rather than logging noise.
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

NS="${K8S_NAMESPACE:-options-edge}"
CTX="${KUBE_CONTEXT:-docker-desktop}"
DEPLOY="${DEPLOY_NAME:-options-edge-databento-feed}"
CONTAINER="${CONTAINER_NAME:-databento-feed}"
INTERVAL="${INTERVAL_SECONDS:-60}"
OUT="${OUT_FILE:-$HOME/oe-ops/dev-feed-memory.csv}"
ONCE="${ONCE:-false}"

K="kubectl --context $CTX -n $NS"

log() { printf '%s %s\n' "$(date -u '+%FT%TZ')" "$*" >&2; }

# ET phase label, so a reader can find the open without converting timestamps by hand. The open is
# the interval this file exists to capture.
phase_for() {
  local hm; hm=$(TZ=America/New_York date '+%H%M')
  local dow; dow=$(TZ=America/New_York date '+%u')
  if [ "$dow" -gt 5 ]; then echo "weekend"; return; fi
  if   [ "$hm" -lt 0400 ]; then echo "overnight"
  elif [ "$hm" -lt 0930 ]; then echo "premarket"
  elif [ "$hm" -lt 0945 ]; then echo "OPEN"
  elif [ "$hm" -lt 1600 ]; then echo "rth"
  else echo "postclose"; fi
}

# cgroup v2 first, then v1. Missing files yield an empty field rather than a fabricated number.
read_cgroup() {
  local pod="$1"
  $K exec "$pod" -c "$CONTAINER" -- sh -c '
    cur=""; peak=""; lim=""
    for f in /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory/memory.usage_in_bytes; do
      [ -r "$f" ] && { cur=$(cat "$f"); break; }
    done
    for f in /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory/memory.max_usage_in_bytes; do
      [ -r "$f" ] && { peak=$(cat "$f"); break; }
    done
    for f in /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes; do
      [ -r "$f" ] && { lim=$(cat "$f"); break; }
    done
    echo "$cur,$peak,$lim"
  ' 2>/dev/null
}

sample_once() {
  local pod restarts vals cur peak lim pct
  pod=$($K get pods -l app.kubernetes.io/name="$DEPLOY" \
        -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | head -1)
  # Fall back to a name match when the label differs from the deployment name.
  [ -z "$pod" ] && pod=$($K get pods --no-headers 2>/dev/null | awk -v d="$DEPLOY" '$1 ~ d && $3=="Running" {print $1; exit}')
  if [ -z "$pod" ]; then
    return 0   # dev scaled down (off-hours) or feed absent — nothing to sample, and that is not an error
  fi

  restarts=$($K get pod "$pod" -o jsonpath="{.status.containerStatuses[?(@.name=='$CONTAINER')].restartCount}" 2>/dev/null)
  vals=$(read_cgroup "$pod")
  cur=${vals%%,*}; peak=$(echo "$vals" | cut -d, -f2); lim=$(echo "$vals" | cut -d, -f3)
  [ -z "$cur" ] && { log "WARN: could not read cgroup memory for $pod (exec failed or files absent)"; return 0; }

  # "max" is cgroup v2's unlimited sentinel; leave the ratio empty rather than inventing one.
  pct=""
  if [ -n "$lim" ] && [ "$lim" != "max" ] && [ "$lim" -gt 0 ] 2>/dev/null; then
    pct=$(awk -v c="$cur" -v l="$lim" 'BEGIN{printf "%.1f", (c/l)*100}')
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%FT%TZ')" "$(TZ=America/New_York date '+%FT%T')" "$(phase_for)" \
    "$pod" "${restarts:-}" "$cur" "$peak" "$lim" "$pct" >>"$OUT"
}

mkdir -p "$(dirname "$OUT")" || { log "FATAL: cannot create $(dirname "$OUT")"; exit 1; }
[ -s "$OUT" ] || echo "ts_utc,ts_et,phase,pod,restarts,current_bytes,peak_bytes,limit_bytes,pct_of_limit" >"$OUT"

$K get deploy "$DEPLOY" >/dev/null 2>&1 || { log "FATAL: cannot reach $NS/$DEPLOY on context $CTX"; exit 1; }

if [ "$ONCE" = "true" ]; then
  sample_once
  exit 0
fi

log "sampling $DEPLOY every ${INTERVAL}s -> $OUT (Ctrl-C to stop)"
while true; do
  sample_once
  sleep "$INTERVAL"
done
