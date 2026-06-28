#!/usr/bin/env bash
# Platform-wide error/exception watcher → Discord (deduped, every 5 min).
#
# Greps EVERY deployment's logs in the namespace for ERROR/Exception/FATAL,
# normalizes each line to a stable signature (strips timestamps/ids/numbers),
# flattens by occurrence (counts repeats), and posts ONE consolidated Discord
# message per poll — but ONLY for signatures it has not already alerted. A
# signature that keeps recurring is suppressed (not re-sent); if it goes quiet
# for SUPPRESS_WINDOW_MIN and later returns, it re-alerts. So you get told once
# per distinct error, not spammed every 5 minutes.
#
# Runs continuously (loop, default 5-min interval) — or one pass with --once for
# an external scheduler. State persists in STATE_FILE across passes/restarts.
#
# Needs: kubectl (cluster read), curl, plus a Discord webhook. Read-only — it
# never mutates the cluster.
set -uo pipefail

# ---- config (override via env) ----
NS="${K8S_NAMESPACE:-options-edge}"
INTERVAL="${INTERVAL:-300}"                 # poll period seconds (5 min)
LOG_SINCE="${LOG_SINCE:-8m}"                # window per poll — generously > INTERVAL so polls overlap (covers collection time + a brief restart); dedup makes overlap harmless
LOG_TAIL="${LOG_TAIL:-5000}"               # max log lines scanned per service (bounded)
TOP_N="${TOP_N:-15}"                        # max distinct NEW errors listed per Discord message
SUPPRESS_WINDOW_MIN="${SUPPRESS_WINDOW_MIN:-60}"  # a quiet-then-returning signature re-alerts after this
STATE_FILE="${STATE_FILE:-/tmp/platform-error-watch.state}"
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
# Lines matched as errors, and noise to drop. Override to taste.
GREP_PATTERN="${GREP_PATTERN:-ERROR|Exception|FATAL| SEVERE |Caused by:}"
EXCLUDE_PATTERN="${EXCLUDE_PATTERN:-}"      # e.g. 'HealthCheck|debug-probe' ; empty = keep all
EXCLUDE_SERVICES="${EXCLUDE_SERVICES:-}"    # space-separated deployment names to skip
ONCE=0; [ "${1:-}" = "--once" ] && ONCE=1

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] [err-watch] $*"; }
need() { command -v "$1" >/dev/null 2>&1 || { log "FATAL: missing required tool: $1"; exit 1; }; }
need kubectl; need curl; need python3

# Validate numeric config (a non-numeric value would corrupt arithmetic/sleep).
for v in INTERVAL LOG_TAIL TOP_N SUPPRESS_WINDOW_MIN; do
  case "${!v}" in ''|*[!0-9]*) log "FATAL: $v must be a positive integer (got '${!v}')"; exit 1;; esac
done

# Read logs from ALL replicas if this kubectl supports --all-pods; else one pod.
ALLPODS=""
if kubectl logs --help 2>&1 | grep -q -- '--all-pods'; then ALLPODS="--all-pods=true"; fi

# Single-instance lock (a second watcher racing the same STATE_FILE would lose
# dedup state and spam). Best-effort: skip if flock is unavailable.
if command -v flock >/dev/null 2>&1; then
  exec 8>"${STATE_FILE}.lock" 2>/dev/null || true
  flock -n 8 2>/dev/null || { log "another watcher holds ${STATE_FILE}.lock — exiting"; exit 0; }
fi

discord() {  # $1 = description text. Sent as an embed (4096-char limit; the
             # plain `content` field maxes at 2000, which truncation tripped on).
  [ -n "$DISCORD_WEBHOOK_URL" ] || { log "(no webhook set — would post)"; return 0; }
  local json
  json=$(printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps({"username":"OptionsEdge Errors","embeds":[{"title":"Platform error watch","description":sys.stdin.read()[:4000],"color":15158332}]}))' 2>/dev/null) \
    || { log "WARN: payload encode failed"; return 1; }
  # URL via stdin config (-K -) so the secret stays out of process argv.
  if printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
       | curl -fsS --max-time 12 -K - -H 'Content-Type: application/json' -d "$json" >/dev/null 2>&1; then
    return 0
  else
    log "WARN: Discord post failed"; return 1
  fi
}

# Normalize a raw log line to a stable signature: drop the timestamp/level head,
# then mask timestamps, UUIDs, hex, and numbers so "offset 123"/"offset 456"
# collapse to one signature. Truncate so a giant stack line stays bounded.
normalize_sed='s/[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9:.,]+(Z|[+-][0-9:]+)?//g; s/[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}//g; s/0x[0-9a-fA-F]+//g; s/[0-9]+/#/g; s/[[:space:]]+/ /g'

# Collect "count<TAB>service|signature" for the current window across all deploys.
collect() {
  local svcs s
  # pipefail makes this fail if `kubectl get deploy` errors (cluster/RBAC down),
  # so we return 2 ("blind") instead of silently reporting zero errors.
  if ! svcs=$(kubectl -n "$NS" get deploy -o name 2>/dev/null | sed 's#.*/##'); then
    return 2
  fi
  [ -n "$svcs" ] || { log "WARN: no deployments found in ns $NS"; return 0; }
  local tmp; tmp=$(mktemp)
  for s in $svcs; do
    case " $EXCLUDE_SERVICES " in *" $s "*) continue;; esac
    kubectl -n "$NS" logs "deploy/$s" --since="$LOG_SINCE" --tail="$LOG_TAIL" --all-containers $ALLPODS --prefix=false 2>/dev/null \
      | grep -aE "$GREP_PATTERN" \
      | { if [ -n "$EXCLUDE_PATTERN" ]; then grep -avE "$EXCLUDE_PATTERN"; else cat; fi; } \
      | sed -E "$normalize_sed" \
      | sed -E "s/^[[:space:]]*//; s/[[:space:]]*$//" \
      | cut -c1-200 \
      | grep -v '^$' \
      | sed "s#^#${s}|#" >>"$tmp"
  done
  # count occurrences of each service|signature
  sort "$tmp" | uniq -c | sed -E 's/^[[:space:]]*([0-9]+)[[:space:]]+(.*)$/\1\t\2/'
  rm -f "$tmp"
}

one_pass() {
  local now cutoff; now=$(date +%s); cutoff=$(( now - SUPPRESS_WINDOW_MIN*60 ))

  # Load state: assoc array key(service|signature) -> last_seen_epoch
  declare -A SEEN
  if [ -f "$STATE_FILE" ]; then
    while IFS=$'\t' read -r epoch key; do
      [ -n "$key" ] || continue
      [ "$epoch" -ge "$cutoff" ] 2>/dev/null && SEEN["$key"]="$epoch"   # prune stale on load
    done < "$STATE_FILE"
  fi

  # Current window errors. rc=2 means we are BLIND (cluster unreachable) — do NOT
  # touch state or claim "no errors"; just skip this pass.
  local cur rc
  cur=$(collect); rc=$?
  if [ "$rc" -eq 2 ]; then log "WARN: cluster unreachable — skipping pass (state untouched)"; return 0; fi

  local new_keys=(); local new_lines=(); local total_new=0 total_occ=0
  if [ -n "$cur" ]; then
    while IFS=$'\t' read -r count key; do
      [ -n "$key" ] || continue
      total_occ=$((total_occ + count))
      if [ -n "${SEEN[$key]:-}" ]; then
        SEEN["$key"]="$now"                          # already alerted → refresh, stay suppressed
      else
        new_keys+=("$key")                           # NEW — do NOT mark seen until the post succeeds
        new_lines+=("${count}"$'\t'"${key}")
        total_new=$((total_new + 1))
      fi
    done <<< "$cur"
  fi

  local posted_ok=1
  if [ "$total_new" -gt 0 ]; then
    # Build the consolidated Discord message (top-N new signatures, by count desc)
    local body="🚨 **${total_new} new error signature(s)** across the platform (last ${LOG_SINCE})"
    body+=$'\n'"_repeats already alerted are suppressed; counts are occurrences this window_"$'\n'
    local count key svc sig
    while IFS=$'\t' read -r count key; do
      [ -n "$key" ] || continue
      svc="${key%%|*}"; sig="${key#*|}"
      body+=$'\n'"• **${svc}** ${count}× — \`$(printf '%s' "$sig" | cut -c1-140)\`"
    done < <(printf '%s\n' "${new_lines[@]}" | sort -t$'\t' -k1 -rn | head -"$TOP_N")
    [ "$total_new" -gt "$TOP_N" ] && body+=$'\n'"…and $((total_new - TOP_N)) more new signatures."
    body=$(printf '%s' "$body" | cut -c1-3900)
    log "posting ${total_new} new error signature(s) to Discord"
    discord "$body" && posted_ok=1 || posted_ok=0
    # Only mark the NEW signatures as alerted if the post actually went out.
    # On failure they stay "new" and are retried next poll (no missed alerts).
    if [ "$posted_ok" = 1 ]; then for k in "${new_keys[@]}"; do SEEN["$k"]="$now"; done
    else log "post failed — keeping ${total_new} signatures unalerted for retry next poll"; fi
  else
    log "no new errors (${total_occ} total error-lines this window, all already alerted)"
  fi

  # Persist state ATOMICALLY (temp + mv) so a crash mid-write can't lose dedup state.
  local stmp; stmp=$(mktemp)
  for k in "${!SEEN[@]}"; do printf '%s\t%s\n' "${SEEN[$k]}" "$k" >>"$stmp"; done
  mv -f "$stmp" "$STATE_FILE" 2>/dev/null || log "WARN: could not persist state to $STATE_FILE"
}

trap 'log "stopping"; exit 0' INT TERM
log "starting (ns=$NS interval=${INTERVAL}s window=$LOG_SINCE suppress=${SUPPRESS_WINDOW_MIN}m once=$ONCE)"
if [ "$ONCE" = 1 ]; then
  one_pass
else
  while true; do
    one_pass || log "pass error (continuing)"
    sleep "$INTERVAL"
  done
fi
