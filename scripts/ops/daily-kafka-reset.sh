#!/usr/bin/env bash
# Daily 0DTE clean-slate reset for OptionsEdge prod Kafka + Streams apps.
#
# Runs on the prod host (Jenkins job options-edge-kafka-reset, or cron). Because
# OptionsEdge trades 0DTE and there is no market data after close, all Kafka
# Streams state from the session is disposable. Each night this:
#   1) deletes disposable topics: every *-changelog / *-repartition (Streams
#      state), plus any *replay* / *smoke* / dev.* leftovers,
#   2) rolling-restarts the options-edge app deployments so they recreate their
#      internal topics fresh on startup.
#
# SAFETY:
#   * NEVER deletes system topics (__consumer_offsets, _schemas,
#     __transaction_state) or real data topics (options.*, underlying.*, etc.).
#   * Uses `kubectl rollout restart` (rolling) — it NEVER drops a deployment to
#     0 replicas, so a transient k8s API blip cannot leave prod down.
#   * Dry-run by default. Set DRY_RUN=false to actually mutate.
set -uo pipefail

# ---- config (override via env) ----
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="${K8S_NAMESPACE:-options-edge}"
SELECTOR="${APP_SELECTOR:-app.kubernetes.io/part-of=options-edge}"
DRY_RUN="${DRY_RUN:-true}"
# 2026-07-08: Kafka now self-expires data via a 12h broker retention
# (log.retention.ms=43200000). This daily topic-delete + rolling-restart is therefore
# REDUNDANT and is DISABLED by default (WIPE_KAFKA=false -> the run is a logged no-op).
# The Jenkins job (options-edge-kafka-reset) can stay wired but does nothing unless
# WIPE_KAFKA=true is explicitly set for a one-off hard reset.
WIPE_KAFKA="${WIPE_KAFKA:-false}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-180}"
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"   # empty -> no Discord post
KT="$KAFKA_BIN/kafka-topics.sh"

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] $*"; }
# Post a one-line summary to Discord (no-op if no webhook / no curl).
discord() {
  [ -n "$DISCORD_WEBHOOK_URL" ] || return 0
  command -v curl >/dev/null 2>&1 || return 0
  local msg="$1"
  # Pass the webhook URL via stdin config (-K -), NOT argv, so the secret is not
  # exposed in process listings (ps/argv).
  printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
    | curl -fsS --max-time 10 -K - -H 'Content-Type: application/json' \
        -d "{\"username\":\"OptionsEdge Ops\",\"content\":\"${msg}\"}" \
        >/dev/null 2>&1 || log "WARN: Discord post failed"
}

# disposable = ends in -changelog/-repartition, OR contains replay/smoke, OR starts with dev.
# system topics (__*, _schemas) and real data topics (options.*, underlying.*) are always kept.
is_disposable() {
  local t="$1"
  case "$t" in
    __*|_schemas) return 1 ;;                       # system: keep
  esac
  # THE RESET-PRESERVED KEEP-LIST WINS over every pattern below. This was not hypothetical: the
  # disposable rule deletes *-changelog, and ...gamma-migration-scorer-changelog is a topic
  # scripts/ops/premarket-reset.sh preserves BY NAME and which exists on dev and production right now --
  # so this reset deleted what another reset path protects (deploy Codex round 2 of the
  # trading.order.{intent,status} classification, 2026-10-13).
  if is_reset_preserved "$t"; then return 1; fi
  if printf '%s\n' "$t" | grep -qE "$PRESERVE_TOPICS_REGEX"; then return 1; fi
  case "$t" in
    *-changelog|*-repartition) return 0 ;;          # Streams state: delete (recreated fresh)
    *replay*|*smoke*) return 0 ;;                    # one-off run leftovers: delete
    dev.*) return 0 ;;                               # pre-prefix-fix orphans: delete
  esac
  return 1                                           # everything else (real data): keep
}

# ---- the keep-list, one definition shared with every other destructive path ----
#
# TWO mechanisms protect topics in this estate and this script needs BOTH:
#   * the exact names in topics.env (OPTIONS_EDGE_RESET_PRESERVED_TOPICS), via is_reset_preserved;
#   * PRESERVE_TOPICS_REGEX in scripts/ops/premarket-reset.sh, which carries the WILDCARD arms that
#     topics.env cannot name -- Streams-internal changelogs. That is not a theoretical difference:
#     ...gamma-migration-scorer-changelog matches the regex, exists on dev and production, and is
#     deleted by the *-changelog rule below unless this script reads the regex too.
# The regex is read from premarket-reset.sh rather than copied, so there is one definition; failing to
# read it is a REFUSAL, because this script's whole job is deleting topics.
_dkr_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_dkr_keep="$_dkr_dir/../kafka/reset-preserved-topics.sh"
if [ ! -r "$_dkr_keep" ]; then
  echo "daily-kafka-reset: cannot read $_dkr_keep — refusing to delete anything." >&2
  echo "  This script does not travel alone; ship ops/{daily-kafka-reset,premarket-reset}.sh and" >&2
  echo "  kafka/{reset-preserved-topics.sh,topics.env} together (Jenkinsfile.kafka-reset does)." >&2
  exit 1
fi
# shellcheck source=/dev/null
. "$_dkr_keep"
PRESERVE_TOPICS_REGEX="$( { grep -m1 "^PRESERVE_TOPICS_REGEX=" "$_dkr_dir/premarket-reset.sh" || true; } \
  | sed "s/^PRESERVE_TOPICS_REGEX='//; s/'$//")"
if [ -z "$PRESERVE_TOPICS_REGEX" ]; then
  echo "daily-kafka-reset: cannot read PRESERVE_TOPICS_REGEX from $_dkr_dir/premarket-reset.sh — refusing to delete anything" >&2
  echo "  This script does not travel alone. Ship the repo layout it resolves against:" >&2
  echo "    ops/daily-kafka-reset.sh  ops/premarket-reset.sh" >&2
  echo "    kafka/reset-preserved-topics.sh  kafka/topics.env" >&2
  echo "  Jenkinsfile.kafka-reset does exactly that (~/oe-reset/{ops,kafka})." >&2
  exit 1
fi

# ---- single-instance lock ----
LOCK="/tmp/daily-kafka-reset.lock"
exec 9>"$LOCK" || exit 1
if ! flock -n 9; then log "another reset is already running; exiting"; exit 0; fi

log "=== daily kafka reset start (DRY_RUN=$DRY_RUN WIPE_KAFKA=$WIPE_KAFKA) ==="

# Disabled-by-default gate: Kafka's 12h broker retention self-expires the session's
# data, so we no longer delete topics or rolling-restart apps nightly. Exit as a no-op
# unless a hard reset is explicitly requested.
if [ "$WIPE_KAFKA" != "true" ]; then
  log "WIPE_KAFKA=false — Kafka self-expires via 12h broker retention; skipping topic delete + rolling restart (no-op)."
  discord "💤 Kafka daily-reset skipped — 12h broker retention self-expires data; no topic delete (set WIPE_KAFKA=true for a hard reset)."
  exit 0
fi

# ---- sanity: both kafka and k8s must be reachable, else abort untouched ----
if ! "$KT" --bootstrap-server "$BOOTSTRAP" --list >/tmp/dkr-topics.txt 2>/dev/null; then
  log "ABORT: cannot reach Kafka at $BOOTSTRAP"; exit 1
fi
if ! kubectl -n "$NS" get deploy >/dev/null 2>&1; then
  log "ABORT: cannot reach k8s namespace $NS"; exit 1
fi
TOTAL=$(wc -l </tmp/dkr-topics.txt | tr -d ' ')
DELELIGIBLE=$(while read -r t; do is_disposable "$t" && echo "$t"; done </tmp/dkr-topics.txt | wc -l | tr -d ' ')
log "topics total=$TOTAL  disposable=$DELELIGIBLE  keep=$((TOTAL-DELELIGIBLE))"

if [ "$DRY_RUN" = "true" ]; then
  log "DRY-RUN: would delete $DELELIGIBLE topics, then rolling-restart apps ($SELECTOR):"
  while read -r t; do is_disposable "$t" && echo "    DEL $t"; done </tmp/dkr-topics.txt | head -40
  log "(showing up to 40; total $DELELIGIBLE). No changes made."
  discord "🧪 Kafka daily-reset DRY-RUN — would delete ${DELELIGIBLE} disposable topics (of ${TOTAL}); no changes made."
  exit 0
fi

# ---- 1) delete disposable topics (one retry each) ----
deleted=0; failed=0
while read -r t; do
  is_disposable "$t" || continue
  if "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1 \
     || "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1; then
    deleted=$((deleted+1))
  else
    failed=$((failed+1)); log "  WARN failed to delete topic: $t"
  fi
done </tmp/dkr-topics.txt
log "topic deletion done: deleted=$deleted failed=$failed"

# ---- 2) rolling restart so apps recreate internal topics fresh ----
# rollout restart keeps replicas >= (desired - maxUnavailable); it never scales to 0,
# so a mid-run API blip cannot leave prod down. 0-replica deployments are a no-op.
log "rolling-restart of deployments matching $SELECTOR ..."
if ! kubectl -n "$NS" rollout restart deploy -l "$SELECTOR" >/dev/null 2>&1; then
  log "  WARN 'rollout restart' returned errors (some deployments may not have been triggered)"
fi

# ---- 3) wait for rollouts (best-effort; do not hard-fail on a slow one) ----
for d in $(kubectl -n "$NS" get deploy -l "$SELECTOR" -o name 2>/dev/null); do
  if kubectl -n "$NS" rollout status "$d" --timeout="${ROLLOUT_TIMEOUT}s" >/dev/null 2>&1; then
    log "  rolled out: $d"
  else
    log "  WARN rollout slow/incomplete: $d"
  fi
done

log "=== daily kafka reset complete ==="
if [ "$failed" -eq 0 ]; then
  discord "🧹 Kafka daily-reset complete — deleted ${deleted} disposable topics (of ${TOTAL}); apps rolling-restarted."
else
  discord "⚠️ Kafka daily-reset finished with errors — deleted ${deleted}, failed ${failed} (of ${TOTAL} total)."
fi
