#!/bin/bash
# "clean up prod" (owner rule 2026-09-16, "make prod similar to dev"): the dev-cleanup.sh flow on prod.
#
#   1. pause the es4->prod MirrorMaker launchd agents on this Mac (they produce into topics that are about to go)
#   2. on .252 as root: scripts/ops/offhours-clean-slate.sh with TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none —
#      scale every pipeline deployment to 0 (Keycloak exempt), DELETE every non-whitelisted topic (state and
#      data; the reset-preserved whitelist from topics.env incl. the prod-only entry and the system topics are
#      never touched), delete the idle consumer groups, EMPTY every Streams-state PVC in place (PVCs kept)
#   3. from this Mac (exactly what the Jenkins deploy does): scripts/kafka/apply-topics.sh with
#      ENVIRONMENT=production recreates the declared set at its declared shape, then
#      scripts/kafka/ensure-partition-only-topics.sh creates the config-less partition-only topics
#   4. resume the mirrors — every one EXCEPT those that copy a topic step 3 could not reconcile or did
#      not create (scripts/ops/clean-slate-decision.sh decides; scripts/ops/mirror-topic-filter.sh
#      answers it per agent). Two different outcomes, deliberately not treated alike: a recreate that
#      WALKED THE WHOLE declared list and could not reconcile some of it resumes the mirrors with no
#      stake in those topics (and still does NOT bring prod up, step 5); a recreate that did not reach
#      the end of the list at all resumes nothing, because nothing is known about the rest of it.
#   5. full bring-up on .252 (systemctl restart oe-boot-bringup: waves + partition doctor) unless `down`
#
#   prod-clean-slate.sh dry            steps 2 (DRY_RUN) + what 1/3 would do; changes nothing
#   prod-clean-slate.sh wipe           steps 1-5
#   prod-clean-slate.sh wipe down      steps 1-4, leave prod at 0 (Keycloak up)
#
# The calendar guard inside the host script refuses to run inside market hours. Source tree: DEPLOY_SRC
# (default: the deploy repo checkout) — the host gets the script, reset-preserved-topics.sh, topics.env and
# market_calendar.py in the repo layout it sources (~/offhours/{ops,kafka,jenkins}).
set -u
MODE="${1:-dry}"; AFTER="${2:-up}"
DEPLOY_SRC="${DEPLOY_SRC:-/Users/abhinav/development/workspace/options-edge-deploy}"
HOST=abhinav@192.168.100.252
PROD_BS=192.168.100.252:9092
PW=$(cat ~/oe-ops/.prod-ssh-pw)
WEBHOOK=$( [ -r ~/oe-ops/.prod-discord-webhook ] && cat ~/oe-ops/.prod-discord-webhook || true )
PAUSED_LIST="${PROD_MIRRORS_PAUSED:-$HOME/oe-ops/.prod-mirrors-paused}"
LOG=~/oe-ops/logs/prod-clean-slate.log; mkdir -p ~/oe-ops/logs
case "$MODE" in dry) DRY=true; WIPE=false ;; wipe) DRY=false; WIPE=true ;; *) echo "usage: $0 dry|wipe [up|down]"; exit 2 ;; esac
say() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }
# The recreate decision (what a partial apply permits) lives in its own file so a test can drive every
# combination of statuses; this script only ACTS on it. Sourced from DEPLOY_SRC like everything else.
# shellcheck source=/dev/null
. "$DEPLOY_SRC/scripts/ops/clean-slate-decision.sh" || { echo "cannot source $DEPLOY_SRC/scripts/ops/clean-slate-decision.sh"; exit 1; }
# ...and the paused-mirror ledger, for the same reason: which agents may start, and what must stay
# recorded as down, is driven by scripts/ops/mirror-ledger-test.sh rather than read out of this file.
# shellcheck source=/dev/null
. "$DEPLOY_SRC/scripts/ops/mirror-ledger.sh" || { echo "cannot source $DEPLOY_SRC/scripts/ops/mirror-ledger.sh"; exit 1; }
# Returns the REMOTE command's exit status (the filter stages would otherwise hide it: a function returns
# its last pipe stage, and sed always succeeds).
ssh_root() { local rc; sshpass -p "$PW" ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 "$HOST" "su - root -c '$1'" <<EOF 2>&1 | grep -vE "^Password: *$|WARNING: |vulnerable|openssh|^\*\*" | sed 's/^Password: //'
$PW
EOF
  rc=${PIPESTATUS[0]}; return "$rc"
}

# ---- es4->prod mirror agents: the launchd jobs whose producer.properties target the prod broker ----
prod_mirror_agents() {
  python3 - "$HOME/Library/LaunchAgents" "$PROD_BS" <<'PY'
import glob, os, plistlib, re, sys
agents_dir, bs = sys.argv[1], re.escape(sys.argv[2])
target = re.compile(r"^\s*bootstrap\.servers\s*=\s*" + bs + r"\s*$")
for plist in sorted(glob.glob(os.path.join(agents_dir, "com.optionsedge.*.plist"))):
    try:
        job = plistlib.load(open(plist, "rb"))
    except Exception:
        continue
    for arg in (job.get("ProgramArguments") or []) if isinstance(job, dict) else []:
        if not os.path.isabs(str(arg)):
            continue
        props = os.path.join(os.path.dirname(str(arg)), "producer.properties")
        try:
            lines = open(props).read().splitlines()
        except OSError:
            continue
        if any(target.match(l) for l in lines):
            print(job.get("Label") or os.path.basename(plist)[:-6], plist)
            break
PY
}
# RECORD FIRST, STOP SECOND. The merge into the ledger used to be unchecked, and every discovered
# agent was booted out regardless: a failed merge stopped agents that nothing recorded, and the resume
# could then report success with them down (deploy Codex round 5). mirror_ledger_record verifies the
# rows are readable back out of the ledger, and a non-zero return here stops the whole run before
# anything is paused or wiped.
# The stop step, run by mirror_ledger_record UNDER THE LEDGER LOCK and only after every row it is about
# to stop is readable back out of the ledger.
#
# A mirror that is STILL LOADED when this finishes is a failure, not a warning (deploy Codex round 6):
# it is producing into the topics the next step is about to delete, which is the whole reason the pause
# exists. The non-zero status reaches the caller, which then wipes nothing.
_bootout_paused_agents() {
  local uid label plist n=0 stuck=0 i; uid=$(id -u)
  while read -r label plist; do
    [ -n "$label" ] || continue
    launchctl bootout "gui/$uid/$label" >/dev/null 2>&1
    for i in 1 2 3 4 5 6 7 8 9 10; do launchctl list "$label" >/dev/null 2>&1 || break; sleep 3; done
    if launchctl list "$label" >/dev/null 2>&1; then
      stuck=$((stuck+1)); say "   STILL LOADED after bootout: $label"
    else
      n=$((n+1))
    fi
  done < "$PAUSED_LIST.new"
  say "paused $n es4->prod mirror agent(s) (list: $PAUSED_LIST)"
  if [ "$stuck" -gt 0 ]; then
    say "   $stuck mirror agent(s) are STILL PRODUCING — the wipe must not run while they are"
    return 1
  fi
}
pause_mirrors() {
  local rc=0
  prod_mirror_agents | while read -r label plist; do launchctl list "$label" >/dev/null 2>&1 && printf '%s %s\n' "$label" "$plist"; done > "$PAUSED_LIST.new"
  mirror_ledger_record "$PAUSED_LIST" "$PAUSED_LIST.new" _bootout_paused_agents || rc=$?
  rm -f "$PAUSED_LIST.new"
  [ "$rc" -eq 0 ] || say "   pause did not complete: the ledger holds what was recorded, and nothing may be wiped"
  return "$rc"
}
# ---- 0. ship the repo layout the host script sources ----
for f in scripts/ops/offhours-clean-slate.sh scripts/kafka/reset-preserved-topics.sh scripts/kafka/topics.env scripts/jenkins/market_calendar.py scripts/kafka/apply-topics.sh scripts/kafka/ensure-partition-only-topics.sh scripts/kafka/load-kafka-settings.sh scripts/ops/mirror-topic-filter.sh scripts/ops/clean-slate-decision.sh scripts/ops/mirror-ledger.sh; do
  [ -r "$DEPLOY_SRC/$f" ] || { echo "missing $DEPLOY_SRC/$f"; exit 1; }
done
say "=== prod clean-slate $MODE (after=$AFTER) source=$DEPLOY_SRC ==="
sshpass -p "$PW" ssh -o StrictHostKeyChecking=no "$HOST" 'mkdir -p ~/offhours/ops ~/offhours/kafka ~/offhours/jenkins' </dev/null
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/ops/offhours-clean-slate.sh" "$HOST:offhours/ops/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/kafka/reset-preserved-topics.sh" "$DEPLOY_SRC/scripts/kafka/topics.env" "$HOST:offhours/kafka/"
sshpass -p "$PW" scp -q -o StrictHostKeyChecking=no "$DEPLOY_SRC/scripts/jenkins/market_calendar.py" "$HOST:offhours/jenkins/"

# ---- 1. mirrors ----
if [ "$MODE" = wipe ]; then
  pause_mirrors || { say "mirror pause FAILED — nothing was paused, nothing wiped, no bring-up. Fix the ledger, then rerun."; exit 1; }
else
  say "dry: would pause $(prod_mirror_agents | wc -l | tr -d ' ') es4->prod mirror agents"
fi

# ---- 2. wipe on the host ----
say "host: offhours-clean-slate.sh DRY_RUN=$DRY WIPE_ENABLED=$WIPE TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none STATE_RESET_MODE=contents ENVIRONMENT=production"
ssh_root "chmod +x /home/abhinav/offhours/ops/offhours-clean-slate.sh && cd /home/abhinav/offhours/ops && \
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml CALENDAR_DIR=/home/abhinav/offhours/jenkins ENVIRONMENT=production \
  DRY_RUN=$DRY WIPE_ENABLED=$WIPE EXPECTED_ENV=prod TOPIC_WIPE_MODE=delete START_AFTER_WIPE=none STATE_RESET_MODE=contents \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_BIN=/opt/kafka/current/bin KAFKA_LOG_DIR=/home/kafka-logs \
  EXPECTED_KAFKA_CLUSTER_ID=X4faQOI0QBq-htkrXTQISA RECLAIM_DOCKER=false DOCKER_CONTAINER_LOG_GLOB= \
  DISCORD_WEBHOOK_URL=\"$WEBHOOK\" \
  ./offhours-clean-slate.sh" | tee -a "$LOG"
RC=${PIPESTATUS[0]}
say "host wipe exit=$RC"
if [ "$MODE" = dry ]; then
  . "$DEPLOY_SRC/scripts/kafka/topics.env"
  say "dry: would recreate $(echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | wc -w | tr -d ' ') declared topics (apply-topics.sh ENVIRONMENT=production) + partition-only $(echo $OPTIONS_EDGE_PARTITION_ONLY_TOPICS | tr ' ' '\n' | grep -vc -- '-dev-') (skipping $(echo $OPTIONS_EDGE_PARTITION_ONLY_TOPICS | tr ' ' '\n' | grep -c -- '-dev-') dev-named), then bring-up=$AFTER"
  exit "$RC"
fi
[ "$RC" -eq 0 ] || { say "host wipe FAILED (exit $RC) — mirrors stay paused, no recreate, no bring-up. Fix, then rerun."; exit "$RC"; }

# ---- 3. recreate the declared set from this Mac (== Jenkins deploy path) ----
# apply-topics.sh and ensure-partition-only-topics.sh are run SEPARATELY and their statuses kept apart:
# `a && b` behind one PIPESTATUS could not say which of the two failed, and the decision below turns on
# apply-topics.sh's status alone.
say "recreate: apply-topics.sh (ENVIRONMENT=production) + ensure-partition-only-topics.sh against $PROD_BS"
APPLY_OUT=$(mktemp -t prod-clean-slate-apply) || { say "could not create a temp file for the apply output"; exit 1; }
# The ATTESTATION file. apply-topics.sh writes one line here from its endings and nowhere else, so this
# — not its exit status, and not a line on a stream it shares with every kafka CLI it runs — is what
# says the declared list was walked to the end. Created EMPTY: a run that falls over leaves it empty.
# Under `set -e` a child that exits 9 aborts apply-topics.sh WITH status 9, which would otherwise be
# indistinguishable from its skip ending (deploy Codex round 1).
APPLY_RESULT=$(mktemp -t prod-clean-slate-result) || { say "could not create a temp file for the apply result"; exit 1; }
: > "$APPLY_RESULT"
trap 'rm -f "$APPLY_OUT" "$APPLY_RESULT"' EXIT
( cd "$DEPLOY_SRC" && export ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=$PROD_BS APPLY_TOPICS_RESULT_FILE="$APPLY_RESULT" \
  && . scripts/kafka/load-kafka-settings.sh \
  && KAFKA_RECREATE_MISMATCHED_TOPICS=false scripts/kafka/apply-topics.sh ) > "$APPLY_OUT" 2>&1
ARC=$?
grep -vE '^\s*$' "$APPLY_OUT" | tail -25 | tee -a "$LOG"
ERC=0
if [ "$ARC" -eq 0 ] || [ "$ARC" -eq 9 ]; then
  # Independent of the declared set's reconciliation: these are the config-less partition-only topics.
  ( cd "$DEPLOY_SRC" && export ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=$PROD_BS && . scripts/kafka/load-kafka-settings.sh \
    && scripts/kafka/ensure-partition-only-topics.sh ) 2>&1 | grep -vE '^\s*$' | tail -10 | tee -a "$LOG"
  ERC=${PIPESTATUS[0]}
else
  say "skipped ensure-partition-only-topics.sh: apply-topics.sh did not complete (exit $ARC)"
  ERC="$ARC"
fi

# What is actually on the broker, and what is not. MISSING feeds the mirror hold set below: on a topic
# the recreate did not create, a mirror's first produce is what creates it, at the broker default
# partition count (auto.create.topics.enable) -- the defect mirrors are paused for to begin with.
MISSING=""
MISSING=$( cd "$DEPLOY_SRC" && . scripts/kafka/topics.env && want=$(echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | tr ' ' '\n' | sed 's/:.*//' | sort -u) \
  && have=$(kafka-topics --bootstrap-server $PROD_BS --list 2>/dev/null) \
  && comm -23 <(echo "$want") <(echo "$have" | sort -u) )
MRC=$?
if [ "$MRC" -ne 0 ]; then
  # The census itself failed (no broker, no CLI). Nothing may be concluded about what is present, so
  # every mirror is held: this is the same fail-closed direction as mirror-topic-filter.sh.
  say "declared-topic census FAILED (exit $MRC) — treating every declared topic as unverified"
  MISSING=$( cd "$DEPLOY_SRC" && . scripts/kafka/topics.env && echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | tr ' ' '\n' | sed 's/:.*//' | sort -u )
fi
( cd "$DEPLOY_SRC" && . scripts/kafka/topics.env && want=$(echo $OPTIONS_EDGE_TOPICS $OPTIONS_EDGE_PROD_ONLY_TOPICS | tr ' ' '\n' | sed 's/:.*//' | sort -u) \
  && say "declared topics present: $(( $(echo "$want" | wc -l) - $(echo "$MISSING" | grep -c .) ))/$(echo "$want" | wc -l | tr -d ' ')${MISSING:+  MISSING: $(echo $MISSING)}" )

# The topics apply-topics.sh reached the end of the list WITHOUT reconciling — read from the
# attestation file, never from the output. An EMPTY $SKIPPED_NAMES therefore means "no attested skip
# ending", which the decision treats as a failure whenever the status is the skip status; that is what
# closes the exit-9-from-a-child hole. state=ok attests the other ending and names nothing.
read_apply_attestation "$APPLY_RESULT"
SKIPPED_NAMES=""
case "$ATTEST_STATE" in
  skipped) SKIPPED_NAMES="$ATTEST_SKIPPED" ;;
  ok)      say "apply-topics attested state=ok (the whole declared list was reconciled)" ;;
  *)       say "apply-topics left NO usable attestation: ${ATTEST_REASON:-unknown}" ;;
esac

# ---- the mirror decision (2026-10-08) ----
# WHAT CHANGED. apply-topics.sh exits 9 AND attests `state=skipped` when it reached the END of the
# declared list and some topics could not be reconciled; it exits any other non-zero, with no
# attestation, when the run did not complete. Both of those were exit 1 until this block existed, so
# one drifted topic read as "the recreate is unusable" and left all twelve es4->prod mirrors paused on
# 2026-10-07 (options.spx.strike-invasion.current, 1 partition vs a declared 32 — a drift, and one no
# mirror produces into). The status is necessary and not sufficient: the attestation is what makes the
# two distinguishable, because a child process exiting 9 produces the same status.
#
# WHAT DID NOT CHANGE: the owner rule that a reset which did not complete stays DOWN. A partial
# recreate still does NOT bring prod up. Starting the mirrors is a different act from a bring-up: the
# mirrors are a COPY path this script paused itself in step 1, their offsets live on es4, and every one
# that is started here is one whose topics were all reconciled. A mirror with a stake in an
# unreconciled or missing topic stays paused, which is the part that protects key routing.
clean_slate_decide "$ARC" "$ERC" "$SKIPPED_NAMES" "$MISSING" "$ATTEST_STATE"
say "decision: $DECISION_VERDICT (apply=$ARC ensure=$ERC attested=${ATTEST_STATE:-<none>}) resume=$DECISION_RESUME bringup=$DECISION_BRINGUP exit=$DECISION_EXIT"
case "$DECISION_VERDICT" in
  PARTIAL)
    say "recreate PARTIAL: could not reconcile:$(printf ' %s' $SKIPPED_NAMES)"
    say "   every OTHER declared topic WAS created/updated; mirrors that copy one of those topics stay paused."
    ;;
  FAIL)
    # One message for every way the recreate can be unusable, including `apply-topics.sh exited 9 but
    # named no skipped topics`, which the decision treats as a failure precisely because it cannot be
    # told apart from a partial success any other way.
    say "recreate FAILED (apply=$ARC ensure=$ERC) — mirrors stay PAUSED (list: $PAUSED_LIST); no bring-up."
    [ "$ARC" -eq 9 ] 2>/dev/null && [ -z "$SKIPPED_NAMES" ] && \
      say "   (it exited with the skip status but attested no skip ending, so which topics are unreconciled — and therefore which mirrors are safe — is unknowable)"
    exit "$DECISION_EXIT"
    ;;
esac

# ---- 4. mirrors back ----
# Every paused agent EXCEPT those that copy a topic in the hold set. On a full success the hold set is
# empty (apply-topics.sh reconciled the whole declaration), so this is the old unconditional resume.
# Gated on the decision's OWN resume field rather than on "we got past the FAIL arm": the two agree
# today, and a later edit that reorders the arms would otherwise silently start every mirror.
RESUME_RC=0
if [ "$DECISION_RESUME" = yes ]; then
  # Its status is carried to THIS SCRIPT'S exit status, not just logged (deploy Codex round 3): an
  # agent that should be running and is not must not leave a run looking clean. It does not block the
  # bring-up below — prod coming up does not depend on the es4 copy path, and holding prod down over a
  # launchd agent would be a worse trade — but the run ends non-zero and says so last.
  if ! mirror_ledger_resume "$PAUSED_LIST" "$DEPLOY_SRC/scripts/ops/mirror-topic-filter.sh" $DECISION_HOLD; then
    RESUME_RC=1
    say "WARN: not every paused mirror agent is running — see the lines above; $PAUSED_LIST still lists the ones that are not."
  fi
else
  say "mirrors stay PAUSED (decision resume=$DECISION_RESUME)"
fi

# ---- 5. bring-up ----
# A PARTIAL recreate stops here, and stops BEFORE the bring-up: the owner rule is that a reset which
# did not complete stays DOWN. Step 4 above is deliberately on the other side of this line — resuming a
# copy path this script paused itself is not bringing prod up.
if [ "$DECISION_BRINGUP" != yes ]; then
  say "no bring-up: a reset that did not fully reconcile stays DOWN. Fix the drift above, then rerun."
  say "=== prod clean-slate $DECISION_VERDICT (exit $DECISION_EXIT) ==="
  exit "$DECISION_EXIT"
fi
if [ "$AFTER" = up ]; then
  say "full bring-up: systemctl restart oe-boot-bringup (waves + partition doctor)"
  ssh_root "systemctl restart oe-boot-bringup; for i in \$(seq 1 90); do [ \"\$(systemctl is-active oe-boot-bringup)\" != activating ] && break; sleep 10; done; journalctl -u oe-boot-bringup --since \"-25 min\" --no-pager -o cat | grep -E \"wave:|result:|doctor|REPAIR|UNREPAIRABLE|done\" | tail -8" | tee -a "$LOG"
else
  say "left at 0 (wipe down) — bring up with: ssh root@.252 systemctl restart oe-boot-bringup"
fi
if [ "$RESUME_RC" -ne 0 ]; then
  say "=== prod clean-slate FINISHED WITH A MIRROR STILL DOWN (exit 1) — $PAUSED_LIST lists it ==="
  exit 1
fi
say "=== prod clean-slate DONE ==="
