#!/usr/bin/env bash
# install-dev-mac-watchdog.sh — the repo-controlled way to put A5.7's watchdog on the dev Mac.
#
# WHY THIS EXISTS (r19 #2). The watchdog must run on a DIFFERENT host from the reporter, so the .252
# archive unit deliberately does not carry it — and that left it with no controlled install path at
# all. Its plist documented a manual copy, the completeness validator exempted it by name, and the
# result was a watchdog that existed in git and nowhere else: not installed here, and nothing in the
# merged checkout able to notice. "It runs somewhere else" became "it runs nowhere".
#
# This is a HOST action, run deliberately by a person on the dev Mac. It is not a deploy and no
# Jenkins job calls it. What it buys is that the copy on the host is reproducible from the repo, and
# that installing it PROVES it can run instead of hoping.
#
# Usage:   bash scripts/ops/archive/install-dev-mac-watchdog.sh [--dry-run]
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${OE_OPS_DIR:-$HOME/oe-ops}"
AGENTS="${LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
PLIST="com.optionsedge.calibration-progress-watch.plist"
DRY=false; [ "${1:-}" = "--dry-run" ] && DRY=true

# EVERY file the watchdog reads from beside itself. It SOURCES oe-alert.sh (so a missing copy silently
# downgrades every alert to a log line) and it SOURCES calibration-targets.env (so a missing copy makes
# it refuse to run rather than watch whichever cohort is on disk). Both are part of the install or the
# install is a lie. validate-dev-mac-watchdog.sh asserts this list against the script itself.
FILES="calibration-progress-watch.sh oe-alert.sh calibration-targets.env oe_corpus_reader.py"

for f in $FILES; do
  [ -f "$SRC/$f" ] || { echo "FATAL: $f is not in the repo beside this installer" >&2; exit 1; }
done
[ -f "$SRC/launchd/$PLIST" ] || { echo "FATAL: $SRC/launchd/$PLIST missing" >&2; exit 1; }

echo "install: $FILES -> $DEST"
echo "install: launchd/$PLIST -> $AGENTS"
if [ "$DRY" = true ]; then echo "(dry run — nothing written)"; exit 0; fi

# THE PLIST DECLARES THE COMMAND launchd WILL ACTUALLY RUN, and that is the only command worth
# verifying (r24 #2). The old check ran "$DEST/calibration-progress-watch.sh" with the CALLER's bash,
# while launchd runs the interpreter and the absolute path written in ProgramArguments — so the
# installer could pass while the agent's real command was absent or stale, and the test's own healthy
# case passed for exactly that reason. Read both out of the plist and hold the install to them.
read -r PLIST_INTERP PLIST_SCRIPT <<EOF
$(python3 -c "
import plistlib,sys
a = plistlib.load(open(sys.argv[1],'rb'))['ProgramArguments']
print(a[0], a[1])" "$SRC/launchd/$PLIST")
EOF
[ -n "${PLIST_INTERP:-}" ] && [ -n "${PLIST_SCRIPT:-}" ] || {
  echo "FATAL: could not read ProgramArguments from $SRC/launchd/$PLIST" >&2; exit 1; }
[ -x "$PLIST_INTERP" ] || {
  echo "FATAL: the plist runs $PLIST_INTERP, which is not executable on this host. launchd would fail silently every morning." >&2; exit 1; }
if [ "$PLIST_SCRIPT" != "$DEST/calibration-progress-watch.sh" ]; then
  echo "FATAL: the plist runs $PLIST_SCRIPT but this installer installs to $DEST." >&2
  echo "       Installing now would register an agent pointing at a copy nobody updates." >&2
  echo "       Fix the plist, or set OE_OPS_DIR to the directory the plist names." >&2
  exit 1
fi

# STAGE, VERIFY, THEN REPLACE. The first version copied over the LIVE files and only then ran its
# checks, so a refusal left a previously-working agent pointing at the rejected copy — the installer
# broke the very installation it declined to replace (r24 #1). Nothing live is touched until the
# staged copy has proven it runs, and a failure after that point restores what was there.
STAGE="$(mktemp -d)"
BACKUP="$(mktemp -d)"
# (the EXIT trap is installed below, once a rollback is possible — see restore())
trap 'rm -rf "$STAGE" "$BACKUP"' EXIT
for f in $FILES; do cp "$SRC/$f" "$STAGE/$f"; done
chmod +x "$STAGE/calibration-progress-watch.sh"

# PROVE IT RUNS, using the plist's own interpreter, against the STAGED copy. RunAtLoad is false, so
# loading the agent demonstrates nothing; an install that only copies files is how a watchdog reaches
# production broken. A non-zero exit here is fine and expected on a day with no progress record —
# what is NOT fine is the script failing to START.
# THE PLIST'S ENVIRONMENT, NOT THE OPERATOR'S (r25 #3). Verification used to inherit the caller's
# PATH and pass only ENV, so a plist whose PATH lacks python3 verified perfectly from a terminal and
# failed every single morning under launchd — the exact silent failure this watchdog exists to make
# impossible. launchd starts a user agent with almost nothing plus EnvironmentVariables, so reproduce
# that: env -i, HOME and USER (which launchd does set), and every key the plist declares.
_plist_env() {
  python3 -c "
import plistlib, sys, shlex
d = plistlib.load(open(sys.argv[1],'rb')).get('EnvironmentVariables', {})
print(' '.join('%s=%s' % (k, shlex.quote(str(v))) for k, v in d.items()))" "$SRC/launchd/$PLIST"
}
PLIST_ENV="$(_plist_env)"
echo "verify: running the staged copy as launchd would ($PLIST_INTERP, with the plist's environment)"
set +e
out="$(eval env -i HOME=\"\$HOME\" USER=\"\${USER:-}\" $PLIST_ENV \
        ENV=\"\${ENV:-prod}\" ${CHECK_DATE:+CHECK_DATE=\"$CHECK_DATE\"} \
        ${ARCHIVE_DIR:+ARCHIVE_DIR=\"$ARCHIVE_DIR\"} ${ARCHIVE_ROOT_CANDIDATES:+ARCHIVE_ROOT_CANDIDATES=\"$ARCHIVE_ROOT_CANDIDATES\"} \
        \"$PLIST_INTERP\" \"$STAGE/calibration-progress-watch.sh\" 2>&1)"; rc=$?
set -e
printf '%s\n' "$out" | sed 's/^/    /'

# A deliberate refusal. Every refusal in the watchdog shares this phrase so one marker covers all of
# them; validate-dev-mac-watchdog.sh asserts that is still true.
case "$out" in
  *"cannot run"*) echo "REFUSING TO INSTALL: the watchdog cannot run on this host (see above). Nothing was changed. Fix that first." >&2; exit 1 ;;
esac
# Did not start, however it failed. These are the shapes a broken copy actually produces.
for _bad in "Traceback (most recent call last)" "command not found" "syntax error" "unbound variable" "No such file or directory"; do
  case "$out" in
    *"$_bad"*) echo "REFUSING TO INSTALL: the watchdog did not start — '$_bad' in its output. Nothing was changed." >&2; exit 1 ;;
  esac
done
# Reached one of its OWN terminal states: it either completed the evaluation (archiveStatus=) or
# raised an alert about the day (ALERT:). Neither appears if it died on the way there.
case "$out" in
  *"archiveStatus="*|*"ALERT:"*) : ;;
  *) echo "REFUSING TO INSTALL: the watchdog produced no verdict and no alert (exit $rc). It did not run. Nothing was changed." >&2; exit 1 ;;
esac
case "$rc" in
  0|1) : ;;
  *) echo "REFUSING TO INSTALL: unexpected exit $rc — the watchdog exits 0 or 1, so this is not one of its own outcomes. Nothing was changed." >&2; exit 1 ;;
esac
echo "verify: it started and reached one of its own outcomes (exit $rc — nonzero is normal when the day did not land)"

# Only now touch the host. Everything from here is REVERSIBLE, and the revert has to survive the ways
# this script can stop — not just the ways it chooses to stop.
#
# r25 #1: the EXIT trap used to delete STAGE and BACKUP and nothing else, so any `set -e` failure
# during the live copies, the chmod, the plist install or `launchctl load` exited WITHOUT restoring —
# leaving a partial or outright rejected installation live. The trap now performs the rollback.
# r25 #2: and the rollback was not state-preserving. Files and a plist that did NOT exist before were
# never removed, so a failed FIRST install left the rejected copy behind; and it reloaded without
# unloading the rejected agent, so launchd kept the configuration it had just been given.
REPLACING=false
RESTORED=false
BACKED_UP=""          # the names that existed BEFORE, so absence is restorable too
PLIST_EXISTED=false

restore() {
  [ "$REPLACING" = true ] || return 0
  [ "$RESTORED" = false ] || return 0
  RESTORED=true
  echo "rolling back to the previous installation" >&2
  # Unload FIRST: launchd is holding the rejected configuration, and loading over it changes nothing.
  launchctl unload "$AGENTS/$PLIST" 2>/dev/null || true
  for f in $FILES; do
    case " $BACKED_UP " in
      *" $f "*) cp "$BACKUP/$f" "$DEST/$f" ;;
      *)        rm -f "$DEST/$f" ;;          # it did not exist before; leaving it is not "restored"
    esac
  done
  if [ "$PLIST_EXISTED" = true ]; then
    cp "$BACKUP/$PLIST" "$AGENTS/$PLIST"
    launchctl load "$AGENTS/$PLIST" 2>/dev/null || true
  else
    rm -f "$AGENTS/$PLIST"                    # there was no agent before; there must be none after
  fi
}
cleanup() {
  local rc=$?
  [ "$rc" -eq 0 ] || restore
  rm -rf "$STAGE" "$BACKUP"
}
trap cleanup EXIT

mkdir -p "$DEST" "$AGENTS"
for f in $FILES; do
  if [ -f "$DEST/$f" ]; then cp "$DEST/$f" "$BACKUP/$f"; BACKED_UP="$BACKED_UP $f"; fi
done
if [ -f "$AGENTS/$PLIST" ]; then cp "$AGENTS/$PLIST" "$BACKUP/$PLIST"; PLIST_EXISTED=true; fi

REPLACING=true
for f in $FILES; do cp "$STAGE/$f" "$DEST/$f"; done
chmod +x "$DEST/calibration-progress-watch.sh"
cp "$SRC/launchd/$PLIST" "$AGENTS/$PLIST"

launchctl unload "$AGENTS/$PLIST" 2>/dev/null || true
launchctl load "$AGENTS/$PLIST"
# COUNTING IS NOT CHECKING (r21 #2). This printed the count and never required it, so
# "loaded: 0 agent(s)" finished successfully and left nothing scheduled — the exact outcome the
# installer exists to prevent.
# EXACTLY THIS LABEL, not anything containing it. An unanchored substring count let a similarly named
# agent — com.optionsedge.calibration-progress-watch.backup, say — satisfy "exactly one" while the
# label this plist actually declares was absent (r23 #2). launchctl list is PID<TAB>status<TAB>label,
# so compare the label column.
_label="$(python3 -c "import plistlib,sys;print(plistlib.load(open(sys.argv[1],'rb'))['Label'])" "$SRC/launchd/$PLIST")"
_n="$(launchctl list 2>/dev/null | awk -F'\t' -v l="$_label" '$3 == l' | grep -c . || true)"
if [ "${_n:-0}" -ne 1 ]; then
  echo "INSTALL FAILED: launchctl reports $_n agents with the label $_label, expected exactly 1." >&2
  echo "                The new files were written but nothing is scheduled." >&2
  restore
  exit 1
fi
echo "loaded: 1 agent with the label $_label, scheduled 07:00 local"
