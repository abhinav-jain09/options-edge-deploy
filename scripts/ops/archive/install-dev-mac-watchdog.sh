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

mkdir -p "$DEST" "$AGENTS"
for f in $FILES; do cp "$SRC/$f" "$DEST/$f"; done
chmod +x "$DEST/calibration-progress-watch.sh"
cp "$SRC/launchd/$PLIST" "$AGENTS/$PLIST"

# PROVE IT RUNS. RunAtLoad is false, so loading the agent demonstrates nothing; an install that only
# copies files is how a watchdog reaches production broken. A non-zero exit here is fine and expected
# on a day with no progress record — what is NOT fine is the script failing to START.
#
# THE FIRST VERSION OF THIS DID NOT PROVE ANYTHING (r21 #1). It rejected only output containing
# "cannot run", so a Python traceback, a missing command or a syntax error all read as "reached a
# verdict" and the broken copy was loaded anyway. A verification that accepts almost every failure is
# the same defect as the watchdog it installs: a check that reads as real and binds nothing. So the
# run has to END in a state this script can NAME, and anything else is a failure to start.
echo "verify: running it once"
set +e
out="$(ENV=prod bash "$DEST/calibration-progress-watch.sh" 2>&1)"; rc=$?
set -e
printf '%s\n' "$out" | sed 's/^/    /'

# A deliberate refusal. Every refusal in the watchdog shares this phrase so one marker covers all of
# them; validate-dev-mac-watchdog.sh asserts that is still true.
case "$out" in
  *"cannot run"*) echo "REFUSING TO LOAD: the watchdog cannot run on this host (see above). Fix that first." >&2; exit 1 ;;
esac
# Did not start, however it failed. These are the shapes a broken copy actually produces.
for _bad in "Traceback (most recent call last)" "command not found" "syntax error" "unbound variable" "No such file or directory"; do
  case "$out" in
    *"$_bad"*) echo "REFUSING TO LOAD: the watchdog did not start — '$_bad' in its output. Installing it now would put a silent watchdog on this host." >&2; exit 1 ;;
  esac
done
# Reached one of its OWN terminal states: it either completed the evaluation (archiveStatus=) or
# raised an alert about the day (ALERT:). Neither appears if it died on the way there.
case "$out" in
  *"archiveStatus="*|*"ALERT:"*) : ;;
  *) echo "REFUSING TO LOAD: the watchdog produced no verdict and no alert (exit $rc). It did not run." >&2; exit 1 ;;
esac
case "$rc" in
  0|1) : ;;
  *) echo "REFUSING TO LOAD: unexpected exit $rc — the watchdog exits 0 or 1, so this is not one of its own outcomes." >&2; exit 1 ;;
esac
echo "verify: it started and reached one of its own outcomes (exit $rc — nonzero is normal when the day did not land)"

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
  echo "INSTALL FAILED: launchctl reports $_n agents with the label $_label, expected exactly 1. The files are in place but nothing is scheduled." >&2
  exit 1
fi
echo "loaded: 1 agent with the label $_label, scheduled 07:00 local"
