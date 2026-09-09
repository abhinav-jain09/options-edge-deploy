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

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# WHY THERE IS NO AUTOMATIC ROLLBACK ANY MORE.
#
# This installer has now been wrong in fourteen distinct ways across four review rounds, and every
# single defect was in code written to fix the previous one. The last five — a RESTORED flag set
# before the restore succeeded, a restore that aborts halfway under set -e, no host-wide lock, a
# dangling symlink classified as absent, and a verification that let the caller override the
# environment it claimed to reproduce — were ALL edge cases of one idea: undo the change
# automatically if anything goes wrong.
#
# A transactional file-and-daemon swap with rollback is a distributed-systems problem written in
# bash, and its edges are unbounded: every round found another. So the idea is gone. What replaces it
# is an ordering that has almost no edges:
#
#   1. Verify BEFORE touching the host, in the environment launchd will actually provide.
#   2. Refuse anything ambiguous — a destination that is not a plain file, a plist that does not
#      parse, an interpreter that is not executable, another installer already running.
#   3. Write, load, and check registration.
#   4. If step 3 fails, say EXACTLY what is on the host and where the previous copies are, and stop.
#      A person with a two-line instruction recovers correctly; an automatic rollback with fourteen
#      edge cases does not.
#
# The backup is kept in a DURABLE directory that is printed, not a temp dir a trap deletes.
# ─────────────────────────────────────────────────────────────────────────────────────────────────

# ONE INSTALLER AT A TIME (r26 #2). Two concurrent runs could back up different intermediate states
# and leave a mixture that never existed. mkdir is atomic on every filesystem that matters.
LOCK="${OE_WATCHDOG_INSTALL_LOCK:-${TMPDIR:-/tmp}/oe-install-dev-mac-watchdog.lock}"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "REFUSING: another installer holds $LOCK. If you are sure none is running, remove that directory." >&2
  exit 1
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT

# THE PLIST DECLARES THE COMMAND launchd WILL ACTUALLY RUN, and that is the only command worth
# verifying (r24 #2). The old check ran the destination script with the CALLER's bash, while launchd
# runs the interpreter and the absolute path written in ProgramArguments — so the installer could
# pass while the agent's real command was absent or stale.
plutil -lint "$SRC/launchd/$PLIST" >/dev/null 2>&1 || {
  echo "FATAL: $SRC/launchd/$PLIST does not parse. launchd would reject it silently." >&2; exit 1; }
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
  exit 1
fi

# EVERY DESTINATION PATH MUST BE A PLAIN FILE OR ABSENT (r26 #3). `[ -f ]` calls a dangling symlink
# absent, `cp` then writes THROUGH it to wherever it points, and no notion of "the previous state"
# survives that. Anything else here is ambiguous, and ambiguity is what this script must not resolve
# by guessing.
for f in $FILES; do
  if [ -e "$DEST/$f" ] || [ -L "$DEST/$f" ]; then
    [ -f "$DEST/$f" ] && [ ! -L "$DEST/$f" ] || {
      echo "REFUSING: $DEST/$f exists and is not a plain file (symlink, directory, or dangling link)." >&2
      echo "          Remove or resolve it first — writing through it would put the watchdog somewhere else." >&2
      exit 1; }
  fi
done
if [ -e "$AGENTS/$PLIST" ] || [ -L "$AGENTS/$PLIST" ]; then
  [ -f "$AGENTS/$PLIST" ] && [ ! -L "$AGENTS/$PLIST" ] || {
    echo "REFUSING: $AGENTS/$PLIST exists and is not a plain file." >&2; exit 1; }
fi

STAGE="$(mktemp -d)"
for f in $FILES; do cp "$SRC/$f" "$STAGE/$f"; done
chmod +x "$STAGE/calibration-progress-watch.sh"

# VERIFY IN launchd'S ENVIRONMENT, AND ONLY launchd'S (r25 #3, r26 #4). It used to inherit the
# operator's PATH, and then — worse — it injected the caller's ENV/CHECK_DATE/ARCHIVE_DIR after the
# plist's own keys, so an operator could verify a different environment, root or date than the agent
# will ever receive, and even override the plist's ENV. launchd starts a user agent with almost
# nothing plus EnvironmentVariables. Nothing from this shell reaches the verification.
# NUL-delimited, read into an ARRAY, and placed BEFORE the command. A first version piped these into
# `xargs -0 env -i ... interp script`, and xargs appends its input to the END of the command line — so
# every plist variable became an ARGUMENT to the watchdog instead of part of its environment, and the
# verification ran with no PATH and no ARCHIVE_DIR at all. It then resolved the operator's REAL NAS
# and today's date, and passed. The test caught it; the log line naming the resolved root is what made
# it visible, which is the whole reason that line exists.
_plist_env_pairs=()
while IFS= read -r -d '' _kv; do _plist_env_pairs+=("$_kv"); done < <(python3 -c "
import plistlib, sys
d = plistlib.load(open(sys.argv[1],'rb')).get('EnvironmentVariables', {})
for k, v in d.items():
    sys.stdout.write('%s=%s\0' % (k, v))" "$SRC/launchd/$PLIST")
echo "verify: running the staged copy as launchd would ($PLIST_INTERP, with only the plist's environment)"
set +e
out="$(env -i HOME="$HOME" USER="${USER:-}" "${_plist_env_pairs[@]}" \
        "$PLIST_INTERP" "$STAGE/calibration-progress-watch.sh" 2>&1)"; rc=$?
set -e
printf '%s\n' "$out" | sed 's/^/    /'

case "$out" in
  *"cannot run"*) echo "REFUSING TO INSTALL: the watchdog cannot run on this host (see above). Nothing was changed." >&2; exit 1 ;;
esac
for _bad in "Traceback (most recent call last)" "command not found" "syntax error" "unbound variable" "No such file or directory"; do
  case "$out" in
    *"$_bad"*) echo "REFUSING TO INSTALL: the watchdog did not start — '$_bad' in its output. Nothing was changed." >&2; exit 1 ;;
  esac
done
case "$out" in
  *"archiveStatus="*|*"ALERT:"*) : ;;
  *) echo "REFUSING TO INSTALL: the watchdog produced no verdict and no alert (exit $rc). It did not run. Nothing was changed." >&2; exit 1 ;;
esac
case "$rc" in
  0|1) : ;;
  *) echo "REFUSING TO INSTALL: unexpected exit $rc — the watchdog exits 0 or 1, so this is not one of its own outcomes. Nothing was changed." >&2; exit 1 ;;
esac
echo "verify: it started and reached one of its own outcomes (exit $rc — nonzero is normal when the day did not land)"

# Only now touch the host. The previous copies go somewhere DURABLE and printed — a temp directory a
# trap deletes is not a backup, it is the appearance of one.
BACKUP="${OE_WATCHDOG_BACKUP_DIR:-$DEST/.watchdog-backup-$(date -u '+%Y%m%dT%H%M%SZ')}"
mkdir -p "$DEST" "$AGENTS" "$BACKUP"
for f in $FILES; do [ -f "$DEST/$f" ] && cp "$DEST/$f" "$BACKUP/$f"; done
[ -f "$AGENTS/$PLIST" ] && cp "$AGENTS/$PLIST" "$BACKUP/$PLIST"
echo "backup: previous copies (if any) are in $BACKUP"

_label="$(python3 -c "import plistlib,sys;print(plistlib.load(open(sys.argv[1],'rb'))['Label'])" "$SRC/launchd/$PLIST")"
launchctl unload "$AGENTS/$PLIST" 2>/dev/null || true
for f in $FILES; do cp "$STAGE/$f" "$DEST/$f"; done
chmod +x "$DEST/calibration-progress-watch.sh"
cp "$SRC/launchd/$PLIST" "$AGENTS/$PLIST"
rm -rf "$STAGE"

# COUNTING IS NOT CHECKING (r21 #2), AND THIS LABEL, NOT ANYTHING CONTAINING IT (r23 #2).
# launchctl list is PID<TAB>status<TAB>label.
say_state() {
  echo "  the host now has: the NEW files in $DEST and the NEW plist in $AGENTS" >&2
  echo "  the previous copies are in $BACKUP" >&2
  echo "  to put back exactly what was there:" >&2
  echo "      launchctl unload $AGENTS/$PLIST 2>/dev/null; cp $BACKUP/* $DEST/ 2>/dev/null" >&2
  echo "      cp $BACKUP/$PLIST $AGENTS/ 2>/dev/null && launchctl load $AGENTS/$PLIST" >&2
  echo "  (this script does NOT do that for you on purpose: an automatic rollback here was wrong in" >&2
  echo "   five different ways across two review rounds, and a half-finished one is worse than none.)" >&2
}
if ! launchctl load "$AGENTS/$PLIST"; then
  echo "INSTALL FAILED: launchctl could not load $AGENTS/$PLIST." >&2
  say_state
  exit 1
fi
_n="$(launchctl list 2>/dev/null | awk -F'\t' -v l="$_label" '$3 == l' | grep -c . || true)"
if [ "${_n:-0}" -ne 1 ]; then
  echo "INSTALL FAILED: launchctl reports ${_n:-0} agents with the label $_label, expected exactly 1." >&2
  say_state
  exit 1
fi
echo "loaded: 1 agent with the label $_label, scheduled 07:00 local"
