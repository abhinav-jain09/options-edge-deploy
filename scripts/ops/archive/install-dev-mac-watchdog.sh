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
# The lock path is DERIVED FROM THE DESTINATION, not chosen by the caller (r27 #2). An env-selectable
# lock is not a lock: two installers pick two paths and run at once. Deriving it means two runs
# targeting the same $DEST always collide, and runs targeting different destinations never do.
LOCK="${TMPDIR:-/tmp}/oe-install-dev-mac-watchdog.$(printf '%s' "$DEST" | cksum | cut -d' ' -f1).lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "REFUSING: another installer holds $LOCK for $DEST." >&2
  echo "          If you are sure none is running, remove that directory." >&2
  exit 1
fi
# Release it only if it is still OURS. An unconditional rmdir removes a REPLACEMENT lock that another
# installer created after someone deleted ours by hand, which is worse than never locking.
LOCK_TOKEN="$$-$(date -u '+%s')"
printf '%s\n' "$LOCK_TOKEN" > "$LOCK/owner"
trap '[ "$(cat "$LOCK/owner" 2>/dev/null)" = "$LOCK_TOKEN" ] && rm -rf "$LOCK" || true' EXIT

# THE PLIST DECLARES THE COMMAND launchd WILL ACTUALLY RUN, and that is the only command worth
# verifying (r24 #2). The old check ran the destination script with the CALLER's bash, while launchd
# runs the interpreter and the absolute path written in ProgramArguments — so the installer could
# pass while the agent's real command was absent or stale.
# plutil -lint checks XML, not the launchd schema (r27 #3). Every key this installer depends on is
# read HERE, before a single directory is created — the Label used to be read after the backups were
# taken, so a parseable plist with a missing Label touched the host and then aborted under set -e,
# which is the opposite of the verify-before-touching contract this rewrite exists to keep.
plutil -lint "$SRC/launchd/$PLIST" >/dev/null 2>&1 || {
  echo "FATAL: $SRC/launchd/$PLIST does not parse. launchd would reject it silently." >&2; exit 1; }
_label="$(python3 -c "
import plistlib, sys
d = plistlib.load(open(sys.argv[1],'rb'))
lab = d.get('Label')
if not isinstance(lab, str) or not lab.strip():
    sys.exit(1)
a = d.get('ProgramArguments')
if not isinstance(a, list) or len(a) < 2 or not all(isinstance(x, str) and x for x in a[:2]):
    sys.exit(2)
print(lab)" "$SRC/launchd/$PLIST")" || {
  echo "FATAL: $SRC/launchd/$PLIST parses but is not a usable launchd job: it needs a non-empty Label and at least two string ProgramArguments." >&2
  exit 1; }
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
# A BACKUP DIRECTORY MUST BE NEW (r28 #2). The default had one-second resolution and the caller could
# hand back a path already in use, and `mkdir -p` plus overwriting copies then REPLACED an earlier
# backup and its restore.sh — destroying the only record of the state that install had replaced. The
# pid makes the default unique within a second, and `mkdir` (no -p) refuses an existing directory
# instead of writing into it.
BACKUP="${OE_WATCHDOG_BACKUP_DIR:-$DEST/.watchdog-backup-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
mkdir -p "$DEST" "$AGENTS"
if ! mkdir "$BACKUP" 2>/dev/null; then
  echo "REFUSING: the backup directory $BACKUP already exists." >&2
  echo "          Writing into it would overwrite the record of an earlier install's previous state," >&2
  echo "          including its restore.sh. Choose another OE_WATCHDOG_BACKUP_DIR, or move that one aside." >&2
  exit 1
fi
for f in $FILES; do [ -f "$DEST/$f" ] && cp "$DEST/$f" "$BACKUP/$f"; done
[ -f "$AGENTS/$PLIST" ] && cp "$AGENTS/$PLIST" "$BACKUP/$PLIST"

# RECOVERY IS A SCRIPT, NOT A PARAGRAPH (r27 #1). It used to be printed advice, and the advice was
# WRONG: it never removed files that had no predecessor, left a new plist where there had been none,
# and its `cp "$BACKUP"/*` copied the backed-up plist into the ops directory. My own test checked only
# that the TEXT existed and never ran it — the same "reads as a check, binds nothing" shape, sitting
# in the thing I wrote to replace the rollback. Generated here, while the pre-install state is still
# known, so recovery restores exactly what was there INCLUDING absence. The test executes it.
{
  echo '#!/usr/bin/env bash'
  echo '# Restores the state that existed before the install this directory belongs to.'
  # FAIL-CLOSED (r28 #1). Without set -e a failed cp, rm or load fell through to the success message
  # and exited 0 — a recovery that reports it restored the previous installation while leaving a mixed
  # one. That is the third time in this branch that something written to report failure could not.
  echo 'set -euo pipefail'
  # Emitted VERBATIM, not through printf %q. %q escaped every space and quote in the trap body, so the
  # generated line read `trap rc=\$\?\;\ case\ \"\$rc\"\ ...` — valid shell, and unreadable. This
  # script exists so a person can read it before running it on their own machine; a correct line nobody
  # can read fails at the only job it has. Found by reading the EMITTED file rather than the generator.
  # Exit 3 is the stale-backup REFUSAL, which touched nothing — claiming a mixed state for it would be
  # a wrong message, and a wrong message is a defect like any other.
  cat <<'RSTRAP'
trap 'rc=$?; case "$rc" in 0|3) : ;; *) echo "RECOVERY FAILED at exit $rc - the host is in a MIXED state; read this script and finish by hand" >&2 ;; esac' EXIT
RSTRAP
  # A STALE BACKUP UNDOES MORE THAN ITS OWN INSTALL. Found by probing rather than by review: with two
  # backup directories present, running the OLDER one's restore.sh removed the watchdog entirely and
  # reported "restored" — an operator picking the wrong directory silently uninstalls the thing, and
  # is told it worked. Each script now refuses when a newer backup exists, because "restore" has to
  # mean the state this run replaced, not some earlier one.
  printf '_self=%s\n' "$(printf '%q' "$BACKUP")"
  printf '_newer="$(ls -d %s/.watchdog-backup-* 2>/dev/null | sort | awk -v s="$_self" \047$0 > s\047 | head -1)"\n' "$(printf '%q' "$DEST")"
  echo 'if [ -n "${_newer:-}" ] && [ -z "${FORCE:-}" ]; then'
  echo '  echo "REFUSING: a NEWER backup exists ($_newer)." >&2'
  echo '  echo "          Restoring this one would also undo the install that made the newer backup," >&2'
  echo '  echo "          and it would report success while doing it. Restore the newest one, or set FORCE=1." >&2'
  echo '  exit 3'
  echo 'fi'
  printf 'launchctl unload %s 2>/dev/null || true
' "$(printf '%q' "$AGENTS/$PLIST")"
  for f in $FILES; do
    if [ -f "$BACKUP/$f" ]; then
      printf 'cp %s %s
' "$(printf '%q' "$BACKUP/$f")" "$(printf '%q' "$DEST/$f")"
    else
      printf 'rm -f %s
' "$(printf '%q' "$DEST/$f")"
    fi
  done
  if [ -f "$BACKUP/$PLIST" ]; then
    printf 'cp %s %s
' "$(printf '%q' "$BACKUP/$PLIST")" "$(printf '%q' "$AGENTS/$PLIST")"
    printf 'launchctl load %s
' "$(printf '%q' "$AGENTS/$PLIST")"
    echo 'echo "restored the previous installation"'
  else
    printf 'rm -f %s
' "$(printf '%q' "$AGENTS/$PLIST")"
    echo 'echo "restored: there was no watchdog installed before, and there is none now"'
  fi
} > "$BACKUP/restore.sh"
chmod +x "$BACKUP/restore.sh"
echo "backup: previous state is in $BACKUP (run $BACKUP/restore.sh to put it back exactly)"

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
  echo "  to put back exactly what was there, run:" >&2
  echo "      $BACKUP/restore.sh" >&2
  echo "  (that script was generated from the state this run found, so it restores absence too. This" >&2
  echo "   installer does NOT run it for you on purpose: an automatic rollback here was wrong in five" >&2
  echo "   different ways across two review rounds, and a half-finished one is worse than none.)" >&2
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
