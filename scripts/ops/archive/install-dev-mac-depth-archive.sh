#!/usr/bin/env bash
# install-dev-mac-depth-archive.sh — the repo-controlled way to put the DEV depth archive on the dev Mac.
#
# WHY THIS EXISTS. The depth archive (launchd/oe-archive-depth.plist, oe-archive-depth.sh) runs on the dev Mac
# under launchd, not on .252 under the crontab, so the archive deploy job's UNIT does not carry it — and a file
# the unit does not carry has no controlled install path at all. The watchdog beside it went through exactly that
# ("in git and installed nowhere", r19 #2); this installer is the same answer for the same reason: the copy on
# the host is reproducible from the repo, and installing it PROVES it can start instead of hoping.
#
# This is a HOST action, run deliberately by a person on the dev Mac. It is not a deploy and no Jenkins job
# calls it. It follows the watchdog installer's ordering and none of its rollback machinery (see that file for
# the fourteen reasons): verify BEFORE touching the host, refuse anything ambiguous, write + load + check, and
# on failure say exactly what is on the host and where the previous copies are.
#
# Usage:   bash scripts/ops/archive/install-dev-mac-depth-archive.sh [--dry-run]
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${OE_OPS_DIR:-$HOME/oe-ops}"
AGENTS="${LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
PLIST="oe-archive-depth.plist"
WRAPPER="oe-archive-depth.sh"
DRY=false; [ "${1:-}" = "--dry-run" ] && DRY=true

# EVERY committed file the wrapper and the archiver read from beside themselves for THIS set. The wrapper
# sources oe-topics.env (the set) and oe-alert.sh (delivery) and runs oe-archive-kafka.sh; the archiver reads
# oe-topics.env and oe-alert.sh beside itself too, and for the depth set's committed reads it runs
# StrikeArchiveReader.java — from source for the tape, and from the helper jar for the three canonical Avro topics,
# whose decoder source CanonicalArchiveDecoder.java is compiled into that jar and shipped beside the archiver as
# the record of what the jar holds. validate-dev-mac-depth-archive.sh asserts this list against the scripts.
FILES="oe-archive-depth.sh oe-archive-kafka.sh oe-topics.env oe-alert.sh StrikeArchiveReader.java CanonicalArchiveDecoder.java oe-archive-session-chain.py"
# NOT committed, and NOT produced here: the canonical archive helper (StrikeArchiveReader + CanonicalArchiveDecoder
# compiled against the contracts) and the contracts jar. Jenkins archive-scripts-deploy builds both from the
# permitted contracts commit and ships them to .252's oe-ops; the same two files must be placed in $DEST on this
# host (copied from that build, or from .252:/home/abhinav/oe-ops) BEFORE this installer runs. Without them the
# archiver refuses every es.rithmic.depth, es.rithmic.mbo, futures.liquidity-wall.closures and es.mbo.* capture ("canonical archive
# SDK/helper missing"), so installing without them would schedule a job that fails four times a day.
SIDECARS="canonical-archive-reader.jar options-edge-contracts.jar"

for f in $FILES; do
  [ -f "$SRC/$f" ] || { echo "FATAL: $f is not in the repo beside this installer" >&2; exit 1; }
done
[ -f "$SRC/launchd/$PLIST" ] || { echo "FATAL: $SRC/launchd/$PLIST missing" >&2; exit 1; }

echo "install: $FILES -> $DEST"
echo "install: launchd/$PLIST -> $AGENTS"
echo "require: $SIDECARS already in $DEST (built and shipped by Jenkins archive-scripts-deploy; not produced here)"
if [ "$DRY" = true ]; then echo "(dry run — nothing written)"; exit 0; fi

# ONE INSTALLER AT A TIME, with the lock derived from the destination (the watchdog installer's r26/r27).
LOCK="${TMPDIR:-/tmp}/oe-install-dev-mac-depth-archive.$(printf '%s' "$DEST" | cksum | cut -d' ' -f1).lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "REFUSING: another installer holds $LOCK for $DEST. If you are sure none is running, remove that directory." >&2
  exit 1
fi
LOCK_TOKEN="$$-$(date -u '+%s')"
printf '%s\n' "$LOCK_TOKEN" > "$LOCK/owner"
trap '[ "$(cat "$LOCK/owner" 2>/dev/null)" = "$LOCK_TOKEN" ] && rm -rf "$LOCK" || true' EXIT

# THE PLIST DECLARES THE COMMAND launchd WILL ACTUALLY RUN, and that is the only command worth verifying. Every
# key this installer depends on is read HERE, before a single directory is created.
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
cal = d.get('StartCalendarInterval')
if not isinstance(cal, list) or len(cal) != 4:
    sys.exit(3)
print(lab)" "$SRC/launchd/$PLIST")" || {
  echo "FATAL: $SRC/launchd/$PLIST parses but is not the depth agent: it needs a non-empty Label, at least two string ProgramArguments and FOUR StartCalendarInterval entries." >&2
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
  echo "FATAL: the plist runs $PLIST_INTERP, which is not executable on this host. launchd would fail silently four times a day." >&2; exit 1; }
if [ "$PLIST_SCRIPT" != "$DEST/$WRAPPER" ]; then
  echo "FATAL: the plist runs $PLIST_SCRIPT but this installer installs to $DEST." >&2
  echo "       Installing now would register an agent pointing at a copy nobody updates." >&2
  exit 1
fi

# THE SIDECARS MUST ALREADY BE THERE, as plain files. The installer cannot build them (that needs the permitted
# contracts commit and the Jenkins job's guards); it can only refuse to schedule a job that would fail without them.
for j in $SIDECARS; do
  [ -f "$DEST/$j" ] && [ ! -L "$DEST/$j" ] || {
    echo "REFUSING: $DEST/$j is missing (or not a plain file). The archiver refuses every canonical Avro capture" >&2
    echo "          (es.rithmic.depth, es.rithmic.mbo, futures.liquidity-wall.closures, es.mbo.*) without it. Copy the build Jenkins" >&2
    echo "          archive-scripts-deploy shipped (.252:/home/abhinav/oe-ops/$j) into $DEST first." >&2
    exit 1; }
done

# EVERY DESTINATION PATH MUST BE A PLAIN FILE OR ABSENT (a dangling symlink reads as absent to -f, and cp then
# writes THROUGH it to wherever it points).
for f in $FILES; do
  if [ -e "$DEST/$f" ] || [ -L "$DEST/$f" ]; then
    [ -f "$DEST/$f" ] && [ ! -L "$DEST/$f" ] || {
      echo "REFUSING: $DEST/$f exists and is not a plain file (symlink, directory, or dangling link). Resolve it first." >&2
      exit 1; }
  fi
done
if [ -e "$AGENTS/$PLIST" ] || [ -L "$AGENTS/$PLIST" ]; then
  [ -f "$AGENTS/$PLIST" ] && [ ! -L "$AGENTS/$PLIST" ] || {
    echo "REFUSING: $AGENTS/$PLIST exists and is not a plain file." >&2; exit 1; }
fi

STAGE="$(mktemp -d)"
for f in $FILES; do cp "$SRC/$f" "$STAGE/$f"; done
chmod +x "$STAGE/$WRAPPER" "$STAGE/oe-archive-kafka.sh"

# VERIFY IN launchd'S ENVIRONMENT, AND ONLY launchd'S. The staged wrapper is run as the plist's interpreter with
# nothing but HOME, USER and the plist's own EnvironmentVariables — plus the archiver's PRINT_TOPICS seam, placed
# BEFORE the plist's variables so the plist wins. With that seam the archiver prints the EFFECTIVE topic set and
# exits before touching the broker or the NAS; the wrapper's own guards still run first, so this proves, on this
# host, that the policy file defines the set, that an archive root with the NAS sentinel is reachable, and that the
# command launchd will run starts and derives exactly OE_DEPTH_TOPICS_dev. A wrapper that prints anything else, or
# nothing, is not installed.
_plist_env_pairs=()
while IFS= read -r -d '' _kv; do _plist_env_pairs+=("$_kv"); done < <(python3 -c "
import plistlib, sys
d = plistlib.load(open(sys.argv[1],'rb')).get('EnvironmentVariables', {})
for k, v in d.items():
    sys.stdout.write('%s=%s\0' % (k, v))" "$SRC/launchd/$PLIST")
expected="$(unset OE_DEPTH_TOPICS_dev; . "$SRC/oe-topics.env"; printf '%s' "${OE_DEPTH_TOPICS_dev:-}")"
[ -n "$expected" ] || { echo "FATAL: $SRC/oe-topics.env defines no OE_DEPTH_TOPICS_dev — there is no set to schedule" >&2; rm -rf "$STAGE"; exit 1; }
echo "verify: running the staged copy as launchd would ($PLIST_INTERP, with only the plist's environment, PRINT_TOPICS seam)"
set +e
out="$(env -i HOME="$HOME" USER="${USER:-}" PRINT_TOPICS=true LOG=/dev/null "${_plist_env_pairs[@]}" \
        "$PLIST_INTERP" "$STAGE/$WRAPPER" 2>&1)"; rc=$?
set -e
printf '%s\n' "$out" | sed 's/^/    /'
for _bad in "Traceback (most recent call last)" "command not found" "syntax error" "unbound variable" "No such file or directory" "FATAL:"; do
  case "$out" in
    *"$_bad"*) echo "REFUSING TO INSTALL: the wrapper did not start cleanly — '$_bad' in its output. Nothing was changed." >&2; rm -rf "$STAGE"; exit 1 ;;
  esac
done
derived="$(printf '%s\n' "$out" | grep -vE '^\[' | grep -vE '^$' | tail -1)"
if [ "$rc" -ne 0 ] || [ "$derived" != "$expected" ]; then
  echo "REFUSING TO INSTALL: the staged wrapper did not derive the depth set (exit $rc; got '${derived:-<nothing>}', wanted '$expected'). Nothing was changed." >&2
  rm -rf "$STAGE"; exit 1
fi
echo "verify: it started under launchd's environment and derived exactly OE_DEPTH_TOPICS_dev"

# Only now touch the host. Previous copies go somewhere DURABLE and printed.
BACKUP="${OE_DEPTH_BACKUP_DIR:-$DEST/.depth-archive-backup-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
mkdir -p "$DEST" "$AGENTS"
if ! mkdir "$BACKUP" 2>/dev/null; then
  echo "REFUSING: the backup directory $BACKUP already exists — writing into it would overwrite the record of an earlier install's previous state." >&2
  rm -rf "$STAGE"; exit 1
fi
for f in $FILES; do [ -f "$DEST/$f" ] && cp "$DEST/$f" "$BACKUP/$f"; done
[ -f "$AGENTS/$PLIST" ] && cp "$AGENTS/$PLIST" "$BACKUP/$PLIST"
echo "backup: previous copies (where any existed) are in $BACKUP"

launchctl unload "$AGENTS/$PLIST" 2>/dev/null || true
for f in $FILES; do cp "$STAGE/$f" "$DEST/$f"; done
chmod +x "$DEST/$WRAPPER" "$DEST/oe-archive-kafka.sh" "$DEST/oe-archive-session-chain.py"
# The exchange calendar the session checker reads (closed days hold no session) lives in scripts/jenkins in the
# repo; put the committed copy beside the checker when this checkout has it (the checker falls back to weekdays only,
# and says so, when it is absent).
if [ -f "$SRC/../../jenkins/market_calendar.py" ]; then cp "$SRC/../../jenkins/market_calendar.py" "$DEST/market_calendar.py"; fi
cp "$SRC/launchd/$PLIST" "$AGENTS/$PLIST"
rm -rf "$STAGE"

say_state() {
  echo "  the host now has: the NEW files in $DEST and the NEW plist in $AGENTS" >&2
  echo "  the previous copies are in $BACKUP (copy them back by hand to restore; absent there = absent before)" >&2
}
if ! launchctl load "$AGENTS/$PLIST"; then
  echo "INSTALL FAILED: launchctl could not load $AGENTS/$PLIST." >&2
  say_state
  exit 1
fi
# COUNTING IS NOT CHECKING, AND THIS LABEL, NOT ANYTHING CONTAINING IT. launchctl list is PID<TAB>status<TAB>label.
_n="$(launchctl list 2>/dev/null | awk -F'\t' -v l="$_label" '$3 == l' | grep -c . || true)"
if [ "${_n:-0}" -ne 1 ]; then
  echo "INSTALL FAILED: launchctl reports ${_n:-0} agents with the label $_label, expected exactly 1." >&2
  say_state
  exit 1
fi
echo "loaded: 1 agent with the label $_label, scheduled 02:00, 08:00, 14:00 and 20:00 in the host's LOCAL time (every six hours; those are ET hours only on an America/New_York host)"
