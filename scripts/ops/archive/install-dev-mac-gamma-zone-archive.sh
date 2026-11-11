#!/usr/bin/env bash
# install-dev-mac-gamma-zone-archive.sh — the repo-controlled way to put the GAMMA ZONE CORPUS
# archive on the dev Mac.
#
# WHY THIS EXISTS. The job runs on the dev Mac under launchd, not on .252 under the crontab, so the
# archive deploy job's unit does not carry it — and a file the unit does not carry has no controlled
# install path at all. The depth archive and the watchdog beside it went through exactly that ("in
# git and installed nowhere"); this installer is the same answer for the same reason: the copy on the
# host is reproducible from the repo, and installing it PROVES it can run instead of hoping.
#
# This is a HOST action, run deliberately by a person on the dev Mac. It is not a deploy and no
# Jenkins job calls it.
#
# SIMPLER THAN THE DEPTH INSTALLER, on purpose: options.spx.gamma-zone.log is JSON on the wire, so
# there is no canonical-archive helper jar, no contracts jar and no Java reader to place. If that
# ever changes, this installer must grow the sidecar refusal the depth one has — a job scheduled
# without its reader fails four times a day and says nothing useful.
#
# Usage:   bash scripts/ops/archive/install-dev-mac-gamma-zone-archive.sh [--dry-run]
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${OE_OPS_DIR:-$HOME/oe-ops}"
AGENTS="${LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
PLIST="oe-archive-gamma-zone.plist"
WRAPPER="oe-archive-gamma-zone.sh"
LABEL="com.optionsedge.oe-archive-gamma-zone"
DRY=false; [ "${1:-}" = "--dry-run" ] && DRY=true

# EVERY committed file the wrapper and the archiver read from beside themselves. The wrapper sources
# oe-topics.env (the topic set) and oe-alert.sh (delivery) and runs oe-archive-kafka.sh; the archiver
# reads oe-topics.env and oe-alert.sh beside itself too. A missing one is a job that fails hourly.
FILES="$WRAPPER oe-archive-kafka.sh oe-topics.env oe-alert.sh"

say() { echo "[install-gamma-zone-archive] $*"; }
die() { echo "[install-gamma-zone-archive] FATAL: $*" >&2; exit 1; }

# ---- verify BEFORE touching the host -------------------------------------------------------
[ "$(uname -s)" = "Darwin" ] || die "this installer is for the dev Mac; uname says $(uname -s)"
for f in $FILES; do
  [ -r "$SRC/$f" ] || die "repo file '$SRC/$f' is missing or unreadable — refusing a partial install"
done
[ -r "$SRC/launchd/$PLIST" ] || die "repo file '$SRC/launchd/$PLIST' is missing"

# The topic set must RESOLVE from the committed file, here, before anything is written. A plist that
# schedules a wrapper whose set is undefined is a job that exits 1 every hour.
expected_topics="$(unset GAMMA_ZONE_CORPUS; . "$SRC/oe-topics.env"; printf '%s' "${GAMMA_ZONE_CORPUS:-}")"
[ -n "$expected_topics" ] || die "$SRC/oe-topics.env defines no GAMMA_ZONE_CORPUS — there is no set to schedule"
say "topic set from the repo: $expected_topics"

# The plist must point at the wrapper this installer actually places, and carry this label. A plist
# naming a different path is the failure that looks like a working install.
grep -q "$DEST/$WRAPPER" "$SRC/launchd/$PLIST" || die "$PLIST does not reference '$DEST/$WRAPPER'"
grep -q "<string>$LABEL</string>" "$SRC/launchd/$PLIST" || die "$PLIST does not carry the label $LABEL"

bash -n "$SRC/$WRAPPER" || die "$WRAPPER does not parse"
/usr/bin/plutil -lint "$SRC/launchd/$PLIST" >/dev/null || die "$PLIST is not a valid plist"
# plutil is LENIENT: it accepted a comment containing '--', which is illegal anywhere inside an XML
# comment, and so did launchd. A strict parser is what caught it, so one runs here too.
python3 -c "import plistlib,sys; plistlib.loads(open(sys.argv[1],'rb').read())" "$SRC/launchd/$PLIST" \
  || die "$PLIST is not well-formed XML (plutil is lenient about this; expat is not)"

if $DRY; then
  say "DRY RUN — would install:"
  for f in $FILES; do say "  $SRC/$f -> $DEST/$f"; done
  say "  $SRC/launchd/$PLIST -> $AGENTS/$PLIST, then bootout + bootstrap $LABEL"
  exit 0
fi

# ---- back up what is already on the host, BEFORE replacing any of it ------------------------
# set -e stops this script; it does not undo what it already wrote. Overwriting live files one at a
# time and then booting out the running agent can leave the host with a partial copy and nothing
# loaded (Codex round 1). Every failure after this point names the backup directory, so the previous
# working copy is always one `cp` away rather than gone.
STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
BACKUP="$DEST/.gamma-zone-install-backup-$STAMP"
restore_hint() {
  echo "[install-gamma-zone-archive] the previous copies are in $BACKUP" >&2
  echo "[install-gamma-zone-archive] restore with:  cp $BACKUP/* $DEST/ && cp $BACKUP/$PLIST $AGENTS/ && launchctl bootstrap gui/$(id -u) $AGENTS/$PLIST" >&2
}
mkdir -p "$DEST" "$AGENTS" "$BACKUP"
for f in $FILES; do
  [ -e "$DEST/$f" ] && cp "$DEST/$f" "$BACKUP/$f"
done
[ -e "$AGENTS/$PLIST" ] && cp "$AGENTS/$PLIST" "$BACKUP/$PLIST"
say "backed up the existing host copies to $BACKUP"
trap 'restore_hint' ERR

# ---- write ---------------------------------------------------------------------------------
for f in $FILES; do
  cp "$SRC/$f" "$DEST/$f"
  say "placed $DEST/$f"
done
chmod +x "$DEST/$WRAPPER" "$DEST/oe-archive-kafka.sh"
cp "$SRC/launchd/$PLIST" "$AGENTS/$PLIST"
say "placed $AGENTS/$PLIST"

# ---- the staged copy must derive the SAME set under launchd's own environment ---------------
# Not the repo copy: the one on the host, read the way the job will read it. A stale or truncated
# copy that still parses would otherwise be discovered an hour later, in a log nobody is reading.
staged="$(env -i HOME="$HOME" PATH=/opt/homebrew/bin:/usr/bin:/bin /opt/homebrew/bin/bash -c \
  "unset GAMMA_ZONE_CORPUS; . '$DEST/oe-topics.env'; printf '%s' \"\${GAMMA_ZONE_CORPUS:-}\"")"
[ "$staged" = "$expected_topics" ] \
  || die "the staged oe-topics.env derives '$staged', not '$expected_topics' — host copy is wrong"
say "verified: the staged copy derives exactly GAMMA_ZONE_CORPUS"

# ---- load, and prove exactly one agent registered -------------------------------------------
uid="$(id -u)"
launchctl bootout "gui/$uid/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$uid" "$AGENTS/$PLIST"
# Count whole-word label matches: a grep -c on a substring would count a sibling label that merely
# contains this one and report a healthy install.
n="$(launchctl list 2>/dev/null | awk -v l="$LABEL" '$3 == l' | wc -l | tr -d ' ')"
[ "$n" = "1" ] || die "expected exactly one '$LABEL' agent after load, found $n"
say "loaded $LABEL (hourly, at minute 0)"

trap - ERR
say "DONE (previous host copies kept in $BACKUP). It will not run until the next hour boundary;"
say "to prove it now:"
say "  bash $DEST/$WRAPPER   # then: tail $DEST/archive-gamma-zone.log"
