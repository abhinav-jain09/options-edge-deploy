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
FILES="calibration-progress-watch.sh oe-alert.sh calibration-targets.env"

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
# on a day with no progress record — what is NOT fine is the script failing to start at all.
echo "verify: running it once"
set +e
out="$(ENV=prod bash "$DEST/calibration-progress-watch.sh" 2>&1)"; rc=$?
set -e
printf '%s\n' "$out" | sed 's/^/    /'
case "$out" in
  *"cannot run"*) echo "REFUSING TO LOAD: the watchdog cannot run on this host (see above). Fix that first." >&2; exit 1 ;;
esac
echo "verify: it started and reached a verdict (exit $rc — nonzero is normal when the day did not land)"

launchctl unload "$AGENTS/$PLIST" 2>/dev/null || true
launchctl load "$AGENTS/$PLIST"
echo "loaded: $(launchctl list | grep -c com.optionsedge.calibration-progress-watch) agent(s) named com.optionsedge.calibration-progress-watch"
