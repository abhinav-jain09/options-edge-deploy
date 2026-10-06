#!/usr/bin/env bash
# Install this repo's dev-cleanup.sh as the Mac's live copy (~/oe-ops/dev-cleanup.sh) and PROVE it.
#
# The live copy is what launchd/an operator actually runs; it had drifted from this file (a
# rotated-log fix made on the Mac on 2026-10-05 and never upstreamed, then a safety guard applied to
# both by hand on 2026-10-06). A fix reviewed here that never reaches ~/oe-ops is not a fix. So:
# one command installs, and `--check` fails loudly when the two differ, so a drift is a visible
# failure instead of a surprise the next time the clean runs.
#
#   scripts/ops/install-dev-cleanup.sh          install (backs up the previous live copy)
#   scripts/ops/install-dev-cleanup.sh --check  exit 1 if the live copy differs from this repo's
set -euo pipefail
cd "$(dirname "$0")/../.."
SRC=scripts/ops/dev-cleanup.sh
DST="${DEV_CLEANUP_LIVE:-$HOME/oe-ops/dev-cleanup.sh}"
sha() { shasum -a 256 "$1" | awk '{print $1}'; }
if [ "${1:-}" = "--check" ]; then
  if [ ! -f "$DST" ]; then echo "DRIFT: live copy $DST is missing"; exit 1; fi
  if [ "$(sha "$SRC")" = "$(sha "$DST")" ]; then
    echo "OK: live dev-cleanup.sh == repo ($(sha "$SRC" | cut -c1-12))"; exit 0
  fi
  echo "DRIFT: $DST differs from $SRC — the clean that runs is not the one reviewed:"
  diff "$SRC" "$DST" | head -20 | sed 's/^/  /'
  exit 1
fi
bash -n "$SRC"
mkdir -p "$(dirname "$DST")"
if [ -f "$DST" ] && [ "$(sha "$SRC")" != "$(sha "$DST")" ]; then
  cp -p "$DST" "$DST.bak-$(date +%Y%m%d-%H%M%S)-pre-install"
fi
install -m 0755 "$SRC" "$DST"
echo "installed $SRC -> $DST ($(sha "$DST" | cut -c1-12))"
