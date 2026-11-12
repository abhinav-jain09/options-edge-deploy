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
LIVE_DIR="$(dirname "${DEV_CLEANUP_LIVE:-$HOME/oe-ops/dev-cleanup.sh}")"
# The clean is not one file. dev-cleanup.sh SOURCES the registry prune, and that prune deletes
# manifests and garbage-collects the blob store — so an unreviewed copy of it is as dangerous as an
# unreviewed clean. It lived only on the Mac until 2026-11-12, untracked and untested, and its
# keep set missed init containers; see dev-registry-prune.sh's header. The prune also runs the pull
# check (dev-registry-verify-pinned.sh/.py). Install and check ALL of them together, or the --check
# that passes proves only part of what runs. (The fix to the prune was merged on 2026-11-12 and the
# machines kept running the OLD copy for hours, because nothing installed it.)
FILES="scripts/ops/dev-cleanup.sh scripts/ops/dev-registry-prune.sh scripts/ops/dev-registry-keep-set.py scripts/ops/dev-registry-verify-pinned.sh scripts/ops/dev-registry-verify-pinned.py"
sha() { shasum -a 256 "$1" | awk '{print $1}'; }
live_of() { printf '%s/%s\n' "$LIVE_DIR" "$(basename "$1")"; }
if [ "${1:-}" = "--check" ]; then
  drift=0
  for SRC in $FILES; do
    DST="$(live_of "$SRC")"
    if [ ! -f "$DST" ]; then echo "DRIFT: live copy $DST is missing"; drift=1; continue; fi
    if [ "$(sha "$SRC")" = "$(sha "$DST")" ]; then
      echo "OK: $(basename "$SRC") == repo ($(sha "$SRC" | cut -c1-12))"; continue
    fi
    echo "DRIFT: $DST differs from $SRC — what runs is not what was reviewed:"
    # diff exits 1 on a difference: under set -e and pipefail that alone would end the script, so the exit status of a drift must not depend on it
    { diff "$SRC" "$DST" | head -20 | sed 's/^/  /'; } || true
    drift=1
  done
  exit "$drift"
fi
mkdir -p "$LIVE_DIR"
for SRC in $FILES; do
  case "$SRC" in *.sh) bash -n "$SRC" ;; *.py) python3 -c "import ast,sys;ast.parse(open(sys.argv[1]).read())" "$SRC" ;; esac
  DST="$(live_of "$SRC")"
  if [ -f "$DST" ] && [ "$(sha "$SRC")" != "$(sha "$DST")" ]; then
    cp -p "$DST" "$DST.bak-$(date +%Y%m%d-%H%M%S)-pre-install"
  fi
  install -m 0755 "$SRC" "$DST"
  echo "installed $SRC -> $DST ($(sha "$DST" | cut -c1-12))"
done
