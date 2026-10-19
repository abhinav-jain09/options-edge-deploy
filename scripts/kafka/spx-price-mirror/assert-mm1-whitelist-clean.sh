#!/usr/bin/env bash
# assert-mm1-whitelist-clean.sh <run-mirror.sh of the dev->.74 MirrorMaker 1 unit>
# FAILS while the file still lists the topic underlying.spx.price (a BYTE copier, group oe-mac74-input-from-dev) in its --whitelist. Matches the
# escaped regex alternative `underlying\.spx\.price` and the bracket form `underlying[.]spx[.]price`, but not underlying.spx.index.price or longer names.
set -euo pipefail
F="${1:?run-mirror.sh path}"
[ -r "$F" ] || { echo "FAIL: cannot read $F: cannot prove the MM1 whitelist no longer copies underlying.spx.price"; exit 1; }
if grep -qE 'underlying(\\[.]|\[[.]\]|[.])spx(\\[.]|\[[.]\]|[.])price($|[^-.A-Za-z\\[])' "$F"; then
  echo "FAIL: the MM1 whitelist in $F still carries underlying.spx.price (a BYTE copier, group oe-mac74-input-from-dev)."
  echo "      Remove that alternative from --whitelist, restart com.optionsedge.dl-mirror-dev-to-mac74, then re-run this job."
  exit 1
fi
echo "  MM1 whitelist no longer carries underlying.spx.price"
