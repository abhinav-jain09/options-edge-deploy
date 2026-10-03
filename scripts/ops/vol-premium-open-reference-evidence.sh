#!/usr/bin/env bash
# vol-premium-open-reference-evidence.sh — reproduces the before/after measurement that PR #1125
# rests on, from the archive, on the host that holds it.
#
# WHY THIS EXISTS. The commit messages and code comments state figures measured on the prod
# archive: of 36,402 index rows on 2026-09-24 the pre-fix reader took 130 and counted 36,272
# "undated"; across the eight archived sessions the capture went from 0 accepted to 6, with
# 23,586-30,027 offset pairs instead of 42-103. A reviewer cannot reach that archive, and a number
# in a comment that nobody else can produce is an assertion, not evidence.
#
# IT RUNS BOTH READERS, END TO END. An earlier version counted parseable timestamps for the "before"
# column and took the verdict from the fixed capture alone, so it could not reproduce the acceptance
# or pair figures at all — the reviewer was right to call that out. This builds a copy of the capture
# with `_to_microseconds` reduced to the identity (which IS the pre-fix reader, and the rewrite is
# asserted, so a copy that failed to differ cannot be reported as "before") and runs each over the
# same files.
#
# IT CLAIMS NOTHING. --out is never passed to either reader, so no ledger entry and no permanent
# .published/ marker can come out of running this. It writes only inside its own mktemp directory.
#
#   ssh 192.168.100.252 '…/vol-premium-open-reference-evidence.sh 2026-09-17 2026-09-18 …'
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CAPTURE="${CAPTURE:-$HERE/vol-premium-open-reference-capture.py}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-/mnt/nas/optionsedge/kafka/prod}"

[ -r "$CAPTURE" ] || { echo "no capture at $CAPTURE" >&2; exit 2; }
[ -d "$ARCHIVE_ROOT" ] || { echo "no archive at $ARCHIVE_ROOT" >&2; exit 2; }
[ "$#" -gt 0 ] || { echo "usage: $0 <session> [session…]" >&2; exit 64; }

WORK="$(mktemp -d)"
BEFORE="$WORK/capture-before-the-fix.py"

# THE "BEFORE" READER IS BUILT, NOT DESCRIBED, and the build is checked. If the anchor ever stops
# matching, this exits rather than printing the FIXED reader's numbers in a column headed "before" —
# which would turn the evidence into a confirmation of itself.
CAPTURE="$CAPTURE" BEFORE="$BEFORE" python3 - <<'MAKE' || exit 3
import os, pathlib, sys
source = pathlib.Path(os.environ["CAPTURE"]).read_text()
anchor = '    return _FRACTIONAL_SECONDS.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), raw, count=1)'
if source.count(anchor) != 1:
    print("the capture's fraction normaliser is not where this script expects it; refusing to "
          "report the fixed reader as 'before'", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(os.environ["BEFORE"]).write_text(source.replace(anchor, "    return raw"))
MAKE

echo "python3: $(python3 -V 2>&1)   archive: $ARCHIVE_ROOT"
echo
printf '%-12s  %-9s %7s %7s %6s   %-9s %7s %7s %6s\n' \
       '' 'BEFORE' 'undated' 'pairs' 'cov%' 'AFTER' 'undated' 'pairs' 'cov%'
for session in "$@"; do
  row=""
  for reader in "$BEFORE" "$CAPTURE"; do
    record="$(python3 "$reader" --session "$session" --archive-root "$ARCHIVE_ROOT" 2>/dev/null)"
    row="$row$(printf '%s' "$record" | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)
except ValueError:
    print("   %-9s %7s %7s %6s" % ("?", "?", "?", "?"), end="")
    raise SystemExit
print("   %-9s %7d %7d %5.0f%%" % ("ACCEPTED" if r["accepted"] else "rejected",
                                   r["indexUndatedRecords"], r["offsetPairs"],
                                   100 * r["offsetCoveredFraction"]), end="")
' 2>/dev/null)"
  done
  printf '%-12s%s\n' "$session" "$row"
done
echo
echo "READ IT LIKE THIS. 'BEFORE' is the capture with its fraction normaliser removed, which is the"
echo "reader as it stood; 'AFTER' is the committed one. On the production host (Python 3.9) the two"
echo "columns differ and the BEFORE verdicts are all 'rejected'. On Python 3.11+ they are IDENTICAL,"
echo "because fromisoformat accepts nanoseconds there — which is exactly why the Jenkins agent's"
echo "suite could not see this bug, and is itself worth seeing."
echo
echo "Neither reader was given --out, so nothing was claimed. Scratch: $WORK"
