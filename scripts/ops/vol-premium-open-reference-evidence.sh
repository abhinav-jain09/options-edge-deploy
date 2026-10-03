#!/usr/bin/env bash
# vol-premium-open-reference-evidence.sh — reproduces the before/after measurement that PR #1125
# rests on, from the archive, on the host that holds it.
#
# WHY THIS EXISTS. The commit message and the code comments state figures measured on the prod
# archive: of 36,402 index rows on 2026-09-24 the pre-fix reader took 130 and called 36,272
# "undated"; across the eight archived sessions the capture went from 0 accepted to 6, with
# 23,586-30,027 offset pairs instead of 42-103. A reviewer cannot reach that archive, and a number
# in a comment that nobody else can produce is an assertion, not evidence. This runs both readers
# over the same files and prints the table, so the claim is checkable by anyone with the host.
#
# IT IS READ-ONLY and writes no ledger: --out is deliberately never passed, so running this cannot
# claim a session. Expect a few seconds per session.
#
#   ssh 192.168.100.252 '…/vol-premium-open-reference-evidence.sh 2026-09-17 2026-09-18 …'
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CAPTURE="${CAPTURE:-$HERE/vol-premium-open-reference-capture.py}"
ARCHIVE_ROOT="${ARCHIVE_ROOT:-/mnt/nas/optionsedge/kafka/prod}"

[ -r "$CAPTURE" ] || { echo "no capture at $CAPTURE" >&2; exit 2; }
[ -d "$ARCHIVE_ROOT" ] || { echo "no archive at $ARCHIVE_ROOT" >&2; exit 2; }
[ "$#" -gt 0 ] || { echo "usage: $0 <session> [session…]" >&2; exit 64; }

# The PRE-FIX reader, as it stood: strict fromisoformat, so the host's interpreter decides. Run on
# the same host as the capture or it proves nothing — this is a statement about Python 3.9's
# fromisoformat, and on 3.11+ the two readers agree and the table is all zeroes, which is itself
# the finding worth seeing.
echo "python3: $(python3 -V 2>&1)   archive: $ARCHIVE_ROOT"
printf '%-12s %-9s %8s %8s %8s   %s\n' session verdict rows strict tolerant note
for session in "$@"; do
  ARCHIVE_ROOT="$ARCHIVE_ROOT" OE_DAY="$session" python3 - <<'COUNT'
import datetime as dt, glob, gzip, json, os, re
root, day = os.environ["ARCHIVE_ROOT"], os.environ["OE_DAY"]
NANO = re.compile(r"(?<=:\d\d)\.(\d+)")
rows = strict = tolerant = 0
for path in sorted(glob.glob(os.path.join(root, "underlying.spx.index.price", f"dt={day}", "*.jsonl.gz"))):
    try:
        handle = gzip.open(path, "rt", errors="replace")
    except OSError:
        continue
    with handle:
        for line in handle:
            brace = line.find("{")
            if brace < 0:
                continue
            try:
                raw = json.loads(line[brace:]).get("eventTime")
            except ValueError:
                continue
            rows += 1
            for parser, kind in ((lambda v: v, "strict"),
                                 (lambda v: NANO.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), v, count=1), "tolerant")):
                try:
                    stamp = dt.datetime.fromisoformat(parser(raw).replace("Z", "+00:00"))
                except (ValueError, AttributeError, TypeError):
                    continue
                if stamp.tzinfo is not None:
                    if kind == "strict":
                        strict += 1
                    else:
                        tolerant += 1
with open(os.environ.get("EVIDENCE_COUNTS", "/dev/stdout"), "w") as out:
    out.write(f"{rows} {strict} {tolerant}\n")
COUNT
done > /tmp/.vp-evidence-counts.$$ 2>/dev/null
i=0
for session in "$@"; do
  i=$((i + 1))
  counts=$(sed -n "${i}p" /tmp/.vp-evidence-counts.$$)
  record=$(python3 "$CAPTURE" --session "$session" --archive-root "$ARCHIVE_ROOT" 2>/dev/null)
  verdict=$(printf '%s' "$record" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("ACCEPTED" if r["accepted"] else "rejected")' 2>/dev/null)
  note=$(printf '%s' "$record" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("pairs=%s covered=%s/%s %s" % (r["offsetPairs"], r["offsetCoveredMinutes"], r["scoreableMinutes"], r["rejectedBecause"] or ""))' 2>/dev/null)
  # shellcheck disable=SC2086
  printf '%-12s %-9s %8s %8s %8s   %s\n' "$session" "${verdict:-?}" ${counts:-? ? ?} "$note"
done
# The counts file is this run's scratch and nothing reads it afterwards; it is left in place rather
# than removed, because nothing on this host deletes files it did not create.
echo "(raw counts: /tmp/.vp-evidence-counts.$$)"
echo
echo "READ IT LIKE THIS: 'strict' is what the reader took before the fix and 'tolerant' what it"
echo "takes now. On Python 3.11+ the two columns are equal and the verdicts are the post-fix ones —"
echo "which is exactly why the Jenkins agent's suite could not see this bug."
