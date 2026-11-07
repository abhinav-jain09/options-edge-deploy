#!/usr/bin/env python3
"""Compose ONE Discord message from one check's output (FLEET-ALERT-DISCORD-REQUIREMENT R1e).

  fleet-alert.py --env dev --check crash-loop --rc 1 --input out.txt [--mode observation-only]

Prints the message on stdout, or NOTHING when there is nothing to send. The caller passes it to
oe-alert.sh's alert(), which is the one implementation of delivery (R1a) - composition is here so it can be
tested without a webhook.

THE CONTRACT, each clause a thing that would otherwise go wrong:
  * ONE message per (environment, check, run). Three checks x three environments = at most 9 per run.
  * Offenders are ranked by RANK below, descending, ties broken on the offender's name ascending, so the
    same fleet state always produces the same message.
  * At most 5 offenders, then "… and N more — see the build". Five is the cap, not "a few".
  * The header is capped at 300 characters and each offender line at 200, and THEN the whole composed
    string is capped at 1,900 UTF-16 CODE UNITS - the unit Discord counts. Every emoji here is non-BMP and
    costs 2, so a message under 2,000 by len(str) can still be rejected with HTTP 400. That rejection
    dropped 250-950 alerts a day for a week (BZ 366).
  * exit 2 ("could not check") is delivered in EVERY mode, including observation-only: a check that cannot
    run is a broken check whatever its thresholds, and silence is indistinguishable from health.
  * observation-only suppresses FINDINGS only (the partition rules, until their thresholds are calibrated
    against market-hours traffic).
"""
import argparse
import re
import sys

# rank, emoji. Higher ranks are more urgent; see R1e.
RANK = {
    "CRASHLOOP": (5, "🔴"),
    "IMAGE_PULL": (5, "🔴"),
    "RESTARTS": (4, "🟠"),
    "NO_CONSUMER": (4, "🟠"),
    "LAG_INSUFFICIENT_DRAIN": (3, "🟡"),
    "LAG_FIRST_SIGHT": (3, "🟡"),
    "SKEW": (2, "🔵"),
    "SINGLE_PARTITION_HOT": (2, "🔵"),
    "UNATTRIBUTED": (1, "⚪"),
}
MAX_OFFENDERS = 5
HEADER_CHARS = 300
LINE_CHARS = 200
CONTENT_UNITS = 1900          # UTF-16 code units, the unit Discord counts
LINE = re.compile(r"^(?P<cls>[A-Z_]+) (?P<rest>.+)$")

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--check", required=True)
ap.add_argument("--rc", type=int, required=True)
ap.add_argument("--input", required=True, help="the check's combined output")
ap.add_argument("--mode", choices=("paging", "observation-only"), default="paging")
ap.add_argument("--build-url", default=None)
a = ap.parse_args()


def units(s):
    """UTF-16 code units, not characters: an emoji is 2."""
    return len(s.encode("utf-16-le")) // 2


def clip_chars(s, n):
    return s if len(s) <= n else s[: n - 1] + "…"


def clip_units(s, n):
    """Truncate to n UTF-16 code units without splitting a surrogate pair."""
    if units(s) <= n:
        return s
    out = []
    used = 0
    for ch in s:
        c = units(ch)
        if used + c > n - 1:
            break
        out.append(ch)
        used += c
    return "".join(out) + "…"


try:
    raw = open(a.input, encoding="utf-8", errors="replace").read()
except OSError as e:
    # The composer failing must not be silent either: say so, and say it is the composer.
    print("⚠️ **%s** / %s — could not read the check's own output: %s" % (a.env, a.check, e))
    sys.exit(0)

if a.rc == 2:
    detail = ""
    for line in raw.splitlines():
        if line.startswith("ERROR: "):
            detail = line[len("ERROR: "):]
            break
    msg = "⚠️ **%s** / `%s` — **could not check**%s" % (
        a.env, a.check, (": " + detail) if detail else "")
    if a.build_url:
        msg += "\n<%s>" % a.build_url
    print(clip_units(clip_chars(msg, HEADER_CHARS + LINE_CHARS), CONTENT_UNITS))
    sys.exit(0)

if a.rc == 0:
    sys.exit(0)                      # nothing to say, and silence is correct here

offenders = []
for line in raw.splitlines():
    m = LINE.match(line.strip())
    if not m:
        continue
    cls = m.group("cls")
    if cls not in RANK:
        continue
    offenders.append((RANK[cls][0], cls, m.group("rest")))

if not offenders:
    # A non-zero exit with no recognisable finding is itself worth saying: the check disagrees with the
    # composer, and reporting nothing would turn a red build into silence.
    msg = ("⚠️ **%s** / `%s` — exit %d but no recognisable finding line; read the build"
           % (a.env, a.check, a.rc))
    if a.build_url:
        msg += "\n<%s>" % a.build_url
    print(clip_units(msg, CONTENT_UNITS))
    sys.exit(0)

if a.mode == "observation-only":
    sys.exit(0)                      # findings are silent here; exit 2 above is not

# rank descending, then name ascending - deterministic for a given fleet state
offenders.sort(key=lambda o: (-o[0], o[2]))
top_emoji = RANK[offenders[0][1]][1]
header = "%s **%s** / `%s` — %d finding(s)" % (top_emoji, a.env, a.check, len(offenders))
lines = [clip_chars("%s `%s` %s" % (RANK[cls][1], cls, rest), LINE_CHARS)
         for _, cls, rest in offenders[:MAX_OFFENDERS]]
parts = [clip_chars(header, HEADER_CHARS)] + lines
if len(offenders) > MAX_OFFENDERS:
    parts.append("… and %d more — see the build" % (len(offenders) - MAX_OFFENDERS))
if a.build_url:
    parts.append("<%s>" % a.build_url)
print(clip_units("\n".join(parts), CONTENT_UNITS))
