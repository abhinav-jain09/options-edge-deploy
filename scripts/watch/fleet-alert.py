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

PAGING DISCIPLINE (one page per INCIDENT, not per run). A check runs every 3-15 minutes; a finding that stays
true must not become a message every time. An incident is one offender key (class + subject, with the pod's
ReplicaSet/pod hash and every number removed so a restart count that ticks does not make a new incident),
kept per (environment, check) in --state-dir/alerts-<env>-<check>.json:
  * first sight            -> ONE page ("new").
  * still open             -> a reminder no sooner than REMIND_MIN (30) minutes after the previous message.
  * open >= ESCALATE_MIN   -> ONE escalation page (120 min after it opened); after it, nothing but a DAILY
                              summary (every SUMMARY_MIN = 24 h) for as long as it stays open.
  * absent for GRACE_MIN   -> ONE recovery message ("resolved, open for ..."). GRACE_MIN (30) is the flap
                              guard: a crash-looping pod is "clean" for a run or two between restarts and that
                              must not close and reopen the incident.
  * exit 2 ("could not check") is an incident of its own (key `check-failed`) with the same rules, and a run
    that could not check leaves every OTHER incident exactly as it was: not seeing is not recovering.
  * observation-only findings neither page nor touch the state.
  * an unreadable or unwritable state never silences anything: every finding is paged as new and the message
    says that de-duplication is off.
"""
import argparse
import datetime as dt
import json
import os
import re
import sys

# rank, emoji. Higher ranks are more urgent; see R1e.
RANK = {
    "CRASHLOOP": (5, "🔴"),
    "BRIDGE_NOT_READY": (5, "🔴"),
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
REMIND_MIN = 30               # a reminder is never sooner than this after the previous message
ESCALATE_MIN = 120            # an incident open this long is escalated ONCE
SUMMARY_MIN = 24 * 60         # after the escalation: one summary a day
GRACE_MIN = 30                # absent this long = recovered (flap guard)
LINE = re.compile(r"^(?P<cls>[A-Z_]+) (?P<rest>.+)$")

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True)
ap.add_argument("--check", required=True)
ap.add_argument("--rc", type=int, required=True)
ap.add_argument("--input", required=True, help="the check's combined output")
ap.add_argument("--mode", choices=("paging", "observation-only"), default="paging")
ap.add_argument("--build-url", default=None)
ap.add_argument("--state-dir", required=True, help="where the open incidents of this (env, check) are kept")
ap.add_argument("--now", default=None, help="ISO instant, for the tests")
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


def parse_ts(x):
    return dt.datetime.fromisoformat(x.replace("Z", "+00:00"))


NOW = parse_ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)
STATE_FILE = os.path.join(a.state_dir, "alerts-%s-%s.json" % (re.sub(r"[^A-Za-z0-9_.-]", "_", a.env),
                                                              re.sub(r"[^A-Za-z0-9_.-]", "_", a.check)))
POD_HASH = re.compile(r"-[a-z0-9]{8,10}-[a-z0-9]{5}$")


def incident_key(cls, rest):
    """cls + subject + the finding text with its numbers removed: the pod hash of a replacement pod and a restart
    count that ticks up must not turn one incident into many."""
    subject = POD_HASH.sub("", rest.split(None, 1)[0].rstrip(":"))
    tail = re.sub(r"\d+", "#", rest.split(None, 1)[1] if " " in rest else "")
    return ("%s %s %s" % (cls, subject, tail)).strip()[:200]


def dur(minutes):
    m = int(minutes)
    return "%dh %02dm" % (m // 60, m % 60) if m >= 60 else "%dm" % m


def mins(since):
    return (NOW - parse_ts(since)).total_seconds() / 60.0


def load_state():
    """Returns (incidents, problem). A problem NEVER silences: the caller pages everything as new."""
    try:
        if not os.path.exists(STATE_FILE):
            return {}, None
        data = json.load(open(STATE_FILE))
        inc = data["incidents"]
        if not isinstance(inc, dict):
            raise ValueError("`incidents` is not an object")
        for k, v in inc.items():
            for f in ("cls", "rest", "rank", "opened", "lastSeen", "lastNotified", "escalated", "lastSummary"):
                if f not in v:
                    raise ValueError("incident %r has no %s" % (k, f))
            parse_ts(v["opened"]), parse_ts(v["lastSeen"]), parse_ts(v["lastNotified"]), parse_ts(v["lastSummary"])
        return inc, None
    except Exception as e:  # noqa: BLE001
        return {}, "cannot read the alert state %s: %s" % (STATE_FILE, e)


def save_state(inc):
    try:
        os.makedirs(a.state_dir, exist_ok=True)
        tmp = STATE_FILE + ".tmp.%d" % os.getpid()
        with open(tmp, "w") as fh:
            json.dump({"incidents": inc}, fh, indent=1, sort_keys=True)
        os.replace(tmp, STATE_FILE)        # atomic: a half-written state would read as "unreadable" next run
        return None
    except Exception as e:  # noqa: BLE001
        return "cannot write the alert state %s: %s" % (STATE_FILE, e)


def new_incident(cls, rank, rest):
    t = NOW.isoformat()
    return {"cls": cls, "rest": rest, "rank": rank, "opened": t, "lastSeen": t, "lastNotified": t,
            "escalated": False, "lastSummary": t}


try:
    raw = open(a.input, encoding="utf-8", errors="replace").read()
except OSError as e:
    # The composer failing must not be silent either: say so, and say it is the composer.
    print("⚠️ **%s** / %s — could not read the check's own output: %s" % (a.env, a.check, e))
    sys.exit(0)

# --- what this run observed -------------------------------------------------------------------------------
observed = {}            # key -> (cls, rank, rest)
could_not_check = a.rc == 2
if could_not_check:
    detail = ""
    for line in raw.splitlines():
        if line.startswith("ERROR: "):
            detail = line[len("ERROR: "):]
            break
    observed["check-failed"] = ("CHECK_FAILED", 5, detail)
elif a.rc != 0:
    for line in raw.splitlines():
        m = LINE.match(line.strip())
        if not m or m.group("cls") not in RANK:
            continue
        cls, rest = m.group("cls"), m.group("rest")
        observed.setdefault(incident_key(cls, rest), (cls, RANK[cls][0], rest))
    if not observed:
        # A non-zero exit with no recognisable finding is itself worth saying: the check disagrees with the
        # composer, and reporting nothing would turn a red build into silence.
        observed["unrecognised"] = ("UNRECOGNISED", 5, "exit %d but no recognisable finding line; read the build" % a.rc)

if a.mode == "observation-only" and not could_not_check:
    sys.exit(0)                      # findings are silent here, and do not touch the state; exit 2 is not

# --- state transitions --------------------------------------------------------------------------------------
state, problem = load_state()
fresh, remind, escalate, summary, resolved = [], [], [], [], []
for key, (cls, rank, rest) in observed.items():
    inc = state.get(key)
    if inc is None:
        state[key] = new_incident(cls, rank, rest)
        fresh.append(key)
        continue
    age = mins(inc["opened"])
    inc.update(lastSeen=NOW.isoformat(), rest=rest, rank=rank, cls=cls)
    if not inc["escalated"] and age >= ESCALATE_MIN:
        inc.update(escalated=True, lastNotified=NOW.isoformat(), lastSummary=NOW.isoformat())
        escalate.append(key)
    elif not inc["escalated"]:
        if mins(inc["lastNotified"]) >= REMIND_MIN:
            inc["lastNotified"] = NOW.isoformat()
            remind.append(key)
    elif mins(inc["lastSummary"]) >= SUMMARY_MIN:
        inc["lastSummary"] = NOW.isoformat()
        summary.append(key)
resolved_info = {}
if not could_not_check:              # a run that could not look must not declare anything recovered
    for key in [k for k in state if k not in observed]:
        if mins(state[key]["lastSeen"]) >= GRACE_MIN:
            resolved_info[key] = (state[key], mins(state[key]["opened"]))
            resolved.append(key)
            del state[key]
if problem:
    fresh = list(observed)            # dedup is off: page everything, and say so
werr = save_state(state)
if werr:
    problem = (problem + "; " if problem else "") + werr
    fresh = list(observed)
    remind = escalate = summary = []

# --- composition ----------------------------------------------------------------------------------------
def bullet(key, extra=""):
    inc = state.get(key) or resolved_info[key][0]
    return clip_chars("%s `%s` %s%s" % (RANK_EMOJI(inc["cls"]), inc["cls"], inc["rest"], extra), LINE_CHARS)


def RANK_EMOJI(cls):
    return RANK[cls][1] if cls in RANK else "⚠️"


def block(title_emoji, title, keys, extra=lambda k: ""):
    keys = sorted(keys, key=lambda k: (-(state.get(k) or resolved_info[k][0])["rank"],
                                       (state.get(k) or resolved_info[k][0])["rest"]))
    head = clip_chars("%s **%s** / `%s` — %s" % (title_emoji, a.env, a.check, title % len(keys)), HEADER_CHARS)
    out = [head] + [bullet(k, extra(k)) for k in keys[:MAX_OFFENDERS]]
    if len(keys) > MAX_OFFENDERS:
        out.append("… and %d more — see the build" % (len(keys) - MAX_OFFENDERS))
    return "\n".join(out)


def open_for(k):
    return " — open %s" % dur(mins(state[k]["opened"]))


blocks = []
if fresh:
    if could_not_check and fresh == ["check-failed"]:
        d = observed["check-failed"][2]
        blocks.append("⚠️ **%s** / `%s` — **could not check**%s" % (a.env, a.check, (": " + d) if d else ""))
    elif fresh == ["unrecognised"]:
        blocks.append("⚠️ **%s** / `%s` — %s" % (a.env, a.check, observed["unrecognised"][2]))
    else:
        offenders = sorted(((observed[k][1], observed[k][0], observed[k][2]) for k in fresh),
                           key=lambda o: (-o[0], o[2]))
        top = RANK[offenders[0][1]][1] if offenders[0][1] in RANK else "⚠️"
        parts = [clip_chars("%s **%s** / `%s` — %d finding(s)" % (top, a.env, a.check, len(offenders)), HEADER_CHARS)]
        parts += [clip_chars("%s `%s` %s" % (RANK_EMOJI(c), c, r), LINE_CHARS) for _, c, r in offenders[:MAX_OFFENDERS]]
        if len(offenders) > MAX_OFFENDERS:
            parts.append("… and %d more — see the build" % (len(offenders) - MAX_OFFENDERS))
        blocks.append("\n".join(parts))
if escalate:
    blocks.append(block("🚨", "ESCALATED: %d incident(s) open for over " + dur(ESCALATE_MIN)
                        + "; no more reminders, one summary a day", escalate, open_for))
if remind:
    blocks.append(block("🔁", "still open: %d incident(s)", remind, open_for))
if summary:
    blocks.append(block("📋", "daily summary: %d incident(s) still open", summary, open_for))
if resolved:
    blocks.append(block("✅", "resolved: %d incident(s)", resolved,
                        lambda k: " — was open %s" % dur(resolved_info[k][1])))
if problem:
    blocks.append("⚠️ **%s** / `%s` — alert de-duplication is OFF this run: %s" % (a.env, a.check, clip_chars(problem, 200)))

if not blocks:
    sys.exit(0)                      # an incident already announced: silence is the point
msg = "\n\n".join(blocks)
if a.build_url:
    msg += "\n<%s>" % a.build_url
print(clip_units(msg, CONTENT_UNITS))
