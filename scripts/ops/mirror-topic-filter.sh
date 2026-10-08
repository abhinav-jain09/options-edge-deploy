#!/usr/bin/env bash
# Does this es4->target MirrorMaker-1 launchd agent copy any of the named topics?
#
#   mirror-topic-filter.sh <plist> [<topic> ...]
#     exit 0  CLEAR  -- no named topic can match this agent's --whitelist; it is safe to start
#     exit 1  HOLD   -- a named topic matches it, or the agent could not be read well enough to say
#     exit 2         -- usage
#   and one line on stdout: "CLEAR <label>" / "HOLD <label>: <topic> ~ /<regex>/" / "HOLD <label>: ..."
#
# WHY. scripts/ops/prod-clean-slate.sh pauses every es4->prod mirror before the wipe and starts them
# again after the recreate. Until 2026-10-08 that was all-or-nothing on apply-topics.sh's exit code, so
# on 2026-10-07 ONE drifted topic (options.spx.strike-invasion.current, which no mirror produces into)
# left all twelve mirrors paused. A mirror only has a stake in the topics its whitelist matches; this
# answers that question per agent, so the wrapper can start the rest.
#
# TWO REASONS TO HOLD, and they are the same answer:
#   * the topic was SKIPPED by apply-topics.sh, so its shape on the broker is not the declared one, and
#     producing into it is how a mirrored key lands on the wrong partition;
#   * the topic is MISSING from the broker, where auto.create.topics.enable means the mirror's first
#     produce CREATES it at the broker default (1 partition on prod) -- the defect PR #1048 pauses
#     mirrors for in the first place.
# The caller passes both sets; this file does not distinguish them.
#
# WHAT THE VERDICT IS DERIVED FROM: ProgramArguments[0] -- the program launchd actually starts -- which
# must be an absolute path, readable, and carry a --whitelist of its own. Any other run-mirror*.sh in
# the same directory is then unioned in, so a stale or extra script can only ADD holds, never explain
# one away.
#
# FAIL CLOSED. Anything that stops this from reading the agent's real whitelist -- no plist, a plist
# that does not parse, a relative or missing program path, an unreadable script, no --whitelist in the
# program, a regex that will not compile -- is a HOLD. A mirror left paused is a stale panel; a mirror
# started against a wrong-shaped topic re-routes keys.
#
# OVER-INCLUSIVE ON PURPOSE. MirrorMaker-1 subscribes by pattern and Kafka matches a topic against it
# in FULL, so `es\.futures\.cvd` does not actually match es.futures.cvd.bars. This holds on a partial
# match as well, because the cost of the two mistakes is not symmetric: an unnecessary hold is visible
# in the log and fixed by a rerun; a wrong start is silent and corrupts key routing. The 2026-10-07
# case is unaffected -- no mirror's whitelist mentions options.spx.strike-invasion.current at all.
set -uo pipefail
[ "$#" -ge 1 ] || { echo "usage: $0 <plist> [<topic> ...]" >&2; exit 2; }
PLIST="$1"; shift
# No topics to check at all: nothing can match, so this is a CLEAR by definition. (The caller skips the
# call entirely in that case; answering it here keeps the contract total.)
python3 - "$PLIST" "$@" <<'PY'
import glob, os, plistlib, re, sys

plist_path, topics = sys.argv[1], sys.argv[2:]
label = os.path.basename(plist_path)[:-6] if plist_path.endswith(".plist") else os.path.basename(plist_path)

def hold(msg):
    print("HOLD %s: %s" % (label, msg))
    sys.exit(1)

try:
    with open(plist_path, "rb") as fh:
        job = plistlib.load(fh)
except Exception as exc:
    hold("cannot read the plist (%s)" % exc.__class__.__name__)
if not isinstance(job, dict):
    hold("the plist is not a job dictionary")
label = job.get("Label") or label

# THE PROGRAM THE JOB ACTUALLY RUNS comes first, and it must be readable and carry a whitelist of its
# own; a verdict derived from a SIBLING script in the same directory could describe a stale or unused
# file rather than the mirror launchd will start (deploy Codex round 1).
args = [str(a) for a in (job.get("ProgramArguments") or [])]
program = args[0] if args else ""
if not program or not os.path.isabs(program):
    hold("ProgramArguments[0] is not an absolute path, so the program it runs is unknown")

# Everything else in the unit directory is then UNIONED in, because a whitelist found anywhere beside
# the program is a topic this unit may plausibly copy, and holding on it is the cheap mistake. Both the
# program and the extra files must parse; an unreadable one is a HOLD, not a skip.
unit_dir = os.path.dirname(program)
scripts = [program]
for extra in sorted(glob.glob(os.path.join(unit_dir, "run-mirror*.sh"))):
    if extra not in scripts:
        scripts.append(extra)

# --whitelist 'es\.futures\.cvd\.bars'  /  --whitelist es\.futures\.auction
WL = re.compile(r"--whitelist\s+(?:'([^']*)'|\"([^\"]*)\"|(\S+))")

def patterns_in(path):
    # Any failure to read the file as text is a HOLD with a message, not a traceback: a plist whose
    # ProgramArguments[0] is a BINARY (a /bin/bash wrapper layout) raised UnicodeDecodeError here, which
    # happened to exit non-zero and so happened to be safe -- the wrong reason to be right.
    try:
        with open(path, "r", errors="replace") as fh:
            text = fh.read()
    except Exception as exc:
        hold("cannot read %s as text (%s)" % (os.path.basename(path), exc.__class__.__name__))
    found = []
    for m in WL.finditer(text):
        pat = m.group(1) or m.group(2) or m.group(3)
        if pat and pat not in found:
            found.append(pat)
    return found

# The program's OWN whitelist is required: without it this does not know what launchd would start, and
# a sibling's whitelist is not an answer about the program.
patterns = patterns_in(program)
if not patterns:
    hold("no --whitelist in the program the job runs (%s)" % os.path.basename(program))
for extra in scripts[1:]:
    for pat in patterns_in(extra):
        if pat not in patterns:
            patterns.append(pat)

for pat in patterns:
    try:
        rx = re.compile(pat)
    except re.error as exc:
        hold("--whitelist %r does not compile (%s)" % (pat, exc))
    for t in topics:
        if rx.fullmatch(t) or rx.search(t):
            hold("%s ~ /%s/" % (t, pat))

print("CLEAR %s (%d whitelist pattern(s), %d topic(s) checked)" % (label, len(patterns), len(topics)))
PY
