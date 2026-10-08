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
# must be an absolute path, readable, and carry a LITERAL --whitelist of its own on a line that runs.
# Any other run-mirror*.sh in the same directory is then unioned in, so a stale or extra script can
# only ADD holds, never explain one away. Comment lines are ignored (a comment is not executed) and a
# --whitelist assembled from a variable or a command substitution is a HOLD, because its runtime value
# is not in the file.
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
# THE ACCEPTED LAUNCHER FORMAT.
#
# Earlier rounds of this file inferred the whitelist with a regex scan over the script's text, then
# patched the scan: comment lines dropped, dynamic values refused. A scan cannot be made to hold
# (deploy Codex round 5): an inline comment, a here-doc body, a continued command or a wrapper that
# execs the real launcher from elsewhere are all text this file would read as a command, or not read at
# all -- so a CLEAR could rest on a decoy and a HOLD on a here-doc.
#
# These scripts are GENERATED, by the mirror install paths in this repo and by ansible/es-mirrors.yml,
# in one shape. So instead of parsing shell, this ACCEPTS that shape and refuses everything else:
#
#   * logical lines are formed by joining trailing-backslash continuations, and lines whose first
#     non-space character is # are dropped;
#   * EXACTLY ONE logical line may mention kafka-mirror-maker, and it must be, in full,
#       exec [path/]kafka-mirror-maker (--flag [value])...
#     with every value a single-quoted literal, a double-quoted literal with no $ or backtick, or a
#     bare token with none of $ ` " ' # ; | & < > ( );
#   * that line must carry EXACTLY ONE --whitelist, QUOTED, and its value is the pattern (an unquoted
#     value is rewritten by the shell before the program sees it, so the file does not hold it);
#   * every other line must be a shebang, a `set -flags`, one `export KAFKA_LOG4J_OPTS="..."`, a comment
#     or blank -- which is all any generator in this repo emits -- so nothing can change what runs;
#   * the FILE (comments and all) may contain the text --whitelist exactly once, so a commented or
#     here-doc'd decoy cannot sit beside the real one;
#   * the file may not contain <<, eval, source, or a dot-include: each of those can bring in a
#     whitelist this file cannot see.
#
# Anything outside the shape is a HOLD that names what it saw. A mirror held because its launcher was
# hand-edited is a stale panel and a line in the log; a mirror STARTED on a whitelist that is not the
# runtime one re-routes keys.
CONT = re.compile(r"\\\s*$")
LAUNCH = re.compile(
    r"^exec\s+\"?(?P<prog>[^\"\s$`]*kafka-mirror-maker)\"?"
    r"(?P<args>(?:\s+--[A-Za-z0-9.-]+(?:\s+(?:'[^']*'|\"[^\"$`]*\"|[^'\"\s$`#;|&<>()]+))?)+)\s*$"
)
ARG = re.compile(r"--(?P<flag>[A-Za-z0-9.-]+)(?:\s+(?:'(?P<sq>[^']*)'|\"(?P<dq>[^\"$`]*)\"|(?P<bare>[^'\"\s$`#;|&<>()]+)))?")
FORBIDDEN = ("<<", "eval ", "source ")
# Every OTHER logical line in an accepted file must be one of these. Each generator in this repo
# (seven Jenkinsfile.*-mirror pipelines and ansible/templates/run-mirror.sh.j2) emits exactly a
# shebang, `set -euo pipefail`, one `export KAFKA_LOG4J_OPTS="..."`, comments, and the launcher. A line
# outside that set can change WHAT RUNS -- `PATH=/somewhere-else` in front of a bare `exec
# kafka-mirror-maker` is the plain example -- and then the whitelist in the file says nothing about the
# process launchd starts (deploy Codex round 6).
PRELUDE = (
    re.compile(r"^#!"),
    re.compile(r"^set\s+-[A-Za-z]+(\s+-?[A-Za-z]+)*$"),
    re.compile(r"^export\s+KAFKA_LOG4J_OPTS=\"[^\"$`]*\"$"),
)

def logical_lines(text):
    out, buf = [], ""
    for raw in text.splitlines():
        line = raw.rstrip("\n")
        if CONT.search(line):
            buf += CONT.sub(" ", line)
            continue
        buf += line
        out.append(buf)
        buf = ""
    if buf:
        out.append(buf)
    return out

def whitelist_of(path):
    """The single whitelist literal of an accepted launcher script, or a HOLD."""
    try:
        with open(path, "r", errors="replace") as fh:
            text = fh.read()
    except Exception as exc:
        hold("cannot read %s as text (%s)" % (os.path.basename(path), exc.__class__.__name__))

    base = os.path.basename(path)
    if text.count("--whitelist") != 1:
        hold("%s mentions --whitelist %d time(s); an accepted launcher mentions it exactly once"
             % (base, text.count("--whitelist")))
    for bad in FORBIDDEN:
        if bad in text:
            hold("%s contains %r, which can bring in a whitelist this file cannot see"
                 % (base, bad.strip()))
    # A dot-include: `. somefile` at the start of a logical line.
    launchers = []
    for line in logical_lines(text):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.match(r"^\.\s+\S", stripped):
            hold("%s dot-includes another file, which can bring in a whitelist this file cannot see" % base)
        if "kafka-mirror-maker" in stripped:
            launchers.append(stripped)
            continue
        if not any(rx.match(stripped) for rx in PRELUDE):
            hold("%s has a line outside the accepted shape (%r), which could change what actually runs"
                 % (base, stripped[:60]))
    if len(launchers) != 1:
        hold("%s has %d line(s) running kafka-mirror-maker; an accepted launcher has exactly one"
             % (base, len(launchers)))
    m = LAUNCH.match(launchers[0])
    if not m:
        hold("%s's launcher line is not the accepted `exec [path/]kafka-mirror-maker --flag value ...` shape"
             % base)
    wl_args = [a for a in ARG.finditer(m.group("args")) if a.group("flag") == "whitelist"]
    if len(wl_args) != 1:
        hold("%s's launcher line carries %d --whitelist argument(s)" % (base, len(wl_args)))
    a = wl_args[0]
    # QUOTED only. A BARE value is processed by the shell before kafka-mirror-maker sees it, so the text
    # in the file is not the runtime pattern: `--whitelist es\.safe` reaches the program as `es.safe`,
    # where the dot matches any character and the pattern is BROADER than the file suggests -- which is
    # the direction that produces a wrong CLEAR (deploy Codex round 6).
    if a.group("sq") is None and a.group("dq") is None:
        hold("%s's --whitelist value is unquoted (%r); the shell would rewrite it before the program sees it"
             % (base, a.group("bare")))
    pat = a.group("sq") if a.group("sq") is not None else a.group("dq")
    if not pat:
        hold("%s's --whitelist value is empty" % base)
    return pat

# The program the plist runs decides; every other run-mirror*.sh beside it must ALSO be an accepted
# launcher, and its whitelist is unioned in, so an extra script can only add holds.
patterns = [whitelist_of(program)]
for extra in scripts[1:]:
    pat = whitelist_of(extra)
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
