#!/usr/bin/env bash
# Which MirrorMaker agents are LOADED RIGHT NOW and producing into a given broker — asked of launchd,
# not of the plist files.
#
#   loaded_mirror_agents_for <bootstrap-host:port>
#     prints one label per line, and returns
#       0  the answer is complete (possibly empty)
#       1  it could not be established — then the output is NOT an answer and the caller must refuse
#
# WHY NOT THE PLISTS. scripts/ops/prod-clean-slate.sh discovers agents by reading
# ~/Library/LaunchAgents/com.optionsedge.*.plist and checking each with `launchctl list`. That is the
# right way to RECORD what to resume later (the ledger needs the plist path) and the wrong way to ask
# "is anything still producing into the broker I am about to wipe": a job stays loaded when its plist is
# moved or edited, a plist that fails to parse was silently skipped, and a `launchctl list` that failed
# read as "not loaded" (deploy Codex round 8). Each of those turns a live mirror into a clear gate.
#
# So this asks launchd: `launchctl list` for the loaded labels, then `launchctl list <label>` for each,
# which prints the job AS LOADED including the Program and ProgramArguments it runs. The unit is whichever
# of those paths has a producer.properties beside it -- EVERY absolute path is considered, because a
# wrapper layout runs ["/bin/bash", "/unit/run-mirror.sh"] and the unit is the second one. Two candidate
# directories that both hold one is a refusal, not a guess.
#
# WHAT COUNTS AS TARGETING THE BROKER: bootstrap.servers is a LIST. `host:9092,other:9092` targets this
# broker just as `host:9092` does, and requiring the whole value to equal it hid exactly that case
# (deploy Codex round 9).
#
# And the file is a JAVA PROPERTIES file, which is not a line of `key=value` (deploy Codex round 10).
# `bootstrap.servers=host:9092\,other:9092` is ONE entry to a naive comma split and TWO to Kafka, which
# unescapes the comma first -- so a live prod mirror could be missed by reading the raw text. What is
# implemented here, deliberately and no more:
#   * comment lines (# or !) and blank lines are skipped;
#   * a line ending in an ODD number of backslashes continues onto the next, which is joined after its
#     leading whitespace is dropped;
#   * the key ends at the first UNESCAPED `=`, `:` or whitespace run, and the separator's surrounding
#     whitespace is dropped -- so `bootstrap.servers host:9092` and `bootstrap.servers:host:9092` read
#     the same as `bootstrap.servers=host:9092`;
#   * in the value, `\\ \n \r \t \f \uXXXX` are unescaped and any other `\x` becomes `x` -- which is
#     what makes `\,` one comma inside one entry rather than a separator;
#   * the LAST assignment wins.
# What is NOT implemented: nothing else in the format matters to a host:port list, and the comment says
# which rules are in rather than claiming "java.util.Properties semantics" wholesale.
#
# FAILS CLOSED, and the distinction matters:
#   * a loaded com.optionsedge.* job whose unit directory has NO producer.properties is NOT a mirror --
#     that is how a mirror is identified at all;
#   * a job whose program launchd will not report, whose unit directory cannot be searched or read,
#     whose producer.properties cannot be read, or which has one with NO bootstrap.servers, is a job
#     this cannot classify: a REFUSAL, not an empty answer.
#
# launchctl is called through PATH on purpose, so scripts/ops/loaded-mirror-agents-test.sh drives this
# very code with a stub.

loaded_mirror_agents_for() {
  [ -n "${1-}" ] || { echo "loaded_mirror_agents_for: need <bootstrap-host:port>" >&2; return 1; }
  python3 - "$1" <<'PY'
import os, re, subprocess, sys

target = sys.argv[1].strip()

def fail(msg):
    sys.stderr.write("loaded_mirror_agents_for: %s\n" % msg)
    sys.exit(1)

def launchctl(*args):
    try:
        p = subprocess.run(["launchctl"] + list(args), capture_output=True, text=True)
    except Exception as exc:
        fail("could not run `launchctl %s` (%s)" % (" ".join(args), exc.__class__.__name__))
    if p.returncode != 0:
        fail("`launchctl %s` failed (status %d)" % (" ".join(args), p.returncode))
    return p.stdout

# The table is PID \t Status \t Label, with a header line. The label is everything from the third
# TAB-separated field on, so a label containing spaces survives (a last-field parse lost it).
labels = []
for line in launchctl("list").splitlines():
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    label = "\t".join(parts[2:]).strip()
    if label.startswith("com.optionsedge."):
        labels.append(label)

PROG = re.compile(r'^\s*"Program"\s*=\s*"(?P<p>.*)";\s*$')
ARGS_OPEN = re.compile(r'^\s*"ProgramArguments"\s*=\s*\(\s*$')
ARG = re.compile(r'^\s*"(?P<p>.*)";\s*$')

def paths_of(text):
    """EVERY absolute path launchd reports for the job: the Program and all ProgramArguments entries.
    Not just the first one -- a wrapper layout runs ["/bin/bash", "/unit/run-mirror.sh"], where the unit
    is the SECOND argument and [0] is a shell whose directory holds no producer.properties, so taking [0]
    called a loaded prod mirror 'not a mirror' (deploy Codex round 14)."""
    out, in_args = [], False
    for line in text.splitlines():
        m = PROG.match(line)
        if m:
            if m.group("p") and m.group("p") not in out:
                out.append(m.group("p"))
            continue
        if ARGS_OPEN.match(line):
            in_args = True
            continue
        if in_args:
            if line.strip().startswith(")"):
                in_args = False
                continue
            m = ARG.match(line)
            if m and m.group("p") and m.group("p") not in out:
                out.append(m.group("p"))
    return out

def _logical_lines(text):
    """Properties logical lines: a trailing ODD number of backslashes continues onto the next line, and
    the continuation's leading whitespace is dropped."""
    out, buf = [], None
    for raw in text.split("\n"):
        line = raw.rstrip("\r")
        if buf is None:
            buf = line
        else:
            buf += line.lstrip(" \t\f")
        trailing = len(buf) - len(buf.rstrip("\\"))
        if trailing % 2 == 1:
            buf = buf[:-1]          # drop the continuation backslash and read on
            continue
        out.append(buf)
        buf = None
    if buf is not None:
        out.append(buf)
    return out

def _split_key(logical):
    """(key, value) with the separator's whitespace dropped, honouring escaped separators."""
    i, n = 0, len(logical)
    key = []
    while i < n:
        c = logical[i]
        if c == "\\" and i + 1 < n:
            key.append(logical[i + 1]); i += 2; continue
        if c in "=: \t\f":
            break
        key.append(c); i += 1
    # skip whitespace, then at most one = or :, then whitespace again
    while i < n and logical[i] in " \t\f":
        i += 1
    if i < n and logical[i] in "=:":
        i += 1
        while i < n and logical[i] in " \t\f":
            i += 1
    return "".join(key), logical[i:]

def _entries(value):
    """The comma-separated entries of a properties VALUE, with escapes resolved inside each entry."""
    entries, cur, i, n = [], [], 0, len(value)
    while i < n:
        c = value[i]
        if c == "\\" and i + 1 < n:
            nxt = value[i + 1]
            if nxt == "u" and i + 5 < n + 1:
                try:
                    cur.append(chr(int(value[i + 2:i + 6], 16))); i += 6; continue
                except ValueError:
                    pass
            cur.append({"n": "\n", "r": "\r", "t": "\t", "f": "\f"}.get(nxt, nxt))
            i += 2
            continue
        if c == ",":
            entries.append("".join(cur)); cur = []; i += 1; continue
        cur.append(c); i += 1
    entries.append("".join(cur))
    return [e.strip() for e in entries if e.strip()]

def bootstrap_list(path):
    """Every bootstrap.servers entry of the LAST such assignment, or None when there is none."""
    with open(path, "r", errors="replace") as fh:
        text = fh.read()
    if text.startswith("\ufeff"):
        text = text[1:]
    value = None
    for logical in _logical_lines(text):
        stripped = logical.lstrip(" \t\f")
        if not stripped or stripped[0] in "#!":
            continue
        key, val = _split_key(stripped)
        if key == "bootstrap.servers":
            value = val
    if value is None:
        return None
    return _entries(value)

out = []
for label in labels:
    paths = paths_of(launchctl("list", label))
    if not paths:
        fail("%s is loaded but launchd reports no program for it" % label)
    abs_paths = [p for p in paths if os.path.isabs(p)]
    if not abs_paths:
        fail("%s runs no absolute program (%r)" % (label, paths))

    # The unit is the directory that holds a producer.properties. A job may name several paths (a
    # wrapper and its script); each candidate directory must be readable to answer the question at all.
    units = []
    for p in abs_paths:
        d = os.path.dirname(p)
        if not os.path.isdir(d):
            # A path whose directory is gone cannot be the unit, and cannot be ruled out either.
            fail("%s runs %r, whose directory does not exist, so where it produces is unknown" % (label, p))
        if not os.access(d, os.R_OK | os.X_OK):
            fail("%s's candidate unit directory %r cannot be read, so where it produces is unknown" % (label, d))
        props = os.path.join(d, "producer.properties")
        if os.path.exists(props) and props not in units:
            units.append(props)
    if not units:
        continue                      # not a mirror: no producer config beside anything it runs
    if len(units) > 1:
        fail("%s runs programs in %d directories that each hold a producer.properties (%s), so which one "
             "it produces with is ambiguous" % (label, len(units), ", ".join(units)))
    props = units[0]
    if not os.access(props, os.R_OK):
        fail("%s has an unreadable %s, so where it produces is unknown" % (label, props))
    entries = bootstrap_list(props)
    if entries is None:
        fail("%s's %s has no bootstrap.servers, so where it produces is unknown" % (label, props))
    if target in entries:
        out.append(label)

print("\n".join(out))
PY
}
