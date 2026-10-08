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
# which prints the job AS LOADED including the Program it runs. The unit directory is that program's
# directory, exactly as the plist-based discovery derives it, and the agent produces into this broker
# when the producer.properties beside it says so.
#
# WHAT COUNTS AS TARGETING THE BROKER: bootstrap.servers is a LIST. `host:9092,other:9092` targets this
# broker just as `host:9092` does, and requiring the whole value to equal it hid exactly that case
# (deploy Codex round 9). The value is split on commas and each entry compared, after stripping
# whitespace and a trailing CR; the LAST assignment in the file wins, which is java.util.Properties'
# own rule.
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

def program_of(label, text):
    prog, in_args = "", False
    for line in text.splitlines():
        m = PROG.match(line)
        if m:
            return m.group("p")
        if ARGS_OPEN.match(line):
            in_args = True
            continue
        if in_args:
            m = ARG.match(line)
            if m:
                return m.group("p")
            if line.strip().startswith(")"):
                in_args = False
    return prog

def bootstrap_list(path):
    """Every bootstrap.servers entry of the LAST such assignment, or None when there is none."""
    value = None
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            m = re.match(r"^\s*bootstrap\.servers\s*=\s*(.*)$", line)
            if m:
                value = m.group(1)
    if value is None:
        return None
    return [e.strip() for e in value.replace("\r", "").split(",") if e.strip()]

out = []
for label in labels:
    prog = program_of(label, launchctl("list", label))
    if not prog:
        fail("%s is loaded but launchd reports no program for it" % label)
    if not os.path.isabs(prog):
        fail("%s runs a non-absolute program (%r)" % (label, prog))
    d = os.path.dirname(prog)
    # A directory that cannot be searched or read cannot answer "is there a producer.properties", so it
    # is a refusal rather than a "not a mirror".
    if not os.path.isdir(d):
        fail("%s runs %r, whose directory does not exist, so where it produces is unknown" % (label, prog))
    if not os.access(d, os.R_OK | os.X_OK):
        fail("%s's unit directory %r cannot be read, so where it produces is unknown" % (label, d))
    props = os.path.join(d, "producer.properties")
    if not os.path.exists(props):
        continue                      # not a mirror
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
