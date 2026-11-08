"""Does every test module that must RUN in CI actually get run by a mandatory workflow step?

Invoked by scripts/ci/validate-tests-are-collectable.sh, which is an independently wired step of
.github/workflows/deploy-validation.yml. Reads the workflow and the classification markers; prints lines
beginning FAIL for every problem and a final OK line when there are none.

WHY NOT IN THE MODULES THEMSELVES. A module asserting its own wiring is circular: delete the step that
runs it and the module never runs, so its assertion never fires. (Codex found that.) The check has to run
from somewhere independently wired.

WHAT THIS STILL CANNOT DO, stated rather than implied: it cannot enforce its OWN continued invocation. If
the collectability step is deleted or disabled, nothing here notices. No script can close that; only a
required status check / branch protection on the workflow can. (Codex r2 finding 3.)
"""
import re
import shlex
import sys

import yaml

REQUIRED = "CI-REQUIRED"
COLLECT_ONLY = "CI-COLLECT-ONLY"

# unittest options that CONSUME the next argument. Without this, `-k tests.foo` would leave `tests.foo`
# looking like a positional target, and a step that only filters by name would satisfy the check while
# running nothing of the sort. (Codex r2 finding 1 - my earlier parser dropped `-k` but kept its value.)
VALUE_OPTS = {"-k", "-p", "--pattern", "-s", "--start-directory", "-t", "--top-level-directory"}

# Shell constructs that let a matching command fail without failing the step. (Codex r2 finding 2.)
SUPPRESSORS = (
    ("||", "an || fallback swallows a non-zero exit"),
    ("&&", "a && chain can leave the invocation's status unchecked"),
    ("|", "a pipeline reports the LAST command's status, not the test run's"),
    ("&", "backgrounding detaches the invocation from the step's exit status"),
    (";", "a following command's status replaces the test run's"),
    ("set +e", "set +e disables failure propagation for the rest of the step"),
    ("|| true", "|| true discards the failure outright"),
    ("exit 0", "an unconditional exit 0 discards the failure"),
)


def unittest_targets(line):
    """The positional module targets of a `python -m unittest` invocation in `line`, or None.

    Returns None when the line is not such an invocation at all.
    """
    try:
        tokens = shlex.split(line, comments=True)
    except ValueError:
        return None
    # find `python`/`python3` ... `-m` `unittest`
    for i, tok in enumerate(tokens):
        if not re.fullmatch(r"(?:.*/)?python3?(?:\.\d+)?", tok):
            continue
        rest = tokens[i + 1:]
        if "-m" not in rest:
            continue
        j = rest.index("-m")
        if j + 1 >= len(rest) or rest[j + 1] != "unittest":
            continue
        args, skip = [], False
        for a in rest[j + 2:]:
            if skip:
                skip = False
                continue
            if a in VALUE_OPTS:
                skip = True
                continue
            if a.startswith("-"):
                continue
            args.append(a)
        return args
    return None


def main():
    workflow_path, classified = sys.argv[1], sys.argv[2:]
    required = [m.split("=", 1)[1] for m in classified if m.startswith("required=")]

    doc = yaml.safe_load(open(workflow_path))
    jobs = doc.get("jobs") or {}
    steps = [(name, st) for name, job in jobs.items() for st in (job.get("steps") or [])]
    if not steps:
        print("FAIL no steps parsed out of %s" % workflow_path)
        return

    problems = 0
    for mod in required:
        invoking = []
        for job_name, st in steps:
            run = st.get("run") or ""
            for line in run.splitlines():
                targets = unittest_targets(line)
                if targets and mod in targets:
                    invoking.append((job_name, st, run, line))
                    break
        if not invoking:
            print("FAIL %s is marked %s but no workflow step invokes it as a `python3 -m unittest` "
                  "target; none of its assertions would run in CI" % (mod, REQUIRED))
            problems += 1
            continue
        for job_name, st, run, line in invoking:
            label = "%s / %s" % (job_name, st.get("name", "<unnamed>"))
            if st.get("continue-on-error"):
                print("FAIL %s is run by step [%s] which is continue-on-error, so its failure is "
                      "ignored" % (mod, label))
                problems += 1
            if "if" in st:
                print("FAIL %s is run by step [%s] which is conditional (if: %r), so it may not run"
                      % (mod, label, st["if"]))
                problems += 1
            # Error suppression is judged on the WHOLE run block: `set +e` on an earlier line, or a
            # trailing `|| true`, both let the invocation fail without failing the step.
            stripped = "\n".join(l for l in run.splitlines() if not l.strip().startswith("#"))
            for token, why in SUPPRESSORS:
                if token in stripped:
                    print("FAIL %s is run by step [%s] whose command contains %r: %s"
                          % (mod, label, token, why))
                    problems += 1
                    break
    if problems == 0:
        print("OK %d module(s) marked %s, each run by a mandatory step" % (len(required), REQUIRED))


main()
