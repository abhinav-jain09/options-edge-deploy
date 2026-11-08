"""Does every test module that must RUN in CI actually get run by a mandatory workflow step?

Invoked by scripts/ci/validate-tests-are-collectable.sh, which is an independently wired step of
.github/workflows/deploy-validation.yml. Reads the workflow and the classification markers, prints a line
beginning FAIL for every problem and a final OK line when there are none.

WHY NOT IN THE MODULES THEMSELVES. A module asserting its own wiring is circular: delete the step that
runs it and the module never runs, so its assertion never fires. (Codex r1.) The check has to run from
somewhere independently wired.

WHY A WHITELIST AND NOT A BLACKLIST. The first version accepted any step whose `run:` contained a
`python -m unittest` invocation naming the module, and then tried to exclude the ways such a line can fail
to run or fail to matter. That is unwinnable, and Codex walked through it three times: `-k MODULE` makes
the module a name FILTER rather than a target; `|| true`, `|`, `&`, `;`, `set +e` and `set +o errexit` all
let the invocation fail without failing the step; a leading `!` inverts its status; and a line inside a
heredoc body never executes at all. Each round closed the named case and left the shape unconstrained.

So the rule is now a narrow accepted FORM, and anything else - including anything unparseable - simply
does not qualify:

    the step's `run:`, minus blank and comment-only lines, is EXACTLY ONE line, and that line tokenises
    as   <python> -m unittest [boolean flags] <tests.module> [more tests.modules]

with the flags drawn from an explicit list of ones that take no value. A heredoc is more than one line.
`!`, `|`, `&&`, `||`, `;` and `&` are tokens that are neither an allowed flag nor a module. `set +o
errexit` is a second line. `-k` is not an allowed flag. None of them need their own rule.

Verified against reality before being adopted: all 17 steps that host a CI-REQUIRED module today are
single-line plain invocations and every one is accepted.

WHAT THIS STILL CANNOT DO, stated rather than implied: it cannot enforce its OWN invocation. If the
collectability step is deleted or disabled, nothing here notices. No script can close that; only a
required status check / branch protection on the workflow can. (Codex r2 finding 3.)
"""
import re
import shlex
import sys

import yaml

REQUIRED = "CI-REQUIRED"

PYTHON = re.compile(r"^(?:[A-Za-z0-9_.+-]*/)*python3?(?:\.\d+)?$")
MODULE = re.compile(r"^tests\.[A-Za-z0-9_]+$")

# unittest flags that take NO value, read off `python3 -m unittest --help` rather than from memory:
#     -v/--verbose  -q/--quiet  --locals  -f/--failfast  -c/--catch  -b/--buffer
# A flag that CONSUMES the next argument must not be here. With `-k MODULE` or `--durations MODULE` the
# module is swallowed as the option's value, so the step does not run it - and `--durations` in particular
# I had wrongly listed as boolean until Codex r4 caught it; the interpreter's own message is
# "argument --durations: invalid int value: 'tests.nonexistent'".
BOOLEAN_FLAGS = {
    "-v", "--verbose", "-q", "--quiet", "-f", "--failfast", "-c", "--catch", "-b", "--buffer", "--locals",
}
# Named only so the rejection can say WHY. Anything absent from BOOLEAN_FLAGS is rejected regardless, so
# a flag added to unittest in a future Python is fail-closed rather than quietly accepted.
VALUE_FLAGS = {"-k", "-s", "--start-directory", "-p", "--pattern", "-t", "--top-level-directory",
               "--durations"}
# These run no tests at all.
NON_RUNNING_FLAGS = {"-h", "--help"}
SHORT_BOOLEANS = {f[1] for f in BOOLEAN_FLAGS if len(f) == 2}


def qualifies(run, module):
    """Does this step's `run:` block run `module` in a form this checker can vouch for?

    Returns (True, "") or (False, reason).
    """
    lines = [l.strip() for l in (run or "").splitlines()]
    lines = [l for l in lines if l and not l.startswith("#")]
    if not lines:
        return False, "the run block is empty"
    if len(lines) != 1:
        return False, ("the run block holds %d command lines, not one; a multi-line block is not "
                       "vouched for (a heredoc body, a `set +e`, or a conditional could neutralise it)"
                       % len(lines))
    try:
        tokens = shlex.split(lines[0], comments=True)
    except ValueError as exc:
        return False, "the command does not tokenise as shell (%s)" % exc
    if len(tokens) < 4 or not PYTHON.match(tokens[0]):
        return False, "the command does not begin with a python interpreter"
    if tokens[1] != "-m" or tokens[2] != "unittest":
        return False, "the command is not `<python> -m unittest ...`"
    modules, flags = [], []
    for tok in tokens[3:]:
        if MODULE.match(tok):
            modules.append(tok)
        elif tok in BOOLEAN_FLAGS:
            flags.append(tok)
        elif (len(tok) > 2 and tok[0] == "-" and tok[1] != "-"
              and set(tok[1:]) <= SHORT_BOOLEANS):
            # a combined short form such as -vb; legitimate, and refusing it would be a false rejection
            flags.append(tok)
        elif tok in NON_RUNNING_FLAGS:
            return False, "the command passes %s, which prints usage and runs no tests" % tok
        elif tok in VALUE_FLAGS:
            return False, ("the command passes %s, which CONSUMES the next argument, so the module name "
                           "after it is that option's value and is not run" % tok)
        else:
            return False, ("the token %r is neither a tests.* module nor a flag known to take no value, "
                           "so what this command runs cannot be established" % tok)
    if not modules:
        return False, "the command names no tests.* module"
    if module not in modules:
        return False, "the command runs %s, not %s" % (", ".join(modules), module)
    return True, ""


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
    for module in required:
        accepted, rejected = [], []
        for job_name, st in steps:
            run = st.get("run") or ""
            if module not in run:
                continue
            ok, why = qualifies(run, module)
            label = "%s / %s" % (job_name, st.get("name", "<unnamed>"))
            if ok:
                accepted.append((label, st))
            else:
                rejected.append((label, why))
        if not accepted:
            print("FAIL %s is marked %s but no step runs it in an accepted form; its assertions would "
                  "not run in CI" % (module, REQUIRED))
            for label, why in rejected:
                print("FAIL   step [%s] mentions it but %s" % (label, why))
            problems += 1
            continue
        for label, st in accepted:
            if st.get("continue-on-error"):
                print("FAIL %s is run by step [%s] which is continue-on-error, so its failure is ignored"
                      % (module, label))
                problems += 1
            if "if" in st:
                print("FAIL %s is run by step [%s] which is conditional (if: %r), so it may not run"
                      % (module, label, st["if"]))
                problems += 1
    if problems == 0:
        print("OK %d module(s) marked %s, each run by a mandatory step in an accepted form"
              % (len(required), REQUIRED))


if __name__ == "__main__":
    main()
