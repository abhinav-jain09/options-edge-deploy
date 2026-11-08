#!/usr/bin/env bash
# validate-tests-are-collectable.sh — CI invariant.
#
# Every tests/test_*.py must be collectable by `python3 -m unittest`, which is the only runner this
# repo has: pytest is not installed and .github/workflows/deploy-validation.yml invokes unittest.
#
# WHY. unittest collects unittest.TestCase methods; it does NOT collect bare module-level `test_`
# functions the way pytest does. Three files were written in that style and therefore reported
# "Ran 0 tests ... OK" -- green, and testing nothing. tests/test_es4_topics_ssot.py sat like that
# with a genuinely failing assertion inside it (an orphan-topic list that #656 had deliberately
# reversed), and nothing noticed, because the file could not be collected in the first place.
#
# A file that cannot be collected is worse than a missing file: it looks like coverage.
set -euo pipefail
cd "$(dirname "$0")/../.."

fail=0
shopt -s nullglob
files=(tests/test_*.py)
shopt -u nullglob
if [ "${#files[@]}" -eq 0 ]; then
  echo "FAIL: no tests/test_*.py found — the glob and the repo layout have diverged" >&2
  exit 1
fi

for f in "${files[@]}"; do
  mod="tests.$(basename "$f" .py)"
  # Bare module-level test functions: pytest-style, invisible to unittest.
  if grep -qE '^def test_' "$f"; then
    echo "FAIL: $f defines module-level 'def test_' functions. unittest collects TestCase methods"
    echo "      only, so such a function runs ONLY if something calls it by hand: on its own the file"
    echo "      reports 'Ran 0 tests ... OK', and beside a hand-written wrapper the next function"
    echo "      added runs nowhere while the file still looks green. Put the bodies in a TestCase."
    fail=1
    continue
  fi
  # Collectable, IMPORTABLE, and non-empty.
  #
  # loadTestsFromName does NOT raise when the module fails to import: it returns a _FailedTest
  # placeholder whose countTestCases() is 1 and records the reason in loader.errors. So a file with
  # a syntax error, a missing dependency, or an exception at import time looked to the first version
  # of this gate like a healthy one-test module. That is the same "looks like coverage" failure this
  # script exists to catch, so loader.errors is the thing to check (found by Codex).
  #
  # THE COUNT IS DELIMITED, AND THE MODULE'S OWN STDOUT IS REDIRECTED AWAY. Importing a test module
  # runs its top level, so anything it prints lands in the same capture: a file containing only
  # `print("1", end="")` collects ZERO tests, yet its "1" concatenated with the count "0" read as
  # "10" and sailed through a bare is-this-a-number check (found by Codex). So the load runs with
  # stdout redirected to stderr -- where it stays visible in the CI log -- and the count is emitted
  # on its own COLLECT line, which is the only line parsed.
  out="$(python3 - "$mod" <<'PY'
import contextlib, sys, unittest
loader = unittest.TestLoader()
try:
    with contextlib.redirect_stdout(sys.stderr):
        suite = loader.loadTestsFromName(sys.argv[1])
except BaseException as e:
    print("COLLECT ERR %s: %s" % (type(e).__name__, e)); raise SystemExit(0)
if loader.errors:
    print("COLLECT ERR %s" % loader.errors[0].strip().splitlines()[-1]); raise SystemExit(0)
print("COLLECT %d" % suite.countTestCases())
PY
)" || out="COLLECT ERR python3 exited non-zero"
  n="$({ printf '%s\n' "$out" | grep '^COLLECT ' || true; } | tail -1)"
  n="${n#COLLECT }"
  case "$n" in
    ''|ERR*|*[!0-9]*)
      echo "FAIL: $mod is not importable/collectable by unittest:"
      printf '%s\n' "${n:-<no COLLECT line: the module replaced or suppressed stdout>}" | sed 's/^/        /'
      fail=1
      ;;
    0)
      echo "FAIL: $mod collects 0 tests — it would report OK while testing nothing."
      fail=1
      ;;
  esac
done

# ---------------------------------------------------------------------------------------------------
# SECOND INVARIANT: a module marked CI-REQUIRED must actually be RUN by a step of the workflow.
#
# Collectable is not the same as executed. deploy-validation.yml names its test modules by hand and
# nothing discovers them, so a new module is orphaned by default: this script imports it, which catches
# an import error, but not one of its assertions ever runs. Measured when this was written: 138 modules
# under tests/, 26 named by the workflow, 112 never executed. That is the same "looks like coverage"
# failure this script already exists to catch, one level up.
#
# WHY THE CHECK LIVES HERE AND NOT IN THE MODULE IT PROTECTS. The obvious place is a self-assertion in
# the module itself, and it cannot work, because it is circular: delete the module's step and the module
# never runs in CI, so its own assertion never fires and the workflow stays green. A mutation test only
# appears to catch that, because the harness invokes the module out-of-band. The check has to run from
# somewhere independently wired, and this script is exactly that: one pre-existing step whose whole job
# is the test-module inventory. (Codex found the circularity in the self-assertion version.)
#
# A module opts in with a `# CI-REQUIRED` comment line. The step that runs it must invoke it through
# python3 -m unittest with the module as an ARGUMENT, must not be continue-on-error, and must carry no
# `if:` - each of those can leave the module unrun while a looser check still passes.
required=()
for f in "${files[@]}"; do
  if grep -qE '^[[:space:]]*#[[:space:]]*CI-REQUIRED([[:space:]]|$)' "$f"; then
    required+=("tests.$(basename "$f" .py)")
  fi
done

if [ "${#required[@]}" -eq 0 ]; then
  echo "FAIL: no test module carries a '# CI-REQUIRED' marker, so this invariant checks nothing." >&2
  echo "      If the marker was renamed, rename it here too." >&2
  fail=1
else
  wiring="$(python3 - ".github/workflows/deploy-validation.yml" "${required[@]}" <<'PY'
import re
import sys

import yaml

workflow, modules = sys.argv[1], sys.argv[2:]
doc = yaml.safe_load(open(workflow))
jobs = doc.get("jobs") or {}
steps = [st for job in jobs.values() for st in (job.get("steps") or [])]
if not steps:
    print("FAIL no steps parsed out of %s" % workflow)
    raise SystemExit(0)

INVOCATION = re.compile(r"\bpython3?\b.*?-m\s+unittest\b")

for mod in modules:
    # The module must appear as an ARGUMENT of a real `python3 -m unittest` invocation. A step whose
    # `run:` merely CONTAINS the module name is not enough: `echo unittest tests.foo` would satisfy that,
    # and so would a mention in a comment.
    invoked = []
    for st in steps:
        for line in (st.get("run") or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if not INVOCATION.search(stripped):
                continue
            after = stripped.split("-m", 1)[1]
            args = [a for a in after.split() if a and not a.startswith("-") and a != "unittest"]
            if mod in args:
                invoked.append(st)
                break
    if not invoked:
        print("FAIL %s carries the CI-REQUIRED marker but no step runs it with python3 -m unittest; "
              "its assertions would never execute in CI" % mod)
        continue
    for st in invoked:
        name = st.get("name", "<unnamed>")
        if st.get("continue-on-error"):
            print("FAIL %s is run by step %r which is continue-on-error, so a failure is ignored"
                  % (mod, name))
        if "if" in st:
            print("FAIL %s is run by step %r which is conditional (if: %r), so it may not run at all"
                  % (mod, name, st["if"]))
print("OK %d module(s) required and wired" % len(modules))
PY
)" || wiring="FAIL the wiring check itself could not run"
  printf '%s\n' "$wiring" | sed 's/^/  /'
  if printf '%s\n' "$wiring" | grep -q '^FAIL'; then
    fail=1
  elif ! printf '%s\n' "$wiring" | grep -q '^OK '; then
    echo "FAIL: the wiring check produced neither OK nor FAIL - treating as failed." >&2
    fail=1
  fi
fi

if [ "$fail" -ne 0 ]; then
  echo "=== validate-tests-are-collectable: FAILED ==="
  exit 1
fi
echo "checked ${#files[@]} test module(s); all collectable by unittest"
echo "checked ${#required[@]} CI-REQUIRED module(s); each is run by a mandatory workflow step"
echo "=== validate-tests-are-collectable: OK ==="
