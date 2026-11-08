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
WORKFLOW=".github/workflows/deploy-validation.yml"

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
# SECOND INVARIANT: every test module is CLASSIFIED, and one marked CI-REQUIRED is actually RUN.
#
# Collectable is not executed. deploy-validation.yml names its test modules by hand and nothing discovers
# them, so a new module is orphaned by default: this script IMPORTS it, which catches an import error, but
# not one of its assertions ever runs. Measured when this was written: 138 modules under tests/, 27 named
# by the workflow, 111 never executed. That is the same "looks like coverage" failure this script already
# exists to catch, one level up.
#
# So every module must carry EXACTLY ONE marker:
#   # CI-REQUIRED       its assertions must run; the wiring is enforced below
#   # CI-COLLECT-ONLY   imported only, and said so out loud
# An UNCLASSIFIED module fails the build. Without that, a marker is opt-in and the next module added is an
# orphan again unless its author remembers - which is how the 111 happened (Codex r2 finding 4).
#
# WHY THE WIRING CHECK IS NOT IN THE MODULES THEMSELVES: a module asserting its own wiring is circular -
# delete its step and the module never runs, so the assertion never fires (Codex r1). It has to run from
# somewhere independently wired, and this script is that place.
#
# WHAT IT STILL CANNOT DO: enforce its OWN invocation. Delete or disable THIS step and nothing here
# notices. No script can close that; only a required status check / branch protection on the workflow can
# (Codex r2 finding 3). Said plainly so nobody reads this gate as more than it is.
unclassified=()
required_args=()
nrequired=0
ncollect=0
for f in "${files[@]}"; do
  mod="tests.$(basename "$f" .py)"
  has_req=0
  has_only=0
  grep -qE '^[[:space:]]*#[[:space:]]*CI-REQUIRED([[:space:]]|$)' "$f" && has_req=1
  grep -qE '^[[:space:]]*#[[:space:]]*CI-COLLECT-ONLY([[:space:]]|$)' "$f" && has_only=1
  if [ "$has_req" = 1 ] && [ "$has_only" = 1 ]; then
    echo "FAIL: $f carries BOTH CI-REQUIRED and CI-COLLECT-ONLY; exactly one is required." >&2
    fail=1
  elif [ "$has_req" = 1 ]; then
    required_args+=("required=$mod")
    nrequired=$((nrequired + 1))
  elif [ "$has_only" = 1 ]; then
    ncollect=$((ncollect + 1))
  else
    unclassified+=("$f")
  fi
done

if [ "${#unclassified[@]}" -ne 0 ]; then
  echo "FAIL: these test modules carry neither '# CI-REQUIRED' nor '# CI-COLLECT-ONLY':" >&2
  printf '        %s\n' "${unclassified[@]}" >&2
  echo "       Add one. CI-REQUIRED means a mandatory workflow step must run it (enforced); " >&2
  echo "       CI-COLLECT-ONLY means it is imported only, and admits that in writing." >&2
  fail=1
fi

if [ "$nrequired" -eq 0 ]; then
  echo "FAIL: no module is marked CI-REQUIRED, so the wiring invariant checks nothing." >&2
  echo "      If the marker was renamed, rename it in this script too." >&2
  fail=1
else
  wiring="$(python3 scripts/ci/ci_wiring_check.py "$WORKFLOW" "${required_args[@]}" 2>&1)" \
    || wiring="FAIL the wiring check itself could not run: $wiring"
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
echo "classified: $nrequired CI-REQUIRED (wiring enforced), $ncollect CI-COLLECT-ONLY (imported only)"
echo "=== validate-tests-are-collectable: OK ==="
