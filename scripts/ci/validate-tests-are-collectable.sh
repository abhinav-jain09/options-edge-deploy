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
    echo "FAIL: $f defines module-level 'def test_' functions. unittest cannot collect those, so"
    echo "      the file runs as 0 tests and reports OK. Put them in a unittest.TestCase."
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

if [ "$fail" -ne 0 ]; then
  echo "=== validate-tests-are-collectable: FAILED ==="
  exit 1
fi
echo "checked ${#files[@]} test module(s); all collectable by unittest"
echo "=== validate-tests-are-collectable: OK ==="
