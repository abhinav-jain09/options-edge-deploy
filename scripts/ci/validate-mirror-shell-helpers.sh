#!/usr/bin/env bash
# A shell FUNCTION used in a Jenkins `sh` block that does not DEFINE it is a runtime failure that
# nothing else here catches: `bash -n` parses `es4 "..."` happily as a command, Groovy sees a string,
# and review sees a helper that plainly exists -- twelve lines away, in a different stage's shell.
# Each `sh` step is its own process; nothing carries a function across.
#
# This walks every `sh '''...'''` block in every mirror Jenkinsfile and fails when a block calls one
# of the declared helpers without defining it in that same block.
set -uo pipefail
cd "$(dirname "$0")/../.."
FAILED=0
CHECKED=0

python3 - <<'PY'
import glob, re, sys

# Helpers whose absence is a runtime failure. Extend when a new one is added.
HELPERS = ["es4", "es4_absent_and_prove", "mac_unit_absent_and_prove", "target_is_remote",
           "pid_of_label", "eff", "insp"]

bad = 0
checked = 0
for path in sorted(glob.glob("Jenkinsfile.*-mirror")):
    text = open(path).read()
    # Non-greedy sh ''' ... ''' blocks, in order.
    for m in re.finditer(r"sh\s+'''(.*?)'''", text, re.S):
        block = m.group(1)
        checked += 1
        line_no = text[:m.start()].count("\n") + 1
        for h in HELPERS:
            # A DEFINITION looks like `name() {` (optionally with spaces); a USE is the bare word at
            # the start of a command or after a pipe/&&/;/$( .
            defined = re.search(r"(?m)^\s*%s\s*\(\)\s*\{" % re.escape(h), block) is not None
            used = re.search(r"(?m)(^|[|;&(]|\$\()\s*%s(\s|\")" % re.escape(h), block) is not None
            if used and not defined:
                print(f"  FAIL {path}: sh block at line {line_no} calls `{h}` but does not define it")
                bad += 1
print(f"checked {checked} sh block(s) in mirror Jenkinsfiles")
sys.exit(1 if bad else 0)
PY
rc=$?
if [ "$rc" = "0" ]; then echo "=== validate-mirror-shell-helpers: OK ==="; else echo "=== validate-mirror-shell-helpers: FAILED ==="; fi
exit $rc
