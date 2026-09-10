#!/usr/bin/env bash
# A backslash escape that Groovy does not accept inside a '''...''' sh block fails the WHOLE pipeline at
# compile time: "unexpected char: '\'". Nothing else here catches it — bash -n parses `sed 's/\(x\)/\1/'`
# happily, review reads it as shell, and it merges. It shipped in Jenkinsfile.opra-definition-enumeration-mirror
# (#1027) and broke every action of that job until the Jenkins linter was run by hand.
#
# Groovy single-quoted strings accept only: \\ \' \" \$ \b \f \n \r \t \u and a line continuation.
# Anything else must be written doubled (\\( in the file is \( in the shell).
set -uo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import glob, re, sys
files = sys.argv[1:] or sorted(glob.glob("Jenkinsfile*"))
ok = set("\\'\"$bfnrtu\n")
bad = 0; checked = 0
for path in files:
    s = open(path).read()
    for m in re.finditer(r"'''(.*?)'''", s, re.S):
        checked += 1
        body, base = m.group(1), m.start(1)
        i = 0
        while i < len(body):
            if body[i] == "\\":
                nxt = body[i + 1] if i + 1 < len(body) else ""
                if nxt == "\\":
                    i += 2; continue
                if nxt not in ok:
                    line = s.count("\n", 0, base + i) + 1
                    print(f"  FAIL {path}:{line}: invalid Groovy escape {repr(chr(92) + nxt)} — write it as {repr(chr(92)*2 + nxt)}")
                    bad += 1
            i += 1
print(f"checked {checked} ''' block(s) in {len(files)} Jenkinsfile(s)")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-jenkinsfile-groovy-escapes: OK ===" || echo "=== validate-jenkinsfile-groovy-escapes: FAILED ==="
exit $rc
