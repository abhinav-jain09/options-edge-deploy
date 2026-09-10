#!/usr/bin/env bash
# A backslash escape inside a '''...''' sh block is read by GROOVY first, then by the shell. Two ways it goes wrong:
#
#   1. Groovy rejects it and the WHOLE pipeline fails to compile ("unexpected char: '\'"). #1027 shipped
#      `sed 's/\(x\)/\1/'` in Jenkinsfile.opra-definition-enumeration-mirror and every action of that job
#      failed before running a step.
#   2. Groovy ACCEPTS it and hands the shell a different character. In that same line `\1` is a legal Groovy
#      octal escape: it compiles, and sed receives U+0001 instead of a back-reference. `\s` compiles to a space.
#
# bash -n sees neither — it is handed the file text, not what Groovy makes of it.
#
# Accepted, per GroovyLexer.g4 EscapeSequence and meaning the same thing to the shell author:
#   \b \t \n \f \r \" \' \\ \$     \u+XXXX (exactly four hex digits)     backslash + LF / CR / CRLF
# Refused: everything Groovy rejects, plus octal (\0..\377) and \s, which compile to another character.
# Write the backslash doubled (\\( in the file is \( in the shell).
set -uo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import os, re, sys
if sys.argv[1:]:
    files = sys.argv[1:]
else:
    # Recursive: a Jenkinsfile under templates/ or a service directory compiles the same way.
    files = []
    for root, dirs, names in os.walk("."):
        dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "target")]
        files += [os.path.join(root, n) for n in names if n.startswith("Jenkinsfile")]
    files.sort()
BS = "\\"
SAME = set("btnfr\"'$") | {BS}
HEX = set("0123456789abcdefABCDEF")

def check(body):
    """Yield (index, text, why) for every escape that fails to compile or compiles to another character."""
    i, n = 0, len(body)
    while i < n:
        if body[i] != BS:
            i += 1
            continue
        nxt = body[i + 1] if i + 1 < n else ""
        if nxt == "":
            yield i, BS, "a backslash immediately before the closing ''' does not compile"
            i += 1
        elif nxt in SAME:
            i += 2
        elif nxt in "\r\n":
            i += 2                                   # line continuation; the LF of a CRLF is plain text
        elif nxt in "01234567":
            yield i, body[i:i + 2], "compiles as an OCTAL escape — the shell receives a control character"
            i += 2
        elif nxt == "s":
            yield i, body[i:i + 2], "compiles to a SPACE — the shell never sees \\s"
            i += 2
        elif nxt == "u":
            j = i + 1
            while j < n and body[j] == "u":
                j += 1
            if j + 4 <= n and all(c in HEX for c in body[j:j + 4]):
                i = j + 4
            else:
                yield i, body[i:j + 4], "malformed unicode escape — does not compile"
                i = j
        else:
            yield i, body[i:i + 2], "not a Groovy escape — does not compile"
            i += 2

bad = checked = 0
for path in files:
    s = open(path, newline="").read()
    for m in re.finditer(r"'''(.*?)'''", s, re.S):
        checked += 1
        for off, text, why in check(m.group(1)):
            line = s.count("\n", 0, m.start(1) + off) + 1
            print(f"  FAIL {path}:{line}: {text!r} {why}; write the backslash doubled")
            bad += 1
print(f"checked {checked} ''' block(s) in {len(files)} Jenkinsfile(s)")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-jenkinsfile-groovy-escapes: OK ===" || echo "=== validate-jenkinsfile-groovy-escapes: FAILED ==="
exit $rc
