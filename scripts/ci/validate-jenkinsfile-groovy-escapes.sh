#!/usr/bin/env bash
# A backslash escape inside a '''...''' sh block is read by GROOVY first, then by the shell. Three outcomes:
#
#   1. Groovy rejects it: the WHOLE pipeline fails to compile ("unexpected char: '\'"). #1027 shipped
#      `sed 's/\(x\)/\1/'` in Jenkinsfile.opra-definition-enumeration-mirror and every action of that job
#      failed before running a step.                                                         -> always FAIL
#   2. Groovy accepts it as octal (\0..\377) or \s: the shell receives a control character or a space. `\1` in
#      that same line is legal octal — sed would have received U+0001, not a back-reference. The repo has
#      no legitimate use of either.                                                          -> always FAIL
#   3. Groovy accepts and TRANSLATES it (\b \t \n \f \r \" \' \$ \uXXXX): the shell sees the translated
#      character, not the backslash. Sometimes that is what the author meant (a newline in printf), sometimes
#      not (`\$HOME` expands). Pipelines in service already carry these, so they are RATCHETED: every existing
#      one is counted per file in groovy-escape-baseline.txt, and any NEW one fails.          -> FAIL if new
#
# bash -n sees none of this — it is handed the file text, not what Groovy makes of it. Fix by doubling the
# backslash (\\( in the file is \( in the shell). After removing translating escapes, regenerate the baseline:
#   scripts/ci/validate-jenkinsfile-groovy-escapes.sh --write-baseline
set -uo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import collections, os, sys
BASELINE = "scripts/ci/groovy-escape-baseline.txt"
args = sys.argv[1:]
write_baseline = "--write-baseline" in args
files = [a for a in args if a != "--write-baseline"]
if not files:
    # Recursive: a Jenkinsfile under templates/ or a service directory compiles the same way.
    for root, dirs, names in os.walk("."):
        dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "target")]
        files += [os.path.join(root, n)[2:] for n in names if n.startswith("Jenkinsfile")]
    files.sort()
BS = "\\"
TRANSLATED = set("btnfr\"'$")
HEX = set("0123456789abcdefABCDEF")

def blocks(s):
    """Yield (start, body) of every '''...''' string, closing only on an UNESCAPED ''' as Groovy's lexer does."""
    i, n = 0, len(s)
    while True:
        j = s.find("'''", i)
        if j < 0:
            return
        k = start = j + 3
        while k < n and not s.startswith("'''", k):
            k += 2 if s[k] == BS else 1
        yield start, s[start:min(k, n)]
        i = k + 3

def escapes(body):
    """Yield (index, text, verdict) for every escape: 'fail' (cases 1-2) or 'translated' (case 3)."""
    i, n = 0, len(body)
    while i < n:
        if body[i] != BS:
            i += 1
            continue
        nxt = body[i + 1] if i + 1 < n else ""
        if nxt == BS or nxt in ("\r", "\n"):
            i += 2                                   # a real backslash, or a line continuation
        elif nxt == "":
            yield i, BS, "fail:a backslash at end of file does not compile"
            i += 1
        elif nxt in TRANSLATED:
            yield i, body[i:i + 2], "translated"
            i += 2
        elif nxt in "01234567":
            yield i, body[i:i + 2], "fail:compiles as an OCTAL escape — the shell receives a control character"
            i += 2
        elif nxt == "s":
            yield i, body[i:i + 2], "fail:compiles to a SPACE — the shell never sees \\s"
            i += 2
        elif nxt == "u":
            j = i + 1
            while j < n and body[j] == "u":
                j += 1
            if j + 4 <= n and all(c in HEX for c in body[j:j + 4]):
                yield i, "\\u", "translated"
                i = j + 4
            else:
                yield i, body[i:j + 4], "fail:malformed unicode escape — does not compile"
                i = j
        else:
            yield i, body[i:i + 2], "fail:not a Groovy escape — does not compile"
            i += 2

baseline = collections.Counter()
if os.path.exists(BASELINE) and not write_baseline:
    for line in open(BASELINE):
        line = line.rstrip("\n")
        if line and not line.startswith("#"):
            path, kind, count = line.rsplit(" ", 2)
            baseline[(path, kind)] = int(count)
found = collections.Counter()
where = collections.defaultdict(list)
bad = checked = 0
for path in files:
    s = open(path, newline="").read()
    for start, body in blocks(s):
        checked += 1
        for off, text, verdict in escapes(body):
            line = s.count("\n", 0, start + off) + 1
            if verdict.startswith("fail:"):
                print(f"  FAIL {path}:{line}: {text!r} {verdict[5:]}; write the backslash doubled")
                bad += 1
            else:
                found[(path, text)] += 1
                where[(path, text)].append(line)
if write_baseline:
    with open(BASELINE, "w") as out:
        out.write("# Translating Groovy escapes that pre-date the ratchet: <path> <escape> <count>.\n"
                  "# Generated by validate-jenkinsfile-groovy-escapes.sh --write-baseline. Counts may only go DOWN.\n")
        for (path, kind), c in sorted(found.items()):
            out.write(f"{path} {kind} {c}\n")
    print(f"wrote {BASELINE}: {sum(found.values())} escape(s) in {len({p for p, _ in found})} file(s)")
    sys.exit(0)
for key, c in sorted(found.items()):
    if c > baseline[key]:
        path, kind = key
        print(f"  FAIL {path}: {c - baseline[key]} new {kind!r} escape(s) (lines {where[key]}) — Groovy translates it "
              f"before the shell sees it; write the backslash doubled, or if the translation is intended "
              f"regenerate the baseline and say why in review")
        bad += 1
print(f"checked {checked} ''' block(s) in {len(files)} Jenkinsfile(s); "
      f"{sum(found.values())} translating escape(s) within the baseline of {sum(baseline.values())}")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-jenkinsfile-groovy-escapes: OK ===" || echo "=== validate-jenkinsfile-groovy-escapes: FAILED ==="
exit $rc
