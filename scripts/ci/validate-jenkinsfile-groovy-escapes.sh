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
#      one is recorded with the line it sits on in groovy-escape-baseline.jsonl; any NEW or MOVED one fails.          -> FAIL if new
#
# bash -n sees none of this — it is handed the file text, not what Groovy makes of it. Fix by doubling the
# backslash (\\( in the file is \( in the shell). After removing translating escapes, regenerate the baseline:
#   scripts/ci/validate-jenkinsfile-groovy-escapes.sh --write-baseline
set -uo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import collections, json, os, sys
BASELINE = "scripts/ci/groovy-escape-baseline.jsonl"
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
    # Yield (start, body) of every triple-single-quoted string Groovy would actually lex as one.
    # Everything else that can CONTAIN three quotes is skipped first, as the lexer does: // and /* */
    # comments, triple-double-quoted strings, and single-line '...' and "..." strings. Inside a body a
    # backslash consumes the next character, so only an unescaped delimiter closes it.
    i, n = 0, len(s)
    while i < n:
        if s.startswith("'''", i):
            k = start = i + 3
            while k < n and not s.startswith("'''", k):
                k += 2 if s[k] == BS else 1
            yield start, s[start:min(k, n)]
            i = k + 3
        elif s.startswith('"""', i):
            k = i + 3
            while k < n and not s.startswith('"""', k):
                k += 2 if s[k] == BS else 1
            i = k + 3
        elif s.startswith("//", i):
            k = s.find("\n", i)
            i = n if k < 0 else k + 1
        elif s.startswith("/*", i):
            k = s.find("*/", i + 2)
            i = n if k < 0 else k + 2
        elif s[i] in "'\"":
            q, k = s[i], i + 1
            while k < n and s[k] != q and s[k] != "\n":
                k += 2 if s[k] == BS else 1
            i = k + 1
        else:
            i += 1

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
    for line in open(BASELINE, encoding="utf-8"):
        if line.strip() and not line.startswith("#"):
            rec = json.loads(line)
            baseline[(rec["path"], rec["escape"], rec["line"])] = rec["count"]
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
                # Keyed by the LINE the escape sits on, not a per-file count: moving an escape, or adding
                # one while removing another, changes the key and fails.
                text_of_line = s.splitlines()[line - 1].strip()
                found[(path, text, text_of_line)] += 1
                where[(path, text, text_of_line)].append(line)
if write_baseline:
    with open(BASELINE, "w", encoding="utf-8") as out:
        out.write("# Translating Groovy escapes that pre-date the ratchet, one JSON object per (file, escape, line text).\n"
                  "# Generated by validate-jenkinsfile-groovy-escapes.sh --write-baseline. Entries may only disappear.\n")
        for (path, kind, text), c in sorted(found.items()):
            out.write(json.dumps({"path": path, "escape": kind, "line": text, "count": c}, ensure_ascii=False) + "\n")
    print(f"wrote {BASELINE}: {sum(found.values())} escape(s) on {len(found)} line(s) in {len({k[0] for k in found})} file(s)")
    sys.exit(0)
for key, c in sorted(found.items()):
    if c > baseline[key]:
        path, kind, text = key
        print(f"  FAIL {path}:{where[key][0]}: new {kind!r} escape — Groovy translates it before the shell sees it; "
              f"write the backslash doubled, or if the translation is intended regenerate the baseline and say why in review")
        bad += 1
print(f"checked {checked} ''' block(s) in {len(files)} Jenkinsfile(s); "
      f"{sum(found.values())} translating escape(s) within the baseline of {sum(baseline.values())}")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-jenkinsfile-groovy-escapes: OK ===" || echo "=== validate-jenkinsfile-groovy-escapes: FAILED ==="
exit $rc
