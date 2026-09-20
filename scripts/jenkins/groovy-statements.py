#!/usr/bin/env python3
"""A small Groovy reader that REFUSES what it does not model, so a Jenkinsfile assertion can decide on
STATEMENTS instead of on text.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge, and imported by the repository suites that assert "this step runs immediately after that
verification": option-edge-feed-gateway's scripts/jenkins/gateway-guard-test.py (the Test stage's
compile) and options-edge-deploy's tests/test_jenkins_permitted_sha_guard.py (the nifty deploy helper).

WHY THE FAILURE DIRECTION CHANGED. This predicate has now been beaten four times, and the first three
fixes were each a better TEXT RULE:

  * v1 decided adjacency with `count("sh ") == 0`; a `writeFile` walked through it.
  * v2 searched raw text with index/rindex/find; the verification was commented out and both suites said
    ALL PASS, and a second copy of the guarded step was never looked at.
  * v3 (this file, round 3) lexed comments and strings and counted occurrences — but treated `/*` as a
    comment opener without knowing slashy or dollar-slashy strings, and the "independent" count used the
    SAME classification. So `def x = $/ /* /$` … `sh 'mvn -B test'` … `// */` hid a real, executing
    second compile from BOTH checks. Codex ran all three variants under Groovy 4.0.24; each printed
    `EXECUTED: mvn -B test`. v3's header claimed a mistake shows up "never as a pass". That was false.

A fourth text rule would lose too. So the direction is inverted: **this reader refuses any file
containing a construct it does not model, and `verified_steps()` returns False on a refusal.** A shape
it cannot read can no longer be read as absence.

WHAT IT MODELS
  * `//` line comments and `/* … */` block comments.
  * String literals: `'…'`, `"…"`, `'''…'''`, `\"\"\"…\"\"\"`, with backslash escapes.
  * GString interpolation inside the double-quoted forms: `${ … }` is lexed RECURSIVELY AS CODE, with
    its own nested strings, comments and brace nesting, and `$identifier[.identifier…]` is read as a
    plain reference. So `"${ \"/*\" }"` does not open a comment, and `\"${sh('mvn -B test')}\"` is a
    command occurrence that the count sees.
  * Brace / paren / bracket nesting, over CODE only.

WHAT IT REFUSES, by name, with the line number (this list IS the contract):
  * `$/` — a dollar-slashy string.
  * a `/` at CODE position that does not begin `//` or `/*` — that is either a slashy string or
    division, and neither is modelled. None of the definitions these suites read contains one.
  * an unterminated string literal, block comment, or `${` interpolation.
  * interpolation nested more than 8 deep.
  * a byte-order mark.
  * a backslash at end of line in CODE (a line continuation), which the statement splitter does not model.
A refusal names the construct and the line; `verified_steps()` turns it into False, and the calling suite
goes red. Adding a construct to the modelled set is a deliberate change with its own negative control.

WHAT IT STILL CANNOT ESTABLISH, said plainly, because a predicate must name its limits:
  * It reads a FILE, not a running pipeline. It does not compile Groovy, so it does not prove Jenkins
    accepts the file, and it says nothing about what an `agent` expression does at run time.
  * It cannot see through a SHARED-LIBRARY call. If `oeSomething()` runs the guarded command inside the
    library, no text in this file names it and no count here finds it. `verified_steps()` therefore
    establishes a property of THIS FILE's literal commands, not of everything the pipeline executes. The
    validator's own rules (a dedicated step, a fixed command template) are what keep library calls out of
    the effect paths; this reader is the adjacency check, not a call graph.
  * Statement splitting is newline / `;` / `}`-based with a continuation-character rule. A split it gets
    wrong shows up as a refusal (the step is not a statement of its own, or its predecessor is not the
    verification), never as a pass — and, unlike v3, the constructs that could make that claim false are
    refused outright rather than guessed at.

Run `python3 groovy-statements.py --self-test` to execute this file's own assertions, and
`python3 groovy-statements.py --lex <file>…` to print what it refuses in a file.
"""
from __future__ import annotations

import re
import sys

CODE, STRING, COMMENT = 0, 1, 2
# A statement does not end at a newline when the code so far ends on one of these: the expression
# continues on the next line.
CONTINUES = set("+-*/%,&|=<>?:.([{!~^")
MAX_INTERPOLATION_DEPTH = 8

# The canonical provenance-verification statement, in full. `verified_steps()` requires the statement
# before the guarded step to match this ENTIRELY — not to start with it. Codex round 4: a check of
# `itxt.startswith("sh '" + prefix)` accepted
#     sh 'PERMITTED_SHA="${PERMITTED_SHA:-}" bash scripts/jenkins/verify-permitted-tree.sh --dir . || true'
# which turns a dirty-tree refusal into shell success and lets the step proceed. Nothing may follow the
# declared arguments: no `||`, `&&`, `;`, `|`, redirection or trailing comment.
_ALLOW_NAME = r"(?=[A-Za-z0-9._-]*[A-Za-z0-9_-])[A-Za-z0-9._-]+"
_ALLOW_COMP = r"(?:\*|" + _ALLOW_NAME + r")"
_ALLOW_ARG = (r"(?:" + _ALLOW_NAME + r"(?:/" + _ALLOW_NAME + r")*"
              r"|\"" + _ALLOW_COMP + r"(?:/" + _ALLOW_COMP + r")*\")")
VERIFY_STATEMENT = re.compile(
    r"sh 'PERMITTED_SHA=\"\$\{[A-Z][A-Z0-9_]*:[-?]\}\" "
    r"bash scripts/jenkins/verify-permitted-tree\.sh"
    r" --dir (?P<dir>[A-Za-z0-9._][A-Za-z0-9._/-]*)"
    r"(?P<allow>(?: --allow-ignored " + _ALLOW_ARG + r")*)'"
)


class Refusal(Exception):
    """Raised inside the lexer; callers receive it as a (False, reason) verdict."""


def _line_of(src: str, i: int) -> int:
    return src.count("\n", 0, i) + 1


def lex(src: str) -> tuple[list[int], list[str]]:
    """Return (mark, refusals): one CODE/STRING/COMMENT mark per character, and every refusal found.

    When `refusals` is non-empty the marks are NOT trustworthy and no caller may decide on them."""
    mark = [CODE] * len(src)
    refusals: list[str] = []

    def refuse(i: int, what: str) -> None:
        refusals.append("line %d: %s" % (_line_of(src, i), what))

    def paint(a: int, b: int, kind: int) -> None:
        for k in range(a, min(b, len(src))):
            mark[k] = kind

    if src.startswith("﻿"):
        refuse(0, "a byte-order mark, which this reader does not model")

    def scan_code(i: int, end: int, depth: int) -> int:
        """Lex CODE from i until `end` (or an unmatched `}` when depth > 0). Returns the index after."""
        while i < end:
            c = src[i]
            two = src[i:i + 2]
            if two == "//":
                j = src.find("\n", i)
                j = end if j < 0 or j > end else j
                paint(i, j, COMMENT)
                i = j
            elif two == "/*":
                j = src.find("*/", i + 2)
                if j < 0:
                    refuse(i, "an unterminated /* block comment")
                    paint(i, end, COMMENT)
                    return end
                paint(i, j + 2, COMMENT)
                i = j + 2
            elif two == "$/":
                refuse(i, "a dollar-slashy string ($/…/$), which this reader does not model")
                return end
            elif c == "/":
                refuse(i, "a '/' that begins neither // nor /* — a slashy string or a division, "
                          "neither of which this reader models")
                return end
            elif src[i:i + 3] in ("'''", '"""'):
                i = scan_string(i, src[i:i + 3], end, depth)
            elif c in "'\"":
                i = scan_string(i, c, end, depth)
            elif c == "\\" and src[i:i + 2] == "\\\n":
                refuse(i, "a backslash line continuation in code, which the statement splitter "
                          "does not model")
                return end
            elif depth > 0 and c == "}":
                return i
            elif depth > 0 and c == "{":
                i = scan_code(i + 1, end, depth + 1)
                if i < end and src[i] == "}":
                    i += 1
            else:
                i += 1
        return i

    def scan_string(i: int, quote: str, end: int, depth: int) -> int:
        """Lex a string literal starting at i. Interpolation in a double-quoted form is CODE."""
        interpolating = quote[0] == '"'
        j = i + len(quote)
        paint(i, j, STRING)
        while j < end:
            if src[j] == "\\":
                paint(j, j + 2, STRING)
                j += 2
                continue
            if src[j:j + len(quote)] == quote:
                paint(j, j + len(quote), STRING)
                return j + len(quote)
            if interpolating and src[j:j + 2] == "${":
                if depth + 1 > MAX_INTERPOLATION_DEPTH:
                    refuse(j, "interpolation nested more than %d deep" % MAX_INTERPOLATION_DEPTH)
                    return end
                paint(j, j + 2, CODE)
                k = scan_code(j + 2, end, depth + 1)
                if k >= end or src[k] != "}":
                    refuse(j, "an unterminated ${…} interpolation")
                    return end
                paint(k, k + 1, CODE)
                j = k + 1
                continue
            if interpolating and src[j] == "$" and (j + 1 < end and (src[j + 1].isalpha() or src[j + 1] == "_")):
                k = j + 1
                while k < end and (src[k].isalnum() or src[k] in "_."):
                    k += 1
                paint(j, k, CODE)
                j = k
                continue
            paint(j, j + 1, STRING)
            j += 1
        refuse(i, "an unterminated %s string literal" % ("triple-quoted" if len(quote) == 3 else "quoted"))
        return end

    try:
        scan_code(0, len(src), 0)
    except RecursionError:                      # pathological nesting: a refusal, not a crash
        refusals.append("line 1: nesting too deep for this reader")
    return mark, refusals


def code_text(src: str, mark: list[int], a: int, b: int) -> str:
    """src[a:b] with comment bytes dropped and code whitespace collapsed; string bytes kept verbatim."""
    out: list[str] = []
    gap = False
    for i in range(a, b):
        if mark[i] == COMMENT:
            gap = True
            continue
        if mark[i] == CODE and src[i].isspace():
            gap = True
            continue
        if gap and out:
            out.append(" ")
        gap = False
        out.append(src[i])
    return "".join(out).strip()


def statements(src: str, mark: list[int], a: int, b: int) -> list[tuple[int, int]]:
    """The TOP-LEVEL statements of the block body src[a:b], as (start, end) spans.

    A statement ends at a `;` or at a `}` that returns to depth 0, or at a newline where nothing is open
    and the code so far does not end on a continuation character."""
    out: list[tuple[int, int]] = []
    depth, start = 0, -1
    i = a
    while i < b:
        c, k = src[i], mark[i]
        if k == CODE and c in "([{":
            depth += 1
        elif k == CODE and c in ")]}":
            depth -= 1
            if depth < 0:                       # the body's own closing brace
                break
        if start < 0 and not (k == COMMENT or (k == CODE and c.isspace())):
            start = i
        if start >= 0 and depth == 0:
            end = -1
            if k == CODE and c == ";":
                end = i
            elif k == CODE and c == "}":
                end = i + 1
            elif k == CODE and c == "\n":
                txt = code_text(src, mark, start, i)
                if txt and txt[-1] not in CONTINUES:
                    end = i
            if end >= 0:
                if code_text(src, mark, start, end):
                    out.append((start, end))
                start = -1
        i += 1
    if start >= 0 and code_text(src, mark, start, b):
        out.append((start, b))
    return out


def block_bodies(src: str, mark: list[int], a: int, b: int) -> list[tuple[int, int]]:
    """(open, close) index pairs of every matching CODE brace pair opening inside src[a:b]."""
    stack: list[int] = []
    out: list[tuple[int, int]] = []
    for i in range(a, b):
        if mark[i] != CODE:
            continue
        if src[i] == "{":
            stack.append(i)
        elif src[i] == "}" and stack:
            out.append((stack.pop(), i))
    return out


def sibling_lists(src: str, mark: list[int], a: int, b: int) -> list[list[tuple[int, int]]]:
    """Every block body's statement list, for every brace pair inside src[a:b]."""
    return [statements(src, mark, o + 1, c) for o, c in block_bodies(src, mark, a, b)]


def span(text: str, opener: str, closer: str) -> tuple[int, int]:
    """The half-open range of `text` from `opener` to `closer`; (-1, -1) if either is absent."""
    try:
        return text.index(opener), text.index(closer)
    except ValueError:
        return -1, -1


def verified_steps(text: str, a: int, b: int, step: str, verify_dir: str) -> tuple[bool, str]:
    """Is every statement running `step` inside [a,b) and immediately preceded by the verification?

    `step` is the exact shell command (e.g. `mvn -B test`); a statement running it must be exactly
    `sh '<step>'`. Its predecessor must match VERIFY_STATEMENT entirely, for `--dir <verify_dir>`.

    THREE counts must agree, over the WHOLE FILE, not just the span:
      * executable occurrences of the command text anywhere in the file,
      * statements that are exactly `sh '<step>'` anywhere in the file,
      * those of them inside [a,b) whose preceding sibling is the verification.
    A helper defined outside the span, a second copy inside it, or a command in a shape this reader does
    not model as a statement all make the counts differ, and a difference is a refusal.

    Returns (ok, reason). Never raises: a shape this reader does not model is a refusal with a reason."""
    mark, refusals = lex(text)
    if refusals:
        return False, "the reader refuses this file: " + "; ".join(refusals[:3])
    if a < 0 or b < 0 or b <= a:
        return False, "the searched span was not found in the file"
    want = "sh '" + step + "'"
    running = sum(1 for i in range(0, len(text) - len(step) + 1)
                  if text[i:i + len(step)] == step and mark[i] != COMMENT)
    total = 0                                   # statements that ARE the step, anywhere in the file
    verified = 0                                # ...inside the span, preceded by the verification
    for sibs in sibling_lists(text, mark, 0, len(text)):
        for n, (s0, s1) in enumerate(sibs):
            if code_text(text, mark, s0, s1) != want:
                continue
            total += 1
            if not (a <= s0 < b):
                return False, "a statement running the step sits outside the searched span, at line %d" % _line_of(text, s0)
            if n == 0:
                return False, "the step is the first statement of its block: nothing precedes it"
            p0, p1 = sibs[n - 1]
            ptxt = code_text(text, mark, p0, p1)
            m = re.fullmatch(r"timeout\(time: [0-9]+, unit: 'MINUTES'\) \{ (?P<inner>.*) \}", ptxt)
            if not m:
                return False, "the statement before the step is not a timeout block: " + ptxt[:80]
            inner_spans = statements(text, mark, text.index("{", p0) + 1, p1 - 1)
            if len(inner_spans) != 1:
                return False, "the timeout block before the step holds %d statements, not 1" % len(inner_spans)
            itxt = code_text(text, mark, *inner_spans[0])
            vm = VERIFY_STATEMENT.fullmatch(itxt)
            if not vm:
                return False, "the timeout block before the step is not the complete verification statement: " + itxt[:100]
            if vm.group("dir") != verify_dir:
                return False, "the verification before the step is for --dir %s, not %s" % (vm.group("dir"), verify_dir)
            verified += 1
    if verified == 0:
        return False, "no statement is exactly `%s`" % want
    if not (verified == total == running):
        return False, ("%d verified, %d statement(s) are `%s`, and the command occurs %d time(s) in "
                       "executable text — something runs it in a shape this reader does not model"
                       % (verified, total, want, running))
    return True, "%d verified step(s)" % verified


def self_test() -> int:
    ok = fail = 0

    def check(name: str, cond: bool) -> None:
        nonlocal ok, fail
        if cond:
            ok += 1
            print("ok   [%s]" % name)
        else:
            fail += 1
            print("FAIL [%s]" % name)

    src = "a // b { c\nd /* e } */ f\ng 'h } i' j\nk '''l } m''' n\n"
    m, r = lex(src)
    check("a clean file has no refusals", not r)
    check("a line comment is COMMENT to the newline", m[src.index("// b")] == COMMENT and m[src.index("\nd")] == CODE)
    check("a block comment is COMMENT", m[src.index("/* e")] == COMMENT and m[src.index(" f")] == CODE)
    check("a single-quoted literal is STRING", m[src.index("'h }")] == STRING)
    check("a triple-quoted literal is STRING", m[src.index("'''l")] == STRING)
    check("braces inside strings and comments are not code",
          all(m[i] != CODE for i in range(len(src)) if src[i] == "}"))
    esc = "x 'a\\'b } c' y\n"
    check("a backslash escape does not end a literal", lex(esc)[0][esc.index("}")] == STRING)

    # INTERPOLATION is code, with its own strings — the round-4 hiding places
    gs = 'x "${ "/*" }" y\nsh \'mvn -B test\'\n// */\n'
    gm, gr = lex(gs)
    check("interpolation containing a comment opener does not open a comment",
          not gr and gm[gs.index("sh 'mvn")] == CODE)
    gs2 = 'echo("${sh(\'mvn -B test\')}")\n'
    gm2, gr2 = lex(gs2)
    check("a command inside interpolation is CODE, so the count sees it",
          not gr2 and gm2[gs2.index("mvn -B test")] != COMMENT)
    check("a simple $reference in a GString is code", lex('"$env.FOO bar"')[0][2] == CODE)
    check("interpolation in a SINGLE-quoted string is not interpolation",
          lex("'${ \"/*\" }'")[0][3] == STRING)

    # REFUSALS — the contract
    for name, text, expect in [
        ("a dollar-slashy string", "def x = $/ /* /$\n", "dollar-slashy"),
        ("a slashy string", "def x = /a\\/*/\n", "slashy string or a division"),
        ("a division in code", "def y = a / b\n", "slashy string or a division"),
        ("an unterminated block comment", "a /* b\n", "unterminated /* block comment"),
        ("an unterminated string", "a 'b\n", "unterminated"),
        ("an unterminated interpolation", 'a "${ b\n', "unterminated"),
        ("a byte-order mark", "﻿pipeline { }\n", "byte-order mark"),
        ("a backslash line continuation", "def a = b \\\nc\n", "line continuation"),
    ]:
        _, rr = lex(text)
        check("REFUSED: " + name, any(expect in x for x in rr))

    body = "{\n  one()\n  two(a,\n      b)\n  three { four() }\n  // five()\n  six()\n}\n"
    bm, _ = lex(body)
    st = statements(body, bm, 1, len(body) - 2)
    texts = [code_text(body, bm, s, e) for s, e in st]
    check("statements split on newlines at depth 0", texts[:2] == ["one()", "two(a, b)"])
    check("a block statement ends at its closing brace", texts[2] == "three { four() }")
    check("a commented-out statement is not a statement", "five()" not in " ".join(texts))
    check("statements after a comment still parse", texts[-1] == "six()")

    V = "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir ."
    good = ("stage('X') {\n  steps {\n    script {\n"
            "      timeout(time: 10, unit: 'MINUTES') {\n        sh '" + V + " --allow-ignored target'\n      }\n"
            "      sh 'mvn -B test'\n    }\n  }\n}\nstage('Y') {\n}\n")

    def verdict(t: str) -> tuple[bool, str]:
        return verified_steps(t, *span(t, "stage('X')", "stage('Y')"), "mvn -B test", ".")

    check("the good shape is accepted", verdict(good)[0])
    commented = good.replace("      timeout(time: 10, unit: 'MINUTES') {\n", "      // timeout(time: 10, unit: 'MINUTES') {\n") \
                    .replace("        sh '" + V + " --allow-ignored target'\n", "        // sh '" + V + " --allow-ignored target'\n") \
                    .replace("      }\n      sh 'mvn -B test'", "      // }\n      sh 'mvn -B test'")
    check("a verification that exists only in comments is refused", not verdict(commented)[0])
    check("a second, unverified copy of the step is refused",
          not verdict(good.replace("      sh 'mvn -B test'\n", "      sh 'mvn -B test'\n      writeFile file: 'pom.xml', text: 'x'\n      sh 'mvn -B test'\n"))[0])
    check("a step between the verification and the step is refused",
          not verdict(good.replace("      sh 'mvn -B test'", "      writeFile file: 'pom.xml', text: 'x'\n      sh 'mvn -B test'"))[0])
    check("a writer inside the verification timeout is refused",
          not verdict(good.replace("        sh '" + V + " --allow-ignored target'\n", "        sh '" + V + " --allow-ignored target'\n        writeFile file: 'pom.xml', text: 'x'\n"))[0])
    check("the command in a shape this reader does not model is refused",
          not verdict(good.replace("      sh 'mvn -B test'", "      sh 'set -eu; mvn -B test'"))[0])
    check("a span with no such step at all is refused", not verdict(good.replace("      sh 'mvn -B test'\n", ""))[0])
    check("a span that is not in the file is refused",
          not verified_steps(good, *span(good, "stage('Z')", "stage('Y')"), "mvn -B test", ".")[0])

    # ROUND 4: the failure direction, and the complete verification statement
    for name, mutated in [
        ("a dollar-slashy comment hiding a second compile",
         good.replace("      sh 'mvn -B test'\n",
                      "      sh 'mvn -B test'\n      def x = $/ /* /$\n      writeFile file: 'pom.xml', text: 'x'\n      sh 'mvn -B test'\n      // */\n")),
        ("a slashy-string comment hiding a second compile",
         good.replace("      sh 'mvn -B test'\n",
                      "      sh 'mvn -B test'\n      def x = /a\\/*/\n      sh 'mvn -B test'\n      // */\n")),
        ("nested interpolation quotes hiding a second compile",
         good.replace("      sh 'mvn -B test'\n",
                      "      sh 'mvn -B test'\n      def x = \"${ \"/*\" }\"\n      sh 'mvn -B test'\n      // */\n")),
        ("an executable compile inside dollar-slashy interpolation",
         good.replace("      sh 'mvn -B test'\n",
                      "      sh 'mvn -B test'\n      echo($/ // ${sh('mvn -B test')} /$)\n")),
        ("a helper defined outside the span that runs the step",
         "def helper() { sh 'mvn -B test' }\n" + good),
        ("the verification's failure suppressed with || true",
         good.replace(" --allow-ignored target'", " --allow-ignored target || true'")),
        ("a command appended to the verification with ;",
         good.replace(" --allow-ignored target'", " --allow-ignored target; true'")),
        ("a trailing comment appended to the verification",
         good.replace(" --allow-ignored target'", " --allow-ignored target #x'")),
        ("the verification pointed at another directory",
         good.replace("--dir . --allow-ignored target", "--dir app-src --allow-ignored target")),
        ("an undeclarable --allow-ignored argument",
         good.replace("--allow-ignored target", "--allow-ignored ../target")),
    ]:
        check("REFUSED: " + name, not verdict(mutated)[0])

    print("groovy-statements self-test: %d passed, %d failed" % (ok, fail))
    if fail == 0 and ok >= 40:
        print("groovy-statements self-test: ALL PASS")
        return 0
    return 1


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    if "--lex" in argv:
        bad = 0
        for path in argv[argv.index("--lex") + 1:]:
            with open(path, encoding="utf-8") as fh:
                _, refusals = lex(fh.read())
            print("%-60s %s" % (path, "ok" if not refusals else "REFUSED: " + "; ".join(refusals[:3])))
            bad += bool(refusals)
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
