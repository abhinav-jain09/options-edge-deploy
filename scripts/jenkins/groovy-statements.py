#!/usr/bin/env python3
"""A small Groovy reader, so a Jenkinsfile assertion can decide on STATEMENTS instead of on text.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge, and imported by the repository suites that assert "this step runs immediately after that
verification": option-edge-feed-gateway's scripts/jenkins/gateway-guard-test.py (the Test stage's
compile) and options-edge-deploy's tests/test_jenkins_permitted_sha_guard.py (the nifty deploy helper).

WHY IT EXISTS. Those two assertions were written three times and were wrong twice, both times in the
same way: they accepted more than they named.

  * v1 decided adjacency with `count("sh ") == 0`. A `writeFile` between the verification and the
    compile walked through it, because it only looked for shell steps.
  * v2 searched raw text with `index` / `rindex` / `find`. Codex prefixed all three lines of the
    verification block with `// `, left the real step alone, and both suites reported ALL PASS with no
    executable verification anywhere — and separately appended a SECOND copy of the guarded step, which
    the first-occurrence lookup never looked at.

A third string rule would have been a third guess. The defect class is a predicate that accepts more
than it names, and the cheap test for it is to run the assertion against a tree where the thing it
asserts is ABSENT. Both suites now do that, against this reader.

WHAT THIS ESTABLISHES about a file: which byte ranges are code, which are string literals and which are
comments; and, for any block body, the list of top-level statements in it. On top of that,
`verified_steps()` answers one question: is every statement that runs a named step immediately preceded,
as a sibling statement, by the verification step — with comments read as comments.

WHAT IT CANNOT ESTABLISH, said plainly, because a predicate must name its limits:
  * It reads a FILE, not a running pipeline. It does not compile Groovy, so it does not prove Jenkins
    accepts the file, and it says nothing about what a shared-library method, a `load`ed script or an
    `agent` expression does at run time.
  * It models the lexical shapes these four repositories actually use: `//` and `/* */` comments,
    '...', "...", '''...''' and \"\"\"...\"\"\" literals with backslash escapes, and brace/paren/bracket
    nesting. It does NOT model Groovy slashy (`/.../`) or dollar-slashy strings. None appears in these
    repositories; a file that introduced one would be read WRONGLY rather than refused, which is why
    `verified_steps()` also counts: the number of statements that ARE the step must equal the number of
    executable occurrences of the step's command anywhere in the searched span, and any difference is a
    refusal rather than a pass.
  * Statement splitting is newline/`;`/`}`-based with a continuation-character rule. A statement split
    the reader gets wrong shows up as a refusal (the step is not a statement of its own, or its
    predecessor is not the verification), never as a pass.

Run `python3 groovy-statements.py --self-test` to execute this file's own assertions.
"""
from __future__ import annotations

import sys

CODE, STRING, COMMENT = 0, 1, 2
# A statement does not end at a newline when the code so far ends on one of these: the expression
# continues on the next line.
CONTINUES = set("+-*/%,&|=<>?:.([{!~^")


def kinds(src: str) -> list[int]:
    """One CODE / STRING / COMMENT mark per character of `src`."""
    mark = [CODE] * len(src)
    i, n = 0, len(src)
    while i < n:
        two = src[i:i + 2]
        three = src[i:i + 3]
        if two == "//":
            j = src.find("\n", i)
            j = n if j < 0 else j
            for k in range(i, j):
                mark[k] = COMMENT
            i = j
        elif two == "/*":
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            for k in range(i, j):
                mark[k] = COMMENT
            i = j
        elif three == "'''" or three == '"""':
            j, closed = i + 3, False
            while j < n:
                if src[j] == "\\":
                    j += 2
                    continue
                if src[j:j + 3] == three:
                    j += 3
                    closed = True
                    break
                j += 1
            if not closed:
                j = n
            for k in range(i, min(j, n)):
                mark[k] = STRING
            i = j
        elif src[i] in "'\"":
            q, j = src[i], i + 1
            while j < n and src[j] != q:
                j += 2 if src[j] == "\\" else 1
            j = min(j + 1, n)
            for k in range(i, j):
                mark[k] = STRING
            i = j
        else:
            i += 1
    return mark


def code_text(src: str, mark: list[int], a: int, b: int) -> str:
    """src[a:b] with comment bytes dropped and code whitespace collapsed; string literals kept verbatim."""
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
    and the code so far does not end on a continuation character. Comment-only and blank runs are not
    statements."""
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


def verified_steps(text: str, a: int, b: int, step: str, verify_prefix: str) -> tuple[bool, str]:
    """Is every statement running `step` immediately preceded by the verification step?

    `step` is the exact shell command (e.g. `mvn -B test`); the statement must be exactly `sh '<step>'`.
    `verify_prefix` is the start of the verification command; the predecessor must be exactly
    `timeout(time: N, unit: 'MINUTES') { sh '<verify_prefix…>' }` with that one statement in its body.

    Returns (ok, reason). Never raises: a shape this reader does not model is a refusal with a reason."""
    if a < 0 or b < 0 or b <= a:
        return False, "the searched span was not found in the file"
    mark = kinds(text)
    want = "sh '" + step + "'"
    running = 0                                  # executable occurrences of the command, in any shape
    for i in range(a, b - len(step) + 1):
        if text[i:i + len(step)] == step and mark[i] != COMMENT:
            running += 1
    found = 0
    for sibs in sibling_lists(text, mark, a, b):
        for n, (s0, s1) in enumerate(sibs):
            if code_text(text, mark, s0, s1) != want:
                continue
            found += 1
            if n == 0:
                return False, "the step is the first statement of its block: nothing precedes it"
            p0, p1 = sibs[n - 1]
            ptxt = code_text(text, mark, p0, p1)
            if not ptxt.startswith("timeout(time: ") or not ptxt.endswith("}"):
                return False, "the statement before the step is not a timeout block: " + ptxt[:80]
            inner = statements(text, mark, text.index("{", p0) + 1, p1 - 1)
            if len(inner) != 1:
                return False, "the timeout block before the step holds %d statements, not 1" % len(inner)
            itxt = code_text(text, mark, *inner[0])
            if not itxt.startswith("sh '" + verify_prefix):
                return False, "the timeout block before the step is not the verification: " + itxt[:80]
    if found == 0:
        return False, "no statement is exactly `%s`" % want
    if found != running:
        return False, ("%d statement(s) are `%s` but the command occurs %d time(s) in executable text — "
                       "something runs it in a shape this reader does not model" % (found, want, running))
    return True, "%d verified step(s)" % found


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
    m = kinds(src)
    check("a line comment is COMMENT to the newline", m[src.index("// b")] == COMMENT and m[src.index("\nd")] == CODE)
    check("a block comment is COMMENT", m[src.index("/* e")] == COMMENT and m[src.index(" f")] == CODE)
    check("a single-quoted literal is STRING", m[src.index("'h }")] == STRING)
    check("a triple-quoted literal is STRING", m[src.index("'''l")] == STRING)
    check("braces inside strings and comments are not code",
          all(m[i] != CODE for i in range(len(src)) if src[i] == "}"))
    esc = "x 'a\\'b } c' y\n"
    check("a backslash escape does not end a literal", kinds(esc)[esc.index("}")] == STRING)

    body = "{\n  one()\n  two(a,\n      b)\n  three { four() }\n  // five()\n  six()\n}\n"
    bm = kinds(body)
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
    a, b = span(good, "stage('X')", "stage('Y')")
    check("the good shape is accepted", verified_steps(good, a, b, "mvn -B test", V)[0])
    commented = good.replace("      timeout(time: 10, unit: 'MINUTES') {\n", "      // timeout(time: 10, unit: 'MINUTES') {\n") \
                    .replace("        sh '" + V + " --allow-ignored target'\n", "        // sh '" + V + " --allow-ignored target'\n") \
                    .replace("      }\n      sh 'mvn -B test'", "      // }\n      sh 'mvn -B test'")
    check("a verification that exists only in comments is refused", not verified_steps(commented, *span(commented, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    second = good.replace("      sh 'mvn -B test'\n", "      sh 'mvn -B test'\n      writeFile file: 'pom.xml', text: 'x'\n      sh 'mvn -B test'\n")
    check("a second, unverified copy of the step is refused", not verified_steps(second, *span(second, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    between = good.replace("      sh 'mvn -B test'", "      writeFile file: 'pom.xml', text: 'x'\n      sh 'mvn -B test'")
    check("a step between the verification and the step is refused", not verified_steps(between, *span(between, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    inside = good.replace("        sh '" + V + " --allow-ignored target'\n", "        sh '" + V + " --allow-ignored target'\n        writeFile file: 'pom.xml', text: 'x'\n")
    check("a writer inside the verification timeout is refused", not verified_steps(inside, *span(inside, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    hidden = good.replace("      sh 'mvn -B test'", "      sh 'set -eu; mvn -B test'")
    check("the command in a shape this reader does not model is refused", not verified_steps(hidden, *span(hidden, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    none = good.replace("      sh 'mvn -B test'\n", "")
    check("a span with no such step at all is refused", not verified_steps(none, *span(none, "stage('X')", "stage('Y')"), "mvn -B test", V)[0])
    check("a span that is not in the file is refused", not verified_steps(good, *span(good, "stage('Z')", "stage('Y')"), "mvn -B test", V)[0])

    print("groovy-statements self-test: %d passed, %d failed" % (ok, fail))
    if fail == 0 and ok >= 18:
        print("groovy-statements self-test: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv else 0)
