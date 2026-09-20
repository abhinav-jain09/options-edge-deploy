#!/usr/bin/env python3
"""A small Groovy reader that REFUSES what it cannot decide, so a Jenkinsfile assertion can decide on
STATEMENTS instead of on text.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge, and imported by the repository suites that assert "this command runs only immediately
after that verification": option-edge-feed-gateway's scripts/jenkins/gateway-guard-test.py (every Maven
invocation that consumes the primary checkout) and options-edge-deploy's
tests/test_jenkins_permitted_sha_guard.py (the nifty deploy helper).

WHY THE FAILURE DIRECTION IS WHAT IT IS. This predicate has been beaten five times, and the first three
fixes were each a better TEXT RULE:

  * v1 decided with `count("sh ") == 0`; a `writeFile` walked through it.
  * v2 searched raw text; the verification was commented out and both suites said ALL PASS.
  * v3 lexed comments and strings, but did not know slashy or dollar-slashy strings, and its
    "independent" count used the same classification — so `$/ /* /$` hid an executing second compile.
  * v4 inverted the direction and refused unmodelled LEXICAL constructs. That shut the lexical class —
    and the next defect was on a different axis: `if (false)` on its own line, with the verification
    block on the next, is two statements to a newline-splitter and one statement to Groovy, so the
    verification never ran and the compile did. Lexical refusal does not establish EXECUTION ORDER.
  * v4 also matched the guarded command by its literal spelling, so `sh 'mvn\\u0020-B test'` — which
    Groovy reads as exactly `mvn -B test` — and plain `sh 'mvn test'` both slipped past, and it matched
    the verification's permission source as "any uppercase variable", so `${GIT_COMMIT:-}` counted as a
    permission.

So the rule is the same one, applied on every axis this has been broken on: **decide, or refuse.** There
is no "never as a pass" sentence in this header any more — it was written twice and was false twice.
What is here instead is the list of what is decided and the list of what is refused.

WHAT IT MODELS
  * Groovy's pre-lexical `\\uXXXX` unicode escapes, decoded across the whole source first, exactly as the
    Groovy lexer does (an even number of preceding backslashes means it is not an escape). So
    `sh 'mvn\\u0020-B test'` is read as `sh 'mvn -B test'` and is classified like any other.
  * `//` line comments and `/* … */` block comments.
  * String literals: `'…'`, `"…"`, `'''…'''`, `\"\"\"…\"\"\"`, with backslash escapes.
  * `${ … }` interpolation inside the double-quoted forms, lexed RECURSIVELY AS CODE with its own nested
    strings, comments and braces, and `$identifier[.identifier…]` as a plain reference.
  * Brace / paren / bracket nesting over CODE. Interpolation depth and ordinary brace depth are counted
    separately, so ordinary nesting never consumes the interpolation allowance.
  * Statements of a block body, split at `;`, at a `}` returning to depth 0, or at a newline where
    nothing is open and the code does not end on a continuation character.

WHAT IT REFUSES — by name, with the line number. This list is the contract.
  LEXICAL
  * `$/` — a dollar-slashy string.
  * a `/` at CODE position that begins neither `//` nor `/*` — a slashy string or a division.
  * an unterminated string literal, block comment, or `${` interpolation.
  * interpolation nested more than 8 deep (interpolation only; ordinary braces do not count).
  * a byte-order mark.
  * a backslash line continuation in CODE, with either LF or CRLF.
  CONTROL FLOW — because lexical correctness does not establish execution order
  * a brace-less control-flow head: a statement beginning `if`, `else`, `for`, `while`, `do`, `try`,
    `catch`, `finally`, `switch` or `synchronized` that does not end with `}`. Such a head OWNS the next
    statement in Groovy and does not in a newline-splitter, so no ordering claim can be made around it.
  * a label (`name:` alone), for the same reason.
  COMMAND CLASSIFICATION
  * an executable occurrence of the watched command token that the caller's classifier cannot decide,
    or that is not the whole body of a `sh '…'` statement when it is a guarded one. An occurrence counts
    as executable when it sits in CODE (inside a `${…}` interpolation, say) or inside a string literal
    whose OWNER is an `sh` step — `sh '…'`, `sh '''…'''`, `sh(…)`, including one written inside an
    interpolation such as `echo("${sh('…')}")`. A string owned by anything else (a `description:`, an
    `echo`, an `error(…)` message) is DATA: Jenkins does not run it, and prose that mentions the command
    is not the command.
A refusal makes the calling assertion False and CI red. Extending the modelled set is a deliberate
change with its own negative control.

WHAT IT STILL CANNOT ESTABLISH
  * It reads a FILE, not a running pipeline: it does not compile Groovy, so it does not prove Jenkins
    accepts the file, and it says nothing about what an `agent` expression does at run time. It also
    does not reject every malformed input — a file Groovy would refuse may be read without complaint.
  * It cannot see through a SHARED-LIBRARY call. If a library method runs the guarded command, no text
    in this file names it. This is an adjacency check over this file's literal commands, not a call
    graph; the validator's own rules are what keep library calls out of the effect paths.

Run `python3 groovy-statements.py --self-test`, or `--lex <file>…` to print what it refuses in a file.
"""
from __future__ import annotations

import re
import sys

CODE, STRING, COMMENT = 0, 1, 2
CONTINUES = set("+-*/%,&|=<>?:.([{!~^")
MAX_INTERPOLATION_DEPTH = 8
CONTROL_HEADS = ("if", "else", "for", "while", "do", "try", "catch", "finally", "switch", "synchronized")

# The canonical provenance-verification statement, in full. The statement before a guarded command must
# match this ENTIRELY — not start with it (round 4: `… --dir . || true` was accepted, turning a
# dirty-tree refusal into shell success) — and its PERMISSION SOURCE is compared against the name the
# caller expects (round 5: `${GIT_COMMIT:-}` was accepted as a permission because the pattern allowed
# any uppercase variable).
_ALLOW_NAME = r"(?=[A-Za-z0-9._-]*[A-Za-z0-9_-])[A-Za-z0-9._-]+"
_ALLOW_COMP = r"(?:\*|" + _ALLOW_NAME + r")"
_ALLOW_ARG = (r"(?:" + _ALLOW_NAME + r"(?:/" + _ALLOW_NAME + r")*"
              r"|\"" + _ALLOW_COMP + r"(?:/" + _ALLOW_COMP + r")*\")")
VERIFY_STATEMENT = re.compile(
    r"sh 'PERMITTED_SHA=\"\$\{(?P<perm>[A-Z][A-Z0-9_]*):[-?]\}\" "
    r"bash scripts/jenkins/verify-permitted-tree\.sh"
    r" --dir (?P<dir>[A-Za-z0-9._][A-Za-z0-9._/-]*)"
    r"(?P<allow>(?: --allow-ignored " + _ALLOW_ARG + r")*)'"
)

GUARDED, OTHER, UNDECIDABLE = "GUARDED", "OTHER", "UNDECIDABLE"


def decode_unicode_escapes(src: str) -> str:
    """Groovy decodes `\\uXXXX` across the WHOLE source before lexing; so does this.

    An even number of backslashes before the `u` means the backslash is itself escaped and this is not a
    unicode escape. One or more `u`s may follow the backslash."""
    out: list[str] = []
    i, n = 0, len(src)
    while i < n:
        if src[i] != "\\":
            out.append(src[i])
            i += 1
            continue
        j = i
        while j < n and src[j] == "\\":
            j += 1
        runs = j - i
        k = j
        while k < n and src[k] == "u":
            k += 1
        if runs % 2 == 1 and k > j and k + 4 <= n and all(c in "0123456789abcdefABCDEF" for c in src[k:k + 4]):
            out.append("\\" * (runs - 1))
            out.append(chr(int(src[k:k + 4], 16)))
            i = k + 4
        else:
            out.append("\\" * runs)
            i = j
    return "".join(out)


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

    def scan_code(i: int, end: int, interp: int, braces: int) -> int:
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
                i = scan_string(i, src[i:i + 3], end, interp)
            elif c in "'\"":
                i = scan_string(i, c, end, interp)
            elif c == "\\" and (src[i:i + 2] == "\\\n" or src[i:i + 3] == "\\\r\n"):
                refuse(i, "a backslash line continuation in code, which the statement splitter "
                          "does not model")
                return end
            elif interp > 0 and braces == 0 and c == "}":
                return i                      # the interpolation's own closing brace
            elif c == "{":
                i = scan_code(i + 1, end, interp, braces + 1)
                if i < end and src[i] == "}":
                    i += 1
                continue
            elif braces > 0 and c == "}":
                return i
            else:
                i += 1
        return i

    def scan_string(i: int, quote: str, end: int, interp: int) -> int:
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
                if interp + 1 > MAX_INTERPOLATION_DEPTH:
                    refuse(j, "interpolation nested more than %d deep" % MAX_INTERPOLATION_DEPTH)
                    return end
                paint(j, j + 2, CODE)
                k = scan_code(j + 2, end, interp + 1, 0)
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
        scan_code(0, len(src), 0, 0)
    except RecursionError:
        refusals.append("line 1: nesting too deep for this reader")
    return mark, refusals


def code_text(src: str, mark: list[int], a: int, b: int) -> str:
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
    out: list[tuple[int, int]] = []
    depth, start = 0, -1
    i = a
    while i < b:
        c, k = src[i], mark[i]
        if k == CODE and c in "([{":
            depth += 1
        elif k == CODE and c in ")]}":
            depth -= 1
            if depth < 0:
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
    return [statements(src, mark, o + 1, c) for o, c in block_bodies(src, mark, a, b)]


LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*:$")


def string_owner(src: str, mark: list[int], i: int) -> str:
    """The identifier that owns the string literal containing index `i`: sh, echo, description, ...

    A string is a SHELL BODY when its owner is `sh`; anything else is data. It is "the token just before
    the opening quote, skipping whitespace, '(' and ':'", which reads sh 'x', sh('x') and
    echo("${sh('x')}") correctly, and reads `description: 'x'` as description."""
    j = i
    while j > 0 and mark[j - 1] == STRING:
        j -= 1
    k = j - 1
    while k >= 0 and (src[k].isspace() or src[k] in "(:"):
        k -= 1
    end = k + 1
    while k >= 0 and (src[k].isalnum() or src[k] == "_"):
        k -= 1
    return src[k + 1:end]


def control_flow_refusals(src: str, mark: list[int], sibs: list[list[tuple[int, int]]]) -> list[str]:
    """Statement shapes whose execution ORDER this reader cannot prove.

    Round 5: `if (false)` on its own line owns the next statement in Groovy and does not in a
    newline-splitter, so a verification written under it never runs while the splitter reports it as an
    ordinary preceding sibling. Any brace-less control-flow head, and any label, is refused."""
    out: list[str] = []
    for lst in sibs:
        for s0, s1 in lst:
            txt = code_text(src, mark, s0, s1)
            first = txt.split("(")[0].split()
            head = first[0] if first else ""
            if head in CONTROL_HEADS and not txt.endswith("}"):
                out.append("line %d: a brace-less `%s` head, which owns the statement after it in Groovy "
                           "but not in this reader's splitter — write the body in braces" % (_line_of(src, s0), head))
            elif LABEL_RE.match(txt):
                out.append("line %d: a label (`%s`), whose scope this reader does not model"
                           % (_line_of(src, s0), txt))
    return out


def mvn_classifier(primary_pom: str = "pom.xml"):
    """A classifier for `mvn`: does this invocation consume the PRIMARY checkout's source?

    "However spelled" (round 5): not `mvn -B test` literally, but any invocation whose goals reach the
    compile phase or later and whose project is this checkout's own pom. `-f <other>/pom.xml` is another
    source, verified by its own verify; a plugin goal such as `help:evaluate` reads no source."""
    PHASES = {"compile", "test-compile", "test", "package", "pre-integration-test", "integration-test",
              "post-integration-test", "verify", "install", "deploy"}
    VALUE_OPTS = {"-f", "--file", "-s", "--settings", "-gs", "-t", "--toolchains", "-P", "--activate-profiles",
                  "-pl", "--projects", "-T", "--threads", "-rf", "--resume-from", "-l", "--log-file"}

    def classify(cmd: str) -> tuple[str, str]:
        words = cmd.split()
        if not words or words[0] != "mvn":
            return OTHER, ""
        for w in words:
            if w.startswith("$") or "${" in w or w.startswith("`"):
                return UNDECIDABLE, "an mvn invocation with a value this reader cannot resolve: " + w
        goals, skip = [], False
        for w in words[1:]:
            if skip:
                skip = False
                continue
            if w in VALUE_OPTS:
                skip = True
                continue
            if w.startswith("-"):
                continue
            goals.append(w)
        pom = primary_pom
        for n, w in enumerate(words):
            if w in ("-f", "--file") and n + 1 < len(words):
                pom = words[n + 1]
        if pom != primary_pom:
            return OTHER, ""                  # another project's source, bound by its own verification
        if any(g in PHASES for g in goals):
            return GUARDED, ""
        return OTHER, ""

    return classify


def literal_classifier(command: str):
    """A classifier for one exact command, e.g. `bash scripts/deploy/service-deploy.sh`."""
    def classify(cmd: str) -> tuple[str, str]:
        return (GUARDED, "") if cmd == command else (OTHER, "")
    return classify


def verified_commands(text: str, token: str, classify, permission_var: str, verify_dir: str) -> tuple[bool, str]:
    """Is EVERY guarded command in this file immediately preceded, as a sibling statement, by the
    complete verification statement for `permission_var` and `verify_dir`?

    `token` is the command word whose every executable occurrence must be accounted for (`mvn`, or the
    first word of a literal command). An occurrence the classifier cannot decide, or a guarded one that
    is not the whole body of an `sh '…'` statement, is a refusal — not an absence.

    Returns (ok, reason). Never raises."""
    src = decode_unicode_escapes(text)
    mark, refusals = lex(src)
    if refusals:
        return False, "the reader refuses this file: " + "; ".join(refusals[:3])
    sibs = sibling_lists(src, mark, 0, len(src))
    cf = control_flow_refusals(src, mark, sibs)
    if cf:
        return False, "the reader refuses this file: " + "; ".join(cf[:3])

    sh_stmt = re.compile(r"^sh '([^']*)'$")
    guarded_spans: list[tuple[int, int]] = []
    verified = 0
    for lst in sibs:
        for n, (s0, s1) in enumerate(lst):
            m = sh_stmt.match(code_text(src, mark, s0, s1))
            if not m:
                continue
            kind, why = classify(m.group(1))
            if kind == UNDECIDABLE:
                return False, "line %d: %s" % (_line_of(src, s0), why)
            if kind != GUARDED:
                continue
            guarded_spans.append((s0, s1))
            if n == 0:
                return False, "line %d: the guarded command is the first statement of its block" % _line_of(src, s0)
            p0, p1 = lst[n - 1]
            ptxt = code_text(src, mark, p0, p1)
            if not re.fullmatch(r"timeout\(time: [0-9]+, unit: 'MINUTES'\) \{ .* \}", ptxt):
                return False, "line %d: the statement before the guarded command is not a timeout block: %s" % (
                    _line_of(src, s0), ptxt[:80])
            inner = statements(src, mark, src.index("{", p0) + 1, p1 - 1)
            if len(inner) != 1:
                return False, "line %d: the timeout block before it holds %d statements, not 1" % (
                    _line_of(src, s0), len(inner))
            vm = VERIFY_STATEMENT.fullmatch(code_text(src, mark, *inner[0]))
            if not vm:
                return False, "line %d: the timeout block before it is not the complete verification statement" % _line_of(src, s0)
            if vm.group("perm") != permission_var:
                return False, "line %d: the verification reads ${%s}, not the expected ${%s}" % (
                    _line_of(src, s0), vm.group("perm"), permission_var)
            if vm.group("dir") != verify_dir:
                return False, "line %d: the verification is for --dir %s, not %s" % (
                    _line_of(src, s0), vm.group("dir"), verify_dir)
            verified += 1

    # every executable occurrence of the token must be inside a statement this reader classified, or be
    # classifiable where it stands (a command inside a larger shell body); anything else is a refusal
    for m in re.finditer(r"(?<![A-Za-z0-9_./-])" + re.escape(token) + r"(?![A-Za-z0-9_-])", src):
        i = m.start()
        if mark[i] == COMMENT:
            continue
        if mark[i] == STRING and string_owner(src, mark, i) != "sh":
            continue                          # a description, an echo, an error message: data, not a command
        if any(s0 <= i < s1 for s0, s1 in guarded_spans):
            continue
        tail = src[i:]
        cut = min((p for p in (tail.find(c) for c in ("\n", ";", "&&", "||", "|", ")", "'")) if p > 0), default=len(tail))
        kind, why = classify(tail[:cut].strip())
        if kind == GUARDED:
            return False, "line %d: a guarded command that is not a dedicated, verified step: %s" % (
                _line_of(src, i), tail[:cut].strip()[:80])
        if kind == UNDECIDABLE:
            return False, "line %d: %s" % (_line_of(src, i), why)
    if verified == 0:
        return False, "no guarded command was found at all"
    return True, "%d verified guarded command(s)" % verified


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

    gs = 'x "${ "/*" }" y\nsh \'mvn -B test\'\n// */\n'
    check("interpolation containing a comment opener does not open a comment",
          not lex(gs)[1] and lex(gs)[0][gs.index("sh 'mvn")] == CODE)
    check("a simple $reference in a GString is code", lex('"$env.FOO bar"')[0][2] == CODE)
    check("interpolation in a SINGLE-quoted string is not interpolation",
          lex("'${ \"/*\" }'")[0][3] == STRING)
    deep = 'def x = "${ { -> { -> { -> { -> { -> { -> { -> { -> "${1}" } } } } } } } } }"\n'
    check("ordinary braces do not consume the interpolation allowance", not lex(deep)[1])
    nested = '"${1}"'
    for _ in range(9):
        nested = '"${ ' + nested + ' }"'
    check("interpolation really nested past 8 is refused",
          any("nested more than 8" in x for x in lex(nested)[1]))

    # unicode escapes are decoded exactly as Groovy decodes them
    check("a unicode escape is decoded", decode_unicode_escapes("sh 'mvn\\u0020-B test'") == "sh 'mvn -B test'")
    check("multiple u's are allowed", decode_unicode_escapes("\\uu0041") == "A")
    check("an escaped backslash is not an escape", decode_unicode_escapes("a\\\\u0041b") == "a\\\\u0041b")

    for name, text, expect in [
        ("a dollar-slashy string", "def x = $/ /* /$\n", "dollar-slashy"),
        ("a slashy string", "def x = /a\\/*/\n", "slashy string or a division"),
        ("a division in code", "def y = a / b\n", "slashy string or a division"),
        ("an unterminated block comment", "a /* b\n", "unterminated /* block comment"),
        ("an unterminated string", "a 'b\n", "unterminated"),
        ("an unterminated interpolation", 'a "${ b\n', "unterminated"),
        ("a byte-order mark", "﻿pipeline { }\n", "byte-order mark"),
        ("a backslash-LF continuation", "def a = b \\\nc\n", "line continuation"),
        ("a backslash-CRLF continuation", "def a = b \\\r\nc\n", "line continuation"),
    ]:
        check("REFUSED (lexical): " + name, any(expect in x for x in lex(text)[1]))

    V = "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir ."
    good = ("pipeline {\n  stages {\n    stage('X') {\n      steps {\n        script {\n"
            "          timeout(time: 10, unit: 'MINUTES') {\n            sh '" + V + " --allow-ignored target'\n          }\n"
            "          sh 'mvn -B test'\n        }\n      }\n    }\n  }\n}\n")

    def verdict(t: str, perm: str = "PERMITTED_SHA", d: str = ".") -> tuple[bool, str]:
        return verified_commands(t, "mvn", mvn_classifier(), perm, d)

    check("the good shape is accepted", verdict(good)[0])
    check("a verification that exists only in comments is refused",
          not verdict(good.replace("          timeout(time: 10, unit: 'MINUTES') {\n            sh '" + V + " --allow-ignored target'\n          }\n",
                                   "          // timeout { sh 'x' }\n"))[0])
    for name, mutated in [
        ("a second, unverified copy", good.replace("          sh 'mvn -B test'\n",
                                                   "          sh 'mvn -B test'\n          writeFile file: 'pom.xml', text: 'x'\n          sh 'mvn -B test'\n")),
        ("a writer between them", good.replace("          sh 'mvn -B test'", "          writeFile file: 'pom.xml', text: 'x'\n          sh 'mvn -B test'")),
        ("a writer inside the verification timeout", good.replace(" --allow-ignored target'\n", " --allow-ignored target'\n            writeFile file: 'pom.xml', text: 'x'\n")),
        ("the command in a compound shell body", good.replace("sh 'mvn -B test'", "sh 'set -eu; mvn -B test'")),
        ("the verification's failure suppressed", good.replace(" --allow-ignored target'", " --allow-ignored target || true'")),
        ("a command appended to the verification", good.replace(" --allow-ignored target'", " --allow-ignored target; true'")),
        ("the verification pointed at another directory", good.replace("--dir . --allow-ignored", "--dir app-src --allow-ignored")),
        # round 5
        ("a brace-less `if` owning the verification", good.replace("          timeout(time: 10,", "          if (false)\n          timeout(time: 10,")),
        ("a brace-less `for` owning the verification", good.replace("          timeout(time: 10,", "          for (int i = 0; i < 0; i++)\n          timeout(time: 10,")),
        ("a label before the verification", good.replace("          timeout(time: 10,", "          skip:\n          timeout(time: 10,")),
        ("the same compile spelled with a unicode escape", good.replace("          sh 'mvn -B test'\n",
                                                                        "          sh 'mvn -B test'\n          sh 'mvn\\u0020-B test'\n")),
        ("the same compile spelled differently", good.replace("          sh 'mvn -B test'\n",
                                                              "          sh 'mvn -B test'\n          sh 'mvn -B -q test'\n")),
        ("a plain `mvn test` elsewhere in the file", good.replace("pipeline {", "def helper() { sh 'mvn test' }\npipeline {")),
        ("an mvn with an unresolvable argument", good.replace("          sh 'mvn -B test'\n",
                                                              "          sh 'mvn -B test'\n          sh 'mvn ${GOALS}'\n")),
        ("an mvn inside a larger shell body", good.replace("          sh 'mvn -B test'\n",
                                                           "          sh 'mvn -B test'\n          sh 'echo hi; mvn -B package'\n")),
    ]:
        check("REFUSED: " + name, not verdict(mutated)[0])
    check("REFUSED: the verification reading the wrong permission variable",
          not verdict(good.replace('${PERMITTED_SHA:-}', '${GIT_COMMIT:-}'))[0])
    check("REFUSED: the caller expecting a different permission variable", not verdict(good, perm="OTHER_SHA")[0])
    check("an mvn against another project's pom is not this assertion's business",
          verdict(good.replace("          sh 'mvn -B test'\n",
                               "          sh 'mvn -B test'\n          sh 'mvn -B -f .deps/other/pom.xml install'\n"))[0])
    check("a read-only plugin goal is not a guarded command",
          verdict(good.replace("          sh 'mvn -B test'\n",
                               "          sh 'mvn -B test'\n          sh 'mvn -q help:evaluate -Dexpression=x'\n"))[0])
    check("a brace-full `if` is fine",
          verdict(good.replace("        script {\n", "        script {\n          if (true) { echo 'x' }\n"))[0])
    check("a file with no guarded command at all is refused", not verdict(good.replace("          sh 'mvn -B test'\n", ""))[0])

    # the literal classifier, used for a deploy helper
    lit = good.replace("sh 'mvn -B test'", "sh 'bash scripts/deploy/service-deploy.sh'")
    check("the literal classifier accepts its verified command",
          verified_commands(lit, "scripts/deploy/service-deploy.sh",
                            literal_classifier("bash scripts/deploy/service-deploy.sh"), "PERMITTED_SHA", ".")[0])
    check("REFUSED: a second copy of the literal command",
          not verified_commands(lit.replace("          sh 'bash scripts/deploy/service-deploy.sh'\n",
                                            "          sh 'bash scripts/deploy/service-deploy.sh'\n          sh 'bash scripts/deploy/service-deploy.sh'\n"),
                                "scripts/deploy/service-deploy.sh",
                                literal_classifier("bash scripts/deploy/service-deploy.sh"), "PERMITTED_SHA", ".")[0])

    body = "{\n  one()\n  two(a,\n      b)\n  three { four() }\n  // five()\n  six()\n}\n"
    bm, _ = lex(body)
    texts = [code_text(body, bm, s, e) for s, e in statements(body, bm, 1, len(body) - 2)]
    check("statements split on newlines at depth 0", texts[:2] == ["one()", "two(a, b)"])
    check("a block statement ends at its closing brace", texts[2] == "three { four() }")
    check("a commented-out statement is not a statement", "five()" not in " ".join(texts))

    print("groovy-statements self-test: %d passed, %d failed" % (ok, fail))
    if fail == 0 and ok >= 48:
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
                src = decode_unicode_escapes(fh.read())
            mark, refusals = lex(src)
            if not refusals:
                refusals = control_flow_refusals(src, mark, sibling_lists(src, mark, 0, len(src)))
            print("%-60s %s" % (path, "ok" if not refusals else "REFUSED: " + "; ".join(refusals[:3])))
            bad += bool(refusals)
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
