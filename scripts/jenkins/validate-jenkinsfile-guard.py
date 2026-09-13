#!/usr/bin/env python3
"""Every Jenkins deploy job carries the permitted-commit guard, in canonical form, before its first effect.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge; each repository supplies a manifest classifying every Jenkinsfile* in its root:

    <Jenkinsfile> | in|out | options | reason
    options (semicolon-separated): before=<stage>,<stage>   stages allowed before the primary guard
                                   reguard=<stage>          the container stage whose first nested
                                                            stage must be the deploy-workspace re-guard
                                   contracts=<dir>          a second-source clone that must be bound

It is ALSO what scripts/jenkins/require-guarded-downstream.sh runs, with --only, on a downstream job's
Jenkinsfile fetched at the exact commit the caller forwards: the definition a child will load is judged
by the same rules before the child is triggered.

Deployment Permission Rule (options-edge rule.md): "Jenkins enforces the permitted commit, not the
assistant." A job without the guard is not available to the assistant; a Jenkinsfile nobody classified
fails here.

RULES for every IN-scope Jenkinsfile (textual, over comment-stripped, whitespace-collapsed text plus a
Groovy lexer that knows strings, comments and block nesting — control flow is matched by CANONICAL
FORM, so a guard written in a different but equivalent shape fails and must be brought back to the
form; that is deliberate):
  1. parameters: `string(name: 'PERMITTED_SHA', defaultValue: '', trim: true …)` and
     `string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '<sha256>' …)` whose default equals the
     sha256 of <root>/scripts/jenkins/permitted-sha-guard.sh (the guard refuses to run under any other).
  2. `disableRestartFromStage()`.
  3. The primary stage 'Permitted commit guard' is exactly:
       stage('Permitted commit guard') { [agent {…}] options { timeout(time: N, unit: 'MINUTES') } steps { script {
         def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh[ --ref "${X:?}"]')
         if (rc != 0) { error(…) }
         env.PERMITTED_SHA_GUARD = 'PASSED' } } }
     — no other statement, no shell operator in the script string, no catchError/try.
  4. Every stage before the primary guard is in the manifest's before= set, and no mutation token
     appears between `stages {` and it.
  5. A container stage with its own agent after the primary guard (`agent … stages {`) must open with
     the canonical stage 'Permitted commit guard (deploy workspace)' — rule 3's form, gated as in rule
     13, setting `env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'`.
  6. A steps-stage with its own agent (not `agent none`) after the primary guard that runs repository
     code (`scripts/`) or carries a mutation token must begin, after `steps {` and an optional
     `checkout scm`, with the canonical inline re-guard `script { timeout(time: N, unit: 'MINUTES') { def V =
     sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh') if (V != 0) { error(…) } } …`.
  7. Every `build job: '<job>'` forwards exactly one `string(name: 'PERMITTED_SHA', value: params.V)`
     (or `params.V.trim()` / `env.V`), and is protected by the canonical compatibility check FOR THAT
     JOB AND THAT SHA VARIABLE:
       def C = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh <job> "${V:?}"[ EXTRA…]')
       if (C != 0) { error(…) }
     EXECUTABLY, in one of two ways:
       (a) same stage, and the trigger lies INSIDE the very block the check is a direct statement of,
           after it — so any `return`, skipped branch, caught error or uncalled closure that bypasses the
           check leaves that block and bypasses the trigger as well. A check in an earlier sibling
           `script {}`/`dir {}` block, or under `if (false)`, `catchError`, `try`, a closure or a loop the
           trigger is not also inside, is refused;
       (b) the check is a top-level statement of its stage's `steps { script { … } }`, followed, as its
           very next statement, by `env.GUARDED_DOWNSTREAM_<JOB> = 'PASSED'` — the ONLY assignment of that
           flag anywhere in the file — in an earlier stage, and the trigger's stage `when` gate (rule 13)
           requires `env.GUARDED_DOWNSTREAM_<JOB> == 'PASSED'`: a skipped, caught or never-reached check
           leaves the flag unset and the trigger stage is skipped.
     A self-trigger (`build job: env.JOB_NAME`) needs only the forward. No annotation exempts a trigger.
  8. Every mutation token inside `post {}` lies inside an `if (…) {` block (or `else if`) whose whole
     condition is a conjunction of `env.<FLAG> == 'PASSED'` terms that includes the needed flag
     (DEPLOY_WORKSPACE_PERMITTED for files with a re-guard container, PERMITTED_SHA_GUARD otherwise).
     `!(…)`, `! (…)`, `(… ) == false`, `||`, `!=`, an else branch, a token after the block: refused.
  9. Every source acquisition after the primary guard: the ROOT workspace may not be re-acquired (except
     the leading `checkout scm` of a rule-6 stage). A NESTED source is acquired by a DEDICATED ACQUISITION STEP and
     bound by a DEDICATED GUARD STEP that is the very next statement of the same block — nothing runs between:
         sh <triple-quoted: [set -eu] [rm -rf <dir>]  git clone <url> <dir>  [git -C <dir> checkout main]>
           (or  dir('<dir>') { git url: <env.X | 'url'>, branch: 'main' } )
         timeout(time: N, unit: 'MINUTES') {
           sh 'PERMITTED_SHA="${X_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir <dir> --ref main'
         }
     The acquisition step holds nothing but those commands (no `;`, `&&`, `||`, other command or effect). The guard
     step is recognised from the Groovy token structure, never from text: the `sh` token is code and its sole argument
     is ONE single-quoted literal equal to the fixed template (only the permission variable, a literal directory and
     `--ref main` vary), directly inside a timeout block holding nothing else — the same text inside another string,
     a triple-quoted block, a GString or a comment is not the step. Both paths are RESOLVED through their enclosing
     dir('<literal>') blocks and must be equal; a dir() with a variable, `..`, `.`, an absolute or `~` path, and any
     ws()/node() wrapper, are refused; a second acquisition into an already guarded resolved path is refused. The
     guard step must run UNSKIPPABLY: every enclosing block below the stage's `steps` is a sequencing block
     (script/dir/withEnv/timeout/…) and no return/if/else/try/catch/catchError/loop/break/continue precedes it in any
     of them, so no later step or stage consuming the source can run without it.
 10. contracts=<dir>: that directory is acquired (and therefore, by rule 9, bound) at least once.
 11. Every mention of permitted-sha-guard.sh outside comments and parameters{} descriptions is one of the
     canonical forms (rule 3/5 guard stage, rule 6 inline re-guard, rule 9 dedicated step) — any other
     invocation, in any shell block or string, is refused. Every guard has an overall deadline: the guard
     stages' `options { timeout(time: N, unit: 'MINUTES') }`, and for inline re-guards and dedicated steps a
     `timeout(time: N, unit: 'MINUTES') { … }` block containing nothing but the guard (1 <= N <= 30).
 12. The guard script exists and parses (bash -n).
 13. EXECUTABLE GATES: every stage after the primary guard carries exactly
       when { [beforeAgent true] expression { [return] env.PERMITTED_SHA_GUARD == 'PASSED'[ && env.F == 'PASSED'…][ && (<its own condition>)] } }
     with DEPLOY_WORKSPACE_PERMITTED also required for every stage after the re-guard inside a re-guard
     container, and the GUARDED_DOWNSTREAM flag required by rule 7(b). The own condition is one
     parenthesised group (its parentheses matched, strings respected) — nothing can be or-ed around the
     flags. A refused guard therefore not only errors the build: every later stage is skipped by its
     own gate as well.
 14. The flags PERMITTED_SHA_GUARD, DEPLOY_WORKSPACE_PERMITTED and GUARDED_DOWNSTREAM_* are assigned only
     by their canonical statements and otherwise appear only as `env.<FLAG> == 'PASSED'` (or inside a
     shell ''' body, which cannot write Groovy's env) — never in parameters{}, environment{}, withEnv.
 15. Groovy string escapes: a backslash in any string literal must be a valid Groovy escape (\\\\ \\' \\"
     \\$ \\b \\t \\n \\f \\r \\uXXXX or a line continuation) — `\\.`, `\\d`, octal and `\\s` do not compile
     or do not reach the shell as written.

LIMITS — what this cannot prove: stage order is the order of `stage('…')` lines (no dynamic or parallel
stages are modelled — none exist in scope); mutation tokens are a fixed list, so a helper script called
before the guard is invisible unless its name is a token — the manifest's before= set is the reviewed
statement that those stages are effect-free; tokens in `//`/`#` comment lines and whole-line `echo '…'`
messages are ignored; nothing here executes a pipeline, compiles Declarative, or proves the controller runs THIS
file — require-guarded-downstream.sh establishes, for a child, that the job's SCM definition points at
this file at the forwarded commit.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import os
import re
import subprocess
import sys

GUARD_STAGE = "Permitted commit guard"
REGUARD_STAGE = "Permitted commit guard (deploy workspace)"
GUARD_SCRIPT_REL = "scripts/jenkins/permitted-sha-guard.sh"
FLAG = "PERMITTED_SHA_GUARD"
REFLAG = "DEPLOY_WORKSPACE_PERMITTED"
DOWNSTREAM_FLAG_PREFIX = "GUARDED_DOWNSTREAM_"
STR = r"(?:\"[^\"]*\"|'[^']*')"
STAGE_RE = re.compile(r"^\s*stage\(\s*'((?:[^'\\]|\\.)*)'")
BUILD_JOB_RE = re.compile(r"\bbuild\s*\(?\s*job:\s*(env\.JOB_NAME|'([^']+)')")
ACQUIRE_RE = re.compile(r"\bgit url:|\bgit clone\b|\bcheckout\(|\bcheckout scm\b|\bgit pull\b|\bgit checkout\b|\bgit -C \S+ checkout\b")
GATE_FLAG_ONLY = "when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } } "
CANON_GUARD = re.compile(
    r"^stage\('(?P<name>[^']+)'\) \{ (?P<when>when \{ expression \{ env\.PERMITTED_SHA_GUARD == 'PASSED' \} \} )?(?:agent \{ label [^}]+ \} )?"
    r"options \{ timeout\(time: (?P<tmo>[0-9]+), unit: 'MINUTES'\) \} steps \{ script \{ "
    r"def rc = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh(?P<args>( --ref \"\$\{[A-Z_]+:\?\}\")?)'\) "
    r"if \(rc != 0\) \{ error\(" + STR + r"\) \} "
    r"env\.(?P<flag>PERMITTED_SHA_GUARD|DEPLOY_WORKSPACE_PERMITTED) = 'PASSED' \} \} \}$"
)
INLINE_REGUARD = re.compile(
    r"^(?:checkout scm )?script \{ timeout\(time: (?P<tmo>[0-9]+), unit: 'MINUTES'\) \{ def (?P<v>\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh'\) "
    r"if \((?P=v) != 0\) \{ error\(" + STR + r"\) \} \}"
)
COMPAT_FORM = re.compile(
    r"^def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream\.sh (?P<job>[A-Za-z0-9_.-]+) "
    r"\"\$\{(?P<shavar>[A-Z][A-Z0-9_]*):\?\}\"(?: [A-Z][A-Z0-9_]*)*'\) "
    r"if \(\1 != 0\) \{ error\(" + STR + r"\) \}(?P<flag> env\.(?P<flagname>[A-Z][A-Z0-9_]*) = 'PASSED')?"
)
FORWARD_RE = re.compile(r"string\(\s*name:\s*'PERMITTED_SHA',\s*value:\s*(?P<v>(?:params|env)\.[A-Za-z0-9_]+(?:\.trim\(\))?|[^,)\]]*)\s*\)")
FORWARD_VALUE = re.compile(r"^(?:params|env)\.(?P<var>[A-Z][A-Z0-9_]*)(?:\.trim\(\))?$")
# The ONLY accepted guard for a nested (second-source) checkout: a DEDICATED `sh` step whose script is exactly one
# guard command. Nothing else can sit in that shell — no other command, trap, exit, function, subshell,
# substitution, separator, redirection or comment — so the step fails if and only if the guard refuses, and every
# shape that could turn a refusal into success (exit 0 first, an EXIT trap, `|| true`, backticks, a subshell,
# returnStatus/returnStdout) is simply not the template. The only variable parts: the source's own permission
# variable, a literal directory, `--ref main`.
DEDICATED_GUARD = re.compile(
    r"^sh '(?P<cmd>PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*)_PERMITTED_SHA:[-?]\}\" "
    r"bash scripts/jenkins/permitted-sha-guard\.sh --dir (?P<dir>[A-Za-z0-9._][A-Za-z0-9._/-]*) --ref main)'$"
)
GUARD_INVOCATION = re.compile(r"permitted-sha-guard\.sh")   # any mention outside comments and parameters{} descriptions
MUTATION_TOKENS = [
    (r"\bkubectl\b", "kubectl"),
    (r"\bdocker\s+(build|buildx|push|run|compose|rm|update)\b", "docker build/push/run/rm"),
    (r"\bbuild\s*\(?\s*job:", "build job: (downstream trigger)"),
    (r"\brsync\b", "rsync"),
    (r"\bscp\b", "scp"),
    (r"\bssh\b", "ssh"),
    (r"\blaunchctl\b", "launchctl"),
    (r"\bkafka-(topics|configs)\b", "kafka-topics/kafka-configs"),
    (r"\bcrontab\b", "crontab"),
    (r"\bhelm\b", "helm"),
    (r"\bansible-playbook\b", "ansible-playbook"),
    (r"\bmvn\b", "mvn"),
    (r"\bgit\s+push\b", "git push"),
    (r"vix-unpause\.sh", "vix-unpause.sh (scales a deployment)"),
    (r"post-deploy-recovery\.sh", "post-deploy-recovery.sh (may scale a deployment)"),
    (r"\bnohup\b", "nohup (starts a process)"),
    (r"(?<![\w-])(kill|pkill|killall)(?![\w-])", "kill/pkill/killall (stops a process)"),
]
MUTATION_RES = [(re.compile(p), n) for p, n in MUTATION_TOKENS]
TRANSPARENT = re.compile(
    r"^(?:script|steps|stages|stage\s*\(.*\)|dir\s*\(.*\)|withEnv\s*\(.*\)|withCredentials\s*\(.*\)|timeout\s*\(.*\)|node(?:\s*\(.*\))?|ws\s*\(.*\))$",
    re.S,
)
BS = "\\"


# ----------------------------------------------------------------------------------------- lexing
class Block:
    __slots__ = ("open", "close", "header", "parent", "kids")

    def __init__(self, open_: int, header: str, parent: int | None):
        self.open, self.close, self.header, self.parent, self.kids = open_, -1, header, parent, []


class Groovy:
    """A small Groovy lexer: which characters are code, where each `{…}` block opens and closes, the
    statement text that heads it, and every string literal (for the escape lint)."""

    def __init__(self, text: str):
        self.text = text
        n = len(text)
        self.blocks: list[Block] = []
        self.strings: list[tuple[str, int, int]] = []   # (quote, body_start, body_end)
        self.comments: list[tuple[int, int]] = []       # /* … */ spans
        stack: list[int] = []
        stmt = 0
        paren = 0
        i = 0
        while i < n:
            c = text[i]
            if text.startswith("'''", i) or text.startswith('"""', i):
                q = text[i:i + 3]
                k = i + 3
                while k < n and not text.startswith(q, k):
                    k += 2 if text[k] == BS else 1
                self.strings.append((q, i + 3, min(k, n)))
                i = k + 3
                continue
            if text.startswith("//", i):
                k = text.find("\n", i)
                self.comments.append((i, n if k < 0 else k))
                i = n if k < 0 else k
                continue
            if text.startswith("/*", i):
                k = text.find("*/", i + 2)
                self.comments.append((i, n if k < 0 else k + 2))
                i = n if k < 0 else k + 2
                continue
            if c in "'\"":
                k = i + 1
                while k < n and text[k] != c and text[k] != "\n":
                    k += 2 if text[k] == BS else 1
                self.strings.append((c, i + 1, min(k, n)))
                i = k + 1
                continue
            if c in "([":
                paren += 1
            elif c in ")]":
                paren = max(0, paren - 1)
            elif c == "{":
                header = re.sub(r"\s+", " ", text[stmt:i]).strip()
                b = Block(i, header, stack[-1] if stack else None)
                if stack:
                    self.blocks[stack[-1]].kids.append(len(self.blocks))
                self.blocks.append(b)
                stack.append(len(self.blocks) - 1)
                stmt = i + 1
                paren = 0
            elif c == "}":
                if stack:
                    self.blocks[stack.pop()].close = i
                stmt = i + 1
                paren = 0
            elif (c == ";" or c == "\n") and paren == 0:
                stmt = i + 1
            i += 1
        for b in self.blocks:
            if b.close < 0:
                b.close = n

    def innermost(self, pos: int) -> int | None:
        best = None
        for idx, b in enumerate(self.blocks):
            if b.open < pos < b.close and (best is None or b.open > self.blocks[best].open):
                best = idx
        return best

    def ancestors(self, pos: int) -> list[int]:
        out = []
        idx = self.innermost(pos)
        while idx is not None:
            out.append(idx)
            idx = self.blocks[idx].parent
        return list(reversed(out))

    def control_chain(self, pos: int) -> tuple[int, ...]:
        """The enclosing blocks that decide WHETHER code runs (if/else/try/catchError/closures/loops…),
        outermost first. script/steps/stage/dir/withEnv/… only sequence code and are left out."""
        return tuple(i for i in self.ancestors(pos) if not TRANSPARENT.match(self.blocks[i].header))

    def stage_of(self, pos: int) -> int | None:
        for i in reversed(self.ancestors(pos)):
            if self.blocks[i].header.startswith("stage("):
                return i
        return None

    def is_code(self, pos: int) -> bool:
        """True when pos is executable Groovy: not inside any string literal (quotes included) and not in a comment."""
        if any(st - len(q) <= pos < en + len(q) for q, st, en in self.strings):
            return False
        return not any(st <= pos < en for st, en in self.comments)

    def sh_single_quoted_step(self, line_start: int, line_end: int) -> str | None:
        """When the physical line [line_start, line_end) is exactly the Groovy STATEMENT `sh '<one-line literal>'`
        — the `sh` token is code, its sole argument is ONE single-quoted string literal that closes on this line, and
        nothing but whitespace follows — the literal's content; otherwise None. Text inside another string, a
        triple-quoted block, a GString or a comment never qualifies."""
        seg = self.text[line_start:line_end]
        pos = line_start + len(seg) - len(seg.lstrip())
        if not seg.lstrip().startswith("sh '") or not self.is_code(pos):
            return None
        lit = next(((st, en) for q, st, en in self.strings if q == "'" and st == pos + 4), None)
        if lit is None or self.text[lit[1]:lit[1] + 1] != "'" or self.text[lit[1] + 1:line_end].strip():
            return None
        return self.text[lit[0]:lit[1]]

    def in_block_comment(self, pos: int) -> bool:
        return any(s <= pos < e for s, e in self.comments)

    def code_only(self, a: int, b: int) -> str:
        """text[a:b] with every string literal (quotes included) and comment blanked."""
        chars = list(self.text[a:b])
        spans = [(st - len(q), en + len(q)) for q, st, en in self.strings] + list(self.comments)
        for st, en in spans:
            for k in range(max(a, st), min(b, en)):
                chars[k - a] = " "
        return re.sub(r"//[^\n]*", " ", "".join(chars))

    def call_args(self, quote_start: int) -> str | None:
        """When the string literal whose opening quote is at quote_start is an argument of `sh(...)`, the code of
        the WHOLE parenthesised argument list with strings blanked — arguments AFTER the string included."""
        head = self.text[max(0, quote_start - 300):quote_start]
        m = re.search(r"\bsh\s*\((?P<pre>[^()]*)$", head)
        if not m:
            return None
        open_at = quote_start - len(head) + head.index("(", m.start())
        code = self.code_only(open_at, min(len(self.text), open_at + 50000))
        depth = 0
        for k, c in enumerate(code):
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return code[:k + 1]
        return None

    def in_triple_string(self, pos: int) -> bool:
        return any(q in ("'''", '"""') and s <= pos < e for q, s, e in self.strings)


def bad_escapes(body: str) -> list[tuple[int, str, str]]:
    """Escapes Groovy refuses to compile, or compiles to something else than the shell is shown."""
    out = []
    i, n = 0, len(body)
    while i < n:
        if body[i] != BS:
            i += 1
            continue
        nxt = body[i + 1] if i + 1 < n else ""
        if nxt == BS or nxt in ("\r", "\n") or nxt in "btnfr\"'$":
            i += 2
        elif nxt == "u":
            j = i + 1
            while j < n and body[j] == "u":
                j += 1
            if j + 4 <= n and all(ch in "0123456789abcdefABCDEF" for ch in body[j:j + 4]):
                i = j + 4
            else:
                out.append((i, body[i:j + 4], "malformed unicode escape — does not compile"))
                i = j
        elif nxt in "01234567":
            out.append((i, body[i:i + 2], "compiles as an OCTAL escape — the shell receives a control character"))
            i += 2
        elif nxt == "s":
            out.append((i, body[i:i + 2], "compiles to a SPACE — the shell never sees \\s"))
            i += 2
        elif nxt == "":
            out.append((i, BS, "a backslash at the end of a string does not compile"))
            i += 1
        else:
            out.append((i, body[i:i + 2], "not a Groovy escape — does not compile"))
            i += 2
    return out


# ----------------------------------------------------------------------------------------- helpers
def is_comment(line: str) -> bool:
    s = line.strip()
    return s.startswith("//") or s.startswith("#") or s.startswith("*") or s.startswith("/*")


def is_message(line: str) -> bool:
    s = line.strip()
    return s.startswith("echo ") and len(s) > 6 and s[5] in "'\"" and s.endswith(s[5]) and s.count(s[5]) == 2


def token_at(line: str) -> str | None:
    if is_comment(line) or is_message(line):
        return None
    for rx, name in MUTATION_RES:
        if rx.search(line):
            return name
    return None


def block_end(lines: list[str], start: int) -> int:
    depth = 0
    seen = False
    for i in range(start, len(lines)):
        for ch in lines[i]:
            if ch == "{":
                depth += 1
                seen = True
            elif ch == "}":
                depth -= 1
                if seen and depth == 0:
                    return i
    return len(lines) - 1


def find_block(lines: list[str], header_re: re.Pattern, start: int = 0) -> tuple[int, int] | None:
    for i in range(start, len(lines)):
        if header_re.match(lines[i]) and not is_comment(lines[i]):
            return i, block_end(lines, i)
    return None


def stage_lines(lines: list[str]) -> list[tuple[int, str]]:
    return [(i, m.group(1)) for i, l in enumerate(lines) if not is_comment(l) for m in [STAGE_RE.match(l)] if m]


def canon(lines: list[str]) -> str:
    kept = [l.strip() for l in lines if l.strip() and not is_comment(l)]
    return re.sub(r"\s+", " ", " ".join(kept)).strip()


def matched_close(s: str, open_at: int) -> int | None:
    """Index of the `)` matching s[open_at] == '(' , skipping quoted strings; None if unbalanced."""
    depth = 0
    i = open_at
    while i < len(s):
        c = s[i]
        if c in "'\"":
            k = i + 1
            while k < len(s) and s[k] != c:
                k += 2 if s[k] == BS else 1
            i = k + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


GATE_TERM = re.compile(r"env\.([A-Z][A-Z0-9_]*) == 'PASSED'")


def parse_gate(when_canon: str) -> tuple[set[str], str] | None:
    """`when { [beforeAgent true] expression { [return] env.A == 'PASSED'[ && env.B == 'PASSED'…][ && (rest)] } }`
    -> ({A, B…}, rest). None if the block is not exactly that shape."""
    m = re.match(r"^when \{ (?:beforeAgent true )?expression \{ (?:return )?(?P<cond>.*) \} \}$", when_canon)
    if not m:
        return None
    cond = m.group("cond")
    flags: set[str] = set()
    pos = 0
    while True:
        tm = GATE_TERM.match(cond, pos)
        if not tm:
            return None
        flags.add(tm.group(1))
        pos = tm.end()
        if pos == len(cond):
            return flags, ""
        if not cond.startswith(" && ", pos):
            return None
        pos += 4
        if cond.startswith("(", pos):
            close = matched_close(cond, pos)
            if close is None or close != len(cond) - 1 or not cond[pos + 1:close].strip():
                return None
            return flags, cond[pos + 1:close].strip()


def parse_manifest(path: str) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    with open(path, encoding="utf-8") as fh:
        for n, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != 4:
                raise SystemExit(f"{path}:{n}: expected 4 pipe-separated fields, got {len(parts)}")
            name, scope, opts, reason = parts
            if scope not in ("in", "out"):
                raise SystemExit(f"{path}:{n}: scope must be in|out, got '{scope}'")
            if name in entries:
                raise SystemExit(f"{path}:{n}: {name} listed twice")
            e = {"scope": scope, "before": set(), "reguard": "", "contracts": "", "reason": reason}
            for opt in [o for o in opts.split(";") if o.strip()]:
                k, _, v = opt.partition("=")
                k, v = k.strip(), v.strip()
                if k == "before":
                    e["before"].update(s.strip() for s in v.split(",") if s.strip())
                elif k in ("reguard", "contracts"):
                    e[k] = v
                else:
                    raise SystemExit(f"{path}:{n}: unknown option '{k}' (before=, reguard=, contracts=)")
            if not reason:
                raise SystemExit(f"{path}:{n}: {name} needs a reason")
            entries[name] = e
    return entries


def stage_range(lines: list[str], stages: list[tuple[int, str]], idx: int) -> tuple[int, int]:
    start = stages[idx][0]
    return start, block_end(lines, start) + 1


def check_guard_stage(lines: list[str], lo: int, hi: int, label: str, flag: str, ref_allowed: bool, gated: bool) -> list[str]:
    text = canon(lines[lo:hi])
    m = CANON_GUARD.match(text)
    if not m:
        return [f"'{label}' stage is not the canonical guard: {text[:160]}"]
    if not 1 <= int(m.group("tmo")) <= 30:
        return [f"'{label}' stage's options {{ timeout(time: N, unit: 'MINUTES') }} must have 1 <= N <= 30"]
    if m.group("flag") != flag:
        return [f"'{label}' stage must set env.{flag} = 'PASSED' (it sets {m.group('flag')})"]
    if m.group("args") and not ref_allowed:
        return [f"'{label}' stage may not pass --ref (that is the primary guard's business)"]
    if gated and not m.group("when"):
        return [f"'{label}' stage must open with {GATE_FLAG_ONLY.strip()}"]
    if not gated and m.group("when"):
        return [f"'{label}' stage is the first guard and cannot be gated on its own flag"]
    return []


def enclosing_dir(lines: list[str], i: int, lo: int) -> str | None:
    for j in range(i - 1, lo - 1, -1):
        m = re.search(r"\bdir\('([^']+)'\)\s*\{", lines[j])
        if m and not is_comment(lines[j]) and block_end(lines, j) >= i:
            return m.group(1)
    return None


def acquisition_dir(lines: list[str], i: int, lo: int) -> str:
    l = lines[i]
    m = re.search(r"\bgit clone\b[^\n]*?\s(\S+)\s*$", l.rstrip("\\ "))
    if m and not m.group(1).startswith("-"):
        cand = m.group(1)
        if not re.search(r"://|@", cand):
            return cand
    m = re.search(r"\bgit -C (\S+) checkout\b", l)
    if m:
        return m.group(1).strip("\"'")
    m = re.search(r"\bdir\('([^']+)'\)", l)
    if m:
        return m.group(1)
    d = enclosing_dir(lines, i, lo)
    return d if d else "."


def is_prefix(a: tuple, b: tuple) -> bool:
    return len(a) <= len(b) and b[:len(a)] == a


def downstream_flag(job: str) -> str:
    return DOWNSTREAM_FLAG_PREFIX + re.sub(r"[^A-Za-z0-9]", "_", job).upper()


# ----------------------------------------------------------------------------------------- the rules
def check_in_scope(path: str, entry: dict, guard_hash: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    lines = text.split("\n")
    offs = [0]
    for l in lines:
        offs.append(offs[-1] + len(l) + 1)
    g = Groovy(text)
    problems: list[str] = []

    def pos_of(line_idx: int, col: int = 0) -> int:
        return offs[line_idx] + col

    def first_code_col(line_idx: int) -> int:
        return len(lines[line_idx]) - len(lines[line_idx].lstrip())

    def dedicated_at(line_idx: int):
        """The DEDICATED_GUARD match for line line_idx only when that line is a real `sh '…'` Groovy statement."""
        content = g.sh_single_quoted_step(offs[line_idx], offs[line_idx] + len(lines[line_idx]))
        if content is None:
            return None
        return DEDICATED_GUARD.match("sh '" + content + "'")

    def stage_block(line_idx: int) -> int | None:
        lo_off, hi_off = offs[line_idx], offs[line_idx + 1]
        for k, b in enumerate(g.blocks):
            if lo_off <= b.open < hi_off and b.header.startswith("stage("):
                return k
        return None

    # 15. escapes
    for q, s, e in g.strings:
        for off, esc, why in bad_escapes(text[s:e]):
            ln = text.count("\n", 0, s + off) + 1
            problems.append(f"line {ln}: {esc!r} in a Groovy string: {why}; write the backslash doubled")

    # 1. parameters
    pb = find_block(lines, re.compile(r"^\s*parameters\s*\{"))
    if pb is None:
        problems.append("no parameters {} block")
    else:
        lo, hi = pb
        ptext = canon(lines[lo:hi + 1])
        if not re.search(r"string\(name: 'PERMITTED_SHA', defaultValue: '', trim: true,", ptext):
            problems.append("parameters {} lacks string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, …)")
        m = re.search(r"string\(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '([0-9a-f]*)'", ptext)
        if not m:
            problems.append("parameters {} lacks string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '<sha256>' …)")
        elif m.group(1) != guard_hash:
            problems.append(f"PERMITTED_SHA_GUARD_VERSION default is {m.group(1) or '(empty)'} but scripts/jenkins/permitted-sha-guard.sh hashes to {guard_hash} — the job would declare a guard it does not run")

    # 2. restart from stage
    if not any("disableRestartFromStage()" in l and not is_comment(l) for l in lines):
        problems.append("options {} lacks disableRestartFromStage()")

    # 3./4. the primary guard and what precedes it
    stages = [(i, n) for i, n in stage_lines(lines) if not g.in_block_comment(pos_of(i, first_code_col(i)))]
    names = [n for _, n in stages]
    if GUARD_STAGE not in names:
        problems.append(f"no stage named '{GUARD_STAGE}'")
        return problems
    gi = names.index(GUARD_STAGE)
    for i in range(gi):
        if names[i] not in entry["before"]:
            problems.append(f"stage '{names[i]}' precedes the guard but is not in the manifest's before= set")
    sb = find_block(lines, re.compile(r"^\s*stages\s*\{"))
    if sb is None:
        problems.append("no stages {} block")
    else:
        for i in range(sb[0], stages[gi][0]):
            t = token_at(lines[i])
            if t:
                problems.append(f"mutation token before the guard: line {i + 1}: {t}: {lines[i].strip()[:90]}")
    g_lo, g_hi = stage_range(lines, stages, gi)
    problems += check_guard_stage(lines, g_lo, g_hi, GUARD_STAGE, FLAG, ref_allowed=True, gated=False)

    # 5./6. stages after the guard that own an agent
    reguard_ranges: list[tuple[int, int]] = []
    container_ranges: list[tuple[int, int, int]] = []   # (reguard_hi, container_lo, container_hi)
    has_container = False
    for si in range(gi + 1, len(stages)):
        lo, hi = stage_range(lines, stages, si)
        body = lines[lo:hi]
        sblk = stage_block(lo)
        direct = [g.blocks[k].header for k in (g.blocks[sblk].kids if sblk is not None else [])]
        direct_text = ""
        if sblk is not None:
            # the stage's own directive text: its body with every nested block removed
            b = g.blocks[sblk]
            cut, last = [], b.open + 1
            for k in b.kids:
                cut.append(text[last:g.blocks[k].open])
                last = g.blocks[k].close + 1
            cut.append(text[last:b.close])
            direct_text = " ".join(cut)
        own_agent = any(h == "agent" for h in direct) or bool(re.search(r"(?m)^\s*agent\s+(any|none)\b", direct_text))
        if not own_agent:
            continue
        if re.search(r"(?m)^\s*agent\s+none\b", direct_text):
            continue   # a flyweight stage: no workspace, no repository code; its build job: is judged by rule 7
        ctext = canon(body)
        if "stages" in direct:
            has_container = True
            cont = names[si]
            if entry["reguard"] and entry["reguard"] != cont:
                problems.append(f"container stage '{cont}' owns an agent but the manifest names '{entry['reguard']}' as the re-guard container")
            if si + 1 >= len(names) or names[si + 1] != REGUARD_STAGE:
                problems.append(f"container stage '{cont}' must open with '{REGUARD_STAGE}'")
            else:
                rlo, rhi = stage_range(lines, stages, si + 1)
                problems += check_guard_stage(lines, rlo, rhi, REGUARD_STAGE, REFLAG, ref_allowed=False, gated=True)
                reguard_ranges.append((rlo, rhi))
                container_ranges.append((rhi, lo, hi))
            continue
        needs = any(token_at(l) for l in body) or any("scripts/" in l and not is_comment(l) for l in body)
        if not needs:
            continue
        after_steps = ctext.split(" steps { ", 1)
        if len(after_steps) != 2 or not INLINE_REGUARD.match(after_steps[1]):
            problems.append(f"stage '{names[si]}' runs on its own agent after the guard but does not open with the canonical inline re-guard (steps {{ [checkout scm] script {{ timeout(time: N, unit: 'MINUTES') {{ def V = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh') if (V != 0) {{ error(…) }} }} …)")
    if entry["reguard"] and not has_container:
        problems.append(f"manifest names reguard={entry['reguard']} but no container stage with its own agent follows the guard")

    # 14. flags: assigned canonically, otherwise only compared
    flag_re = re.compile(rf"\b(?:env\.)?({FLAG}|{REFLAG}|{DOWNSTREAM_FLAG_PREFIX}[A-Z0-9_]+)\b")
    compat_flag_lines: dict[str, list[int]] = {}
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        for fm in flag_re.finditer(l):
            p = pos_of(i, fm.start())
            if g.in_triple_string(p):
                continue
            name = fm.group(1)
            rest = l[fm.start():]
            if re.match(rf"env\.{name} == 'PASSED'", rest):
                continue
            if re.match(rf"env\.{name}\s*=(?!=)", rest):
                canonical = re.match(rf"env\.{name} = 'PASSED'\s*$", rest) is not None
                if canonical and name == FLAG and g_lo <= i < g_hi:
                    continue
                if canonical and name == REFLAG and any(lo <= i < hi for lo, hi in reguard_ranges):
                    continue
                if canonical and name.startswith(DOWNSTREAM_FLAG_PREFIX):
                    compat_flag_lines.setdefault(name, []).append(i)
                    continue
                where = f"the '{GUARD_STAGE}' stage" if name == FLAG else (f"the '{REGUARD_STAGE}' stage" if name == REFLAG else "its compatibility check")
                problems.append(f"line {i + 1}: env.{name} may be set only by {where}")
                continue
            problems.append(f"line {i + 1}: {name} may appear only as `env.{name} == 'PASSED'` or in its canonical assignment: {l.strip()[:90]}")

    # 13. executable gates on every stage after the primary guard
    def required_flags(si: int) -> set[str]:
        req = {FLAG}
        s_line = stages[si][0]
        for rhi, clo, chi in container_ranges:
            if rhi <= s_line < chi:
                req.add(REFLAG)
        return req

    gates: dict[int, set[str]] = {}
    for si in range(gi + 1, len(stages)):
        lo, hi = stage_range(lines, stages, si)
        if names[si] == REGUARD_STAGE:
            gates[si] = {FLAG}
            continue
        sblk = stage_block(lo)
        whens = [k for k in (g.blocks[sblk].kids if sblk is not None else []) if g.blocks[k].header == "when"]
        if len(whens) != 1:
            problems.append(f"stage '{names[si]}' after the guard has no `when` gate: every such stage must carry when {{ expression {{ env.PERMITTED_SHA_GUARD == 'PASSED'[ && …] }} }}")
            gates[si] = set()
            continue
        wb = g.blocks[whens[0]]
        wtext = re.sub(r"\s+", " ", "when " + "\n".join(x for x in text[wb.open:wb.close + 1].split("\n") if not is_comment(x))).strip()
        parsed = parse_gate(wtext)
        if parsed is None:
            problems.append(f"stage '{names[si]}': its `when` is not the canonical gate (env.{FLAG} == 'PASSED'[ && env.F == 'PASSED'…][ && (condition)] — nothing may be or-ed or negated around the flags): {wtext[:160]}")
            gates[si] = set()
            continue
        gates[si] = parsed[0]
        missing = required_flags(si) - parsed[0]
        if missing:
            problems.append(f"stage '{names[si]}': its `when` gate must require {', '.join('env.' + f + ' == ' + repr('PASSED') for f in sorted(missing))}")

    # 7. downstream triggers
    compat_sites = []   # (line, job, shavar, flag_line, innermost block, stage block, top-level-of-script, flag name)
    for i, l in enumerate(lines):
        if is_comment(l) or "require-guarded-downstream.sh" not in l or not l.lstrip().startswith("def ") or not g.is_code(pos_of(i, first_code_col(i))):
            continue
        cm = COMPAT_FORM.match(canon(lines[i:i + 12]))
        if not cm:
            continue
        p = pos_of(i, first_code_col(i))
        inner = g.innermost(p)
        flag_line = None
        if cm.group("flagname"):
            # the ONE line the flag assignment may sit on: the statement that completes this very match
            for j in range(i + 1, min(i + 12, len(lines))):
                if lines[j].strip() == f"env.{cm.group('flagname')} = 'PASSED'":
                    fm = COMPAT_FORM.fullmatch(canon(lines[i:j + 1]))
                    if fm and fm.group("flagname") == cm.group("flagname"):
                        flag_line = j
                    break
        top = (inner is not None and g.blocks[inner].header == "script" and g.blocks[inner].parent is not None
               and g.blocks[g.blocks[inner].parent].header == "steps")
        compat_sites.append((i, cm.group("job"), cm.group("shavar"), flag_line, inner, g.stage_of(p), top, cm.group("flagname")))
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        m = BUILD_JOB_RE.search(l)
        if not m:
            continue
        call_lines = [lines[i]]
        for x in lines[i + 1: i + 60]:
            if BUILD_JOB_RE.search(x) or STAGE_RE.match(x):
                break
            call_lines.append(x)
        fwd = FORWARD_RE.findall("\n".join(x for x in call_lines if not is_comment(x)))
        if len(fwd) != 1:
            problems.append(f"line {i + 1}: build job: does not forward PERMITTED_SHA exactly once")
            continue
        fv = FORWARD_VALUE.match(fwd[0].strip())
        if m.group(1) == "env.JOB_NAME":
            if not fv or fv.group("var") != "PERMITTED_SHA":
                problems.append(f"line {i + 1}: a self-trigger must forward its own params.PERMITTED_SHA")
            continue
        job = m.group(2)
        if not fv:
            problems.append(f"line {i + 1}: build job: '{job}' forwards PERMITTED_SHA from '{fwd[0].strip()}' — forward params.<V> (or params.<V>.trim() / env.<V>), the same variable its compatibility check judged")
            continue
        var = fv.group("var")
        tp = pos_of(i, m.start())
        tstage = g.stage_of(tp)
        tanc = g.ancestors(tp)
        tsi = max(k for k, (s, _) in enumerate(stages) if s <= i)
        ok = False
        want = downstream_flag(job)
        for (ci, cjob, cvar, cflag_line, cinner, cstage, ctop, cflag) in compat_sites:
            if cjob != job or cvar != var:
                continue
            # (a) the trigger lies INSIDE the very block the check is a direct statement of, after it: a
            #     `return`, a skipped branch, a caught error or a closure that bypasses the check has to
            #     leave that block, and so bypasses the trigger too. A check in an earlier sibling
            #     script/dir/withEnv block, or under if/catchError/try, is refused.
            if cstage == tstage and ci < i and cinner is not None and cinner in tanc:
                ok = True
                break
            # (b) the check is a top-level statement of its stage's `steps { script { … } }`, the flag
            #     assignment is the statement completing it, it is the ONLY assignment of that flag in the
            #     whole file, and the trigger's stage gate requires the flag.
            csi = max(k for k, (s, _) in enumerate(stages) if s <= ci)
            if (cflag == want and ctop and cflag_line is not None and csi < tsi and want in gates.get(tsi, set())
                    and compat_flag_lines.get(want, []) == [cflag_line]):
                ok = True
                break
        if not ok:
            problems.append(
                f"line {i + 1}: build job: '{job}' is not executably protected by the canonical compatibility check for {job} with \"${{{var}:?}}\": "
                f"(a) earlier in the very block the check is a direct statement of (not an earlier sibling script/dir block, not under if/catchError/try/closure), or (b) followed by env.{downstream_flag(job)} = 'PASSED' "
                f"with the trigger's stage gated on env.{downstream_flag(job)} == 'PASSED'")
    for name, fls in compat_flag_lines.items():
        canonical_lines = {x[3] for x in compat_sites if x[7] == name and x[3] is not None and x[6]}
        if len(fls) != 1:
            problems.append(f"env.{name} is assigned {len(fls)} times (lines {', '.join(str(f + 1) for f in fls)}): a downstream flag has exactly ONE assignment, the statement completing its compatibility check")
        for fl in fls:
            if fl not in canonical_lines:
                problems.append(f"line {fl + 1}: env.{name} may be set only as the statement right after its compatibility check, at the top level of the stage's script block")
    if any("UNBOUND-DOWNSTREAM" in l for l in lines):
        problems.append("an UNBOUND-DOWNSTREAM annotation is not a gate: refuse the path or bind it")

    # 8. post{} structural gating
    flag_needed = REFLAG if has_container else FLAG
    seen_posts: set[int] = set()
    start = 0
    while True:
        pb2 = find_block(lines, re.compile(r"^\s*post\s*\{"), start)
        if pb2 is None:
            break
        lo, hi = pb2
        start = lo + 1
        if lo in seen_posts:
            continue
        seen_posts.add(lo)
        for ln in range(lo, hi + 1):
            l = lines[ln]
            if is_comment(l) or is_message(l):
                continue
            for rx, name in MUTATION_RES:
                for tm in rx.finditer(l):
                    p = pos_of(ln, tm.start())
                    gated = False
                    for bi in g.ancestors(p):
                        hm = re.match(r"^(?:else )?if \((?P<c>.*)\)$", g.blocks[bi].header)
                        if not hm or g.blocks[bi].open < offs[lo]:
                            continue
                        cond = hm.group("c").strip()
                        if re.fullmatch(r"env\.[A-Z][A-Z0-9_]* == 'PASSED'(?: && env\.[A-Z][A-Z0-9_]* == 'PASSED')*", cond) and f"env.{flag_needed} == 'PASSED'" in cond:
                            gated = True
                            break
                    if not gated:
                        problems.append(f"post {{}} line {ln + 1}: {name} is not inside the true branch of an `if (env.{flag_needed} == 'PASSED'[ && env.F == 'PASSED'…])` gate")

    # 9./10. source acquisitions after the guard
    shell_blocks = [(s, e) for q, s, e in g.strings if q == "'''"]

    def unskippable(pos: int) -> str | None:
        """None when the statement at pos runs whenever its stage's steps run: every enclosing block below the
        stage's `steps` is a sequencing block (script/dir/withEnv/…), and no return/if/try/catchError/loop/
        break/continue precedes it inside any of them — so no later sibling step or stage can run without it.
        Otherwise why not."""
        anc = g.ancestors(pos)
        steps_idx = next((k for k in range(len(anc) - 1, -1, -1) if g.blocks[anc[k]].header == "steps"), None)
        if steps_idx is None:
            return "it is not inside a stage's steps"
        chain = anc[steps_idx:]
        for n, b in enumerate(chain):
            blk_ = g.blocks[b]
            if n > 0 and not TRANSPARENT.match(blk_.header):
                return f"it sits under `{blk_.header[:40]}`"
            upto = g.blocks[chain[n + 1]].open if n + 1 < len(chain) else pos
            code = list(g.code_only(blk_.open + 1, upto))
            # a completed sequencing closure before it (an earlier `script {}` / `dir {}` step) cannot skip it: a
            # `return` in there leaves only that closure. A completed if/try block CAN (its return leaves this one).
            for kid in blk_.kids:
                kb = g.blocks[kid]
                if kb.open > blk_.open and kb.close < upto and TRANSPARENT.match(kb.header):
                    for k in range(kb.open, kb.close + 1):
                        code[k - blk_.open - 1] = " "
            code = "".join(code)
            hit = re.search(r"\b(return|if|else|try|catch|catchError|warnError|while|for|break|continue|switch)\b", code)
            if hit:
                return f"a `{hit.group(1)}` precedes it in the same block"
        return None
    contracts_seen = False
    SEG = r"[A-Za-z0-9_.][A-Za-z0-9_.-]*"
    SAFE_PATH = re.compile(rf"^{SEG}(?:/{SEG})*$")
    URL_FORMS = r"(?:git@github\.com:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\.git|\"\$[A-Z][A-Z0-9_]*\"|\"\$\{[A-Z][A-Z0-9_]*\}\")"

    def safe_rel(path: str) -> bool:
        return bool(SAFE_PATH.match(path)) and not any(c in (".", "..") for c in path.split("/"))

    def stmt_wrapped(start: int, end: int) -> tuple[int, int, list[str], str | None]:
        """Grow a statement outward through dir('literal') { <only this statement> } wrappers. Returns the outer
        statement span, the dir literals outermost first, and a problem (a non-literal or unsafe dir, a wrapper that
        also holds other statements)."""
        dirs: list[str] = []
        while True:
            blk = g.innermost(start)
            if blk is None:
                return start, end, dirs, None
            b = g.blocks[blk]
            m = re.fullmatch(r"dir\('([^']*)'\)", b.header)
            if not m:
                if b.header.startswith("ws(") or b.header.startswith("node("):
                    return start, end, dirs, f"it sits under `{b.header[:40]}`, which changes the workspace"
                if b.header.startswith("dir("):
                    return start, end, dirs, f"it sits under `{b.header[:40]}`, whose directory cannot be resolved to a literal path"
                return start, end, dirs, None
            inner = (g.code_only(b.open + 1, start) + g.code_only(end, b.close)).strip()
            if inner:
                return start, end, dirs, f"its dir('{m.group(1)}') block holds other statements"
            if not safe_rel(m.group(1)):
                return start, end, dirs, f"dir('{m.group(1)}') is not a plain relative path (no .., ., variables, absolute or ~ forms)"
            dirs.insert(0, m.group(1))
            start = text.rfind("dir(", 0, b.open)
            end = b.close + 1

    def workspace_changes(pos: int) -> str | None:
        for bi in g.ancestors(pos):
            h = g.blocks[bi].header
            if h.startswith("ws(") or h.startswith("node(") or h.startswith("dir(") and not re.fullmatch(r"dir\('[^']*'\)", h):
                return f"it sits under `{h[:40]}`, which changes the workspace"
        return None

    def acquisition_at(i: int):
        """(statement start, end, relative destination, problem) for the acquisition on line i."""
        p = pos_of(i, first_code_col(i))
        tq = next(((s0, e0) for q, s0, e0 in g.strings if q == "'''" and s0 <= p < e0), None)
        if tq is not None:
            opener_line_start = text.rfind("\n", 0, tq[0] - 3) + 1
            opener = text[opener_line_start:tq[0] - 3]
            if not re.fullmatch(r"\s*sh\s+", opener):
                return None, None, None, "a nested acquisition must be a plain `sh '''…'''` step of its own"
            body = [x.strip() for x in text[tq[0]:tq[1]].split("\n")]
            body = [x for x in body if x and not x.startswith("#")]
            dest = None
            k = 0
            if k < len(body) and body[k] in ("set -eu", "set -euo pipefail"):
                k += 1
            rm = re.fullmatch(rf"rm -rf ({SEG}(?:/{SEG})*)", body[k]) if k < len(body) else None
            if rm:
                k += 1
            cl = re.fullmatch(rf"git clone {URL_FORMS} ({SEG}(?:/{SEG})*)", body[k]) if k < len(body) else None
            if not cl:
                return None, None, None, "a nested acquisition step may contain only [set -eu] [rm -rf <dir>] git clone <url> <dir> [git -C <dir> checkout main] — no other command, separator or effect"
            dest = cl.group(1)
            k += 1
            if k < len(body) and re.fullmatch(rf"git -C {re.escape(dest)} checkout main", body[k]):
                k += 1
            if k != len(body) or (rm and rm.group(1) != dest) or not safe_rel(dest):
                return None, None, None, "a nested acquisition step may contain only [set -eu] [rm -rf <dir>] git clone <url> <dir> [git -C <dir> checkout main] — no other command, separator or effect"
            return text.rfind("sh", 0, tq[0] - 3), tq[1] + 3, dest, None
        m = re.match(r"^git url: (?:env\.[A-Za-z_][A-Za-z0-9_]*|'[^']*'), branch: 'main'$", lines[i].strip())
        if m:
            blk = g.innermost(p)
            b = g.blocks[blk] if blk is not None else None
            dm = re.fullmatch(r"dir\('([^']*)'\)", b.header) if b else None
            inner = re.sub(r"\s+", " ", g.code_only(b.open + 1, b.close)).strip() if b else ""
            if not dm or not re.fullmatch(r"git url: (?:env\.[A-Za-z_][A-Za-z0-9_]*|), branch:", inner):
                return None, None, None, "a nested `git url:` acquisition must be the only statement of its own dir('<literal>') { … } step"
            if not safe_rel(dm.group(1)):
                return None, None, None, f"dir('{dm.group(1)}') is not a plain relative path"
            return text.rfind("dir(", 0, b.open), b.close + 1, dm.group(1), None
        if re.search(r"\bgit clone\b|\bgit -C \S+ checkout\b", lines[i]):
            return None, None, None, "a nested acquisition must be a dedicated acquisition step: sh \'\'\'[set -eu] [rm -rf <dir>] git clone <url> <dir> [git -C <dir> checkout main]\'\'\' or dir(\'<dir>\') { git url: …, branch: \'main\' }"
        return None, None, None, None

    bound_paths: dict[str, int] = {}
    seen_stmts: set[int] = set()
    for i in range(g_hi, len(lines)):
        l = lines[i]
        if is_comment(l) or "permitted-sha-guard.sh" in l or not ACQUIRE_RE.search(l):
            continue
        si = max(k for k, (s, _) in enumerate(stages) if s <= i)
        slo, shi = stage_range(lines, stages, si)
        a_start, a_end, rel, why = acquisition_at(i)
        if a_start is None and why is None:
            # not a nested-source acquisition: the root workspace
            body = lines[slo:shi]
            own_agent = any(re.match(r"^\s*agent\b", x) and not is_comment(x) for x in body)
            first_acq = next((k for k in range(slo, shi) if not is_comment(lines[k]) and ACQUIRE_RE.search(lines[k]) and "permitted-sha-guard.sh" not in lines[k]), None)
            leading = (own_agent and "checkout scm" in l and first_acq == i
                       and canon(lines[slo:shi]).split(" steps { ", 1)[-1].startswith("checkout scm script {"))
            if not leading:
                problems.append(f"line {i + 1}: the guarded workspace is re-acquired after the guard: {l.strip()[:90]}")
            continue
        if why:
            problems.append(f"line {i + 1}: {why}: {l.strip()[:90]}")
            continue
        if a_start in seen_stmts:
            continue
        seen_stmts.add(a_start)
        o_start, o_end, dirs, why = stmt_wrapped(a_start, a_end)
        why = why or workspace_changes(o_start)
        resolved = "/".join(dirs + [rel])
        if why:
            problems.append(f"line {i + 1}: '{resolved}' acquisition cannot be tied to a guarded path: {why}")
            continue
        if entry["contracts"] and resolved == entry["contracts"]:
            contracts_seen = True
        if resolved in bound_paths:
            problems.append(f"line {i + 1}: '{resolved}' is acquired again after its guard (line {bound_paths[resolved] + 1}) — a second checkout into a guarded path is refused")
            continue
        # The very next statement in the same block must be the dedicated guard step for the same RESOLVED path.
        parent = g.innermost(o_start)
        after = text[o_end:(g.blocks[parent].close if parent is not None else len(text))]
        nxt = re.match(r"(?:\s|//[^\n]*\n)*", after)
        g_start = o_end + nxt.end()
        why_not = ""
        rebound = False
        hm = re.match(r"(?:dir\('[^']*'\)\s*\{\s*)*timeout\(time: [0-9]+, unit: 'MINUTES'\)\s*\{\s*(sh '[^\n]*')\s*\}", text[g_start:])
        if not hm:
            why_not = "the statement right after the acquisition step is not the dedicated guard step (nothing may run between them)"
        else:
            sh_pos = g_start + hm.start(1)
            gl = text.count("\n", 0, sh_pos)
            dm2 = dedicated_at(gl)
            tb = g.innermost(sh_pos)
            t_start = text.rfind("timeout(", 0, g.blocks[tb].open) if tb is not None else sh_pos
            go_start, go_end, gdirs, gwhy = stmt_wrapped(t_start, g.blocks[tb].close + 1 if tb is not None else sh_pos)
            if not dm2:
                why_not = "the guard after the acquisition is not the dedicated template"
            elif gwhy or workspace_changes(go_start):
                why_not = gwhy or workspace_changes(go_start)
            elif g.innermost(go_start) != parent or go_start != g_start:
                why_not = "the guard step is not the next statement of the same block"
            elif not safe_rel(dm2.group("dir")):
                why_not = f"--dir {dm2.group('dir')} is not a plain relative path"
            elif "/".join(gdirs + [dm2.group("dir")]) != resolved:
                why_not = f"the guard resolves to '{'/'.join(gdirs + [dm2.group('dir')])}', the acquisition to '{resolved}' — a guard on another checkout binds nothing"
            else:
                dom = unskippable(sh_pos)
                if dom:
                    why_not = f"its dedicated guard step can be skipped while later steps still consume '{resolved}': {dom}"
                else:
                    rebound = True
                    bound_paths[resolved] = gl
        if not rebound:
            problems.append(f"line {i + 1}: '{resolved}' acquired after the guard is not re-bound by the dedicated guard step for that same resolved path, immediately after its acquisition step: {why_not}")
    if entry["contracts"] and not contracts_seen:
        problems.append(f"manifest names contracts={entry['contracts']} but no acquisition of it was found after the guard")

    # 11. every invocation of the guard is a canonical form, and every stage that runs one has a deadline
    canonical_lines: set[int] = set()
    guard_stage_lines: set[int] = set()
    for si, (sline, sname) in enumerate(stages):
        lo, hi = stage_range(lines, stages, si)
        own_block = stage_block(lo)
        for k in range(lo, hi):
            lk = lines[k]
            if is_comment(lk) or "permitted-sha-guard.sh" not in lk or g.stage_of(pos_of(k, first_code_col(k))) != own_block:
                continue
            st = lk.strip()
            code_line = g.is_code(pos_of(k, first_code_col(k)))
            primary = code_line and (sname in (GUARD_STAGE, REGUARD_STAGE) and st.startswith("def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh"))
            inline = code_line and re.match(r"^def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh'\)$", st) is not None
            dedicated = dedicated_at(k) is not None
            if primary or inline or dedicated:
                canonical_lines.add(k)
                guard_stage_lines.add(si)
    params_block = find_block(lines, re.compile(r"^\s*parameters\s*\{"))
    for k, lk in enumerate(lines):
        if is_comment(lk) or k in canonical_lines or (params_block and params_block[0] <= k <= params_block[1]):
            continue
        if GUARD_INVOCATION.search(lk):
            problems.append(f"line {k + 1}: the guard is invoked outside its canonical forms (primary/re-guard stage, inline re-guard, dedicated nested-source step): {lk.strip()[:100]}")
    for k in sorted(canonical_lines):
        st = lines[k].strip()
        sb_ = g.stage_of(pos_of(k, first_code_col(k)))
        if sb_ is not None and g.blocks[sb_].header in (f"stage('{GUARD_STAGE}')", f"stage('{REGUARD_STAGE}')"):
            continue   # a canonical guard stage: its deadline is the stage's own options { timeout(...) } (rule 3)
        blk = g.innermost(pos_of(k, first_code_col(k)))
        hdr = g.blocks[blk].header if blk is not None else ""
        tm = re.fullmatch(r"timeout\(time: ([0-9]+), unit: 'MINUTES'\)", hdr)
        body = re.sub(r"\s+", " ", g.code_only(g.blocks[blk].open + 1, g.blocks[blk].close)).strip() if blk is not None else ""
        only = body == "sh" if dedicated_at(k) else re.fullmatch(r"def (\w+) = sh\(returnStatus: true, script: \) if \(\1 != 0\) \{ error\( \) \}", body) is not None
        if not tm or not 1 <= int(tm.group(1)) <= 30 or not only:
            problems.append(f"line {k + 1}: the guard must be the only statement of a `timeout(time: N, unit: 'MINUTES') {{ … }}` block with 1 <= N <= 30 — a stalled git fetch must end the build: {st[:80]}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--only", help="judge only this Jenkinsfile (the downstream-definition check); the others need not be present")
    a = ap.parse_args()
    root = os.path.abspath(a.root)
    guard = os.path.join(root, GUARD_SCRIPT_REL)
    failures: list[str] = []
    if not os.path.isfile(a.manifest):
        print(f"FAIL: manifest missing: {a.manifest}")
        return 1
    entries = parse_manifest(a.manifest)
    present = sorted(os.path.basename(p) for p in glob.glob(os.path.join(root, "Jenkinsfile*")) if os.path.isfile(p))
    if a.only:
        if a.only not in present:
            failures.append(f"{a.only}: not present in {root}")
        elif a.only not in entries:
            failures.append(f"{a.only}: not classified in the manifest")
        elif entries[a.only]["scope"] != "in":
            failures.append(f"{a.only}: classified out of scope — it carries no guard")
        present = [a.only] if a.only in present else []
    else:
        for f in present:
            if f not in entries:
                failures.append(f"{f}: not classified in {os.path.relpath(a.manifest, root)} — add it as in (guarded) or out (with the reason)")
        for f in entries:
            if f not in present:
                failures.append(f"{f}: listed in the manifest but not present in {root}")
    guard_hash = ""
    if not os.path.isfile(guard):
        failures.append(f"guard script missing: {guard}")
    else:
        r = subprocess.run(["bash", "-n", guard], capture_output=True, text=True)
        if r.returncode != 0:
            failures.append(f"guard script does not parse: {r.stderr.strip()}")
        with open(guard, "rb") as fh:
            guard_hash = hashlib.sha256(fh.read()).hexdigest()
    n_in = n_out = 0
    for f in present:
        e = entries.get(f)
        if e is None:
            continue
        if e["scope"] == "out":
            n_out += 1
            print(f"  out  {f}: {e['reason']}")
            continue
        n_in += 1
        probs = check_in_scope(os.path.join(root, f), e, guard_hash)
        if probs:
            failures += [f"{f}: {p}" for p in probs]
            print(f"  FAIL {f}")
        else:
            print(f"  ok   {f}")
    if failures:
        print(f"FAIL: {len(failures)} problem(s):")
        for x in failures:
            print(f"  - {x}")
        return 1
    print(f"OK: {n_in} Jenkinsfile(s) carry the canonical permitted-commit guard (version {guard_hash[:12]}…) before any effect; {n_out} classified out of scope")
    return 0


if __name__ == "__main__":
    sys.exit(main())
