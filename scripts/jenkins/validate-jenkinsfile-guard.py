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
       stage('Permitted commit guard') { [agent {…}] steps { script {
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
     `checkout scm`, with the canonical inline re-guard `script { def V = sh(returnStatus: true, script:
     'bash scripts/jenkins/permitted-sha-guard.sh') if (V != 0) { error(…) } …`.
  7. Every `build job: '<job>'` forwards exactly one `string(name: 'PERMITTED_SHA', value: params.V)`
     (or `params.V.trim()` / `env.V`), and is protected by the canonical compatibility check FOR THAT
     JOB AND THAT SHA VARIABLE:
       def C = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh <job> "${V:?}"[ EXTRA…]')
       if (C != 0) { error(…) }
     EXECUTABLY, in one of two ways:
       (a) same stage, before the trigger, and the check's enclosing CONTROL blocks (everything except
           script/steps/stages/stage/dir/withEnv/withCredentials/timeout/node/ws) are a prefix of the
           trigger's — so the trigger cannot run unless the check ran first and did not error. A check
           inside `if (false) {…}`, `catchError {…}`, `try {…}`, a closure or a loop the trigger is not
           also inside is refused;
       (b) the check is followed, as its very next statement, by `env.GUARDED_DOWNSTREAM_<JOB> =
           'PASSED'` (set nowhere else), in an earlier stage, and the trigger's stage `when` gate (rule
           13) requires `env.GUARDED_DOWNSTREAM_<JOB> == 'PASSED'` — a skipped, caught or never-reached
           check leaves the flag unset and the trigger stage is skipped.
     A self-trigger (`build job: env.JOB_NAME`) needs only the forward. No annotation exempts a trigger.
  8. Every mutation token inside `post {}` lies inside an `if (…) {` block (or `else if`) whose whole
     condition is a conjunction of `env.<FLAG> == 'PASSED'` terms that includes the needed flag
     (DEPLOY_WORKSPACE_PERMITTED for files with a re-guard container, PERMITTED_SHA_GUARD otherwise).
     `!(…)`, `! (…)`, `(… ) == false`, `||`, `!=`, an else branch, a token after the block: refused.
  9. Every source acquisition after the primary guard: the ROOT workspace may not be re-acquired (except
     the leading `checkout scm` of a rule-6 stage); a NESTED directory must be re-bound, before the next
     token or acquisition, by the canonical nested guard for THAT directory with that source's OWN
     permission — Groovy `def V = sh(returnStatus: true, script: 'PERMITTED_SHA="${X_PERMITTED_SHA:-}"
     bash scripts/jenkins/permitted-sha-guard.sh --dir <dir> --ref main') if (V != 0) { error(…) }`
     whose control blocks are a prefix of the acquisition's, or a shell line that STARTS with
     `PERMITTED_SHA="${X_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir <dir> --ref
     main || exit 1` at the TOP LEVEL of its `sh '''` block (not inside if/case/loop/function/group/
     heredoc, not continued from the previous line, not prefixed by echo or anything else).
 10. contracts=<dir>: that directory is acquired (and therefore, by rule 9, bound) at least once.
 11. Inside every `sh '''` block, a line running permitted-sha-guard.sh ends with `|| exit 1`, and the
     block sets no `set +e`; `|| true` anywhere on such a line is refused.
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
messages are ignored; the shell top-level test is a lexical approximation (keywords and braces outside
quotes); nothing here executes a pipeline, compiles Declarative, or proves the controller runs THIS
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
    r"^stage\('(?P<name>[^']+)'\) \{ (?P<when>when \{ expression \{ env\.PERMITTED_SHA_GUARD == 'PASSED' \} \} )?(?:agent \{ label [^}]+ \} )?steps \{ script \{ "
    r"def rc = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh(?P<args>( --ref \"\$\{[A-Z_]+:\?\}\")?)'\) "
    r"if \(rc != 0\) \{ error\(" + STR + r"\) \} "
    r"env\.(?P<flag>PERMITTED_SHA_GUARD|DEPLOY_WORKSPACE_PERMITTED) = 'PASSED' \} \} \}$"
)
INLINE_REGUARD = re.compile(
    r"^(?:checkout scm )?script \{ def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh'\) "
    r"if \(\1 != 0\) \{ error\(" + STR + r"\) \}"
)
COMPAT_FORM = re.compile(
    r"^def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream\.sh (?P<job>[A-Za-z0-9_.-]+) "
    r"\"\$\{(?P<shavar>[A-Z][A-Z0-9_]*):\?\}\"(?: [A-Z][A-Z0-9_]*)*'\) "
    r"if \(\1 != 0\) \{ error\(" + STR + r"\) \}(?P<flag> env\.(?P<flagname>[A-Z][A-Z0-9_]*) = 'PASSED')?"
)
FORWARD_RE = re.compile(r"string\(\s*name:\s*'PERMITTED_SHA',\s*value:\s*(?P<v>(?:params|env)\.[A-Za-z0-9_]+(?:\.trim\(\))?|[^,)\]]*)\s*\)")
FORWARD_VALUE = re.compile(r"^(?:params|env)\.(?P<var>[A-Z][A-Z0-9_]*)(?:\.trim\(\))?$")
NESTED_GROOVY = re.compile(
    r"^def (\w+) = sh\(returnStatus: true, script: 'PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*)_PERMITTED_SHA:-\}\" "
    r"bash scripts/jenkins/permitted-sha-guard\.sh --dir (?P<dir>[^ ']+) --ref main'\) if \(\1 != 0\) \{ error\(" + STR + r"\) \}"
)
NESTED_SHELL = re.compile(
    r"^\s*PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*)_PERMITTED_SHA:-\}\" bash scripts/jenkins/permitted-sha-guard\.sh --dir (?P<dir>\S+) --ref main \|\| exit 1\s*$"
)
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

    def in_block_comment(self, pos: int) -> bool:
        return any(s <= pos < e for s, e in self.comments)

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


def shell_top_level(block_lines: list[str], idx: int) -> str | None:
    """None when block_lines[idx] runs unconditionally at the top level of its shell block; otherwise why not."""
    depth = 0
    heredoc = None
    prev = ""
    for raw in block_lines[:idx]:
        s = raw.strip()
        if heredoc is not None:
            if s == heredoc:
                heredoc = None
            continue
        if not s or s.startswith("#"):
            continue
        hm = re.search(r"<<-?\s*['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?", s)
        bare = re.sub(r"'[^']*'|\"(?:[^\"\\]|\\.)*\"|\$\{[^}]*\}|\$\([^)]*\)", " ", s)
        bare = re.sub(r"\s#.*$", "", bare)
        for w in re.findall(r"(?<![\w$./-])(if|fi|case|esac|do|done|\{|\})(?![\w./-])", bare):
            depth += 1 if w in ("if", "case", "do", "{") else -1
        if hm:
            heredoc = hm.group(1)
        prev = bare.rstrip()
    if heredoc is not None:
        return "it sits inside a heredoc"
    if depth != 0:
        return "it sits inside a shell if/case/loop/function/group"
    if re.search(r"(\\|&&|\|\||\||\bthen|\bdo|\belse)$", prev):
        return "the previous line continues into it"
    return None


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
            problems.append(f"stage '{names[si]}' runs on its own agent after the guard but does not open with the canonical inline re-guard (steps {{ [checkout scm] script {{ def V = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh') if (V != 0) {{ error(…) }} …)")
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
    compat_sites = []   # (line, job, shavar, flagname, chain, stage_block)
    for i, l in enumerate(lines):
        if is_comment(l) or "require-guarded-downstream.sh" not in l or not l.lstrip().startswith("def "):
            continue
        cm = COMPAT_FORM.match(canon(lines[i:i + 12]))
        if not cm:
            continue
        p = pos_of(i, first_code_col(i))
        compat_sites.append((i, cm.group("job"), cm.group("shavar"), cm.group("flagname"), g.control_chain(p), g.stage_of(p)))
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
        tchain, tstage = g.control_chain(tp), g.stage_of(tp)
        tsi = max(k for k, (s, _) in enumerate(stages) if s <= i)
        ok = False
        for (ci, cjob, cvar, cflag, cchain, cstage) in compat_sites:
            if cjob != job or cvar != var:
                continue
            if cstage == tstage and ci < i and is_prefix(cchain, tchain):
                ok = True
                break
            want = downstream_flag(job)
            csi = max(k for k, (s, _) in enumerate(stages) if s <= ci)
            if cflag == want and csi < tsi and want in gates.get(tsi, set()) and compat_flag_lines.get(want, []) and all(
                    any(x[0] <= fl < x[0] + 12 and x[3] == want for x in compat_sites) for fl in compat_flag_lines[want]):
                ok = True
                break
        if not ok:
            problems.append(
                f"line {i + 1}: build job: '{job}' is not executably protected by the canonical compatibility check for {job} with \"${{{var}:?}}\": "
                f"(a) in the same stage, before it, under no control block the trigger is not also under, or (b) followed by env.{downstream_flag(job)} = 'PASSED' "
                f"with the trigger's stage gated on env.{downstream_flag(job)} == 'PASSED'")
    for name, fls in compat_flag_lines.items():
        for fl in fls:
            if not any(x[0] <= fl < x[0] + 12 and x[3] == name for x in compat_sites):
                problems.append(f"line {fl + 1}: env.{name} may be set only as the statement right after its compatibility check")
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
    contracts_seen = False
    for i in range(g_hi, len(lines)):
        l = lines[i]
        if is_comment(l) or "permitted-sha-guard.sh" in l or not ACQUIRE_RE.search(l):
            continue
        si = max(k for k, (s, _) in enumerate(stages) if s <= i)
        slo, shi = stage_range(lines, stages, si)
        d = acquisition_dir(lines, i, slo)
        if d == ".":
            body = lines[slo:shi]
            own_agent = any(re.match(r"^\s*agent\b", x) and not is_comment(x) for x in body)
            first_acq = next((k for k in range(slo, shi) if not is_comment(lines[k]) and ACQUIRE_RE.search(lines[k]) and "permitted-sha-guard.sh" not in lines[k]), None)
            leading = (own_agent and "checkout scm" in l and first_acq == i
                       and canon(lines[slo:shi]).split(" steps { ", 1)[-1].startswith("checkout scm script {"))
            if not leading:
                problems.append(f"line {i + 1}: the guarded workspace is re-acquired after the guard: {l.strip()[:90]}")
            continue
        if entry["contracts"] and d == entry["contracts"]:
            contracts_seen = True
        acq_chain = g.control_chain(pos_of(i, first_code_col(i)))
        rebound = False
        why_not = ""
        for j in range(i + 1, shi):
            lj = lines[j]
            if is_comment(lj):
                continue
            sm = NESTED_SHELL.match(lj)
            if sm and sm.group("dir") == d:
                p = pos_of(j, first_code_col(j))
                blk = next(((s, e) for s, e in shell_blocks if s <= p < e), None)
                if blk is None:
                    why_not = "its shell guard is not inside a sh ''' block"
                    break
                blines = text[blk[0]:p].split("\n")
                reason = shell_top_level(blines[:-1] + [blines[-1]], len(blines) - 1)
                if reason:
                    why_not = f"its shell guard does not run unconditionally: {reason}"
                    break
                if not is_prefix(g.control_chain(blk[0]), acq_chain):
                    why_not = "its shell guard sits under a control block the acquisition is not under"
                    break
                rebound = True
                break
            if lj.lstrip().startswith("def "):
                gm = NESTED_GROOVY.match(canon(lines[j:min(j + 8, shi)]))
                if gm and gm.group("dir") == d:
                    if not is_prefix(g.control_chain(pos_of(j, first_code_col(j))), acq_chain):
                        why_not = "its guard sits under a control block (if/catchError/try/closure) the acquisition is not under"
                        break
                    rebound = True
                    break
            if token_at(lj):
                break
            if ACQUIRE_RE.search(lj) and "permitted-sha-guard.sh" not in lj and acquisition_dir(lines, j, slo) != d:
                break   # a different source acquired first — this one must have been bound before it
        if not rebound:
            problems.append(f"line {i + 1}: '{d}' acquired after the guard is not re-bound before the next effect by the canonical nested guard for it (PERMITTED_SHA=\"${{X_PERMITTED_SHA:-}}\" bash scripts/jenkins/permitted-sha-guard.sh --dir {d} --ref main …){': ' + why_not if why_not else ''}")
    if entry["contracts"] and not contracts_seen:
        problems.append(f"manifest names contracts={entry['contracts']} but no acquisition of it was found after the guard")

    # 11. shell blocks that run the guard
    for s, e in shell_blocks:
        blk = text[s:e].split("\n")
        if not any("permitted-sha-guard.sh" in x for x in blk):
            continue
        for x in blk:
            if "permitted-sha-guard.sh" in x and not is_comment(x):
                if "|| true" in x or not x.rstrip().endswith("|| exit 1"):
                    problems.append(f"shell guard line must end with `|| exit 1` and never `|| true`: {x.strip()[:100]}")
        if any(re.search(r"\bset \+e\b", x) for x in blk):
            problems.append("a shell block that runs the guard must not `set +e`")
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
