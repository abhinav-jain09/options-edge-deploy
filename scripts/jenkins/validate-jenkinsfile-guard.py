#!/usr/bin/env python3
"""Every Jenkins deploy job carries the permitted-commit guard, in canonical form, before its first effect.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge; each repository supplies a manifest classifying every checked-in Jenkinsfile* ANYWHERE in
the repository — not only the ones in its root. A Jenkins job can be defined by a Jenkinsfile at any
path (`<service>/Jenkinsfile` is the monorepo-subdir shape), and a definition nobody classified is a
definition nobody judged, so discovery walks the whole tree (git-ignored paths and `.git` excluded: the
manifest classifies CHECKED-IN definitions, not build output a workspace happens to hold):

    <path/to/Jenkinsfile> | in|out | options | reason
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
           after it — so any `return`, skipped branch, caught error or uncalled closure that skips the
           check leaves that block and skips the trigger as well. A check in an earlier sibling
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
     a triple-quoted block, a GString or a comment is not the step. Both paths are RESOLVED through EVERY enclosing
     block — each dir('<literal>') counts, whatever script/withEnv/timeout/… blocks sit between — and must be equal;
     a dir() with a variable, `..`, `.`, an absolute or `~` path, and any ws()/node() wrapper, are refused; a second
     acquisition into an already guarded resolved path is refused. The
     guard step must run UNSKIPPABLY: every enclosing block below the stage's `steps` is a sequencing block
     (script/dir/withEnv/timeout/…) and no return/if/else/try/catch/catchError/loop/break/continue precedes it in any
     of them, so no later step or stage consuming the source can run without it.
  9b. Provenance verification, structurally. The guard proves HEAD == permitted at one moment; the files an effect
     consumes later can still be replaced while HEAD stays the permitted commit. So:
     WHAT COUNTS AS AN EFFECT — say it exactly, because the rule is only as wide as this list. A step is a
     source-consuming EFFECT when it runs, at command position: `mvn` reaching the PACKAGE phase or a later
     lifecycle phase (package, pre-integration-test, integration-test, post-integration-test, verify, install,
     deploy) or a plugin goal that publishes or rewrites (MVN_EFFECT_GOALS / MVN_EFFECT_PLUGIN_RE below);
     `docker build` / `docker buildx build`; `rsync`; `scp`; `helm install|upgrade`; `ansible-playbook` with a
     playbook operand; `ssh` fed local data; or a repository script that (transitively) runs one of those.
     `mvn compile` and `mvn test` are deliberately NOT on that list: they compile the checkout but produce no
     artifact that is installed, shipped or published, so the artifact chain has nothing to bind. This is a
     REAL limit, not an oversight, and it is the reason the claim is "every step that PACKAGES, INSTALLS,
     PUBLISHES or SHIPS source" and never "every step that builds source" — a compile-only step can still read a
     file that was replaced after the guard ran. Two things narrow that: the compile happens under the guard
     (rule 3), and a pipeline that wants the tree proved before it compiles puts the ordinary verify step in
     front of the compile, exactly as it does before an effect (option-edge-feed-gateway's Test stage does).
     Deliberate mutation work — a mutation-testing campaign that edits a source, compiles it, and restores it —
     could not run at all under a literal "every compilation is the permitted tree" policy, so that policy is
     not claimed here.
       (i)  EVERY source-consuming effect is a DEDICATED STEP — a plain `sh '…'` / `sh '''…'''` whose whole body is ONE
            command (backslash continuations allowed; nothing else: no second command, no `; && || | &`, `$( )`, backticks,
            redirections, here-documents, cd/pushd/export, assignment prefixes, comments, globs) — read against a FIXED
            per-command template with an explicit option list and the consumed sources as LITERAL paths:
              mvn               -B/-q/-ntp/-o/-e/-U/-am/-amd/-N/-V/-X/…, -f/-s/-gs/-t <literal path>, -pl/-T/-rf/-P <literal or
                                "${VAR}">, -P<profiles>, -D<key>[=<literal or "…${VAR}…">], lifecycle phases and a fixed list
                                of read-only plugin goals (a goal that rewrites sources or publishes through a plugin: refused);
              docker build / buildx build
                                --builder/--platform/-t/--label/--build-arg/--metadata-file/--iidfile/--progress/--target/
                                --network/--cache-from/--cache-to/--provenance/--sbom/--attest/--output <literal or "${VAR}">,
                                --push/--load/--no-cache/--pull/-q, -f <literal path>, exactly ONE literal context
                                (--build-context, --secret, --ssh, -v and anything else: refused);
              rsync             short flags, --delete…/--exclude/--include/--chmod…, -e/--rsh only `ssh [-o K=V] [-p N] [-i f]`,
                                literal workspace sources, a remote `host:path` destination;
              scp               -o/-P/-i/-F/-J/-l/-c <literal>, -q/-r/-p/-C/-B/-4/-6/-v/-3, literal workspace sources, a remote
                                destination (-S refused);
              helm install/upgrade  fixed flags, -f/--values <literal path>, a release and ONE chart (local charts literal);
              ansible-playbook  one literal playbook, -i/--inventory literal, -e key=value (never @file), fixed flags;
              a repository script that (transitively) runs one of these: `[bash|sh|python3] <literal script> [args]`.
            Effects are found in EVERY string of the file (stage steps, post{}, top-level methods and constants), at command
            position in a shell-aware reading: quoted and escaped command names are read unquoted, assignment prefixes,
            control words and wrappers (env/xargs/timeout/nice or a `$wrapper` variable) are stepped over, `bash -c`,
            `eval` and here-documents fed to a local shell are read too, a program piped into a local shell is refused,
            `"$DIR/name"` is classified by its literal basename, and — in a step's own shell — a command name computed at
            run time (a variable or a Groovy interpolation) is refused. A script a step runs is read from the tree (or from
            the here-document in this file that writes it); one that is neither is refused.
       (ii) the dedicated effect step is IMMEDIATELY preceded, in the same block, by the dedicated verify step for EVERY
            checkout a consumed path resolves into or equals — the primary workspace or the deepest nested checkout
            containing it — and, for consumers handed a directory whole (a docker context, an rsync/scp source, a chart
            directory), every nested checkout lying inside it; each verify with the SAME permission variable its checkout's
            guard bound. Consecutive verify steps directly before the effect form its chain; anything else between them
            breaks coverage.
     The verify step is recognised from the Groovy token structure exactly like the guard (a real `sh` token whose sole
     argument is one single-quoted literal equal to the template), sits alone in a timeout(time: N, unit: 'MINUTES')
     block inside a stage's steps. verify-permitted-tree.sh re-checks HEAD == permitted AND a clean working tree at run
     time (nothing modified, staged, deleted or untracked; ignored paths only at or under a declared
     --allow-ignored PATH, anchored at the checkout root — a bare name is not matched wherever it occurs).
  9c. The shell text of every `sh` step is READABLE: one string literal, or a top-level constant that is one literal ending
     at a line boundary followed by one literal (as in `sh JDK_SETUP + <literal>`) — never pieces joined at run time.
     The permission variables (PERMITTED_SHA, X_PERMITTED_SHA) are READ-ONLY: defined only by parameters{}, never assigned
     by `env.X =`, withEnv([...]) or environment{}. `parallel` is not accepted (a concurrent branch could change a tree
     between a verify and its effect). A git command that moves a checkout's HEAD or worktree after the guard is refused
     outside the dedicated acquisition step.
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

LIMITS — what this cannot prove: a repository script's OWN body is read only for the effects it runs (literal command
names, here-documents, `bash -c`, scripts it runs by a literal path) — a command name it computes at run time, or a script
it reaches by a computed path, is not seen (a step's own shell refuses both); a program installed on the agent by an
absolute path is a host tool whose behaviour is outside this validator; Python helpers are read by token; stage order is the order of `stage('…')` lines (no dynamic or parallel
stages are modelled — none exist in scope); mutation tokens are a fixed list, so a helper script called
before the guard is invisible unless its name is a token — the manifest's before= set is the reviewed
statement that those stages are effect-free; tokens in `//`/`#` comment lines and whole-line `echo '…'`
messages are ignored; nothing here executes a pipeline, compiles Declarative, or proves the controller runs THIS
file — require-guarded-downstream.sh establishes, for a child, that the job's SCM definition points at
this file at the forwarded commit.
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import os
import re
import shlex
import subprocess
import sys

GUARD_STAGE = "Permitted commit guard"
REGUARD_STAGE = "Permitted commit guard (deploy workspace)"
GUARD_SCRIPT_REL = "scripts/jenkins/permitted-sha-guard.sh"
FLAG = "PERMITTED_SHA_GUARD"
REFLAG = "DEPLOY_WORKSPACE_PERMITTED"
DOWNSTREAM_FLAG_PREFIX = "GUARDED_DOWNSTREAM_"
STR = r"(?:\"[^\"]*\"|'[^']*')"
# An --allow-ignored declaration (verify-permitted-tree.sh): a PATH relative to the verified checkout, never a bare
# name matched wherever it occurs. A component is a literal name or the single character `*` (exactly one whole
# component). A declaration containing `*` MUST be double-quoted in the shell text, or the shell would expand it
# against the workspace before the verifier ever sees it. `**` matches neither component form and is refused here as
# it is refused by the verifier.
ALLOW_COMP = r"(?:\*|[A-Za-z0-9._][A-Za-z0-9._-]*)"
ALLOW_PATH = ALLOW_COMP + r"(?:/" + ALLOW_COMP + r")*"
ALLOW_ARG = r"(?:[A-Za-z0-9._][A-Za-z0-9._/-]*|\"" + ALLOW_PATH + r"\")"
STAGE_RE = re.compile(r"^\s*stage\(\s*'((?:[^'\\]|\\.)*)'")
BUILD_JOB_RE = re.compile(r"\bbuild\s*\(?\s*job:\s*(env\.JOB_NAME|'([^']+)')")
ACQUIRE_RE = re.compile(r"\bgit url:|\bgit\s*\(|\bgit\s+(?:branch|credentialsId|changelog|poll)\s*:|\bgit clone\b|\bcheckout\(|\bcheckout scm\b|\bgit pull\b|\bgit checkout\b|\bgit -C \S+ checkout\b")
GATE_FLAG_ONLY = "when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } } "
CANON_GUARD = re.compile(
    r"^stage\('(?P<name>[^']+)'\) \{ (?P<when>when \{ expression \{ env\.PERMITTED_SHA_GUARD == 'PASSED' \} \} )?(?:agent \{ label (?:[^{}]|\$\{[^{}]*\})+ \} )?"
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
# The provenance verifier, in the SAME dedicated-step form as the guard: a real `sh` step whose sole argument is one
# single-quoted literal running verify-permitted-tree.sh for one literal --dir with zero or more literal
# --allow-ignored paths, and nothing else. It re-proves, immediately before an effect, that the tree the effect
# will consume is still the permitted commit's tree (verify-permitted-tree.sh). The only variable part is the source's
# own permission variable, the literal directory and the allow-list.
VERIFY_STEP = re.compile(
    r"^sh '(?P<cmd>PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9_]*):[-?]\}\" "
    r"bash scripts/jenkins/verify-permitted-tree\.sh --dir (?P<dir>[A-Za-z0-9._][A-Za-z0-9._/-]*)"
    r"(?P<allow>(?: --allow-ignored " + ALLOW_ARG + r")*))'$"
)
# The exact literals of the guard, verify and downstream-check invocations (primary / inline re-guard, dedicated nested
# guard, dedicated verify, compatibility check). Their shape and placement are judged by rules 3, 6, 7, 9 and 9b; the
# effect scan does not read them as script invocations.
CANONICAL_LITERAL = re.compile(
    r"bash scripts/jenkins/permitted-sha-guard\.sh(?: --ref \"\$\{[A-Z_]+:\?\}\")?"
    r"|PERMITTED_SHA=\"\$\{[A-Z][A-Z0-9_]*:[-?]\}\" bash scripts/jenkins/permitted-sha-guard\.sh --dir [A-Za-z0-9._][A-Za-z0-9._/-]* --ref main"
    r"|PERMITTED_SHA=\"\$\{[A-Z][A-Z0-9_]*:[-?]\}\" bash scripts/jenkins/verify-permitted-tree\.sh --dir [A-Za-z0-9._][A-Za-z0-9._/-]*(?: --allow-ignored " + ALLOW_ARG + r")*"
    r"|bash scripts/jenkins/require-guarded-downstream\.sh [A-Za-z0-9_.-]+ \"\$\{[A-Z][A-Z0-9_]*:\?\}\"(?: [A-Z][A-Z0-9_]*)*"
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


# ----------------------------------------------------------------------------------------- shell views
# A shell body is read in TWO views. The CODE view blanks everything that is data to the shell — single-quoted
# contents, double-quoted contents (except a `$( … )` command substitution inside them, which IS code), `#` comments
# and here-document bodies — so a command name found in it sits at a place the shell would execute it, never inside a
# message or a remote ssh argument. The ORIGINAL text is what the template grammar reads once a command is known to be
# an effect.
def groovy_decode(body: str, gstring: bool = False) -> tuple[str, list[int]]:
    """What the shell receives from a Groovy string literal, and for each decoded character its offset in the source
    body: a doubled backslash becomes one, an escaped quote or dollar becomes the character, an escaped n / t becomes a
    newline / tab, and a backslash before a newline is a Groovy line continuation. In a GString (double-quoted), every
    Groovy interpolation (`${…}`, `$name`) is replaced by GROOVY_VALUE characters: its text is chosen at run time."""
    out: list[str] = []
    pos: list[int] = []
    i, n = 0, len(body)
    while i < n:
        c = body[i]
        if c == BS and i + 1 < n:
            nx = body[i + 1]
            if nx in (BS, "'", '"', "$"):
                out.append(nx)
                pos.append(i)
                i += 2
                continue
            if nx in ("n", "t"):
                out.append("\n" if nx == "n" else "\t")
                pos.append(i)
                i += 2
                continue
            if nx == "\n":
                i += 2
                continue
        if gstring and c == "$":
            m = re.match(r"\$\{[^{}]*\}|\$[A-Za-z_][A-Za-z0-9_.]*", body[i:])
            if m:
                for k in range(m.end()):
                    out.append(GROOVY_VALUE)
                    pos.append(i + k)
                i += m.end()
                continue
        out.append(c)
        pos.append(i)
        i += 1
    return "".join(out), pos


GROOVY_VALUE = "\x01"


def shell_code_view(s: str) -> str:
    """The CODE view of a shell text, same length: everything that is data to the shell is blanked — single-quoted
    contents, double-quoted contents (except a `$( … )` command substitution inside them, which IS code), `#` comments,
    here-document bodies, `${…}` parameter expansions, `[[ … ]]` test expressions and `case` arm patterns — and a
    backslash-newline continuation joins its lines. A command name found in this view sits where the shell executes it."""
    s = s.replace(BS + "\n", "  ")
    out = list(s)
    n = len(s)
    i = 0
    heredoc_tags: list[str] = []

    def blank(a: int, b: int, keep_nl: bool = True) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n" or not keep_nl:
                out[k] = " "

    def skip_subst(k: int) -> int:
        """k at the `(` of a `$(`: index just past its matching `)` (quotes inside respected, nested parens counted)."""
        depth = 0
        while k < n:
            c = s[k]
            if c == BS:
                k += 2
                continue
            if c == "'":
                e = s.find("'", k + 1)
                k = n if e < 0 else e + 1
                continue
            if c == '"':
                k += 1
                while k < n and s[k] != '"':
                    if s[k] == BS:
                        k += 1
                    elif s.startswith("$(", k):
                        k = skip_subst(k + 1)
                        continue
                    k += 1
                k += 1
                continue
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return k + 1
            k += 1
        return n

    def skip_braces(k: int) -> int:
        """k at the `{` of a `${`: index of its matching `}`."""
        depth = 0
        while k < n:
            if s[k] == BS:
                k += 2
                continue
            if s[k] == "{":
                depth += 1
            elif s[k] == "}":
                depth -= 1
                if depth == 0:
                    return k
            k += 1
        return n

    while i < n:
        c = s[i]
        if c == BS:
            i += 2
            continue
        if c == "\n" and heredoc_tags:
            tag = heredoc_tags.pop(0)
            j = i + 1
            while j < n:
                e = s.find("\n", j)
                e = n if e < 0 else e
                line = s[j:e]
                blank(j, e)
                j = e + 1
                if line.strip() == tag:
                    break
            i = j
            continue
        if c == "#" and (i == 0 or s[i - 1] in " \t\n;&|(`{"):
            e = s.find("\n", i)
            e = n if e < 0 else e
            blank(i, e)
            i = e
            continue
        if c == "'":
            e = s.find("'", i + 1)
            e = n if e < 0 else e
            blank(i + 1, e, keep_nl=False)
            i = e + 1
            continue
        if c == '"':
            k = i + 1
            while k < n and s[k] != '"':
                if s[k] == BS:
                    blank(k, k + 2, keep_nl=False)
                    k += 2
                    continue
                if s.startswith("$((", k):
                    e = s.find("))", k + 3)
                    e = n if e < 0 else e
                    blank(k + 3, e, keep_nl=False)
                    k = e + 2
                    continue
                if s.startswith("$(", k):
                    # a command substitution inside double quotes IS code: its own view, recursively
                    e = skip_subst(k + 1)
                    inner = shell_code_view(s[k + 2:max(k + 2, e - 1)])
                    out[k + 2:k + 2 + len(inner)] = list(inner)
                    k = e
                    continue
                out[k] = " "
                k += 1
            i = k + 1
            continue
        if s.startswith("$((", i):
            e = s.find("))", i + 3)
            e = n if e < 0 else e
            blank(i + 3, e, keep_nl=False)
            i = e + 2
            continue
        if s.startswith("${", i):
            e = skip_braces(i + 1)
            blank(i + 2, e, keep_nl=False)
            i = e + 1
            continue
        if s.startswith("[[", i) and (i == 0 or s[i - 1] in " \t\n;&|(!"):
            e = s.find("]]", i + 2)
            e = n if e < 0 else e
            blank(i + 2, e, keep_nl=False)
            i = e + 2
            continue
        if s.startswith("<<", i) and not s.startswith("<<<", i):
            m = re.match(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1", s[i:])
            if m:
                heredoc_tags.append(m.group(2))
                i += m.end()
                continue
        i += 1
    v = "".join(out)

    def _blank_pat(m: re.Match) -> str:
        pat = m.group(2)
        if pat.strip() in ("esac", "") or pat.strip().startswith("$("):
            return m.group(0)
        return m.group(1) + re.sub(r"[^\n]", " ", pat)
    # a `case` arm's pattern list (`pat | pat)` after `in` or `;;`) is not a command
    return re.sub(r"((?:\bin\b|;;&?|;&)\s*)(\(?[^;()\n]*?\))", _blank_pat, v)


def simple_command_end(view: str, start: int) -> int:
    """End offset of the simple command starting at `start` in a code view: the first `;`, `&`, `|`, `)`, `` ` `` or
    newline (continuations are already joined in the view)."""
    k = start
    n = len(view)
    while k < n:
        c = view[k]
        if c == BS:
            k += 2
            continue
        if c in ";&|)`\n":
            return k
        k += 1
    return n


def raw_words(s: str, i: int, end: int) -> list[tuple[int, str, bool]]:
    """The shell words of s[i:end]: (start offset, the word with its quoting removed, whether it contains a `$` outside
    single quotes). Stops at an unquoted redirection or separator."""
    out: list[tuple[int, str, bool]] = []
    n = min(end, len(s))
    while i < n:
        while i < n and s[i] in " \t":
            i += 1
        if i >= n or s[i] in ";&|()<>\n":
            break
        st, buf, dollar = i, [], False
        while i < n and s[i] not in " \t;&|()<>\n":
            c = s[i]
            if c == BS and i + 1 < n:
                buf.append(s[i + 1])
                i += 2
            elif c == "'":
                e = s.find("'", i + 1)
                e = n if e < 0 else e
                buf.append(s[i + 1:e])
                i = e + 1
            elif c == '"':
                e = i + 1
                while e < n and s[e] != '"':
                    if s.startswith("$(", e):
                        d, e = 0, e + 1
                        while e < n:
                            if s[e] == "(":
                                d += 1
                            elif s[e] == ")":
                                d -= 1
                                if d == 0:
                                    break
                            e += 1
                        e += 1
                        continue
                    e += 2 if s[e] == BS else 1
                part = s[i + 1:e]
                dollar = dollar or "$" in part
                buf.append(part)
                i = e + 1
            elif c == "$" and i + 1 < n and s[i + 1] == "(":
                # a command substitution is part of this word (its own commands are found at their own command start)
                d, e = 0, i + 1
                while e < n:
                    if s[e] == "(":
                        d += 1
                    elif s[e] == ")":
                        d -= 1
                        if d == 0:
                            break
                    e += 1
                buf.append(s[i:e + 1])
                dollar = True
                i = e + 1
            else:
                dollar = dollar or c == "$"
                buf.append(c)
                i += 1
        out.append((st, "".join(buf), dollar))
    return out


CMD_START_RE = re.compile(r"(?:^|(?<=[;&|(`{\n])|(?<=\$\())[ \t]*(?=\S)", re.M)
CONTROL_WORDS = {"then", "do", "else", "if", "elif", "while", "until", "!", "time", "nohup", "exec", "sudo", "command", "builtin", "{"}
WRAPPER_CMDS = {"env", "xargs", "timeout", "gtimeout", "nice", "stdbuf", "caffeinate"}
WRAPPABLE = {"docker", "mvn", "rsync", "scp", "helm", "ansible-playbook", "ssh", "git", "kubectl", "curl", "python3", "python", "bash", "sh"}
EFFECT_CMD_NAMES = {"mvn", "docker", "rsync", "scp", "helm", "ansible-playbook", "ssh", "eval", "bash", "sh", "python3", "python", "source", "."}
SCRIPT_EXT = (".sh", ".bash", ".py")


def command_names(text: str, view: str, strict: bool):
    """Every command a shell text runs: (offset of the command word, the command word unquoted, a refusal reason or None).
    Assignment prefixes, control words and wrapper commands (env/xargs/timeout/…, or a variable holding a wrapper such as
    `$tmo`) are stepped over; a quoted or escaped name is read unquoted; `"$DIR/name"` is classified by its literal
    basename. In STRICT mode (the Jenkinsfile's own shell steps) a name computed at run time — a variable or a Groovy
    interpolation — is refused unless it only wraps a known command."""
    for m in CMD_START_RE.finditer(view):
        start = m.end()
        if start >= len(view) or view[start] in "\n#":
            continue
        end = simple_command_end(view, start)
        ws = raw_words(text, start, end)
        k, wrapped = 0, False
        while k < len(ws):
            w = ws[k][1]
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", w, re.S) and not text[ws[k][0]:].startswith(("'", '"')):
                k += 1
                continue
            if w in CONTROL_WORDS:
                k += 1
                continue
            if w in WRAPPER_CMDS:
                k += 1
                while k < len(ws) and (ws[k][1].startswith("-") or re.fullmatch(r"[0-9.]+[smhd]?", ws[k][1]) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", ws[k][1], re.S)):
                    k += 1
                continue
            if ws[k][2] and re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", w):
                wrapped = True
                k += 1
                continue
            break
        if k >= len(ws):
            continue
        pos, name, dollar = ws[k]
        if GROOVY_VALUE in name:
            if strict:
                yield pos, name, "the command name is a Groovy interpolation (chosen at run time) — name the command literally"
            continue
        if dollar:
            stripped = re.sub(r"^\$\{?(?:WORKSPACE|PWD)\}?/", "", name)
            if "$" not in stripped:
                name = stripped
            else:
                base = re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?(?:/[^$/]+)*/([^$/]+)", name)
                if base and base.group(1) in EFFECT_CMD_NAMES:
                    yield pos, base.group(1), None      # "$DIR/docker" is docker (its dedicated template then refuses the form)
                elif base and not base.group(1).endswith(SCRIPT_EXT):
                    pass                                 # "$KBIN/kafka-topics": a tool named by its literal basename
                elif strict:
                    yield pos, name, "the command name is computed at run time (a variable), so the validator cannot tell what runs — name the command literally"
                continue
        if wrapped and name not in WRAPPABLE:
            if strict:
                yield pos, name, f"a command behind a run-time variable (`{ws[k - 1][1]} {name}`) — name the command literally"
            continue
        if name in EFFECT_CMD_NAMES or name.endswith(SCRIPT_EXT):
            yield pos, name, None


# Maven goals that PRODUCE the artifact (the package phase and everything after it), or plugin goals that publish or
# rewrite: any of these makes the invocation a source-consuming effect. `compile`, `test-compile` and `test` are NOT
# here, and their absence is the exact width of the claim (rule 9b, WHAT COUNTS AS AN EFFECT): they compile the
# checkout but produce nothing that is installed, shipped or published. A pipeline that wants its tree proved before
# it compiles places the ordinary verify step in front of the compile; the validator does not require it there.
MVN_EFFECT_GOALS = {"package", "pre-integration-test", "integration-test", "post-integration-test", "verify", "install", "deploy"}
MVN_EFFECT_PLUGIN_RE = re.compile(r"^(?:jib|docker|dockerfile|spring-boot|deploy|install|release|scm|versions|assembly|shade|jar|war|source|javadoc|gpg|nexus-staging|buildplan)\b")
# the same effects written in a Python helper (argv lists or command strings)
PY_EFFECT_RE = re.compile(r"""['"](?:mvn|rsync|scp|ansible-playbook)['"]|['"]docker['"]\s*,\s*(?:['"]buildx['"]\s*,\s*)?['"]build['"]"""
                          r"""|['"]helm['"]\s*,\s*['"](?:install|upgrade)['"]|['"](?:mvn|rsync|scp|ansible-playbook)\s|docker\s+(?:buildx\s+)?build\b|helm\s+(?:install|upgrade)\b""")


def effect_commands(text: str, resolver=None, strict: bool = True, seen: frozenset = frozenset()) -> list[dict]:
    """Every source-consuming effect a shell text runs. Each item: {"pos": offset in `text` of the command word, "kind":
    mvn|docker|rsync|scp|helm|ansible|script|ssh, "cmd": the simple command's text (continuations joined), "why": a
    refusal reason when the command cannot be classified safely}.

    `resolver(path) -> (exists, text)` reads a script named by a literal repository-relative path; a script that runs an
    effect (transitively) makes its invocation an effect of kind `script`. In STRICT mode a script that is not in the tree
    is refused; followed scripts are read in non-strict mode (effects only)."""
    view = shell_code_view(text)
    joined = text.replace(BS + "\n", "  ")   # same length as `text`: continuations joined, offsets preserved
    found: list[dict] = []
    for pos, cmd, why_name in command_names(joined, view, strict):
        end = simple_command_end(view, pos)
        raw = joined[pos:end]
        words = raw.split()
        rest = words[1:] if words else []
        item = {"pos": pos, "cmd": raw.strip(), "kind": None, "why": None}
        if why_name:
            item["kind"] = "script"
            item["why"] = why_name
            found.append(item)
            continue
        if cmd == "mvn":
            skip_next = False
            goals = []
            for w in rest:   # arguments of options that take a value are not goals
                if skip_next:
                    skip_next = False
                    continue
                if w in ("-f", "--file", "-pl", "--projects", "-s", "--settings", "-T", "--threads", "-rf", "--resume-from", "-P", "--activate-profiles", "-gs", "-t", "--toolchains", "-l", "--log-file"):
                    skip_next = True
                    continue
                if w.startswith(("-", '"', "'", "$")):
                    continue
                goals.append(w)
            if any(g in MVN_EFFECT_GOALS or MVN_EFFECT_PLUGIN_RE.match(g) for g in goals):
                item["kind"] = "mvn"
        elif cmd == "docker":
            if rest[:1] == ["build"] or rest[:2] == ["buildx", "build"]:
                item["kind"] = "docker"
        elif cmd == "rsync":
            item["kind"] = "rsync"
        elif cmd == "scp":
            item["kind"] = "scp"
        elif cmd == "helm":
            if rest[:1] in (["install"], ["upgrade"]):
                item["kind"] = "helm"
        elif cmd == "ansible-playbook":
            if any(not w.startswith("-") for w in rest):   # `--version` reads nothing; a playbook operand is an effect
                item["kind"] = "ansible"
        elif cmd == "eval":
            if effect_commands(text[pos + 4:end].replace("'", " ").replace('"', " "), resolver, strict, seen):
                item["kind"] = "script"
                item["why"] = "runs a source-consuming effect through `eval`; make it a dedicated effect step"
        elif cmd == "ssh":
            # ssh runs a command ELSEWHERE; it is a local source ship only when local data is fed into it: a redirected
            # file (`< path`, not a here-document) or a pipe into it (`tar c … | ssh …`).
            if re.search(r"(?<!<)<(?!<)", view[pos:end]) or view[:pos].rstrip().endswith("|"):
                item["kind"] = "ssh"
                item["why"] = "ships local data over ssh (a redirected file or a pipe into ssh); ship a verified tree with scp or rsync instead"
        else:
            path = cmd
            if cmd in ("bash", "sh", "python3", "python", "source", "."):
                args = [w for w in rest if not (w.startswith("-") and cmd in ("bash", "sh", "python3", "python"))]
                if cmd in ("bash", "sh") and "-c" in rest:
                    # the inline string is shell too: read it with its quotes dropped
                    qm = re.search(r"-c\s+(.*)", text[pos:end], re.S)
                    inner = (qm.group(1) if qm else "").replace("'", " ").replace('"', " ")
                    if effect_commands(inner, resolver, False, seen):
                        item["kind"] = "script"
                        item["why"] = "runs a source-consuming effect inside an inline `bash -c` / `sh -c` string; make it a dedicated effect step"
                        found.append(item)
                    continue
                if cmd in ("bash", "sh", "python3", "python") and (not args or args[0].startswith("<") or args[0] == "-"):
                    # a LOCAL interpreter reading its program from stdin: a here-document (read here) or a pipe (unreadable)
                    hm = re.search(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1", text[pos:end])
                    if view[:pos].rstrip().endswith("|"):
                        item["kind"] = "script"
                        item["why"] = "pipes a program into a local shell or interpreter, which the validator cannot read — commit it as a repository script"
                        found.append(item)
                    elif hm:
                        nl = text.find("\n", end)
                        body_lines, k = [], (len(text) if nl < 0 else nl + 1)
                        while k < len(text):
                            e2 = text.find("\n", k)
                            e2 = len(text) if e2 < 0 else e2
                            if text[k:e2].strip() == hm.group(2):
                                break
                            body_lines.append(text[k:e2])
                            k = e2 + 1
                        body_ = "\n".join(body_lines)
                        if (PY_EFFECT_RE.search(body_) if cmd.startswith("python") else effect_commands(body_, resolver, False, seen)):
                            item["kind"] = "script"
                            item["why"] = "runs a source-consuming effect from a here-document fed to a local interpreter; make it a dedicated effect step"
                            found.append(item)
                    continue
                path = args[0] if args else ""
            path = re.sub(r"^\$\{?(?:WORKSPACE|PWD)\}?/", "", path.strip("\"'"))
            if not path.endswith(SCRIPT_EXT):
                continue
            if path.startswith(("/", "~")):
                continue   # a program installed on the host (outside the repository): a host tool, like any binary
            if "$" in path:
                if strict:
                    item["kind"] = "script"
                    item["why"] = f"runs a script by a computed path ('{path}'), which the validator cannot read — run it by its literal repository path"
                    found.append(item)
                continue
            if resolver is None:
                continue
            npath = os.path.normpath(path)
            if npath in seen:
                continue
            exists, body = resolver(npath)
            if not exists:
                if strict:
                    item["kind"] = "script"
                    item["why"] = f"invokes '{path}', which is not a file in the tree the validator sees — it cannot be judged"
                    found.append(item)
                continue
            if script_has_effects(body, resolver, seen | {npath}, npath.endswith(".py")):
                item["kind"] = "script"
                item["path"] = npath
        if item["kind"]:
            found.append(item)
    return found


def script_has_effects(body: str, resolver, seen: frozenset, python: bool = False) -> bool:
    """True when a repository script (or a script it runs by a literal path, transitively) runs a source-consuming
    command. A Python helper is read by token (an argv list or a command string naming the effect)."""
    if python:
        return bool(PY_EFFECT_RE.search(body))
    return any(e["kind"] for e in effect_commands(body, resolver, False, seen))


# ----------------------------------------------------------------------------------------- effect templates
# The ONLY accepted form of a source-consuming effect is a DEDICATED step whose whole shell body is that one command,
# read against a per-command fixed template: an explicit list of option slots, each with a fixed value shape, and the
# consumed source paths as LITERALS so they can be resolved and covered by a verify step. Nothing else can be in that
# shell: no second command, no `; && || | & ( ) { } < > `` ` `` $( ) here-document, no cd/pushd/export, no
# assignment prefix, no comment, no glob. A body the grammar does not accept is refused and must be restructured — the
# preparation moves to an earlier step that is not an effect, the values reach the effect as environment variables.
# (always used with fullmatch: a `$` anchor admits a trailing newline). `!` inside a word is literal in a non-interactive
# shell (only a whole leading `!` negates, and every template requires its command as the first word).
WORD_LIT = re.compile(r"[A-Za-z0-9_./:=@%+,!-]+")
DQ_BODY = re.compile(r"(?:[A-Za-z0-9_./:=@%+,-]|\$\{[A-Za-z_][A-Za-z0-9_]*\}|\$[A-Za-z_][A-Za-z0-9_]*)*")
VAR_REF = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}|\$[A-Za-z_][A-Za-z0-9_]*")


def shell_words(cmd: str):
    """The words of ONE simple command: [(text, quoted: bool)…], or a str saying why the command is not a plain word list
    (a metacharacter, a single quote, a mixed-quoting word, a `$` outside double quotes, an assignment prefix)."""
    words: list[tuple[str, bool]] = []
    i, n = 0, len(cmd)
    while i < n:
        c = cmd[i]
        if c in " \t":
            i += 1
            continue
        # an optional unquoted literal prefix (e.g. `-Dkey=`), then optionally ONE double-quoted part closing the word
        e = i
        while e < n and cmd[e] not in ' \t"':
            e += 1
        prefix = cmd[i:e]
        if e < n and cmd[e] == '"':
            if prefix and (not WORD_LIT.fullmatch(prefix) or "$" in prefix):
                return f"a word whose unquoted part is outside the template alphabet ({prefix!r})"
            q_end = e + 1
            while q_end < n and cmd[q_end] != '"':
                if cmd[q_end] == BS:
                    return "a backslash escape inside a double-quoted word"
                q_end += 1
            if q_end >= n:
                return "an unterminated double quote"
            body = cmd[e + 1:q_end]
            if q_end + 1 < n and cmd[q_end + 1] not in " \t":
                return f"a word that continues after its double-quoted part ({cmd[i:q_end + 2]!r})"
            if not DQ_BODY.fullmatch(body):
                return f"a double-quoted word with characters outside the template alphabet or a `$` form other than ${{VAR}}/$VAR ({body!r})"
            words.append((prefix + body, True))
            i = q_end + 1
            continue
        w = prefix
        if "'" in w:
            return "a single quote (single-quoted words are not in any template)"
        if re.search(r"[;&|<>`(){}*?\[\]~#\\]", w) or "$" in w or w == "!":
            return f"a shell metacharacter, glob or unquoted `$` in {w!r} — only plain words and double-quoted \"…${{VAR}}…\" values are accepted"
        if not WORD_LIT.fullmatch(w):
            return f"a word outside the template alphabet ({w!r})"
        words.append((w, False))
        i = e
    if not words:
        return "an empty command"
    if "=" in words[0][0] and not words[0][0].startswith("-"):
        return "an assignment prefix before the command (values reach a dedicated effect step through the environment, not an inline assignment)"
    return words


def is_lit(w: tuple[str, bool]) -> bool:
    return "$" not in w[0]


def is_val(w: tuple[str, bool]) -> bool:
    """A literal, or a double-quoted value — never one that begins with `-` (a parser could read it as an option)."""
    return (is_lit(w) or w[1]) and not w[0].startswith("-")


def lit_path(w: tuple[str, bool], allow_dot: bool = True, allow_trailing_slash: bool = True) -> str | None:
    """A LITERAL repository-relative path (no variable, no leading /, ~ or -, no `..` or `.` components except a lone
    `.`), normalised without a trailing slash; None otherwise."""
    if not is_lit(w):
        return None
    p = w[0]
    if allow_trailing_slash and p.endswith("/") and len(p) > 1:
        p = p.rstrip("/")
    if p == ".":
        return "." if allow_dot else None
    if p.startswith("./"):
        p = p[2:]
    if not p or p.startswith(("/", "~", "-")):
        return None
    if any(c in ("", ".", "..") for c in p.split("/")):
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.][A-Za-z0-9_.@+-]*(?:/[A-Za-z0-9_.][A-Za-z0-9_.@+-]*)*", p):
        return None
    return p


def parse_effect_template(kind: str, cmd: str) -> tuple[list[str], str | None]:
    """(consumed literal paths, None) when `cmd` (one simple command) matches the fixed template for `kind`; otherwise
    ([], why)."""
    ws = shell_words(cmd)
    if isinstance(ws, str):
        return [], ws
    if kind == "mvn":
        return parse_mvn(ws)
    if kind == "docker":
        return parse_docker_build(ws)
    if kind == "rsync":
        return parse_rsync(ws)
    if kind == "scp":
        return parse_scp(ws)
    if kind == "helm":
        return parse_helm(ws)
    if kind == "ansible":
        return parse_ansible(ws)
    if kind == "script":
        return parse_script(ws)
    return [], f"no template exists for '{kind}'"


MVN_FLAGS = {"-B", "--batch-mode", "-q", "--quiet", "-e", "--errors", "-ntp", "--no-transfer-progress", "-o", "--offline", "-U", "--update-snapshots",
             "-am", "--also-make", "-amd", "--also-make-dependents", "-N", "--non-recursive", "-V", "--show-version", "-fae", "--fail-at-end",
             "-ff", "--fail-fast", "-fn", "--fail-never", "-X", "--debug", "-nsu", "--no-snapshot-updates", "-C", "--strict-checksums"}
MVN_PATH_OPTS = {"-f", "--file", "-s", "--settings", "-gs", "--global-settings", "-t", "--toolchains"}
MVN_VAL_OPTS = {"-pl", "--projects", "-T", "--threads", "-rf", "--resume-from", "-P", "--activate-profiles"}
MVN_PHASES = {"clean", "validate", "initialize", "generate-sources", "process-sources", "generate-resources", "process-resources", "compile",
              "process-classes", "generate-test-sources", "process-test-sources", "generate-test-resources", "process-test-resources", "test-compile",
              "process-test-classes", "test", "prepare-package", "package", "pre-integration-test", "integration-test", "post-integration-test",
              "verify", "install", "deploy"}
MVN_SAFE_PLUGIN_GOALS = {"failsafe:integration-test", "failsafe:verify", "surefire:test", "help:evaluate", "help:effective-pom", "dependency:resolve",
                         "dependency:go-offline", "dependency:tree", "enforcer:enforce", "jacoco:report", "jacoco:check", "spotless:check", "checkstyle:check"}
PL_VALUE = re.compile(r"!?[A-Za-z0-9_.][A-Za-z0-9_./,!-]*")


def parse_mvn(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("mvn", False):
        return [], "the command is not a plain `mvn`"
    consumed: list[str] = ["."]
    pom_dir = None
    goals: list[str] = []
    i = 1
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and t in MVN_FLAGS:
            i += 1
            continue
        if not w[1] and t in MVN_PATH_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a literal path"
            p = lit_path(ws[i + 1], allow_dot=False)
            if p is None:
                return [], f"`{t}` must name a literal repository path (got {ws[i + 1][0]!r})"
            if t in ("-f", "--file"):
                pom_dir = re.sub(r"/pom\.xml$", "", p) if p.endswith("pom.xml") else p
            else:
                consumed.append(p)
            i += 2
            continue
        if not w[1] and t in MVN_VAL_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a value"
            v = ws[i + 1]
            if not is_val(v) or (is_lit(v) and not PL_VALUE.fullmatch(v[0])):
                return [], f"`{t}` takes a literal module/profile list or one \"${{VAR}}\" (got {v[0]!r})"
            i += 2
            continue
        if not w[1] and re.fullmatch(r"-P[A-Za-z0-9_,.!-]+", t):
            i += 1
            continue
        if re.fullmatch(r"-D[A-Za-z_][A-Za-z0-9_.-]*(?:=.*)?", t):
            key, _, val = t.partition("=")
            if val and not w[1] and "$" in val:
                return [], f"a `-D` value with a `$` must be double-quoted as a whole (got {t!r})"
            if w[1] and not DQ_BODY.fullmatch(val):
                return [], f"a `-D` value outside the template alphabet ({t!r})"
            i += 1
            continue
        if w[1]:
            return [], f"a double-quoted word that is not a `-D`/`-pl`/`-P` value ({t!r})"
        if t in MVN_PHASES or t in MVN_SAFE_PLUGIN_GOALS:
            goals.append(t)
            i += 1
            continue
        if ":" in t:
            return [], f"plugin goal {t!r} is not in the template's goal list (a goal that rewrites sources or publishes through a plugin is refused)"
        return [], f"{t!r} is not an option or goal of the mvn template"
    if not goals:
        return [], "no goal"
    if pom_dir is not None:
        consumed = [pom_dir] + [c for c in consumed if c != "."]
    return consumed, None


DOCKER_VAL_OPTS = {"--builder", "--platform", "-t", "--tag", "--label", "--build-arg", "--metadata-file", "--iidfile", "--progress", "--target",
                   "--network", "--cache-from", "--cache-to", "--provenance", "--sbom", "--attest", "--output", "-o"}
DOCKER_FLAGS = {"--push", "--load", "--no-cache", "--pull", "--quiet", "-q", "--rm", "--force-rm", "--no-cache-filter"}
DOCKER_PATH_OPTS = {"-f", "--file"}
OUTPUT_LIT = re.compile(r"[A-Za-z0-9_.,=/:@-]+")


def parse_docker_build(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("docker", False):
        return [], "the command is not a plain `docker`"
    if [w[0] for w in ws[1:3]] == ["buildx", "build"] and not ws[1][1] and not ws[2][1]:
        i = 3
    elif len(ws) > 1 and ws[1] == ("build", False):
        i = 2
    else:
        return [], "not `docker build` / `docker buildx build`"
    consumed: list[str] = []
    context = None
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and t in DOCKER_FLAGS:
            i += 1
            continue
        if not w[1] and t in DOCKER_PATH_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a literal path"
            p = lit_path(ws[i + 1], allow_dot=False)
            if p is None:
                return [], f"`{t}` must name a literal repository path (got {ws[i + 1][0]!r})"
            consumed.append(p)
            i += 2
            continue
        if not w[1] and t in DOCKER_VAL_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a value"
            v = ws[i + 1]
            if not is_val(v):
                return [], f"`{t}` takes a literal or one double-quoted \"…${{VAR}}…\" value (got {v[0]!r})"
            if t in ("--output", "-o") and is_lit(v) and (not OUTPUT_LIT.fullmatch(v[0]) or "dest=" in v[0]):
                return [], f"`--output` must be an image/registry export spec without a destination path (got {v[0]!r})"
            i += 2
            continue
        if not w[1] and re.fullmatch(r"--(?:progress|provenance|sbom|platform|builder|output|network|target)=[A-Za-z0-9_.,=/:@-]+", t):
            i += 1
            continue
        if t.startswith("-") and not w[1]:
            return [], f"{t!r} is not an option of the docker build template (`--build-context`, `--secret`, `--ssh`, `-v` and any other option are refused)"
        if w[1] and "$" in t:
            return [], f"the build context must be a literal path (got {t!r})"
        if context is not None:
            return [], f"a second positional argument ({t!r}); the template takes exactly one context"
        p = lit_path(w)
        if p is None:
            return [], f"the build context must be a literal repository path (got {t!r})"
        context = p
        i += 1
    if context is None:
        return [], "no build context"
    return [context] + consumed, None


RSYNC_SHORT = re.compile(r"-[avzrlptgoDqcnhHAXxu]+")
RSYNC_FLAGS = {"--delete", "--delete-excluded", "--delete-after", "--delete-before", "--archive", "--compress", "--verbose", "--quiet", "--dry-run",
               "--checksum", "--omit-dir-times", "--no-perms", "--no-owner", "--no-group", "--no-times", "--partial", "--progress", "--stats",
               "--recursive", "--links", "--perms", "--times", "--human-readable", "--prune-empty-dirs", "--itemize-changes", "--copy-links"}
RSYNC_VAL_OPTS = {"--exclude", "--include", "--chmod", "--timeout", "--bwlimit", "--max-size", "--min-size"}
RSYNC_RSH = re.compile(r"ssh(?: -o [A-Za-z]+=[A-Za-z0-9._@+-]+| -p [0-9]+| -i [A-Za-z0-9_./~-]+)*")


def parse_rsync(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("rsync", False):
        return [], "the command is not a plain `rsync`"
    consumed: list[str] = []
    positional: list[tuple[str, bool]] = []
    i = 1
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and (RSYNC_SHORT.fullmatch(t) or t in RSYNC_FLAGS):
            i += 1
            continue
        if not w[1] and t in RSYNC_VAL_OPTS:
            if i + 1 >= len(ws) or not is_lit(ws[i + 1]):
                return [], f"`{t}` takes a literal value"
            i += 2
            continue
        if not w[1] and re.fullmatch(r"--(?:exclude|include|chmod|timeout|bwlimit|max-size|min-size)=[A-Za-z0-9_./*?,+=:-]+", t):
            i += 1
            continue
        if not w[1] and t in ("-e", "--rsh"):
            if i + 1 >= len(ws) or not is_lit(ws[i + 1]) or not RSYNC_RSH.fullmatch(ws[i + 1][0]):
                return [], "`-e`/`--rsh` must be a literal `ssh [-o K=V] [-p N] [-i path]` (a remote-shell command is the one place rsync would run something else)"
            i += 2
            continue
        if not w[1] and t.startswith("--rsh="):
            if not RSYNC_RSH.fullmatch(t[len("--rsh="):]):
                return [], "`--rsh=` must be a literal `ssh [-o K=V] [-p N] [-i path]`"
            i += 1
            continue
        if t.startswith("-") and not w[1]:
            return [], f"{t!r} is not an option of the rsync template (`--rsync-path`, `--files-from`, `--filter`, `--link-dest`, `--remove-source-files` and any other option are refused)"
        positional.append(w)
        i += 1
    if len(positional) < 2:
        return [], "rsync needs at least one literal source and one destination"
    for src in positional[:-1]:
        p = lit_path(src)
        if p is None or ":" in src[0].split("/")[0]:
            return [], f"every rsync source must be a literal path inside the workspace (got {src[0]!r})"
        consumed.append(p)
    dest = positional[-1]
    if not is_val(dest) or ":" not in dest[0]:
        return [], f"the rsync destination must be a remote `host:path` (a literal or one double-quoted \"…${{VAR}}…\" value; got {dest[0]!r}) — the template ships a verified tree, it does not write into the workspace"
    return consumed, None


SCP_VAL_OPTS = {"-o", "-P", "-i", "-F", "-J", "-l", "-c", "-S"}
SCP_FLAGS = {"-q", "-r", "-p", "-C", "-B", "-4", "-6", "-v", "-3"}


def parse_scp(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("scp", False):
        return [], "the command is not a plain `scp`"
    consumed: list[str] = []
    positional: list[tuple[str, bool]] = []
    i = 1
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and t in SCP_FLAGS:
            i += 1
            continue
        if not w[1] and t in SCP_VAL_OPTS:
            if i + 1 >= len(ws) or not is_lit(ws[i + 1]):
                return [], f"`{t}` takes a literal value"
            if t == "-S":
                return [], "`-S` (an ssh program of the caller's choosing) is not in the scp template"
            i += 2
            continue
        if t.startswith("-") and not w[1]:
            return [], f"{t!r} is not an option of the scp template"
        positional.append(w)
        i += 1
    if len(positional) < 2:
        return [], "scp needs at least one literal source and one destination"
    for src in positional[:-1]:
        p = lit_path(src, allow_dot=False)
        if p is None or ":" in src[0].split("/")[0]:
            return [], f"every scp source must be a literal path inside the workspace (got {src[0]!r})"
        consumed.append(p)
    dest = positional[-1]
    if not is_val(dest) or ":" not in dest[0]:
        return [], f"the scp destination must be a remote `host:path` (a literal or one double-quoted \"…${{VAR}}…\" value; got {dest[0]!r})"
    return consumed, None


HELM_VAL_OPTS = {"--namespace", "-n", "--set", "--set-string", "--timeout", "--version", "--kubeconfig", "--kube-context", "--repo", "--history-max"}
HELM_FLAGS = {"--install", "--wait", "--atomic", "--create-namespace", "--cleanup-on-fail", "--dry-run", "--debug", "--reuse-values", "--reset-values"}
HELM_PATH_OPTS = {"-f", "--values"}


def parse_helm(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("helm", False) or len(ws) < 2 or ws[1][1] or ws[1][0] not in ("install", "upgrade"):
        return [], "not `helm install` / `helm upgrade`"
    consumed: list[str] = []
    positional: list[tuple[str, bool]] = []
    i = 2
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and t in HELM_FLAGS:
            i += 1
            continue
        if not w[1] and t in HELM_PATH_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a literal path"
            p = lit_path(ws[i + 1], allow_dot=False)
            if p is None:
                return [], f"`{t}` must name a literal repository path (got {ws[i + 1][0]!r})"
            consumed.append(p)
            i += 2
            continue
        if not w[1] and t in HELM_VAL_OPTS:
            if i + 1 >= len(ws) or not is_val(ws[i + 1]):
                return [], f"`{t}` takes a literal or one double-quoted \"…${{VAR}}…\" value"
            i += 2
            continue
        if t.startswith("-") and not w[1]:
            return [], f"{t!r} is not an option of the helm template (`--post-renderer` and any other option are refused)"
        positional.append(w)
        i += 1
    if len(positional) != 2:
        return [], "helm install/upgrade takes exactly a release name and a chart"
    if not is_val(positional[0]):
        return [], "the release name must be a literal or one double-quoted \"${VAR}\""
    chart = positional[1]
    if not is_lit(chart):
        return [], f"the chart must be a literal (got {chart[0]!r})"
    c = chart[0]
    if c.startswith("./") or c.startswith("/") or c.startswith("~") or c.startswith(".."):
        p = lit_path(chart, allow_dot=False)
        if p is None:
            return [], f"a local chart must be a literal repository path (got {c!r})"
        consumed.insert(0, p)
    elif "/" in c and not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", c) and not c.startswith("oci://"):
        return [], f"chart {c!r} is neither a repository chart reference (repo/chart, oci://…) nor a local ./path"
    elif "/" in c and not c.startswith("oci://"):
        # `repo/chart` is a repository reference UNLESS that path exists in the tree — then it is a local chart and is consumed
        consumed.insert(0, c)
    return consumed, None


ANSIBLE_VAL_OPTS = {"-i", "--inventory", "-e", "--extra-vars", "--tags", "-t", "--skip-tags", "-l", "--limit", "-u", "--user", "--vault-password-file", "--connection", "-c"}
ANSIBLE_FLAGS = {"--check", "--diff", "-v", "-vv", "-vvv", "-vvvv", "--flush-cache", "--force-handlers", "-K", "--ask-become-pass", "-b", "--become"}


def parse_ansible(ws) -> tuple[list[str], str | None]:
    if ws[0] != ("ansible-playbook", False):
        return [], "the command is not a plain `ansible-playbook`"
    consumed: list[str] = []
    playbook = None
    i = 1
    while i < len(ws):
        w = ws[i]
        t = w[0]
        if not w[1] and t in ANSIBLE_FLAGS:
            i += 1
            continue
        if not w[1] and t in ANSIBLE_VAL_OPTS:
            if i + 1 >= len(ws):
                return [], f"`{t}` needs a value"
            v = ws[i + 1]
            if t in ("-i", "--inventory", "--vault-password-file"):
                p = lit_path(v, allow_dot=False)
                if p is None:
                    return [], f"`{t}` must name a literal repository path (got {v[0]!r})"
                consumed.append(p)
            elif t in ("-e", "--extra-vars"):
                if not is_val(v) or "=" not in v[0] or v[0].startswith("@"):
                    return [], f"`-e` takes one `key=value` (a literal or a double-quoted \"key=${{VAR}}\"), never a file (got {v[0]!r})"
            elif not is_val(v):
                return [], f"`{t}` takes a literal or one double-quoted value"
            i += 2
            continue
        if t.startswith("-") and not w[1]:
            return [], f"{t!r} is not an option of the ansible-playbook template"
        if playbook is not None:
            return [], "the template takes exactly one playbook"
        p = lit_path(w, allow_dot=False)
        if p is None or not p.endswith((".yml", ".yaml")):
            return [], f"the playbook must be a literal repository path ending in .yml (got {t!r})"
        playbook = p
        i += 1
    if playbook is None:
        return [], "no playbook"
    return [playbook] + consumed, None


def parse_script(ws) -> tuple[list[str], str | None]:
    """`[bash|sh|python3 [-x|-e|-u|-eu|-ux|-xe]] <literal repository script> [literal or "${VAR}" arguments]`, or the
    script itself when it is executable. The script is the consumed source; it runs from the verified tree."""
    i = 0
    if ws[0][0] in ("bash", "sh", "python3", "python") and not ws[0][1]:
        i = 1
        while i < len(ws) and not ws[i][1] and re.fullmatch(r"-[xeuv]+", ws[i][0]):
            i += 1
    elif ws[0][0] in ("source", ".") and not ws[0][1]:
        return [], "a script that runs effects may not be sourced into the effect step's shell (`source`/`.`); run it as its own dedicated step"
    if i >= len(ws):
        return [], "no script path"
    p = lit_path(ws[i], allow_dot=False)
    if p is None or not p.endswith(SCRIPT_EXT):
        return [], f"the script must be a literal repository path (got {ws[i][0]!r})"
    for w in ws[i + 1:]:
        if not is_val(w):
            return [], f"script arguments must be literals or double-quoted \"…${{VAR}}…\" values (got {w[0]!r})"
        if w[0].startswith("-") and not w[1] and not re.fullmatch(r"--?[A-Za-z0-9][A-Za-z0-9_-]*(?:=[A-Za-z0-9_./:@%+,-]*)?", w[0]):
            return [], f"script argument {w[0]!r} is outside the template alphabet"
    return [p], None


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
def check_in_scope(path: str, entry: dict, guard_hash: str, root_dir: str = ".") -> list[str]:
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
            #     `return`, a skipped branch, a caught error or a closure that skips the check has to
            #     leave that block, and so skips the trigger too. A check in an earlier sibling
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

    def safe_rel_or_dot(path: str) -> bool:
        # a verify --dir may be "." (the enclosing directory itself); otherwise a plain relative path with no
        # .., leading /, ~ or variable component.
        return path == "." or safe_rel(path)

    def stmt_wrapped(start: int, end: int) -> tuple[int, int]:
        """Grow a statement's span outward through dir('literal') { <only this statement> } wrappers (for adjacency);
        stop at any other block. Directories are resolved separately, by resolve_dirs()."""
        while True:
            blk = g.innermost(start)
            if blk is None:
                return start, end
            b = g.blocks[blk]
            if not re.fullmatch(r"dir\('([^']*)'\)", b.header):
                return start, end
            if (g.code_only(b.open + 1, start) + g.code_only(end, b.close)).strip():
                return start, end
            start = text.rfind("dir(", 0, b.open)
            end = b.close + 1

    def resolve_dirs(pos: int) -> tuple[list[str], str | None]:
        """The directory pos runs in, folded through EVERY enclosing block (script, withEnv, timeout, if, a dir()
        holding other statements, …): the dir('<literal>') literals outermost first, or why it cannot be resolved —
        dir(<anything but one literal>), ws(…), node(…), or a literal with .., ., an absolute or ~ form."""
        dirs: list[str] = []
        for bi in g.ancestors(pos):
            h = g.blocks[bi].header
            m = re.fullmatch(r"dir\('([^']*)'\)", h)
            if m:
                if not safe_rel(m.group(1)):
                    return dirs, f"dir('{m.group(1)}') is not a plain relative path (no .., ., variables, absolute or ~ forms)"
                dirs.append(m.group(1))
            elif re.match(r"(?:ws|node)\b", h):
                return dirs, f"it sits under `{h[:40]}`, which changes the workspace"
            elif re.match(r"dir\b", h) or re.search(r"(?:^|[^\w.])dir\s*\(", h):
                return dirs, f"it sits under `{h[:40]}`, whose directory cannot be resolved to a literal path"
        return dirs, None

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
    bound_guards: dict[str, tuple[int, int, str]] = {}  # resolved path -> (offset after its guard line, guard line, permission var)
    acquisition_spans: list[tuple[int, int]] = []
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
        acquisition_spans.append((a_start, a_end))
        o_start, o_end = stmt_wrapped(a_start, a_end)
        dirs, why = resolve_dirs(a_start)
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
            go_start, go_end = stmt_wrapped(t_start, g.blocks[tb].close + 1 if tb is not None else sh_pos)
            gdirs, gwhy = resolve_dirs(sh_pos)
            if not dm2:
                why_not = "the guard after the acquisition is not the dedicated template"
            elif gwhy:
                why_not = gwhy
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
                    bound_guards[resolved] = (offs[gl + 1], gl, dm2.group("own") + "_PERMITTED_SHA")
        if not rebound:
            problems.append(f"line {i + 1}: '{resolved}' acquired after the guard is not re-bound by the dedicated guard step for that same resolved path, immediately after its acquisition step: {why_not}")
    if entry["contracts"] and not contracts_seen:
        problems.append(f"manifest names contracts={entry['contracts']} but no acquisition of it was found after the guard")

    # 9b. Provenance verification, structurally. The guard proves HEAD == permitted at one moment; the FILES an effect
    # consumes later can still be replaced while HEAD stays the permitted commit. Two things close that, together:
    #   (i)  every source-consuming effect is a DEDICATED STEP whose whole shell body is that one command, read against
    #        a fixed per-command template (an explicit option list; the consumed sources as literals) — so nothing can run
    #        inside the effect's own step between the verification and the consumption, and
    #   (ii) that step is immediately preceded, in the same block, by the dedicated verify-permitted-tree step for EVERY
    #        checkout a consumed path resolves into (the primary workspace, or the nested checkout containing it) —
    #        verify-permitted-tree.sh re-checks HEAD == permitted AND a clean tree at run time, whatever the writer was.
    # Effects are found at COMMAND POSITION in a shell-aware view of every string (messages, comments, here-documents
    # and remote ssh arguments are data, not commands), so an effect hidden in any other shape of step is refused too.
    def fold(dirs: list[str], rel: str) -> str:
        return "/".join(list(dirs) + [c for c in rel.split("/") if c not in ("", ".")])

    def verify_at(line_idx: int):
        content = g.sh_single_quoted_step(offs[line_idx], offs[line_idx] + len(lines[line_idx]))
        if content is None:
            return None
        return VERIFY_STEP.match("sh '" + content + "'")

    # Every dedicated verify step: (resolved dir, permission var, the statement's start/end offsets, its parent block).
    # It must sit alone inside a `timeout(time: N, unit: 'MINUTES') { … }` block (the guard's deadline template), inside a
    # stage's steps and never under `parallel`. It need not be unskippable on its own: it covers ONLY the effect that is
    # the very next statement of the same block, so whatever control flow skips the verify skips that effect with it.
    verify_steps: list[dict] = []
    for li in range(g_hi, len(lines)):
        vm = verify_at(li)
        if vm is None:
            continue
        sh_pos = offs[li] + first_code_col(li)
        gdirs, gwhy = resolve_dirs(sh_pos)
        if gwhy or not safe_rel_or_dot(vm.group("dir")):
            problems.append(f"line {li + 1}: a verify-permitted-tree step whose directory cannot be resolved to a literal path")
            continue
        rv = fold(gdirs, vm.group("dir"))
        tb = g.innermost(sh_pos)
        header = g.blocks[tb].header if tb is not None else ""
        if not re.fullmatch(r"timeout\(time: [0-9]+, unit: 'MINUTES'\)", header) or g.code_only(g.blocks[tb].open + 1, sh_pos).strip() or g.code_only(offs[li] + len(lines[li]), g.blocks[tb].close).strip():
            problems.append(f"line {li + 1}: the verify-permitted-tree step for '{rv or '.'}' is not alone inside a timeout(time: N, unit: 'MINUTES') deadline block")
            continue
        anc = g.ancestors(sh_pos)
        if not any(g.blocks[a].header == "steps" for a in anc):
            problems.append(f"line {li + 1}: the verify-permitted-tree step for '{rv or '.'}' is not inside a stage's steps")
            continue
        if any(re.match(r"parallel\b", g.blocks[a].header) for a in anc):
            problems.append(f"line {li + 1}: the verify-permitted-tree step for '{rv or '.'}' sits under `parallel` — a concurrent branch could change the tree between the verify and the effect")
            continue
        t_start = text.rfind("timeout(", 0, g.blocks[tb].open)
        verify_steps.append({"dir": rv, "var": vm.group("own"), "start": t_start, "end": g.blocks[tb].close + 1,
                             "parent": g.blocks[tb].parent, "line": li})

    # a git command that MOVES a checkout's HEAD or worktree (as opposed to reading it). `fetch`/`clone` alone update
    # refs or create a checkout (the latter only inside the dedicated acquisition step) and do not move an existing
    # worktree, so they are not here — the worktree-moving verb that would follow (reset/checkout/…) is.
    GIT_MOVE = re.compile(r"\bgit\b(?:\s+-\S+(?:\s+\S+)?)*\s+(pull|checkout|switch|restore|reset|merge|rebase|am|cherry-pick|revert|submodule|stash|clean|read-tree|checkout-index)\b")
    nested_dirs = sorted((p for p in bound_guards), key=len, reverse=True)

    def attribute(consumed: str) -> str:
        """The checkout a consumed path belongs to: the deepest nested guarded checkout it is inside or equal to, else the
        primary workspace ('')."""
        if consumed in ("", "."):
            return ""
        for d in nested_dirs:
            if consumed == d or consumed.startswith(d + "/"):
                return d
        return ""

    # Scripts the pipeline itself WRITES from a here-document in this file (`cat > <path> <<'TAG' … TAG`): a later step that
    # runs or sources one is judged by that body. A script that is neither in the tree nor generated so is refused.
    generated: dict[str, str] = {}
    for q_, gs0, ge0 in g.strings:
        gbody = groovy_decode(text[gs0:ge0], gstring=q_ in ('"', '"""'))[0]
        for gm_ in re.finditer(r"(?:cat|tee)\s*>\s*\"?(?:\$\{?WORKSPACE\}?/)?([A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:sh|bash|py))\"?\s*<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\2[^\n]*\n", gbody):
            tail = gbody[gm_.end():]
            out_lines = []
            for ln_ in tail.split("\n"):
                if ln_.strip() == gm_.group(3):
                    break
                out_lines.append(ln_)
            generated[os.path.normpath(gm_.group(1))] = "\n".join(out_lines)

    def make_resolver(base: list[str]):
        """Reads a repository-relative script as the step's shell would find it: relative to the step's resolved dir()
        (a script inside a nested checkout is not in the tree the validator sees, so it is refused, never guessed)."""
        def r(path: str):
            rel = os.path.normpath(os.path.join(*base, path) if base else path)
            found_, body_ = resolver(rel)
            if not found_ and rel in generated:
                return True, generated[rel]
            return found_, body_
        return r

    def resolver(path: str):
        full = os.path.join(root_dir, path)
        if not os.path.isfile(full):
            return False, None
        try:
            with open(full, encoding="utf-8", errors="replace") as fh:
                return True, fh.read()
        except OSError:
            return False, None

    def dedicated_effect_step(s_tuple):
        """When the string literal s_tuple (quote, body_start, body_end) is the SOLE argument of a plain `sh` statement whose
        body is exactly ONE command (a one-line single-quoted literal, or a triple-quoted block holding one command with
        `\\`-newline continuations and nothing else), (statement start offset, the command text); else (None, why)."""
        q, s0, e0 = s_tuple
        open_at = s0 - len(q)
        line_start = text.rfind("\n", 0, open_at) + 1
        head = text[line_start:open_at]
        if not re.fullmatch(r"\s*sh\s+", head) or not g.is_code(line_start + len(head) - len(head.lstrip())):
            return None, "it is not a plain `sh '…'` / `sh '''…'''` step of its own (no `sh(`, no returnStatus/returnStdout, no prefix concatenation)"
        after = text[e0 + len(q):]
        nl = after.find("\n")
        tail = after if nl < 0 else after[:nl]
        if tail.strip():
            return None, "something follows the step's shell literal on its line (a concatenation or another argument)"
        if q == '"' or q == '"""':
            return None, "the body is a GString (double-quoted); a dedicated effect step is a single-quoted literal"
        body = groovy_decode(text[s0:e0], gstring=q in ('"', '"""'))[0]
        if q == "'''":
            if "#" in body:
                return None, "a `#` in the body (no comments: the body is exactly one command)"
            joined = re.sub(r"[ \t]*\\\n[ \t]*", " ", body)
            cmd_lines = [l.strip() for l in joined.split("\n") if l.strip()]
            if len(cmd_lines) != 1:
                return None, f"the body holds {len(cmd_lines)} lines, not exactly one command (a preparation belongs in an earlier step that is not an effect)"
            return line_start + len(head) - len(head.lstrip()), cmd_lines[0]
        if "\n" in body:
            return None, "a newline inside the literal"
        return line_start + len(head) - len(head.lstrip()), body

    def verify_chain(step_start: int) -> list[dict]:
        """The dedicated verify steps that immediately precede step_start in its block (nothing but whitespace or comments
        between each and the next), nearest first."""
        e_block = g.innermost(step_start)
        chain: list[dict] = []
        cursor = step_start
        while True:
            best = None
            for v in verify_steps:
                if v["parent"] != e_block or v["end"] > cursor or g.code_only(v["end"], cursor).strip():
                    continue
                if best is None or v["end"] > best["end"]:
                    best = v
            if best is None:
                return chain
            chain.append(best)
            cursor = best["start"]

    # Directories the pipeline itself generates OUTSIDE the workspace: an environment{} variable bound to a literal
    # absolute path with no `..` and no WORKSPACE, every interpolation in it sanitised by
    # .replaceAll('[^A-Za-z0-9-]', '-'). An scp or rsync whose every source is a plain file directly under such a variable
    # ships generated files, not a checkout, so it consumes no source and needs no verify step.
    abs_env: set[str] = set()
    eb = find_block(lines, re.compile(r"^\s*environment\s*\{"))
    while eb is not None:
        for k in range(eb[0], eb[1] + 1):
            em = re.match(r"""^\s*([A-Z][A-Z0-9_]*)\s*=\s*(?:"(/[^"]*)"|'(/[^']*)')\s*$""", lines[k])
            if not em:
                continue
            val = em.group(2) if em.group(2) is not None else em.group(3)
            sanitised = re.findall(r"\$\{\((?:[^{}]*)\)\.replaceAll\('\[\^A-Za-z0-9-\]', '-'\)\}", val)
            if ".." not in val and "WORKSPACE" not in val and val.count("${") == len(sanitised) and "$" not in re.sub(r"\$\{\((?:[^{}]*)\)\.replaceAll\('\[\^A-Za-z0-9-\]', '-'\)\}", "", val):
                abs_env.add(em.group(1))
        eb = find_block(lines, re.compile(r"^\s*environment\s*\{"), eb[1] + 1)

    def env_absolute_sources_only(eff: dict) -> bool:
        if eff["kind"] not in ("scp", "rsync"):
            return False
        try:
            toks = shlex.split(eff["cmd"])
        except ValueError:
            return False
        ops, skip = [], False
        for t in toks[1:]:
            if skip:
                skip = False
                continue
            if t.startswith("-"):
                skip = t in ("-o", "-P", "-i", "-F", "-J", "-l", "-c", "-e", "--exclude", "--include", "--chmod", "--rsh")
                continue
            ops.append(t)
        if len(ops) < 2:
            return False
        for src in ops[:-1]:
            sm = re.match(r"^\$\{?([A-Z][A-Z0-9_]*)\}?/[A-Za-z0-9_][A-Za-z0-9_.-]*$", src)
            if not sm or sm.group(1) not in abs_env:
                return False
        return True

    seen_effects: set[int] = set()
    # EVERY string literal in the file is read — stage steps, post{} blocks, top-level methods and constants alike — except
    # the parameters{} block's descriptions. An effect found anywhere must be a dedicated, verified step.
    pblk = find_block(lines, re.compile(r"^\s*parameters\s*\{"))
    p_lo, p_hi = (offs[pblk[0]], offs[pblk[1] + 1]) if pblk else (-1, -1)
    for s_tuple in g.strings:
        q, s0, e0 = s_tuple
        if p_lo <= s0 < p_hi or any(a <= s0 < b for a, b in g.comments):
            continue
        raw_body = text[s0:e0]
        body, bmap = groovy_decode(raw_body, gstring=q in ('"', '"""'))
        if CANONICAL_LITERAL.fullmatch(body):
            continue   # the guard / verify / downstream-check literals: judged by their own templates (rules 3, 6, 7, 9, 9b)
        sdirs, _swhy = resolve_dirs(s0)
        # the literal argument of an `sh` step is read STRICTLY (a command name computed at run time is refused); any other
        # string (a Groovy constant, a message, a value passed to `sh` later) is read for effects it names
        head_code = g.code_only(max(0, s0 - len(q) - 400), s0 - len(q))
        strict = bool(re.search(r"\bsh\s*(?:\(\s*(?:\w+\s*:\s*[^,()]*,\s*)*(?:script\s*:\s*)?)?(?:[A-Za-z_][\w.]*\s*\+\s*)*$", head_code))
        for eff in effect_commands(body, make_resolver(sdirs), strict):
            pos = s0 + (bmap[eff["pos"]] if eff["pos"] < len(bmap) else len(raw_body))
            if env_absolute_sources_only(eff):
                continue
            ln = text.count("\n", 0, pos) + 1
            if pos in seen_effects:
                continue
            seen_effects.add(pos)
            head = f"line {ln}: a source-consuming effect (`{eff['cmd'][:60]}`)"
            if eff.get("why"):
                problems.append(f"{head} {eff['why']}")
                continue
            step_start, cmd_or_why = dedicated_effect_step(s_tuple)
            if step_start is None:
                problems.append(f"{head} is not a DEDICATED effect step whose shell body is only that command: {cmd_or_why}")
                continue
            consumed_rel, why = parse_effect_template(eff["kind"], cmd_or_why)
            if why:
                problems.append(f"{head} does not fit the fixed `{eff['kind']}` template — {why}; restructure it (fail closed)")
                continue
            base_dirs, base_why = resolve_dirs(step_start)
            if base_why:
                problems.append(f"{head} runs in a directory that cannot be resolved to a literal path: {base_why}")
                continue
            consumed = [fold(base_dirs, c) or "." for c in consumed_rel]
            # docker contexts, rsync/scp sources and chart directories are handed over WHOLE (a Dockerfile COPY, a
            # recursive copy); mvn, ansible and a script read their own project/file.
            wholesale = set(consumed) if eff["kind"] in ("docker", "rsync", "scp", "helm") else set()
            # scp/rsync/script/ansible sources and mvn poms must lie inside the workspace; lit_path already refused
            # absolute, ~, .. and variable forms, so every consumed path resolves to a checkout here.
            needed = {}
            for c in consumed:
                # the checkout the path resolves into, AND every nested checkout lying inside it: a docker context, an
                # rsync/scp source directory or a chart directory hands its whole subtree to the consumer (a Dockerfile
                # COPY, a recursive copy), so each checkout under it is consumed as well.
                for d in [attribute(c)] + [n for n in nested_dirs if c in wholesale and (c in (".", "") or n.startswith(c.rstrip("/") + "/"))]:
                    needed[d] = bound_guards[d][2] if d in bound_guards else "PERMITTED_SHA"
            chain = verify_chain(step_start)
            if not chain:
                where = ", ".join(("the primary checkout '.'" if d == "" else f"the nested checkout '{d}'") for d in sorted(needed))
                problems.append(f"{head} builds, ships or deploys from {where} but no dedicated verify-permitted-tree step immediately precedes it (nothing may run between the verify and the effect) — need timeout {{ sh 'PERMITTED_SHA=\"${{{needed[sorted(needed)[0]]}:-}}\" bash scripts/jenkins/verify-permitted-tree.sh --dir {sorted(needed)[0] or '.'} …' }} immediately before it")
                continue
            for d, want_var in sorted(needed.items()):
                where = "the primary checkout '.'" if d == "" else f"the nested checkout '{d}'"
                hits = [v for v in chain if v["dir"] == d]
                if not hits:
                    problems.append(f"{head} consumes a path inside {where} but the verify step(s) immediately before it re-check {', '.join(repr(v['dir'] or '.') for v in chain)} — every consumed path must be covered by a verify of the checkout it resolves into (a verify of the primary workspace does not vouch for a nested checkout, nor a nested one for its parent)")
                elif not any(v["var"] == want_var for v in hits):
                    problems.append(f"{head}: the verify step for {where} uses PERMITTED_SHA source '{hits[0]['var']}', but this checkout is guarded with '{want_var}' — the verify must re-check the SAME permitted commit the guard bound")

    # The shell text of every `sh` step is READABLE: one string literal, optionally prefixed by a top-level constant that
    # is itself one string literal ending at a line boundary (`sh JDK_SETUP + '''…'''`). Anything else — two literals
    # joined (`'doc' + 'ker build'`), a local variable, a method call — would let a command be assembled at run time from
    # pieces no scan sees whole.
    top_constants: dict[str, str] = {}
    for cm_ in re.finditer(r"(?m)^([A-Z][A-Z0-9_]*)\s*=\s*(?='|\")", text):
        lit_ = next(((q_, s_, e_) for q_, s_, e_ in g.strings if s_ - len(q_) == cm_.end()), None)
        if lit_ is not None and g.is_code(cm_.start()):
            top_constants[cm_.group(1)] = groovy_decode(text[lit_[1]:lit_[2]], gstring=lit_[0] in ('"', '"""'))[0]
    lit_at = {s_ - len(q_): (q_, s_, e_) for q_, s_, e_ in g.strings}
    for shm in re.finditer(r"\bsh\b(?=\s*[('\"A-Za-z_])", text):
        if not g.is_code(shm.start()) or text[max(0, shm.start() - 4):shm.start()].endswith("def "):
            continue
        i = shm.end()
        n_ = len(text)
        while i < n_ and text[i] in " \t":
            i += 1
        paren = i < n_ and text[i] == "("
        if paren:
            close = matched_close(g.code_only(i, min(n_, i + 20000)), 0)
            args = g.code_only(i, i + (close or 0) + 1) if close is not None else ""
            sm_ = re.search(r"\bscript\s*:", args)
            if sm_:
                i = i + sm_.end()
            else:
                i += 1
            while i < n_ and text[i] in " \t\n":
                i += 1
        parts: list[tuple[str, str]] = []
        while True:
            if i in lit_at:
                q_, s_, e_ = lit_at[i]
                parts.append(("lit", text[s_:e_]))
                i = e_ + len(q_)
            else:
                im_ = re.match(r"[A-Za-z_][A-Za-z0-9_.]*(?:\(\))?", text[i:])
                if not im_:
                    break
                parts.append(("id", im_.group(0)))
                i += im_.end()
            k_ = i
            while k_ < n_ and text[k_] in " \t":
                k_ += 1
            if k_ < n_ and text[k_] == "+":
                i = k_ + 1
                while i < n_ and text[i] in " \t\n":
                    i += 1
                continue
            break
        ok_ = (len(parts) == 1 and parts[0][0] == "lit") or (
            len(parts) == 2 and parts[0][0] == "id" and parts[1][0] == "lit" and parts[0][1] in top_constants
            and re.search(r"\n[ \t]*$", top_constants[parts[0][1]]) is not None)
        if not ok_:
            ln = text.count("\n", 0, shm.start()) + 1
            problems.append(f"line {ln}: the shell text of this `sh` step is not one string literal (optionally prefixed by a top-level constant that is one literal ending at a line boundary) — a command assembled from pieces at run time cannot be read: {lines[ln - 1].strip()[:90]}")

    # The permission variables are READ-ONLY. The guard and every verify step read PERMITTED_SHA / X_PERMITTED_SHA from the
    # build's environment; a Groovy `env.X_PERMITTED_SHA = …`, a `withEnv(['X_PERMITTED_SHA=…'])` or an environment{}
    # entry would re-point a later verify at a commit nobody permitted. Only the parameters{} block may define them.
    for pm in re.finditer(r"\b(?:env\.)?((?:[A-Z][A-Z0-9_]*_)?PERMITTED_SHA)\s*=(?!=)|['\"]((?:[A-Z][A-Z0-9_]*_)?PERMITTED_SHA)=", text):
        ppos = pm.start()
        if p_lo <= ppos < p_hi or any(a <= ppos < b for a, b in g.comments):
            continue
        name = pm.group(1) or pm.group(2)
        if pm.group(1) and not g.is_code(ppos):
            continue   # inside a shell string: a shell-local assignment (the dedicated templates' own `PERMITTED_SHA="${X:-}"` prefix) reaches no other step
        if pm.group(2) and not re.search(r"withEnv\s*\(\s*\[[^\]]*$", text[max(0, ppos - 400):ppos]):
            continue
        ln = text.count("\n", 0, ppos) + 1
        problems.append(f"line {ln}: {name} is re-assigned after the parameters{{}} block — a permission variable is read-only (the guard and every verify step read it from the build's environment)")
    envblk = find_block(lines, re.compile(r"^\s*environment\s*\{"))
    while envblk is not None:
        for k in range(envblk[0], envblk[1] + 1):
            if re.match(r"^\s*(?:[A-Z][A-Z0-9_]*_)?PERMITTED_SHA\s*=", lines[k]):
                problems.append(f"line {k + 1}: a permission variable is defined in environment{{}} — it may come only from parameters{{}}")
        envblk = find_block(lines, re.compile(r"^\s*environment\s*\{"), envblk[1] + 1)
    for pm in re.finditer(r"\bparallel\b", text):
        if g.is_code(pm.start()):
            ln = text.count("\n", 0, pm.start()) + 1
            problems.append(f"line {ln}: `parallel` — a concurrent branch could change a checkout between a verify step and the effect it covers; parallel execution is not accepted in a guarded pipeline")

    # A bound checkout must not be MOVED after its guard: a git command that changes HEAD or the worktree, anywhere
    # after the primary guard (in code or in a shell body), is refused unless it is the clone/checkout inside the
    # dedicated acquisition step rule 9 already validates.
    primary_end = offs[g_hi] if g_hi < len(offs) else len(text)
    for gm in GIT_MOVE.finditer(text):
        gpos = gm.start()
        if gpos < primary_end:
            continue
        if any(a <= gpos < b for a, b in acquisition_spans):
            continue
        if any(a <= gpos < b for a, b in g.comments):
            continue
        ln = text.count("\n", 0, gpos) + 1
        problems.append(f"line {ln}: `git {gm.group(1)}` after the guard moves a checkout's HEAD or worktree — a bound source may not be re-moved after it is verified: {lines[ln - 1].strip()[:90]}")

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


def discover_jenkinsfiles(root: str) -> list[str]:
    """Every checked-in Jenkinsfile* in the repository, as repository-relative POSIX paths.

    Repository-WIDE, because a Jenkins job's definition is a path, not a name: `<service>/Jenkinsfile` is
    an ordinary shape and a root-only search reports such a job as "not present" — which reads as "there
    is nothing there", the one answer a scope check must never give by accident.

    `.git` is skipped, and so is anything git reports as ignored (one `git ls-files -oi --directory` call
    at the root, whose output names each ignored file or the top of each ignored directory). That keeps a
    WORKSPACE's build output — `.deps/<a cloned sibling repository>/Jenkinsfile`, an unpacked archive —
    out of the classification, which is about definitions this repository checks in. A repository that is
    not a git checkout (a fixture tree) simply has nothing ignored, and the walk sees everything."""
    ignored_files: set[str] = set()
    ignored_dirs: list[str] = []
    try:
        r = subprocess.run(["git", "-C", root, "ls-files", "-z", "-o", "-i", "--directory", "--exclude-standard"],
                           capture_output=True, text=True)
        if r.returncode == 0:
            for raw in r.stdout.split("\0"):
                if not raw:
                    continue
                if raw.endswith("/"):
                    ignored_dirs.append(raw.rstrip("/"))
                else:
                    ignored_files.add(raw)
    except OSError:
        pass                     # no git on this host: the walk classifies everything it finds
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
        rel_dir = "" if rel_dir == "." else rel_dir
        dirnames[:] = sorted(d for d in dirnames
                             if d != ".git" and f"{rel_dir}/{d}".lstrip("/") not in ignored_dirs)
        for fn in filenames:
            if not fn.startswith("Jenkinsfile"):
                continue
            rel = f"{rel_dir}/{fn}".lstrip("/")
            if rel in ignored_files or not os.path.isfile(os.path.join(dirpath, fn)):
                continue
            found.append(rel)
    return sorted(found)


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
    present = discover_jenkinsfiles(root)
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
                failures.append(f"{f}: not classified in {os.path.relpath(a.manifest, root)} — add it as in (guarded) or out (with the reason); an executable definition nobody classified is one nobody judged")
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
        probs = check_in_scope(os.path.join(root, f), e, guard_hash, root)
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
