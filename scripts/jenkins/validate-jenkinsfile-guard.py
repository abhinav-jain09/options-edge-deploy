#!/usr/bin/env python3
"""Every Jenkins deploy job carries the permitted-commit guard, in canonical form, before its first effect.

Shared byte-for-byte by options-edge-deploy, option-edge-feed-gateway, options-edge-processing and
options-edge; each repository supplies a manifest classifying every Jenkinsfile* in its root:

    <Jenkinsfile> | in|out | options | reason
    options (semicolon-separated): before=<stage>,<stage>   stages allowed before the primary guard
                                   reguard=<stage>          the container stage whose first nested
                                                            stage must be the deploy-workspace re-guard
                                   contracts=<dir>          a second-source clone that must be bound

Deployment Permission Rule (options-edge rule.md): "Jenkins enforces the permitted commit, not the
assistant." A job without the guard is not available to the assistant (rule line 364); a Jenkinsfile
nobody classified fails here.

RULES for every IN-scope Jenkinsfile (textual, over comment-stripped, whitespace-collapsed text —
control flow is matched by CANONICAL FORM, so a guard written in a different but equivalent shape
fails and must be brought back to the form; that is deliberate):
  1. parameters: `string(name: 'PERMITTED_SHA', defaultValue: '', trim: true …)` and
     `string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '<sha256>' …)` whose default equals the
     sha256 of <root>/scripts/jenkins/permitted-sha-guard.sh — the version the guard itself refuses to
     run without, and the value a caller's compatibility check demands of this job's live definition.
  2. `disableRestartFromStage()`.
  3. The primary stage 'Permitted commit guard' is exactly:
       stage('Permitted commit guard') { [agent {…}] steps { script {
         def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh[ --ref "${X:?}"]')
         if (rc != 0) { error(…) }
         env.PERMITTED_SHA_GUARD = 'PASSED' } } }
     — no other statement, no shell operator in the script string (`|| true` is refused), `rc == 0`,
     catchError, warnError, unstable, try: refused.
  4. Every stage before the primary guard is in the manifest's before= set, and no mutation token
     (kubectl, docker build/push/run, build job:, rsync, scp, ssh, launchctl, kafka-topics/configs,
     crontab, helm, ansible-playbook, mvn, git push, vix-unpause.sh) appears between `stages {` and it.
  5. A container stage with its own agent after the primary guard (`agent … stages {`) must open with
     the canonical stage 'Permitted commit guard (deploy workspace)', identical to rule 3 but setting
     `env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'`; that flag is set nowhere else in the file.
  6. A steps-stage with its own agent (not `agent none`, which has no workspace) after the primary
     guard that runs repository code (`scripts/`) or carries a mutation token must begin, after `steps {` and an optional `checkout scm`, with the
     canonical inline re-guard: `script { def V = sh(returnStatus: true, script: 'bash
     scripts/jenkins/permitted-sha-guard.sh') if (V != 0) { error(…) } …` — it works from a second
     workspace, which must be bound before anything runs from it.
  7. Every `build job: '<job>'` is preceded — in the same stage, or the stage immediately before it —
     by the canonical compatibility check `def V = sh(returnStatus: true, script: 'bash
     scripts/jenkins/require-guarded-downstream.sh <job>[ EXTRA…]') if (V != 0) { error(…) }`, and its
     parameter list forwards `PERMITTED_SHA`. A self-trigger (`build job: env.JOB_NAME`) needs only the
     forward. No annotation exempts a trigger.
  8. Every mutation token inside `post {}` lies inside the TRUE branch of an `if (…)` whose condition
     contains `env.<FLAG> == 'PASSED'` (FLAG = DEPLOY_WORKSPACE_PERMITTED for files with a re-guard
     container, PERMITTED_SHA_GUARD otherwise) and contains no `||`, `!=`, `!(` or `! env`. A token
     after the block, in an `else`, under a negated condition, or preceded merely by the flag's name,
     is refused.
  9. Every source acquisition after the primary guard (`checkout scm`, `checkout(`, `git url:`,
     `git clone`, `git pull`, `git checkout`, `git -C <d> checkout`): the ROOT workspace may not be
     re-acquired at all (except the leading `checkout scm` of a rule-6 stage); a NESTED directory must
     be re-bound, before the next token or acquisition, by the canonical nested guard for THAT
     directory: Groovy `def V = sh(returnStatus: true, script: 'PERMITTED_SHA="${X_PERMITTED_SHA:-}"
     bash scripts/jenkins/permitted-sha-guard.sh --dir <dir> --ref main') if (V != 0) { error(…) }` or
     the shell line `PERMITTED_SHA="${X_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh
     --dir <dir> --ref main || exit 1`, where X names that source's OWN permission (never the root's).
 10. contracts=<dir>: that directory is acquired (and therefore, by rule 9, bound) at least once.
 11. Inside every `sh '''` block, a line running permitted-sha-guard.sh ends with `|| exit 1`, and the
     block sets no `set +e`; `|| true` anywhere on such a line is refused.
 12. The guard script exists and parses (bash -n).

LIMITS — what this cannot prove: stage order is the order of `stage('…')` lines (no dynamic or
parallel stages are modelled — none exist in scope); mutation tokens are a fixed list, so a helper
script called before the guard is invisible unless its name is a token — the manifest's before= set
is the reviewed statement that those stages are effect-free; tokens in `//`/`#` comment lines and in
whole-line `echo '…'` messages are ignored, tokens after a mid-line comment marker are not; nothing
here executes a pipeline or proves the controller runs THIS file (pipeline-from-SCM loads the
Jenkinsfile from the commit it checks out, which is what the guard then binds); the scripts the
guards call are executed by each repository's tests.
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
STR = r"(?:\"[^\"]*\"|'[^']*')"
STAGE_RE = re.compile(r"^\s*stage\(\s*'((?:[^'\\]|\\.)*)'")
BUILD_JOB_RE = re.compile(r"\bbuild\s*\(?\s*job:\s*(env\.JOB_NAME|'([^']+)')")
ACQUIRE_RE = re.compile(r"\bgit url:|\bgit clone\b|\bcheckout\(|\bcheckout scm\b|\bgit pull\b|\bgit checkout\b|\bgit -C \S+ checkout\b")
CANON_GUARD = re.compile(
    r"^stage\('(?P<name>[^']+)'\) \{ (?:agent \{ label [^}]+ \} )?steps \{ script \{ "
    r"def rc = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh(?P<args>( --ref \"\$\{[A-Z_]+:\?\}\")?)'\) "
    r"if \(rc != 0\) \{ error\(" + STR + r"\) \} "
    r"env\.(?P<flag>PERMITTED_SHA_GUARD|DEPLOY_WORKSPACE_PERMITTED) = 'PASSED' \} \} \}$"
)
INLINE_REGUARD = re.compile(
    r"^(?:checkout scm )?script \{ def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh'\) "
    r"if \(\1 != 0\) \{ error\(" + STR + r"\) \}"
)
COMPAT_FORM = re.compile(
    r"def (\w+) = sh\(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream\.sh (?P<job>[A-Za-z0-9_.-]+)(?: [A-Z_]+)*'\) "
    r"if \(\1 != 0\) \{ error\(" + STR + r"\) \}"
)
NESTED_GROOVY = re.compile(
    r"def (\w+) = sh\(returnStatus: true, script: 'PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*)_PERMITTED_SHA:-\}\" "
    r"bash scripts/jenkins/permitted-sha-guard\.sh --dir (?P<dir>[^ ']+) --ref main'\) if \(\1 != 0\) \{ error\(" + STR + r"\) \}"
)
NESTED_SHELL = re.compile(
    r"PERMITTED_SHA=\"\$\{(?P<own>[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*)_PERMITTED_SHA:-\}\" bash scripts/jenkins/permitted-sha-guard\.sh --dir (?P<dir>\S+) --ref main \|\| exit 1\s*$"
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
    (r"\bnohup\b", "nohup (starts a process)"),
]
MUTATION_RES = [(re.compile(p), n) for p, n in MUTATION_TOKENS]


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


def check_guard_stage(lines: list[str], lo: int, hi: int, label: str, flag: str, ref_allowed: bool) -> list[str]:
    text = canon(lines[lo:hi])
    m = CANON_GUARD.match(text)
    if not m:
        return [f"'{label}' stage is not the canonical guard: {text[:160]}"]
    if m.group("flag") != flag:
        return [f"'{label}' stage must set env.{flag} = 'PASSED' (it sets {m.group('flag')})"]
    if m.group("args") and not ref_allowed:
        return [f"'{label}' stage may not pass --ref (that is the primary guard's business)"]
    return []


def enclosing_dir(lines: list[str], i: int, lo: int) -> str | None:
    """The `dir('X') {` block containing line i within [lo, i), if any."""
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


def check_in_scope(path: str, entry: dict, guard_hash: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().split("\n")
    problems: list[str] = []

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
    stages = stage_lines(lines)
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
    problems += check_guard_stage(lines, g_lo, g_hi, GUARD_STAGE, FLAG, ref_allowed=True)

    # 5./6. stages after the guard that own an agent
    reguard_ranges: list[tuple[int, int]] = []
    has_container = False
    for si in range(gi + 1, len(stages)):
        lo, hi = stage_range(lines, stages, si)
        body = lines[lo:hi]
        if not any(re.match(r"^\s*agent\b", l) and not is_comment(l) for l in body):
            continue
        if any(re.match(r"^\s*agent\s+none\b", l) and not is_comment(l) for l in body):
            continue   # a flyweight stage: no workspace, no repository code; its build job: is judged by rule 7
        ctext = canon(body)
        if re.match(r"^stage\('[^']+'\) \{ agent [^{]*\{[^}]*\} stages \{", ctext) or re.search(r"\} stages \{", ctext[:400]):
            has_container = True
            cont = names[si]
            if entry["reguard"] and entry["reguard"] != cont:
                problems.append(f"container stage '{cont}' owns an agent but the manifest names '{entry['reguard']}' as the re-guard container")
            if si + 1 >= len(names) or names[si + 1] != REGUARD_STAGE:
                problems.append(f"container stage '{cont}' must open with '{REGUARD_STAGE}'")
            else:
                rlo, rhi = stage_range(lines, stages, si + 1)
                problems += check_guard_stage(lines, rlo, rhi, REGUARD_STAGE, REFLAG, ref_allowed=False)
                reguard_ranges.append((rlo, rhi))
            continue
        needs = any(token_at(l) for l in body) or any("scripts/" in l and not is_comment(l) for l in body)
        if not needs:
            continue
        after_steps = ctext.split(" steps { ", 1)
        if len(after_steps) != 2 or not INLINE_REGUARD.match(after_steps[1]):
            problems.append(f"stage '{names[si]}' runs on its own agent after the guard but does not open with the canonical inline re-guard (steps {{ [checkout scm] script {{ def V = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh') if (V != 0) {{ error(…) }} …)")
    if entry["reguard"] and not has_container:
        problems.append(f"manifest names reguard={entry['reguard']} but no container stage with its own agent follows the guard")
    for i, l in enumerate(lines):
        if not is_comment(l) and re.search(rf"env\.{REFLAG}\s*=(?!=)", l) and not any(lo <= i < hi for lo, hi in reguard_ranges):
            problems.append(f"line {i + 1}: env.{REFLAG} may be set only by the '{REGUARD_STAGE}' stage")

    # 7. downstream triggers
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        m = BUILD_JOB_RE.search(l)
        if not m:
            continue
        call = "\n".join(lines[i: i + 60])
        if "name: 'PERMITTED_SHA'" not in call:
            problems.append(f"line {i + 1}: build job: does not forward PERMITTED_SHA")
        if m.group(1) == "env.JOB_NAME":
            continue
        job = m.group(2)
        si = max(k for k, (s, _) in enumerate(stages) if s <= i)
        slo, _ = stage_range(lines, stages, si)
        candidates = [canon(lines[slo:i])]
        if si > 0:
            plo, phi = stage_range(lines, stages, si - 1)
            candidates.append(canon(lines[plo:phi]))
        ok = any(cm.group("job") == job for c in candidates for cm in COMPAT_FORM.finditer(c))
        if not ok:
            problems.append(f"line {i + 1}: build job: '{job}' is not preceded (same stage or the one before) by the canonical compatibility check for {job}: def V = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh {job} …') if (V != 0) {{ error(…) }}")
    if any("UNBOUND-DOWNSTREAM" in l for l in lines):
        problems.append("an UNBOUND-DOWNSTREAM annotation is not a gate: refuse the path or bind it")

    # 8. post{} structural gating
    flag_needed = REFLAG if has_container else FLAG
    for pb2 in [b for b in [find_block(lines, re.compile(r"^\s*post\s*\{"), s) for s in range(len(lines))] if b]:
        pass  # (placeholder: iterated below without duplicates)
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
        blanked = [("" if (is_comment(l) or is_message(l)) else l) for l in lines[lo:hi + 1]]
        text = "\n".join(blanked)
        gates: list[tuple[int, int]] = []
        for gm in re.finditer(r"\bif \(([^{}]*)\)\s*\{", text):
            cond = gm.group(1)
            if (re.search(rf"env\.{flag_needed} == 'PASSED'", cond) and "||" not in cond and "!=" not in cond
                    and "!(" not in cond and not re.search(r"!\s*env\.", cond)):
                open_at = gm.end() - 1
                depth = 0
                close_at = None
                for k in range(open_at, len(text)):
                    if text[k] == "{":
                        depth += 1
                    elif text[k] == "}":
                        depth -= 1
                        if depth == 0:
                            close_at = k
                            break
                if close_at is not None:
                    gates.append((open_at, close_at))
        for rx, name in MUTATION_RES:
            for tm in rx.finditer(text):
                if not any(a < tm.start() < b for a, b in gates):
                    ln = lo + text[: tm.start()].count("\n") + 1
                    problems.append(f"post {{}} line {ln}: {name} is not inside the true branch of an `if (env.{flag_needed} == 'PASSED')` gate")

    # 9./10. source acquisitions after the guard
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
        rebound = False
        for j in range(i + 1, shi):
            lj = lines[j]
            if is_comment(lj):
                continue
            sm = NESTED_SHELL.search(lj)
            if sm and sm.group("dir") == d:
                rebound = True
                break
            gm = NESTED_GROOVY.search(canon(lines[j:min(j + 8, shi)]))
            if gm and gm.group("dir") == d:
                rebound = True
                break
            if token_at(lj):
                break
            if ACQUIRE_RE.search(lj) and "permitted-sha-guard.sh" not in lj and acquisition_dir(lines, j, slo) != d:
                break   # a different source acquired first — this one must have been bound before it
        if not rebound:
            problems.append(f"line {i + 1}: '{d}' acquired after the guard is not re-bound before the next effect by the canonical nested guard for it (PERMITTED_SHA=\"${{X_PERMITTED_SHA:-}}\" bash scripts/jenkins/permitted-sha-guard.sh --dir {d} --ref main …)")
    if entry["contracts"] and not contracts_seen:
        problems.append(f"manifest names contracts={entry['contracts']} but no acquisition of it was found after the guard")

    # 11. shell blocks that run the guard
    in_block = False
    blk: list[str] = []
    for i, l in enumerate(lines):
        if not in_block and re.search(r"sh\s*(?:\(\s*[^)]*script:\s*)?'''\s*$", l):
            in_block, blk = True, []
            continue
        if in_block and re.match(r"^\s*'''", l):
            in_block = False
            if any("permitted-sha-guard.sh" in x for x in blk):
                for x in blk:
                    if "permitted-sha-guard.sh" in x and not is_comment(x):
                        if "|| true" in x or not x.rstrip().endswith("|| exit 1"):
                            problems.append(f"shell guard line must end with `|| exit 1` and never `|| true`: {x.strip()[:100]}")
                if any(re.search(r"\bset \+e\b", x) for x in blk):
                    problems.append("a shell block that runs the guard must not `set +e`")
            continue
        if in_block:
            blk.append(l)
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--manifest", required=True)
    a = ap.parse_args()
    root = os.path.abspath(a.root)
    guard = os.path.join(root, GUARD_SCRIPT_REL)
    failures: list[str] = []
    if not os.path.isfile(a.manifest):
        print(f"FAIL: manifest missing: {a.manifest}")
        return 1
    entries = parse_manifest(a.manifest)
    present = sorted(os.path.basename(p) for p in glob.glob(os.path.join(root, "Jenkinsfile*")) if os.path.isfile(p))
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
