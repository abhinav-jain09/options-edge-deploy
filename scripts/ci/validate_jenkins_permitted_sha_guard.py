#!/usr/bin/env python3
"""Every Jenkins deploy job carries the permitted-commit guard, and carries it BEFORE its first effect.

Deployment Permission Rule (options-edge rule.md): "Jenkins enforces the permitted commit, not the
assistant. Every deployment job the assistant may trigger takes a PERMITTED_SHA parameter and, in its
checkout stage — before any stage that builds, publishes, applies, rolls, restarts or alters anything —
fails closed unless the checked-out commit equals PERMITTED_SHA exactly." A job without the guard is
not available to the assistant at all (rule line 364), so this file also forces every Jenkinsfile in
the repository to be classified: in scope (guarded) or out of scope (with the reason), in
scripts/ci/jenkins-permitted-sha-scope.txt. A new Jenkinsfile that nobody classified fails here.

What is checked for every IN-scope Jenkinsfile, TEXTUALLY (see LIMITS):
  1. `string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, ...)` inside the parameters block.
  2. `disableRestartFromStage()` in the options — "Restart from Stage" would otherwise let a run
     resume past the guard.
  3. A stage named exactly 'Permitted commit guard' whose body IS the canonical guard, not merely
     resembles it: `def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh…')`,
     then `if (rc != 0) {` `error(…)` `}`, then `env.PERMITTED_SHA_GUARD = 'PASSED'`, nothing else.
     `rc == 0`, a missing error(), catchError / warnError / unstable / try — all refused. The flag
     DEPLOY_WORKSPACE_PERMITTED may be set by nothing but the re-guard (rule 5).
  4. Stage ORDER: every stage textually before the guard must be listed in the manifest's
     `before=` set for that file (e.g. an existing branch refusal that runs first), and the source
     text between the top-level `stages {` and the guard stage must contain no mutation token
     (kubectl, docker build/push, build job:, rsync, scp, ssh, launchctl, kafka-topics, crontab,
     helm, ansible-playbook, mvn, git push, vix-unpause.sh) outside comments.
  5. For pipelines whose deploy stages run on their own agent (manifest `reguard=<container stage>`):
     the container stage must be immediately followed by 'Permitted commit guard (deploy workspace)',
     the same canonical guard setting its OWN flag `env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'`.
  6. Every `build job:` (a downstream deployment trigger) is preceded, within the same stage's
     text, by the fail-closed compatibility check `require-guarded-downstream.sh <that job>` and
     forwards `PERMITTED_SHA` in its parameter list. A self-trigger (`build job: env.JOB_NAME`) runs
     this same definition and only needs the forward. No annotation exempts a trigger.
  7. A post{} block that contains a mutation token must gate it: an `if (env.<FLAG> == 'PASSED')`
     line before the first token (a conjunction with && is still a gate; an || is not), where FLAG
     is DEPLOY_WORKSPACE_PERMITTED for reguard files and PERMITTED_SHA_GUARD otherwise. The flag's
     NAME appearing in an echo, or an inverted `!=` branch, is not a gate.
  8. Any source acquired AFTER the guard (`git url:`, `git clone`, `checkout(`, `checkout scm`,
     `git pull`, `git checkout`) must be re-bound — a `permitted-sha-guard.sh --dir` call — before
     the next mutation token. A later checkout that silently switches commits is what rule item 5
     forbids.
  9. The guard script exists and parses (bash -n).

LIMITS — what a textual check cannot prove, and what review must still do:
  * Stage order is the order of `stage('...')` lines in the file. Groovy that builds stages
    dynamically, a stage defined inside a method, or `parallel {}` branches are not modelled; none of
    the in-scope files do that today. A stage line inside a comment IS excluded.
  * Mutation tokens are a fixed list. A helper script called before the guard (`bash scripts/x.sh`)
    is invisible unless its name is a token; the manifest's `before=` set is the reviewed statement
    of which pre-guard stages are effect-free. Tokens inside `//` or `#` comment lines and inside
    whole-line `echo '…'` messages are ignored; tokens after a mid-line comment marker are not.
  * Control flow is matched by canonical text. A guard rewritten in a different but equivalent
    shape fails here and must be brought back to the canonical form — that is deliberate.
  * Nothing here proves the Jenkins controller runs THIS file: pipeline-from-SCM loads the
    Jenkinsfile from the same commit it checks out, which is what the guard then binds. Nothing here
    executes a pipeline; the scripts the guards call are executed by tests/test_jenkins_permitted_sha_guard.py.
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import subprocess
import sys

GUARD_STAGE = "Permitted commit guard"
REGUARD_STAGE = "Permitted commit guard (deploy workspace)"
GUARD_SCRIPT_REL = "scripts/jenkins/permitted-sha-guard.sh"
FLAG = "PERMITTED_SHA_GUARD"
REFLAG = "DEPLOY_WORKSPACE_PERMITTED"
PARAM_RE = re.compile(r"string\(\s*name:\s*'PERMITTED_SHA'")
STAGE_RE = re.compile(r"^\s*stage\(\s*'((?:[^'\\]|\\.)*)'")
ACQUIRE_RE = re.compile(r"\bgit url:|\bgit clone\b|\bcheckout\(|\bcheckout scm\b|\bgit pull\b|\bgit checkout\b|\bgit -C \S+ checkout\b")
BUILD_JOB_RE = re.compile(r"\bbuild\s*\(?\s*job:\s*(env\.JOB_NAME|'([^']+)')")
COMPAT_RE = re.compile(r"require-guarded-downstream\.sh\s+([A-Za-z0-9_.-]+)")
# The canonical guard body, comments and blank lines removed, whitespace collapsed to single spaces.
CANON_GUARD = re.compile(
    r"^stage\('(?P<name>[^']+)'\) \{ (?:agent \{ label [^}]+ \} )?steps \{ script \{ "
    r"def rc = sh\(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard\.sh[^']*'\) "
    r"if \(rc != 0\) \{ error\((?:\"[^\"]*\"|'[^']*')\) \} "
    r"env\.(?P<flag>PERMITTED_SHA_GUARD|DEPLOY_WORKSPACE_PERMITTED) = 'PASSED' \} \} \}$"
)
MUTATION_TOKENS = [
    (r"\bkubectl\b", "kubectl"),
    (r"\bdocker\s+(build|buildx|push|run|compose)\b", "docker build/push/run"),
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
]
MUTATION_RES = [(re.compile(p), n) for p, n in MUTATION_TOKENS]


def is_comment(line: str) -> bool:
    s = line.strip()
    return s.startswith("//") or s.startswith("#") or s.startswith("*") or s.startswith("/*")


def is_message(line: str) -> bool:
    s = line.strip()
    return s.startswith("echo ") and len(s) > 6 and s[5] in "'\"" and s.endswith(s[5]) and s.count(s[5]) == 2


def block_end(lines: list[str], start: int) -> int:
    """Index of the line holding the brace that closes the block opened on lines[start]."""
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


def find_block(lines: list[str], header_re: re.Pattern) -> tuple[int, int] | None:
    for i, l in enumerate(lines):
        if header_re.match(l) and not is_comment(l):
            return i, block_end(lines, i)
    return None


def stage_lines(lines: list[str]) -> list[tuple[int, str]]:
    out = []
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        m = STAGE_RE.match(l)
        if m:
            out.append((i, m.group(1)))
    return out


def token_at(line: str) -> str | None:
    if is_comment(line) or is_message(line):
        return None
    for rx, name in MUTATION_RES:
        if rx.search(line):
            return name
    return None


def mutation_hits(lines: list[str], lo: int, hi: int) -> list[str]:
    hits = []
    for i in range(lo, min(hi, len(lines))):
        name = token_at(lines[i])
        if name:
            hits.append(f"line {i + 1}: {name}: {lines[i].strip()[:100]}")
    return hits


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
            before: set[str] = set()
            reguard = ""
            for opt in [o for o in opts.split(";") if o.strip()]:
                k, _, v = opt.partition("=")
                k, v = k.strip(), v.strip()
                if k == "before":
                    before.update(s.strip() for s in v.split(",") if s.strip())
                elif k == "reguard":
                    reguard = v
                else:
                    raise SystemExit(f"{path}:{n}: unknown option '{k}' (before=, reguard=)")
            if not reason:
                raise SystemExit(f"{path}:{n}: {name} needs a reason")
            entries[name] = {"scope": scope, "before": before, "reguard": reguard, "reason": reason}
    return entries


def stage_body_range(lines: list[str], stages: list[tuple[int, str]], idx: int) -> tuple[int, int]:
    start = stages[idx][0]
    return start, block_end(lines, start) + 1


def check_guard_stage(lines: list[str], stages: list[tuple[int, str]], idx: int, label: str, flag: str) -> list[str]:
    lo, hi = stage_body_range(lines, stages, idx)
    text = canon(lines[lo:hi])
    m = CANON_GUARD.match(text)
    if not m:
        return [f"'{label}' stage is not the canonical guard (sh(returnStatus: true, …permitted-sha-guard.sh…); if (rc != 0) {{ error(…) }}; env.{flag} = 'PASSED'; nothing else): {text[:160]}"]
    if m.group("flag") != flag:
        return [f"'{label}' stage must set env.{flag} = 'PASSED' (it sets {m.group('flag')})"]
    return []


def check_in_scope(path: str, entry: dict) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().split("\n")
    problems: list[str] = []

    # 1. parameter
    pb = find_block(lines, re.compile(r"^\s*parameters\s*\{"))
    if pb is None:
        problems.append("no parameters {} block")
    else:
        lo, hi = pb
        hit = [i for i in range(lo, hi + 1) if PARAM_RE.search(lines[i]) and not is_comment(lines[i])]
        if not hit:
            problems.append("parameters {} lacks string(name: 'PERMITTED_SHA', ...)")
        else:
            decl = " ".join(l.strip() for l in lines[hit[0]:hit[0] + 4])
            if "defaultValue: ''" not in decl:
                problems.append("PERMITTED_SHA must have defaultValue: '' (nothing is ever pre-filled)")
            if "trim: true" not in decl:
                problems.append("PERMITTED_SHA must be declared with trim: true")

    # 2. restart from stage disabled
    if not any("disableRestartFromStage()" in l and not is_comment(l) for l in lines):
        problems.append("options {} lacks disableRestartFromStage()")

    # 3./4. the guard stage and the stage order before it
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
        for h in mutation_hits(lines, sb[0], stages[gi][0]):
            problems.append(f"mutation token before the guard: {h}")
    problems += check_guard_stage(lines, stages, gi, GUARD_STAGE, FLAG)
    guard_lo, guard_hi = stage_body_range(lines, stages, gi)

    # 5. the re-guard, with its own flag — set nowhere else
    reguard_range = None
    if entry["reguard"]:
        cont = entry["reguard"]
        if cont not in names:
            problems.append(f"reguard container stage '{cont}' not found")
        else:
            ci = names.index(cont)
            if ci + 1 >= len(names) or names[ci + 1] != REGUARD_STAGE:
                problems.append(f"stage '{cont}' must be immediately followed by '{REGUARD_STAGE}'")
            else:
                problems += check_guard_stage(lines, stages, ci + 1, REGUARD_STAGE, REFLAG)
                reguard_range = stage_body_range(lines, stages, ci + 1)
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        if re.search(rf"env\.{REFLAG}\s*=", l):
            if reguard_range is None or not (reguard_range[0] <= i < reguard_range[1]):
                problems.append(f"line {i + 1}: env.{REFLAG} may be set only by the '{REGUARD_STAGE}' stage")

    # 6. downstream triggers: compatibility-checked and forwarded
    for i, l in enumerate(lines):
        if is_comment(l):
            continue
        m = BUILD_JOB_RE.search(l)
        if not m:
            continue
        call = "\n".join(lines[i: i + 45])
        if "name: 'PERMITTED_SHA'" not in call:
            problems.append(f"line {i + 1}: build job: does not forward PERMITTED_SHA")
        if m.group(1) == "env.JOB_NAME":
            continue
        job = m.group(2)
        # the enclosing stage's text before the trigger
        st_start = max([s for s, _ in stages if s <= i], default=0)
        before = "\n".join(l2 for l2 in lines[st_start:i] if not is_comment(l2))
        checked = COMPAT_RE.findall(before)
        if job not in checked:
            problems.append(f"line {i + 1}: build job: '{job}' is not preceded in its stage by require-guarded-downstream.sh {job}")
    if any("UNBOUND-DOWNSTREAM" in l for l in lines):
        problems.append("an UNBOUND-DOWNSTREAM annotation is not a gate: refuse the path or bind it")

    # 7. post{} gating
    flag_needed = REFLAG if entry["reguard"] else FLAG
    pb2 = find_block(lines, re.compile(r"^\s*post\s*\{"))
    if pb2 is not None:
        lo, hi = pb2
        first_token = next((i for i in range(lo, hi + 1) if token_at(lines[i])), None)
        if first_token is not None:
            # A gate is an `if (` whose condition REQUIRES the flag: `env.FLAG == 'PASSED'` on the
            # if-line, joined only by && (an || would let another condition open the gate).
            def is_gate(l: str, before_col: int | None = None) -> bool:
                if is_comment(l) or "||" in l:
                    return False
                m = re.search(rf"\bif \([^{{]*env\.{flag_needed} == 'PASSED'[^{{]*\)", l)
                return m is not None and (before_col is None or m.start() < before_col)
            gate = next((i for i in range(lo, first_token) if is_gate(lines[i])), None)
            if gate is None:
                tok_line = lines[first_token]
                tok_col = min(m.start() for rx, _ in MUTATION_RES for m in [rx.search(tok_line)] if m)
                if is_gate(tok_line, tok_col):
                    gate = first_token
            if gate is None:
                problems.append(f"post {{}} mutates a target (line {first_token + 1}: {lines[first_token].strip()[:80]}) without an `if (env.{flag_needed} == 'PASSED')` gate before it")

    # 8. source acquired after the guard must be re-bound before the next effect
    for i in range(guard_hi, len(lines)):
        l = lines[i]
        if is_comment(l) or not ACQUIRE_RE.search(l) or "permitted-sha-guard.sh" in l:
            continue
        rebound = False
        for j in range(i + 1, len(lines)):
            if "permitted-sha-guard.sh --dir" in lines[j] and not is_comment(lines[j]):
                rebound = True
                break
            if token_at(lines[j]):
                break
        if not rebound:
            problems.append(f"line {i + 1}: source acquired after the guard is not re-bound (permitted-sha-guard.sh --dir …) before the next effect: {l.strip()[:90]}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="repository root (Jenkinsfile* live here)")
    ap.add_argument("--manifest", default=None, help="scope manifest (default <root>/scripts/ci/jenkins-permitted-sha-scope.txt)")
    ap.add_argument("--guard-script", default=None, help="guard script to bash -n (default <root>/scripts/jenkins/permitted-sha-guard.sh)")
    a = ap.parse_args()
    root = os.path.abspath(a.root)
    manifest = a.manifest or os.path.join(root, "scripts/ci/jenkins-permitted-sha-scope.txt")
    guard = a.guard_script or os.path.join(root, GUARD_SCRIPT_REL)

    failures: list[str] = []
    if not os.path.isfile(manifest):
        print(f"FAIL: manifest missing: {manifest}")
        return 1
    entries = parse_manifest(manifest)
    present = sorted(os.path.basename(p) for p in glob.glob(os.path.join(root, "Jenkinsfile*")) if os.path.isfile(p))
    for f in present:
        if f not in entries:
            failures.append(f"{f}: not classified in {os.path.relpath(manifest, root)} — add it as in (guarded) or out (with the reason)")
    for f in entries:
        if f not in present:
            failures.append(f"{f}: listed in the manifest but not present in {root}")

    if not os.path.isfile(guard):
        failures.append(f"guard script missing: {guard}")
    else:
        r = subprocess.run(["bash", "-n", guard], capture_output=True, text=True)
        if r.returncode != 0:
            failures.append(f"guard script does not parse: {r.stderr.strip()}")

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
        probs = check_in_scope(os.path.join(root, f), e)
        if probs:
            for p in probs:
                failures.append(f"{f}: {p}")
            print(f"  FAIL {f}")
        else:
            print(f"  ok   {f}")

    if failures:
        print(f"FAIL: {len(failures)} problem(s):")
        for x in failures:
            print(f"  - {x}")
        return 1
    print(f"OK: {n_in} Jenkinsfile(s) carry the permitted-commit guard before any effect; {n_out} classified out of scope")
    return 0


if __name__ == "__main__":
    sys.exit(main())
