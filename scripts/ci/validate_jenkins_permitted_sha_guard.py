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
  3. A stage named exactly 'Permitted commit guard' whose body calls scripts/jenkins/permitted-sha-guard.sh
     through sh(returnStatus: true, ...) and error()s on any non-zero status; catchError / warnError /
     unstable / try inside that stage are refused (a coloured result is not a stop).
  4. Stage ORDER: every stage textually before the guard must be listed in the manifest's
     `before=` set for that file (e.g. an existing branch refusal that runs first), and the source
     text between the top-level `stages {` and the guard stage must contain no mutation token
     (kubectl, docker build/push, build job:, rsync, scp, ssh, launchctl, kafka-topics, crontab,
     helm, ansible-playbook, mvn, git push) outside comments.
  5. For pipelines whose deploy stages run on their own agent (manifest `reguard=<container stage>`):
     the container stage must be immediately followed by 'Permitted commit guard (deploy workspace)',
     the same guard re-established in the workspace the effects come from.
  6. Every `build job:` (a downstream deployment trigger) forwards a permitted SHA — a `PERMITTED_SHA`
     parameter within the call — or is marked `// UNBOUND-DOWNSTREAM:` with the reason (a downstream
     job that has no guard yet is a path the assistant may not use).
  7. A post{} block that contains a mutation token must be gated on the guard's PERMITTED_SHA_GUARD
     flag, so a refused build cannot mutate a target through post/finally.
  8. The guard script exists and parses (bash -n).

LIMITS — what a textual check cannot prove, and what review must still do:
  * Stage order is the order of `stage('...')` lines in the file. Groovy that builds stages
    dynamically, a stage defined inside a method, or `parallel {}` branches are not modelled; none of
    the in-scope files do that today, and a stage line inside a comment IS excluded.
  * Mutation tokens are a fixed list. A helper script called before the guard (`bash scripts/x.sh`)
    is invisible unless its name is a token; the manifest's `before=` set is the reviewed statement
    of which pre-guard stages are effect-free. Tokens inside `//` or `#` comment lines are ignored,
    tokens after a mid-line comment marker are not.
  * The post{} check only sees tokens literally present in the post block; a script it calls
    (scripts/deploy/vix-unpause.sh in service-deploy) is reviewed by hand — that one exits before
    touching the cluster unless a marker the deploy stage writes exists.
  * Nothing here proves the Jenkins controller runs THIS file: pipeline-from-SCM loads the
    Jenkinsfile from the same commit it checks out, which is what the guard then binds.
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
PARAM_RE = re.compile(r"string\(\s*name:\s*'PERMITTED_SHA'")
STAGE_RE = re.compile(r"^\s*stage\(\s*'((?:[^'\\]|\\.)*)'")
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
]
MUTATION_RES = [(re.compile(p), n) for p, n in MUTATION_TOKENS]


def is_comment(line: str) -> bool:
    s = line.strip()
    return s.startswith("//") or s.startswith("#") or s.startswith("*") or s.startswith("/*")


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


def mutation_hits(lines: list[str], lo: int, hi: int) -> list[str]:
    hits = []
    for i in range(lo, min(hi, len(lines))):
        l = lines[i]
        if is_comment(l):
            continue
        # A whole-line quoted message — `echo '...'` / `echo "..."` (post{} advice such as "verify with
        # kubectl ... | grep x") — is not a command. An echo that ends outside its quotes (a pipe
        # after the closing quote, a redirection) is judged like any other line.
        s = l.strip()
        if s.startswith("echo ") and len(s) > 6 and s[5] in "'\"" and s.endswith(s[5]) and s.count(s[5]) == 2:
            continue
        for rx, name in MUTATION_RES:
            if rx.search(l):
                hits.append(f"line {i + 1}: {name}: {l.strip()[:100]}")
                break
    return hits


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


def check_guard_stage(lines: list[str], stages: list[tuple[int, str]], idx: int, label: str) -> list[str]:
    """The guard stage's body: from its stage line to the next stage line (or EOF)."""
    start = stages[idx][0]
    end = stages[idx + 1][0] if idx + 1 < len(stages) else len(lines)
    body = "\n".join(l for l in lines[start:end] if not is_comment(l))
    problems = []
    if GUARD_SCRIPT_REL not in body:
        problems.append(f"'{label}' stage does not call {GUARD_SCRIPT_REL}")
    if "returnStatus: true" not in body:
        problems.append(f"'{label}' stage must run the guard with sh(returnStatus: true, ...) and judge the status itself")
    if "error(" not in body:
        problems.append(f"'{label}' stage must error() on a non-zero guard status (fail closed)")
    for bad in ("catchError", "warnError", "unstable(", "try {", "try{"):
        if bad in body:
            problems.append(f"'{label}' stage contains '{bad}': a caught or coloured failure is not a stop")
    if f"env.{FLAG} = 'PASSED'" not in body and f"env.{FLAG}='PASSED'" not in body:
        problems.append(f"'{label}' stage must set env.{FLAG} = 'PASSED' after the guard permits (post{{}} paths key off it)")
    return problems


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

    # 3./4. stage order and the guard stage
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
    problems += check_guard_stage(lines, stages, gi, GUARD_STAGE)

    # 5. re-guard in the deploy workspace
    if entry["reguard"]:
        cont = entry["reguard"]
        if cont not in names:
            problems.append(f"reguard container stage '{cont}' not found")
        else:
            ci = names.index(cont)
            if ci + 1 >= len(names) or names[ci + 1] != REGUARD_STAGE:
                problems.append(f"stage '{cont}' must be immediately followed by '{REGUARD_STAGE}'")
            else:
                problems += check_guard_stage(lines, stages, ci + 1, REGUARD_STAGE)

    # 6. downstream triggers
    for i, l in enumerate(lines):
        if is_comment(l) or not re.search(r"\bbuild\s*\(?\s*job:", l):
            continue
        window = "\n".join(lines[max(0, i - 15): i + 45])
        if "PERMITTED_SHA" not in "\n".join(lines[i: i + 45]) and "UNBOUND-DOWNSTREAM:" not in window:
            problems.append(f"line {i + 1}: build job: forwards no PERMITTED_SHA and carries no // UNBOUND-DOWNSTREAM: marker")

    # 7. post{} gating
    pb2 = find_block(lines, re.compile(r"^\s*post\s*\{"))
    if pb2 is not None:
        lo, hi = pb2
        hits = mutation_hits(lines, lo, hi + 1)
        if hits and not any(FLAG in lines[i] for i in range(lo, hi + 1)):
            problems.append(f"post {{}} mutates a target but is not gated on {FLAG}: " + "; ".join(hits[:3]))
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
