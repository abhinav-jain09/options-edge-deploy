#!/usr/bin/env python3
"""Coverage check for the Schema sync gate (docs/schema-governance R11a, slice 1).

1. scripts/deploy/service-deploy.sh must call scripts/ci/schema-sync.sh BEFORE its first `kubectl apply` (dry-run or real), and the
   committed tool jar must match the sha256 in tools/schema-governance/PIN.
2. Every Jenkinsfile that changes workloads must either go through service-deploy.sh (so it inherits 1) or be named in
   scripts/ci/schema-sync-coverage-allowlist.txt with a reason. An unlisted path fails: a new deploy route cannot appear silently.
Comments are stripped before matching (Groovy // and /* */, shell full-line #), so a mention in a comment proves nothing.
Slice 1: a text scan of `sh` bodies; the full tokeniser of R11a (ssh payload recursion, alias resolution) is a later slice.
"""
import hashlib, os, re, sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
WORKLOAD = re.compile(r"\bkubectl\s+(?:-\S+\s+\S+\s+)*(apply|set\s+image|rollout|scale|delete|patch|replace|create)\b|\bapply\.sh\b|\bcleanup-es4\.sh\b|\bservice-deploy\.sh\b")
VIA = re.compile(r"\bservice-deploy\.sh\b")


def strip(text):
    out, i, n = [], 0, len(text)
    q = None
    while i < n:
        c = text[i]
        if q:
            out.append(c)
            if text.startswith(q, i) and (q in ("'", '"') and text[i - 1] != "\\" or len(q) == 3):
                out.append(text[i + 1:i + len(q)]); i += len(q); q = None; continue
            i += 1; continue
        if text.startswith("'''", i) or text.startswith('"""', i):
            q = text[i:i + 3]; out.append(q); i += 3; continue
        if c in "'\"":
            q = c; out.append(c); i += 1; continue
        if text.startswith("//", i):
            while i < n and text[i] != "\n": i += 1
            continue
        if text.startswith("/*", i):
            j = text.find("*/", i + 2); i = n if j < 0 else j + 2; continue
        out.append(c); i += 1
    lines = [l for l in "".join(out).split("\n") if not l.lstrip().startswith("#")]
    return "\n".join(lines)


SH_OPEN = re.compile(r"""\bsh\s*\(?\s*(?:script\s*:\s*)?('\'\'|\"\"\"|'|")""")


def sh_bodies(code):
    """Text of every `sh` step body (single, double or triple quoted). A string elsewhere in the file is not an invocation."""
    bodies, pos = [], 0
    while True:
        m = SH_OPEN.search(code, pos)
        if not m: return "\n".join(bodies)
        q, i = m.group(1), m.end()
        if len(q) == 3:
            j = code.find(q, i)
        else:
            j = i
            while j < len(code) and code[j] != q:
                j += 2 if code[j] == "\\" else 1
            j = j if j < len(code) else -1
        if j < 0: fail("unterminated sh step body - cannot place the gate")
        bodies.append(code[i:j]); pos = j + len(q)


def fail(msg):
    print("FAIL: " + msg); sys.exit(1)


def read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f: return f.read()


# 1. service-deploy.sh order + jar pin
sd = "\n".join(l for l in read("scripts/deploy/service-deploy.sh").split("\n") if not l.lstrip().startswith("#"))
sync_at = sd.find("scripts/ci/schema-sync.sh")
apply_at = sd.find("kubectl apply")
if sync_at < 0: fail("scripts/deploy/service-deploy.sh does not call scripts/ci/schema-sync.sh")
if apply_at < 0: fail("scripts/deploy/service-deploy.sh has no kubectl apply - this check cannot place the gate")
if sync_at > apply_at: fail("scripts/deploy/service-deploy.sh calls schema-sync.sh AFTER its first kubectl apply")
pin = re.search(r"^sha256=([0-9a-f]{64})$", read("tools/schema-governance/PIN"), re.M)
if not pin: fail("tools/schema-governance/PIN has no sha256= line")
with open(os.path.join(ROOT, "tools/schema-governance/schema-governance-tool.jar"), "rb") as f:
    if hashlib.sha256(f.read()).hexdigest() != pin.group(1): fail("the committed tool jar does not match tools/schema-governance/PIN")

# 2. Jenkinsfile coverage
allow = {}
for l in read("scripts/ci/schema-sync-coverage-allowlist.txt").split("\n"):
    l = l.strip()
    if l and not l.startswith("#"):
        name, _, reason = l.partition("|")
        if not reason.strip(): fail("allow-list entry '%s' has no reason" % name)
        allow[name.strip()] = reason.strip()
seen = set()
for fn in sorted(os.listdir(ROOT)):
    if not fn.startswith("Jenkinsfile"): continue
    code = strip(read(fn))
    if not WORKLOAD.search(code): continue
    seen.add(fn)
    if VIA.search(sh_bodies(code)): continue
    if fn not in allow: fail("%s changes workloads without service-deploy.sh and is not in scripts/ci/schema-sync-coverage-allowlist.txt" % fn)
stale = sorted(set(allow) - seen)
if stale: fail("allow-list names Jenkinsfiles with no workload-changing step (stale): %s" % ", ".join(stale))
print("schema-sync wiring OK: service-deploy.sh gates before its first apply; %d Jenkinsfile(s) outside it are listed with a reason" % len(allow))
