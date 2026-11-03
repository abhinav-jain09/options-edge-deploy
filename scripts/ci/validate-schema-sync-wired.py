#!/usr/bin/env python3
"""Coverage check for the Schema sync gate (docs/schema-governance R11a, slice 1).

1. scripts/deploy/service-deploy.sh must call scripts/ci/schema-sync.sh BEFORE its first `kubectl apply` (dry-run or real), and the
   committed tool jar must match the sha256 in tools/schema-governance/PIN.
2. Every Jenkinsfile that changes workloads must either go through service-deploy.sh (so it inherits 1) or be named in
   scripts/ci/schema-sync-coverage-allowlist.txt with a reason. An unlisted path fails: a new deploy route cannot appear silently.
Comments are stripped before matching (Groovy // and /* */, shell full-line #), so a mention in a comment proves nothing.
THREAT MODEL (honest scope): this is a ratchet against ACCIDENTAL drift - a new deploy route, a removed or reordered gate call, a new
direct kubectl command, a swapped ancillary command - in files that are themselves code-reviewed. It is a text scan, not a
reachability analysis: a deliberately obfuscated change (dead branch around the call, a command built at run time) is not detected and
remains the job of review of any change to these files. Ancillary direct commands are pinned by FINGERPRINT (sha256 of the normalised
statement), not by count, so replacing one with a different command fails.
Slice 1: a text scan of `sh` bodies; the full tokeniser of R11a (ssh payload recursion, alias resolution) is a later slice.
"""
import hashlib, os, re, sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
# kubectl (or a conventional variable holding it: $KC, ${KUBECTL}) followed, within the same statement, by a workload-changing verb, in
# ANY flag form (`-n ns`, `--namespace=ns`, `--kubeconfig f`). Statements end at ; & | or a newline not escaped by a backslash.
_KUBECTL = r"(?:\bkubectl\b|\$\{?[A-Za-z_]*(?:KC|KUBECTL)[A-Za-z_]*\}?)"
_VERB = r"(?:apply|set\s+image|rollout|scale|delete|patch|replace|create)"
_KCMD = _KUBECTL + r"[^;&|\n]*?\s" + _VERB + r"\b"
WORKLOAD = re.compile(_KCMD + r"|\bapply\.sh\b|\bcleanup-es4\.sh\b|\bservice-deploy\.sh\b")
# service-deploy.sh counts as REALLY invoked only as a command: at the start of a line or after ; && || | ( , with optional VAR=value
# prefixes and an optional `bash`/`sh`. An echo, a variable assignment or a string argument is not an invocation.
VIA = re.compile(r"(?:^|[;&|(]|\n)[ \t]*(?:[A-Za-z_][A-Za-z0-9_]*=(?:\"[^\"]*\"|'[^']*'|\S*)[ \t]+)*(?:bash|sh)[ \t]+(?:-\S+[ \t]+)*(?:\./)?scripts/deploy/(?:service-deploy|apply)\.sh\b")
DIRECT = re.compile(_KCMD + r"|\bapply\.sh\b|\bcleanup-es4\.sh\b")


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
    joined = re.sub(r"\\\n", " ", "".join(out))      # a backslash-newline continues the statement
    lines = [l for l in joined.split("\n") if not l.lstrip().startswith("#")]
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
# An EXECUTABLE, FAIL-CLOSED call: `bash scripts/ci/schema-sync.sh` as a command at the start of a line (after the env-var
# continuation lines), followed by `|| { ... exit 1; }`. The text appearing in an echo, a `:` no-op, a comment or after `|| true`
# is not a call and does not count.
CALL = re.compile(r"^[ \t]*bash[ \t]+scripts/ci/schema-sync\.sh[ \t]*\\?\n[ \t]*\|\|[ \t]*\{[^}]*\bexit[ \t]+1\b", re.M)
m_call = CALL.search(sd)
sync_at = m_call.start() if m_call else -1
apply_at = sd.find("kubectl apply")
if sync_at < 0: fail("scripts/deploy/service-deploy.sh does not EXECUTE scripts/ci/schema-sync.sh fail-closed (bash ... || { ...; exit 1; })")
if apply_at < 0: fail("scripts/deploy/service-deploy.sh has no kubectl apply - this check cannot place the gate")
if sync_at > apply_at: fail("scripts/deploy/service-deploy.sh calls schema-sync.sh AFTER its first kubectl apply")
# 1b. scripts/deploy/apply.sh (the monolithic full deploy) must EXECUTE schema-sync-all.sh, fail-closed, before its first workload mutation
ap = "\n".join(l for l in read("scripts/deploy/apply.sh").split("\n") if not l.lstrip().startswith("#"))
ap = re.sub(r"\\\n", " ", ap)
CALL_ALL = re.compile(r"^[ \t]*bash[ \t]+scripts/ci/schema-sync-all\.sh[^\n|]*\|\|[ \t]*\{[^\n]*?\bexit[ \t]+1\b", re.M)
mc = CALL_ALL.search(ap)
if not mc: fail("scripts/deploy/apply.sh does not EXECUTE scripts/ci/schema-sync-all.sh fail-closed (bash ... || { ...; exit 1; })")
m_mut = re.search(r"\bkubectl\b[^;&|\n]*?\s(?:apply|delete|patch|replace|create|scale|rollout|set\s+image)\b", ap)
if m_mut and m_mut.start() < mc.start(): fail("scripts/deploy/apply.sh runs a kubectl mutation BEFORE its schema-sync-all.sh call")
pin = re.search(r"^sha256=([0-9a-f]{64})$", read("tools/schema-governance/PIN"), re.M)
if not pin: fail("tools/schema-governance/PIN has no sha256= line")
with open(os.path.join(ROOT, "tools/schema-governance/schema-governance-tool.jar"), "rb") as f:
    if hashlib.sha256(f.read()).hexdigest() != pin.group(1): fail("the committed tool jar does not match tools/schema-governance/PIN")

# 2. Jenkinsfile coverage
allow, via_cap = {}, {}
for l in read("scripts/ci/schema-sync-coverage-allowlist.txt").split("\n"):
    l = l.strip()
    if l and not l.startswith("#"):
        parts = [x.strip() for x in l.split("|")]
        if len(parts) < 3 or not parts[2]: fail("allow-list entry '%s' needs <file>|<kind>|<reason>" % (parts[0] if parts else l))
        name, kind, reason = parts[0], parts[1], "|".join(parts[2:])
        if kind == "ungated": allow[name] = reason
        elif re.fullmatch(r"via:direct=(?:[0-9a-f]{12}(?:,[0-9a-f]{12})*)?", kind): via_cap[name] = sorted(x for x in kind.split("=", 1)[1].split(",") if x)
        else: fail("allow-list entry '%s' has unknown kind '%s' (ungated | via:direct=<12-hex fingerprints, comma separated>)" % (name, kind))
seen = set()
for fn in sorted(os.listdir(ROOT)):
    if not fn.startswith("Jenkinsfile"): continue
    code = strip(read(fn))
    bodies = sh_bodies(code)
    direct_here = bodies
    via = VIA.search(bodies) is not None
    direct_stmts = [re.sub(r"\s+", " ", m.group(0)).strip() for m in re.finditer(r"(?:" + DIRECT.pattern + r")[^;&|\n]*", VIA.sub(" ", bodies))]
    direct_fps = sorted(hashlib.sha256(s.encode()).hexdigest()[:12] for s in direct_stmts)
    direct = len(direct_fps)
    if not (WORKLOAD.search(code) or via): continue
    seen.add(fn)
    if fn in allow: continue              # listed ungated with a reason: the reason is the review
    if via:
        cap = via_cap.get(fn)
        if direct and cap is None: fail("%s calls service-deploy.sh but also has %d direct workload command(s) in sh steps; review them and list it as 'via:direct=%s' with a reason" % (fn, direct, ",".join(direct_fps)))
        if cap is not None and direct_fps != cap:
            fail("%s: the direct workload commands in its sh steps changed. allow-list pins %s, found %s. Review each new or changed command (it must not roll an image or scale up), then update the fingerprints; the statements are: %s" % (fn, ",".join(cap) or "none", ",".join(direct_fps) or "none", " || ".join(direct_stmts)))
        continue
    if fn in via_cap: fail("%s is listed as via but does not execute service-deploy.sh" % fn)
    if fn not in allow: fail("%s changes workloads without really executing service-deploy.sh and is not listed 'ungated' in scripts/ci/schema-sync-coverage-allowlist.txt" % fn)
stale = sorted((set(allow) | set(via_cap)) - seen)
if stale: fail("allow-list names Jenkinsfiles with no workload-changing step (stale): %s" % ", ".join(stale))
print("schema-sync wiring OK: service-deploy.sh gates before its first apply; %d Jenkinsfile(s) go through it, %d outside it are listed 'ungated' with a reason" % (len([f for f in seen if f not in allow]), len(allow)))
