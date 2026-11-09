#!/usr/bin/env python3
"""Generate the manifest header `activeMirrors` from the mirror jobs of this repository (docs/schema-governance R3f).

  python3 scripts/ci/schema-mirrors-generate.py           rewrite schema-manifest/manifest.json's activeMirrors
  python3 scripts/ci/schema-mirrors-generate.py --check   exit 1 unless the committed header equals what the jobs say

Every mirror job states how it moves records in header comments the generator reads (`//` in a Jenkinsfile, `#` in a properties file):

  MIRROR-TRANSPORT: bytes | reencode      bytes = the record is copied unchanged (framed ids travel); reencode = decoded with the source
                                          registry and serialised with the target's
  MIRROR-TOPICS:    a, b | @choice(TOPIC)   the topics it carries; @choice(TOPIC) reads the job's own TOPIC choice parameter, and an
                                          explicit list must equal that parameter when the job has one
  MIRROR-SUBJECTS:  s1, s2 | none         the registry subjects those topics carry; `none` = no registry-framed value (JSON / plain
                                          bytes), which becomes "unframed": true and is a DECLARATION the validator cannot prove
  MIRROR-PATHS:     es4->prod; dev->mac74   optional; otherwise derived from the job's existing MIRROR-DIRECTION marker
                                          (es4->default = es4->dev + es4->prod, default->es4 = dev->es4 + prod->es4). An endpoint that
                                          is not a governed registry (dev, prod, es4) is listed under ungovernedPaths, never dropped.
  MIRROR-INSTALL: reencodes               required on a `reencode` job that has an `install` ACTION, unless the next marker applies:
  MIRROR-LEGACY-INSTALL: retired          the job's install predates the canonical mirror and byte-copies; it must be refused by a stage
                                          named 'Legacy byte-copy install is retired' in the file, so "reencode" is true of every path.

A Jenkinsfile that looks like a mirror (*mirror*, *bridge*, *reencoder*) or carries any of these markers, and infra/es4/mm2/mm2.properties,
but declares no MIRROR-TRANSPORT FAILS the generator: a mirror cannot be added without saying how it moves records.
A job that trips the NAME heuristic without being a mirror - a release job for a bridge, say - declares
`// NOT-A-MIRROR: <reason>` and is skipped; declaring both that and any MIRROR-* marker is an error,
and declaring neither still FAILS.
"""
import argparse, glob, json, os, re, sys

GOVERNED = ("dev", "prod", "es4")
LOOKS_LIKE_MIRROR = re.compile(r"mirror|bridge|reencoder", re.I)
RETIRE_STAGE = "Legacy byte-copy install is retired"
EXTRA_SOURCES = ["infra/es4/mm2/mm2.properties"]


def markers(text):
    out = {}
    for m in re.finditer(r"^\s*(?://|#)\s*(MIRROR-[A-Z-]+):[ \t]*(.*?)[ \t]*$", text, re.M):
        if m.group(1) in out:
            raise ValueError("%s declared twice" % m.group(1))
        out[m.group(1)] = m.group(2)
    return out


def split(value):
    return [x.strip() for x in value.split(",") if x.strip()]


def choice_topics(text):
    m = re.search(r"choice\(\s*name:\s*'TOPIC'\s*,\s*choices:\s*\[(.*?)\]", text, re.S)
    return re.findall(r"'([^']+)'", m.group(1)) if m else None


def direction_paths(direction):
    if direction == "es4->default":
        return [("es4", "dev"), ("es4", "prod")]
    if direction == "default->es4":
        return [("dev", "es4"), ("prod", "es4")]
    raise ValueError("unknown MIRROR-DIRECTION '%s'" % direction)


def job_mirrors(path, root):
    text = open(os.path.join(root, path)).read()
    name = os.path.basename(path)
    mk = markers(text)
    if "MIRROR-TRANSPORT" not in mk:
        raise ValueError("%s: no MIRROR-TRANSPORT marker; a mirror must declare how it moves records (R3f)" % name)
    transport = mk["MIRROR-TRANSPORT"]
    if transport not in ("bytes", "reencode"):
        raise ValueError("%s: MIRROR-TRANSPORT must be bytes or reencode, got '%s'" % (name, transport))
    for need in ("MIRROR-TOPICS", "MIRROR-SUBJECTS"):
        if not mk.get(need):
            raise ValueError("%s: no %s marker (R3f)" % (name, need))
    choice = choice_topics(text)
    if mk["MIRROR-TOPICS"] == "@choice(TOPIC)":
        if choice is None:
            raise ValueError("%s: MIRROR-TOPICS is @choice(TOPIC) but the job has no TOPIC choice parameter" % name)
        topics = choice
    else:
        topics = split(mk["MIRROR-TOPICS"])
        if choice is not None and sorted(choice) != sorted(topics):
            raise ValueError("%s: MIRROR-TOPICS %s differs from the job's TOPIC choice %s" % (name, topics, choice))
    if not topics:
        raise ValueError("%s: MIRROR-TOPICS is empty" % name)
    unframed = mk["MIRROR-SUBJECTS"] == "none"
    subjects = [] if unframed else split(mk["MIRROR-SUBJECTS"])
    if "MIRROR-PATHS" in mk:
        pairs = [tuple(x.strip() for x in p.split("->")) for p in mk["MIRROR-PATHS"].split(";") if p.strip()]
        if not pairs or any(len(p) != 2 or not all(p) for p in pairs):
            raise ValueError("%s: MIRROR-PATHS must be 'a->b; c->d'" % name)
    elif "MIRROR-DIRECTION" in mk:
        pairs = direction_paths(mk["MIRROR-DIRECTION"])
    else:
        raise ValueError("%s: needs MIRROR-PATHS or MIRROR-DIRECTION" % name)
    if transport == "reencode" and re.search(r"choice\(\s*name:\s*'ACTION'\s*,\s*choices:\s*\[[^\]]*'install'", text):
        kind = mk.get("MIRROR-INSTALL")
        if kind == "reencodes":
            pass
        elif mk.get("MIRROR-LEGACY-INSTALL") == "retired":
            if RETIRE_STAGE not in text:
                raise ValueError("%s: MIRROR-LEGACY-INSTALL is 'retired' but no stage named '%s' exists (R3f)" % (name, RETIRE_STAGE))
        else:
            raise ValueError("%s: a reencode job with an install ACTION must say either 'MIRROR-INSTALL: reencodes' (every action re-encodes) or 'MIRROR-LEGACY-INSTALL: retired' plus a stage named '%s' (R3f)" % (name, RETIRE_STAGE))
    governed = [(a, b) for a, b in pairs if a in GOVERNED and b in GOVERNED]
    ungoverned = ["%s->%s" % (a, b) for a, b in pairs if (a, b) not in governed]
    out = []
    for i, (a, b) in enumerate(sorted(set(governed))):
        rec = {"job": name, "from": a, "to": b, "transport": transport, "topics": sorted(topics), "subjects": sorted(subjects)}
        if unframed:
            rec["unframed"] = True
        if ungoverned and i == 0:
            rec["ungovernedPaths"] = ungoverned
        out.append(rec)
    if ungoverned and not out:
        raise ValueError("%s: every path is outside the governed registries %s; nothing to record" % (name, list(GOVERNED)))
    return out


def discover(root):
    found = []
    for p in sorted(glob.glob(os.path.join(root, "Jenkinsfile*"))):
        name = os.path.basename(p)
        text = open(p).read()
        if LOOKS_LIKE_MIRROR.search(name) or re.search(r"^\s*//\s*MIRROR-(DIRECTION|TRANSPORT):", text, re.M):
            # A job may DECLARE that it is not a mirror, and must give a reason. The name heuristic
            # catches *mirror*, *bridge* and *reencoder*, so it also catches jobs that merely DEPLOY
            # one — Jenkinsfile.rithmic-depth-bridge-release is a release job for a bridge, not a
            # mirror, and it moves no records at all. Excluding such names in this script instead
            # would be a blacklist: the next oddly-named job breaks it again, silently. So the
            # exclusion is a POSITIVE statement by the job, with its reason, and silence still FAILS —
            # the same discipline as MIRROR-TRANSPORT itself.
            m = re.search(r"^\s*//\s*NOT-A-MIRROR:\s*(\S.*?)\s*$", text, re.M)
            if m:
                if re.search(r"^\s*//\s*MIRROR-(DIRECTION|TRANSPORT|TOPICS|SUBJECTS):", text, re.M):
                    raise ValueError("%s: declares NOT-A-MIRROR and also carries MIRROR-* markers; "
                                     "decide which it is" % name)
                continue
            found.append(name)
    for extra in EXTRA_SOURCES:
        if os.path.exists(os.path.join(root, extra)):
            found.append(extra)
    return found


def generate(root):
    mirrors = []
    for f in discover(root):
        mirrors.extend(job_mirrors(f, root))
    return sorted(mirrors, key=lambda m: (m["job"], m["from"], m["to"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--manifest", default="schema-manifest/manifest.json")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    try:
        mirrors = generate(a.root)
    except ValueError as e:
        sys.exit("FAIL: " + str(e))
    mpath = os.path.join(a.root, a.manifest)
    manifest = json.load(open(mpath))
    if a.check:
        if manifest.get("activeMirrors") != mirrors:
            have = {json.dumps(m, sort_keys=True) for m in manifest.get("activeMirrors", [])}
            want = {json.dumps(m, sort_keys=True) for m in mirrors}
            for x in sorted(want - have): print("MISSING from the manifest header: " + x, file=sys.stderr)
            for x in sorted(have - want): print("STALE in the manifest header:     " + x, file=sys.stderr)
            sys.exit("FAIL: manifest activeMirrors differs from the mirror jobs; run scripts/ci/schema-mirrors-generate.py (R3f)")
        print("activeMirrors OK: %d mirror path(s) from %d job file(s)" % (len(mirrors), len({m['job'] for m in mirrors})))
        return
    manifest["activeMirrors"] = mirrors
    json.dump(manifest, open(mpath, "w"), indent=1)
    open(mpath, "a").write("\n")
    print("wrote %d mirror path(s) from %d job file(s)" % (len(mirrors), len({m['job'] for m in mirrors})))


if __name__ == "__main__":
    main()
