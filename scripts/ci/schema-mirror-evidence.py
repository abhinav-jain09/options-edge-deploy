#!/usr/bin/env python3
"""Evidence for the manifest's `activeMirrors` declarations (docs/schema-governance R3f). READ-ONLY: samples, never writes, never joins a group.

For every BYTES mirror path the newest record of each topic is read on the mirror's SOURCE cluster and compared with the declaration:
  unframed topic   the record must NOT start with the Confluent magic byte 0x00
  framed topic     the record must start with 0x00 + a 4-byte schema id; the id is resolved in the source registry and the record name (or one
                   of the id's subjects) must be a declared subject
A topic with no record is UNMEASURED and a topic that does not exist on the source is ABSENT (neither is a pass: nothing was proven). ABSENT is tolerated ONLY for a
(environment, topic) listed with a reason in schema-manifest/mirror-evidence-absent.txt; an unlisted one FAILS (a declared source topic must not
disappear unnoticed), and a listed one that now exists FAILS until removed (the list stays true). Any other read
error, an unreachable broker or an unreachable registry is a FAILURE. Proves the newest record only.
  python3 scripts/ci/schema-mirror-evidence.py [--root .]      exit 1 on any FAIL
"""
import argparse, json, os, subprocess, sys, urllib.error, urllib.request


def classify(sample, declared, unframed, lookup):
    """-> (status, detail). `lookup(id)` -> (record full name, [subjects]) or raises."""
    if sample is None or len(sample) == 0:
        return "UNMEASURED", "no record to sample"
    framed = sample[0] == 0 and len(sample) >= 5
    if unframed:
        return ("FAIL", "declared unframed but the newest record is Confluent-framed (id %d)" % int.from_bytes(sample[1:5], "big")) if framed else ("PASS", "not framed")
    if not framed:
        return "FAIL", "declared framed (subjects %s) but the newest record has no Confluent frame" % sorted(declared)
    sid = int.from_bytes(sample[1:5], "big")
    name, subjects = lookup(sid)
    if name in declared or any(s in declared for s in subjects):
        return "PASS", "id %d = %s" % (sid, name)
    return "FAIL", "id %d resolves to %s (subjects %s), not one of the declared %s" % (sid, name, subjects, sorted(declared))


def make_lookup(registry):
    def get(path):
        with urllib.request.urlopen(registry.rstrip("/") + path, timeout=15) as r:
            return json.load(r)
    def lookup(sid):
        schema = json.loads(get("/schemas/ids/%d" % sid)["schema"])
        full = (schema.get("namespace", "") + "." if schema.get("namespace") else "") + schema.get("name", "")
        try:
            subjects = [x["subject"] for x in get("/schemas/ids/%d/versions" % sid)]
        except urllib.error.HTTPError as e:
            if e.code != 404:  # 404 = the id belongs to no live subject; anything else is an unreadable registry, never "no subjects"
                raise
            subjects = []
        return full, subjects
    return lookup


class TopicAbsent(RuntimeError):
    """The broker answered and said the topic does not exist: not a transport failure, and not evidence either."""


def run(argv, timeout):
    return subprocess.run(argv, capture_output=True, timeout=timeout)


def sample(bootstrap, topic, runner=run):
    meta = runner(["kcat", "-b", bootstrap, "-L", "-t", topic, "-m", "10"], 40)
    if meta.returncode != 0 and b"Unknown topic or partition" in meta.stderr:
        raise TopicAbsent("topic does not exist on %s" % bootstrap)
    if meta.returncode != 0:
        raise RuntimeError("broker %s unreachable: %s" % (bootstrap, meta.stderr.decode(errors="replace")[:200]))
    try:
        out = runner(["kcat", "-b", bootstrap, "-t", topic, "-C", "-o", "-1", "-c", "1", "-e", "-q", "-f", "%s"], 45)
    except subprocess.TimeoutExpired:
        return None
    if out.returncode != 0 and b"Unknown topic or partition" in out.stderr:
        raise TopicAbsent("topic does not exist on %s" % bootstrap)
    if out.returncode != 0:  # an empty topic exits 0; any error (unknown topic, auth, broker failure) exits non-zero and must never read as "no record"
        raise RuntimeError("reading %s on %s failed (rc=%d): %s" % (topic, bootstrap, out.returncode, out.stderr.decode(errors="replace")[:200]))
    return out.stdout[:16]


def evaluate(mir, topic, allowed, sampler, lookup_factory, bootstrap):
    """One mirror topic -> (status, detail); every error path is a FAIL except an explicitly listed ABSENT."""
    listed = (mir["from"], topic) in allowed
    try:
        status, detail = classify(sampler(bootstrap, topic), set(mir["subjects"]), bool(mir.get("unframed")), lookup_factory())
    except TopicAbsent as e:
        if listed:
            return "ABSENT", "%s (listed in mirror-evidence-absent.txt)" % e
        return "FAIL", "%s and is not listed in schema-manifest/mirror-evidence-absent.txt" % e
    except Exception as e:
        return "FAIL", "%s" % e
    if listed and status != "FAIL":
        return "FAIL", "listed as ABSENT in mirror-evidence-absent.txt but the topic exists now (%s); remove it from the list" % detail
    return status, detail


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--root", default="."); a = ap.parse_args()
    m = json.load(open(os.path.join(a.root, "schema-manifest/manifest.json")))
    kafka = json.load(open(os.path.join(a.root, "schema-manifest/kafka.json")))
    regs = json.load(open(os.path.join(a.root, "schema-manifest/registries.json")))
    allowed = set()
    ap_path = os.path.join(a.root, "schema-manifest/mirror-evidence-absent.txt")
    for line in open(ap_path):
        line = line.split("#")[0].strip()
        if line:
            env, topic = line.split()
            allowed.add((env, topic))
    seen, fails, counts = set(), 0, {"PASS": 0, "UNMEASURED": 0, "ABSENT": 0, "FAIL": 0}
    for mir in m["activeMirrors"]:
        if mir["transport"] != "bytes":
            continue
        for topic in mir["topics"]:
            key = (mir["from"], topic, tuple(mir["subjects"]), bool(mir.get("unframed")))
            if key in seen:
                continue
            seen.add(key)
            status, detail = evaluate(mir, topic, allowed, sample, lambda: make_lookup(regs[mir["from"]]), kafka[mir["from"]])
            counts[status] += 1
            print("%-10s %s %s: %s" % (status, mir["from"], topic, detail))
    print("mirror evidence: %d pass, %d unmeasured, %d absent, %d fail" % (counts["PASS"], counts["UNMEASURED"], counts["ABSENT"], counts["FAIL"]))
    sys.exit(1 if counts["FAIL"] else 0)


if __name__ == "__main__":
    main()
