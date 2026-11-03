#!/usr/bin/env python3
"""Evidence for the manifest's `activeMirrors` declarations (docs/schema-governance R3f). READ-ONLY: samples, never writes, never joins a group.

For every BYTES mirror path the newest record of each topic is read on the mirror's SOURCE cluster and compared with the declaration:
  unframed topic   the record must NOT start with the Confluent magic byte 0x00
  framed topic     the record must start with 0x00 + a 4-byte schema id; the id is resolved in the source registry and the record name (or one
                   of the id's subjects) must be a declared subject
A topic with no record is UNMEASURED (reported, not passed). An unreachable broker or registry is a FAILURE. Proves the newest record only.
  python3 scripts/ci/schema-mirror-evidence.py [--root .]      exit 1 on any FAIL
"""
import argparse, json, os, subprocess, sys, urllib.request


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
        except Exception:
            subjects = []
        return full, subjects
    return lookup


def sample(bootstrap, topic):
    meta = subprocess.run(["kcat", "-b", bootstrap, "-L", "-t", topic, "-m", "10"], capture_output=True, timeout=40)
    if meta.returncode != 0:
        raise RuntimeError("broker %s unreachable: %s" % (bootstrap, meta.stderr.decode(errors="replace")[:200]))
    try:
        out = subprocess.run(["kcat", "-b", bootstrap, "-t", topic, "-C", "-o", "-1", "-c", "1", "-e", "-q", "-f", "%s"], capture_output=True, timeout=45)
    except subprocess.TimeoutExpired:
        return None
    return out.stdout[:16]


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--root", default="."); a = ap.parse_args()
    m = json.load(open(os.path.join(a.root, "schema-manifest/manifest.json")))
    kafka = json.load(open(os.path.join(a.root, "schema-manifest/kafka.json")))
    regs = json.load(open(os.path.join(a.root, "schema-manifest/registries.json")))
    seen, fails, counts = set(), 0, {"PASS": 0, "UNMEASURED": 0, "FAIL": 0}
    for mir in m["activeMirrors"]:
        if mir["transport"] != "bytes":
            continue
        for topic in mir["topics"]:
            key = (mir["from"], topic, tuple(mir["subjects"]), bool(mir.get("unframed")))
            if key in seen:
                continue
            seen.add(key)
            try:
                status, detail = classify(sample(kafka[mir["from"]], topic), set(mir["subjects"]), bool(mir.get("unframed")), make_lookup(regs[mir["from"]]))
            except Exception as e:
                status, detail = "FAIL", "%s" % e
            counts[status] += 1
            print("%-10s %s %s: %s" % (status, mir["from"], topic, detail))
    print("mirror evidence: %d pass, %d unmeasured, %d fail" % (counts["PASS"], counts["UNMEASURED"], counts["FAIL"]))
    sys.exit(1 if counts["FAIL"] else 0)


if __name__ == "__main__":
    main()
