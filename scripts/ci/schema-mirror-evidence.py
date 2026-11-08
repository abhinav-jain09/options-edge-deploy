#!/usr/bin/env python3
"""Evidence for the manifest's `activeMirrors` declarations (docs/schema-governance R3f). READ-ONLY: samples, never writes, never joins a group.

For every BYTES mirror path the newest record of each topic is read on the mirror's SOURCE cluster and compared with the declaration:
  unframed topic   the record must NOT start with the Confluent magic byte 0x00
  framed topic     the record must start with 0x00 + a 4-byte schema id; the id is resolved in the source registry and the record name (or one
                   of the id's subjects) must be a declared subject
A topic with no record, or whose newest record has a zero-byte value, is UNMEASURED and a topic that does not exist on the source is ABSENT (neither is a pass: nothing was proven). ABSENT is tolerated ONLY for a
(environment, topic) listed with a reason in schema-manifest/mirror-evidence-absent.txt; an unlisted one FAILS (a declared source topic must not
disappear unnoticed), and a listed one that now exists FAILS until removed (the list stays true). Any other read
error, a read that times out, an unreachable broker or an unreachable registry is a FAILURE. Proves the newest record only.

A FRAMED declaration whose source environment has not had its JSON -> Avro cutover yet is listed in schema-manifest/mirror-evidence-pending.txt
(`<environment> <topic> <cutover reference>`) and reports PENDING, which is NOT a pass and does NOT fail the run. PENDING is deliberately not the
`unframed` flag: `unframed` declares JSON the intended steady state and switches the framed id check off for good, which is exactly what must not
happen across a cutover. A pending entry instead ASSERTS the condition it stands for and expires on its own:
  * the newest source record must STILL be the pre-cutover serialization: unframed AND parsing as JSON. Once framing begins the entry FAILS and has to
    be removed, so the declared id is validated again; bytes that are neither a frame nor JSON FAIL too, and so does a zero-byte or null value, because
    none of those prove either serialization (an empty TOPIC stays UNMEASURED: there is no record to disagree with);
  * EVERY destination registry the topic is byte-copied to must already map the manifest's fixed id to the declared subject (one id in every registry
    is the whole reason a byte-copied frame decodes on the far side), so a pending source is never the reason the destination side goes unchecked;
  * a pending entry on an `unframed` declaration, on a declaration with no subject at all, on a subject that has no single fixed id, listed in
    mirror-evidence-absent.txt too, or naming no declared mirror source topic at all, is refused.
The destination proof runs BEFORE the sample is looked at, so an empty pending topic is not a night on which the destination went unchecked. The SOURCE
registry is deliberately not contacted while pending: registering the subject there is the cutover step that is outstanding.
A value that starts with 0x00 but is shorter than the five bytes a frame needs is a FAILURE everywhere: it is a truncated or ambiguous record, evidence
of neither serialization, and reading it as "not framed" would let a pending marker stand on nothing.
  python3 scripts/ci/schema-mirror-evidence.py [--root .]      exit 1 on any FAIL
"""
import argparse, json, os, subprocess, sys, urllib.error, urllib.request


def classify(sample, declared, unframed, lookup, pending=None, dest_proof=None):
    """-> (status, detail). `lookup(id)` -> (record full name, [subjects]) or raises. `pending` is the cutover reference when this
    (source environment, topic) is listed in mirror-evidence-pending.txt; `dest_proof()` -> (ok, detail) proves the destination registry."""
    if pending and unframed:
        return "FAIL", ("listed in mirror-evidence-pending.txt (%s) but the mirror is declared unframed: a pending cutover is only meaningful for a "
                        "FRAMED declaration, and the `unframed` flag is not a pending state" % pending)
    dest = None
    if pending:
        # BEFORE the sample is looked at, so an empty pending topic cannot be the reason the destination side goes unchecked for a night.
        ok, dest = dest_proof()
        if not ok:
            return "FAIL", "cutover pending (%s) but the destination registry is not ready for the fixed id: %s" % (pending, dest)
    if sample is None:
        return "UNMEASURED", "no record to sample" + (" (cutover pending (%s); %s)" % (pending, dest) if pending else "")
    if len(sample) == 0:
        # A record EXISTS and its value is zero bytes (a null tombstone, or an empty value). That is not "no record": on a pending source it proves
        # neither serialization, so the marker must not stand on it. For the other declarations it stays UNMEASURED — a tombstone is legitimate on the
        # compacted mirror topics, and reddening those on an unverified assumption is the very failure this check exists to avoid.
        if pending:
            return "FAIL", ("listed in mirror-evidence-pending.txt (%s) but the newest record's value is zero bytes: a null or empty value is evidence "
                            "of neither serialization, so the marker does not stand" % pending)
        return "UNMEASURED", "the newest record's value is zero bytes"
    if sample[0] == 0 and len(sample) < 5:
        # 0x00 then fewer than the four id bytes: a truncated or ambiguous value. It is not a readable frame and it is not proof of JSON either, so it
        # must never be reported as "not framed" — on a pending source that would read as "still unframed" and hold the marker open on nothing.
        return "FAIL", "the newest record starts with the Confluent magic byte 0x00 but is only %d byte(s) long: a truncated frame is evidence of neither serialization" % len(sample)
    framed = sample[0] == 0
    if unframed:
        return ("FAIL", "declared unframed but the newest record is Confluent-framed (id %d)" % int.from_bytes(sample[1:5], "big")) if framed else ("PASS", "not framed")
    if pending:
        if framed:
            return "FAIL", ("listed in mirror-evidence-pending.txt (%s) but the newest record is now Confluent-framed (id %d): the cutover has "
                            "happened — remove the entry so the declared id is validated" % (pending, int.from_bytes(sample[1:5], "big")))
        # "not framed" is not the same as "still the pre-cutover serialization". A pending topic is awaiting a JSON -> Avro roll-forward, so the only
        # value that keeps the marker standing is one that PARSES as JSON; corrupt or foreign bytes must turn the build red, not read as "still JSON".
        try:
            json.loads(sample.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as e:
            return "FAIL", ("listed in mirror-evidence-pending.txt (%s) and the newest record is not Confluent-framed, but it does not parse as the "
                            "pre-cutover JSON either (%s): neither serialization is proven, so the marker does not stand" % (pending, e))
        return "PENDING", "cutover pending (%s): the newest record is still valid pre-cutover JSON, and %s" % (pending, dest)
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


def make_dest_proof(entries, subjects, dest_envs, registries, lookup_factory=make_lookup):
    """EVERY destination of a pending mirror must ALREADY carry the manifest's fixed id for every declared subject: the first frame the cutover
    produces is byte-copied there and names its schema by registry id. `dest_envs` is every environment a byte mirror copies this source topic to --
    the run de-duplicates identical source declarations, so the destinations have to be collected before that, or a second destination would never be
    looked at. -> a callable giving (ok, detail); every error path is `not ok`, never a silent pass."""
    def proof():
        if not subjects:
            # A framed declaration with no subject has no fixed id to prove anywhere, so a pending entry on it would prove nothing on either side and
            # would suppress the mirror's evidence for as long as it stood. (Outside pending this case already fails on the declared-subject check.)
            return False, "the mirror declares no subject, so there is no fixed id to prove on any destination"
        if not dest_envs:
            return False, "no byte mirror declares a destination for this topic"
        shown = []
        for dest_env in sorted(set(dest_envs)):
            dest_registry = registries.get(dest_env)
            if dest_registry is None:
                return False, "no registry is declared for the destination environment %s" % dest_env
            lookup = lookup_factory(dest_registry)
            for subject in sorted(subjects):
                entry = entries.get(subject)
                if entry is None:
                    return False, "%s is not a manifest entry" % subject
                if "idByEnvironment" in entry:
                    return False, "%s has per-environment ids, so no single id can name it on both sides of a byte copy" % subject
                if dest_env not in entry.get("environments", []):
                    return False, "the manifest does not list %s among the environments of %s" % (dest_env, subject)
                sid = entry["id"]
                try:
                    name, got = lookup(sid)
                except Exception as e:
                    return False, "reading id %d from the %s registry failed: %s" % (sid, dest_env, e)
                if subject != name and subject not in got:
                    return False, "id %d on %s is %s (subjects %s), not %s" % (sid, dest_env, name, got, subject)
                shown.append("%s %s = %d" % (dest_env, subject, sid))
        return True, "the destination registries already map %s" % ", ".join(shown)
    return proof


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
        # The literal "r" ahead of %s is a RECORD-PRESENCE sentinel. With a bare "%s" an empty topic and a newest record whose value is zero bytes
        # (a null tombstone) both print nothing, so "no record at all" could not be told from "a record proving nothing" — and on a pending source the
        # second one has to be a failure, not a quiet UNMEASURED. Verified against the prod broker: literal text in -f is emitted verbatim, and an
        # empty topic still prints zero bytes.
        out = runner(["kcat", "-b", bootstrap, "-t", topic, "-C", "-o", "-1", "-c", "1", "-e", "-q", "-f", "r%s"], 45)
    except subprocess.TimeoutExpired as e:
        # A hung consumer read is a READ ERROR, never "no record". As None it became UNMEASURED, which on a pending source would leave the marker
        # standing without the source having been proven still JSON -- and it contradicts this check's own rule that any read error is a FAILURE.
        raise RuntimeError("reading %s on %s timed out after %ss" % (topic, bootstrap, getattr(e, "timeout", "?")))
    if out.returncode != 0 and b"Unknown topic or partition" in out.stderr:
        raise TopicAbsent("topic does not exist on %s" % bootstrap)
    if out.returncode != 0:  # an empty topic exits 0; any error (unknown topic, auth, broker failure) exits non-zero and must never read as "no record"
        raise RuntimeError("reading %s on %s failed (rc=%d): %s" % (topic, bootstrap, out.returncode, out.stderr.decode(errors="replace")[:200]))
    if not out.stdout:
        return None                 # no record at all on the topic
    return out.stdout[1:]           # the WHOLE value past the sentinel, b"" for a zero-byte or null value


def evaluate(mir, topic, allowed, sampler, lookup_factory, bootstrap, pending=None, dest_proof=None):
    """One mirror topic -> (status, detail); every error path is a FAIL except an explicitly listed ABSENT."""
    listed = (mir["from"], topic) in allowed
    try:
        status, detail = classify(sampler(bootstrap, topic), set(mir["subjects"]), bool(mir.get("unframed")), lookup_factory(),
                                  pending=pending, dest_proof=dest_proof)
    except TopicAbsent as e:
        if listed:
            return "ABSENT", "%s (listed in mirror-evidence-absent.txt)" % e
        return "FAIL", "%s and is not listed in schema-manifest/mirror-evidence-absent.txt" % e
    except Exception as e:
        return "FAIL", "%s" % e
    if listed and status != "FAIL":
        return "FAIL", "listed as ABSENT in mirror-evidence-absent.txt but the topic exists now (%s); remove it from the list" % detail
    return status, detail


def read_pending(path, allowed):
    """mirror-evidence-pending.txt -> {(environment, topic): cutover reference}. A malformed, duplicated or absent-listed line is refused outright:
    the file is a ledger of outstanding cutovers, so a line nobody can read must never silently widen what the run tolerates."""
    pending = {}
    if not os.path.exists(path):
        return pending
    with open(path) as fh:
        lines = fh.readlines()
    for lineno, raw in enumerate(lines, 1):
        line = raw.split("#")[0].strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3 or not parts[2].strip():
            raise SystemExit("mirror-evidence-pending.txt:%d: expected `<environment> <topic> <cutover reference>`, got %r" % (lineno, line))
        key, ref = (parts[0], parts[1]), parts[2].strip()
        if key in pending:
            raise SystemExit("mirror-evidence-pending.txt:%d: %s %s is listed twice" % (lineno, key[0], key[1]))
        if key in allowed:
            raise SystemExit("mirror-evidence-pending.txt:%d: %s %s is also listed in mirror-evidence-absent.txt; a topic cannot be both absent by "
                             "design and awaiting a cutover" % (lineno, key[0], key[1]))
        pending[key] = ref
    return pending


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--root", default="."); a = ap.parse_args()
    m = json.load(open(os.path.join(a.root, "schema-manifest/manifest.json")))
    kafka = json.load(open(os.path.join(a.root, "schema-manifest/kafka.json")))
    regs = json.load(open(os.path.join(a.root, "schema-manifest/registries.json")))
    entries = {e["subject"]: e for e in m["entries"] if e.get("subject")}
    allowed = set()
    ap_path = os.path.join(a.root, "schema-manifest/mirror-evidence-absent.txt")
    for line in open(ap_path):
        line = line.split("#")[0].strip()
        if line:
            env, topic = line.split()
            allowed.add((env, topic))
    pending = read_pending(os.path.join(a.root, "schema-manifest/mirror-evidence-pending.txt"), allowed)
    dests = {}   # (source environment, topic) -> every environment a byte mirror copies it to; collected before the de-duplication below
    for mir in m["activeMirrors"]:
        if mir["transport"] == "bytes":
            for topic in mir["topics"]:
                dests.setdefault((mir["from"], topic), set()).add(mir["to"])
    seen, used, counts = set(), set(), {"PASS": 0, "PENDING": 0, "UNMEASURED": 0, "ABSENT": 0, "FAIL": 0}
    for mir in m["activeMirrors"]:
        if mir["transport"] != "bytes":
            continue
        for topic in mir["topics"]:
            key = (mir["from"], topic, tuple(mir["subjects"]), bool(mir.get("unframed")))
            if key in seen:
                continue
            seen.add(key)
            ref = pending.get((mir["from"], topic))
            if ref:
                used.add((mir["from"], topic))
            status, detail = evaluate(mir, topic, allowed, sample, lambda: make_lookup(regs[mir["from"]]), kafka[mir["from"]], pending=ref,
                                      dest_proof=make_dest_proof(entries, mir["subjects"], dests[(mir["from"], topic)], regs))
            counts[status] += 1
            print("%-10s %s %s: %s" % (status, mir["from"], topic, detail))
    for env, topic in sorted(set(pending) - used):
        counts["FAIL"] += 1
        print("%-10s %s %s: listed in mirror-evidence-pending.txt but no byte-copy mirror declares it as a source topic; remove the entry" % ("FAIL", env, topic))
    print("mirror evidence: %d pass, %d pending, %d unmeasured, %d absent, %d fail" % (
        counts["PASS"], counts["PENDING"], counts["UNMEASURED"], counts["ABSENT"], counts["FAIL"]))
    sys.exit(1 if counts["FAIL"] else 0)


if __name__ == "__main__":
    main()
