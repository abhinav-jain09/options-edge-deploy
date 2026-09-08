"""oe_corpus_reader — THE reader of the Candle Direction calibration corpus (A5.4/A5.5/A5.6).

There is exactly one of these on purpose. The progress reporter (A5.7) and the PushValidationArtifact
evaluator (A5.8) both decide whether a session is COMPLETE, and if they each carried their own copy of
that judgement the two would drift: the reporter would say the corpus is complete on a day the
evaluator would call NOT_EVALUABLE, and no third thing would notice. Every rule below — the logical
collapse, the conflict, the chain recomputation, the owed-session calendar — lives here once.

Nothing in this module is authorizing. It reads files and reports what it found.
"""
import gzip, json, os, re, sys, glob, hashlib, datetime

HORIZONS = ("H3", "H5", "H15")


def canonical(o):
    """A1's canonical JSON: keys sorted, no whitespace, every scalar a string."""
    if isinstance(o, dict):
        return "{" + ",".join('%s:%s' % (json.dumps(k), canonical(v)) for k, v in sorted(o.items())) + "}"
    if isinstance(o, list):
        return "[" + ",".join(canonical(v) for v in o) + "]"
    if o is None:
        return "null"
    if isinstance(o, bool):
        return "true" if o else "false"
    if isinstance(o, str):
        return json.dumps(o)
    return json.dumps(str(o))


def cell_key(call):
    """A4.9's cell, with the hash and horizon factored out (A5.8).

    `roles` is a LIST in the record because it is a List<String> in the engine, and the engine's own
    cellPrefix joins it with "+". Reading it as if it were a string renders "['CALL_WALL']", which
    matches no declared cell — so every required cell would count ZERO forever and the per-cell
    threshold could never be met by any amount of real data. One definition, used by every caller.
    """
    roles = (call.get("node") or {}).get("roles")
    if isinstance(roles, (list, tuple)):
        roles = "+".join(str(r) for r in roles) if roles else "NONE"
    elif not roles:
        roles = "NONE"
    return "%s|%s|%s" % (call.get("enteredState"), roles, call.get("regime"))


def physical_key(rec):
    """The key the PRODUCER wrote, reconstructed from the payload — not a tuple the reader invents."""
    if rec.get("kind") == "seal":
        return "%s|%s|%s" % (rec.get("sessionDate"), rec.get("parameterSetHash"), rec.get("sessionLineageId"))
    lid = rec.get("callId") if rec.get("kind") == "call" else "%s|%s" % (rec.get("callId"), rec.get("horizon"))
    return "%s|%s|%s" % (rec.get("parameterSetHash"), rec.get("sessionLineageId"), lid)


def read_logical(root):
    """Collapse every archived record by (key, recomputed digest).

    A5.3: equal key + equal digest is ONE logical record; equal key + a different digest is a CONFLICT
    and poisons its session. The digest is RECOMPUTED from the payload — a digest carried in the record
    is diagnostic only, or a corrupted payload would verify itself.

    A read error is NOT a quiet day. Swallowing one would let a truncated archive report a smaller
    population as complete, which is the exact failure this reader exists to catch.
    """
    logical, files, coords = {}, 0, {}
    conflicts_by_session, read_errors_by_session, bad_keys = {}, {}, {}
    for f in sorted(glob.glob(os.path.join(root, "dt=*", "*.jsonl.gz"))):
        files += 1
        try:
            with gzip.open(f, "rt") as fh:
                for lineno, line in enumerate(fh, 1):
                    i = line.find("{")
                    if i < 0:
                        continue
                    try:
                        rec = json.loads(line[i:])
                    except Exception:
                        dt = re.search(r"dt=(\d{4}-\d{2}-\d{2})", f)
                        read_errors_by_session.setdefault(dt.group(1) if dt else "?", []).append(
                            "%s:%d unparseable" % (os.path.basename(f), lineno))
                        continue
                    if "kind" not in rec:
                        continue
                    body = {k: v for k, v in rec.items()
                            if k not in ("ts", "publishedAtMs", "runId", "semanticDigest")}
                    dig = hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()
                    pkey = physical_key(rec)
                    prefix = line[:i]
                    m = re.search(r"Offset:(\d+)", prefix)
                    off = int(m.group(1)) if m else None
                    pm = re.search(r"Partition:(\d+)", prefix)
                    part = int(pm.group(1)) if pm else None
                    # the ACTUAL Kafka key, not one derived from the value: a record written under a
                    # different key than its payload implies is exactly the substitution to catch
                    km = re.search(r"Offset:\d+\s+(\S+)\s*$", prefix) or re.search(r"\s(\S+)\s*$", prefix)
                    actual_key = km.group(1) if km else None
                    sd_of = rec.get("sessionDate")
                    if actual_key is not None and actual_key != pkey:
                        bad_keys.setdefault(sd_of, []).append(actual_key)
                    coords[pkey] = (part, off)
                    prev = logical.get(pkey)
                    if prev is None:
                        logical[pkey] = (dig, rec, off)
                    elif prev[0] != dig:
                        conflicts_by_session[sd_of] = conflicts_by_session.get(sd_of, 0) + 1
                    elif off is not None and (prev[2] is None or off < prev[2]):
                        logical[pkey] = (dig, rec, off)      # A5.4: keep the LOWEST physical offset
        except Exception as e:
            dt = re.search(r"dt=(\d{4}-\d{2}-\d{2})", f)
            read_errors_by_session.setdefault(dt.group(1) if dt else "?", []).append(
                "%s unreadable: %s" % (os.path.basename(f), e.__class__.__name__))
    # A flat list too: a per-session error is invisible to any caller that only walks SESSIONS, because a
    # file that will not open may be the only evidence a session existed at all (r7 #3).
    flat = []
    for sd, errs in sorted(read_errors_by_session.items()):
        flat += ["%s: %s" % (sd, e) for e in errs]
    return {"logical": logical, "files": files, "conflictsBySession": conflicts_by_session,
            "readErrorsBySession": read_errors_by_session, "readErrors": flat,
            "badKeys": bad_keys, "coords": coords,
            "generation": topic_generation(root)}


def canonical_topic_id(raw):
    """Kafka's CLI prints a TopicId as Uuid.toString() — base64url of the 16 bytes, no padding — while
    the producer stamps the seal with the canonical lower-case hex UUID. Two spellings of one
    generation can never be compared, so a topic recreation could not be tied across producer, archive
    and manifest (r8 #3). This normalises to the producer's spelling ON READ; the archiver's stored file
    is deliberately left alone, because it is compared byte-for-byte against the CLI on the next run and
    rewriting it would fire a spurious reset.
    """
    if not raw:
        return None
    v = raw.strip()
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", v):
        return v.lower()
    try:
        import base64
        b = base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
        if len(b) != 16:
            return v.lower()
        h = b.hex()
        return "%s-%s-%s-%s-%s" % (h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
    except Exception:
        return v.lower()


def topic_generation(root):
    """The Kafka TopicId the archiver recorded, as its canonical lower-case hex UUID. Offsets restart
    after a recreation, so an ordering without the generation is not a total order (A5.6).

    The archiver's file is <topic>.identity. Globbing *.id matched nothing, so every manifest carried
    generation: null and the coordinate was not actually a coordinate (r8 #3).
    """
    for cand in (os.path.join(os.path.dirname(root.rstrip("/")), "_manifest"),
                 os.path.join(root, "..", "_manifest")):
        try:
            for f in sorted(glob.glob(os.path.join(cand, "*.identity"))):
                for line in open(f):
                    if line.startswith("topic_id="):
                        return canonical_topic_id(line.split("=", 1)[1])
        except Exception:
            continue
    return None


def read_sidecar(root, read_errors):
    """The archiver's own discontinuity sidecar: a log reset it saw poisons every session it spans."""
    out = []
    for f in glob.glob(os.path.join(os.path.dirname(root.rstrip("/")), "_manifest", "*.discontinuities.jsonl")) + \
             glob.glob(os.path.join(root, "..", "_manifest", "*.discontinuities.jsonl")):
        try:
            for line in open(f):
                out.append(json.loads(line))
        except Exception:
            read_errors.append("discontinuity sidecar unreadable")
    return out


def chain(domain, recs):
    """A5.4's chain, recomputed from the archive in lowest-offset order.

    This is the PROOF. Counts alone accept a missing record paired with a substituted one, and a wrong
    seal would pass. The length prefix on the digest is 32 BY CONTRACT, matching the producer.
    """
    d = b"\x00" * 32
    for _off, pkey, dig in sorted(recs, key=lambda r: (r[0] if r[0] is not None else 0, r[1])):
        k = pkey.encode("utf-8")
        raw = bytes.fromhex(dig)
        d = hashlib.sha256(d + bytes([domain]) + len(k).to_bytes(4, "big") + k
                           + len(raw).to_bytes(4, "big") + raw).digest()
    return d.hex()


def classify_sessions(read, sidecar, today):
    """Per-session archiveStatus. Only COMPLETE sessions may enter any counter or any statistic.

    Every branch below is a way the corpus can be wrong that a count would not notice. The order
    matters: an unreadable file is reported as CORRUPT rather than as a smaller session.
    """
    logical = read["logical"]
    conflicts_by_session = read["conflictsBySession"]
    read_errors_by_session = read["readErrorsBySession"]
    bad_keys = read["badKeys"]
    seals, calls, outcomes = {}, [], []
    for _pkey, (_d, rec, _off) in logical.items():
        kind = rec.get("kind")
        if kind == "seal":
            seals[(rec.get("sessionDate"), rec.get("parameterSetHash"))] = rec
        elif kind == "call":
            calls.append(rec)
        elif kind == "outcome":
            outcomes.append(rec)

    sessions = {}
    for (sd, ph), seal in seals.items():
        lin = seal.get("sessionLineageId")
        # LINEAGE-SCOPED: records of another lineage of the same session are not this seal's population
        mine_c = [(o, k, dg) for k, (dg, r, o) in logical.items()
                  if r.get("kind") == "call" and r.get("sessionDate") == sd
                  and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin]
        mine_o = [(o, k, dg) for k, (dg, r, o) in logical.items()
                  if r.get("kind") == "outcome" and r.get("sessionDate") == sd
                  and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin]
        got_calls, got_out = len(mine_c), len(mine_o)
        want_calls = int(seal.get("logicalCallCount", -1))
        want_out = int(seal.get("logicalOutcomeCount", -1))
        # Every join here is scoped by HASH as well as lineage. Without the hash a call under one
        # parameter set answers for an outcome under another, and a cross-substituted session reads
        # COMPLETE (r7 #4).
        call_ids = {r.get("callId") for _dg, r, _o in logical.values()
                    if r.get("kind") == "call" and r.get("sessionDate") == sd
                    and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin}
        orphans = [r.get("callId") for _dg, r, _o in logical.values()
                   if r.get("kind") == "outcome" and r.get("sessionDate") == sd
                   and r.get("parameterSetHash") == ph
                   and r.get("sessionLineageId") == lin and r.get("callId") not in call_ids]
        horizons = {}
        for _dg, r, _o in logical.values():
            if r.get("kind") == "outcome" and r.get("sessionDate") == sd \
                    and r.get("parameterSetHash") == ph and r.get("sessionLineageId") == lin:
                horizons.setdefault(r.get("callId"), set()).add(r.get("horizon"))
        bad_horizons = [c for c in call_ids if horizons.get(c, set()) != set(HORIZONS)]
        # ONLY a discontinuity dated for THIS session poisons it
        spans = [d for d in sidecar if d.get("dt") == sd]
        errs = read_errors_by_session.get(sd, [])
        offs = sorted(o for o, _k, _d in (mine_c + mine_o) if o is not None)
        want_first, want_last = seal.get("firstOffset"), seal.get("lastOffset")
        # A5.2 pins these on EVERY record, the seal included. delivery and trackFromPush were checked on
        # outcomes only, and semanticStamp on nothing at all — a corpus whose records carry no semantic
        # stamp cannot be told apart from one built under different literals (r7 #4).
        required = ("parameterSetHash", "sessionLineageId", "sessionDate", "phaseAtCall",
                    "trackFromPush", "delivery", "semanticStamp")
        missing_fields = []
        for _dg, r, _o in logical.values():
            if r.get("sessionDate") != sd or r.get("parameterSetHash") != ph \
                    or r.get("sessionLineageId") != lin or r.get("kind") == "seal":
                continue
            missing_fields += ["%s:%s" % (r.get("kind"), f) for f in required if r.get(f) in (None, "")]
        att_bad = attrition_violations(seal)
        # A5.2 pins these on the seal itself. A seal missing one describes a population whose cohort,
        # lineage or clock nobody can establish, and the counts below would be counts of an unknown
        # thing (r6 #6).
        seal_missing = [f for f in ("parameterSetHash", "sessionLineageId", "sessionDate",
                                    "trackFromPush", "delivery", "ledgerTopic", "generation",
                                    "phaseAtCall", "semanticStamp",
                                    "callsDigest", "outcomesDigest")
                        if seal.get(f) in (None, "")]
        # An envelope is a coordinate or it is nothing: a nonempty session whose seal carries no offset
        # bounds cannot be checked against the archive at all, and the old test simply skipped it.
        envelope_missing = bool(offs) and (want_first is None or want_last is None)
        if errs:
            status, why = "CORRUPT", "; ".join(errs[:3])
        elif bad_keys.get(sd):
            status, why = "CORRUPT", "%d record(s) written under a key their payload does not imply" % len(bad_keys[sd])
        elif seal.get("conflicts", 0) or conflicts_by_session.get(sd):
            status, why = "CORRUPT", "conflicting records for one key"
        elif att_bad:
            status, why = "CORRUPT", "attrition rows violate A4.12 shape/arithmetic at hour(s) %s" % att_bad
        elif seal_missing:
            status, why = "INCOMPLETE", "the seal is missing pinned field(s): %s" % seal_missing
        elif envelope_missing:
            status, why = "INCOMPLETE", "the seal carries no offset envelope for a session that has records"
        elif offs and want_first is not None and (offs[0] != want_first or offs[-1] != want_last):
            status, why = "CORRUPT", "archived offsets [%s,%s] are not the seal's [%s,%s]" % (
                offs[0], offs[-1], want_first, want_last)
        elif offs and len(offs) != len(set(offs)):
            status, why = "CORRUPT", "duplicate offsets in the archive"
        elif missing_fields:
            status, why = "INCOMPLETE", "records missing pinned fields: %s" % sorted(set(missing_fields))[:4]
        elif seal.get("discontinuities") or spans:
            status, why = "DISCONTINUITY", ";".join(seal.get("discontinuities") or []) or "archiver recorded a log reset"
        elif orphans:
            status, why = "CORRUPT", "%d outcome(s) belong to no call in this lineage" % len(orphans)
        elif bad_horizons:
            status, why = "INCOMPLETE", "%d call(s) do not have exactly H3/H5/H15" % len(bad_horizons)
        elif got_calls != want_calls or got_out != want_out:
            status, why = "INCOMPLETE", "archived %d/%d calls, %d/%d outcomes" % (
                got_calls, want_calls, got_out, want_out)
        elif want_out != 3 * want_calls:
            status, why = "INCOMPLETE", "the seal itself is not 3 horizons per call"
        elif chain(0x01, mine_c) != seal.get("callsDigest") or chain(0x02, mine_o) != seal.get("outcomesDigest"):
            status, why = "CORRUPT", "the seal's chains are not reproducible from the archive"
        else:
            status, why = "COMPLETE", ""
        sessions["%s|%s|%s" % (sd, ph, lin)] = {
            "sessionDate": sd, "sessionLineageId": lin, "parameterSetHash": ph,
            "archiveStatus": status, "reason": why, "calls": got_calls, "outcomes": got_out,
            "phase": "CALIBRATION", "trackFromPush": seal.get("trackFromPush"),
            "attritionRows": len(seal.get("attrition", []) or [])}

    # A day with records but no seal has not finished. It AGES: unsealed by the following close it is
    # INCOMPLETE, not perpetually "pending" — a status that never changes is a status nobody acts on.
    for c in calls:
        k = "%s|%s|%s" % (c.get("sessionDate"), c.get("parameterSetHash"), c.get("sessionLineageId"))
        if k not in sessions:
            sd = c.get("sessionDate")
            aged = sd < today
            sessions[k] = {"sessionDate": sd, "sessionLineageId": c.get("sessionLineageId"),
                           "parameterSetHash": c.get("parameterSetHash"),
                           "archiveStatus": "INCOMPLETE" if aged else "PENDING_SEAL",
                           "reason": "records archived, no seal by the following close" if aged
                                     else "records archived, seal not yet written",
                           "calls": 0, "outcomes": 0, "phase": "CALIBRATION",
                           "trackFromPush": None, "attritionRows": 0}
    return sessions, seals, calls, outcomes


def attrition_violations(seal):
    """A4.12's shape and arithmetic, checked BEFORE any number from attrition is quoted (A5.8 r3 #6)."""
    bad = []
    for row in (seal.get("attrition") or []):
        graded = row.get("graded")
        reasons = [v for k, v in row.items() if k not in ("sessionDate", "etHour", "graded")
                   and isinstance(v, (int, float))]
        if not isinstance(graded, (int, float)) or graded < 0 or any(v < 0 for v in reasons) \
                or sum(reasons) > graded:
            bad.append(row.get("etHour"))
    return bad


def session_refusal_rate(seal):
    """A5.8's ONE equation, pooled over hours and reasons — not a worst hour and not a worst reason.

        rate = Σ_hours Σ_reasons count(hour, reason) / Σ_hours graded(hour)

    Σ graded == 0 makes the session NOT_EVALUABLE, never a rate of zero: a session in which nothing
    was graded is one about which refusal says nothing, and calling that 0% would let a blind day
    read as a perfect one.
    """
    refused = graded = 0
    for row in (seal.get("attrition") or []):
        g = row.get("graded")
        if not isinstance(g, (int, float)):
            return None
        graded += g
        refused += sum(v for k, v in row.items()
                       if k not in ("sessionDate", "etHour", "graded") and isinstance(v, (int, float)))
    if graded == 0:
        return None
    return float(refused) / float(graded)


def calendar_identity(cal):
    """WHICH calendar the owed-day list came from, as part of the pin (r8 #7). Two runs that disagree
    about a holiday produce two different owed-day lists and therefore two different verdicts from the
    same archive — so the calendar is an INPUT, and an input outside the pin is an input nobody can
    re-run against."""
    if cal is None:
        return None
    try:
        import inspect, sys as _s
        mod = inspect.getmodule(type(cal))
        src = inspect.getsource(mod).encode("utf-8")
        tz = None
        try:
            import zoneinfo
            tz = getattr(zoneinfo, "TZPATH", None) and "zoneinfo"
        except Exception:
            tz = None
        return {"module": hashlib.sha256(src).hexdigest(),
                "extraHolidays": sorted(d.isoformat() for d in getattr(cal, "_extra_holidays", set())),
                "extraEarlyCloses": sorted(d.isoformat() for d in getattr(cal, "_extra_early_closes", set())),
                "tz": tz, "python": "%d.%d" % _s.version_info[:2]}
    except Exception:
        return None


def build_manifest(read, topic, env, corpus_start=None, cal=None):
    """A5.6's manifest, which IS the corpus version.

    The old version hashed a simplified view of the live archive, which is not a pin: a mutable pointer
    means a calibration cannot be re-run against the inputs it actually used, and recomputing the hash
    after a deletion produced a new self-consistent value (r7 #6). This is the real thing — a FULL
    enumeration, ordered by (topic, generation, partition, offset) because offsets restart after a
    recreation and an ordering without the generation is not a total order, and naming the high-water
    mark as a whole coordinate rather than a bare offset so membership is unambiguous.
    """
    logical, coords = read["logical"], read["coords"]
    generation = read.get("generation")
    entries = []
    for pkey, (dig, rec, _off) in logical.items():
        part, off = coords.get(pkey, (None, None))
        entries.append({"topic": topic, "generation": generation,
                        "partition": -1 if part is None else part,
                        "offset": -1 if off is None else off,
                        "key": pkey, "kind": rec.get("kind"),
                        "sessionDate": rec.get("sessionDate"), "digest": dig})
    entries.sort(key=lambda e: (e["topic"] or "", e["generation"] or "", e["partition"], e["offset"], e["key"]))
    hwm = entries[-1] if entries else None
    manifest = {
        "manifestVersion": 2, "env": env, "topic": topic, "generation": generation,
        # the preregistered window start and the calendar are INPUTS to every owed-day judgement made
        # against this version, so they are part of the version (r8 #7)
        "corpusStartDate": corpus_start, "calendar": calendar_identity(cal),
        "highWaterMark": None if hwm is None else {"generation": hwm["generation"],
                                                   "partition": hwm["partition"], "offset": hwm["offset"]},
        "recordCount": len(entries), "filesRead": read["files"],
        "entries": entries,
    }
    return manifest


def manifest_version(manifest):
    return hashlib.sha256(canonical(manifest).encode("utf-8")).hexdigest()


def publish_manifest(out_root, manifest):
    """Published by ATOMIC RENAME to corpus/<corpusVersion>/manifest.json and never mutated afterwards.
    A version that already exists is left exactly as it is — rewriting it is the one thing a pin may
    never do."""
    version = manifest_version(manifest)
    d = os.path.join(out_root, "corpus", version)
    path = os.path.join(d, "manifest.json")
    if os.path.exists(path):
        return version, path, False
    os.makedirs(d, exist_ok=True)
    import tempfile
    tmp = tempfile.NamedTemporaryFile("w", dir=d, delete=False, suffix=".tmp")
    tmp.write(canonical(manifest))
    tmp.close()
    os.replace(tmp.name, path)
    return version, path, True


def load_manifest(out_root, version):
    """Read a PUBLISHED version. The evaluator reads this rather than recomputing from the live archive,
    which is the whole point of publishing it."""
    path = os.path.join(out_root, "corpus", version, "manifest.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as fh:
            body = fh.read()
    except Exception:
        return None
    if manifest_version(json.loads(body)) != version:
        return None                 # a manifest that does not hash to its own name is not that version
    return json.loads(body)


def corpus_version(logical):
    """Kept for callers that only want a content address of what they just read. It is NOT the A5.6 pin
    — build_manifest/publish_manifest are."""
    manifest = sorted("%s|%s|%s" % (k, v[2] if v[2] is not None else -1, v[0]) for k, v in logical.items())
    return hashlib.sha256("\n".join(manifest).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------------------------------
# A2's estimator, mechanically. The design does not merely say "bootstrap": it names the PRNG
# (java.util.SplittableRandom seeded with BOOTSTRAP_SEED), the ordering (sessions ascending by
# sessionDate), and that ONE common resample matrix is generated once per artifact run and reused across
# every clause, the generator never reset or advanced in clause-dependent order. Python's random.Random
# with a per-clause matrix is a different estimator that happens to be a bootstrap (r7 #5).
# ---------------------------------------------------------------------------------------------------
_M64 = (1 << 64) - 1
GOLDEN_GAMMA = 0x9E3779B97F4A7C15


def mix64(z):
    z = (z + GOLDEN_GAMMA) & _M64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _M64
    return z ^ (z >> 31)


class SplittableRandom:
    """java.util.SplittableRandom, faithfully: nextSeed() advances by GOLDEN_GAMMA and mix32 is the
    32-bit finalizer nextInt(bound) actually uses, including its rejection loop."""

    def __init__(self, seed):
        self.seed = seed & _M64

    def _next_seed(self):
        self.seed = (self.seed + GOLDEN_GAMMA) & _M64
        return self.seed

    @staticmethod
    def _mix32(z):
        z = ((z ^ (z >> 33)) * 0x62A9D9ED799705F5) & _M64
        z = ((z ^ (z >> 28)) * 0xCB24D0A5C88C35B3) & _M64
        v = (z >> 32) & 0xFFFFFFFF
        return v - (1 << 32) if v >= (1 << 31) else v         # Java int is signed

    def next_int(self, bound):
        r = self._mix32(self._next_seed())
        m = bound - 1
        if (bound & m) == 0:
            return r & m
        u = (r & 0xFFFFFFFF) >> 1
        while True:
            r = u % bound
            if u + m - r < (1 << 31):                          # Java's overflow test, made explicit
                return r
            u = (self._mix32(self._next_seed()) & 0xFFFFFFFF) >> 1


def resample_matrix(session_dates, b, seed):
    """ONE matrix, once per run, reused by every clause. Sessions ascending by sessionDate so the
    index->session mapping is stable."""
    ids = sorted(session_dates)
    n = len(ids)
    if n == 0:
        return ids, []
    rnd = SplittableRandom(seed)
    return ids, [[rnd.next_int(n) for _ in range(n)] for _ in range(b)]


def load_calendar():
    """The REAL trading calendar, not "weekday". Labor Day is a weekday and the market is shut; owing a
    session on it reports a permanent MISSING no run can ever satisfy, and an alarm that can never clear
    is an alarm that gets ignored.

    market_calendar exposes a CLASS. Probing the module for a bare is_trading_day always failed, so this
    silently ran on the weekday fallback and called 2026-07-03 and 2026-09-07 owed trading days — the
    fallback was never reached in anger, so nothing looked wrong (r7 #7).
    """
    # An explicit CALENDAR_DIR is EXCLUSIVE. Searching on past it would mean an operator who names a
    # calendar directory can silently get a different calendar than the one they named, and the whole
    # reason this is fatal-on-absence is that a wrong calendar is worse than no calendar.
    named = os.environ.get("CALENDAR_DIR")
    if named:
        if not os.path.isfile(os.path.join(named, "market_calendar.py")):
            return None
        sys.path.insert(0, named)
    else:
        for p in ("/home/abhinav/oe-ops",
                  os.path.dirname(os.path.abspath(__file__)),
                  os.path.expanduser("~/development/workspace/options-edge-deploy/scripts/jenkins")):
            if p and os.path.isdir(p):
                sys.path.insert(0, p)
    try:
        import market_calendar as mc
    except Exception:
        return None
    try:
        cal = mc.MarketCalendar()
        cal.is_trading_day(datetime.date(2026, 9, 7))     # prove the API before trusting it
        return cal
    except Exception:
        return None


def owed(start, end, cal):
    """The trading days in [start, end]. A missing calendar is FATAL to the caller rather than a quiet
    downgrade to weekdays: a wrong owed-day list is how a holiday becomes a permanent MISSING and a real
    gap becomes invisible."""
    if cal is None:
        raise RuntimeError("no market calendar: refusing to enumerate owed trading days by weekday")
    days, d = [], datetime.date.fromisoformat(start)
    last = datetime.date.fromisoformat(end)
    while d <= last:
        if cal.is_trading_day(d):
            days.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return days


RTH_CLOSE_HOUR, RTH_CLOSE_MINUTE = 16, 0
EARLY_CLOSE_HOUR, EARLY_CLOSE_MINUTE = 13, 0


def is_rth_close(ms, cal):
    """A5.8 requires the stopping boundary to be an RTH close instant, so it can never cut through an
    A4.12 hour row and no slicing rule is needed. An arbitrary instant would silently reintroduce one."""
    if cal is None:
        return False
    try:
        import zoneinfo
        et = datetime.datetime.fromtimestamp(ms / 1000.0, zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        return False
    if not cal.is_trading_day(et.date()):
        return False
    if et.second or et.microsecond:
        return False
    h, m = ((EARLY_CLOSE_HOUR, EARLY_CLOSE_MINUTE) if cal.is_early_close(et.date())
            else (RTH_CLOSE_HOUR, RTH_CLOSE_MINUTE))
    return et.hour == h and et.minute == m
