# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
import importlib.util, os, unittest
spec = importlib.util.spec_from_file_location("ev", os.path.join(os.path.dirname(__file__), "../scripts/ci/schema-mirror-evidence.py"))
ev = importlib.util.module_from_spec(spec); spec.loader.exec_module(ev)
FRAMED = b"\x00\x00\x00\x0a\x8d" + b"rest"
LOOK = lambda sid: ("com.x.Flow", ["flow-value"]) if sid == 2701 else ("com.x.Other", ["other-value"])


class Evidence(unittest.TestCase):
    def test_no_record_is_unmeasured_never_pass(self):
        self.assertEqual(ev.classify(None, set(), True, LOOK)[0], "UNMEASURED")
        self.assertEqual(ev.classify(b"", {"com.x.Flow"}, False, LOOK)[0], "UNMEASURED")

    def test_unframed_topic_with_json_passes_and_with_a_frame_fails(self):
        self.assertEqual(ev.classify(b'{"schemaVers', set(), True, LOOK)[0], "PASS")
        self.assertEqual(ev.classify(FRAMED, set(), True, LOOK)[0], "FAIL")

    def test_framed_topic_must_resolve_to_a_declared_subject(self):
        self.assertEqual(ev.classify(FRAMED, {"com.x.Flow"}, False, LOOK)[0], "PASS")
        self.assertEqual(ev.classify(FRAMED, {"flow-value"}, False, LOOK)[0], "PASS")
        s, d = ev.classify(FRAMED, {"com.x.Different"}, False, LOOK)
        self.assertEqual(s, "FAIL"); self.assertIn("com.x.Flow", d)

    def test_framed_declaration_over_unframed_data_fails(self):
        self.assertEqual(ev.classify(b'{"a":1}', {"com.x.Flow"}, False, LOOK)[0], "FAIL")

    def test_a_truncated_magic_byte_record_is_never_evidence_for_either_declaration(self):
        for declared, unframed in (({"com.x.Flow"}, False), (set(), True)):
            s, d = ev.classify(b"\x00\x00\x00", declared, unframed, LOOK)
            self.assertEqual(s, "FAIL", unframed)
            self.assertIn("truncated frame", d)


class Sampling(unittest.TestCase):
    class R:
        def __init__(self, rc, out=b"", err=b""): self.returncode, self.stdout, self.stderr = rc, out, err

    def test_a_failed_read_is_an_error_never_an_empty_sample(self):
        calls = iter([self.R(0), self.R(1, b"", b"Broker: Request timed out")])
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        self.assertIn("timed out", str(c.exception))

    def test_an_unknown_topic_is_absent_not_failed_but_other_errors_still_fail(self):
        calls = iter([self.R(0), self.R(1, b"", b"% ERROR: Topic t error: Broker: Unknown topic or partition")])
        with self.assertRaises(ev.TopicAbsent):
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        calls = iter([self.R(0), self.R(1, b"", b"Broker: Topic authorization failed")])
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        self.assertNotIsInstance(c.exception, ev.TopicAbsent)

    def test_an_unknown_topic_at_the_metadata_probe_is_also_absent(self):
        with self.assertRaises(ev.TopicAbsent):
            ev.sample("b:9092", "t", runner=lambda argv, t: self.R(1, b"", b"% ERROR: Topic t error: Broker: Unknown topic or partition"))

    def test_a_registry_error_other_than_404_is_not_read_as_no_subjects(self):
        import io, json as _j, urllib.error
        responses = {"/schemas/ids/7": _j.dumps({"schema": _j.dumps({"type": "record", "name": "Flow", "namespace": "com.x"})})}
        class Resp(io.BytesIO):
            def __enter__(self): return self
            def __exit__(self, *a): return False
        def fake(code):
            def urlopen(url, timeout=15):
                path = url.split("//", 1)[1].split("/", 1)[1]
                if path.endswith("/versions"):
                    raise urllib.error.HTTPError(url, code, "x", {}, None)
                return Resp(responses["/" + path].encode())
            return urlopen
        real = ev.urllib.request.urlopen
        try:
            ev.urllib.request.urlopen = fake(500)
            with self.assertRaises(urllib.error.HTTPError):
                ev.make_lookup("http://reg")(7)
            ev.urllib.request.urlopen = fake(404)
            self.assertEqual(ev.make_lookup("http://reg")(7), ("com.x.Flow", []))
        finally:
            ev.urllib.request.urlopen = real

    def test_a_consumer_read_that_times_out_is_an_error_never_no_record(self):
        """As None it became UNMEASURED, which on a pending source would leave the marker standing on an unread topic."""
        import subprocess
        calls = iter([self.R(0), None])
        def runner(argv, t):
            nxt = next(calls)
            if nxt is None:
                raise subprocess.TimeoutExpired(argv, t)
            return nxt
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=runner)
        self.assertIn("timed out", str(c.exception))
        self.assertNotIsInstance(c.exception, ev.TopicAbsent)

    def test_an_unreachable_broker_is_an_error(self):
        with self.assertRaises(RuntimeError):
            ev.sample("b:9092", "t", runner=lambda argv, t: self.R(1, b"", b"transport failure"))

    def test_an_empty_topic_and_a_zero_byte_value_are_told_apart_and_the_whole_value_is_returned(self):
        """kcat prints a record-presence sentinel ahead of the value, so "no record at all" (None) is distinguishable from "a record whose value is zero
        bytes" (b""). Without it both printed nothing, and on a pending source the second one has to fail. The WHOLE value comes back: framing needs
        five bytes, but a PENDING source has to be parsed in full to prove it is still JSON."""
        self.assertIsNone(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"")))
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"r")), b"")
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"r\x00abcdefghijklmnopqrstuv")), b"\x00abcdefghijklmnopqrstuv")

    def test_the_consumer_format_carries_the_presence_sentinel(self):
        seen = []
        ev.sample("b", "t", runner=lambda argv, t: seen.append(argv) or self.R(0, b"r{}"))
        self.assertEqual(seen[1][-1], "r%s", seen[1])


class Evaluate(unittest.TestCase):
    MIR = {"from": "es4", "subjects": [], "unframed": True}

    def absent(self, b, t): raise ev.TopicAbsent("gone")

    def test_an_absent_topic_fails_unless_listed(self):
        self.assertEqual(ev.evaluate(self.MIR, "t", set(), self.absent, lambda: None, "b")[0], "FAIL")
        self.assertEqual(ev.evaluate(self.MIR, "t", {("es4", "t")}, self.absent, lambda: None, "b")[0], "ABSENT")

    def test_a_listed_topic_that_exists_again_fails_until_removed(self):
        s, d = ev.evaluate(self.MIR, "t", {("es4", "t")}, lambda b, t: b'{"a":1}', lambda: None, "b")
        self.assertEqual(s, "FAIL"); self.assertIn("remove it from the list", d)
        self.assertEqual(ev.evaluate(self.MIR, "t", set(), lambda b, t: b'{"a":1}', lambda: None, "b")[0], "PASS")

    def test_a_read_error_fails_even_when_listed(self):
        def boom(b, t): raise RuntimeError("timed out")
        self.assertEqual(ev.evaluate(self.MIR, "t", {("es4", "t")}, boom, lambda: None, "b")[0], "FAIL")


OK_PROOF = lambda: (True, "the es4 registry already maps com.x.Flow = 2701")


class CutoverPending(unittest.TestCase):
    """mirror-evidence-pending.txt: a FRAMED declaration whose source environment has not been cut over yet. It must be its own status, must assert
    the condition it stands for, and must fail the moment the condition ends -- it is NOT the `unframed` flag."""

    def test_an_unframed_newest_record_is_pending_not_a_pass_and_names_the_reference(self):
        s, d = ev.classify(b'{"symbol":"VIX","price":23.7}', {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
        self.assertEqual(s, "PENDING")
        self.assertIn("docs/CUTOVER.md", d)
        self.assertIn("still valid pre-cutover JSON", d)

    def test_pending_fails_once_framing_begins_so_the_entry_must_be_removed(self):
        s, d = ev.classify(FRAMED, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
        self.assertEqual(s, "FAIL")
        self.assertIn("remove the entry", d)
        self.assertIn("2701", d)

    def test_pending_is_refused_on_an_unframed_declaration(self):
        """The `unframed` flag asserts JSON is the steady state; conflating the two is exactly how the framed id check would be switched off for good."""
        for smp in (b'{"a":1}', FRAMED, None):
            s, d = ev.classify(smp, set(), True, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
            self.assertEqual(s, "FAIL", smp)
            self.assertIn("not a pending state", d)

    def test_pending_never_reads_the_source_registry(self):
        """Registering the subject on the source registry IS the outstanding cutover step, so a pending source must not depend on it."""
        def boom(sid):
            raise AssertionError("the source registry was consulted while pending")
        self.assertEqual(ev.classify(b'{"a":1}', {"com.x.Flow"}, False, boom, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)[0], "PENDING")

    def test_an_empty_topic_stays_unmeasured_and_proves_nothing(self):
        s, d = ev.classify(None, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
        self.assertEqual(s, "UNMEASURED")
        self.assertIn("cutover pending", d)   # the marker is still reported, but nothing was proven about the source

    def test_a_zero_byte_newest_value_is_not_an_empty_topic_and_fails(self):
        """A record EXISTS and its value is zero bytes (a null tombstone): it proves neither serialization, so the marker must not stand on it."""
        s, d = ev.classify(b"", {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
        self.assertEqual(s, "FAIL")
        self.assertIn("zero bytes", d)
        # for the other declarations it stays UNMEASURED: a tombstone is legitimate on the compacted mirror topics
        self.assertEqual(ev.classify(b"", {"com.x.Flow"}, False, LOOK)[0], "UNMEASURED")
        self.assertEqual(ev.classify(b"", set(), True, LOOK)[0], "UNMEASURED")

    def test_an_empty_pending_topic_still_fails_on_an_unready_destination(self):
        """The destination proof runs BEFORE the sample is read: an empty source must not be the reason the far side goes unchecked for a night."""
        for smp in (None, b""):
            s, d = ev.classify(smp, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=lambda: (False, "connection refused"))
            self.assertEqual(s, "FAIL", smp)
            self.assertIn("destination registry is not ready", d)

    def test_a_truncated_magic_byte_record_fails_and_never_reads_as_still_unframed(self):
        """0x00 then fewer than the four id bytes is evidence of neither serialization; as "not framed" it would hold the marker open on nothing."""
        for smp in (b"\x00", b"\x00\x00", b"\x00\x00\x00\x0a"):
            s, d = ev.classify(smp, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
            self.assertEqual(s, "FAIL", smp)
            self.assertIn("truncated frame", d)

    def test_bytes_that_are_neither_a_frame_nor_json_fail_instead_of_holding_the_marker(self):
        """"not framed" is not "still JSON": corrupt, binary or foreign output proves neither serialization, so it must turn the build red."""
        for smp in (b"\x89PNG\r\n\x1a\n", b'{"schemaVers', b"plain text", b"\xff\xfe\x00\x01binary"):
            s, d = ev.classify(smp, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)
            self.assertEqual(s, "FAIL", smp)
            self.assertIn("does not parse as the pre-cutover JSON", d)

    def test_json_that_is_not_an_object_still_counts_as_json(self):
        for smp in (b'[{"a":1}]', b'  {"a":1}  '):
            self.assertEqual(ev.classify(smp, {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md", dest_proof=OK_PROOF)[0], "PENDING", smp)

    def test_a_destination_that_is_not_ready_for_the_fixed_id_fails(self):
        s, d = ev.classify(b'{"a":1}', {"com.x.Flow"}, False, LOOK, pending="docs/CUTOVER.md",
                           dest_proof=lambda: (False, "id 2701 on es4 is com.x.Other"))
        self.assertEqual(s, "FAIL")
        self.assertIn("destination registry is not ready", d)


class DestinationProof(unittest.TestCase):
    """While a source is pending nothing else samples the destination, so the pending state itself has to prove the far side maps the fixed id."""
    ENTRIES = {"com.x.Flow": {"subject": "com.x.Flow", "id": 2701, "environments": ["dev", "prod", "es4"]}}

    REGS = {"dev": "http://dev", "prod": "http://prod", "es4": "http://es4"}

    def proof(self, entries=None, subjects=("com.x.Flow",), dests=("es4",), regs=None, lookup=None):
        return ev.make_dest_proof(self.ENTRIES if entries is None else entries, list(subjects), set(dests),
                                  self.REGS if regs is None else regs, lookup_factory=lambda url: (lookup or LOOK))()

    def test_the_declared_id_resolving_to_the_declared_subject_is_the_proof(self):
        ok, d = self.proof()
        self.assertTrue(ok); self.assertIn("es4 com.x.Flow = 2701", d)

    def test_every_destination_is_proven_not_just_one(self):
        """The run de-duplicates identical source declarations, so a topic mirrored to two clusters would otherwise have one side never looked at."""
        ok, d = self.proof(dests=("dev", "prod"))
        self.assertTrue(ok)
        self.assertIn("dev com.x.Flow = 2701", d); self.assertIn("prod com.x.Flow = 2701", d)
        seen = []
        ok, d = self.proof(dests=("dev", "es4"), lookup=lambda sid: seen.append(sid) or ("com.x.Flow", []))
        self.assertTrue(ok); self.assertEqual(len(seen), 2)
        ok, d = self.proof(dests=("es4", "prod"), entries={"com.x.Flow": {"id": 2701, "environments": ["es4"]}})
        self.assertFalse(ok); self.assertIn("does not list prod", d)

    def test_no_declared_destination_at_all_is_refused(self):
        ok, d = self.proof(dests=())
        self.assertFalse(ok); self.assertIn("no byte mirror declares a destination", d)

    def test_a_framed_declaration_with_no_subject_cannot_be_pending(self):
        """subjects: [] without the `unframed` flag has no fixed id to prove on either side, so a pending entry on it would suppress the evidence
        while proving nothing. Outside pending that case already fails on the declared-subject check."""
        ok, d = self.proof(subjects=())
        self.assertFalse(ok); self.assertIn("declares no subject", d)
        s, detail = ev.classify(b'{"a":1}', set(), False, LOOK, pending="docs/CUTOVER.md",
                                dest_proof=ev.make_dest_proof(self.ENTRIES, [], {"es4"}, self.REGS, lookup_factory=lambda u: LOOK))
        self.assertEqual(s, "FAIL"); self.assertIn("declares no subject", detail)

    def test_a_subject_match_is_enough_even_when_the_record_name_differs(self):
        ok, _ = self.proof(subjects=("flow-value",), entries={"flow-value": {"id": 2701, "environments": ["es4"]}})
        self.assertTrue(ok)

    def test_a_different_schema_under_the_id_is_refused(self):
        ok, d = self.proof(entries={"com.x.Flow": {"id": 9, "environments": ["es4"]}})
        self.assertFalse(ok); self.assertIn("com.x.Other", d)

    def test_an_unreadable_destination_registry_is_never_a_pass(self):
        def boom(sid):
            raise RuntimeError("connection refused")
        ok, d = self.proof(lookup=boom)
        self.assertFalse(ok); self.assertIn("connection refused", d)
        ok, d = self.proof(regs={})
        self.assertFalse(ok); self.assertIn("no registry is declared", d)

    def test_an_ungoverned_or_per_environment_subject_is_refused(self):
        ok, d = self.proof(entries={})
        self.assertFalse(ok); self.assertIn("not a manifest entry", d)
        ok, d = self.proof(entries={"com.x.Flow": {"id": 2701, "environments": ["es4"], "idByEnvironment": {"es4": 2701}}})
        self.assertFalse(ok); self.assertIn("per-environment ids", d)
        ok, d = self.proof(entries={"com.x.Flow": {"id": 2701, "environments": ["dev"]}})
        self.assertFalse(ok); self.assertIn("does not list es4", d)


class PendingLedger(unittest.TestCase):
    def write(self, text):
        import tempfile
        f = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False)
        f.write(text); f.close()
        return f.name

    def test_a_well_formed_line_is_read_and_comments_are_ignored(self):
        p = self.write("# header\nprod underlying.vix.price  docs/X.md, Production section  # why\n\n")
        self.assertEqual(ev.read_pending(p, set()), {("prod", "underlying.vix.price"): "docs/X.md, Production section"})

    def test_a_missing_file_lists_nothing(self):
        self.assertEqual(ev.read_pending("/nonexistent/mirror-evidence-pending.txt", set()), {})

    def test_a_line_without_a_cutover_reference_is_refused(self):
        with self.assertRaises(SystemExit):
            ev.read_pending(self.write("prod underlying.vix.price\n"), set())

    def test_a_duplicate_line_is_refused(self):
        with self.assertRaises(SystemExit):
            ev.read_pending(self.write("prod t docs/X.md\nprod t docs/X.md\n"), set())

    def test_an_entry_also_listed_as_absent_is_refused(self):
        with self.assertRaises(SystemExit) as c:
            ev.read_pending(self.write("prod t docs/X.md\n"), {("prod", "t")})
        self.assertIn("mirror-evidence-absent.txt", str(c.exception))


class CommittedPendingLedger(unittest.TestCase):
    """The committed ledger must stay true: every entry names a topic some byte-copy mirror really declares as a FRAMED source."""

    def test_every_committed_entry_names_a_framed_byte_mirror_source(self):
        import json, os
        root = os.path.join(os.path.dirname(__file__), "..")
        m = json.load(open(os.path.join(root, "schema-manifest/manifest.json")))
        framed = {(a["from"], t) for a in m["activeMirrors"] if a["transport"] == "bytes" and not a.get("unframed") for t in a["topics"]}
        for key, ref in ev.read_pending(os.path.join(root, "schema-manifest/mirror-evidence-pending.txt"), set()).items():
            self.assertIn(key, framed, key)
            self.assertTrue(ref.strip(), key)


if __name__ == "__main__":
    unittest.main()
