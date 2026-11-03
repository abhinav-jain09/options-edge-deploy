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


if __name__ == "__main__":
    unittest.main()
