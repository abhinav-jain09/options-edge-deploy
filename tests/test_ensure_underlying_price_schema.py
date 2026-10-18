"""scripts/kafka/ensure-underlying-price-schema.sh against the stand-in registry: a fresh registry gets the canonical
underlying.spx.price subject under FULL, a re-run changes nothing, a drifted subject-level config is repaired to
FULL by read-back, a schema file whose record is not the subject is refused, and a dry run mutates nothing."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-underlying-price-schema.sh"
SUBJECT = "com.optionsedge.contracts.hpsf.UnderlyingPrice"


def schema_file(name: str = "UnderlyingPrice") -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".avsc", delete=False)
    json.dump({"type": "record", "name": name, "namespace": "com.optionsedge.contracts.hpsf",
               "fields": [{"name": "symbol", "type": "string"},
                          {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 18}}]}, f)
    f.close()
    return f.name


def run(url: str, path: str, dry: bool = False):
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMA_FILE=path, DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class EnsureUnderlyingPriceSchema(unittest.TestCase):
    def test_fresh_registry_gets_the_subject_under_full(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, schema_file())
            self.assertEqual(rc, 0, out)
            self.assertEqual(len(reg.subjects[SUBJECT]), 1)
            self.assertEqual(reg.configs[SUBJECT], "FULL")
            self.assertIn("compatibility: " + SUBJECT + " FULL", out)
        finally:
            server.shutdown()

    def test_rerun_is_idempotent_and_repairs_a_drifted_config(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            path = schema_file()
            self.assertEqual(run(url, path)[0], 0)
            reg.configs[SUBJECT] = "BACKWARD"
            rc, out = run(url, path)
            self.assertEqual(rc, 0, out)
            self.assertEqual(len(reg.subjects[SUBJECT]), 1, "no second version")
            self.assertEqual(reg.configs[SUBJECT], "FULL")
        finally:
            server.shutdown()

    def test_a_schema_that_is_not_the_subject_is_refused_before_any_call(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, schema_file("SomethingElse"))
            self.assertNotEqual(rc, 0)
            self.assertIn("not the subject", out)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_a_newer_version_after_the_canonical_one_is_refused(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            path = schema_file()
            self.assertEqual(run(url, path)[0], 0)
            reg.register(SUBJECT, json.dumps({"type": "record", "name": "UnderlyingPrice", "namespace": "com.optionsedge.contracts.hpsf",
                                              "fields": [{"name": "symbol", "type": "string"}]}))
            rc, out = run(url, path)
            self.assertNotEqual(rc, 0)
            self.assertIn("latest version", out)
        finally:
            server.shutdown()

    def test_dry_run_mutates_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, schema_file(), dry=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("DRY RUN", out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual(reg.calls, ["POST /subjects/" + SUBJECT + " {schema…}"])
            self.assertEqual(reg.configs, {})
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
