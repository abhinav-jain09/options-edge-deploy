# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""scripts/kafka/ensure-es-underlying-price-schema.sh against the stand-in registry: a fresh registry gets the canonical
ES price subject under FULL, a re-run changes nothing, a drifted subject-level config is repaired to
FULL by read-back, a schema file whose record is not the subject is refused, and a dry run mutates nothing."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-es-underlying-price-schema.sh"
SUBJECT = "com.optionsedge.contracts.hpsf.EsUnderlyingPrice"


CANONICAL = ROOT / "tests/fixtures/es-underlying-price/EsUnderlyingPrice.avsc"


def schema_file(canonical: bool = True) -> str:
    if canonical:
        return str(CANONICAL)
    f = tempfile.NamedTemporaryFile("w", suffix=".avsc", delete=False)
    json.dump({"type": "record", "name": "EsUnderlyingPrice", "namespace": "com.optionsedge.contracts.hpsf",
               "fields": [{"name": "symbol", "type": "string"}]}, f)
    f.close()
    return f.name


def run(url: str, path: str, dry: bool = False):
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMA_FILE=path, DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class EnsureEsUnderlyingPriceSchema(unittest.TestCase):
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

    def test_any_file_but_the_pinned_canonical_bytes_is_refused_before_any_call(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run(url, schema_file(canonical=False))
            self.assertNotEqual(rc, 0)
            self.assertIn("not the canonical", out)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_a_newer_version_after_the_canonical_one_is_refused(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            path = schema_file()
            self.assertEqual(run(url, path)[0], 0)
            reg.register(SUBJECT, open(schema_file(canonical=False)).read())
            rc, out = run(url, path)
            self.assertNotEqual(rc, 0)
            self.assertIn("latest version", out)
        finally:
            server.shutdown()

    def test_a_subject_override_in_the_environment_is_ignored(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            e = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMA_FILE=schema_file(), SUBJECT="some.other.Subject")
            r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=e)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertEqual(list(reg.subjects), [SUBJECT])
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
