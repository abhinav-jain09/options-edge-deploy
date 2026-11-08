# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RawPostgresWriterDeployTest(unittest.TestCase):
    def test_raw_writer_rollout_replaces_one_pod_at_a_time(self) -> None:
        deployment = (ROOT / "k8s" / "base" / "raw-postgres-writer-deployment.yaml").read_text()

        self.assertIn("name: raw-postgres-writer", deployment)
        self.assertIn("strategy:", deployment)
        self.assertIn("type: RollingUpdate", deployment)
        self.assertIn("maxSurge: 0", deployment)
        self.assertIn("maxUnavailable: 1", deployment)


if __name__ == "__main__":
    unittest.main()
