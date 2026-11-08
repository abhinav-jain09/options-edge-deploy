# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
