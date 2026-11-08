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


class PriorityStreamThreadsTest(unittest.TestCase):
    @staticmethod
    def env_value(manifest: str, deployment_name: str, env_name: str) -> str:
        start = manifest.index(f"name: {deployment_name}")
        next_doc = manifest.find("\n---", start)
        block = manifest[start:] if next_doc == -1 else manifest[start:next_doc]
        marker = f"- name: {env_name}\n"
        env_start = block.index(marker)
        value_marker = "value:"
        value_start = block.index(value_marker, env_start) + len(value_marker)
        return block[value_start:block.find("\n", value_start)].strip().strip('"')

    def assert_base_env(self, file_name: str, deployment: str, env_name: str, expected: str) -> None:
        manifest = (ROOT / "k8s" / "base" / file_name).read_text()
        self.assertEqual(expected, self.env_value(manifest, deployment, env_name))

    def test_priority_realtime_stream_processors_use_eight_threads(self) -> None:
        self.assert_base_env(
            "strike-flow-classifier-deployment.yaml",
            "strike-flow-classifier-databento",
            "KAFKA_STRIKE_FLOW_STREAMS_NUM_THREADS",
            "8",
        )
        self.assert_base_env(
            "strike-flow-avro-adapter-deployment.yaml",
            "strike-flow-avro-adapter",
            "KAFKA_STRIKE_FLOW_AVRO_STREAMS_NUM_THREADS",
            "8",
        )
        self.assert_base_env(
            "unified-sr-deployment.yaml",
            "unified-sr-service",
            "KAFKA_UNIFIED_SR_STREAMS_NUM_THREADS",
            "8",
        )
        self.assert_base_env(
            "databento-gex-deployment.yaml",
            "databento-gex-service",
            "KAFKA_GEX_STREAMS_NUM_THREADS",
            "8",
        )


if __name__ == "__main__":
    unittest.main()
