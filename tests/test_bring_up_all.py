# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class BringUpAllPipelineTest(unittest.TestCase):
    def test_skip_kafka_topics_is_declared_and_forwarded_to_deploy(self):
        pipeline = (ROOT / "Jenkinsfile.bring-up-all").read_text()

        self.assertIn("booleanParam(name: 'SKIP_KAFKA_TOPICS', defaultValue: false", pipeline)
        self.assertGreaterEqual(
            pipeline.count("booleanParam(name: 'SKIP_KAFKA_TOPICS', value: params.SKIP_KAFKA_TOPICS)"),
            2,
        )


if __name__ == "__main__":
    unittest.main()
