# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
