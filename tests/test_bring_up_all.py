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
