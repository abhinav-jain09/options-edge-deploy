"""The dev clean must pre-create the DEV-ONLY overnight estimate topics before any service starts.

2026-10-21: after a clean, the estimate sidecar created options.indicators.estimate.bars AFTER context-tape-service had started, so context-tape
subscribed only to the forming topic and the Order tab drew one candle replaced every minute. The topics are deliberately not in topics.env
(dev-only), so the clean script itself creates them, with the shapes the sidecar uses."""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "scripts/ops/dev-cleanup.sh").read_text()
TOPICS_ENV = (ROOT / "scripts/kafka/topics.env").read_text()


class DevCleanEstimateTopics(unittest.TestCase):
    def test_function_creates_both_topics_with_the_sidecar_shapes(self):
        body = re.search(r"ensure_dev_only_estimate_topics\(\) \{(.*?)\n\}", SCRIPT, re.S).group(1)
        self.assertIn("--create --if-not-exists --topic options.indicators.estimate.bars --partitions 1", body)
        self.assertIn("cleanup.policy=delete --config retention.ms=259200000", body)
        self.assertIn("--topic options.indicators.estimate.bars.current --partitions 1", body)
        self.assertIn("cleanup.policy=compact --config retention.ms=86400000", body)

    def test_ensure_topics_calls_it_on_the_success_path(self):
        ensure = re.search(r"\nensure_topics\(\) \{(.*?)\n\}\n", SCRIPT, re.S).group(1)
        self.assertIn("ensure_dev_only_estimate_topics", ensure)
        self.assertLess(ensure.index("reconcile_declared_topics"), ensure.index("ensure_dev_only_estimate_topics"))

    def test_the_estimate_topics_stay_out_of_the_shared_topics_env(self):
        self.assertNotIn("options.indicators.estimate", TOPICS_ENV, "dev-only: prod must not grow these topics")


if __name__ == "__main__":
    unittest.main()
