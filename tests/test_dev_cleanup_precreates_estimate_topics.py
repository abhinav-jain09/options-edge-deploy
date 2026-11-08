# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
        self.assertIn('"options.indicators.estimate.bars delete 259200000"', body)
        self.assertIn('"options.indicators.estimate.bars.current compact 86400000"', body)
        self.assertIn("--create --if-not-exists", body)
        self.assertIn("--partitions 1 --replication-factor 1", body)
        self.assertIn("for i in 1 2 3", body, "a transient admin failure is retried")
        self.assertIn("WARNING: could not create", body, "a final failure is loud")

    def test_ensure_topics_calls_it_on_the_success_path(self):
        ensure = re.search(r"\nensure_topics\(\) \{(.*?)\n\}\n", SCRIPT, re.S).group(1)
        self.assertIn("ensure_dev_only_estimate_topics", ensure)
        self.assertLess(ensure.index("reconcile_declared_topics"), ensure.index("ensure_dev_only_estimate_topics"))

    def test_the_estimate_topics_stay_out_of_the_shared_topics_env(self):
        self.assertNotIn("options.indicators.estimate", TOPICS_ENV, "dev-only: prod must not grow these topics")


if __name__ == "__main__":
    unittest.main()
