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

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
HISTORY_TOPIC = "rates.sofr.databento-sr3-feed-service.history"


def deployment_env(path: pathlib.Path, name: str) -> dict[str, str]:
    for document in yaml.safe_load_all(path.read_text()):
        if document and document.get("kind") == "Deployment" and document["metadata"]["name"] == name:
            container = document["spec"]["template"]["spec"]["containers"][0]
            return {item["name"]: item.get("value") for item in container.get("env", [])}
    raise AssertionError(f"deployment {name} missing from {path}")


class MarketCarryRateHistoryDeployTest(unittest.TestCase):
    def test_feed_dual_publishes_and_carry_reads_history_in_every_render(self):
        for environment in ("dev", "production", "experiment"):
            feed = deployment_env(
                ROOT / "k8s/services/databento-sr3-feed/overlays" / environment / "manifest.yaml",
                "databento-sr3-feed-service",
            )
            carry = deployment_env(
                ROOT / "k8s/services/market-carry/overlays" / environment / "manifest.yaml",
                "market-carry-service",
            )
            self.assertEqual(HISTORY_TOPIC, feed["SR3_HISTORY_TOPIC"])
            self.assertEqual(HISTORY_TOPIC, carry["MARKET_CARRY_SR3_TOPIC"])
            self.assertNotEqual(feed["SR3_OUTPUT_TOPIC"], feed["SR3_HISTORY_TOPIC"])

    def test_history_topic_is_single_partition_and_bounded(self):
        topics = (ROOT / "scripts/kafka/topics.env").read_text()
        self.assertIn(f"{HISTORY_TOPIC}:1", topics)
        self.assertIn(f"{HISTORY_TOPIC}=1800000", topics)


if __name__ == "__main__":
    unittest.main()
