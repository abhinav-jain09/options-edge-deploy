# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The gamma-effort RELAY flag on the feed-gateway, and the topic classes the corpus depends on.

gamma-effort-service writes options.spx.gamma-effort.{current,log}. The gateway relays `current`
only when GAMMA_EFFORT_ENABLED is set, and the page at /gamma-effort reads it.

DEV ONLY, and the asymmetry matters in one direction: gamma-effort-service is declared envs: [dev],
so on production the flag would subscribe the gateway to a topic that does not exist there.
Production stays default-off by NOT setting it, the same shape as GAMMA_ZONE_ENABLED beside it.

The topic classes are asserted here too, because getting them wrong destroys the one thing the
service is for. The log is the calibration corpus: the design wants 20 to 25 sessions before any
threshold is trusted, and its own measurements say the shipped parameters need settling. A log that
is compacted, unpreserved, or purged by the pre-market reset silently removes that.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_PATCH = ROOT / "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml"
DEV_SLICE = ROOT / "k8s/services/feed-gateway/overlays/dev/manifest.yaml"
SERVICES = ROOT / "services.yaml"
TOPICS = ROOT / "scripts/kafka/topics.env"
PREMARKET = ROOT / "scripts/ops/premarket-reset.sh"
FLAG = "GAMMA_EFFORT_ENABLED"
LOG = "options.spx.gamma-effort.log"
CURRENT = "options.spx.gamma-effort.current"


def env_of(path: Path) -> dict:
    """The container env of the Deployment in this file.

    safe_load_all, not safe_load: the generated slice is a MULTI-document file (a Service beside the
    Deployment), and safe_load raises on the second document rather than returning the first.
    """
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d.get("kind") == "Deployment"]
    assert len(docs) == 1, f"expected exactly one Deployment in {path}, got {len(docs)}"
    containers = docs[0]["spec"]["template"]["spec"]["containers"]
    for container in containers:
        env = {e["name"]: e.get("value") for e in container.get("env", [])}
        if FLAG in env:
            return env
    return {e["name"]: e.get("value") for e in containers[0].get("env", [])}


class RelayFlag(unittest.TestCase):
    def test_the_relay_is_ON_for_dev(self):
        self.assertEqual("true", env_of(DEV_PATCH).get(FLAG))

    def test_the_generated_slice_carries_it_too(self):
        """The patch is the source; the slice is what service-deploy applies. A flag in one and not
        the other is the shape that looks configured and deploys off."""
        self.assertEqual("true", env_of(DEV_SLICE).get(FLAG))

    def test_production_does_NOT_set_it(self):
        """gamma-effort-service is dev-only, so on production the flag would subscribe the gateway
        to a topic that does not exist. Default-off by absence, like GAMMA_ZONE_ENABLED."""
        for path in sorted((ROOT / "k8s/overlays/production").glob("*feed-gateway*.yaml")):
            self.assertNotIn(FLAG, path.read_text(), f"{path.name} must not set {FLAG}")
        prod_slice = ROOT / "k8s/services/feed-gateway/overlays/production/manifest.yaml"
        if prod_slice.is_file():
            self.assertNotIn(FLAG, prod_slice.read_text())

    def test_the_service_it_relays_is_still_dev_only(self):
        """If gamma-effort ever gains a production environment this test should fail, so the
        decision above is re-taken deliberately instead of silently becoming wrong."""
        line = [l for l in SERVICES.read_text().splitlines() if "name: gamma-effort," in l]
        self.assertEqual(1, len(line), "exactly one gamma-effort service entry")
        self.assertIn("envs: [dev]", line[0],
                      "gamma-effort is no longer dev-only: re-take the production relay decision")


class CorpusTopic(unittest.TestCase):
    """The log is the calibration corpus. Each of these, if wrong, deletes it quietly."""

    def topics(self) -> str:
        return TOPICS.read_text()

    def test_the_log_is_DELETE_only_and_never_compacted(self):
        """The key is the session, so compaction keeps one record per session and discards exactly
        the intraday history the topic exists to accumulate."""
        self.assertNotIn(LOG, self._assignment("OPTIONS_EDGE_PURE_COMPACT_TOPICS"),
                         "the corpus log must never be compacted")
        self.assertIn(CURRENT, self._assignment("OPTIONS_EDGE_PURE_COMPACT_TOPICS"))

    def test_the_log_is_reset_preserved_and_never_recreated(self):
        self.assertIn(LOG, self._assignment("OPTIONS_EDGE_RESET_PRESERVED_TOPICS"))
        self.assertIn(LOG, self._assignment("OPTIONS_EDGE_NEVER_RECREATE_TOPICS"))

    def test_current_is_rebuildable_rather_than_preserved(self):
        """The next folded bar rewrites it and the service re-reads its lookback on every start, so
        preserving it would keep a stale record alive across a reset for no gain."""
        self.assertIn(CURRENT, self._assignment("OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"))
        self.assertNotIn(CURRENT, self._assignment("OPTIONS_EDGE_RESET_PRESERVED_TOPICS"))

    def test_the_premarket_reset_preserves_the_log(self):
        """The pre-market reset purges by a regex, not by the topics.env classes, so a topic can be
        declared preserved and still be wiped every morning. The fleet validator catches this; it is
        asserted here too because the cost is the whole corpus."""
        text = PREMARKET.read_text()
        match = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", text, re.MULTILINE)
        self.assertIsNotNone(match, "PRESERVE_TOPICS_REGEX not found")
        self.assertRegex(LOG, match.group(1), "the pre-market reset would wipe the corpus")
        self.assertNotRegex(CURRENT, match.group(1), "`current` is rebuildable: preserving it buys nothing")

    def _assignment(self, var: str) -> str:
        """Every assignment of var, joined. The file declares some classes in one literal near the
        top and others as column-0 appends beside the topics they describe; reading only the first
        would miss exactly the declarations a new service adds."""
        return " ".join(re.findall(rf"^{var}=\"(.*)\"$", self.topics(), re.MULTILINE))


if __name__ == "__main__":
    unittest.main()
