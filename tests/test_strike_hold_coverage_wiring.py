"""The coverage input's wiring, asserted with a YAML parser rather than with awk.

WHY THIS FILE EXISTS. `ensure-strike-hold-topics.sh` has to refuse a strike-hold deploy when
es.mbo.coverage is absent, because Kafka Streams will not start without a source topic and the pod
would crash-loop instead of reporting ABSORPTION_COVERAGE_UNAVAILABLE. Three attempts to have the
barrier work out the topic name for itself each failed in the same direction — toward silently
checking nothing:

  1. it read $KAFKA_STRIKE_HOLD_COVERAGE_TOPIC, which the Jenkins stage never exports, so the whole
     check was skipped on every real deploy while the mock suite set the variable by hand;
  2. it parsed the Deployment with awk, which could not see a flow-style entry, a comment between
     the key and its value, or a valueFrom, and read each of those as "no coverage source";
  3. the hardened parse fell back from the overlay to the base — so an overlay that REMOVED the key
     would still enforce the dependency, and an unreadable overlay would silently use base — and it
     was not scoped to the strike-hold container, so a sole matching key in a sidecar or a second
     document counted as this service's setting.

So the barrier now verifies a CONSTANT, and the pairing it was trying to derive is asserted here,
where a YAML parser and a container scope are available and exact.
"""
import os
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BARRIER = ROOT / "scripts/kafka/ensure-strike-hold-topics.sh"
TOPIC = "es.mbo.coverage"
KEY = "KAFKA_STRIKE_HOLD_COVERAGE_TOPIC"


def env_of(rel: str, container: str) -> dict:
    """Every env entry of one container, across every document in the file."""
    out = {}
    for doc in yaml.safe_load_all((ROOT / rel).read_text()):
        if not doc:
            continue
        spec = (doc.get("spec") or {}).get("template", {}).get("spec", {})
        for c in spec.get("containers") or []:
            if c.get("name") != container:
                continue
            for e in c.get("env") or []:
                if "value" in e:
                    out[e["name"]] = e["value"]
                else:
                    out[e["name"]] = e            # valueFrom and friends, kept as-is
    return out


class CoverageWiring(unittest.TestCase):
    def test_the_barrier_verifies_the_topic_the_deployment_configures(self):
        # The constant the barrier verifies.
        # `-` and not `:-` on purpose: an explicit empty value must stay a way to say "no coverage
        # source", which `:-` silently took the default for.
        m = re.search(r'COVERAGE_IN="\$\{KAFKA_STRIKE_HOLD_COVERAGE_TOPIC-([^}"]+)\}"',
                      BARRIER.read_text())
        self.assertIsNotNone(m, "the barrier must verify a declared default coverage topic")
        self.assertEqual(m.group(1), TOPIC)

        # And the Deployment the deploy applies must configure exactly that, in the strike-hold
        # container, with a literal value a running pod can use.
        for rel in ("k8s/services/strike-hold/base/strike-hold-deployment.yaml",
                    "k8s/services/strike-hold/overlays/dev/manifest.yaml"):
            env = env_of(rel, "strike-hold")
            self.assertIn(KEY, env, f"{rel} must set {KEY}, or the barrier's constant is stale")
            self.assertEqual(env[KEY], TOPIC,
                             f"{rel} sets {KEY}={env[KEY]!r}; the barrier verifies {TOPIC!r}")

    def test_the_barrier_refuses_rather_than_skips_when_the_topic_is_missing(self):
        # The refusal has to say what to do: a deploy-time failure that does not name the fix is a
        # crash loop with extra steps.
        text = BARRIER.read_text()
        self.assertIn("CRASH-LOOP", text)
        self.assertIn("ensure-es-liquidity-engine-topics.sh", text,
                      "the refusal must name the barrier that creates the topic")
        self.assertNotIn("coverage_from_manifest", text,
                         "the awk manifest parse is gone; this file replaces it")

    def test_the_mocked_barrier_suite_exists_and_the_gate_runs_it(self):
        suite = ROOT / "scripts/kafka/ensure-strike-hold-topics-test.sh"
        self.assertTrue(suite.is_file() and os.access(suite, os.X_OK))
        self.assertIn("bash scripts/kafka/ensure-strike-hold-topics-test.sh",
                      (ROOT / ".github/workflows/deploy-validation.yml").read_text())


if __name__ == "__main__":
    unittest.main()
