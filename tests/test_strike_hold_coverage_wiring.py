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


DEPLOYMENT = "strike-hold-service"
CONTAINER = "strike-hold"


def env_of(rel: str) -> dict:
    """The env of the ONE container of the ONE Deployment this service deploys.

    Scoped to kind and metadata.name, and requiring exactly one match of each — not merged across
    every document that happens to hold a container of that name. Merging was the fault: a later
    unrelated document could satisfy or overwrite the real service's setting, so the test could pass
    while the Deployment did not configure the topic at all (Codex deploy r8).
    """
    docs = [d for d in yaml.safe_load_all((ROOT / rel).read_text())
            if d and d.get("kind") == "Deployment" and (d.get("metadata") or {}).get("name") == DEPLOYMENT]
    if len(docs) != 1:
        raise AssertionError(f"{rel}: expected exactly one Deployment named {DEPLOYMENT}, found {len(docs)}")
    containers = [c for c in ((docs[0].get("spec") or {}).get("template", {})
                              .get("spec", {}).get("containers") or [])
                  if c.get("name") == CONTAINER]
    if len(containers) != 1:
        raise AssertionError(f"{rel}: expected exactly one container named {CONTAINER}, found {len(containers)}")
    out = {}
    for e in containers[0].get("env") or []:
        # A valueFrom entry is kept as the mapping, so the assertion below REJECTS it rather than
        # resolving a reference this test cannot see the target of.
        out[e["name"]] = e["value"] if "value" in e else e
    return out


class CoverageWiring(unittest.TestCase):
    def test_the_barrier_verifies_the_topic_the_deployment_configures(self):
        # The constant the barrier verifies.
        # A LITERAL assignment, with no parameter expansion: the deployed containers hardcode the
        # topic, so an environment override would let the barrier validate a topology nobody
        # deployed. The regex refuses a '${...}' form for that reason, not merely as style.
        text = BARRIER.read_text()
        m = re.search(r'^COVERAGE_IN=([^\s$]+)$', text, re.M)
        self.assertIsNotNone(m, "the barrier must verify a LITERAL coverage topic, not an override")
        self.assertEqual(m.group(1), TOPIC)
        self.assertNotRegex(text, r'COVERAGE_IN="?\$\{',
                            "an environment override here would validate a different topology")

        # And the Deployment the deploy applies must configure exactly that, in the strike-hold
        # container, with a literal value a running pod can use.
        for rel in ("k8s/services/strike-hold/base/strike-hold-deployment.yaml",
                    "k8s/services/strike-hold/overlays/dev/manifest.yaml"):
            env = env_of(rel)
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
