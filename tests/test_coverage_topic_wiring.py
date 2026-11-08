# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The coverage topic's wiring on BOTH sides, asserted with a YAML parser rather than with awk.

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
TOPIC = "es.mbo.coverage"

# BOTH SIDES, because the bypass is symmetric. The engine CREATES the topic and strike-hold READS
# it; each barrier was honouring an environment override while its pod hardcodes the literal, so
# either one could have validated a topic its own pod does not use. Fixing it for strike-hold alone
# left the engine's mock suite green under its default environment with the same risk restored
# (Codex deploy r9).
SIDES = (
    {
        "name": "strike-hold",
        "barrier": "scripts/kafka/ensure-strike-hold-topics.sh",
        "assign": "COVERAGE_IN",
        "key": "KAFKA_STRIKE_HOLD_COVERAGE_TOPIC",
        "deployment": "strike-hold-service",
        "container": "strike-hold",
        "manifests": ("k8s/services/strike-hold/base/strike-hold-deployment.yaml",
                      "k8s/services/strike-hold/overlays/dev/manifest.yaml"),
    },
    {
        "name": "es-liquidity-engine",
        "barrier": "scripts/kafka/ensure-es-liquidity-engine-topics.sh",
        "assign": "COVERAGE",
        "key": "ES_MBO_COVERAGE_TOPIC",
        "deployment": "es-liquidity-engine-service",
        "container": "es-liquidity-engine",
        "manifests": ("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml",
                      "k8s/services/es-liquidity-engine/overlays/dev/manifest.yaml"),
    },
)


def env_of(rel: str, deployment: str, container: str) -> dict:
    """The env of the ONE container of the ONE Deployment this service deploys.

    Scoped to kind and metadata.name, and requiring exactly one match of each — not merged across
    every document that happens to hold a container of that name. Merging was the fault: a later
    unrelated document could satisfy or overwrite the real service's setting, so the test could pass
    while the Deployment did not configure the topic at all (Codex deploy r8).
    """
    docs = [d for d in yaml.safe_load_all((ROOT / rel).read_text())
            if d and d.get("kind") == "Deployment" and (d.get("metadata") or {}).get("name") == deployment]
    if len(docs) != 1:
        raise AssertionError(f"{rel}: expected exactly one Deployment named {deployment}, found {len(docs)}")
    containers = [c for c in ((docs[0].get("spec") or {}).get("template", {})
                              .get("spec", {}).get("containers") or [])
                  if c.get("name") == container]
    if len(containers) != 1:
        raise AssertionError(f"{rel}: expected exactly one container named {container}, found {len(containers)}")
    out = {}
    for e in containers[0].get("env") or []:
        # A valueFrom entry is kept as the mapping, so the assertion below REJECTS it rather than
        # resolving a reference this test cannot see the target of.
        out[e["name"]] = e["value"] if "value" in e else e
    return out


class CoverageWiring(unittest.TestCase):
    def test_each_barrier_verifies_the_topic_its_own_deployment_configures(self):
        for side in SIDES:
            with self.subTest(side=side["name"]):
                text = (ROOT / side["barrier"]).read_text()

                # A LITERAL assignment, with no parameter expansion: the deployed containers
                # hardcode the topic, so an environment override would let the barrier validate a
                # topology nobody deployed. The second assertion refuses a '${...}' form for that
                # reason, not as style.
                m = re.search(rf'^{side["assign"]}=([^\s$]+)$', text, re.M)
                self.assertIsNotNone(
                    m, f'{side["barrier"]} must verify a LITERAL coverage topic, not an override')
                self.assertEqual(m.group(1), TOPIC)
                self.assertNotRegex(
                    text, rf'{side["assign"]}="?\$\{{',
                    f'an environment override in {side["barrier"]} would validate a different topology')

                # And the Deployment the deploy applies must configure exactly that, in that
                # service's own container, with a literal value a running pod can use.
                for rel in side["manifests"]:
                    env = env_of(rel, side["deployment"], side["container"])
                    self.assertIn(side["key"], env,
                                  f'{rel} must set {side["key"]}, or the barrier constant is stale')
                    self.assertEqual(env[side["key"]], TOPIC,
                                     f'{rel} sets {side["key"]}={env[side["key"]]!r}, barrier verifies {TOPIC!r}')

    def test_the_barrier_refuses_rather_than_skips_when_the_topic_is_missing(self):
        # The refusal has to say what to do: a deploy-time failure that does not name the fix is a
        # crash loop with extra steps.
        text = (ROOT / "scripts/kafka/ensure-strike-hold-topics.sh").read_text()
        self.assertIn("CRASH-LOOP", text)
        self.assertIn("ensure-es-liquidity-engine-topics.sh", text,
                      "the refusal must name the barrier that creates the topic")
        self.assertNotIn("coverage_from_manifest", text,
                         "the awk manifest parse is gone; this file replaces it")
        # AND THE REMEDIATION MUST BE TRUE. It told the operator to unset
        # KAFKA_STRIKE_HOLD_COVERAGE_TOPIC "to run without a coverage source" — which the barrier
        # deliberately ignores, so following the instruction changed nothing (Codex deploy r10). A
        # false instruction in an error message is worse than a false comment: someone acts on it.
        self.assertNotRegex(text, r"unset\s*\n?.*KAFKA_STRIKE_HOLD_COVERAGE_TOPIC",
                            "the refusal must not tell the operator to unset a variable it ignores")
        self.assertIn("no environment escape", text,
                      "the refusal should say plainly that there is no override")

    def test_the_mocked_barrier_suite_exists_and_the_gate_runs_it(self):
        wf = (ROOT / ".github/workflows/deploy-validation.yml").read_text()
        for side in SIDES:
            suite = ROOT / side["barrier"].replace(".sh", "-test.sh")
            self.assertTrue(suite.is_file() and os.access(suite, os.X_OK),
                            f"{suite.name} must exist and be executable")
            self.assertIn(f"bash {suite.relative_to(ROOT)}", wf,
                          f"{suite.name} must be wired into the gate — a suite nothing runs is not coverage")
        self.assertIn("python3 -m unittest -v tests.test_coverage_topic_wiring", wf)


if __name__ == "__main__":
    unittest.main()
