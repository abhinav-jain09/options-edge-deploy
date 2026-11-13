# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The dev web must listen to the topics dev's hunt WRITES.

Dev's hunt runs under its own identity (REVERSAL_HUNT_INSTANCE_ID, processing #700), so every topic it owns is
prefixed `<instance>.`: dev.es.reversal.hunt.{status,alerts,state} out, dev.es.reversal.hunt.commands in. The web
defaults to the UNPREFIXED names, which are production's hunt on the same es4 broker. Left at the default, the dev
page hears whoever else writes there: when that stopped (2026-11-02) every "Bottom found" / "Top found" shout and the
LIVE badge went silent while dev's own hunt kept publishing voice-eligible calls into topics nothing read.

The expected names are DERIVED from the instance the dev hunt actually runs with, so changing the instance cannot
leave the web behind. The test parses the YAML (comments cannot hide or fake a key).
"""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

# env var on the web -> the hunt's own topic suffix (ReversalHuntTopics in options-edge-contracts)
WEB_VAR_TO_TOPIC = {
    "HUNT_STATUS_TOPIC": "es.reversal.hunt.status",
    "HUNT_ALERTS_TOPIC": "es.reversal.hunt.alerts",
    "HUNT_STATE_TOPIC": "es.reversal.hunt.state",
    "HUNT_COMMANDS_TOPIC": "es.reversal.hunt.commands",
}
# BOTH dev web overlays: the monolith and the standalone slice must render identically (the mirror rule).
WEB_PATCHES = (
    "k8s/overlays/dev/web-dev-env-patch.yaml",
    "k8s/services/web/overlays/dev/web-dev-env-patch.yaml",
)
HUNT_SLICE = "k8s/services/reversal-confirmation/overlays/dev/manifest.yaml"


def container_env(path: str, deployment: str) -> dict:
    out = {}
    for doc in yaml.safe_load_all((ROOT / path).read_text()):
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        if doc.get("metadata", {}).get("name") != deployment:
            continue
        for c in doc["spec"]["template"]["spec"]["containers"]:
            for e in c.get("env", []) or []:
                out[e["name"]] = e.get("value")
    return out


def dev_hunt_instance() -> str:
    env = container_env(HUNT_SLICE, "reversal-confirmation-service")
    return (env.get("REVERSAL_HUNT_INSTANCE_ID") or "").strip()


class DevWebHuntTopics(unittest.TestCase):
    def test_dev_hunt_has_an_identity(self):
        # If this ever becomes empty the hunt writes the un-prefixed topics again and the expectations below
        # change meaning: say so loudly instead of passing on a vacuous prefix.
        self.assertEqual(dev_hunt_instance(), "dev",
                         "dev's hunt no longer runs as instance 'dev': re-derive the web topics deliberately")

    def test_web_listens_to_the_topics_the_dev_hunt_writes(self):
        instance = dev_hunt_instance()
        prefix = f"{instance}." if instance else ""
        for patch in WEB_PATCHES:
            env = container_env(patch, "options-edge-web")
            for var, topic in WEB_VAR_TO_TOPIC.items():
                with self.subTest(patch=patch, var=var):
                    self.assertEqual(env.get(var), prefix + topic)

    def test_the_two_web_overlays_agree(self):
        a, b = (container_env(p, "options-edge-web") for p in WEB_PATCHES)
        self.assertEqual({k: a.get(k) for k in WEB_VAR_TO_TOPIC}, {k: b.get(k) for k in WEB_VAR_TO_TOPIC})


if __name__ == "__main__":
    unittest.main()
