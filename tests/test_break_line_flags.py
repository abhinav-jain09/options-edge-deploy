"""D3 of the Break Score rev 13: the dev flags that turn the display on (gateway relay, web commands). Last step of the roll order (design liq-S-break-score-rev13-design.md 17.2 step 9); rollback is
setting both back to false. DEV ONLY: no base, production, experiment or es4 manifest carries either flag, so each stays at its code default (false) everywhere else."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
GATEWAY_DEV = ("k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/services/feed-gateway/overlays/dev/manifest.yaml")
WEB_DEV = ("k8s/overlays/dev/web-dev-env-patch.yaml", "k8s/services/web/overlays/dev/web-dev-env-patch.yaml")


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def env_values(rel: str, flag: str) -> list:
    out = []
    for doc in yaml.safe_load_all(read(rel)):
        if not doc:
            continue
        spec = doc.get("spec", {}).get("template", {}).get("spec", {})
        for c in spec.get("containers", []):
            out += [e.get("value") for e in c.get("env", []) if e.get("name") == flag]
    return out


class Flags(unittest.TestCase):
    def test_the_gateway_relay_flag_is_true_in_both_dev_manifests_and_once(self):
        for rel in GATEWAY_DEV:
            self.assertEqual(env_values(rel, "BREAK_LINE_RELAY_ENABLED"), ["true"], rel)

    def test_the_web_commands_flag_is_true_in_both_dev_web_patches_and_once(self):
        for rel in WEB_DEV:
            self.assertEqual(env_values(rel, "BREAK_LINE_COMMANDS_ENABLED"), ["true"], rel)

    def test_no_other_manifest_carries_either_flag(self):
        allowed = set(GATEWAY_DEV) | set(WEB_DEV)
        offenders = []
        for p in list((ROOT / "k8s").rglob("*.yaml")) + [ROOT / "services.yaml"]:
            rel = str(p.relative_to(ROOT))
            if rel in allowed:
                continue
            text = p.read_text()
            if re.search(r"BREAK_LINE_(RELAY|COMMANDS)_ENABLED", text):
                offenders.append(rel)
        self.assertEqual(offenders, [], "the flags exist only in the dev patches and their generated slice or mirror")

    def test_the_rollback_is_exactly_those_two_values_and_nothing_else_in_the_patches_mentions_break_line_but_comments(self):
        for rel in GATEWAY_DEV + WEB_DEV:
            lines = [l for l in read(rel).splitlines() if "BREAK_LINE" in l and not l.lstrip().startswith("#")]
            self.assertEqual(len(lines), 1, rel)
            self.assertRegex(lines[0], r"name: BREAK_LINE_(RELAY|COMMANDS)_ENABLED")


if __name__ == "__main__":
    unittest.main()
