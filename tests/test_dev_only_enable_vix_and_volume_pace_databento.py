"""vix-option-inteligence-service and volume-pace-databento-service RUN on dev and stay HELD everywhere else.

Owner instruction 2026-10-31: turn both on, on dev only. Four independent facts make that true, and any one of them failing silently would either
leave the services down on dev or switch them on where they must stay off, so each is asserted on its own:

  1. the dev render has replicas 1, the production and experiment renders still have 0;
  2. neither name is in DISABLED_DEV, which is what `dev-cleanup start` consults (a name left there re-holds the service at 0 on every start, whatever the manifest renders);
  3. both names are STILL in the production hold lists (KEEP_DOWN in morning-autostart.sh and oe-boot-bringup.sh): production's morning start and boot bring-up would otherwise raise them;
  4. the vix legacy research writer is still the literal STRING "false" on dev: enabling the service must not touch the schema-v7 interlock.
"""
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SERVICES = {"vix-option-inteligence": "vix-option-inteligence-service", "volume-pace": "volume-pace-databento-service"}


def read(rel):
    return (ROOT / rel).read_text()


def deployment(slug, env, name):
    docs = [d for d in yaml.safe_load_all(read(f"k8s/services/{slug}/overlays/{env}/manifest.yaml")) if d]
    found = [d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == name]
    assert len(found) == 1, f"{slug}/{env}: expected one Deployment named {name}, found {len(found)}"
    return found[0]


def assignment(rel, var):
    """The text of the single-line VAR='...' / VAR="..." assignment, so a name in a comment never counts as being in the list."""
    lines = [l for l in read(rel).splitlines() if re.match(rf"\s*{var}=", l)]
    # Exactly one: with several, a stale earlier assignment that still names the service would satisfy a membership check
    # while a later, effective one did not (or the reverse).
    assert len(lines) == 1, f"{rel}: expected exactly one {var}= assignment, found {len(lines)}"
    return lines[0]


def names(text, name):
    return re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", text) is not None


class DevOnlyEnable(unittest.TestCase):
    def test_dev_renders_one_replica_and_production_and_experiment_still_render_zero(self):
        for slug, name in SERVICES.items():
            for env, want in (("dev", 1), ("production", 0), ("experiment", 0)):
                self.assertEqual(deployment(slug, env, name)["spec"]["replicas"], want, f"{name} {env}")

    def test_dev_cleanup_no_longer_holds_either_service(self):
        text = assignment("scripts/ops/dev-cleanup.sh", "DISABLED_DEV")
        for name in SERVICES.values():
            self.assertFalse(names(text, name), f"DISABLED_DEV still names {name}: dev-cleanup start would re-hold it at 0")

    def test_production_hold_lists_still_name_both_services(self):
        for rel in ("scripts/ops/morning-autostart.sh", "scripts/ops/oe-boot-bringup.sh"):
            text = assignment(rel, "KEEP_DOWN")
            for name in SERVICES.values():
                self.assertTrue(names(text, name), f"{rel}: KEEP_DOWN must still hold {name} (this change is dev-only)")

    def test_the_vix_legacy_research_writer_is_still_the_literal_string_false_on_dev(self):
        c = deployment("vix-option-inteligence", "dev", "vix-option-inteligence-service")["spec"]["template"]["spec"]["containers"][0]
        entries = [e for e in c["env"] if e["name"] == "ZERODTE_RESEARCH_ENABLED"]
        self.assertEqual(len(entries), 1)
        self.assertIs(type(entries[0]["value"]), str)
        self.assertEqual(entries[0]["value"], "false")
        self.assertNotIn("valueFrom", entries[0])


class DevCpuRequests(unittest.TestCase):
    """The dev node was 98% CPU-RESERVED (15.8 of 16 cores) when these were enabled, so the base requests left both Unschedulable. Dev lowers the REQUEST only."""

    # name -> (container, dev request, base request that production and experiment must keep)
    CPU = {"vix-option-inteligence-service": ("vix-option-inteligence", "vix-option-inteligence", "50m", "500m"),
           "volume-pace-databento-service": ("volume-pace", "volume-pace", "100m", "250m")}

    def request(self, slug, env, name, container):
        c = [c for c in deployment(slug, env, name)["spec"]["template"]["spec"]["containers"] if c["name"] == container]
        self.assertEqual(len(c), 1, f"{slug}/{env}: container {container}")
        return c[0]["resources"]["requests"]["cpu"]

    def test_dev_requests_the_reduced_cpu_and_production_and_experiment_keep_the_base_value(self):
        slugs = {"vix-option-inteligence-service": "vix-option-inteligence", "volume-pace-databento-service": "volume-pace"}
        for name, (container, _unused, dev, base) in self.CPU.items():
            self.assertEqual(self.request(slugs[name], "dev", name, container), dev, f"{name} dev")
            for env in ("production", "experiment"):
                self.assertEqual(self.request(slugs[name], env, name, container), base, f"{name} {env} must keep the base request")

    def test_dev_memory_and_cpu_limits_are_not_touched(self):
        # only the CPU REQUEST changes: the memory request/limit stay what the base declares, and no CPU limit is introduced
        for name, slug in (("vix-option-inteligence-service", "vix-option-inteligence"), ("volume-pace-databento-service", "volume-pace")):
            for env in ("dev", "production"):
                r = deployment(slug, env, name)["spec"]["template"]["spec"]["containers"][0]["resources"]
                self.assertNotIn("cpu", r.get("limits", {}), f"{name} {env}: a CPU limit would throttle the service")
            dev_r = deployment(slug, "dev", name)["spec"]["template"]["spec"]["containers"][0]["resources"]
            prod_r = deployment(slug, "production", name)["spec"]["template"]["spec"]["containers"][0]["resources"]
            self.assertEqual(dev_r["limits"], prod_r["limits"], f"{name}: memory limit must match production")
            self.assertEqual(dev_r["requests"]["memory"], prod_r["requests"]["memory"], f"{name}: memory request must match production")


class ProductionReadinessGate(unittest.TestCase):
    def test_the_production_premarket_gate_does_not_require_either_service(self):
        # premarket-readiness.sh runs on the PROD host and hard-FAILs any PIPELINE_SERVICES entry that is at 0 desired replicas. Production still
        # holds both at 0, so listing either would turn the prod readiness card red every morning. Re-add them there only when PRODUCTION runs them.
        text = read("scripts/ops/premarket-readiness.sh")
        block = re.search(r'PIPELINE_SERVICES="\$\{PIPELINE_SERVICES:-\\\n(.*?)\}"', text, re.S)
        self.assertIsNotNone(block, "PIPELINE_SERVICES default block not found")
        for name in SERVICES.values():
            self.assertFalse(names(block.group(1), name), f"premarket-readiness PIPELINE_SERVICES names {name}, which production holds at 0")


if __name__ == "__main__":
    unittest.main()
