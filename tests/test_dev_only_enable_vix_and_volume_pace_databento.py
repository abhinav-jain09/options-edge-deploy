"""vix-option-inteligence-service and volume-pace-databento-service RUN on dev and stay HELD everywhere else.

Owner instruction 2026-10-31: turn both on, on dev only. Four independent facts make that true, and any one of them failing silently would either
leave the services down on dev or switch them on where they must stay off, so each is asserted on its own:

  1. the dev render has replicas 1, the production and experiment renders still have 0;
  2. neither name is in DISABLED_DEV, which is what `dev-cleanup start` consults (a name left there re-holds the service at 0 on every start, whatever the manifest renders);
  3. both names are STILL in the production hold lists (KEEP_DOWN in morning-autostart.sh and oe-boot-bringup.sh): production's morning start and boot bring-up would otherwise raise them;
  4. the vix legacy research writer is still the literal STRING "false" on dev: enabling the service must not touch the schema-v7 interlock.
"""
import re
import subprocess
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
    """The dev node was 98% CPU-RESERVED (15.8 of 16 cores) when these two were enabled, so both were Unschedulable ("Insufficient cpu").

    The room comes from unified-sr-service, whose 2-core request was sized for the 24-core PRODUCTION host (PR #1234) and reached dev through the shared base. The new services keep their BASE
    requests: the vix base records that a 100m request let CFS shares starve it at the open until its input fell ~84 minutes behind (2026-07-21), so lowering it on dev is not safe.
    """

    # (slug, deployment, container) -> (dev, production, experiment) CPU request
    CPU = {
        ("vix-option-inteligence", "vix-option-inteligence-service", "vix-option-inteligence"): ("500m", "500m", "500m"),
        ("volume-pace", "volume-pace-databento-service", "volume-pace"): ("250m", "250m", "250m"),
        ("unified-sr", "unified-sr-service", "unified-sr"): ("500m", "2", "2"),
    }
    ENVS = ("dev", "production", "experiment")

    @staticmethod
    def named(doc, container):
        found = [c for c in doc["spec"]["template"]["spec"]["containers"] if c["name"] == container]
        assert len(found) == 1, f"expected exactly one container named {container}, found {len(found)}"
        return found[0]

    def test_the_committed_slices_carry_the_expected_cpu_request_per_environment(self):
        for (slug, name, container), wanted in self.CPU.items():
            for env, want in zip(self.ENVS, wanted):
                got = self.named(deployment(slug, env, name), container)["resources"]["requests"]["cpu"]
                self.assertEqual(str(got), want, f"{name} {env}")

    def test_the_source_overlays_render_the_same_requests(self):
        # The slices are GENERATED; a mistake in the SOURCE patch or the kustomization wiring would pass a slice-only check as long as the stale slice still looked right.
        # Render the real overlays (kubectl kustomize, as the other wiring tests do) and compare the same containers.
        for env in self.ENVS:
            out = subprocess.run(["kubectl", "kustomize", str(ROOT / f"k8s/overlays/{env}")], capture_output=True, text=True, check=True, timeout=180).stdout
            docs = {d["metadata"]["name"]: d for d in yaml.safe_load_all(out) if d and d.get("kind") == "Deployment"}
            for (slug, name, container), wanted in self.CPU.items():
                got = self.named(docs[name], container)["resources"]["requests"]["cpu"]
                self.assertEqual(str(got), wanted[self.ENVS.index(env)], f"{name} {env} (source overlay)")

    def test_only_the_cpu_request_differs_between_dev_and_production(self):
        # no CPU limit anywhere (a limit would throttle), and memory request/limit on the NAMED container are identical in dev and production
        for (slug, name, container) in self.CPU:
            dev = self.named(deployment(slug, "dev", name), container)["resources"]
            prod = self.named(deployment(slug, "production", name), container)["resources"]
            for label, r in (("dev", dev), ("production", prod)):
                self.assertNotIn("cpu", r.get("limits", {}), f"{name} {label}: a CPU limit would throttle the service")
            self.assertEqual(dev["limits"], prod["limits"], f"{name}: memory limit must match production")
            self.assertEqual(dev["requests"]["memory"], prod["requests"]["memory"], f"{name}: memory request must match production")


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
