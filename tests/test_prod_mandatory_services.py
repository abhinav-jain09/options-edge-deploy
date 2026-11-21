# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The production MANDATORY always-up services (config/prod-mandatory-services.txt) must never be held down.

On 2026-11-18 production was scaled down by hand to the services the owner called mandatory. Two that the
Gamma Lab stock page depends on were left out: stock-gex-service (the page showed no data) and
stock-gex-close-scheduler (its 11-18 closing-board run was killed mid-flight and stayed unpublished).
The declared set lives in ONE file; every path that can scale production to 0 is checked against it here.
"""
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANDATORY = ROOT / "config" / "prod-mandatory-services.txt"
BOOT = ROOT / "scripts" / "ops" / "oe-boot-bringup.sh"
MORNING = ROOT / "scripts" / "ops" / "morning-autostart.sh"
OFFHOURS = ROOT / "scripts" / "ops" / "offhours-clean-slate.sh"
ES_PREDOWN = ROOT / "scripts" / "ops" / "es-predown.sh"
PROD_OVERLAY = ROOT / "k8s" / "overlays" / "production"
NAME = re.compile(r"^[a-z0-9][a-z0-9-]*[a-z0-9]$")

# The owner-confirmed set. A change to the file must change this too, on purpose: the count of
# mandatory services is a decision, not an accident.
OWNER_SET = {
    "oe-keycloak", "options-edge-web", "options-edge-redis", "feed-gateway-service",
    "options-edge-databento-feed", "raw-to-display-databento-service", "raw-postgres-writer",
    "hpsf-postgres-writer-service", "stock-gex-service", "stock-gex-close-scheduler",
    # gc-feed (owner 2026-11-21, gold replaces the NIFTY plan): Globex runs ~24h, so every
    # production scale-down path must leave the gold feed up.
    "gc-feed-service",
}


def mandatory(path=MANDATORY):
    names = []
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            names.append(line)
    return names


def boot_keep_down(path=BOOT):
    found = re.findall(r"^KEEP_DOWN='([^']*)'$", path.read_text(), re.M)
    if len(found) != 1:
        raise AssertionError(f"{path.name}: expected exactly one KEEP_DOWN='...' line, found {len(found)}")
    return found[0].split("|")


def morning_keep_down(path=MORNING):
    found = re.findall(r'^KEEP_DOWN="\$\{KEEP_DOWN:-([^}]*)\}"$', path.read_text(), re.M)
    if len(found) != 1:
        raise AssertionError(f"{path.name}: expected exactly one KEEP_DOWN line, found {len(found)}")
    return found[0].split()


def es_down_set(path=ES_PREDOWN):
    found = re.findall(r'^ES_DOWN_SET="\$\{ES_DOWN_SET:-([^}]*)\}"$', path.read_text(), re.M)
    if len(found) != 1:
        raise AssertionError(f"{path.name}: expected exactly one ES_DOWN_SET line, found {len(found)}")
    return found[0].split()


def scale_exempt_alternation(path=OFFHOURS):
    found = re.findall(r'^SCALE_EXEMPT_RE="\$\{SCALE_EXEMPT_RE:-\^\(([^)]*)\)\$\}"$', path.read_text(), re.M)
    if len(found) != 1:
        raise AssertionError(f"{path.name}: expected exactly one SCALE_EXEMPT_RE='^(a|b)$' default, found {len(found)}")
    return found[0].split("|")


def production_zero_replica_patches(overlay=PROD_OVERLAY):
    """(file, deployment name) for every Deployment/StatefulSet document in the production overlay
    that sets spec.replicas to 0."""
    held = []
    for path in sorted(overlay.rglob("*.yaml")):
        try:
            docs = list(yaml.safe_load_all(path.read_text()))
        except yaml.YAMLError:
            continue
        for doc in docs:
            if not isinstance(doc, dict) or doc.get("kind") not in ("Deployment", "StatefulSet"):
                continue
            if (doc.get("spec") or {}).get("replicas") == 0:
                held.append((path.name, (doc.get("metadata") or {}).get("name")))
    return held


class ProdMandatoryServices(unittest.TestCase):
    def test_the_declared_set_is_the_owner_set(self):
        names = mandatory()
        self.assertEqual(len(names), len(set(names)), "a mandatory service is listed twice")
        self.assertEqual([n for n in names if not NAME.match(n)], [], "not plain deployment names")
        self.assertEqual(set(names), OWNER_SET)

    def test_no_keep_down_list_holds_a_mandatory_service(self):
        m = set(mandatory())
        self.assertEqual(sorted(m & set(boot_keep_down())), [], f"{BOOT.name} KEEP_DOWN holds mandatory services")
        self.assertEqual(sorted(m & set(morning_keep_down())), [], f"{MORNING.name} KEEP_DOWN holds mandatory services")

    def test_the_es_preopen_down_list_leaves_them_up(self):
        self.assertEqual(sorted(set(mandatory()) & set(es_down_set())), [])

    def test_the_clean_slate_never_scales_them_to_zero(self):
        self.assertEqual(sorted(scale_exempt_alternation()), sorted(mandatory()),
                         "SCALE_EXEMPT_RE in offhours-clean-slate.sh must be exactly the mandatory set")

    def test_production_overlay_never_renders_them_at_zero(self):
        held = [(f, n) for f, n in production_zero_replica_patches() if n in set(mandatory())]
        self.assertEqual(held, [], "a production overlay patch renders a mandatory service at replicas 0")


class TheChecksCanFail(unittest.TestCase):
    """Each extractor must see a mandatory name when one is planted, so a green run means something."""

    def planted(self, tmp, name, text):
        p = Path(tmp) / name
        p.write_text(text)
        return p

    def test_keep_down_extractors_see_a_planted_name(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            boot = self.planted(tmp, "boot.sh", "KEEP_DOWN='a-service|stock-gex-service'\n")
            morning = self.planted(tmp, "morning.sh", 'KEEP_DOWN="${KEEP_DOWN:-a-service stock-gex-service}"\n')
            es = self.planted(tmp, "es.sh", 'ES_DOWN_SET="${ES_DOWN_SET:-a-service stock-gex-service}"\n')
            self.assertIn("stock-gex-service", boot_keep_down(boot))
            self.assertIn("stock-gex-service", morning_keep_down(morning))
            self.assertIn("stock-gex-service", es_down_set(es))

    def test_scale_exempt_extractor_sees_a_dropped_name(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.planted(tmp, "off.sh", 'SCALE_EXEMPT_RE="${SCALE_EXEMPT_RE:-^(oe-keycloak)$}"\n')
            self.assertNotEqual(sorted(scale_exempt_alternation(p)), sorted(mandatory()))

    def test_zero_replica_scan_sees_a_planted_patch(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "p.yaml").write_text(
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: stock-gex-service\nspec:\n  replicas: 0\n")
            self.assertEqual(production_zero_replica_patches(Path(tmp)), [("p.yaml", "stock-gex-service")])


if __name__ == "__main__":
    unittest.main()
