# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""The BOOK input of the level absorption service (document A revision 12, work package K3) as the deploy repository declares it.

The book is on for development only (the engine's version 2 records exist); it is configured on development only; its environment literal is the engine's; and its gate is never set.
"""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
PATCH = ROOT / "k8s/overlays/dev/level-absorption-dev-enable-patch.yaml"
SLICE = ROOT / "k8s/services/level-absorption/overlays/dev/manifest.yaml"
ENGINE = ROOT / "k8s/services/es-liquidity-engine/overlays/dev/manifest.yaml"


def env(text, name):
    m = re.search(r"- name: %s\n\s+value: (.+)" % re.escape(name), text)
    return m.group(1).strip().strip('"') if m else None


class BookDeploy(unittest.TestCase):
    def test_the_book_is_declared_on_for_dev_and_the_slice_mirrors_the_patch(self):
        for f in (PATCH, SLICE):
            t = f.read_text()
            self.assertEqual(env(t, "LEVEL_ABSORPTION_BOOK_ENABLED"), "true", f)
            self.assertEqual(env(t, "LEVEL_ABSORPTION_BOOK_CONTRACTS"), "classpath:/book-contracts.txt", f)
            self.assertEqual(env(t, "LEVEL_ABSORPTION_BOOK_ENVIRONMENT"), "Rithmic Paper Trading", f)

    def test_the_environment_literal_is_the_engines(self):
        self.assertEqual(env(PATCH.read_text(), "LEVEL_ABSORPTION_BOOK_ENVIRONMENT"), env(ENGINE.read_text(), "ES_MBO_ENVIRONMENT"))

    def test_the_gate_and_the_test_literal_are_never_set_anywhere(self):
        for f in ROOT.glob("k8s/**/*.yaml"):
            t = f.read_text()
            self.assertIsNone(re.search(r"- name: LEVEL_ABSORPTION_BOOK_(GATE|ALLOW_TEST)\b", t), f)

    def test_production_declares_no_book_setting(self):
        for f in ROOT.glob("k8s/services/level-absorption/overlays/production/*.yaml"):
            self.assertIsNone(re.search(r"- name: LEVEL_ABSORPTION_BOOK", f.read_text()), f)
        for f in list(ROOT.glob("k8s/overlays/production/**/*.yaml")) + list(ROOT.glob("k8s/services/level-absorption/overlays/experiment/**/*.yaml")):
            self.assertIsNone(re.search(r"- name: LEVEL_ABSORPTION_BOOK", f.read_text()), f)


if __name__ == "__main__":
    unittest.main()
