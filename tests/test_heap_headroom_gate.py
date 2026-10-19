"""The heap-headroom gate's own arithmetic and its fail-closed behaviour.

A gate that misreads a quantity, or skips one it cannot read, reports coverage it does not have —
the same "looks like coverage" failure validate-tests-are-collectable.sh exists to catch. The
first version of this gate did both: it read "1G" as 1024Mi (7% too generous, enough to make a
borderline container look compliant) and silently skipped anything it could not parse.
"""
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

from heap_headroom import Unparseable, to_mib  # noqa: E402


class QuantityArithmeticTest(unittest.TestCase):
    def test_binary_suffixes_are_powers_of_1024(self) -> None:
        self.assertAlmostEqual(to_mib("1Gi"), 1024.0, places=6)
        self.assertAlmostEqual(to_mib("2048Mi"), 2048.0, places=6)
        self.assertAlmostEqual(to_mib("1048576Ki"), 1024.0, places=6)

    def test_decimal_suffixes_are_powers_of_1000_not_1024(self) -> None:
        # The distinction is the point: 1G is 1000^3 bytes, which is 953.67Mi, NOT 1024Mi.
        self.assertAlmostEqual(to_mib("1G"), 1000 ** 3 / 1024 ** 2, places=6)
        self.assertAlmostEqual(to_mib("500M"), 500 * 1000 ** 2 / 1024 ** 2, places=6)
        self.assertNotAlmostEqual(to_mib("1G"), to_mib("1Gi"), places=1)

    def test_fractions_and_exponents_are_valid_quantities(self) -> None:
        self.assertAlmostEqual(to_mib("0.5Gi"), 512.0, places=6)
        self.assertAlmostEqual(to_mib("3e9"), 3e9 / 1024 ** 2, places=6)
        self.assertAlmostEqual(to_mib("1024"), 1024 / 1024 ** 2, places=6)

    def test_an_unreadable_quantity_raises_rather_than_being_skipped(self) -> None:
        for bad in ("3.5.2Gi", "Gi", "abc", "12Xi", "", "   "):
            with self.subTest(quantity=bad):
                with self.assertRaises(Unparseable):
                    to_mib(bad)


class GateBehaviourTest(unittest.TestCase):
    SCRIPT = ROOT / "scripts" / "ci" / "validate-heap-leaves-container-headroom.sh"

    def run_gate(self, env_pct=None):
        env = None
        if env_pct is not None:
            import os
            env = dict(os.environ, HEAP_MAX_PERCENT_OF_LIMIT=str(env_pct))
        return subprocess.run(["bash", str(self.SCRIPT)], cwd=ROOT, env=env,
                              capture_output=True, text=True)

    def test_the_repo_is_currently_within_the_band(self) -> None:
        r = self.run_gate()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("all at or below", r.stdout)

    def test_the_gate_can_actually_fail(self) -> None:
        # A threshold of 0% must reject everything. A gate that cannot be made to fail is
        # indistinguishable from one that is not running.
        r = self.run_gate(env_pct=0)
        self.assertNotEqual(r.returncode, 0, "the gate must exit non-zero on a violation")
        self.assertIn("FAIL:", r.stdout)
        self.assertNotIn("validate-heap-leaves-container-headroom: OK", r.stdout)

    def test_the_gate_refuses_to_pass_vacuously(self) -> None:
        # It must check a real number of containers, not report OK over an empty set.
        r = self.run_gate()
        self.assertRegex(r.stdout, r"checked [1-9]\d* heap-pinned container\(s\)")


if __name__ == "__main__":
    unittest.main()
