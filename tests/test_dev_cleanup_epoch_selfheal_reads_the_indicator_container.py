"""The dev-cleanup epoch self-heal must read the logs of the container that actually fails.

2026-10-22: after a dev clean the indicator-service pod looped on
`EpochFatalException: no epoch recovery copy and INDICATOR_EPOCH_GENESIS is not authorized`, and
dev-cleanup.sh's self-heal - written for exactly this - never ran. The pod has two containers
(indicator-estimate, indicator-service); `kubectl logs deploy/indicator-service` defaults to the FIRST,
which never contains the exception. Measured on the live pod: the default container matched the
signature 0 times, `-c indicator-service` 3 times. The check therefore counted 0 on every run and
the one-time genesis was skipped, leaving indicator-service and everything behind it
(market-structure) not Ready until the container was named.

This pins the contract: every log read of the indicator deployment in dev-cleanup.sh names the
container.
"""
import re
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "dev-cleanup.sh"


class EpochSelfHealReadsTheRightContainer(unittest.TestCase):
    def setUp(self):
        self.text = SCRIPT.read_text()

    def test_every_indicator_log_read_names_the_container(self):
        reads = re.findall(r"logs deploy/indicator-service\b[^\n|]*", self.text)
        self.assertTrue(reads, "the epoch self-heal's log read is gone; this test must be updated deliberately")
        for read in reads:
            self.assertIn(
                "-c indicator-service",
                read,
                f"a bare log read hits the default container (indicator-estimate): {read!r}",
            )

    def test_the_failure_signature_is_still_what_the_service_throws(self):
        # The grep only works while the service's message and the script's signature agree.
        self.assertIn("INDICATOR_EPOCH_GENESIS is not authorized", self.text)

    def test_genesis_is_turned_back_off_after_the_bootstrap(self):
        on = self.text.index("INDICATOR_EPOCH_GENESIS=true")
        off = self.text.index("INDICATOR_EPOCH_GENESIS=false", on)
        self.assertLess(on, off, "genesis must be reset to false after the one-time bootstrap")


if __name__ == "__main__":
    unittest.main()
