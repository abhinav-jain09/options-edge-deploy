"""The prod boot bring-up and the morning wake-up must hold down the SAME services.

scripts/ops/oe-boot-bringup.sh (systemd, after every prod reboot) scales up every deployment sitting at
0 except its KEEP_DOWN; scripts/ops/morning-autostart.sh (06:15 ET) scales every selected deployment to
1 except its KEEP_DOWN. A service on only the morning list is resurrected by the next reboot: on
2026-08-24 the boot list was shorter and that resurrection took production down twice. The two lists
had drifted again by 2026-09-15 (oi-shadow-service and raw-to-display-service held down only in the
morning, prod-pgadmin only at boot), and nothing but a comment asked anyone to keep them together.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOOT = ROOT / "scripts" / "ops" / "oe-boot-bringup.sh"
MORNING = ROOT / "scripts" / "ops" / "morning-autostart.sh"
NAME = re.compile(r"^[a-z0-9][a-z0-9-]*[a-z0-9]$")


def assignments(path, pattern):
    return re.findall(pattern, path.read_text(), re.M)


def boot_keep_down():
    # KEEP_DOWN='a|b|c' — used as `grep -vE "$KEEP_DOWN"`
    found = assignments(BOOT, r"^KEEP_DOWN='([^']*)'$")
    if len(found) != 1:
        raise AssertionError(f"{BOOT.name}: expected exactly one KEEP_DOWN='...' line, found {len(found)}")
    return found[0].split("|")


def morning_keep_down():
    # KEEP_DOWN="${KEEP_DOWN:-a b c}" — iterated word by word
    found = assignments(MORNING, r'^KEEP_DOWN="\$\{KEEP_DOWN:-([^}]*)\}"$')
    if len(found) != 1:
        raise AssertionError(f'{MORNING.name}: expected exactly one KEEP_DOWN="${{KEEP_DOWN:-...}}" line, found {len(found)}')
    return found[0].split()


class KeepDownListsAgree(unittest.TestCase):
    def assert_well_formed(self, script, entries):
        self.assertTrue(entries, f"{script}: KEEP_DOWN is empty")
        bad = [e for e in entries if not NAME.match(e)]
        self.assertEqual(bad, [], f"{script}: KEEP_DOWN entries that are not plain deployment names")
        dupes = sorted({e for e in entries if entries.count(e) > 1})
        self.assertEqual(dupes, [], f"{script}: KEEP_DOWN lists these more than once")

    def test_each_list_is_a_list_of_deployment_names(self):
        self.assert_well_formed(BOOT.name, boot_keep_down())
        self.assert_well_formed(MORNING.name, morning_keep_down())

    def test_boot_and_morning_hold_down_the_same_set(self):
        boot, morning = set(boot_keep_down()), set(morning_keep_down())
        self.assertEqual(
            (sorted(morning - boot), sorted(boot - morning)),
            ([], []),
            "(held down only by morning-autostart.sh -> a reboot resurrects them, "
            "held down only by oe-boot-bringup.sh) — change BOTH lists or neither",
        )


if __name__ == "__main__":
    unittest.main()
