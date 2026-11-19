"""The quarantine half of the restart budget: the one clear command, and the three bring-up scripts that must NOT undo a stop.

scripts/watch/restart-budget.py scales a Deployment to 0 and annotates it options-edge.io/quarantined-at. A stop that the next 06:15 autostart, the next
dev-cleanup start or the next boot bring-up silently reverses is no stop. Each script's quarantine block is extracted from the real file and run against a
stub kubectl, so what is asserted is the code that ships, and each assertion is mutation-checked at the bottom.
"""
import atexit
import io
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OPS = REPO / "scripts/ops"
CLEAR = OPS / "quarantine-clear.sh"
STUB = r"""#!/usr/bin/env bash
echo "$*" >> "$CALLS"
case "$*" in
  *" scale "*)    [ "${FAIL:-}" = scale ] && { echo "scale denied" >&2; exit 1; }; exit 0 ;;
  *" annotate "*) [ "${FAIL:-}" = annotate ] && { echo "annotate denied" >&2; exit 1; }; exit 0 ;;
  *"get deploy -o jsonpath"*) [ "${FAIL:-}" = read ] && { echo "api down" >&2; exit 1; }; cat "$FIX/list"; exit 0 ;;
  *"get deploy/"*"quarantine-prev-replicas"*) cat "$FIX/prev"; exit 0 ;;
  *"get deploy/"*"quarantine-reason"*) cat "$FIX/why"; exit 0 ;;
  *"get deploy/"*"quarantined-at"* | *"get deploy "*"quarantined-at"*) [ "${FAIL:-}" = read ] && { echo "api down" >&2; exit 1; }; cat "$FIX/at"; exit 0 ;;
  *"get deploy"*) [ "${FAIL:-}" = read ] && { echo "api down" >&2; exit 1; }; cat "$FIX/list"; exit 0 ;;
esac
exit 0
"""


class Bench:
    def __init__(self, at="", prev="", why="", listing=""):
        d = Path(tempfile.mkdtemp(prefix="rbq-"))
        atexit.register(shutil.rmtree, str(d), True)
        for n, v in (("at", at), ("prev", prev), ("why", why), ("list", listing)):
            (d / n).write_text(v)
        self.calls = d / "calls"
        self.calls.write_text("")
        self.stub = d / "kubectl"
        self.stub.write_text(STUB)
        self.stub.chmod(self.stub.stat().st_mode | stat.S_IEXEC)
        self.env = dict(os.environ, CALLS=str(self.calls), FIX=str(d), KUBECTL=str(self.stub), KUBECONFIG="/dev/null")

    def sh(self, script, *args, fail="", timeout=60):
        r = subprocess.run(["bash", str(script), *args], capture_output=True, text=True, timeout=timeout, env=dict(self.env, FAIL=fail))
        return r.returncode, r.stdout + r.stderr

    def bash(self, code, fail=""):
        r = subprocess.run(["bash", "-c", code], capture_output=True, text=True, timeout=60, env=dict(self.env, FAIL=fail))
        return r.returncode, r.stdout + r.stderr

    def lines(self):
        return [l.strip() for l in self.calls.read_text().splitlines() if l.strip()]


class QuarantineClear(unittest.TestCase):
    def test_restores_the_recorded_replicas_then_removes_the_marker(self):
        b = Bench(at="2026-11-19T10:00:00Z", prev="2", why="7 restarts in 60 min (limit 5)")
        rc, out = b.sh(CLEAR, "dev", "svc-a")
        self.assertEqual(0, rc, out)
        scale = [l for l in b.lines() if " scale " in " " + l]
        ann = [l for l in b.lines() if " annotate " in " " + l]
        self.assertEqual(["-n options-edge scale deploy/svc-a --replicas=2"], scale)
        self.assertEqual(["-n options-edge annotate deploy/svc-a options-edge.io/quarantined-at- options-edge.io/quarantine-reason- options-edge.io/quarantine-prev-replicas-"], ann)
        self.assertLess(b.lines().index(scale[0]), b.lines().index(ann[0]),
                        "scale first: a failure between the two must leave a marker on a running deployment, never an unmarked deployment at 0")

    def test_replicas_flag_overrides(self):
        b = Bench(at="2026-11-19T10:00:00Z", prev="2")
        rc, out = b.sh(CLEAR, "dev", "svc-a", "--replicas", "1")
        self.assertEqual(0, rc, out)
        self.assertIn("-n options-edge scale deploy/svc-a --replicas=1", b.lines())

    def test_not_quarantined_changes_nothing(self):
        b = Bench(at="")
        rc, out = b.sh(CLEAR, "dev", "svc-a")
        self.assertEqual(0, rc, out)
        self.assertIn("not quarantined", out)
        self.assertFalse([l for l in b.lines() if " scale " in " " + l or " annotate " in " " + l], b.lines())

    def test_a_failed_scale_keeps_the_quarantine(self):
        b = Bench(at="2026-11-19T10:00:00Z", prev="1")
        rc, out = b.sh(CLEAR, "dev", "svc-a", fail="scale")
        self.assertEqual(1, rc, out)
        self.assertIn("quarantine is still in place", out)
        self.assertFalse([l for l in b.lines() if " annotate " in " " + l], "the marker must not be removed when the scale failed")

    def test_unusable_recorded_replicas_fall_back_to_one_and_say_so(self):
        b = Bench(at="2026-11-19T10:00:00Z", prev="")
        rc, out = b.sh(CLEAR, "dev", "svc-a")
        self.assertEqual(0, rc, out)
        self.assertIn("using 1", out)
        self.assertIn("-n options-edge scale deploy/svc-a --replicas=1", b.lines())

    def test_list_names_what_is_stopped(self):
        b = Bench(listing="svc-a\t2026-11-19T10:00:00Z\t1\t7 restarts in 60 min\nsvc-b\t\t\t\n")
        rc, out = b.sh(CLEAR, "--list", "dev")
        self.assertEqual(0, rc, out)
        self.assertIn("svc-a  stopped 2026-11-19T10:00:00Z (was 1 replica(s)): 7 restarts in 60 min", out)
        self.assertNotIn("svc-b", out)
        b2 = Bench(listing="svc-b\t\t\t\n")
        self.assertIn("nothing is quarantined", b2.sh(CLEAR, "--list", "dev")[1])

    def test_bad_arguments_are_exit_2(self):
        b = Bench()
        self.assertEqual(2, b.sh(CLEAR)[0])
        self.assertEqual(2, b.sh(CLEAR, "staging", "svc-a")[0])
        self.assertEqual(2, b.sh(CLEAR, "dev", "svc-a", "--replicas", "two")[0])


def block(path, start, end):
    """The text of a script from the line containing `start` through the first later line that IS `end` (compared whole, stripped)."""
    lines = (OPS / path).read_text().splitlines()
    i = next(n for n, l in enumerate(lines) if start in l)
    j = next(n for n in range(i, len(lines)) if lines[n].strip() == end)
    return "\n".join(lines[i:j + 1])


def function(path, name):
    text = (OPS / path).read_text()
    m = re.search(r"^%s\(\) \{\n.*?^\}\n" % re.escape(name), text, re.S | re.M)
    assert m, "%s not found in %s" % (name, path)
    return m.group(0)


LIST = "svc-a\t2026-11-19T10:00:00Z\t1\t7 restarts\nsvc-b\t\t\t\nsvc-c\t\t\t\n"


class MorningAutostartSkipsQuarantined(unittest.TestCase):
    def run_block(self, fail=""):
        b = Bench(listing=LIST)
        code = ('log() { echo "LOG: $*"; }\ndie() { echo "DIE: $*"; exit 3; }\nNS=options-edge\nkcr() { "$KUBECTL" -n "$NS" "$@"; }\n'
                'HOLD_NOW="a b"\n' + block("morning-autostart.sh", "QUAR_RAW=", "fi")
                + '\necho "HOLD_NOW=$HOLD_NOW"\n')
        return b.bash(code, fail)

    def test_a_quarantined_deployment_joins_the_hold_set_and_is_logged(self):
        rc, out = self.run_block()
        self.assertEqual(0, rc, out)
        self.assertIn("QUARANTINED by the restart budget, left at 0", out)
        self.assertIn("svc-a", out)
        self.assertRegex(out, r"HOLD_NOW=a b\s+svc-a\s*$", "only svc-a is quarantined; svc-b/svc-c are not held")
        self.assertNotIn("svc-b ", out.split("HOLD_NOW=")[1])

    def test_an_unreadable_annotation_list_refuses_to_scale(self):
        rc, out = self.run_block(fail="read")
        self.assertEqual(3, rc, out)
        self.assertIn("DIE: cannot read the restart-budget quarantine annotations", out)


class BootBringupSkipsQuarantined(unittest.TestCase):
    def run_scale_up(self, annotation):
        b = Bench(at=annotation)
        code = ('log() { echo "LOG: $*"; }\nKUBECTL="$KUBECTL"\nSA=""\n' + function("oe-boot-bringup.sh", "scale_up") + '\nscale_up "svc-a"\n')
        rc, out = b.bash(code)
        return rc, out, b.lines()

    def test_a_quarantined_deployment_is_left_at_zero(self):
        rc, out, calls = self.run_scale_up("2026-11-19T10:00:00Z")
        self.assertEqual(0, rc, out)
        self.assertIn("QUARANTINED svc-a", out)
        self.assertFalse([c for c in calls if " scale " in " " + c], calls)

    def test_a_normal_deployment_is_scaled_up(self):
        rc, out, calls = self.run_scale_up("")
        self.assertEqual(0, rc, out)
        self.assertTrue([c for c in calls if "scale deploy svc-a --replicas=1" in c], calls)


class DevCleanupStartSkipsQuarantined(unittest.TestCase):
    def helper(self, listing, fail=""):
        b = Bench(listing=listing)
        code = 'KK="$KUBECTL -n options-edge"\n' + function("dev-cleanup.sh", "quarantined_deploys") + '\nquarantined_deploys; echo "rc=$?"\n'
        return b.bash(code, fail)

    def test_lists_only_the_annotated(self):
        rc, out = self.helper(LIST)
        self.assertEqual(0, rc, out)
        self.assertEqual(["svc-a", "rc=0"], out.split())

    def test_nothing_annotated_is_an_empty_list(self):
        self.assertEqual(["rc=0"], self.helper("svc-b\t\t\t\n")[1].split())

    def test_an_unreadable_list_fails_closed(self):
        rc, out = self.helper(LIST, fail="read")
        self.assertIn("rc=1", out, "the callers refuse to scale on a failed read; an empty list here would lift every quarantine")

    def test_both_start_paths_refuse_on_a_failed_read_and_skip_the_quarantined(self):
        src = (OPS / "dev-cleanup.sh").read_text()
        self.assertEqual(2, len(re.findall(r'quar="\$\(quarantined_deploys\)" \|\| \{ echo ".*REFUSED', src)),
                         "do_start AND do_start_overnight must both refuse when the annotations cannot be read")
        self.assertEqual(2, src.count("QUARANTINED by the restart budget, left at 0"), "both start paths must say what they left at 0")


class MutationChecks(unittest.TestCase):
    """The assertions above must be able to fail: break each shipped line and see the matching test fail."""

    def _fails(self, rel, old, new, suite):
        p = OPS / rel
        original = p.read_text()
        self.assertEqual(1, original.count(old), "mutation anchor in %s matches %d times" % (rel, original.count(old)))
        try:
            p.write_text(original.replace(old, new))
            res = unittest.TextTestRunner(stream=io.StringIO()).run(unittest.defaultTestLoader.loadTestsFromTestCase(suite))
            return not res.wasSuccessful()
        finally:
            p.write_text(original)

    def test_morning_autostart_mutations_are_caught(self):
        self.assertTrue(self._fails("morning-autostart.sh", '  HOLD_NOW="$HOLD_NOW $QUARANTINED"\n', "  :\n", MorningAutostartSkipsQuarantined))
        self.assertTrue(self._fails("morning-autostart.sh", '  || die "cannot read the restart-budget quarantine annotations - refusing to scale (it could undo a quarantine)"',
                                    "  || true", MorningAutostartSkipsQuarantined))

    def test_boot_bringup_mutation_is_caught(self):
        self.assertTrue(self._fails("oe-boot-bringup.sh", '    [ -n "$q" ] && { log "QUARANTINED $d', '    [ -n "$q" ] && false && { log "QUARANTINED $d', BootBringupSkipsQuarantined))

    def test_dev_cleanup_mutation_is_caught(self):
        self.assertTrue(self._fails("dev-cleanup.sh", "2>/dev/null)\" || return 1\n  printf", "2>/dev/null)\" || true\n  printf", DevCleanupStartSkipsQuarantined))

    def test_clear_script_mutation_is_caught(self):
        self.assertTrue(self._fails("quarantine-clear.sh", 'k scale "deploy/$DEP" --replicas="$REPLICAS" || { echo "scale failed - the quarantine is still in place" >&2; exit 1; }',
                                    'k scale "deploy/$DEP" --replicas="$REPLICAS" || true', QuarantineClear))


if __name__ == "__main__":
    unittest.main()
