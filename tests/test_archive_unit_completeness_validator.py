"""Tests for scripts/ci/validate-archive-unit-completeness.sh.

WHY THIS FILE EXISTS. The guard decides whether every file the archive crontab depends on is in the
repo, in the deploy job's UNIT, and — for the two that live outside the unit directory — actually
STAGED by the job before the containerised suite runs. Until PR #1125's review round 2 the staging
half of that was a substring match on the whole Jenkinsfile with its newlines squeezed out, so the
characters `cp <source> <dest>` satisfied it from a comment, a string, or a stage that runs after
the suite. A guard that can be green from text that never executes is the defect it exists to catch.

Each case mutates the real job definition in a temporary copy of the repository and asserts the
guard's VERDICT changes. The decoy cases are the point: they leave a convincing `cp` in place and
remove the one that runs.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUARD = "scripts/ci/validate-archive-unit-completeness.sh"
JF = "Jenkinsfile.archive-scripts-deploy"
# The two dependencies that live outside scripts/ops/archive and are therefore staged by the job.
STAGED = {
    "market_calendar.py": "scripts/jenkins/market_calendar.py",
    "vol-premium-open-reference-capture.py": "scripts/ops/vol-premium-open-reference-capture.py",
}
MOUNT = 'scripts/ops/archive:/w:ro'


def _cp(name: str) -> str:
    """The staging command as the job writes it, continuation and all."""
    source = STAGED[name]
    one_line = f"          cp {source} scripts/ops/archive/{name}\n"
    wrapped = (f"          cp {source} \\\n"
               f"             scripts/ops/archive/{name}\n")
    text = (ROOT / JF).read_text()
    if one_line in text:
        return one_line
    assert wrapped in text, f"the job does not stage {name} in either shape"
    return wrapped


class StagingCheckTest(unittest.TestCase):
    def _run(self, edit=None, before=None) -> subprocess.CompletedProcess:
        """Run the guard against a copy of the repo, optionally with the job definition edited."""
        work = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, work, True)
        for part in ("scripts", JF):
            source = ROOT / part
            target = work / part
            if source.is_dir():
                shutil.copytree(source, target, symlinks=True)
            else:
                shutil.copy2(source, target)
        if before is not None:
            before(work)
        if edit is not None:
            (work / JF).write_text(edit((work / JF).read_text()))
        return subprocess.run(["bash", GUARD], cwd=work, capture_output=True, text=True)

    def test_the_committed_tree_passes(self) -> None:
        r = self._run()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("validate-archive-unit-completeness: OK", r.stdout)

    def test_a_removed_staging_command_is_caught(self) -> None:
        for name in STAGED:
            with self.subTest(name=name):
                r = self._run(lambda t, n=name: t.replace(_cp(n), ""))
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn("NOT STAGED FOR THE SUITE", r.stderr)
                self.assertIn(n := name, r.stderr)

    def test_a_commented_out_staging_command_does_not_satisfy_it(self) -> None:
        """THE DECOY. The characters are all there and the command does not run."""
        for name in STAGED:
            with self.subTest(name=name):
                r = self._run(lambda t, n=name: t.replace(_cp(n), "          # " + _cp(n).strip() + "\n"))
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn("NOT STAGED FOR THE SUITE", r.stderr)

    def test_staging_after_the_suite_mount_is_caught(self) -> None:
        """A cp that runs after the container has started stages nothing the suite can see."""
        def move_it_later(text: str) -> str:
            command = _cp("vol-premium-open-reference-capture.py")
            text = text.replace(command, "")
            # put it after the docker run that mounts the unit directory
            anchor = next(l for l in text.split("\n") if MOUNT in l)
            return text.replace(anchor, anchor + "\n" + command.rstrip("\n"))
        r = self._run(move_it_later)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("STAGED TOO LATE", r.stderr)

    def test_the_ordering_the_guard_relies_on_is_asserted_not_assumed(self) -> None:
        """If the job stops mounting the unit directory into a container, the ordering test is
        meaningless — and a guard that silently keeps passing on a premise that has gone is worse
        than one that says so."""
        r = self._run(lambda t: t.replace(MOUNT, "scripts/ops/archive:/elsewhere:ro"))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("CANNOT CHECK STAGING", r.stderr)

    def test_a_stale_staged_copy_does_not_excuse_the_staging(self) -> None:
        """The hole this file found. Both staged files are gitignored inside the unit directory, so a
        reused CI workspace — or any machine where the job or the suite has run once — has a copy
        sitting there. Resolving the dependency to that copy made it look like a unit file and
        skipped the staging requirement altogether: the check went missing precisely where an
        earlier build had left evidence behind."""
        name = "vol-premium-open-reference-capture.py"

        def leave_a_stale_copy(work: Path) -> None:
            (work / "scripts" / "ops" / "archive" / name).write_text("# left by an earlier build\n")

        r = self._run(lambda t: t.replace(_cp(name), ""), before=leave_a_stale_copy)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("NOT STAGED FOR THE SUITE", r.stderr)

    def test_a_dependency_missing_from_unit_is_still_caught(self) -> None:
        """The older half of the guard, which the staging work must not have broken."""
        r = self._run(lambda t: t.replace(" vol-premium-open-reference-capture.py\"", "\"", 1))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("NOT IN UNIT", r.stderr)


if __name__ == "__main__":
    unittest.main()
