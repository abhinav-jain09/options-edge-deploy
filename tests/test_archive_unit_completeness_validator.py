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
            lines = text.split("\n")
            at = next(i for i, l in enumerate(lines) if MOUNT in l)
            # The mount sits inside a continued `docker run …` statement, so the copy must go after
            # that statement ENDS — otherwise it becomes part of it and the guard reports it absent,
            # which is true but is not the ordering this case is about.
            while lines[at].rstrip().endswith("\\"):
                at += 1
            lines.insert(at + 1, command.rstrip("\n"))
            return "\n".join(lines)
        r = self._run(move_it_later)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("STAGED TOO LATE", r.stderr)

    def test_a_staged_copy_that_is_not_declared_to_the_tree_verifier_is_caught(self) -> None:
        """THE BUG THE FIRST REAL INSTALL FOUND. Both staged copies are gitignored where they land,
        and verify-permitted-tree.sh refuses ANY ignored path it was not told to expect. #1125 added
        the second copy and not the declaration, so the install stopped at that gate — and a
        tests-only run never sees it, because it skips the stage. Every file this guard requires a
        staging `cp` for must therefore also be declared."""
        for name in STAGED:
            with self.subTest(name=name):
                r = self._run(lambda t, n=name: t.replace(
                    f" --allow-ignored scripts/ops/archive/{n}", ""))
                self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
                self.assertIn("STAGED BUT NOT DECLARED", r.stderr)
                self.assertIn(name, r.stderr)

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


    def test_inert_text_does_not_satisfy_the_staging_check(self) -> None:
        """THE REVIEWER'S DECOY, and two of its relatives. Each leaves the exact characters
        `cp <source> <dest>` in the job definition on a line that is not a comment, and in each the
        copy does not happen. The guard requires the STATEMENT to be the copy."""
        name = "market_calendar.py"
        command = _cp(name)
        body = command.strip()
        for label, replacement in [
                ("echoed", f'          echo "{body}"\n'),
                ("a Groovy string", f'          def unused = "{body}"\n'),
                ("guarded by false", f"          false && {body}\n"),
                ("a different destination", command.replace(
                    f"scripts/ops/archive/{name}", f"scripts/ops/archive/{name}.bak")),
        ]:
            with self.subTest(decoy=label):
                r = self._run(lambda t, rep=replacement: t.replace(command, rep))
                self.assertEqual(r.returncode, 1, label + r.stdout + r.stderr)
                self.assertIn("NOT STAGED FOR THE SUITE", r.stderr, label)

    def test_the_awk_program_contains_no_apostrophe(self) -> None:
        """The staging check is an awk program delimited by single quotes, so ONE apostrophe inside
        it — in a comment, in the word "jobs" — closes the program early and hands the remainder to
        the shell. That happened while this guard was being written: bash reported an unmatched
        backtick and the guard produced nothing while appearing to run. The program is taken from
        its opening quote to the `' "$JF"` that closes it, and must hold no quote of its own."""
        guard = (ROOT / GUARD).read_text()
        opening = guard.index("staged=$(awk ")
        opening = guard.index("'", opening) + 1
        closing = guard.index("' \"$JF\"", opening)
        program = guard[opening:closing]
        self.assertIn("cp ", program, "the awk program was not located")
        self.assertNotIn("'", program)


class StagedContentTest(unittest.TestCase):
    """scripts/ci/verify-archive-unit-staged.sh is the CONTROL the text guard defers to: it asserts
    the files the containerised suite will read are byte-identical to their committed sources. It is
    what a text rule cannot be — Jenkins reuses workspaces, both staged copies are gitignored inside
    the mounted directory, and a copy an earlier build left behind makes an absent staging step look
    like a present one."""

    VERIFIER = "scripts/ci/verify-archive-unit-staged.sh"

    def _run(self, stage=None) -> subprocess.CompletedProcess:
        work = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, work, True)
        shutil.copytree(ROOT / "scripts", work / "scripts", symlinks=True)
        target = work / "scripts" / "ops" / "archive"
        for name, source in STAGED.items():
            copy = target / name
            if copy.exists():
                copy.unlink()                    # start from an unstaged workspace, every time
        if stage is not None:
            stage(work, target)
        return subprocess.run(["bash", self.VERIFIER], cwd=work, capture_output=True, text=True)

    @staticmethod
    def _stage_both(work: Path, target: Path) -> None:
        for name, source in STAGED.items():
            shutil.copy2(work / source, target / name)

    def test_a_correctly_staged_workspace_passes(self) -> None:
        r = self._run(self._stage_both)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("verify-archive-unit-staged: OK", r.stdout)

    def test_an_unstaged_workspace_fails(self) -> None:
        """The case a text rule cannot see: the job's copy never ran and nothing was left behind."""
        r = self._run()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        for name in STAGED:
            self.assertIn(name, r.stderr)
        self.assertIn("NOT STAGED", r.stderr)

    def test_a_copy_left_by_an_earlier_build_fails_when_it_differs(self) -> None:
        """The reused-workspace case. A stale copy EQUAL to its source is harmless — the suite reads
        the right bytes — so the thing that must fail is one that differs."""
        name = "vol-premium-open-reference-capture.py"

        def stale(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            with (target / name).open("a") as handle:
                handle.write("# left by an earlier build\n")

        r = self._run(stale)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("STALE STAGED COPY", r.stderr)
        self.assertIn(name, r.stderr)

    def test_a_stale_copy_identical_to_its_source_is_accepted(self) -> None:
        """Stated as its own case so the one above is not read as a provenance claim: this verifier
        does not know which build wrote the file, and does not need to."""
        def stale_but_identical(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            os.utime(target / "market_calendar.py", (0, 0))

        r = self._run(stale_but_identical)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_a_missing_source_is_named_as_such(self) -> None:
        def no_source(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            (work / STAGED["market_calendar.py"]).unlink()

        r = self._run(no_source)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("MISSING SOURCE", r.stderr)

    def test_a_symlinked_staged_copy_is_refused(self) -> None:
        """`[ -f ]` and `cmp` both FOLLOW links, so a staged symlink made the comparison true of a
        file somewhere else — and the bytes the container reads could then be changed after this ran
        by swapping the target. Comparing them proved nothing about what the suite sees."""
        name = "market_calendar.py"

        def link_it(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            (target / name).unlink()
            (target / name).symlink_to((work / STAGED[name]).resolve())

        r = self._run(link_it)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("STAGED COPY IS A SYMLINK", r.stderr)

    def test_a_symlinked_source_is_refused(self) -> None:
        """The matching reason: what is committed must be the bytes, not a pointer to them, or the
        comparison is about whatever the pointer currently resolves to."""
        name = "vol-premium-open-reference-capture.py"

        def link_the_source(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            source = work / STAGED[name]
            elsewhere = work / "elsewhere.py"
            shutil.move(source, elsewhere)
            source.symlink_to(elsewhere)

        r = self._run(link_the_source)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("SOURCE IS A SYMLINK", r.stderr)

    def test_a_directory_in_place_of_a_staged_file_is_refused(self) -> None:
        def make_it_a_directory(work: Path, target: Path) -> None:
            self._stage_both(work, target)
            name = "market_calendar.py"
            (target / name).unlink()
            (target / name).mkdir()

        r = self._run(make_it_a_directory)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("NOT STAGED", r.stderr)

    def test_the_deploy_job_runs_it_before_the_container(self) -> None:
        """A control the job does not invoke is not a control. The ORDER matters too: after the
        container has started, the suite is already reading whatever is there."""
        text = (ROOT / JF).read_text()
        call = text.index("bash scripts/ci/verify-archive-unit-staged.sh")
        mount = text.index(MOUNT)
        self.assertLess(call, mount, "the verifier runs after the suite container starts")
        for name, source in STAGED.items():
            self.assertLess(text.index(f"cp {source}"), call,
                            f"{name} is staged after it is verified")


if __name__ == "__main__":
    unittest.main()
