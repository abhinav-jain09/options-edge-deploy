# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Which session the close chain freezes, and the portability rule that failure taught.

THE INCIDENT (2026-09-01, stockgex-close-board build #33). The morning catch-up cron fired at
01:00 ET and was dead one second later:

    date: invalid option -- 'v'

The wrapper asked for yesterday with `date -v-1d`, the BSD spelling, reasoning that "the agents
are Macs". The agent IS a Mac — but its PATH resolves `date` to GNU coreutils, where `-v` is not
a flag. Nothing was published and nothing was damaged (the guard held), but the ONE run whose job
is to heal a lost night could not run at all, and the evening runs stayed green the whole time,
so the chain looked healthy.

Two tests, therefore: the behaviour (with a movable clock, so it is not asserting the
implementation back to itself), and the CLASS — no shell script in this repo may do date
arithmetic through `date`, in either dialect, because which dialect answers is a property of the
host's PATH and not of anything we control.
"""
import datetime
import pathlib
import re
import shlex
import subprocess
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts" / "ops"))

import et_session  # noqa: E402


def _at(iso):
    return datetime.datetime.fromisoformat(iso).replace(tzinfo=et_session.ET)


class SessionResolution(unittest.TestCase):

    def test_the_evening_run_freezes_today(self):
        # 16:10 ET, the first cron: the close just happened, and it is today's board.
        self.assertEqual(et_session.session_for(_at("2026-09-01T16:10")),
                         datetime.date(2026, 9, 1))

    def test_the_morning_catchup_heals_yesterday(self):
        # 01:00 ET, the second cron. Today has not closed; the night to heal is the one behind.
        self.assertEqual(et_session.session_for(_at("2026-09-01T01:00")),
                         datetime.date(2026, 8, 31))

    def test_the_boundary_is_noon_and_it_is_exclusive(self):
        self.assertEqual(et_session.session_for(_at("2026-09-01T11:59")),
                         datetime.date(2026, 8, 31), "11:59 is still catch-up")
        self.assertEqual(et_session.session_for(_at("2026-09-01T12:00")),
                         datetime.date(2026, 9, 1), "noon is not")

    def test_catchup_walks_back_over_month_and_year_ends(self):
        self.assertEqual(et_session.session_for(_at("2026-09-01T01:00")),
                         datetime.date(2026, 8, 31))
        self.assertEqual(et_session.session_for(_at("2026-01-01T01:00")),
                         datetime.date(2025, 12, 31))

    def test_catchup_on_a_monday_names_the_weekend_and_that_is_correct(self):
        # Monday 01:00 ET names Sunday. The wrapper's calendar check then exits NOT_A_TRADING_DAY,
        # harmlessly — the alternative (guessing Friday here) would silently freeze a session two
        # days old under today's date.
        self.assertEqual(et_session.session_for(_at("2026-08-31T01:00")),
                         datetime.date(2026, 8, 30))

    def test_the_cli_prints_hour_and_session_for_the_shell_to_split(self):
        out = subprocess.run([sys.executable, str(REPO / "scripts/ops/et_session.py"),
                              "--now", "2026-09-01T01:00"],
                             capture_output=True, text=True, check=True).stdout.strip()
        self.assertEqual(out, "01 2026-08-31")
        hour, session = out.split(" ")          # exactly how the wrapper reads it
        self.assertTrue(int(hour) < et_session.CATCHUP_BEFORE_HOUR)
        self.assertRegex(session, r"^\d{4}-\d{2}-\d{2}$")


class NoShellDateArithmetic(unittest.TestCase):
    """`date -v…` (BSD) and `date -d …` (GNU) are both bets on the host's PATH. Neither is allowed.

    Reading the clock is fine — `date +%s`, `date +%F`, `date -u +…` mean the same thing
    everywhere. It is ARITHMETIC and PARSING that diverge, and those are what this forbids.
    """

    # `date -v…` in any unit; `date -d <arg>`; and the -j/-r parsing forms BSD alone has.
    FORBIDDEN = re.compile(r"\bdate\s+(-v|-d\s|-j\b|-r\s)")

    def test_no_script_does_date_arithmetic_through_the_shell(self):
        offenders = []
        for path in sorted((REPO / "scripts").rglob("*.sh")):
            for n, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
                code = line.split("#", 1)[0]
                if self.FORBIDDEN.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{n}: {line.strip()}")
        self.assertEqual(offenders, [], "\n".join(
            ["date arithmetic in shell is not portable across the agents' PATHs — this is the",
             "2026-09-01 catch-up failure. Use python3 (scripts/ops/et_session.py is the example):"]
            + offenders))

    # A `sort` separator must not ride in the option word AS `-t=`. `sort -t=` is one word, and the
    # `sort` on the CI runner parses `-t=VALUE` as assignment syntax: the `=` is eaten as the
    # option/value delimiter and the separator arrives EMPTY --
    #     sort: separator must be exactly one character long: ''
    # That message is NOT one GNU emits (GNU says "empty tab" / "multi-character tab", verified on
    # 9.11, which accepts `-t=` happily), and uutils coreutils 0.2.2 reproduces it byte for byte, so
    # whatever the runner ships behaves the uutils way. Nothing in the workflow selects a sort and
    # `sort --version` was not captured from the runner, so that is as far as the claim goes. On
    # uutils 0.2.2: `-t=` fails, `-t"="` fails too (still one word once the shell drops the quotes),
    # `-t '='` works, `-t=:` silently yields `:`, `-t:` is unaffected -- only a leading `=` is
    # consumed. `-t '='` is right on GNU, BSD and uutils alike, so the rule is about the FORM.
    #
    # Found on 2026-11-08 by wiring tests/test_ensure_indicator_topics_recreate.py into CI: it passes
    # on this Mac and every one of its cases failed on the runner, taking the offset read of
    # scripts/kafka/ensure-indicator-topics.sh down with it.
    #
    # EIGHT VERSIONS OF THIS CHECK, and the sequence is why it is shaped the way it is. A regex over
    # the raw line missed `-t"="` and a backslash continuation. shlex.split missed `(sort ..)`,
    # `$(sort ..)`, backticks and an unspaced `a|sort ..`, because shlex keeps the metacharacter glued
    # to the word. Blanking the metacharacters caught those and then FALSELY flagged `sort` used as
    # DATA. Splitting on `#` truncated a quoted `#` argument, and command position never saw
    # `command sort` / `env .. sort` / `time -p sort`; an enumerated break set missed `()`, which
    # shlex merges into one token, so `f(){ sort -t= -k1; }` slipped by. Skipping keywords without
    # tracking position still missed `if sort -t= ..; then`, `while`, `until`,
    # `function f { ..; }`. Treating every wrapper flag as argument-less mistook the OPERAND for the
    # command, so `env -u HOME sort -t=` walked through -- and `env -u` is a form this repo's own
    # scripts use. Stopping at the first well-formed `-t` accepted `sort -t '=' -t= -k1`, whose LAST
    # -t wins. And `bash -c 'sort -t='` hides the whole command line in one quoted token.
    # Each round closed one more SPELLING of the same call, which is the signal that a mechanism is
    # wrong rather than incomplete. So this is one mechanism, in four steps:
    #
    #   1. LEX the line once, with punctuation_chars, which yields the metacharacters as their own
    #      tokens and respects quoting -- `echo "a|sort -t= -k1"` stays a single data token.
    #   2. CUT into commands at any token made ENTIRELY of punctuation characters (a character class,
    #      not a list -- that is what finally saw `()`), and at a bare `{` or `}`.
    #   3. ROOT each command: drop leading `VAR=v` assignments and the keywords whose next word is a
    #      command; if what remains starts with a WRAPPER, re-root on the first `sort` or shell after
    #      it; if that is a shell, re-scan its `-c` payload as a line of its own.
    #   4. READ EVERY ARGUMENT of a `sort` call, skipping the word after a bare `-t`, and refuse any
    #      argument whose `-t` carries its value in the same word.
    #
    # What it still cannot see, said rather than implied: a separator built at runtime (`sort $FLAG`),
    # the command reached through a variable (`$SORT -t=`), and `sort${IFS}-t=`. No static check
    # reaches those, and this repo's scripts do not use them.

    PUNCTUATION = "();<>|&`"
    LIST_DELIMITER = {"{", "}"}
    # Keywords whose next word is a command. `for`, `case`, `select` and `in` are deliberately NOT
    # here: the word after those is a variable or a data word, so skipping them would flag
    # `for f in sort -t= ...`, where sort never runs.
    KEYWORD = {"if", "while", "until", "then", "else", "elif", "do", "done", "fi", "esac",
               "function", "coproc", "!"}
    # A wrapper exists in order to RUN something, and its own options may consume the next word
    # (`env -u HOME`, `nice -n 5`, `sudo -u root`, `stdbuf -o L`, `exec -a x`). Modelling which
    # flags take a value is a list that goes stale, so the rule is coarser and fail-CLOSED: once a
    # wrapper is the command word, the first `sort` anywhere before the next command break is taken
    # to be the command it runs. The cost is that `env X=1 echo sort -t=` would be flagged though
    # sort is data there; nothing here writes that, and erring toward refusing is the safe direction.
    WRAPPER = {"command", "env", "time", "nice", "nohup", "exec", "builtin", "sudo", "stdbuf",
               "setsid", "timeout", "xargs"}
    # `bash -c '<script>'` hides a whole command line inside one quoted token, and this repo uses it
    # heavily, so the payload is re-scanned rather than treated as data.
    SHELL = {"bash", "sh", "zsh", "dash", "ksh"}
    ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")

    def _is_sort(self, word):
        return word == "sort" or word.endswith("/sort")

    def _shell_script(self, words):
        """The `-c` payload of a shell invocation, or None.

        `-c` is a SHORT option: it is either the whole word, or one letter of a single-dash cluster
        such as `-lc`. A long option merely CONTAINING a c - `--norc`, `--noprofile` - is not it, and
        reading one as `-c` took the word after it as the script and stopped before the real payload,
        so `bash --norc -c 'sort -t='` went unseen (Codex r10). Nor does scanning skip the word after
        a long option, which is what then hid `--norc -c '..'` a second time: `--norc` takes no value
        and the skip swallowed the `-c`. Which long options take a value is not knowable here, so
        every word is simply read until a real `-c` appears - `--rcfile /dev/null -c '..'` works
        because `/dev/null` is just another word on the way.
        """
        for i, word in enumerate(words[1:], 1):
            short = word.startswith("-") and not word.startswith("--")
            if word == "-c" or (short and "c" in word[1:]):
                return words[i + 1] if i + 1 < len(words) else None
        return None

    def _commands(self, line):
        """The line's shell commands, as token lists, cut at the command separators."""
        lexer = shlex.shlex(line, posix=True, punctuation_chars=self.PUNCTUATION)
        lexer.whitespace_split = True
        try:
            tokens = list(lexer)
        except ValueError:
            return []                                        # unbalanced quotes: not ours to judge
        commands, current = [], []
        for tok in tokens:
            if (tok and all(c in self.PUNCTUATION for c in tok)) or tok in self.LIST_DELIMITER:
                commands.append(current)
                current = []
            else:
                current.append(tok)
        commands.append(current)
        return commands

    def _broken_separator(self, arguments):
        """Does this call hand `sort` its separator inside the option word?

        EVERY argument is read, not just up to the first well-formed `-t`: `sort -t '=' -t=` is a
        legal command line whose LAST -t wins, so stopping at the good one accepted the broken one
        (Codex r9). The word after a bare `-t` is that option's value and is skipped.
        """
        skip = False
        for argument in arguments:
            if skip:
                skip = False
                continue
            if argument == "-t":
                skip = True                                  # its value is the next word: correct
                continue
            if argument.startswith("-t") and argument[2:3] == "=":
                return True
        return False

    def _sort_separator_offenders(self, text, depth=0):
        """Lines where a `sort` COMMAND is handed its separator inside the option word."""
        offenders = []
        logical = re.sub(r"\\\n\s*", " ", text)              # join shell line-continuations
        for n, line in enumerate(logical.splitlines(), 1):
            for words in self._commands(line):
                while words and (self.ASSIGNMENT.match(words[0]) or words[0] in self.KEYWORD):
                    words = words[1:]                        # in front of the command, not it
                if not words:
                    continue
                if words[0] in self.WRAPPER:
                    # The wrapper runs something: re-root on the first sort OR shell after it, so
                    # `env -u HOME sort ..` and `env -u HOME bash -c '..'` are both reached.
                    at = next((i for i, w in enumerate(words[1:], 1)
                               if self._is_sort(w) or w in self.SHELL), None)
                    if at is None:
                        continue
                    words = words[at:]
                if words[0] in self.SHELL:
                    if depth < 3:
                        payload = self._shell_script(words)
                        if payload is not None and self._sort_separator_offenders(payload, depth + 1):
                            offenders.append("%s: %s" % (n, line.strip()))
                    continue
                if not self._is_sort(words[0]):
                    continue                                 # `sort` here would be data, not a call
                if self._broken_separator(words[1:]):
                    offenders.append("%s: %s" % (n, line.strip()))
        return offenders

    def test_no_script_passes_sort_its_separator_inside_the_option_word(self):
        offenders = []
        for path in sorted((REPO / "scripts").rglob("*.sh")):
            for hit in self._sort_separator_offenders(path.read_text(errors="replace")):
                offenders.append("%s:%s" % (path.relative_to(REPO), hit))
        self.assertEqual(offenders, [], "\n".join(
            ["`sort -t=` is ONE argv word, and the runner's sort eats the `=` as its option/value",
             "delimiter, leaving an EMPTY separator. Pass it as its own word: `sort -t '='`:"]
            + offenders))

    def test_the_separator_guard_sees_every_executable_form_and_nothing_else(self):
        """The guard's own controls: a static check is worth only the forms it can see."""
        must_refuse = [
            "sort -t= -k1,1n", 'sort -t"=" -k1,1n', "sort -t'=' -k1,1n", "sort -n -t= -k1,1n",
            "cat x | sort -t= -k1", "cat a|sort -t= -k1", "/usr/bin/sort -t= -k1",
            "(sort -t= -k1)", "x=$(sort -t= -k1)", "y=`sort -t= -k1`", "f(){ sort -t= -k1; }",
            "a && sort -t= -k1", "sort -t= -k1 > out", "LC_ALL=C sort -t= -k1",
            "command sort -t= -k1", "env LC_ALL=C sort -t= -k1", "time -p sort -t= -k1",
            "nohup sort -t= -k1", "sort -t= -k1 '#x'", "if sort -t= -k1; then :; fi",
            "env -u HOME sort -t= -k1", "nice -n 5 sort -t= -k1", "sudo -u root sort -t= -k1",
            "stdbuf -o L sort -t= -k1", "exec -a x sort -t= -k1", "command -p sort -t= -k1",
            "timeout 5 sort -t= -k1", "xargs sort -t= -k1", "sort -t '=' -t= -k1",
            "bash -c 'sort -t= -k1'", "bash -lc 'sort -t= -k1'", "sh -c \'cat a | sort -t= -k1\'",
            "env -u HOME bash -c 'sort -t= -k1'", "bash --norc -c 'sort -t= -k1'",
            "bash --noprofile -c 'sort -t= -k1'", "bash --rcfile /dev/null -c 'sort -t= -k1'",
            "bash --norc -lc 'sort -t= -k1'", "bash -O extglob -c 'sort -t= -k1'",
            "while sort -t= -k1; do :; done", "until sort -t= -k1; do :; done",
            "function f { sort -t= -k1; }", "coproc sort -t= -k1", "sort \\\n  -t= -k1,1n",
        ]
        must_accept = [
            "sort -t '=' -k1,1n", 'sort -t "=" -k1,1n', "sort -t: -k1,1n", "sort -n",
            "sort -t $'\\\\t' -k1", "resort -t= -k1", "# sort -t= -k1", "x=$(sort -t '=' -k1)",
            "(sort -t ':' -k1)", "echo sort-t=1", "printf '%s\\n' sort -t=",
            "echo 'use sort -t= never'", "grep -w sort -t= file", 'echo "a|sort -t= -k1"',
            "command sort -t '=' -k1", "for f in sort -t= ; do :; done", "echo done sort -t=",
            "if sort -t '=' -k1; then :; fi", "env -u HOME sort -t '=' -k1",
            "time -p sort -t '=' -k1", "nice -n 5 sort -t: -k1", "sort -t= -k1".replace("-t=", "-t ="),
            "bash -c 'sort -t \'=\' -k1'", "bash -c 'echo sort -t='",
            "bash --norc -c 'sort -t \'=\' -k1'", "sort -t$'\\t' -k1", "bash -c 'echo hi'",
        ]
        for bad in must_refuse:
            self.assertNotEqual([], self._sort_separator_offenders(bad), bad)
        for good in must_accept:
            self.assertEqual([], self._sort_separator_offenders(good), good)

    def test_the_close_board_wrapper_resolves_its_session_through_the_helper(self):
        wrapper = (REPO / "scripts/ops/stock-gex-close-board.sh").read_text()
        self.assertIn("scripts/ops/et_session.py", wrapper,
                      "the wrapper must ask the helper, not the shell, which session it is for")


if __name__ == "__main__":
    unittest.main()
