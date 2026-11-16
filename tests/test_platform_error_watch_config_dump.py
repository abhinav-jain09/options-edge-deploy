# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The platform error watch must not alert on a Kafka client's startup CONFIG DUMP.

Every Kafka Streams / consumer / producer client prints its configuration at start, one indented `key = value`
row per setting. The exception-HANDLER rows (`processing.exception.handler = class ...LogAndFailProcessingExceptionHandler`)
carry the capital-E word the watcher greps for in their class VALUE, so each restart of a Streams service was reported
to Discord as new "error signatures" - five of them for es-liquidity-wall-service on 2026-11-16, none an error.
The exclude is deliberately narrow (keys naming an exception handler), so a genuine indented diagnostic such as
`message = java.net.ConnectException: ...` still alerts.

The test takes the watcher's REAL default exclude pattern out of the script (the shell parameter expansion is
parsed, not re-typed) and runs it through the same two greps the watcher runs, on lines taken from the real alert
and on lines that must still alert.
"""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "platform-error-watch.sh"

CONFIG_DUMP = [
    "\tproduction.exception.handler = class org.apache.kafka.streams.errors.DefaultProductionExceptionHandler",
    "\tprocessing.exception.handler = class org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler",
    "\tdeserialization.exception.handler = class org.apache.kafka.streams.errors.LogAndFailExceptionHandler",
    "\tdefault.production.exception.handler = class org.apache.kafka.streams.errors.DefaultProductionExceptionHandler",
    "\tdefault.deserialization.exception.handler = class org.apache.kafka.streams.errors.LogAndFailExceptionHandler",
]
STILL_ALERTS = [
    "2026-11-16T09:17:24.621Z ERROR 1 --- [io-18091-exec-9] o.a.c.c.C.[.[.[/].[dispatcherServlet] : Servlet.service() threw exception",
    "java.lang.IllegalStateException: AppSession already registered: app:c7e72344",
    "Caused by: java.lang.NullPointerException",
    "[main] ERROR org.apache.kafka.streams.KafkaStreams - stream-client [x] Encountered the following exception during processing",
    "Exception in thread \"main\" java.lang.RuntimeException: boom",
    "\tat app.Foo.bar(Foo.java:1) ~[na:na] Caused by: java.lang.IllegalStateException: inline",
    "FATAL: out of memory",
    # indented key = value DIAGNOSTICS must survive: only exception-handler config keys are dropped
    "    message = java.net.ConnectException: Connection refused",
    "\tcause = java.lang.IllegalStateException: AppSession already registered",
    # an indented line that is NOT a key = value row must survive
    "    java.lang.IllegalStateException: indented message with no equals sign",
]


def script_defaults() -> tuple[str, str]:
    text = SCRIPT.read_text()
    grep = re.search(r'^GREP_PATTERN="\$\{GREP_PATTERN:-(.*)\}"', text, re.M)
    excl = re.search(r'^EXCLUDE_PATTERN="\$\{EXCLUDE_PATTERN-(.*)\}"', text, re.M)
    assert grep and excl, "the watcher's default GREP_PATTERN / EXCLUDE_PATTERN assignments moved: re-read the script"
    return grep.group(1), excl.group(1)


def watcher_keeps(lines: list[str]) -> list[str]:
    """The watcher's own two greps, in order: GREP_PATTERN keeps, then EXCLUDE_PATTERN drops."""
    g, x = script_defaults()
    data = "\n".join(lines) + "\n"
    r = subprocess.run(["grep", "-aE", g], input=data, capture_output=True, text=True)
    assert r.returncode in (0, 1), r.stderr   # 1 = no match; anything else is a grep error, not an empty result
    kept = r.stdout
    if x:  # the script only applies the exclude when it is non-empty (`[ -n "$EXCLUDE_PATTERN" ]`); `grep -v ""` would drop everything
        r = subprocess.run(["grep", "-avE", x], input=kept, capture_output=True, text=True)
        assert r.returncode in (0, 1), r.stderr
        kept = r.stdout
    return kept.splitlines()


class ConfigDumpIsNotAnError(unittest.TestCase):
    def test_the_alert_lines_are_matched_by_the_grep_first(self):
        # Guards the premise: if the grep ever stops matching them, this test would pass vacuously.
        g, _ = script_defaults()
        got = subprocess.run(["grep", "-aE", g], input="\n".join(CONFIG_DUMP) + "\n", capture_output=True, text=True).stdout
        self.assertEqual(got.splitlines(), CONFIG_DUMP)

    def test_config_dump_rows_are_dropped(self):
        self.assertEqual(watcher_keeps(CONFIG_DUMP), [])

    def test_real_errors_still_alert(self):
        # every line the GREP matches must survive the exclude, indented or not
        g, _ = script_defaults()
        matched = [l for l in STILL_ALERTS if re.search(g, l)]
        self.assertEqual(len(matched), len(STILL_ALERTS), "every sample must be one the watcher would flag")
        self.assertEqual(watcher_keeps(STILL_ALERTS), STILL_ALERTS)

    def test_the_override_still_replaces_the_default(self):
        # `${EXCLUDE_PATTERN-default}` (no colon): an explicitly EMPTY override means keep everything, as before.
        text = SCRIPT.read_text()
        self.assertIn('EXCLUDE_PATTERN="${EXCLUDE_PATTERN-', text)
        self.assertIn('if [ -n "$EXCLUDE_PATTERN" ]', text)


if __name__ == "__main__":
    unittest.main()
