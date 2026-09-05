#!/usr/bin/env python3
"""Assert one rendered es.tape-zones.board record against the board's payload contract.

Used by Jenkinsfile.es-tape-zones-mirror to prove -- on BOTH the source and the mirrored target --
that a record read with `kafka-console-consumer --property print.key=true` satisfies the contract
the /zones page depends on. The payload is plain JSON (no Avro schema id to check), so this is the
strict structural check that stands in for a schema assertion.

The mirror job also compares the source and target RENDERED LINES for equality. That comparison is
only as good as the encoding assumptions, so those assumptions are asserted here rather than
assumed: exactly one separator, exactly one line, no carriage return, UTF-8 decodable, and a key
that is derivable from the value (`ES|<sessionDate>`). Any violation exits non-zero.

Usage: KVSEP='<sep>' assert-tape-zones-board-record.py <rendered-record-file> <label>
"""
import datetime
import json
import os
import re
import sys

SESSION_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fail(label, message):
    sys.exit("FAIL [%s]: %s" % (label, message))


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: KVSEP=<sep> %s <rendered-record-file> <label>" % sys.argv[0])
    path, label = sys.argv[1], sys.argv[2]

    sep = os.environ.get("KVSEP")
    if not sep:
        fail(label, "KVSEP is not set; cannot split the rendered record")

    # Read as bytes and decode explicitly: a decode error is a contract violation, not a crash to
    # be swallowed, and it is exactly what would make the source/target line comparison meaningless.
    with open(path, "rb") as handle:
        raw = handle.read()
    if not raw:
        fail(label, "rendered record file %s is empty" % path)
    if b"\r" in raw:
        fail(label, "rendered record contains a carriage return; the contract is one LF-terminated line")
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if b"\n" in raw:
        fail(label, "rendered record spans multiple lines; the contract is one single-line JSON object per record")
    try:
        line = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        fail(label, "rendered record is not valid UTF-8: %s" % exc)

    if line.count(sep) != 1:
        fail(label, "rendered record contains %d occurrences of the key separator %r; expected exactly 1"
                    % (line.count(sep), sep))
    key, value = line.split(sep, 1)

    try:
        doc = json.loads(value)
    except ValueError as exc:
        fail(label, "board value is not valid JSON: %s" % exc)
    if not isinstance(doc, dict):
        fail(label, "board value is not a JSON object (got %s)" % type(doc).__name__)

    session_date = doc.get("sessionDate")
    if not isinstance(session_date, str) or not SESSION_DATE_RE.match(session_date):
        fail(label, "board record has no `sessionDate` of the form YYYY-MM-DD (got %r)" % (session_date,))
    try:
        datetime.date.fromisoformat(session_date)
    except ValueError:
        fail(label, "board record `sessionDate` %r is not a real calendar date" % (session_date,))

    terminal_flushed = doc.get("terminalFlushed")
    if not isinstance(terminal_flushed, bool):
        fail(label, "board record has no boolean `terminalFlushed` (got %r)" % (terminal_flushed,))

    # The board is keyed `ES|<sessionDate>`. Asserting the key is derived from the value is what
    # makes the key half of the source/target line comparison meaningful instead of decorative.
    expected_key = "ES|%s" % session_date
    if key != expected_key:
        fail(label, "board key is %r, expected %r (key must be ES|<sessionDate>)" % (key, expected_key))

    print("%s record OK: key=%s sessionDate=%s terminalFlushed=%s engineVersion=%s schemaVersion=%s"
          % (label, key, session_date, terminal_flushed,
             doc.get("engineVersion"), doc.get("schemaVersion")))


if __name__ == "__main__":
    main()
