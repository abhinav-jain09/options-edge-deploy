#!/usr/bin/env python3
"""Assert one rendered es.futures.auction record against the auction desk's record contract.

Used by Jenkinsfile.es-auction-mirror to prove -- on BOTH the source and the mirrored target --
that a record read with `kafka-console-consumer --property print.key=true` satisfies the contract
the SPX Auction Desk depends on. The payload is plain JSON (no Avro schema id to check), so this is
the strict structural check that stands in for a schema assertion.

The mirror job also compares the source and target RENDERED LINES for equality. That comparison is
only as good as the encoding assumptions, so those assumptions are asserted here rather than
assumed: exactly one separator, exactly one line, no carriage return, UTF-8 decodable, a key of the
form `<tradeDate>|<HH:mm>` (a real calendar date, a real minute of day), and a JSON-object value.

The VALUE's field set is deliberately NOT asserted: the desk's payload schema belongs to
es-amt-service and is versioned there. What this file pins is the KEY contract the topic's shape
(1 partition, compact,delete, one record per RTH minute) is built on, and the encoding the
byte-for-byte comparison relies on. Any violation exits non-zero.

Usage: KVSEP='<sep>' assert-es-auction-record.py <rendered-record-file> <label>
"""
import datetime
import json
import os
import re
import sys

KEY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\|(\d{2}):(\d{2})$")


def fail(label, message):
    sys.exit("FAIL [%s]: %s" % (label, message))


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: KVSEP=<sep> %s <rendered-record-file> <label>" % sys.argv[0])
    path, label = sys.argv[1], sys.argv[2]

    sep = os.environ.get("KVSEP")
    if not sep:
        fail(label, "KVSEP is not set; cannot split the rendered record")
    if sep == "|":
        # The key itself carries a bare '|'; a one-character '|' separator could never split it
        # unambiguously. The job uses '#|#'; refuse anything that would make the split ambiguous.
        fail(label, "KVSEP must not be a bare '|': the auction key contains one")

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

    m = KEY_RE.match(key)
    if not m:
        fail(label, "auction key is %r, expected <tradeDate>|<HH:mm> (e.g. 2026-09-08|09:30)" % (key,))
    trade_date, hh, mm = m.group(1), int(m.group(2)), int(m.group(3))
    try:
        datetime.date.fromisoformat(trade_date)
    except ValueError:
        fail(label, "auction key tradeDate %r is not a real calendar date" % (trade_date,))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        fail(label, "auction key minute %02d:%02d is not a real minute of day" % (hh, mm))

    try:
        doc = json.loads(value)
    except ValueError as exc:
        fail(label, "auction value is not valid JSON: %s" % exc)
    if not isinstance(doc, dict):
        fail(label, "auction value is not a JSON object (got %s)" % type(doc).__name__)
    if not doc:
        fail(label, "auction value is an EMPTY JSON object; a minute record must carry a payload")

    print("%s record OK: key=%s tradeDate=%s minute=%02d:%02d fields=%d"
          % (label, key, trade_date, hh, mm, len(doc)))


if __name__ == "__main__":
    main()
