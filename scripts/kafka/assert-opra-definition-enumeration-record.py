#!/usr/bin/env python3
"""Assert one rendered options.databento.opra.definition.enumeration record against its contract.

Used by Jenkinsfile.opra-definition-enumeration-mirror to prove -- on BOTH the source (prod) and the
mirrored target (es4) -- that a record read with `kafka-console-consumer --property print.key=true`
satisfies the contract the SPX Auction Desk depends on. The payload is plain JSON (no Avro schema id
to check), so this is the strict structural check that stands in for a schema assertion.

The mirror job also compares the source and target RENDERED LINES for equality. That comparison is
only as good as the encoding assumptions, so those assumptions are asserted here rather than assumed:
exactly one separator, exactly one line, no carriage return, UTF-8 decodable, a key of the form
`SPX|SPXW|<YYYYMMDD>` (a real calendar date), and a JSON-object value.

WHAT IS PINNED, AND WHY THAT AND NOT MORE. The record's full field set belongs to
options-edge-databento-feed and is versioned there (`definition_enumeration.py`), so asserting it
here would duplicate a contract that moves independently and break this job on a legitimate producer
change. What IS pinned is what the TRANSPORT depends on, and what a mirror could plausibly get wrong:

  * the KEY shape, because it is what the topic's 1-partition ordering contract is keyed on;
  * `status`, because the whole point of this topic is that a COMPLETE and its UNAVAILABLE retraction
    are DIFFERENT facts -- a mirror that carried one as the other is the failure this job exists to
    catch, and a record with neither value is not a record this desk can read at all;
  * `root` and `settlementStyle`, because the owner's constraint is SPXW PM only and a record that
    said otherwise must never reach the desk, whatever the producer thinks it published;
  * `targetExpiryDate` agreeing with the KEY, because a mirror that paired one record's key with
    another's value would otherwise pass every other check here.

Any violation exits non-zero.

Usage: KVSEP='<sep>' assert-opra-definition-enumeration-record.py <rendered-record-file> <label>
"""
import datetime
import json
import os
import re
import sys

KEY_RE = re.compile(r"^SPX\|SPXW\|(\d{8})$")
VALID_STATUS = ("COMPLETE", "UNAVAILABLE")


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
        # The key itself carries two bare '|'s; a one-character '|' separator could never split it
        # unambiguously. The job uses '#|#'; refuse anything that would make the split ambiguous.
        fail(label, "KVSEP must not be a bare '|': the enumeration key contains two")

    # Read as bytes and decode explicitly: a decode error is a contract violation, not a crash to be
    # swallowed, and it is exactly what would make the source/target line comparison meaningless.
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
        fail(label, "enumeration key is %r, expected SPX|SPXW|<YYYYMMDD> (e.g. SPX|SPXW|20260909)" % (key,))
    compact = m.group(1)
    try:
        expiry = datetime.datetime.strptime(compact, "%Y%m%d").date()
    except ValueError:
        fail(label, "enumeration key expiry %r is not a real calendar date" % (compact,))

    try:
        doc = json.loads(value)
    except ValueError as exc:
        fail(label, "enumeration value is not valid JSON: %s" % exc)
    if not isinstance(doc, dict):
        fail(label, "enumeration value is not a JSON object (got %s)" % type(doc).__name__)
    if not doc:
        fail(label, "enumeration value is an EMPTY JSON object; a record must carry a payload")

    status = doc.get("status")
    if status not in VALID_STATUS:
        fail(label, "status is %r; the desk reads only %s -- a record with neither is unreadable, and a "
                    "COMPLETE carried as an UNAVAILABLE (or the reverse) is the exact mirror failure this "
                    "check exists for" % (status, " or ".join(VALID_STATUS)))

    # The owner's constraint, asserted on the wire rather than trusted from the producer.
    for field, want in (("root", "SPXW"), ("settlementStyle", "PM")):
        got = doc.get(field)
        if got != want:
            fail(label, "%s is %r, expected %r -- this desk trades PM-settled SPXW only" % (field, got, want))

    # The KEY and the VALUE must name the same expiry, or a mirror that paired one record's key with
    # another's value would pass everything above.
    target = doc.get("targetExpiryDate")
    if not isinstance(target, str):
        fail(label, "targetExpiryDate is %r, expected a date string" % (target,))
    try:
        target_date = datetime.date.fromisoformat(target)
    except ValueError:
        fail(label, "targetExpiryDate %r is not an ISO calendar date" % (target,))
    if target_date != expiry:
        fail(label, "the key names expiry %s but the value says %s -- key and value are from different records"
                    % (expiry.isoformat(), target_date.isoformat()))

    print("%s record OK: key=%s expiry=%s status=%s root=%s settlement=%s fields=%d"
          % (label, key, expiry.isoformat(), status, doc.get("root"), doc.get("settlementStyle"), len(doc)))


if __name__ == "__main__":
    main()
