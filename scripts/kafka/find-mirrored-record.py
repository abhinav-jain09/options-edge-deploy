#!/usr/bin/env python3
"""Find the SOURCE reference record inside a TARGET consumer dump, by BYTES.

Why this is not a shell loop
----------------------------
The obvious `while IFS= read -r line` over the consumer's output cannot prove what the payload proof
claims. Three separate reasons, all of them found in review:

* `read` silently DISCARDS NUL bytes, so a target record with a NUL inserted into it normalises to
  the source line, hashes equal, and is then written back out WITHOUT the NUL — a corrupted record
  reported as byte-identical.
* Hashing the reference file as-is while hashing each candidate with an appended newline means a
  reference that happens to lack its final LF can never match anything, and a healthy mirror fails
  its install after six retries.
* The consumer's output is newline-DELIMITED, which is not the same as record-delimited. A record
  containing a newline silently becomes two output lines, and line scanning would then be comparing
  fragments while believing it compares records.

The first two are fixed by working in bytes with one explicit normalisation. The third cannot be
fixed by looking at the text at all — the boundary information is simply not in it — so it is
PROVED instead: the consumer reports how many records it processed, and if the dump does not split
into exactly that many lines, at least one record contained a newline and this comparison is refused
rather than trusted.

Usage: find-mirrored-record.py <reference-file> <target-dump> <expected-record-count> <out-file>
Exit 0 and write the matching record to <out-file>; exit 1 with a reason on stderr otherwise.
"""
import sys


def one_line(raw: bytes) -> bytes:
    """The reference file's single record, without the ONE trailing LF the shell's redirect gave it.

    Deliberately no CR handling, on either side. Stripping a trailing CR from target lines looked
    like tolerance for line-ending differences and was actually a false positive: a target record
    corrupted from `...}` to `...}\r` would then match an LF-terminated source, and the sanitised
    copy written out for the structural assertion would hide the corruption completely. Review
    caught it, and caught that the "CRLF on both sides" test was blessing exactly that behaviour.

    A genuinely CRLF dump needs no special case: the reference comes out of the SAME consumer
    through the SAME redirect, so it carries the CR too, and the comparison succeeds on its own. A CR
    on one side only is a real difference and is now reported as one.
    """
    if raw.endswith(b"\n"):
        return raw[:-1]
    return raw


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print(f"usage: {argv[0]} <reference-file> <target-dump> <expected-count> <out-file>", file=sys.stderr)
        return 2
    ref_path, dump_path, expected_raw, out_path = argv[1:]

    with open(ref_path, "rb") as handle:
        reference = one_line(handle.read())
    if not reference:
        print(f"FAIL: reference {ref_path} is empty", file=sys.stderr)
        return 1
    if b"\n" in reference:
        print(
            f"FAIL: reference {ref_path} holds {reference.count(b'\n') + 1} lines; it must be exactly one record",
            file=sys.stderr,
        )
        return 1

    with open(dump_path, "rb") as handle:
        dump = handle.read()
    if not dump:
        print(f"FAIL: target dump {dump_path} is empty", file=sys.stderr)
        return 1

    # Split on LF ONLY, and do not touch the pieces. Whatever bytes a target record carries are the
    # bytes it is compared on.
    lines = dump.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()

    try:
        expected = int(expected_raw)
    except ValueError:
        expected = -1
    if expected < 0:
        # This branch changes the MESSAGE, not the outcome: a negative count can never equal a
        # non-negative line count, so the comparison below would refuse anyway. Said plainly because
        # the alternative is a reader believing it is an independent safety check and "tidying" the
        # comparison in the belief that this still covers it.
        print(
            "FAIL: the consumer's processed-record count was not readable, so line boundaries "
            "cannot be shown to be record boundaries",
            file=sys.stderr,
        )
        return 1
    if len(lines) != expected:
        print(
            f"FAIL: the target dump splits into {len(lines)} line(s) but the consumer processed "
            f"{expected} record(s); at least one record contains a newline, so a line is NOT a "
            "record and this comparison would be comparing fragments",
            file=sys.stderr,
        )
        return 1

    for line in lines:
        if line == reference:
            with open(out_path, "wb") as handle:
                handle.write(line)
                handle.write(b"\n")
            print(f"matched: {len(reference)} bytes, byte-identical, among {len(lines)} target record(s)")
            return 0

    print(
        f"FAIL: none of the {len(lines)} target record(s) is byte-identical to the source reference "
        f"({len(reference)} bytes)",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
