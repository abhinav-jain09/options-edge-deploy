#!/usr/bin/env python3
"""Render the checksetup answers file from the template (REQ-5d, REQ-9).

Kept as its own file rather than inlined in the entrypoint so the two secrets can be passed through
command-scoped environment variables to THIS process only — they never enter Apache's or any CGI's
environment, and they never appear in argv.

Fails closed on two things a silent substitution bug would otherwise hide: a value that could break
out of the Perl string quoting, and any placeholder left unsubstituted.
"""
import os
import sys

def main() -> int:
    if len(sys.argv) != 3:
        return fail("usage: render-answers.py <template> <output>")
    tmpl, out = sys.argv[1], sys.argv[2]

    subs = {
        "BZ_ADMIN_EMAIL":    os.environ["BZ_ADMIN_EMAIL"],
        "BZ_ADMIN_REALNAME": os.environ["BZ_ADMIN_REALNAME"],
        "BZ_ADMIN_PASSWORD": os.environ["PORTAL_ADMIN_PASSWORD"],
        "BZ_DB_HOST":        os.environ["BZ_DB_HOST"],
        "BZ_DB_PORT":        os.environ["BZ_DB_PORT"],
        "BZ_DB_NAME":        os.environ["BZ_DB_NAME"],
        "BZ_DB_USER":        os.environ["BZ_DB_USER"],
        "BZ_DB_PASS":        os.environ["PORTAL_DB_PASS"],
        "BZ_URLBASE":        os.environ["BZ_URLBASE"],
    }

    text = open(tmpl, encoding="utf-8").read()
    for key, value in subs.items():
        # The template wraps every substitution in single quotes, so a value containing a quote,
        # backslash or newline could terminate the Perl string early and corrupt the answers file.
        if any(c in value for c in "'\\\n\r"):
            return fail(f"{key} contains a quote, backslash or newline")
        text = text.replace(f"@@{key}@@", value)

    left = [k for k in subs if f"@@{k}@@" in text]
    if left:
        return fail(f"unsubstituted placeholders remain: {sorted(left)}")

    with open(out, "w", encoding="utf-8") as fh:
        fh.write(text)
    return 0


def fail(msg: str) -> int:
    print(f"FATAL: {msg}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
