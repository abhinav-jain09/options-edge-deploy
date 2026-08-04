#!/usr/bin/env python3
"""Render the checksetup answers file from the template (REQ-5d, REQ-9).

Secrets are read DIRECTLY from the projected secret file whose path is given in argv. They are
never passed through the environment — not even a command-scoped one, which would still be visible
in /proc/<pid>/environ for the lifetime of this process — and never through argv. A path is not a
secret; its contents are.

Fails closed on two things a silent substitution bug would otherwise hide: a value that could break
out of the Perl string quoting, and any placeholder left unsubstituted.
"""
import os
import sys

def read_secret_file(path: str) -> dict:
    """Parse KEY=VALUE lines from the projected secret file. Never logs a value."""
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def main() -> int:
    if len(sys.argv) != 4:
        return fail("usage: render-answers.py <template> <output> <secret-file>")
    tmpl, out, secret_file = sys.argv[1], sys.argv[2], sys.argv[3]

    secrets = read_secret_file(secret_file)
    for required in ("BZ_ADMIN_PASSWORD", "BZ_DB_PASS"):
        if not secrets.get(required):
            return fail(f"{required} missing from the secret file")

    # Non-secret settings still come from the environment: they are deliberately visible, and the
    # deployment mechanism (Compose env, or a k8s ConfigMap) is free to supply them either way.
    subs = {
        "BZ_ADMIN_EMAIL":    os.environ["BZ_ADMIN_EMAIL"],
        "BZ_ADMIN_REALNAME": os.environ["BZ_ADMIN_REALNAME"],
        "BZ_ADMIN_PASSWORD": secrets["BZ_ADMIN_PASSWORD"],
        "BZ_DB_HOST":        os.environ["BZ_DB_HOST"],
        "BZ_DB_PORT":        os.environ["BZ_DB_PORT"],
        "BZ_DB_NAME":        os.environ["BZ_DB_NAME"],
        "BZ_DB_USER":        os.environ["BZ_DB_USER"],
        "BZ_DB_PASS":        secrets["BZ_DB_PASS"],
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
