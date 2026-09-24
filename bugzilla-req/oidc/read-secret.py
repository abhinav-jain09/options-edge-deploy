#!/usr/bin/env python3
"""Read one key from the projected secret file (REQ-9).

Exists so the entrypoint never has to `source` the secret file. Sourcing would EXECUTE it: a
projected Secret is data, not trusted shell code, and a stray `$(...)` in a value would run before
any validation could reject it. This also guarantees the shell and render-answers.py agree on
quoting, comments, whitespace and duplicate keys, because both use this same parse.

Prints the value on stdout and nothing else. Exits non-zero if the key is absent, so the caller's
`|| exit 1` is meaningful.
"""
import sys

from render_answers_parse import parse_secret_file


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: read-secret.py <secret-file> <key>", file=sys.stderr)
        return 2
    path, key = sys.argv[1], sys.argv[2]
    try:
        secrets = parse_secret_file(path)
    except OSError as exc:
        print(f"FATAL: cannot read {path}: {exc}", file=sys.stderr)
        return 1
    if key not in secrets:
        print(f"FATAL: {key} not present in {path}", file=sys.stderr)
        return 1
    sys.stdout.write(secrets[key])
    return 0


if __name__ == "__main__":
    sys.exit(main())
