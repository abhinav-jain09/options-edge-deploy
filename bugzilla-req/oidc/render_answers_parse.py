"""The single strict parser for the projected secret file (REQ-9).

One parser, imported by both consumers, so the entrypoint's shell path and the answers renderer can
never disagree about quoting, comments, whitespace or duplicate keys — a disagreement there would
mean one of them silently authenticated with a different value than the other.

Deliberately strict and dumb: KEY=VALUE, one per line. No shell expansion, no quote stripping, no
line continuations. The file is data; treating it as anything richer is how sourcing became a code
execution path in the first place.
"""
from __future__ import annotations


def parse_secret_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError(f"{path}:{lineno}: not a KEY=VALUE line")
            key, value = line.split("=", 1)
            key = key.strip()
            if key in values:
                # A duplicate key is ambiguous — last-wins and first-wins are both defensible, so
                # neither is chosen. This is exactly the class of disagreement the shared parser
                # exists to prevent.
                raise ValueError(f"{path}:{lineno}: duplicate key {key!r}")
            values[key] = value.strip()
    return values
