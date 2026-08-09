#!/usr/bin/env python3
"""Fail-closed structural guard: prove a cloudflared ingress config cannot serve a retired domain.

Used by infra/prod/cloudflared/README.md on both paths that can put bytes in front of the prod
tunnel — installing a new config, and restoring a backup. It is deliberately ONE implementation so a
future fix cannot land on only one of them.

WHY NOT grep. A line-oriented scan is not a YAML parser. Each spelling below is valid YAML declaring
the retired hostname; give any of them a `path:` and it serves that path from a backend while `/`
still answers 404 — so neither a regex on `hostname:` nor a finite set of cloudflared URL probes can
be trusted:

    - {hostname: fullfunding.nl, path: /secret, service: http://evil:8080}   # flow mapping
    - "hostname": fullfunding.nl                                             # quoted key
      hostname : fullfunding.nl                                              # spacing before colon
      hostname: !!str fullfunding.nl                                         # tagged scalar
      hostname: &r fullfunding.nl   …   hostname: *r                         # anchor / alias
      hostname: "*"                                                          # wildcard catch-all

`cloudflared tunnel ingress validate` accepts all of them. This asks PyYAML instead, which is the
same approach scripts/ops/verify-prod-tunnel.sh takes; its scan_config_for_retired()
delegates here so the rule has exactly one implementation.

Parsing is necessary but not sufficient: the value must also be the TYPE we think it is. YAML tags can
hand back a non-string that a naive str() silently mangles into something harmless-looking —

    - hostname: !!binary ZnVsbGZ1bmRpbmcubmw=      # bytes b'fullfunding.nl'
      path: /secret
      service: http://evil:8080

str() renders that as "b'fullfunding.nl'", which matches no suffix, while cloudflared resolves
/secret to the backend and / to 404. So a present-but-non-string hostname is a hard error (exit 2),
never coerced. Duplicate mapping keys are rejected for the same reason: safe_load would silently keep
the last one.

Exit codes (a check that cannot run is NOT a pass):
    0  clean
    1  violation — a retired/wildcard hostname is declared, or a hostless rule is not the
       terminal http_status:404
    2  could not run: unreadable file, unparseable YAML, unexpected ingress shape

Usage:
    assert-no-retired-hostname.py <config.yml|-> [retired-suffix ...]   # default: fullfunding.nl
                                                                        # "-" reads stdin
"""
import re
import sys

try:
    import yaml
except ImportError:
    print("FATAL: PyYAML not available — cannot structurally verify the config", file=sys.stderr)
    sys.exit(2)

DEFAULT_SUFFIXES = ["fullfunding.nl"]

# A dotted hostname: labels of [a-z0-9-] not starting/ending in "-", at least two of them. Excludes
# leading dots, wildcards, slashes, whitespace and empty labels — every form that would match
# nothing and therefore quietly neuter the guard.
LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"     # DNS label: 1-63 chars, no leading/trailing "-"
SUFFIX_RE = re.compile(rf"{LABEL}(?:\.{LABEL})+")
MAX_SUFFIX_LEN = 253                                 # DNS name limit


class StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate mapping keys instead of silently keeping the last."""


MERGE_TAG = "tag:yaml.org,2002:merge"


def _no_duplicates(loader, node, deep=False):
    # Check EXPLICIT keys before flattening. `<<: *anchor` is legal YAML that cloudflared accepts,
    # and merge semantics let an explicit key override a merged one — so a merged key colliding with
    # an explicit one is correct, not a duplicate. Only a key written twice in the same mapping is.
    seen, merges = set(), 0
    for key_node, _ in node.value:
        if key_node.tag == MERGE_TAG:
            # One `<<` per mapping. A second is itself a duplicate key, and silently dropping it
            # would contradict the fail-closed contract even though cloudflared also rejects it.
            merges += 1
            if merges > 1:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping", node.start_mark,
                    "duplicate merge key '<<'", key_node.start_mark)
            continue
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping", node.start_mark,
                f"duplicate key {key!r}", key_node.start_mark)
        seen.add(key)
    loader.flatten_mapping(node)          # now expand `<<` the way PyYAML normally would
    return yaml.constructor.SafeConstructor.construct_mapping(loader, node, deep=deep)


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicates)


def main(argv):
    if len(argv) < 2:
        print("usage: assert-no-retired-hostname.py <config.yml|-> [retired-suffix ...]",
              file=sys.stderr)
        return 2
    path = argv[1]
    if argv[2:]:
        # A malformed suffix silently disables the guard rather than failing: ".fullfunding.nl"
        # would make the matcher look for "..fullfunding.nl" and pass a genuinely retired config.
        # Only accept a real dotted hostname.
        # A single trailing dot is the DNS-absolute form; more than one is malformed, so strip
        # exactly one rather than collapsing "fullfunding.nl..." into something valid-looking.
        suffixes = [re.sub(r"\.$", "", s.strip().lower()) for s in argv[2:]]
        for suffix in suffixes:
            if not SUFFIX_RE.fullmatch(suffix) or len(suffix) > MAX_SUFFIX_LEN:
                print(f"FATAL: {suffix!r} is not a valid retired-domain suffix — refusing to run "
                      f"with a suffix that would match nothing", file=sys.stderr)
                return 2
    else:
        suffixes = DEFAULT_SUFFIXES

    try:
        if path == "-":
            doc = yaml.load(sys.stdin, Loader=StrictLoader)
            path = "<stdin>"
        else:
            with open(path) as fh:
                doc = yaml.load(fh, Loader=StrictLoader)
    except Exception as exc:                       # unreadable, not YAML, or duplicate keys
        print(f"FATAL: could not parse {path}: {exc}", file=sys.stderr)
        return 2

    if not isinstance(doc, dict):
        print(f"FATAL: {path} is not a YAML mapping", file=sys.stderr)
        return 2
    rules = doc.get("ingress")
    if not isinstance(rules, list) or not rules:
        print(f"FATAL: {path} has no usable 'ingress' list", file=sys.stderr)
        return 2
    if any(not isinstance(r, dict) for r in rules):
        print(f"FATAL: {path} has an ingress entry that is not a mapping", file=sys.stderr)
        return 2

    # Types first. A non-string hostname or service is never coerced — see the !!binary note above.
    for idx, rule in enumerate(rules):
        for field in ("hostname", "service"):
            if field in rule and not isinstance(rule[field], str):
                print(f"FATAL: {path} ingress[{idx}] has a non-string {field} "
                      f"({type(rule[field]).__name__}: {rule[field]!r}) — refusing to coerce it",
                      file=sys.stderr)
                return 2

    # Hostnames, normalised the way cloudflared matches them.
    hosts = [rule.get("hostname", "").strip().lower().rstrip(".") for rule in rules]

    # A wildcard matches the retired domain regardless of spelling, so it is never acceptable here.
    wild = sorted({h for h in hosts if "*" in h})
    if wild:
        print(f"VIOLATION: {path} declares wildcard hostname(s): {', '.join(wild)}")
        return 1

    # Exact match or a true label boundary — `notfullfunding.nl` must NOT trip this.
    hits = sorted({h for h in hosts if h
                   for s in suffixes if h == s or h.endswith("." + s)})
    if hits:
        print(f"VIOLATION: {path} declares retired hostname(s): {', '.join(hits)}")
        return 1

    # A hostless rule matches EVERY hostname, so the only acceptable one is the terminal 404 —
    # and "terminal" is positional: a hostless 404 placed earlier shadows every rule after it.
    last = len(rules) - 1
    offenders = [(i, r) for i, (r, h) in enumerate(zip(rules, hosts))
                 if not h and (r.get("service", "").strip() != "http_status:404" or i != last)]
    if offenders:
        print(f"VIOLATION: {path} has hostless rule(s) that are not the terminal "
              f"http_status:404: {offenders}")
        return 1

    print(f"CLEAN: {path} declares no retired or wildcard hostname "
          f"(hosts: {', '.join(sorted({h for h in hosts if h}))})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
