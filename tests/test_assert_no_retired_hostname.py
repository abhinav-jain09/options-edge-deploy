"""Regression lock for the retired-hostname guard used by the prod Cloudflare tunnel runbook.

`fullfunding.nl` moved to its own tunnel on 2026-08-09. If it is ever declared again in
infra/prod/cloudflared/options-edge-stable.yml, this tunnel starts answering for a domain another
application now owns. scripts/ops/assert-no-retired-hostname.py is the gate that prevents that, on
both paths that can put bytes in front of the tunnel (install and rollback) and inside
scripts/ops/verify-prod-tunnel.sh.

The guard is EXECUTED here against real YAML, not grepped for reassuring strings.

Five fixtures are the dangerous class, confirmed against cloudflared 2026.6.0 on the prod host:
`flow_mapping`, `quoted_key`, `space_before_colon`, `tagged_str` and `anchor_alias` each validate
cleanly and route fullfunding.nl/secret to a backend **while `/` still answers http_status:404** — so
neither `cloudflared ingress validate` nor a finite set of URL probes can catch them. The remaining
hostname-wide fixtures (`uppercase`, `trailing_dot`, `subdomain`, the wildcards) are not path-scoped;
they simply declare the name in a spelling a text scan would miss. The hostless cases are the
exception cloudflared does reject, since a hostless rule shadows every rule after it — locked here
anyway, because this guard must not depend on cloudflared having run first.

Two earlier implementations of this check (a shell `grep`, then a str()-coercing PyYAML block)
shipped holes that these cases catch."""
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GUARD = REPO / "scripts" / "ops" / "assert-no-retired-hostname.py"
CANONICAL = REPO / "infra" / "prod" / "cloudflared" / "options-edge-stable.yml"

CLEAN, VIOLATION, CANNOT_RUN = 0, 1, 2

HEAD = """\
tunnel: options-edge-option-chain
credentials-file: /etc/cloudflared/options-edge-option-chain.json

ingress:
  - hostname: bleadingoptions.com
    service: http://192.168.100.252:8094
"""
TERMINAL = "  - service: http_status:404\n"


def run(config_text=None, *args, path=None):
    argv = [sys.executable, str(GUARD), str(path) if path else "-", *args]
    return subprocess.run(argv, input=config_text, capture_output=True, text=True)


def cfg(rule_block):
    return HEAD + rule_block + TERMINAL


# Shapes that declare a retired or wildcard hostname. The path-scoped ones are the dangerous class
# (see the module docstring); the rest simply declare the name outright, in a spelling a text scan
# would miss. All must be exit 1.
BYPASS_ATTEMPTS = {
    "flow_mapping": "  - {hostname: fullfunding.nl, path: /secret, service: 'http://evil:8080'}\n",
    "quoted_key": '  - "hostname": "fullfunding.nl"\n    path: /secret\n    service: http://evil:8080\n',
    "space_before_colon": "  - hostname : fullfunding.nl\n    path: /secret\n    service: http://evil:8080\n",
    "tagged_str": "  - hostname: !!str fullfunding.nl\n    path: /secret\n    service: http://evil:8080\n",
    "anchor_alias": ("  - hostname: &r fullfunding.nl\n    path: /secret\n    service: http://evil:8080\n"
                     "  - hostname: *r\n    path: /other\n    service: http://evil:8081\n"),
    "uppercase": "  - hostname: FullFunding.NL\n    service: http://evil:8080\n",
    "trailing_dot": "  - hostname: fullfunding.nl.\n    service: http://evil:8080\n",
    "subdomain": "  - hostname: es.fullfunding.nl\n    service: http://evil:8080\n",
    "wildcard_all": '  - hostname: "*"\n    service: http://evil:8080\n',
    "wildcard_tld": '  - hostname: "*.nl"\n    service: http://evil:8080\n',
    "hostless_not_404": "  - service: http://evil:8080\n",
}

# A hostname is not a string here, so any str() coercion renders it harmlessly (b'fullfunding.nl').
# These must FAIL CLOSED rather than be judged, because we cannot know what cloudflared will match.
UNCOERCIBLE = {
    "binary_hostname": "  - hostname: !!binary ZnVsbGZ1bmRpbmcubmw=\n    path: /secret\n    service: http://evil:8080\n",
    "binary_service": "  - hostname: x.bleadingoptions.com\n    service: !!binary ZnVsbGZ1bmRpbmcubmw=\n",
    "int_hostname": "  - hostname: 12345\n    service: http://evil:8080\n",
    "duplicate_keys": "  - hostname: bleadingoptions.com\n    hostname: fullfunding.nl\n    service: http://evil:8080\n",
}


@pytest.mark.parametrize("name", sorted(BYPASS_ATTEMPTS))
def test_declared_retired_hostname_is_a_violation(name):
    res = run(cfg(BYPASS_ATTEMPTS[name]))
    assert res.returncode == VIOLATION, f"{name} was not rejected: {res.stdout}{res.stderr}"


@pytest.mark.parametrize("name", sorted(UNCOERCIBLE))
def test_uncoercible_values_fail_closed(name):
    res = run(cfg(UNCOERCIBLE[name]))
    assert res.returncode == CANNOT_RUN, f"{name} did not fail closed: {res.stdout}{res.stderr}"


@pytest.mark.parametrize("rule_block", [
    # A label boundary is required — these merely CONTAIN the retired name.
    "  - hostname: notfullfunding.nl\n    service: http://127.0.0.1:9\n",
    "  - hostname: fullfunding.nl.example.com\n    service: http://127.0.0.1:9\n",
    # `<<:` is legal YAML that cloudflared accepts; rejecting it would block real rollbacks.
    ("  - &d\n    hostname: x.bleadingoptions.com\n    service: http://127.0.0.1:9\n"
     "  - <<: *d\n    hostname: y.bleadingoptions.com\n"),
])
def test_legitimate_configs_pass(rule_block):
    res = run(cfg(rule_block))
    assert res.returncode == CLEAN, f"false positive: {res.stdout}{res.stderr}"


def test_the_shipped_config_is_clean():
    """The file this whole directory exists to protect must pass its own gate."""
    res = run(path=CANONICAL)
    assert res.returncode == CLEAN, f"{CANONICAL} is not clean: {res.stdout}{res.stderr}"


@pytest.mark.parametrize("args", [("",), (".",), (".fullfunding.nl",), ("*.nl",),
                                  ("foo bar",), ("nl",), ("-bad.nl",)])
def test_malformed_suffix_cannot_silently_disable_the_guard(args):
    res = run(cfg(BYPASS_ATTEMPTS["flow_mapping"]), *args)
    assert res.returncode == CANNOT_RUN, f"guard was disabled by {args!r}: {res.stdout}{res.stderr}"


@pytest.mark.parametrize("suffix,expected,claim", [
    ("fullfunding.nl", VIOLATION, None),        # plain
    ("fullfunding.nl.", VIOLATION, None),       # one trailing dot is the DNS-absolute form
    ("fullfunding.nl..", CANNOT_RUN, None),     # two is malformed, not "normalise until it works"
    ("a" * 63 + ".nl", CLEAN, ("label", 63)),           # max legal label
    ("a" * 64 + ".nl", CANNOT_RUN, ("label", 64)),      # one over
    (("a" * 49 + ".") * 5 + "aaa", CLEAN, ("total", 253)),        # the DNS limit exactly
    (("a" * 49 + ".") * 5 + "aaaa", CANNOT_RUN, ("total", 254)),  # one over
])
def test_suffix_grammar_boundaries(suffix, expected, claim):
    # Assert the claimed size unconditionally: a boundary fixture that drifts off the boundary would
    # otherwise keep passing while testing nothing.
    if claim:
        kind, want = claim
        got = len(suffix.split(".")[0]) if kind == "label" else len(suffix)
        assert got == want, f"fixture claims {kind}={want} but is {got}"
    assert run(cfg(BYPASS_ATTEMPTS["flow_mapping"]), suffix).returncode == expected


def test_a_custom_suffix_is_actually_applied():
    """The suffix argument must select what is checked, not just be validated and ignored."""
    rule = "  - hostname: legacy.example.org\n    service: http://x:1\n"
    assert run(cfg(rule)).returncode == CLEAN                      # not retired by default
    assert run(cfg(rule), "example.org").returncode == VIOLATION   # retired when asked


def test_second_merge_key_in_one_mapping_fails_closed():
    """One `<<` is legal; a second is a duplicate key that safe_load would silently drop."""
    rule = ("  - &a\n    hostname: a.keep.example\n    service: http://x:1\n"
            "  - &b\n    hostname: b.keep.example\n    service: http://x:2\n"
            "  - <<: *a\n    <<: *b\n    hostname: c.keep.example\n")
    assert run(cfg(rule)).returncode == CANNOT_RUN


def test_a_hostless_404_must_be_last():
    """`terminal` is positional — an early hostless 404 shadows every rule after it."""
    early = "  - service: http_status:404\n  - hostname: keep.example\n    service: http://x:1\n"
    assert run(HEAD + early).returncode == VIOLATION


@pytest.mark.parametrize("text", ["", "just: [a, b\n", "- not: a mapping\n", "ingress: {}\n"])
def test_unusable_input_fails_closed(text):
    assert run(text).returncode == CANNOT_RUN


def test_no_arguments_prints_a_usable_usage_line():
    """It used to print `__doc__`'s last line, which was a continuation comment, not the usage."""
    res = subprocess.run([sys.executable, str(GUARD)], capture_output=True, text=True)
    assert res.returncode == CANNOT_RUN
    assert res.stderr.startswith("usage: assert-no-retired-hostname.py"), res.stderr


def test_missing_file_fails_closed():
    assert run(path=REPO / "does-not-exist.yml").returncode == CANNOT_RUN


VERIFY = REPO / "scripts" / "ops" / "verify-prod-tunnel.sh"


def test_verifier_selftest_passes():
    """Executed, not grepped. A string-presence assertion passed while the delegation it claimed to
    prove had broken three of the verifier's own end-to-end scanner cases."""
    res = subprocess.run(["bash", str(VERIFY), "--selftest"], capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr


def test_verifier_rejects_a_helper_that_exits_zero_without_a_verdict(tmp_path):
    """exit 0 alone is not proof the check ran — an empty or truncated helper also exits 0. The scan
    must demand the CLEAN: marker, or a stubbed-out guard would silently green-light anything."""
    stub = tmp_path / "stub.py"
    stub.write_text("")                      # exits 0, prints nothing
    poisoned = cfg(BYPASS_ATTEMPTS["subdomain"])
    script = (
        f'RETIRED_GUARD={shlex.quote(str(stub))}\n'
        'fail=0\n'
        'retired_suffixes() { echo fullfunding.nl; }\n'
        'note() { echo "  $*"; }\n'
        'bad() { echo "  FAIL: $*"; fail=1; }\n'
        f'source <(sed -n "/^scan_config_for_retired() {{/,/^}}/p" {shlex.quote(str(VERIFY))})\n'
        'scan_config_for_retired "fixture" "$(cat)"\n'
        'exit $fail\n'
    )
    res = subprocess.run(["bash", "-c", script], input=poisoned, capture_output=True, text=True)
    assert res.returncode == 1, f"stub helper accepted as clean: {res.stdout}{res.stderr}"


def test_verifier_scan_rejects_the_binary_bypass_end_to_end():
    """The whole point of the delegation: the verifier's own structural scan must fail on a config
    the shared guard cannot judge. Its previous inline str()-coercing block passed this exact file,
    which cloudflared routes at /secret while / answers 404."""
    poisoned = cfg(UNCOERCIBLE["binary_hostname"])
    script = (
        f'REPO_ROOT={shlex.quote(str(REPO))}\n'
        f'RETIRED_GUARD="$REPO_ROOT/scripts/ops/assert-no-retired-hostname.py"\n'
        'fail=0\n'                     # the verifier's real accumulator, written by bad()
        'retired_suffixes() { echo fullfunding.nl; }\n'
        'note() { echo "  $*"; }\n'
        'bad() { echo "  FAIL: $*"; fail=1; }\n'
        # pull in just the function under test, then run it on the poisoned config
        f'source <(sed -n "/^scan_config_for_retired() {{/,/^}}/p" {shlex.quote(str(VERIFY))})\n'
        'scan_config_for_retired "fixture" "$(cat)"\n'
        'exit $fail\n'
    )
    res = subprocess.run(["bash", "-c", script], input=poisoned, capture_output=True, text=True)
    combined = res.stdout + res.stderr
    assert res.returncode == 1, f"verifier scan accepted the !!binary bypass: {combined}"
    assert "non-string hostname" in combined, combined
