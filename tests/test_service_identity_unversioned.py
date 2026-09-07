from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# One Service One Identity Rule: a service has exactly ONE identity, chosen once at creation and never
# version-suffixed. A point-in-time sweep cannot enforce that — a service created after it runs is
# invisible to it (oi-shadow was born as "oi-shadow-v1" eleven days after the platform-wide migration
# and survived five weeks). This guard runs on EVERY change, over EVERY manifest, so the next one is
# caught at the source instead of in a later audit.

# Env keys whose VALUE is a runtime identity (Streams application.id / consumer group).
IDENTITY_KEY = r"[A-Z0-9_]*(?:APP_ID|APPLICATION_ID|GROUP_ID|CONSUMER_GROUP)"

# Deployment env form:   - name: FOO_APP_ID
#                          value: some-identity
DEPLOY_KEY = re.compile(rf"^\s*-?\s*name:\s*({IDENTITY_KEY})\s*$")
DEPLOY_VALUE = re.compile(r"^\s*value:\s*\"?([^\"\s#]+)\"?\s*$")
# ConfigMap form:        FOO_APP_ID: some-identity
CONFIGMAP_ENTRY = re.compile(rf"^\s*({IDENTITY_KEY}):\s*\"?([^\"\s#]+)\"?\s*$")

# Deliberately the widest net the rulebook prescribes for identities: a "v" or "r" followed by a digit,
# anywhere, in any case. It catches every form a real migration leaves behind — separator-prefixed
# (-v1, _V2, -r1), CamelCase-embedded (ServiceV2), all-lowercase (servicev2) and compact (v2r4, v2-1).
# A narrower "-v[0-9]" silently misses the rest; that exact gap let a live "…-r2" identity survive a
# sweep that reported clean.
VERSIONED = re.compile(r"[vVrR][0-9]")

# Identity values that are NOT versions despite matching the pattern. Empty by design: every entry is a
# deliberate, reviewed exception, never a way to quiet a real violation.
ALLOWED_IDENTITY_VALUES: frozenset[str] = frozenset()


def _identities() -> list[tuple[Path, int, str, str]]:
    """(path, line_no, env_key, identity_value) for every identity declared under k8s/."""
    found: list[tuple[Path, int, str, str]] = []
    for path in sorted((ROOT / "k8s").rglob("*.yaml")):
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines):
            entry = CONFIGMAP_ENTRY.match(line)
            if entry:
                found.append((path, i + 1, entry.group(1), entry.group(2)))
                continue
            key = DEPLOY_KEY.match(line)
            if not key:
                continue
            # value: may be the next line, or follow a comment block under the name:
            for j in range(i + 1, min(i + 6, len(lines))):
                value = DEPLOY_VALUE.match(lines[j])
                if value:
                    found.append((path, j + 1, key.group(1), value.group(1)))
                    break
    return found


class ServiceIdentityUnversionedTest(unittest.TestCase):
    def test_no_service_identity_is_version_suffixed(self) -> None:
        violations = [
            f"{path.relative_to(ROOT)}:{line} {key}={value}"
            for path, line, key, value in _identities()
            if VERSIONED.search(value) and value not in ALLOWED_IDENTITY_VALUES
        ]
        self.assertEqual(
            [], violations,
            "One Service One Identity Rule: these identities carry a version suffix. A service is born "
            "unversioned and rotates state in place under the SAME id — it never gets a new one:\n  "
            + "\n  ".join(violations),
        )

    def test_no_identity_env_key_is_version_suffixed(self) -> None:
        # A renamed identity renames its env keys too — the key itself may not carry a version either.
        violations = [
            f"{path.relative_to(ROOT)}:{line} {key}"
            for path, line, key, _ in _identities()
            if VERSIONED.search(key)
        ]
        self.assertEqual([], violations, "version-suffixed identity env key(s): " + ", ".join(violations))

    def test_guard_actually_detects_a_versioned_identity(self) -> None:
        # Regression-sensitivity: a guard that cannot fail protects nothing. Every form the rulebook
        # names must trip the pattern, and a clean identity must not.
        for bad in ("oi-shadow-v1", "opb-service-v2r4", "delta-flow-r2", "ServiceV2", "servicev2", "hpsf-v2-1"):
            self.assertRegex(bad, VERSIONED, f"{bad} must be detected as versioned")
        for good in ("oi-shadow", "options-edge-databento-strike-flow-classifier", "options-flow-display-streams-databento"):
            self.assertNotRegex(good, VERSIONED, f"{good} must NOT be flagged")

    def test_the_scan_actually_sees_the_manifests(self) -> None:
        # A scan that silently matches nothing would pass the assertions above forever.
        identities = _identities()
        self.assertGreater(len(identities), 20, "identity scan found almost nothing — parser is broken")
        self.assertIn("oi-shadow", {value for _, _, _, value in identities})


if __name__ == "__main__":
    unittest.main()
