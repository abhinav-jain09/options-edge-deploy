from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

# One Service One Identity Rule: a service has exactly ONE identity, chosen once at creation and never
# version-suffixed. A point-in-time sweep cannot enforce that — it cannot see a service created after
# it runs (oi-shadow was born as "oi-shadow-v1" eleven days after the platform-wide migration and
# survived five weeks). This guard runs on EVERY change, over EVERY manifest, so the next one is caught
# at the change that introduces it. It is wired into deploy-validation.yml; a red result blocks merge.

# Env keys whose VALUE is a runtime identity (Streams application.id / consumer group).
IDENTITY_KEY = re.compile(r"^[A-Z0-9_]*(?:APP_ID|APPLICATION_ID|GROUP_ID|CONSUMER_GROUP)$")

# Deliberately the widest net the rulebook prescribes for identities: a "v" or "r" followed by a digit,
# anywhere, in any case. It catches every form a real migration leaves behind — separator-prefixed
# (-v1, _V2, -r1), CamelCase-embedded (ServiceV2), all-lowercase (servicev2) and compact (v2r4, v2-1).
# A narrower "-v[0-9]" silently misses the rest; that exact gap let a live "…-r2" identity survive a
# sweep that reported clean.
VERSIONED = re.compile(r"[vVrR][0-9]")

# Identity values that are NOT versions despite matching the pattern. Empty by design: every entry is a
# deliberate, reviewed exception, never a way to quiet a real violation.
ALLOWED_IDENTITY_VALUES: frozenset[str] = frozenset()


def _walk(node: object, path: Path, found: list[tuple[Path, str, str]]) -> None:
    """Collect (file, key, value) identity declarations from a PARSED document.

    Parsing rather than line-matching is what makes this form-agnostic: block env entries, inline
    flow mappings ({name: X, value: Y}), ConfigMap data, kustomize patches, quoted/multiline scalars
    and any nesting all arrive here as the same Python structures. A hand-rolled line scanner missed
    the inline flow form that this repository already contains.
    """
    if isinstance(node, dict):
        # Container env entry: {name: SOME_APP_ID, value: some-identity}
        name = node.get("name")
        if isinstance(name, str) and IDENTITY_KEY.match(name):
            value = node.get("value")
            if isinstance(value, (str, int, float)):
                found.append((path, name, str(value)))
            elif "valueFrom" in node:
                # Sourced from a ConfigMap/Secret: the VALUE is declared there and is scanned at its
                # own definition site, so it is not lost — only not duplicated here.
                pass
        # ConfigMap data / any mapping whose KEY is an identity key.
        for key, value in node.items():
            if isinstance(key, str) and IDENTITY_KEY.match(key) and isinstance(value, (str, int, float)):
                found.append((path, key, str(value)))
            _walk(value, path, found)
    elif isinstance(node, list):
        for item in node:
            _walk(item, path, found)


def identities(root: Path) -> list[tuple[Path, str, str]]:
    """Every identity declared in every YAML manifest under `root` (.yaml AND .yml)."""
    found: list[tuple[Path, str, str]] = []
    for path in sorted(p for ext in ("*.yaml", "*.yml") for p in root.rglob(ext)):
        text = path.read_text()
        try:
            documents = list(yaml.safe_load_all(text))
        except yaml.YAMLError:
            # Kustomize/Helm-style templates are not always plain YAML. Never silently skip: a file we
            # cannot parse is a file we cannot vet, so fall back to a line scan of identity assignments.
            for line in text.splitlines():
                match = re.search(r"([A-Z0-9_]*(?:APP_ID|APPLICATION_ID|GROUP_ID|CONSUMER_GROUP))"
                                  r"\s*[:=]\s*\"?([^\"\s,}#]+)", line)
                if match and IDENTITY_KEY.match(match.group(1)):
                    found.append((path, match.group(1), match.group(2)))
            continue
        for document in documents:
            _walk(document, path, found)
    return found


class ServiceIdentityUnversionedTest(unittest.TestCase):
    def test_no_service_identity_is_version_suffixed(self) -> None:
        violations = [
            f"{path.relative_to(ROOT)} {key}={value}"
            for path, key, value in identities(ROOT / "k8s")
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
            f"{path.relative_to(ROOT)} {key}"
            for path, key, _ in identities(ROOT / "k8s")
            if VERSIONED.search(key)
        ]
        self.assertEqual([], violations, "version-suffixed identity env key(s): " + ", ".join(violations))

    def test_scanner_detects_a_violation_in_every_manifest_form(self) -> None:
        # Regression-sensitivity, exercised through the SCANNER (not just the regex): a guard that
        # cannot fail protects nothing. Each fixture is a real manifest shape this repo uses.
        fixtures = {
            "block-env.yaml": (
                "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
                "      containers:\n        - name: svc\n          env:\n"
                "            - name: OI_SHADOW_APP_ID\n              value: oi-shadow-v1\n"
            ),
            "inline-flow.yaml": (
                "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
                "      containers:\n        - name: svc\n          env:\n"
                '            - { name: KAFKA_GEX_STREAMS_APP_ID, value: "gex-streams-v1" }\n'
            ),
            "configmap.yaml": (
                "apiVersion: v1\nkind: ConfigMap\ndata:\n  SOME_GROUP_ID: writer-r2\n"
            ),
            "quoted-camel.yaml": (
                "apiVersion: v1\nkind: ConfigMap\ndata:\n  X_APPLICATION_ID: 'ServiceV2'\n"
            ),
            "yml-extension.yml": (
                "apiVersion: v1\nkind: ConfigMap\ndata:\n  Y_CONSUMER_GROUP: svc-v2r4\n"
            ),
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for filename, body in fixtures.items():
                (root / filename).write_text(body)
            caught = {path.name for path, _, value in identities(root) if VERSIONED.search(value)}
        self.assertEqual(
            set(fixtures), caught,
            "the scanner missed a versioned identity in: " + ", ".join(sorted(set(fixtures) - caught)),
        )

    def test_scanner_does_not_flag_clean_identities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "clean.yaml").write_text(
                "apiVersion: v1\nkind: ConfigMap\ndata:\n"
                "  A_APP_ID: oi-shadow\n"
                "  B_APP_ID: options-edge-databento-strike-flow-classifier\n"
                "  C_APP_ID: options-flow-display-streams-databento\n"
            )
            flagged = [v for _, _, v in identities(root) if VERSIONED.search(v)]
        self.assertEqual([], flagged, f"clean identities were flagged: {flagged}")

    def test_the_scan_actually_sees_the_real_manifests(self) -> None:
        # A scan that silently matched nothing would keep the assertions above green forever.
        found = identities(ROOT / "k8s")
        self.assertGreater(len(found), 20, "identity scan found almost nothing — the parser is broken")
        values = {value for _, _, value in found}
        self.assertIn("oi-shadow", values)
        # The inline flow-mapping form a line scanner missed must be visible to this one.
        self.assertIn("options-databento-gex-streams", values)


if __name__ == "__main__":
    unittest.main()
