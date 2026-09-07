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


def _walk(node: object, path: Path, found: list[tuple[Path, str, str]],
          indirect: list[tuple[Path, str, str]]) -> None:
    """Collect (file, key, value) identity declarations from a PARSED document.

    Parsing rather than line-matching is what makes this form-agnostic: block env entries, inline
    flow mappings ({name: X, value: Y}), ConfigMap data, kustomize patches, quoted/multiline scalars
    and any nesting all arrive here as the same Python structures. A hand-rolled line scanner missed
    the inline flow form that this repository already contains.

    An identity that is NOT a literal (valueFrom: configMapKeyRef / secretKeyRef / fieldRef) is
    recorded in `indirect` instead: it cannot be vetted here, and silently ignoring it is a fail-open
    hole. The caller resolves repo-local ConfigMap references and FAILS on anything left unresolved.
    """
    if isinstance(node, dict):
        # Container env entry: {name: SOME_APP_ID, value: some-identity}
        name = node.get("name")
        if isinstance(name, str) and IDENTITY_KEY.match(name):
            value = node.get("value")
            if isinstance(value, (str, int, float)):
                found.append((path, name, str(value)))
            elif "valueFrom" in node:
                source = node["valueFrom"]
                ref = source.get("configMapKeyRef") if isinstance(source, dict) else None
                if isinstance(ref, dict) and isinstance(ref.get("name"), str) and isinstance(ref.get("key"), str):
                    indirect.append((path, name, f"{ref['name']}/{ref['key']}"))
                else:
                    # secretKeyRef (base64, unvettable), fieldRef, or a malformed ref: an identity is
                    # a literal or a repo-local ConfigMap value — never anything else.
                    indirect.append((path, name, "<unresolvable-source>"))
        # ConfigMap data / any mapping whose KEY is an identity key.
        for key, value in node.items():
            if isinstance(key, str) and IDENTITY_KEY.match(key) and isinstance(value, (str, int, float)):
                found.append((path, key, str(value)))
            _walk(value, path, found, indirect)
    elif isinstance(node, list):
        for item in node:
            _walk(item, path, found, indirect)


def _configmap_index(documents: list[tuple[Path, object]]) -> set[tuple[str, str]]:
    """(configMapName, dataKey) for every ConfigMap defined in the repository."""
    index: set[tuple[str, str]] = set()
    for _, document in documents:
        if not isinstance(document, dict) or document.get("kind") != "ConfigMap":
            continue
        name = (document.get("metadata") or {}).get("name")
        data = document.get("data")
        if isinstance(name, str) and isinstance(data, dict):
            index.update((name, key) for key in data if isinstance(key, str))
    return index


class ScanResult:
    """What the scan saw, including everything it could NOT vet — never silently dropped."""

    def __init__(self) -> None:
        self.identities: list[tuple[Path, str, str]] = []
        self.unparseable: list[tuple[Path, str]] = []
        self.unresolved: list[tuple[Path, str, str]] = []


def scan(root: Path) -> ScanResult:
    """Scan every YAML manifest under `root` (.yaml AND .yml), FAIL-CLOSED.

    A file that cannot be parsed, or an identity sourced from something this scan cannot read, is
    REPORTED rather than skipped: a guard that quietly ignores what it cannot vet approves it.
    """
    result = ScanResult()
    indirect: list[tuple[Path, str, str]] = []
    documents: list[tuple[Path, object]] = []
    for path in sorted(p for ext in ("*.yaml", "*.yml") for p in root.rglob(ext)):
        try:
            parsed = list(yaml.safe_load_all(path.read_text()))
        except yaml.YAMLError as error:
            # One malformed document used to abandon the whole file, hiding every valid declaration
            # after it behind a line-scan that could not see block form. Now it is a hard failure.
            result.unparseable.append((path, str(error).splitlines()[0]))
            continue
        for document in parsed:
            documents.append((path, document))
            _walk(document, path, result.identities, indirect)

    known = _configmap_index(documents)
    for path, key, reference in indirect:
        name, _, data_key = reference.partition("/")
        if (name, data_key) not in known:
            # The referenced value lives outside this repository (or in a Secret), so its identity can
            # never be vetted here. Fail closed rather than assume it is clean.
            result.unresolved.append((path, key, reference))
    return result


def identities(root: Path) -> list[tuple[Path, str, str]]:
    """Literal identity declarations under `root`. Use scan() when the fail-closed lists matter."""
    return scan(root).identities


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

    def test_every_manifest_is_parseable_and_every_identity_is_literal(self) -> None:
        # FAIL-CLOSED, both directions. An unparseable file cannot be vetted, and an identity sourced
        # from outside this repo (or from a base64 Secret) cannot be read — so neither may be silently
        # skipped: they fail the guard until fixed or deliberately resolved.
        result = scan(ROOT / "k8s")
        self.assertEqual(
            [], [f"{path.relative_to(ROOT)}: {error}" for path, error in result.unparseable],
            "unparseable manifest(s) — the guard cannot vet these, so it refuses to pass them",
        )
        self.assertEqual(
            [], [f"{path.relative_to(ROOT)} {key} <- {ref}" for path, key, ref in result.unresolved],
            "identity sourced from an unresolvable reference — declare it as a literal, or point it at "
            "a ConfigMap defined in this repository so its value can be vetted",
        )

    def test_fail_closed_paths_are_actually_detected(self) -> None:
        # The two holes a previous revision of this guard had: a malformed document silently swallowed
        # the rest of its file, and valueFrom identities were unconditionally ignored.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # A valid versioned identity AFTER a malformed document in the same file: the old
            # line-scan fallback could not see block form, so this used to disappear entirely.
            (root / "malformed.yaml").write_text(
                "apiVersion: v1\nkind: ConfigMap\ndata:\n  A: 1\n"
                "---\n"
                "this: is: not: valid: yaml\n"
                "---\n"
                "apiVersion: v1\nkind: ConfigMap\ndata:\n  Z_APP_ID: hidden-v9\n"
            )
            (root / "indirect.yaml").write_text(
                "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
                "      containers:\n        - name: svc\n          env:\n"
                "            - name: X_APP_ID\n"
                "              valueFrom:\n"
                "                configMapKeyRef:\n"
                "                  name: not-in-this-repo\n                  key: X_APP_ID\n"
            )
            (root / "secret-sourced.yaml").write_text(
                "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
                "      containers:\n        - name: svc\n          env:\n"
                "            - name: S_APP_ID\n"
                "              valueFrom:\n"
                "                secretKeyRef:\n"
                "                  name: some-secret\n                  key: S_APP_ID\n"
            )
            result = scan(root)
        self.assertEqual(
            {"malformed.yaml"}, {path.name for path, _ in result.unparseable},
            "a malformed document must fail the guard, not silently hide the rest of its file",
        )
        self.assertEqual(
            {"indirect.yaml", "secret-sourced.yaml"}, {path.name for path, _, _ in result.unresolved},
            "an identity from an unresolvable ConfigMap/Secret reference must fail the guard",
        )

    def test_a_resolvable_configmap_reference_is_accepted(self) -> None:
        # The flip side: a reference to a ConfigMap defined HERE is vetted at that definition site, so
        # it must not be reported unresolved — otherwise the guard would cry wolf on valid manifests.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "cm.yaml").write_text(
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: shared-config\n"
                "data:\n  X_APP_ID: some-service\n"
            )
            (root / "dep.yaml").write_text(
                "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
                "      containers:\n        - name: svc\n          env:\n"
                "            - name: X_APP_ID\n"
                "              valueFrom:\n"
                "                configMapKeyRef:\n"
                "                  name: shared-config\n                  key: X_APP_ID\n"
            )
            result = scan(root)
        self.assertEqual([], result.unresolved, "a repo-local ConfigMap reference must resolve")
        self.assertIn("some-service", {value for _, _, value in result.identities})

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
