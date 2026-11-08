# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring for the TA-books documents CHART-STRUCTURE-TOOLS, CLASSICAL-CHART-PATTERNS, TICK-TRADE-TOOLS, ELLIOTT-FIB-CYCLE-TOOLS and the market-structure Avro cutover (W0) (the INDICATOR-STUDIES precedent is tests/test_indicator_studies_topics.py): every producer and gateway
flag is the literal "false" in every source and every GENERATED manifest, the two new services are registered everywhere a service must be (the five image
lists, the Jenkins choices, the overlays, image-tags, the three lifecycle hold lists) and are HELD at replicas 0, the W0 rollover flag is never committed,
the new topics are declared, and the destructive Jenkins stage keeps its gates."""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
P = "options.spx.market-structure."


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def env_entries(path: Path, name: str) -> list[tuple[str, str]]:
    """Every env entry called `name`, as (container, value), read from the YAML STRUCTURE.

    This replaced a text regex over the whole file. The regex could not tell two containers apart, so when
    market-structure-service gained a SECOND container - `market-structure-estimate`, the Globex candle
    pattern worker added by #1428, which legitimately runs with different flag values in production than
    the `market-structure` worker beside it - every assertion comparing a file's values against one
    expected value began failing with ['false', 'true'] != ['true']. An expectation keyed only on
    (file, flag) cannot describe two containers that are MEANT to differ; it has to be keyed on
    (file, container, flag). Only two files in the repo are affected (the dev and production
    market-structure slices), but this module is the TA-books step of deploy-validation, and that job is a
    single `bash -e` script, so its failure skipped every later step of the gate.

    The regex had a SECOND blind spot, found while mutation-testing this change: it required block style,
    `- name: X` then `value: Y` on the next line, so it could not see an entry written in FLOW style,
    `- { name: X, value: "false" }`. Measured on the tree at the time: the regex could see 76 of the 80
    layer-flag env entries under k8s/, and all 4 it missed were the estimate container's tools/geometry
    flags in market-structure-estimate-{dev,prod}-patch.yaml, which are written in flow style. Those four
    did not merely compare wrongly - they were invisible, so the repo-wide sweep looped over an empty list
    and reported the files compliant without checking anything. Parsing the structure removes the whole
    class of blind spot rather than widening the pattern.

    A flag given by `valueFrom` is refused outright rather than ignored (it would otherwise read as absent,
    as it did under the regex), and a non-string value raises, because the Kubernetes API rejects a
    non-string env value and a silent "False" would let a manifest that cannot be applied pass this test.
    """
    def containers(node):
        if isinstance(node, dict):
            if isinstance(node.get("name"), str) and isinstance(node.get("env"), list):
                yield node
            for v in node.values():
                yield from containers(v)
        elif isinstance(node, list):
            for v in node:
                yield from containers(v)

    out: list[tuple[str, str]] = []
    for doc in yaml.safe_load_all(path.read_text()):
        for c in containers(doc):
            for e in c["env"]:
                if isinstance(e, dict) and e.get("name") == name:
                    if "value" not in e:
                        # Codex r1: a tracked flag arriving through valueFrom would be INVISIBLE here, and
                        # under the old regex too - it would read as "the flag is absent", which the model
                        # treats as nothing to check. These flags must be literal so the committed manifest
                        # states the layer's state outright; refuse the indirection rather than ignore it.
                        raise AssertionError(
                            f"{path}: container {c['name']} supplies {name} without a literal value "
                            f"({sorted(k for k in e if k != 'name')}); a layer flag must be a literal so "
                            "this model can see it")
                    v = e["value"]
                    if not isinstance(v, str):
                        raise AssertionError(
                            f"{path}: container {c['name']} sets {name} to the non-string {v!r}; "
                            "Kubernetes requires a string env value, so this manifest would not apply")
                    out.append((c["name"], v.strip("\"'")))
    return out


def env_values(path: Path, name: str, container: str | None = None) -> list[str]:
    """The values of `name`, optionally scoped to one container."""
    return [v for c, v in env_entries(path, name) if container is None or c == container]


def _statements_writing_to(text: str, var: str) -> list[str]:
    """The logical shell statements in `text` that redirect into `$var`, each as one string.

    Used to extend a `--from-env-file="$rtEnv"` producer's payload to whatever actually writes that file,
    so the check reads the generated key set instead of the entire file. A statement is grown in BOTH
    directions across backslash continuations, because the writer in Jenkinsfile.experiment-deploy is a
    two-line `printf ... \\` / `  "$VAR..." > "$rtEnv"`.
    """
    lines = text.splitlines()
    pattern = re.compile(r">>?\s*[\"']?\$\{?%s\}?" % re.escape(var))
    out = []
    for i, line in enumerate(lines):
        if not pattern.search(line):
            continue
        start = i
        while start > 0 and lines[start - 1].rstrip().endswith("\\"):
            start -= 1
        end = i
        while end + 1 < len(lines) and lines[end].rstrip().endswith("\\"):
            end += 1
        out.append("\n".join(lines[start:end + 1]))
    return out


class FlagsAreLiterallyFalse(unittest.TestCase):
    EXPECTED = {
        "MARKET_STRUCTURE_TOOLS_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                           "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                           "k8s/services/market-structure/overlays/production/manifest.yaml"],
        "MARKET_STRUCTURE_GEOMETRY_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                              "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                              "k8s/services/market-structure/overlays/production/manifest.yaml"],
        # X2 swing pivots (task T-5 of the level-absorption closure): off in the base and in production, on in dev only.
        "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                                  "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                                  "k8s/services/market-structure/overlays/production/manifest.yaml"],
        "GATEWAY_STRUCTURE_TOOLS_ENABLED": [], "GATEWAY_GEOMETRY_ENABLED": [], "GATEWAY_TRADE_TOOLS_ENABLED": [], "GATEWAY_WAVE_CYCLE_ENABLED": [], "GATEWAY_BREAK_CLASSIFICATION_ENABLED": [],
        "TT_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/services/es-trade-tools/overlays/dev/manifest.yaml",
                       "k8s/services/es-trade-tools/overlays/production/manifest.yaml",
                       "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml"],
        "TT_PF_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_VP_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_FLOW_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_CLIMAX_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "WAVE_CYCLE_ENABLED": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml", "k8s/services/wave-cycle/overlays/dev/manifest.yaml",
                               "k8s/services/wave-cycle/overlays/production/manifest.yaml",
                               "k8s/overlays/production/wave-cycle-prod-enable-patch.yaml"],
        "WAVE_CYCLE_SEASONALITY_ENABLED": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml", "k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"],
        "WAVE_CYCLE_IGNORE_CHECKPOINT": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"],
    }
    GATEWAY_FILES = ["k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/overlays/production/feed-gateway-prod-env-patch.yaml",
                     "k8s/services/feed-gateway/overlays/dev/manifest.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"]

    # 2026-10-21 (owner): the four gateway relay flags are ON in DEV ONLY, for the Order tab analysis layers; production and everything else stays literally false.
    DEV_GATEWAY_FILES = ["k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/services/feed-gateway/overlays/dev/manifest.yaml"]
    DEV_ON = ("GATEWAY_STRUCTURE_TOOLS_ENABLED", "GATEWAY_GEOMETRY_ENABLED", "GATEWAY_TRADE_TOOLS_ENABLED", "GATEWAY_WAVE_CYCLE_ENABLED", "GATEWAY_BREAK_CLASSIFICATION_ENABLED")

    # 2026-10-21 (owner: complete the production deployment, one layer at a time): PRODUCTION layers 2 and 3 = structure tools and geometry. Among the TA-books layer flags
    # (the names in EXPECTED), only those named here are "true", and only in the named production files; the rest stay "false" everywhere.
    PROD_ON = {
        "GATEWAY_STRUCTURE_TOOLS_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        "MARKET_STRUCTURE_TOOLS_ENABLED": ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"),
        "GATEWAY_GEOMETRY_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        # 2026-10-22 (owner: every service out of shadow, live on dev and production): the break-classification relay (Phase F) is ON in production too.
        "GATEWAY_BREAK_CLASSIFICATION_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        "MARKET_STRUCTURE_GEOMETRY_ENABLED": ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"),        # 2026-10-22 (owner: every service calibrated and live, never shadow): es-trade-tools (TICK-TRADE-TOOLS) and wave-cycle (ELLIOTT-FIB-CYCLE-TOOLS) run live in production.
        # The GATEWAY relay flags of those two layers stay "false" until the version-2 gateway and web are live (a later change).
        "TT_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_PF_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_VP_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_FLOW_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_CLIMAX_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "WAVE_CYCLE_ENABLED": ("k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"),
        "WAVE_CYCLE_SEASONALITY_ENABLED": ("k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"),
    }

    # 2026-10-31 (owner: remove es-trade-tools and wave-cycle from the permanent-down path on dev): both run LIVE on dev through their dev enable patches, with the same calibrated release
    # configuration production runs. "true" only in these two named dev files per flag; every other file still has to say "false".
    _TT_DEV = ("k8s/overlays/dev/es-trade-tools-dev-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/dev/manifest.yaml")
    _WC_DEV = ("k8s/overlays/dev/wave-cycle-dev-enable-patch.yaml", "k8s/services/wave-cycle/overlays/dev/manifest.yaml")
    DEV_TA_ON = {
        "TT_ENABLED": _TT_DEV, "TT_PF_ENABLED": _TT_DEV, "TT_VP_ENABLED": _TT_DEV, "TT_FLOW_ENABLED": _TT_DEV, "TT_CLIMAX_ENABLED": _TT_DEV,
        "WAVE_CYCLE_ENABLED": _WC_DEV, "WAVE_CYCLE_SEASONALITY_ENABLED": _WC_DEV,
    }
    # The first test only inspects files listed in EXPECTED, so the dev files must be listed there or their "true" would never be checked. A plain loop: a comprehension in a class body cannot see these names.
    for _name, _files in DEV_TA_ON.items():
        for _f in _files:
            if _f not in EXPECTED[_name]:
                EXPECTED[_name].append(_f)

    # market-structure in DEV: structure tools and geometry are "true" because the running dev Deployment already reads them (the manifest had said "false", which a service-deploy would have applied);
    # swing pivots (X2) are on in dev only. See k8s/overlays/dev/market-structure-dev-env-patch.yaml.
    DEV_MS_FILES = ["k8s/overlays/dev/market-structure-dev-env-patch.yaml", "k8s/services/market-structure/overlays/dev/manifest.yaml"]
    DEV_MS_ON = ("MARKET_STRUCTURE_TOOLS_ENABLED", "MARKET_STRUCTURE_GEOMETRY_ENABLED", "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED")

    # market-structure-service carries TWO containers since #1428 (production Globex candle patterns): the
    # `market-structure` worker and the `market-structure-estimate` worker, each with its own env block and
    # its own per-environment patch file. In PRODUCTION they deliberately DISAGREE - the worker runs tools
    # and geometry, the estimate container does not - so an expectation keyed on (file, flag) alone cannot
    # describe them, and keying it that way is what turned this module red. Estimate is ON in dev only.
    ESTIMATE_CONTAINER = "market-structure-estimate"
    ESTIMATE_ON = ("MARKET_STRUCTURE_TOOLS_ENABLED", "MARKET_STRUCTURE_GEOMETRY_ENABLED")
    ESTIMATE_DEV_FILES = ("k8s/overlays/dev/market-structure-estimate-dev-patch.yaml",
                          "k8s/services/market-structure/overlays/dev/manifest.yaml")
    # Its two patch files are added to EXPECTED below so the per-file test actually inspects them; before
    # this change they were reached only by the repo-wide rglob test.
    for _name in ESTIMATE_ON:
        for _f in ("k8s/overlays/dev/market-structure-estimate-dev-patch.yaml",
                   "k8s/overlays/production/market-structure-estimate-prod-patch.yaml"):
            if _f not in EXPECTED[_name]:
                EXPECTED[_name].append(_f)

    def expected_value(self, rel, name, container=None):
        """The value `name` must carry in `rel`, for `container`.

        The container argument is not optional in spirit: two containers in one file are allowed to differ,
        and the estimate container is resolved FIRST so the worker's rules can never be applied to it.
        """
        if container == self.ESTIMATE_CONTAINER:
            return "true" if name in self.ESTIMATE_ON and rel in self.ESTIMATE_DEV_FILES else "false"
        if name in self.DEV_MS_ON and rel in self.DEV_MS_FILES:
            return "true"
        if rel in self.DEV_TA_ON.get(name, ()):
            return "true"
        if rel in self.PROD_ON.get(name, ()):
            return "true"
        return "true" if name in self.DEV_ON and rel in self.DEV_GATEWAY_FILES else "false"

    def test_each_expected_source_and_generated_manifest_carries_its_flag_with_the_expected_value(self):
        """Every listed file must carry each flag, and every CONTAINER in it must carry the right value.

        Scoped per container because two containers of one Deployment may legitimately differ (see
        ESTIMATE_CONTAINER). Presence is still required - a file that stopped declaring a flag altogether
        would otherwise pass with an empty list - and a container declaring the same flag twice is refused,
        because the last duplicate silently wins at runtime.
        """
        for name, paths in self.EXPECTED.items():
            for rel in (paths or self.GATEWAY_FILES):
                entries = env_entries(ROOT / rel, name)
                self.assertTrue(entries, f"{rel}: {name} is not declared at all")
                seen = [c for c, _ in entries]
                self.assertEqual(len(seen), len(set(seen)),
                                 f"{rel}: {name} is declared twice in one container: {seen}")
                for container, value in entries:
                    self.assertEqual(value, self.expected_value(rel, name, container),
                                     f"{rel}: {name} in container {container}")

    def test_no_manifest_anywhere_sets_any_of_these_flags_to_anything_but_false(self):
        """Repo-wide: no manifest may set a layer flag to anything other than what the model allows.

        Per container, for the same reason as above. Non-vacuity is asserted at the end: this sweep is the
        backstop that catches a flag enabled in a file nobody listed, so if it ever inspected nothing it
        would be the most reassuring test in the file and the least true.
        """
        checked = 0
        for path in sorted((ROOT / "k8s").rglob("*.yaml")):
            rel = str(path.relative_to(ROOT))
            for name in self.EXPECTED:
                entries = env_entries(path, name)
                # Codex r1: the duplicate check used to apply only to the listed files, so an UNLISTED
                # manifest could declare a flag twice in one container - where the last value silently
                # wins - and pass as long as both values matched the model.
                seen = [c for c, _ in entries]
                self.assertEqual(len(seen), len(set(seen)),
                                 f"{rel}: {name} is declared twice in one container: {seen}")
                for container, v in entries:
                    checked += 1
                    self.assertEqual(v, self.expected_value(rel, name, container),
                                     f"{rel}: {name} in container {container}")
        self.assertGreater(checked, 50, "the repo-wide sweep inspected almost nothing")

    def test_no_configmap_or_secret_can_inject_a_layer_flag_through_envFrom(self):
        """The envFrom bypass, closed on the ConfigMap side rather than the container side.

        356 containers in this repo legitimately use `envFrom` to pull the shared options-edge-config
        ConfigMap and options-edge-runtime-secrets Secret, so forbidding envFrom is not an option. But a
        tracked layer flag placed in one of those objects would be injected into every one of those
        containers, and NEITHER the old text regex nor the structural walker above can see it: the sweep
        would report every manifest compliant while the layer was live fleet-wide (Codex r1).

        So the flags are forbidden from appearing as a ConfigMap or Secret KEY anywhere under k8s/, and
        from the payload of any command that imperatively creates one of those objects. Measured when
        written: 0 such keys and 0 such payloads exist, so this is a guard against a future edit, not a
        description of current drift.

        A producer that writes its keys into a temp env file (`--from-env-file="$rtEnv"`) is followed to
        the statements that write that file, so the generated key set is what gets checked - not the whole
        file, which would fail on an unrelated comment. If no writer can be located the whole file IS
        searched, fail-closed, and the failure message says that is why.

        WHAT THIS DOES NOT COVER, stated rather than implied (Codex r3). The imperative half recognises
        `kubectl create secret generic|configmap <name>`, which is how all five of this repo's producers
        work today. An object conjured some other way - `kubectl apply` of YAML generated at runtime,
        `kubectl set env`, a patch, or a name built from a variable - would not be seen. No static test can
        be exhaustive against arbitrary shell; the declarative route is closed completely by the k8s/ scan
        above, the five known producers are closed by the payload scan, and a NEW producer using `create`
        is picked up automatically because the producer set is derived. A reviewer should not read this
        test as proof that no such path exists.
        """
        offenders, objects = [], 0
        for path in sorted((ROOT / "k8s").rglob("*.yaml")):
            for doc in yaml.safe_load_all(path.read_text()):
                if not isinstance(doc, dict) or doc.get("kind") not in ("ConfigMap", "Secret"):
                    continue
                objects += 1
                # `data` and `stringData` both become environment entries under envFrom; a Secret's data
                # keys are plain names even though its values are base64.
                for section in ("data", "stringData"):
                    for key in (doc.get(section) or {}):
                        if key in self.EXPECTED:
                            offenders.append(f"{path.relative_to(ROOT)}: "
                                             f"{doc.get('kind')}/{doc['metadata'].get('name')} "
                                             f"{section}.{key}")
        self.assertEqual([], offenders,
                         "a layer flag in a ConfigMap/Secret is injected into every container that pulls "
                         "it with envFrom, and no per-manifest check can see it:\n  " + "\n  ".join(offenders))
        # Non-vacuity: if the scan found no ConfigMaps or Secrets at all it would be trivially green.
        self.assertGreater(objects, 5, "no ConfigMap/Secret documents were scanned")

        # Codex r2: scanning k8s/ is NOT the whole story. Some of the objects pulled in with envFrom are
        # not committed as YAML at all - options-edge-runtime-secrets and options-edge-databento-feed-env
        # are created imperatively by Jenkinsfile, Jenkinsfile.secrets-sync and Jenkinsfile.experiment-deploy,
        # and es4-runtime-secrets by scripts/es4/bootstrap-es4.sh. A tracked flag added to one of those
        # would reach every container that pulls it and nothing above would see it.
        #
        # Both halves are DERIVED, so a new object or a new producer is covered without editing this test:
        # the object names come from the envFrom references in k8s/, and the producers from whoever creates
        # an object by one of those names. The rule is a flat textual ban rather than a parse of
        # --from-literal keys, because a producer may build its keys indirectly (experiment-deploy uses
        # --from-env-file with a file it writes at runtime) - and these are application LAYER flags, so
        # their appearance in a Secret/ConfigMap producer is wrong however the key is formed.
        envfrom_names = set()

        def envfrom_refs(node):
            if isinstance(node, dict):
                for src in node.get("envFrom") or []:
                    if isinstance(src, dict):
                        for kind in ("configMapRef", "secretRef"):
                            ref = src.get(kind)
                            if isinstance(ref, dict) and ref.get("name"):
                                envfrom_names.add(ref["name"])
                for v in node.values():
                    envfrom_refs(v)
            elif isinstance(node, list):
                for v in node:
                    envfrom_refs(v)

        for path in (ROOT / "k8s").rglob("*.yaml"):
            for doc in yaml.safe_load_all(path.read_text()):
                if doc:
                    envfrom_refs(doc)
        self.assertTrue(envfrom_names, "no envFrom references found - this guard would check nothing")

        creates = re.compile(r"create\s+(?:secret\s+generic|configmap)\s+[\"']?(%s)\b"
                             % "|".join(re.escape(n) for n in sorted(envfrom_names)))
        producers, leaks = {}, []
        for path in sorted(ROOT.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(ROOT).as_posix()
            if rel.startswith(("k8s/", ".git/")) or path.suffix in (".png", ".jpg", ".jar", ".gz", ".zip"):
                continue
            try:
                text = path.read_text()
            except (UnicodeDecodeError, OSError):
                continue
            if not creates.search(text):
                continue
            made = set(creates.findall(text))
            producers[rel] = made
            for m in creates.finditer(text):
                # The PAYLOAD of this one command: from the `create ...` onwards, following shell line
                # continuations, stopping at the first line that does not end in a backslash. Scoped this
                # way (Codex r3) so that a comment, a diagnostic echo or an unrelated command in the same
                # file cannot fail this test - only text that is actually part of the object being built.
                end = m.end()
                while True:
                    nl = text.find("\n", end)
                    if nl == -1:
                        end = len(text)
                        break
                    line = text[end:nl]
                    end = nl + 1
                    if not line.rstrip().endswith("\\"):
                        break
                payload = text[m.start():end]
                # A producer that builds its key set at RUNTIME (--from-env-file / --from-file) has none of
                # the keys in its own payload, so the payload must be EXTENDED to whatever writes that
                # file. Scoped to the writing statements rather than the whole file (Codex r4): widening
                # to the file would re-create exactly the false positive r3 objected to.
                scopes = [("the create command's payload", payload)]
                for var in re.findall(r"--from-(?:env-)?file=[\"']?\$\{?(\w+)\}?", payload):
                    writers = _statements_writing_to(text, var)
                    if writers:
                        scopes += [(f"the statement writing ${var}", w) for w in writers]
                    else:
                        # Fail CLOSED, with the reason named: an env file whose writer cannot be located
                        # could carry anything, so the whole file is searched and the message says why.
                        scopes.append((f"whole file (no writer found for ${var})", text))
                for scope, blob in scopes:
                    for flag in self.EXPECTED:
                        if flag in blob:
                            leaks.append(f"{rel} (creates {','.join(sorted(made))}): {flag} appears in {scope}")
        self.assertEqual([], leaks,
                         "a layer flag in an imperatively-created Secret/ConfigMap is injected through "
                         "envFrom into every container that pulls it:\n  " + "\n  ".join(sorted(set(leaks))))
        # Non-vacuity, and a tripwire: these creators exist today, so finding none means the detection
        # broke rather than that the risk went away.
        self.assertGreaterEqual(len(producers), 3,
                                f"expected to find the imperative Secret/ConfigMap creators, found {producers}")

    def test_production_enables_only_the_structure_tools_geometry_and_break_classification_relays(self):
        for rel in ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"):
            for name in self.DEV_ON:
                self.assertEqual(env_values(ROOT / rel, name), ["true" if name in ("GATEWAY_STRUCTURE_TOOLS_ENABLED", "GATEWAY_GEOMETRY_ENABLED", "GATEWAY_BREAK_CLASSIFICATION_ENABLED") else "false"], f"{rel}: {name}")
        # The market-structure WORKER container. Scoped, because the estimate container beside it is off in
        # production on purpose and an unscoped read returns both values.
        W = "market-structure"
        for rel in ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"):
            self.assertEqual(env_values(ROOT / rel, "MARKET_STRUCTURE_TOOLS_ENABLED", W), ["true"], rel)
        PROD_SLICE = ROOT / "k8s/services/market-structure/overlays/production/manifest.yaml"
        self.assertEqual(env_values(PROD_SLICE, "MARKET_STRUCTURE_GEOMETRY_ENABLED", W), ["true"])
        self.assertEqual(env_values(ROOT / "k8s/services/market-structure/overlays/dev/manifest.yaml", "MARKET_STRUCTURE_TOOLS_ENABLED", W), ["true"])
        self.assertEqual(env_values(PROD_SLICE, "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED", W), ["false"])
        # And the production policy for the estimate container itself, which this test never stated before:
        # the Globex candle pattern worker runs NEITHER layer in production.
        for name in self.ESTIMATE_ON:
            self.assertEqual(env_values(PROD_SLICE, name, self.ESTIMATE_CONTAINER), ["false"],
                             f"the estimate container must not run {name} in production")
            self.assertEqual(env_values(ROOT / "k8s/overlays/production/market-structure-estimate-prod-patch.yaml",
                                        name, self.ESTIMATE_CONTAINER), ["false"])
            self.assertEqual(env_values(ROOT / "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                        name, self.ESTIMATE_CONTAINER), ["true"],
                             f"the estimate container runs {name} on dev")

    def test_the_committed_production_slices_equal_the_production_overlay_render_for_the_layer_flags(self):
        """Source/generated consistency: render the production overlay and compare EVERY container's flags
        with the committed generated slices.

        The render side used to read only the container whose name matched the app - `next(c for c in ...)` -
        so the estimate container's flags were rendered, deployed and never compared with anything. That is
        also why the mismatch surfaced as a confusing list: the render yielded one value and the committed
        slice yielded two. Keyed on (deployment, container, flag) now, so the two containers are each
        checked and neither can drift.
        """
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/production")], capture_output=True, text=True, check=True, timeout=120).stdout
        rendered = {}
        for doc in yaml.safe_load_all(out):
            if isinstance(doc, dict) and doc.get("kind") == "Deployment" and doc["metadata"]["name"] in ("market-structure-service", "feed-gateway-service"):
                for c in doc["spec"]["template"]["spec"]["containers"]:
                    for e in c.get("env", []):
                        if e["name"] in self.EXPECTED:
                            rendered[(doc["metadata"]["name"], c["name"], e["name"])] = e.get("value")
        MS, GW = "market-structure-service", "feed-gateway-service"
        want_keys = {(MS, "market-structure", "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED"),
                     (MS, "market-structure", "MARKET_STRUCTURE_TOOLS_ENABLED"),
                     (MS, "market-structure", "MARKET_STRUCTURE_GEOMETRY_ENABLED"),
                     (MS, self.ESTIMATE_CONTAINER, "MARKET_STRUCTURE_TOOLS_ENABLED"),
                     (MS, self.ESTIMATE_CONTAINER, "MARKET_STRUCTURE_GEOMETRY_ENABLED"),
                     (GW, "feed-gateway", "GATEWAY_STRUCTURE_TOOLS_ENABLED"), (GW, "feed-gateway", "GATEWAY_GEOMETRY_ENABLED"),
                     (GW, "feed-gateway", "GATEWAY_TRADE_TOOLS_ENABLED"), (GW, "feed-gateway", "GATEWAY_WAVE_CYCLE_ENABLED"),
                     (GW, "feed-gateway", "GATEWAY_BREAK_CLASSIFICATION_ENABLED")}
        self.assertEqual(set(rendered), want_keys)
        for dep, container, name in sorted(want_keys):
            rel = "k8s/services/market-structure/overlays/production/manifest.yaml" if dep == MS else "k8s/services/feed-gateway/overlays/production/manifest.yaml"
            self.assertEqual(env_values(ROOT / rel, name, container), [rendered[(dep, container, name)]],
                             f"{rel}: {name} in container {container}")
        # The production policy itself, per container: the worker runs both layers, the estimate worker neither.
        self.assertEqual(rendered[(MS, "market-structure", "MARKET_STRUCTURE_TOOLS_ENABLED")], "true")
        self.assertEqual(rendered[(MS, "market-structure", "MARKET_STRUCTURE_GEOMETRY_ENABLED")], "true")
        self.assertEqual(rendered[(MS, self.ESTIMATE_CONTAINER, "MARKET_STRUCTURE_TOOLS_ENABLED")], "false")
        self.assertEqual(rendered[(MS, self.ESTIMATE_CONTAINER, "MARKET_STRUCTURE_GEOMETRY_ENABLED")], "false")

    def test_the_w0_recovery_rollover_flag_is_never_committed(self):
        for path in (ROOT / "k8s").rglob("*.yaml"):
            self.assertNotIn("name: MARKET_STRUCTURE_IGNORE_RECOVERY", path.read_text(), str(path))

    def test_the_two_new_services_declare_no_prefix_override_so_the_deployed_names_are_the_declared_ones(self):
        for rel in ("k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"):
            self.assertNotIn("TOPIC_PREFIX", read(rel).replace("TOPIC_PREFIX and", "").replace("TOPIC_PREFIX is", "").replace("TOPIC_PREFIX,", ""))


class NewServicesAreHeldAndRegistered(unittest.TestCase):
    SERVICES = {"es-trade-tools": ("es-trade-tools-service", "ES_TRADE_TOOLS_IMAGE", "options-edge-es-trade-tools"),
                "wave-cycle": ("wave-cycle-service", "WAVE_CYCLE_IMAGE", "options-edge-wave-cycle")}

    def test_rendered_slices_are_one_deployment_at_replicas_one_on_dev_and_production(self):
        for svc, (dep, _, image) in self.SERVICES.items():
            for env in ("dev", "production"):
                docs = [d for d in yaml.safe_load_all(read(f"k8s/services/{svc}/overlays/{env}/manifest.yaml")) if d]
                self.assertEqual([d["kind"] for d in docs], ["Deployment"], f"{svc} {env}")
                d = docs[0]
                self.assertEqual(d["metadata"]["name"], dep)
                # both environments render replicas 1 through their own enable patch: production since 2026-10-22, dev since 2026-10-31 (owner: off the permanent-down path)
                self.assertEqual(d["spec"]["replicas"], 1, f"{svc} {env}: see docs/ta-books/DEPLOY-ROLLOUT.md")
                self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", "single replica, a second transactional.id instance would fence the first")
                self.assertIn(image, d["spec"]["template"]["spec"]["containers"][0]["image"])

    def test_services_yaml_registers_both_as_standalone_dev_and_production(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        for svc, (dep, _, image) in self.SERVICES.items():
            self.assertEqual((reg[svc]["managedBy"], reg[svc]["envs"], reg[svc]["image"], reg[svc]["deployments"]), ("standalone", ["dev", "production"], image, [dep]))

    def test_the_image_variable_is_in_every_image_resolution_list(self):
        for svc, (_, var, _) in self.SERVICES.items():
            self.assertEqual(len(re.findall(rf"^{var}=", read("scripts/deploy/resolve-images.sh"), re.M)), 2, "both heredoc branches")
            for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/image-preflight.sh", "Jenkinsfile"):
                self.assertIn(var, read(rel), f"{rel}: {var}")
            self.assertGreaterEqual(read("Jenkinsfile").count(var), 3, "param, defaults map and the promote list")

    def test_image_tags_and_overlays_reference_both(self):
        for svc, (dep, _, image) in self.SERVICES.items():
            self.assertIn(f"{dep}: 192.168.100.252:5000/{image}:dev", read("image-tags/dev.yaml"))
            self.assertIn(f"{dep}: 192.168.100.252:5000/{image}:prod", read("image-tags/production.yaml"))
            for env in ("dev", "production"):
                self.assertIn(f"../../services/{svc}/base", read(f"k8s/overlays/{env}/kustomization.yaml"))
            self.assertIn(f"newName: host.docker.internal:5001/{image}", read("k8s/overlays/dev/kustomization.yaml"))

    def test_no_lifecycle_list_holds_either_service_down_on_production_or_dev(self):
        # production runs both (2026-10-22): a KEEP_DOWN entry would scale them to 0 at the next reboot or morning start.
        # dev runs both (2026-10-31): a DISABLED_DEV entry would hold them at 0 on every `dev-cleanup start`, whatever the manifest renders.
        for dep, _, _ in self.SERVICES.values():
            for rel in ("scripts/ops/oe-boot-bringup.sh", "scripts/ops/morning-autostart.sh"):
                text = "\n".join(l for l in read(rel).splitlines() if re.match(r"\s*KEEP_DOWN=", l))
                self.assertNotRegex(text, rf"(?<![\w-]){re.escape(dep)}(?![\w-])", f"{rel}: {dep} must not be held down on production")
            self.assertNotRegex(read("scripts/ops/dev-cleanup.sh"), rf"(?<![\w-]){re.escape(dep)}(?![\w-])", f"dev-cleanup.sh: {dep} must not be held down on dev")

    def test_the_service_deploy_job_offers_both_and_runs_their_topic_barriers(self):
        text = read("Jenkinsfile.service-deploy")
        for svc in self.SERVICES:
            self.assertIn(f"'{svc}',", text)
            self.assertIn(f"params.SERVICE == '{svc}' && !params.DEPLOY_DRY_RUN", text)
        self.assertIn("scripts/kafka/ensure-es-trade-tools-topics.sh", text)
        self.assertIn("scripts/kafka/ensure-wave-cycle-topics.sh", text)

    def test_the_heap_never_exceeds_the_headroom_gate_and_the_probes_match_the_services_endpoints(self):
        tt = yaml.safe_load(read("k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml"))["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("-Xmx384m", [e["value"] for e in tt["env"] if e["name"] == "JAVA_TOOL_OPTIONS"][0])
        self.assertEqual(tt["resources"]["limits"]["memory"], "1Gi")
        self.assertEqual(tt["ports"][0]["containerPort"], 8153)
        wc = yaml.safe_load(read("k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"))["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(wc["ports"][0]["containerPort"], 8161)
        self.assertEqual(wc["readinessProbe"]["httpGet"]["path"], "/health/ready")
        self.assertNotIn("-Xmx", " ".join(e.get("value", "") for e in wc["env"]), "the image ENTRYPOINT fixes the heap; a value here would be ignored and lie")


class TopicsAndCutoverStage(unittest.TestCase):
    def test_tools_and_geometry_topics_are_declared_one_partition_two_sessions_exact(self):
        out = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"; echo "$OPTIONS_EDGE_TOPICS"; echo "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"; '
                              f'echo "$OPTIONS_EDGE_EXACT_PARTITION_TOPICS"'], capture_output=True, text=True, check=True).stdout.splitlines()
        for t in (P + "tools", P + "geometry"):
            self.assertIn(f"{t}:1", out[0].split())
            self.assertIn(f"{t}=172800000", out[1].split())
            self.assertIn(t, out[2].split())
            self.assertNotRegex(t, r"\.v\d+$")

    def test_the_ensure_script_ensures_tools_and_geometry_unconditionally(self):
        text = read("scripts/kafka/ensure-market-structure-topics.sh")
        self.assertIn('ensure "$TOOLS_TOPIC" 172800000', text)
        self.assertIn('ensure "$GEOMETRY_TOPIC" 172800000', text)
        self.assertIn('ensure "$SWING_PIVOTS_TOPIC" 172800000', text)

    def test_the_wipe_stage_runs_after_the_rollout_and_keeps_its_gates(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertLess(text.index("stage('Deploy (service-scoped)')"), text.index("stage('Roll forward market-structure topics (W0 step 4)')"),
                        "the Avro writer is live BEFORE the wipe (the 2026-10-18 incident)")
        stage = text[text.index("stage('Roll forward market-structure topics (W0 step 4)')"):text.index("stage('Verify web UI (post-rollout)')")]
        for must in ("params.W0_CUTOVER_WIPE", "--producers-attested", "--expect-image-digest", "--expect-gateway-digest", "--gates-only",
                     "roll-forward-market-structure.sh --recreate"):
            self.assertIn(must, stage)
        # a dry run rehearses every gate (--gates-only) instead of skipping the stage
        self.assertNotIn("!params.DEPLOY_DRY_RUN", stage)
        # every W0-only argument is judged BEFORE the rollout stage can mutate anything
        early = text[text.index("stage('Validate W0 cutover parameters')"):text.index("stage('Deploy path')")]
        self.assertLess(text.index("stage('Validate W0 cutover parameters')"), text.index("stage('Deploy (service-scoped)')"))
        for must in ("W0_EXPECT_IMAGE_DIGEST ==~ /sha256:[0-9a-f]{64}/", "W0_EXPECT_GATEWAY_DIGEST ==~ /sha256:[0-9a-f]{64}/", "W0_PRE_CUTOVER_IDS ==~",
                     "W0_PRODUCERS_ATTESTED", "MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER", "SERVICE != 'market-structure'"):
            self.assertIn(must, early)
        # the destructive script is reachable from nowhere else
        self.assertEqual(text.count("scripts/kafka/roll-forward-market-structure.sh --recreate"), 1)

    def test_the_registration_stage_precedes_the_rollout_and_needs_the_contracts_commit(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertLess(text.index("stage('Register TA-books schemas')"), text.index("stage('Deploy (service-scoped)')"))
        stage = text[text.index("stage('Register TA-books schemas')"):text.index("stage('Recreate indicator topics (Avro cutover)')")]
        self.assertIn("CONTRACTS_PERMITTED_SHA", stage)
        self.assertIn("permitted-sha-guard.sh --dir .jenkins-tmp/contracts-ta --ref main", stage)

    def test_the_rollover_parameter_reaches_service_deploy_sh_as_an_environment_variable_it_reads(self):
        self.assertIn("MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER", read("scripts/deploy/service-deploy.sh"))
        self.assertIn("booleanParam(name: 'MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER', defaultValue: false", read("Jenkinsfile.service-deploy"))


class DocumentedRolloutMatchesTheCode(unittest.TestCase):
    def test_the_rollout_doc_exists_and_names_every_script_it_relies_on(self):
        doc = read("docs/ta-books/DEPLOY-ROLLOUT.md")
        for rel in ("scripts/kafka/ensure-fixed-id-schemas.sh", "scripts/kafka/roll-forward-market-structure.sh", "scripts/kafka/ensure-market-structure-topics.sh",
                    "scripts/kafka/ensure-es-trade-tools-topics.sh", "scripts/kafka/ensure-wave-cycle-topics.sh"):
            self.assertIn(Path(rel).name, doc)
            self.assertTrue((ROOT / rel).is_file(), rel)


if __name__ == "__main__":
    unittest.main()
