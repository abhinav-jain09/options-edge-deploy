from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "k8s" / "base"

# The replay-eligible Databento processors (the orchestrator clones ONLY fully-attested Deployments).
ELIGIBLE = {
    "raw-to-display-databento-service",
    "strike-flow-classifier-databento",
    "databento-mission-pace-service",
    "databento-mission-pressure-service",
    "databento-mission-sandwich-service",
    "directional-pressure-databento-service",
    "databento-volume-aggregator",
    "hpsf-stage-a-service",
    "hpsf-stage-b-service",
}

# Default-DENY attestation set: name -> the ONLY value that passes.
REQUIRED = {
    "oe.replay/include": "true",
    "oe.replay/side-effects": "false",
    "oe.replay/topic-remap": "required",
    "oe.replay/state": "ephemeral",
    "oe.replay/determinism": "event-time",
}

# Services that must NEVER carry oe.replay/include=true (feeds, sinks, gateway, IBKR/GEX, external I/O).
MUST_NOT_INCLUDE = {
    "options-edge-databento-feed",
    "hpsf-postgres-writer-service",
    "raw-postgres-writer",
    "pressure-postgres-writer",
    "strike-flow-classifier-ibkr",
    "options-edge-feed-gateway",
}


def _documents(text: str) -> list[str]:
    """Split a (possibly multi-document) YAML file into its individual documents."""
    docs, cur = [], []
    for line in text.split("\n"):
        if line.strip() == "---":
            docs.append("\n".join(cur))
            cur = []
        else:
            cur.append(line)
    docs.append("\n".join(cur))
    return [d for d in docs if d.strip()]


def _top_metadata_labels(doc: str) -> dict[str, str]:
    """Parse ONE document's top-level metadata.labels (dependency-free)."""
    lines = doc.split("\n")
    spec_idx = next((i for i, l in enumerate(lines) if l.rstrip() == "spec:"), len(lines))
    meta = lines[:spec_idx]
    labels: dict[str, str] = {}
    i = 0
    while i < len(meta):
        if meta[i].rstrip() == "  labels:":  # metadata.labels (2-space indent)
            i += 1
            while i < len(meta) and meta[i].startswith("    ") and ":" in meta[i]:
                key, _, val = meta[i].strip().partition(":")
                labels[key.strip()] = val.strip().strip('"').strip("'")
                i += 1
            break
        i += 1
    return labels


def _deployment_name(doc: str) -> str:
    for l in doc.split("\n"):
        if l.startswith("  name:"):
            return l.split(":", 1)[1].strip()
    return ""


def _all_deployments() -> list[tuple[str, str, dict[str, str]]]:
    """(filename, deployment-name, metadata.labels) for EVERY Deployment document in k8s/base."""
    out = []
    for f in sorted(BASE.glob("*deployment*.yaml")):
        for doc in _documents(f.read_text()):
            if "kind: Deployment" not in doc:
                continue
            out.append((f.name, _deployment_name(doc), _top_metadata_labels(doc)))
    return out


class ReplayAttestationLabelsTest(unittest.TestCase):
    def test_every_eligible_processor_carries_the_full_attestation_set(self) -> None:
        by_name = {name: labels for _, name, labels in _all_deployments()}
        for name in ELIGIBLE:
            self.assertIn(name, by_name, f"eligible processor {name} has no Deployment manifest")
            labels = by_name[name]
            for key, want in REQUIRED.items():
                self.assertEqual(labels.get(key), want,
                                 f"{name}: metadata label {key} must be '{want}', got {labels.get(key)!r}")

    def test_ci_lint_include_true_requires_the_other_four_labels(self) -> None:
        # Default-deny invariant across EVERY document in EVERY file: any Deployment opting in
        # (include=true) MUST carry the full, correctly-valued set — a partial/mis-valued attestation
        # is a CI failure (fail closed).
        for fname, name, labels in _all_deployments():
            if labels.get("oe.replay/include") != "true":
                continue
            for key, want in REQUIRED.items():
                self.assertEqual(labels.get(key), want,
                                 f"{fname}[{name}]: include=true but {key}={labels.get(key)!r} (require '{want}')")

    def test_non_eligible_services_are_not_attested(self) -> None:
        # No service outside the eligible set may carry include=true (covers multi-doc files like
        # strike-flow-classifier-deployment.yaml which also holds strike-flow-classifier-ibkr).
        for fname, name, labels in _all_deployments():
            if name in ELIGIBLE:
                continue
            self.assertNotEqual(labels.get("oe.replay/include"), "true",
                                f"{fname}[{name}]: non-eligible service must NOT be replay-attested")


if __name__ == "__main__":
    unittest.main()
