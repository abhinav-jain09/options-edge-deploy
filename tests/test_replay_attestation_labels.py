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


def _top_metadata_labels(text: str) -> dict[str, str]:
    """Parse a single-doc Deployment's top-level metadata.labels (dependency-free)."""
    lines = text.split("\n")
    # metadata is everything before the column-0 'spec:'
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


def _deployment_name(text: str) -> str:
    for l in text.split("\n"):
        if l.startswith("  name:"):
            return l.split(":", 1)[1].strip()
    return ""


def _deployment_files() -> list[Path]:
    return sorted(BASE.glob("*deployment*.yaml"))


class ReplayAttestationLabelsTest(unittest.TestCase):
    def test_every_eligible_processor_carries_the_full_attestation_set(self) -> None:
        seen = set()
        for f in _deployment_files():
            text = f.read_text()
            name = _deployment_name(text)
            if name not in ELIGIBLE:
                continue
            seen.add(name)
            labels = _top_metadata_labels(text)
            for key, want in REQUIRED.items():
                self.assertEqual(labels.get(key), want,
                                 f"{f.name}: metadata label {key} must be '{want}', got {labels.get(key)!r}")
        missing = ELIGIBLE - seen
        self.assertEqual(missing, set(), f"eligible processors missing a deployment manifest: {missing}")

    def test_ci_lint_include_true_requires_the_other_four_labels(self) -> None:
        # The default-deny invariant: any Deployment opting in (include=true) MUST carry the full,
        # correctly-valued set — a partial/mis-valued attestation is a CI failure (fail closed).
        for f in _deployment_files():
            labels = _top_metadata_labels(f.read_text())
            if labels.get("oe.replay/include") != "true":
                continue
            for key, want in REQUIRED.items():
                self.assertEqual(labels.get(key), want,
                                 f"{f.name}: oe.replay/include=true but {key}={labels.get(key)!r} (require '{want}')")

    def test_non_eligible_services_are_not_attested(self) -> None:
        for f in _deployment_files():
            text = f.read_text()
            name = _deployment_name(text)
            if name in MUST_NOT_INCLUDE:
                labels = _top_metadata_labels(text)
                self.assertNotEqual(labels.get("oe.replay/include"), "true",
                                    f"{f.name}: {name} must NOT be replay-attested (external side-effects)")


if __name__ == "__main__":
    unittest.main()
