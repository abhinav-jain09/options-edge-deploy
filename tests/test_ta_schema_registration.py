"""scripts/kafka/ensure-fixed-id-schemas.sh with the three TA-books maps (scripts/kafka/schema-id-maps/{market-structure,es-trade-tools,wave-cycle}.map)
against the stand-in registry (tests/stub_schema_registry.py): every subject is imported under its fixed id with FULL, a re-run is a no-op that still
enforces FULL, a collision fails without repair, a malformed or duplicated map is refused before the registry is touched, and the three maps agree
with each other (no id reused, none inside the indicator family's 2001-2010) and with the services' own subject lists."""
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/ensure-fixed-id-schemas.sh"
MAPS = ROOT / "scripts/kafka/schema-id-maps"
SERVICES = ("market-structure", "es-trade-tools", "wave-cycle")


# The contracts release's table (options-edge-processing docs/ta-books/catalog/SCHEMA-ID-MAP.md, contracts 0.31.0, com.optionsedge.contracts.ta.TaSchemas.SUBJECTS), row by row.
CONTRACTS_TABLE = [
    ("com.optionsedge.contracts.marketstructure.ui.UiProjection", 2201),
    ("com.optionsedge.contracts.marketstructure.ui.UiUnavailable", 2202),
    ("com.optionsedge.contracts.marketstructure.patterns.PatternAnnotationSet", 2203),
    ("com.optionsedge.contracts.marketstructure.patterns.PatternUnavailable", 2204),
    ("com.optionsedge.contracts.marketstructure.sequence.SequenceRecord", 2205),
    ("com.optionsedge.contracts.marketstructure.dlq.DlqRecord", 2206),
    ("com.optionsedge.contracts.marketstructure.recovery.RecoveryCheckpoint", 2207),
    ("com.optionsedge.contracts.marketstructure.recovery.RecoveryPayload", 2208),
    ("com.optionsedge.contracts.marketstructure.tools.StructureToolsSet", 2211),
    ("com.optionsedge.contracts.marketstructure.geometry.GeometryAnnotationSet", 2221),
    ("com.optionsedge.contracts.marketstructure.geometry.GeometryUnavailable", 2222),
    ("com.optionsedge.contracts.tradetools.PfChartState", 2301),
    ("com.optionsedge.contracts.tradetools.PfColumn", 2302),
    ("com.optionsedge.contracts.tradetools.PfBreakEvent", 2303),
    ("com.optionsedge.contracts.tradetools.PfLineEvent", 2304),
    ("com.optionsedge.contracts.tradetools.PfCountEvent", 2305),
    ("com.optionsedge.contracts.tradetools.PfMaCrossEvent", 2306),
    ("com.optionsedge.contracts.tradetools.PfVolumeDivergenceEvent", 2307),
    ("com.optionsedge.contracts.tradetools.PfMoneyFlowDivergenceEvent", 2308),
    ("com.optionsedge.contracts.tradetools.PfBreakVolumeCheck", 2309),
    ("com.optionsedge.contracts.tradetools.VolumeNodeSet", 2310),
    ("com.optionsedge.contracts.tradetools.FlowBar", 2311),
    ("com.optionsedge.contracts.tradetools.ClimaxEvent", 2312),
    ("com.optionsedge.contracts.tradetools.TtCheckpointManifest", 2313),
    ("com.optionsedge.contracts.tradetools.TtCheckpointChunk", 2314),
    ("com.optionsedge.contracts.tradetools.TtFamilyStatus", 2315),
    ("com.optionsedge.contracts.wavecycle.WaveSet", 2401),
    ("com.optionsedge.contracts.wavecycle.CycleSet", 2402),
    ("com.optionsedge.contracts.wavecycle.TimeframeCheckpoint", 2403),
    ("com.optionsedge.contracts.wavecycle.SeriesCheckpoint", 2404),
    ("com.optionsedge.contracts.wavecycle.ControlCheckpoint", 2405),
]


# TICK-TRADE-TOOLS revision 21 (contracts 0.33.0): version 2 of the thirteen header-bearing trade-tools subjects, each under its version-1 id + 20.
TRADE_TOOLS_VERSION_2 = [(f"com.optionsedge.contracts.tradetools.{n}", i + 20) for n, i in (
    ("PfChartState", 2301), ("PfColumn", 2302), ("PfBreakEvent", 2303), ("PfLineEvent", 2304), ("PfCountEvent", 2305), ("PfMaCrossEvent", 2306),
    ("PfVolumeDivergenceEvent", 2307), ("PfMoneyFlowDivergenceEvent", 2308), ("PfBreakVolumeCheck", 2309), ("VolumeNodeSet", 2310), ("FlowBar", 2311),
    ("ClimaxEvent", 2312), ("TtFamilyStatus", 2315))]


# ELLIOTT-FIB-CYCLE-TOOLS revision 21 (contracts 0.36.0): version 2 of WaveSet and CycleSet under NEW ids (TaSchemas.Subject.priorId 2401, 2402).
WAVE_CYCLE_VERSION_2 = [("com.optionsedge.contracts.wavecycle.WaveSet", 2406), ("com.optionsedge.contracts.wavecycle.CycleSet", 2407)]


def read_versioned(service: str) -> list[tuple[str, int, str, int]]:
    """(subject, id, file, version) per map row; the optional fourth column defaults to version 1."""
    rows = []
    for line in (MAPS / f"{service}.map").read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            parts = line.split("|")
            rows.append((parts[0], int(parts[1]), parts[2], int(parts[3]) if len(parts) == 4 else 1))
    return rows


def read_map(service: str) -> list[tuple[str, int, str]]:
    """(subject, id, file) per row of the version-1 reading of a map (a service with several versions per subject lists a subject once per version)."""
    return [(subject, id_, file) for subject, id_, file, _ in read_versioned(service)]


def schema_text(subject: str, version: int) -> str:
    """A fixture schema; each higher version adds one more optional field, so every version of a subject is FULL-compatible with the one before."""
    namespace, _, name = subject.rpartition(".")
    fields = [{"name": "x", "type": "string"}] + [{"name": f"added{v}", "type": ["null", "string"], "default": None} for v in range(2, version + 1)]
    return json.dumps({"type": "record", "name": name, "namespace": namespace, "doc": "fixture", "fields": fields})


def schemas_dir(rows) -> Path:
    d = Path(tempfile.mkdtemp())
    for row in rows:
        subject, _, file = row[:3]
        version = row[3] if len(row) > 3 else 1
        (d / file).parent.mkdir(parents=True, exist_ok=True)
        (d / file).write_text(schema_text(subject, version))
    return d


def run(service: str, url: str, dry: bool = False, map_file: Path | None = None, rows=None):
    rows = rows if rows is not None else read_versioned(service)
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(schemas_dir(rows)), SCHEMA_ID_MAP=str(map_file or MAPS / f"{service}.map"),
               DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class Registration(unittest.TestCase):
    def test_each_map_imports_every_subject_under_its_fixed_id_with_full_and_leaves_no_mode(self):
        for service in SERVICES:
            reg = Registry()
            server, url = serve(reg)
            try:
                rc, out = run(service, url)
                self.assertEqual(rc, 0, f"{service}: {out}")
                for subject, id_, _, version in read_versioned(service):
                    row = reg.subjects[subject][version - 1]
                    self.assertEqual((row["id"], row["version"]), (id_, version), subject)
                    self.assertEqual(reg.configs[subject], "FULL")
                self.assertEqual(reg.subject_modes, {}, "an inherited mode is restored by DELETE")
                self.assertNotIn("zz.ta.importmode.probe", reg.subjects)
            finally:
                server.shutdown()

    def test_rerun_is_a_no_op_that_still_enforces_full(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run("market-structure", url)[0], 0)
            victim = read_map("market-structure")[0][0]
            reg.configs[victim] = "BACKWARD"
            calls = len(reg.calls)
            rc, out = run("market-structure", url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.configs[victim], "FULL")
            self.assertFalse(any(c.startswith("POST /subjects/") and c.endswith("/versions {schema…}") for c in reg.calls[calls:]), "nothing re-imported")
        finally:
            server.shutdown()

    def test_a_fixed_id_held_by_another_schema_fails_in_preflight_and_nothing_at_all_is_imported(self):
        rows = read_map("wave-cycle")
        reg = Registry()
        reg.register("some.other.Subject", json.dumps({"type": "string"}), id=rows[1][1], version=1)
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 1, out)
            self.assertIn(f"fixed id {rows[1][1]} is already held by another schema", out)
            self.assertIn("nothing was changed", out)
            self.assertNotIn(rows[0][0], reg.subjects)   # the collision on the LATER subject stopped the whole set before the first import
            self.assertNotIn(rows[1][0], reg.subjects)
        finally:
            server.shutdown()

    def test_a_later_version_under_a_subject_is_refused(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            self.assertEqual(run("es-trade-tools", url)[0], 0)
            subject, id_, _, _ = [r for r in read_versioned("es-trade-tools") if r[3] == 1][0]
            self.assertEqual(len(reg.subjects[subject]), 2)
            reg.register(subject, '{"type":"record","name":"PfColumn","namespace":"x","fields":[]}')   # a stray version 3 the map never vetted
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("anything else was never vetted by this map", out)
        finally:
            server.shutdown()

    def test_dry_run_mutates_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run("market-structure", url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual(reg.subject_modes, {})
        finally:
            server.shutdown()

    def test_registry_without_mode_mutability_is_refused_before_any_subject(self):
        reg = Registry(mutable=False)
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 1)
            self.assertIn("subject-level IMPORT is not allowed", out)
            self.assertEqual(reg.subjects, {})
        finally:
            server.shutdown()

    def test_a_missing_schema_file_fails(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rows = read_map("wave-cycle")
            env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(schemas_dir(rows[:-1])), SCHEMA_ID_MAP=str(MAPS / "wave-cycle.map"),
                       DEPLOY_DRY_RUN="false")
            r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
            self.assertIn(f"{rows[-1][2]} missing", r.stdout)
        finally:
            server.shutdown()


def seed_version_1(reg: Registry) -> None:
    """The state of dev and production before contracts 0.33.0: every es-trade-tools subject at version 1 under its old id, compatibility FULL."""
    for subject, id_, _, version in read_versioned("es-trade-tools"):
        if version == 1:
            reg.register(subject, schema_text(subject, 1), id=id_, version=1)
            reg.configs[subject] = "FULL"


def seed_wave_cycle_version_1(reg: Registry) -> None:
    """The state of dev and production before contracts 0.36.0: the five wave-cycle subjects at version 1 under 2401-2405, compatibility FULL."""
    for subject, id_, _, version in read_versioned("wave-cycle"):
        if version == 1:
            reg.register(subject, schema_text(subject, 1), id=id_, version=1)
            reg.configs[subject] = "FULL"


class WaveCycleVersionTwoRegistration(unittest.TestCase):
    """ELLIOTT-FIB-CYCLE-TOOLS revision 21: WaveSet and CycleSet gain version 2 under ids 2406 and 2407 on a registry that already holds version 1."""

    def test_a_registry_holding_version_1_receives_only_the_two_version_2_imports(self):
        reg = Registry()
        seed_wave_cycle_version_1(reg)
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("would import as version 2 under id 2406", out)
            self.assertIn("would import as version 2 under id 2407", out)
            self.assertNotIn("would import as version 1", out)
            for id_ in (2401, 2402, 2403, 2404, 2405):
                self.assertIn(f"registered: version 1, id {id_}", out)
            self.assertFalse([c for c in reg.calls if c.startswith("POST /subjects/") and c.endswith("/versions {schema…}")], "a dry run imports nothing")
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 0, out)
            imports = [c for c in reg.calls if c.startswith("POST /subjects/") and c.endswith("/versions {schema…}")]
            self.assertEqual(len(imports), 2, "only version 2 of the two subjects is imported")
            for subject, id_ in WAVE_CYCLE_VERSION_2:
                self.assertEqual([(v["version"], v["id"]) for v in reg.subjects[subject]], [(1, id_ - 5), (2, id_)], subject)
                self.assertEqual(reg.configs[subject], "FULL")
            for name in ("TimeframeCheckpoint", "SeriesCheckpoint", "ControlCheckpoint"):
                self.assertEqual(len(reg.subjects[f"com.optionsedge.contracts.wavecycle.{name}"]), 1, name)
            self.assertEqual(reg.subject_modes, {})
            self.assertTrue(any(c.startswith("POST /compatibility/subjects/") for c in reg.calls), "the registry's own compatibility verdict is asked first")
            calls = len(reg.calls)
            rc, out = run("wave-cycle", url)   # idempotent
            self.assertEqual(rc, 0, out)
            self.assertFalse([c for c in reg.calls[calls:] if c.startswith("POST /subjects/") and c.endswith("/versions {schema…}")], "a re-run imports nothing")
        finally:
            server.shutdown()

    def test_an_incompatible_wave_set_is_refused_in_preflight_and_nothing_at_all_is_imported(self):
        reg = Registry()
        seed_wave_cycle_version_1(reg)
        reg.incompatible.add(WAVE_CYCLE_VERSION_2[1][0])   # the LATER row (CycleSet) fails: the first row must not have been imported either
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("is not compatible with the latest registered version", out)
            self.assertIn("nothing was changed", out)
            for subject, _ in WAVE_CYCLE_VERSION_2:
                self.assertEqual(len(reg.subjects[subject]), 1, subject)
        finally:
            server.shutdown()

    def test_a_version_2_id_held_by_another_schema_fails_in_preflight_with_nothing_changed(self):
        reg = Registry()
        seed_wave_cycle_version_1(reg)
        reg.register("some.other.Subject", json.dumps({"type": "string"}), id=2407, version=1)
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("nothing was changed", out)
            for subject, _ in WAVE_CYCLE_VERSION_2:
                self.assertEqual(len(reg.subjects[subject]), 1, subject)
        finally:
            server.shutdown()

    def test_a_fresh_registry_gets_all_five_subjects_and_both_versions_in_one_run(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 0, out)
            self.assertEqual({s: [(v["version"], v["id"]) for v in rows] for s, rows in reg.subjects.items()},
                             {"com.optionsedge.contracts.wavecycle.WaveSet": [(1, 2401), (2, 2406)],
                              "com.optionsedge.contracts.wavecycle.CycleSet": [(1, 2402), (2, 2407)],
                              "com.optionsedge.contracts.wavecycle.TimeframeCheckpoint": [(1, 2403)],
                              "com.optionsedge.contracts.wavecycle.SeriesCheckpoint": [(1, 2404)],
                              "com.optionsedge.contracts.wavecycle.ControlCheckpoint": [(1, 2405)]})
        finally:
            server.shutdown()


class VersionTwoRegistration(unittest.TestCase):
    """TICK-TRADE-TOOLS revision 21: thirteen subjects gain version 2 under a NEW fixed id on a registry that already holds version 1."""

    def test_a_registry_holding_version_1_receives_only_the_thirteen_version_2_imports(self):
        reg = Registry()
        seed_version_1(reg)
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 0, out)
            imports = [c for c in reg.calls if c.startswith("POST /subjects/") and c.endswith("/versions {schema…}")]
            self.assertEqual(len(imports), 13, "only version 2 of the thirteen subjects is imported")
            for subject, id_ in TRADE_TOOLS_VERSION_2:
                self.assertEqual([(v["version"], v["id"]) for v in reg.subjects[subject]], [(1, id_ - 20), (2, id_)], subject)
                self.assertEqual(reg.configs[subject], "FULL")
            for subject in ("com.optionsedge.contracts.tradetools.TtCheckpointManifest", "com.optionsedge.contracts.tradetools.TtCheckpointChunk"):
                self.assertEqual(len(reg.subjects[subject]), 1)
            self.assertEqual(reg.subject_modes, {})
            self.assertTrue(any(c.startswith("POST /compatibility/subjects/") for c in reg.calls), "the registry's own compatibility verdict is asked first")
            calls = len(reg.calls)
            rc, out = run("es-trade-tools", url)   # idempotent
            self.assertEqual(rc, 0, out)
            self.assertFalse([c for c in reg.calls[calls:] if c.startswith("POST /subjects/") and c.endswith("/versions {schema…}")], "a re-run imports nothing")
        finally:
            server.shutdown()

    def test_an_incompatible_version_2_is_refused_in_preflight_and_nothing_at_all_is_imported(self):
        reg = Registry()
        seed_version_1(reg)
        victim = TRADE_TOOLS_VERSION_2[5][0]
        reg.incompatible.add(victim)
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("is not compatible with the latest registered version", out)
            self.assertIn("nothing was changed", out)
            for subject, _ in TRADE_TOOLS_VERSION_2:
                self.assertEqual(len(reg.subjects[subject]), 1, subject)
            self.assertEqual(reg.subject_modes, {})
        finally:
            server.shutdown()

    def test_a_version_2_id_held_by_another_schema_fails_in_preflight_with_nothing_changed(self):
        reg = Registry()
        seed_version_1(reg)
        reg.register("some.other.Subject", json.dumps({"type": "string"}), id=TRADE_TOOLS_VERSION_2[-1][1], version=1)
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("nothing was changed", out)
            for subject, _ in TRADE_TOOLS_VERSION_2:
                self.assertEqual(len(reg.subjects[subject]), 1, subject)
        finally:
            server.shutdown()

    def test_a_subject_whose_version_1_is_a_different_schema_is_a_collision(self):
        reg = Registry()
        seed_version_1(reg)
        subject = TRADE_TOOLS_VERSION_2[0][0]
        reg.subjects[subject][0]["schema"] = json.dumps({"type": "record", "name": "PfChartState", "namespace": "x", "fields": []})
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("a collision this script never repairs", out)
        finally:
            server.shutdown()

    def test_dry_run_on_a_registry_holding_version_1_mutates_nothing(self):
        reg = Registry()
        seed_version_1(reg)
        before = {s: list(v) for s, v in reg.subjects.items()}
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("would import as version 2", out)
            self.assertEqual(reg.subjects, before)
            self.assertEqual(reg.subject_modes, {})
        finally:
            server.shutdown()

    def test_a_subject_not_at_full_is_refused_in_preflight_because_the_registrys_verdict_would_not_be_the_full_one(self):
        reg = Registry()
        seed_version_1(reg)
        victim = TRADE_TOOLS_VERSION_2[0][0]
        reg.configs[victim] = "BACKWARD"
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("is not at compatibility FULL now", out)
            self.assertIn("nothing was changed", out)
            for subject, _ in TRADE_TOOLS_VERSION_2:
                self.assertEqual(len(reg.subjects[subject]), 1, subject)
            self.assertEqual(reg.configs[victim], "BACKWARD", "and nothing was repaired")
        finally:
            server.shutdown()

    def test_a_subject_already_in_import_mode_is_never_captured_as_the_original_mode(self):
        reg = Registry()
        seed_version_1(reg)
        victim = TRADE_TOOLS_VERSION_2[0][0]
        reg.subject_modes[victim] = "IMPORT"   # another run in flight
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("already in subject-level IMPORT mode", out)
            self.assertEqual(reg.subject_modes, {victim: "IMPORT"}, "left exactly as found: this run neither restored nor cleared it")
            self.assertEqual(len(reg.subjects[victim]), 1, "and did not import into it")
        finally:
            server.shutdown()

    def test_a_probe_mode_that_could_not_be_removed_is_removed_by_the_exit_trap(self):
        reg = Registry()
        reg.fail_next_mode_delete = 1   # the probe's DELETE fails once
        server, url = serve(reg)
        try:
            rc, out = run("wave-cycle", url)
            self.assertEqual(rc, 1, out)
            self.assertIn("could not remove the probe mode", out)
            self.assertEqual(reg.subject_modes, {}, "the trap removed the leftover IMPORT mode of the probe subject")
            self.assertEqual(reg.subjects, {}, "and no real subject was touched")
        finally:
            server.shutdown()

    def test_a_fresh_registry_gets_version_1_and_version_2_in_one_run(self):
        # covered by test_each_map_imports_every_subject...; here the preflight must accept version 2 rows whose version 1 is imported by an earlier row
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = run("es-trade-tools", url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects, {})
        finally:
            server.shutdown()


class MapValidation(unittest.TestCase):
    def refused(self, text: str):
        reg = Registry()
        server, url = serve(reg)
        try:
            f = Path(tempfile.mkdtemp()) / "bad.map"
            f.write_text(text)
            rc, out = run("market-structure", url, map_file=f, rows=[])
            self.assertEqual(rc, 1, out)
            self.assertEqual(reg.calls, [], "the registry was not touched")
        finally:
            server.shutdown()

    def test_empty_malformed_duplicated_maps_are_refused_before_the_registry_is_touched(self):
        self.refused("# nothing\n")
        self.refused("a.B|2201\n")
        self.refused("a.B|x|f.avsc\n")
        self.refused("a.B|2201|f.avsc\na.B|2202|g.avsc\n")
        self.refused("a.B|2201|f.avsc\na.C|2201|g.avsc\n")
        self.refused("a.B|2201|f.avsc|1\na.B|2202|g.avsc|1\n")      # a repeated subject version
        self.refused("a.B|2201|f.avsc|2\n")                         # versions must start at 1
        self.refused("a.B|2201|f.avsc|1\na.B|2202|g.avsc|3\n")      # a gap
        self.refused("a.B|2201|f.avsc|0\n")
        self.refused("a.B|2201|f.avsc\na.B|2202|g.avsc\n")          # the default version is 1, so a second row without a version repeats it


class MapsAgree(unittest.TestCase):
    def test_no_id_is_reused_across_maps_and_none_collides_with_the_indicator_family(self):
        seen: dict[int, str] = {}
        for service in SERVICES:
            for subject, id_, _ in read_map(service):
                self.assertNotIn(id_, seen, f"{subject} reuses {id_} ({seen.get(id_)})")
                seen[id_] = subject
                self.assertFalse(2001 <= id_ <= 2010, f"{subject}: {id_} is inside the indicator family's fixed ids")

    def test_subjects_are_the_record_full_names_in_the_contracts_namespaces(self):
        ns = {"market-structure": "com.optionsedge.contracts.marketstructure.", "es-trade-tools": "com.optionsedge.contracts.tradetools.",
              "wave-cycle": "com.optionsedge.contracts.wavecycle."}
        for service in SERVICES:
            for subject, _, file, version in read_versioned(service):
                self.assertTrue(subject.startswith(ns[service]), subject)
                name = subject.rsplit(".", 1)[1] + ".avsc"
                # the current (highest) version is the flat file; version 1 of a two-version subject is the frozen copy under v1/
                top = max(v for s_, _, _, v in read_versioned(service) if s_ == subject)
                self.assertEqual(file, name if version == top else f"v{version}/{name}", "the highest version is the flat file, earlier ones live under v<N>/")

    def test_the_three_maps_are_exactly_the_contracts_table_of_31_subjects_and_46_ids(self):
        union = {(subject, id_) for service in SERVICES for subject, id_, _ in read_map(service)}
        self.assertEqual(len(CONTRACTS_TABLE), 31)
        self.assertEqual(union, set(CONTRACTS_TABLE) | set(TRADE_TOOLS_VERSION_2) | set(WAVE_CYCLE_VERSION_2))
        self.assertEqual({s for s, _ in union}, {s for s, _ in CONTRACTS_TABLE})
        self.assertEqual(len(union), 46)
        # version 2 of each of the thirteen two-version subjects = its version-1 id + 20 (contracts 0.33.0, TaSchemas.Subject.priorId)
        v1 = {s: i for s, i in CONTRACTS_TABLE}
        for subject, id_ in TRADE_TOOLS_VERSION_2:
            self.assertEqual(id_, v1[subject] + 20, subject)
        self.assertEqual(len(TRADE_TOOLS_VERSION_2), 13)
        # WaveSet 2401 -> 2406 and CycleSet 2402 -> 2407 (contracts 0.36.0, TaSchemas.Subject.priorId); neither offset is the trade-tools +20
        for subject, id_ in WAVE_CYCLE_VERSION_2:
            self.assertEqual(id_, v1[subject] + 5, subject)
        self.assertEqual(len(WAVE_CYCLE_VERSION_2), 2)

    def test_w0_ids_are_the_runbooks(self):
        rows = {s.rsplit(".", 1)[1]: i for s, i, _ in read_map("market-structure")}
        self.assertEqual([rows[n] for n in ("UiProjection", "UiUnavailable", "PatternAnnotationSet", "PatternUnavailable", "SequenceRecord", "DlqRecord",
                                            "RecoveryCheckpoint", "RecoveryPayload")], list(range(2201, 2209)))

    def test_the_trade_tools_map_names_the_services_fifteen_record_types(self):
        names = {s.rsplit(".", 1)[1] for s, _, _ in read_map("es-trade-tools")}
        self.assertEqual(len(names), 15)
        self.assertEqual(names, {"PfColumn", "PfChartState", "PfBreakEvent", "PfLineEvent", "PfCountEvent", "PfMaCrossEvent", "PfVolumeDivergenceEvent",
                                 "PfMoneyFlowDivergenceEvent", "PfBreakVolumeCheck", "VolumeNodeSet", "FlowBar", "ClimaxEvent", "TtCheckpointManifest",
                                 "TtCheckpointChunk", "TtFamilyStatus"})

    def test_the_wave_cycle_map_names_the_five_top_level_records_the_service_resolves_at_boot(self):
        names = {s.rsplit(".", 1)[1] for s, _, _ in read_map("wave-cycle")}
        self.assertEqual(names, {"WaveSet", "CycleSet", "TimeframeCheckpoint", "SeriesCheckpoint", "ControlCheckpoint"})

    def test_the_jenkins_stage_picks_the_map_by_service_name(self):
        text = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.assertIn('SCHEMA_ID_MAP="scripts/kafka/schema-id-maps/${SERVICE_PARAM}.map"', text)
        self.assertIn('SCHEMAS_DIR=".jenkins-tmp/contracts-ta/schemas-registry/ta"', text)
        for service in SERVICES:
            self.assertTrue((MAPS / f"{service}.map").is_file())


if __name__ == "__main__":
    unittest.main()
