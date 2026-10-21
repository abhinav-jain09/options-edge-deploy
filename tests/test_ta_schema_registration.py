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


# The contracts release's table (ta-books-catalog/08_SCHEMA_ID_MAP.md, contracts 0.31.0, com.optionsedge.contracts.ta.TaSchemas.SUBJECTS), row by row.
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


def read_map(service: str) -> list[tuple[str, int, str]]:
    rows = []
    for line in (MAPS / f"{service}.map").read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            subject, id_, file = line.split("|")
            rows.append((subject, int(id_), file))
    return rows


def schemas_dir(rows) -> Path:
    d = Path(tempfile.mkdtemp())
    for subject, _, file in rows:
        namespace, _, name = subject.rpartition(".")
        (d / file).parent.mkdir(parents=True, exist_ok=True)
        (d / file).write_text(json.dumps({"type": "record", "name": name, "namespace": namespace, "doc": "fixture",
                                          "fields": [{"name": "x", "type": "string"}]}))
    return d


def run(service: str, url: str, dry: bool = False, map_file: Path | None = None, rows=None):
    rows = rows if rows is not None else read_map(service)
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
                for subject, id_, _ in read_map(service):
                    self.assertEqual(reg.subjects[subject][0]["id"], id_, subject)
                    self.assertEqual(reg.subjects[subject][0]["version"], 1)
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
            subject, id_, _ = read_map("es-trade-tools")[0]
            reg.register(subject, '{"type":"record","name":"PfColumn","namespace":"x","fields":[]}')
            rc, out = run("es-trade-tools", url)
            self.assertEqual(rc, 1, out)
            self.assertIn(f"only version 1 (the fixed id {id_}) is admissible", out)
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
            for subject, _, file in read_map(service):
                self.assertTrue(subject.startswith(ns[service]), subject)
                self.assertEqual(file, subject.rsplit(".", 1)[1] + ".avsc", "one flat file per record in schemas-registry/ta")

    def test_the_three_maps_are_exactly_the_contracts_table_of_31_subjects(self):
        union = {(subject, id_) for service in SERVICES for subject, id_, _ in read_map(service)}
        self.assertEqual(len(CONTRACTS_TABLE), 31)
        self.assertEqual(union, set(CONTRACTS_TABLE))

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
